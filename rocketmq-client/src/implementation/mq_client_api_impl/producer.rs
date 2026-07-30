// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::request_builder::heartbeat_request;
use super::*;

pub struct ProducerClient<'a> {
    api: &'a MQClientAPIImpl,
}

impl ProducerClient<'_> {
    pub async fn send_heartbeat(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<(i32, Option<RemotingCommand>)> {
        self.api.send_heartbeat(addr, heartbeat_data, timeout_millis).await
    }
}

impl MQClientAPIImpl {
    #[must_use]
    pub fn producer_client(&self) -> ProducerClient<'_> {
        ProducerClient { api: self }
    }
}

impl MQClientAPIImpl {
    #[allow(
        clippy::too_many_arguments,
        reason = "existing send wire adapter signature is tracked by the lint debt registry"
    )]
    pub async fn send_message<T>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<Arc<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait,
    {
        let begin_start_time = Instant::now();
        let msg_type = msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TYPE));
        let is_reply = msg_type
            .as_ref()
            .is_some_and(|msg_type| msg_type.as_str() == mix_all::REPLY_MESSAGE_FLAG);
        let mut request = if is_reply {
            if *SEND_SMART_MSG {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
                RemotingCommand::create_request_command(RequestCode::SendReplyMessageV2, request_header_v2)
            } else {
                RemotingCommand::create_request_command(RequestCode::SendReplyMessage, request_header)
            }
        } else {
            let is_batch_message = msg.as_any().downcast_ref::<MessageBatch>().is_some();
            if *SEND_SMART_MSG || is_batch_message {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
                let request_code = if is_batch_message {
                    RequestCode::SendBatchMessage
                } else {
                    RequestCode::SendMessageV2
                };
                RemotingCommand::create_request_command(request_code, request_header_v2)
            } else {
                RemotingCommand::create_request_command(RequestCode::SendMessage, request_header)
            }
        };

        // Zero-copy optimization: Bytes is reference-counted, clone() only increments ref count
        // This is very cheap (~5ns) compared to deep copying the message body
        // For true zero-copy, we would need to restructure to pass &Bytes through the entire chain
        if let Some(compressed_body) = msg.get_compressed_body() {
            request.set_body_mut_ref(compressed_body.clone());
        } else if let Some(body) = msg.get_body() {
            request.set_body_mut_ref(body.clone());
        } else {
            return Err(mq_client_err!(-1, "Message body is None"));
        }
        match communication_mode {
            CommunicationMode::Sync => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: timeout_millis,
                    });
                }
                let result = self
                    .send_message_sync(addr, broker_name, msg, timeout_millis - cost_time_sync, request)
                    .await?;
                Ok(Some(result))
            }
            CommunicationMode::Async => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: timeout_millis,
                    });
                }
                self.send_message_async(
                    addr,
                    broker_name,
                    msg,
                    timeout_millis,
                    request,
                    send_callback,
                    topic_publish_info,
                    instance,
                    retry_times_when_send_failed,
                    context,
                    producer,
                )
                .await;
                Ok(None)
            }
            CommunicationMode::Oneway => {
                self.remoting_client
                    .invoke_request_oneway(addr, request, timeout_millis)
                    .await;
                Ok(None)
            }
        }
    }

    /// **High-Performance** unbounded oneway send without timeout control.
    ///
    /// This method provides **maximum throughput** by spawning background tasks immediately
    /// without waiting for network send completion, achieving near-zero latency overhead.
    ///
    /// # Performance Characteristics
    /// - **Latency**: < 10μs per send (tokio spawn overhead only)
    /// - **Throughput**: 100K+ messages/second per producer
    /// - **Memory**: ~1KB per spawned task
    /// - **Zero blocking**: Returns immediately after task spawn
    ///
    /// # When to Use
    /// Ideal for high-throughput scenarios where:
    /// - **Fire-and-forget** semantics are required
    /// - Message loss is acceptable (e.g., metrics, logs, telemetry)
    /// - **Maximum throughput** is the priority over reliability
    /// - Latency is critical (< 10μs send overhead)
    ///
    /// # Use Cases
    /// - Log collection and aggregation
    /// - Metrics reporting
    /// - Real-time telemetry
    /// - High-frequency event streaming
    pub async fn send_oneway_unbounded(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.remoting_client.invoke_oneway_unbounded(addr.clone(), request);
        Ok(())
    }

    pub async fn send_message_simple<T>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait,
    {
        self.send_message(
            addr,
            broker_name,
            msg,
            request_header,
            timeout_millis,
            communication_mode,
            None,
            None,
            None,
            0,
            context,
            producer,
        )
        .await
    }

    pub(super) async fn send_message_sync<T>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        T: MessageTrait,
    {
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        self.process_send_response(broker_name, msg, &response, addr)
    }

    pub(super) async fn send_message_async<T: MessageTrait>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<Arc<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) {
        // Extract message metadata before spawning (msg cannot be moved)
        let msg_topic = msg.topic().clone();
        let is_batch_message = msg.as_any().downcast_ref::<MessageBatch>().is_some();

        // For MessageBatch, pre-compute combined uniq_id from all messages
        let msg_uniq_id = if is_batch_message {
            if let Some(batch) = msg.as_any().downcast_ref::<MessageBatch>() {
                let mut combined_id = String::new();
                for msg in &batch.messages {
                    if !combined_id.is_empty() {
                        combined_id.push(',');
                    }
                    if let Some(id) = MessageClientIDSetter::get_uniq_id(msg) {
                        combined_id.push_str(id.as_str());
                    }
                }
                if combined_id.is_empty() {
                    None
                } else {
                    Some(CheetahString::from_string(combined_id))
                }
            } else {
                None
            }
        } else {
            MessageClientIDSetter::get_uniq_id(msg)
        };

        // Clone all necessary data for background task
        let remoting_client = self.remoting_client.clone();
        let client_config = self.client_config.clone();
        let current_addr = addr.clone();
        let current_broker_name = broker_name.clone();
        let current_request = request;
        let topic_publish_info_cloned = topic_publish_info.cloned();
        let instance_cloned = instance.clone();
        let mq_fault_strategy = producer.fault_strategy_snapshot();
        // Snapshot only the immutable hook capability and context data needed by the callback.
        let context_data = context.as_ref().map(|c| AsyncSendHookContext {
            producer_group: c.producer_group.as_ref().cloned(),
            broker_addr: c.broker_addr.as_ref().cloned(),
            born_host: c.born_host.as_ref().cloned(),
            communication_mode: c.communication_mode,
            msg_type: c.msg_type,
            namespace: c.namespace.as_ref().cloned(),
            mq_trace_context: c.mq_trace_context.clone(),
            hooks: producer.send_message_hooks(),
            mq: c.mq.cloned(),
            message_trace_snapshot: c.message_trace_snapshot.clone(),
            trace_start_time: c.trace_start_time,
        });

        Self::send_message_async_impl(
            remoting_client,
            client_config,
            mq_fault_strategy,
            current_addr,
            current_broker_name,
            msg_topic,
            msg_uniq_id,
            is_batch_message,
            timeout_millis,
            current_request,
            send_callback,
            topic_publish_info_cloned,
            instance_cloned,
            retry_times_when_send_failed,
            context_data,
        )
        .await;
    }

    /// Background task implementation for async message sending.
    #[allow(clippy::type_complexity)]
    #[allow(
        clippy::too_many_arguments,
        reason = "existing asynchronous send context is tracked by the lint debt registry"
    )]
    pub(super) async fn send_message_async_impl(
        remoting_client: Arc<RocketmqDefaultClient<ClientRemotingProcessor>>,
        client_config: Arc<ClientConfig>,
        mq_fault_strategy: MQFaultStrategy,
        mut current_addr: CheetahString,
        mut current_broker_name: CheetahString,
        msg_topic: CheetahString,
        msg_uniq_id: Option<CheetahString>,
        _is_batch_message: bool,
        timeout_millis: u64,
        current_request: RemotingCommand,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<TopicPublishInfo>,
        instance: Option<Arc<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context_data: Option<AsyncSendHookContext>,
    ) {
        let begin_start_time_all = Instant::now();
        let mut retry_count = 0_u32;
        let mut retry_request = AsyncRetryRequest::new(current_request);

        loop {
            let elapsed = (Instant::now() - begin_start_time_all).as_millis() as u64;
            if elapsed >= timeout_millis {
                let err = rocketmq_error::RocketMQError::Timeout {
                    operation: "sendMessageAsync",
                    timeout_ms: timeout_millis,
                };
                Self::execute_async_send_hook_after(&context_data, None, Some(Self::context_error(err.to_string())));
                Self::notify_send_callback_exception(&send_callback, &err);
                return;
            }

            let remaining_timeout = timeout_millis - elapsed;
            let begin_attempt_time = Instant::now();
            let keep_request_for_retry = retry_count < retry_times_when_send_failed;
            let attempt_request = retry_request.next_attempt(keep_request_for_retry);
            let result = remoting_client
                .invoke_request(Some(&current_addr), attempt_request, remaining_timeout)
                .await;
            let cost = (Instant::now() - begin_attempt_time).as_millis() as u64;

            match result {
                Ok(response) => {
                    // Determine send status
                    let response_code = ResponseCode::from(response.code());
                    let send_status = match response_code {
                        ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
                        ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
                        ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
                        ResponseCode::Success => SendStatus::SendOk,
                        _ => {
                            // Non-success response: update fault and call callback with an error
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, true, true)
                                .await;
                            let err_obj = mq_client_err!(
                                response.code(),
                                response.remark().map_or("".to_string(), |s| s.to_string())
                            );
                            Self::execute_async_send_hook_after(
                                &context_data,
                                None,
                                Some(Self::context_error(err_obj.to_string())),
                            );
                            Self::notify_send_callback_exception(&send_callback, &err_obj);
                            return;
                        }
                    };

                    // Try to decode response header and build SendResult
                    match response.decode_command_custom_header_fast::<SendMessageResponseHeader>() {
                        Ok(response_header) => {
                            let mut topic = msg_topic.to_string();
                            if let Some(ns) = client_config.get_namespace_v2() {
                                if !ns.is_empty() {
                                    topic =
                                        NamespaceUtil::without_namespace_with_namespace(topic.as_str(), ns.as_str());
                                }
                            }
                            let message_queue = MessageQueue::from_parts(
                                topic.as_str(),
                                &current_broker_name,
                                response_header.queue_id(),
                            );
                            let region_id = response
                                .ext_fields()
                                .and_then(|m| m.get(MessageConst::PROPERTY_MSG_REGION).map(|s| s.to_string()))
                                .unwrap_or_else(|| mix_all::DEFAULT_TRACE_REGION_ID.to_string());
                            let trace_on = trace_on_from_ext_fields(response.ext_fields());
                            let queue_offset = match java_long_to_u64_field(
                                "sendMessage",
                                "queueOffset",
                                response_header.queue_offset(),
                            ) {
                                Ok(queue_offset) => queue_offset,
                                Err(err_obj) => {
                                    mq_fault_strategy
                                        .update_fault_item(current_broker_name.clone(), cost, true, true)
                                        .await;
                                    Self::execute_async_send_hook_after(
                                        &context_data,
                                        None,
                                        Some(Self::context_error(err_obj.to_string())),
                                    );
                                    Self::notify_send_callback_exception(&send_callback, &err_obj);
                                    return;
                                }
                            };

                            let send_result = SendResult {
                                send_status,
                                msg_id: msg_uniq_id.clone(),
                                offset_msg_id: Some(response_header.msg_id().to_string()),
                                message_queue: Some(message_queue),
                                queue_offset,
                                transaction_id: response_header.transaction_id().map(|s| s.to_string()),
                                recall_handle: response_header.recall_handle().map(|s| s.to_string()),
                                region_id: Some(region_id),
                                trace_on,
                                ..Default::default()
                            };

                            // Success: update fault item and invoke callback
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, false, true)
                                .await;
                            Self::execute_async_send_hook_after(&context_data, Some(&send_result), None);
                            Self::notify_send_callback_success(&send_callback, &send_result);
                            return;
                        }
                        Err(_) => {
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, true, true)
                                .await;
                            let err_obj = mq_client_err!("decode SendMessageResponseHeader failed".to_string());
                            Self::execute_async_send_hook_after(
                                &context_data,
                                None,
                                Some(Self::context_error(err_obj.to_string())),
                            );
                            Self::notify_send_callback_exception(&send_callback, &err_obj);
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("send message async error: {:?}", e);
                    mq_fault_strategy
                        .update_fault_item(current_broker_name.clone(), cost, true, true)
                        .await;

                    let retry_elapsed = (Instant::now() - begin_start_time_all).as_millis() as u64;
                    let has_retry_budget = retry_count < retry_times_when_send_failed
                        && retry_elapsed < timeout_millis
                        && Self::should_retry_async_send_error(&e);
                    if has_retry_budget {
                        retry_count += 1;
                        if let Some((retry_addr, retry_broker_name)) = Self::select_async_retry_target(
                            &mq_fault_strategy,
                            topic_publish_info.as_ref(),
                            instance.as_ref(),
                            &current_broker_name,
                            &current_addr,
                        )
                        .await
                        {
                            warn!(
                                "async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}",
                                retry_count, msg_topic, retry_addr, retry_broker_name
                            );
                            current_addr = retry_addr;
                            current_broker_name = retry_broker_name;
                            retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
                            continue;
                        }
                    }

                    Self::execute_async_send_hook_after(&context_data, None, Some(Self::context_error(e.to_string())));
                    Self::notify_send_callback_exception(&send_callback, &e);
                    return;
                }
            }
        }
    }

    pub(super) fn select_async_retry_queue(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: Option<&TopicPublishInfo>,
        broker_name: &CheetahString,
    ) -> Option<MessageQueue> {
        topic_publish_info.and_then(|topic_publish_info| {
            mq_fault_strategy.select_one_message_queue(topic_publish_info, Some(broker_name), false)
        })
    }

    pub(super) fn should_retry_async_send_error(error: &rocketmq_error::RocketMQError) -> bool {
        crate::common::retry_decision::should_retry_async_send_error(error)
    }

    pub(super) async fn select_async_retry_target(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<&Arc<MQClientInstance>>,
        broker_name: &CheetahString,
        current_addr: &CheetahString,
    ) -> Option<(CheetahString, CheetahString)> {
        let mut retry_broker_name = broker_name.clone();
        if let Some(mq_chosen) = Self::select_async_retry_queue(mq_fault_strategy, topic_publish_info, broker_name) {
            retry_broker_name = if let Some(instance) = instance {
                instance.get_broker_name_from_message_queue(&mq_chosen).await
            } else {
                mq_chosen.broker_name().clone()
            };
        }

        let retry_addr = instance
            .and_then(|instance| instance.find_broker_address_in_publish(retry_broker_name.as_ref()))
            .unwrap_or_else(|| current_addr.clone());
        Some((retry_addr, retry_broker_name))
    }

    pub(super) fn execute_async_send_hook_after(
        context_data: &Option<AsyncSendHookContext>,
        send_result: Option<&SendResult>,
        exception: Option<Arc<RocketMQError>>,
    ) {
        let Some(context_data) = context_data.as_ref() else {
            return;
        };

        let context = Some(SendMessageContext {
            producer_group: context_data.producer_group.clone(),
            broker_addr: context_data.broker_addr.clone(),
            born_host: context_data.born_host.clone(),
            communication_mode: context_data.communication_mode,
            send_result,
            exception,
            mq_trace_context: context_data.mq_trace_context.clone(),
            msg_type: context_data.msg_type,
            namespace: context_data.namespace.clone(),
            mq: context_data.mq.as_ref(),
            message_trace_snapshot: context_data.message_trace_snapshot.clone(),
            trace_start_time: context_data.trace_start_time,
            ..Default::default()
        });
        for hook in context_data.hooks.iter() {
            hook.send_message_after(&context);
        }
    }

    pub(super) fn context_error(message: String) -> Arc<RocketMQError> {
        Arc::new(RocketMQError::response_process_failed("send_callback", message))
    }

    pub(super) fn spawn_api_background_task<F>(
        service_context: &ChildServiceContext,
        thread_name: &'static str,
        tracker: &TaskTracker,
        shutdown_token: &CancellationToken,
        task: F,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        if shutdown_token.is_cancelled() {
            return;
        }

        let shutdown_token = shutdown_token.clone();
        let tracked_task = tracker.track_future(async move {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => {},
                _ = task => {},
            }
        });

        if let Err(error) = spawn_client_task_with_context(service_context, thread_name, tracked_task) {
            warn!("Failed to spawn {} background task: {}", thread_name, error);
        }
    }

    pub(super) fn notify_send_callback_success(send_callback: &Option<ArcSendCallback>, send_result: &SendResult) {
        let Some(callback) = send_callback.as_ref().cloned() else {
            return;
        };

        callback.on_success(send_result);
    }

    pub(super) fn notify_send_callback_exception(send_callback: &Option<ArcSendCallback>, error: &RocketMQError) {
        let Some(callback) = send_callback.as_ref().cloned() else {
            return;
        };

        callback.on_exception(error);
    }

    pub(super) fn process_send_response<T>(
        &self,
        broker_name: &CheetahString,
        msg: &T,
        response: &RemotingCommand,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        T: MessageTrait,
    {
        let response_code = ResponseCode::from(response.code());
        let send_status = match response_code {
            ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
            ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
            ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
            ResponseCode::Success => SendStatus::SendOk,
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        };
        let response_header = response.decode_command_custom_header_fast::<SendMessageResponseHeader>()?;
        let mut topic = msg.topic().to_string();
        if let Some(ns) = self.client_config.get_namespace_v2() {
            if !ns.is_empty() {
                topic = NamespaceUtil::without_namespace_with_namespace(topic.as_str(), ns.as_str());
            }
        }
        let message_queue = MessageQueue::from_parts(topic.as_str(), broker_name, response_header.queue_id());
        let mut uniq_msg_id = MessageClientIDSetter::get_uniq_id(msg);
        let msgs = msg.as_any().downcast_ref::<MessageBatch>();

        if let (Some(msgs), true) = (msgs, response_header.batch_uniq_id().is_none()) {
            let mut sb = String::new();
            for msg in &msgs.messages {
                if let Some(uniq_id) = MessageClientIDSetter::get_uniq_id(msg) {
                    if !sb.is_empty() {
                        sb.push(',');
                    }
                    sb.push_str(uniq_id.as_str());
                } else {
                    warn!(
                        "skip empty uniq id while building batch send result for topic={}",
                        msg.topic()
                    );
                }
            }
            if !sb.is_empty() {
                uniq_msg_id = Some(CheetahString::from_string(sb));
            }
        }

        let region_id = response
            .ext_fields()
            .and_then(|fields| fields.get(MessageConst::PROPERTY_MSG_REGION))
            .map_or(mix_all::DEFAULT_TRACE_REGION_ID.to_string(), |s| s.to_string());
        let trace_on = trace_on_from_ext_fields(response.ext_fields());
        let queue_offset = java_long_to_u64_field("sendMessage", "queueOffset", response_header.queue_offset())?;
        let send_result = SendResult {
            send_status,
            msg_id: uniq_msg_id,
            offset_msg_id: Some(response_header.msg_id().to_string()),
            message_queue: Some(message_queue),
            queue_offset,
            transaction_id: response_header.transaction_id().map(|s| s.to_string()),
            recall_handle: response_header.recall_handle().map(|s| s.to_string()),
            region_id: Some(region_id),
            trace_on,
            ..Default::default()
        };

        Ok(send_result)
    }

    pub(super) async fn prepare_retry<T: MessageTrait>(
        &self,
        broker_name: &CheetahString,
        msg: &T,
        _request: &mut RemotingCommand,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<&Arc<MQClientInstance>>,
        producer: &DefaultMQProducerImpl,
    ) -> Option<(CheetahString, CheetahString)> {
        let mut retry_broker_name = broker_name.clone();

        if let Some(topic_publish_info) = topic_publish_info {
            let mq_chosen = producer.select_one_message_queue(topic_publish_info, Some(&retry_broker_name), false);
            let Some(mq_chosen) = mq_chosen.as_ref() else {
                warn!(
                    "prepare async retry failed: no message queue selected for topic={}",
                    msg.topic()
                );
                return None;
            };
            if let Some(instance) = instance {
                retry_broker_name = instance.get_broker_name_from_message_queue(mq_chosen).await;
            }
        }

        if let Some(instance) = instance {
            if let Some(addr) = instance.find_broker_address_in_publish(retry_broker_name.as_ref()) {
                return Some((addr, retry_broker_name));
            }
        }

        None
    }

    pub async fn send_heartbeat(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<(i32, Option<RemotingCommand>)> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok((response.version(), Some(response)));
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn send_heartbeat_async(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.send_heartbeat(addr, heartbeat_data, timeout_millis)
            .await
            .map(|(version, _)| version)
    }

    pub async fn send_heartbeat_oneway(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        self.remoting_client
            .invoke_request_oneway(addr, request, timeout_millis)
            .await;
        Ok(())
    }

    pub async fn register_client(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        Ok(ResponseCode::from(response.code()) == ResponseCode::Success)
    }

    pub async fn send_heartbeat_v2(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HeartbeatV2Result> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(HeartbeatV2Result::from_response(&response));
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn check_client_in_broker(
        &self,
        broker_addr: &str,
        consumer_group: &str,
        client_id: &str,
        subscription_data: &SubscriptionData,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request = RemotingCommand::create_remoting_command(RequestCode::CheckClientConfig);
        let body = CheckClientRequestBody::new(
            client_id.to_string(),
            consumer_group.to_string(),
            subscription_data.clone(),
        );
        request.set_body_mut_ref(body.encode()?);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub async fn recall_message(
        &self,
        addr: &str,
        request_header: RecallMessageRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<String> {
        let request = RemotingCommand::create_request_command(RequestCode::RecallMessage, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(CheetahString::from_slice(addr).as_ref()), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                // Decode RecallMessageResponseHeader from response
                match response.decode_command_custom_header::<RecallMessageResponseHeader>() {
                    Ok(header) => Ok(header.msg_id().to_string()),
                    Err(_) => {
                        // Fallback to remark if header decode fails
                        Ok(response.remark().map_or(String::new(), |s| s.to_string()))
                    }
                }
            }
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or(String::new(), |s| s.to_string()),
                addr.to_string()
            )),
        }
    }

    pub async fn recall_message_async<F>(
        &self,
        addr: &CheetahString,
        request_header: RecallMessageRequestHeader,
        timeout_millis: u64,
        invoke_callback: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: FnOnce(rocketmq_error::RocketMQResult<RemotingCommand>) + Send,
    {
        let request = RemotingCommand::create_request_command(RequestCode::RecallMessage, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        invoke_callback(response);
        Ok(())
    }
}
