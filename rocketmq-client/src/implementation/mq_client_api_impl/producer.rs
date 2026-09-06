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

use super::callback_executor::ClientCallbackExecutor;
use super::request_builder::heartbeat_request;
use super::*;

impl MQClientAPIImpl {
    #[allow(
        clippy::too_many_arguments,
        reason = "existing send wire adapter signature is tracked by the lint debt registry"
    )]
    pub(crate) async fn send_message<T>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        deadline: RequestDeadline,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<Arc<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> Result<Option<SendResult>, RetryInput>
    where
        T: MessageTrait,
    {
        let msg_type = msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TYPE));
        let is_reply = msg_type
            .as_ref()
            .is_some_and(|msg_type| msg_type.as_str() == mix_all::REPLY_MESSAGE_FLAG);
        let mut request = if is_reply {
            if *SEND_SMART_MSG {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
                self.create_request_command(RequestCode::SendReplyMessageV2, request_header_v2)
            } else {
                self.create_request_command(RequestCode::SendReplyMessage, request_header)
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
                self.create_request_command(request_code, request_header_v2)
            } else {
                self.create_request_command(RequestCode::SendMessage, request_header)
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
            return Err(mq_client_err!(-1, "Message body is None").into());
        }
        match communication_mode {
            CommunicationMode::Sync => {
                if deadline.is_expired() {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: deadline.budget_millis(),
                    }
                    .into());
                }
                let result = self
                    .send_message_sync(addr, broker_name, msg, deadline, request)
                    .await?;
                Ok(Some(result))
            }
            CommunicationMode::Async => {
                if deadline.is_expired() {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: deadline.budget_millis(),
                    }
                    .into());
                }
                self.send_message_async(
                    addr,
                    broker_name,
                    msg,
                    deadline,
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
                    .invoke_request_oneway_with_deadline(addr, request, deadline)
                    .await
                    .map_err(RetryInput::BusinessError)?;
                Ok(None)
            }
        }
    }

    /// Sends a one-way request while transferring an existing resource reservation.
    ///
    /// The returned future completes only after the transport writer accepts the frame or
    /// reports an error. The permit remains charged for the complete queued-write lifetime and
    /// is released by RAII on success, timeout, cancellation, or connection failure.
    ///
    /// # Errors
    ///
    /// Returns an error when the deadline expires, the connection cannot be established, the
    /// reservation cannot be transferred into the transport budget, or the writer fails.
    pub async fn send_oneway_with_permit(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: rocketmq_transport::api::RequestDeadline,
        permit: rocketmq_runtime::ResourcePermit,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.remoting_client
            .invoke_oneway_with_permit(addr, request, deadline, permit)
            .await
    }

    pub(crate) async fn send_message_simple<T>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        deadline: RequestDeadline,
        communication_mode: CommunicationMode,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> Result<Option<SendResult>, RetryInput>
    where
        T: MessageTrait,
    {
        self.send_message(
            addr,
            broker_name,
            msg,
            request_header,
            deadline,
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
        deadline: RequestDeadline,
        request: RemotingCommand,
    ) -> Result<SendResult, RetryInput>
    where
        T: MessageTrait,
    {
        let outcome = self
            .remoting_client
            .invoke_request_with_deadline(Some(addr), request, deadline)
            .await
            .map_err(RetryInput::Transport)?;
        let response = match outcome {
            OutboundRequestOutcome::Response(response) => response,
            OutboundRequestOutcome::Rejected(rejection) => return Err(RetryInput::Rejected(rejection)),
            OutboundRequestOutcome::Contract(contract) => return Err(RetryInput::Contract(contract)),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::FlushDiskTimeout
            | ResponseCode::FlushSlaveTimeout
            | ResponseCode::SlaveNotAvailable
            | ResponseCode::Success => self
                .process_send_response(broker_name, msg, &response, addr)
                .map_err(RetryInput::BusinessError),
            _ => Err(RetryInput::Response {
                code: response.code(),
                retry_after: None,
                terminal_error: client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |remark| remark.to_string()),
                    addr.to_string()
                ),
            }),
        }
    }

    pub(super) async fn send_message_async<T: MessageTrait>(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        deadline: RequestDeadline,
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
        let callback_executor = self.callback_executor.clone();
        let current_addr = addr.clone();
        let current_broker_name = broker_name.clone();
        let current_request = request;
        let topic_publish_info_cloned = topic_publish_info.cloned();
        let instance_cloned = instance.clone();
        let mq_fault_strategy = producer.fault_strategy_snapshot();
        let producer_config = producer.producer_config_snapshot();
        let retry_response_codes = producer_config.retry_response_codes().clone();
        let retry_not_store_ok = producer_config.retry_another_broker_when_not_store_ok();
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
            callback_executor,
            mq_fault_strategy,
            current_addr,
            current_broker_name,
            msg_topic,
            msg_uniq_id,
            is_batch_message,
            deadline,
            current_request,
            send_callback,
            topic_publish_info_cloned,
            instance_cloned,
            retry_times_when_send_failed,
            retry_response_codes,
            retry_not_store_ok,
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
        remoting_client: Arc<RemotingClient<ClientRemotingProcessor>>,
        client_config: Arc<ClientConfig>,
        callback_executor: ClientCallbackExecutor,
        mq_fault_strategy: MQFaultStrategy,
        mut current_addr: CheetahString,
        mut current_broker_name: CheetahString,
        msg_topic: CheetahString,
        msg_uniq_id: Option<CheetahString>,
        _is_batch_message: bool,
        deadline: RequestDeadline,
        current_request: RemotingCommand,
        send_callback: Option<ArcSendCallback>,
        mut topic_publish_info: Option<TopicPublishInfo>,
        instance: Option<Arc<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        retry_response_codes: HashSet<i32>,
        retry_not_store_ok: bool,
        context_data: Option<AsyncSendHookContext>,
    ) {
        let max_attempts = retry_times_when_send_failed.saturating_add(1);
        let mut retry_count = 0_u32;
        let mut retry_request = AsyncRetryRequest::new(current_request);
        let mut last_error = None;
        let mut refresh_route_before_send = false;

        loop {
            if deadline.is_expired() {
                let err = Arc::new(last_error.take().unwrap_or(rocketmq_error::RocketMQError::Timeout {
                    operation: "sendMessageAsync",
                    timeout_ms: deadline.budget_millis(),
                }));
                Self::execute_async_send_hook_after(&context_data, None, Some(Arc::clone(&err)));
                Self::notify_send_callback_exception(&callback_executor, &send_callback, err.as_ref()).await;
                return;
            }

            let attempt = retry_count.saturating_add(1);
            if refresh_route_before_send {
                match Self::refresh_async_retry_route(
                    &mq_fault_strategy,
                    &mut topic_publish_info,
                    instance.as_ref(),
                    &mut current_broker_name,
                    &mut current_addr,
                    deadline,
                    &msg_topic,
                )
                .await
                {
                    Ok(()) => refresh_route_before_send = false,
                    Err(input) => {
                        if Self::handle_async_route_refresh_failure(
                            input,
                            &mut retry_request,
                            &mut retry_count,
                            max_attempts,
                            attempt,
                            deadline,
                            &mut last_error,
                            &mut refresh_route_before_send,
                            &current_addr,
                            &callback_executor,
                            &send_callback,
                            &context_data,
                        )
                        .await
                        {
                            continue;
                        }
                        return;
                    }
                }
            }
            let begin_attempt_time = Instant::now();
            let keep_request_for_retry = attempt < max_attempts;
            let attempt_request = retry_request.next_attempt(keep_request_for_retry);
            let result = remoting_client
                .invoke_request_with_deadline(Some(&current_addr), attempt_request, deadline)
                .await;
            let cost = (Instant::now() - begin_attempt_time).as_millis() as u64;

            match result {
                Ok(OutboundRequestOutcome::Response(response)) => {
                    // Determine send status
                    let response_code = ResponseCode::from(response.code());
                    let send_status = match response_code {
                        ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
                        ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
                        ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
                        ResponseCode::Success => SendStatus::SendOk,
                        _ => {
                            let input = RetryInput::Response {
                                code: response.code(),
                                retry_after: None,
                                terminal_error: client_broker_err!(
                                    response.code(),
                                    response.remark().map_or("".to_string(), |s| s.to_string()),
                                    current_addr.to_string()
                                ),
                            };
                            if Self::handle_async_retry_input(
                                &mq_fault_strategy,
                                &mut topic_publish_info,
                                instance.as_ref(),
                                &mut current_broker_name,
                                &mut current_addr,
                                &mut retry_request,
                                &mut retry_count,
                                &mut last_error,
                                &mut refresh_route_before_send,
                                max_attempts,
                                attempt,
                                deadline,
                                &retry_response_codes,
                                input,
                                cost,
                                &msg_topic,
                                &callback_executor,
                                &send_callback,
                                &context_data,
                            )
                            .await
                            {
                                continue;
                            }
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
                                    let err_obj = Arc::new(err_obj);
                                    Self::execute_async_send_hook_after(
                                        &context_data,
                                        None,
                                        Some(Arc::clone(&err_obj)),
                                    );
                                    Self::notify_send_callback_exception(
                                        &callback_executor,
                                        &send_callback,
                                        err_obj.as_ref(),
                                    )
                                    .await;
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

                            let status_input = RetryInput::SendStatus(send_result.send_status);
                            let status_action = RetryPolicy::decide(
                                RetryContext {
                                    operation: RetryOperation::ProducerSend,
                                    idempotency: RetryIdempotency::NonIdempotent,
                                    attempt,
                                    max_attempts,
                                    remaining: deadline.remaining(),
                                    producer_retry_response_codes: Some(&retry_response_codes),
                                    retry_not_store_ok,
                                },
                                &status_input,
                            );
                            if status_action != RetryAction::Stop {
                                match Self::execute_async_retry_action(
                                    status_action,
                                    &mq_fault_strategy,
                                    &mut topic_publish_info,
                                    instance.as_ref(),
                                    &mut current_broker_name,
                                    &mut current_addr,
                                    deadline,
                                    &msg_topic,
                                )
                                .await
                                {
                                    Ok(true) => {
                                        last_error = Some(
                                            crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                                                status_input,
                                                Some(&current_addr),
                                            ),
                                        );
                                        retry_count = retry_count.saturating_add(1);
                                        retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
                                        continue;
                                    }
                                    Ok(false) => {}
                                    Err(input) => {
                                        if Self::handle_async_route_refresh_failure(
                                            input,
                                            &mut retry_request,
                                            &mut retry_count,
                                            max_attempts,
                                            attempt,
                                            deadline,
                                            &mut last_error,
                                            &mut refresh_route_before_send,
                                            &current_addr,
                                            &callback_executor,
                                            &send_callback,
                                            &context_data,
                                        )
                                        .await
                                        {
                                            continue;
                                        }
                                        return;
                                    }
                                }
                            }

                            // Success: update fault item and invoke callback
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, false, true)
                                .await;
                            Self::execute_async_send_hook_after(&context_data, Some(&send_result), None);
                            Self::notify_send_callback_success(&callback_executor, &send_callback, &send_result).await;
                            return;
                        }
                        Err(source) => {
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, true, true)
                                .await;
                            let context = rocketmq_error::ErrorContext::new()
                                .with_text(
                                    rocketmq_error::fields::OPERATION_DIAGNOSTIC,
                                    "decode SendMessageResponseHeader",
                                )
                                .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT);
                            let err_obj = Arc::new(RocketMQError::Shared(Arc::new(
                                rocketmq_error::Error::caused_by(&rocketmq_error::CORE_SERIALIZATION_FAILED, source)
                                    .with_context(context),
                            )));
                            Self::execute_async_send_hook_after(&context_data, None, Some(Arc::clone(&err_obj)));
                            Self::notify_send_callback_exception(&callback_executor, &send_callback, err_obj.as_ref())
                                .await;
                            return;
                        }
                    }
                }
                Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                    let input = RetryInput::Rejected(rejection);
                    if Self::handle_async_retry_input(
                        &mq_fault_strategy,
                        &mut topic_publish_info,
                        instance.as_ref(),
                        &mut current_broker_name,
                        &mut current_addr,
                        &mut retry_request,
                        &mut retry_count,
                        &mut last_error,
                        &mut refresh_route_before_send,
                        max_attempts,
                        attempt,
                        deadline,
                        &retry_response_codes,
                        input,
                        cost,
                        &msg_topic,
                        &callback_executor,
                        &send_callback,
                        &context_data,
                    )
                    .await
                    {
                        continue;
                    }
                    return;
                }
                Ok(OutboundRequestOutcome::Contract(contract)) => {
                    let input = RetryInput::Contract(contract);
                    if Self::handle_async_retry_input(
                        &mq_fault_strategy,
                        &mut topic_publish_info,
                        instance.as_ref(),
                        &mut current_broker_name,
                        &mut current_addr,
                        &mut retry_request,
                        &mut retry_count,
                        &mut last_error,
                        &mut refresh_route_before_send,
                        max_attempts,
                        attempt,
                        deadline,
                        &retry_response_codes,
                        input,
                        cost,
                        &msg_topic,
                        &callback_executor,
                        &send_callback,
                        &context_data,
                    )
                    .await
                    {
                        continue;
                    }
                    return;
                }
                Err(error) => {
                    let input = RetryInput::Transport(error);
                    error!("send message async operational failure");
                    if Self::handle_async_retry_input(
                        &mq_fault_strategy,
                        &mut topic_publish_info,
                        instance.as_ref(),
                        &mut current_broker_name,
                        &mut current_addr,
                        &mut retry_request,
                        &mut retry_count,
                        &mut last_error,
                        &mut refresh_route_before_send,
                        max_attempts,
                        attempt,
                        deadline,
                        &retry_response_codes,
                        input,
                        cost,
                        &msg_topic,
                        &callback_executor,
                        &send_callback,
                        &context_data,
                    )
                    .await
                    {
                        continue;
                    }
                    return;
                }
            }
        }
    }

    pub async fn send_heartbeat(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<(i32, Option<RemotingCommand>)> {
        let request = heartbeat_request(&self.command_factory, heartbeat_data, self.client_config.language)?;
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Rejected(rejection),
                        Some(addr),
                    ),
                );
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Contract(contract),
                        Some(addr),
                    ),
                );
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok((response.version(), Some(response)));
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn send_heartbeat_oneway(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = heartbeat_request(&self.command_factory, heartbeat_data, self.client_config.language)?;
        self.remoting_client
            .invoke_request_oneway(addr, request, timeout_millis)
            .await
    }

    pub async fn register_client(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let request = heartbeat_request(&self.command_factory, heartbeat_data, self.client_config.language)?;
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Rejected(rejection),
                        Some(addr),
                    ),
                );
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Contract(contract),
                        Some(addr),
                    ),
                );
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        Ok(ResponseCode::from(response.code()) == ResponseCode::Success)
    }

    pub async fn send_heartbeat_v2(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HeartbeatV2Result> {
        let request = heartbeat_request(&self.command_factory, heartbeat_data, self.client_config.language)?;
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Rejected(rejection),
                        Some(addr),
                    ),
                );
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Contract(contract),
                        Some(addr),
                    ),
                );
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let mut request = self.create_remoting_command(RequestCode::CheckClientConfig);
        let body = CheckClientRequestBody::new(
            client_id.to_string(),
            consumer_group.to_string(),
            subscription_data.clone(),
        );
        request.set_body_mut_ref(body.encode()?);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr);
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Rejected(rejection),
                        Some(&broker_addr),
                    ),
                );
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Contract(contract),
                        Some(&broker_addr),
                    ),
                );
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::RecallMessage, request_header);

        let remote_addr = CheetahString::from_slice(addr);
        let outcome = self
            .remoting_client
            .invoke_request(Some(&remote_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Rejected(rejection),
                        Some(&remote_addr),
                    ),
                );
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(
                    crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                        RetryInput::Contract(contract),
                        Some(&remote_addr),
                    ),
                );
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };

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
        let request = self.create_request_command(RequestCode::RecallMessage, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => Ok(response),
            Ok(OutboundRequestOutcome::Rejected(rejection)) => Err(
                crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                    RetryInput::Rejected(rejection),
                    Some(addr),
                ),
            ),
            Ok(OutboundRequestOutcome::Contract(contract)) => Err(
                crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
                    RetryInput::Contract(contract),
                    Some(addr),
                ),
            ),
            Err(error) => Err(RocketMQError::Shared(error.into_shared_error())),
        };
        invoke_callback(response);
        Ok(())
    }
}
