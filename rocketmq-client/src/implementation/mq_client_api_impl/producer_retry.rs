// Copyright 2026 The RocketMQ Rust Authors
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
use super::*;

impl MQClientAPIImpl {
    pub(super) fn select_async_retry_queue(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: Option<&TopicPublishInfo>,
        broker_name: &CheetahString,
    ) -> Option<MessageQueue> {
        topic_publish_info.and_then(|topic_publish_info| {
            mq_fault_strategy.select_one_message_queue(topic_publish_info, Some(broker_name), false)
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the retry executor retains the existing asynchronous callback context"
    )]
    pub(super) async fn handle_async_retry_input(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: &mut Option<TopicPublishInfo>,
        instance: Option<&Arc<MQClientInstance>>,
        current_broker_name: &mut CheetahString,
        current_addr: &mut CheetahString,
        retry_request: &mut AsyncRetryRequest,
        retry_count: &mut u32,
        last_error: &mut Option<RocketMQError>,
        refresh_route_before_send: &mut bool,
        max_attempts: u32,
        attempt: u32,
        deadline: RequestDeadline,
        retry_response_codes: &HashSet<i32>,
        input: RetryInput,
        cost: u64,
        msg_topic: &CheetahString,
        callback_executor: &ClientCallbackExecutor,
        send_callback: &Option<ArcSendCallback>,
        context_data: &Option<AsyncSendHookContext>,
    ) -> bool {
        let action = RetryPolicy::decide(
            RetryContext {
                operation: RetryOperation::ProducerSend,
                idempotency: RetryIdempotency::NonIdempotent,
                attempt,
                max_attempts,
                remaining: deadline.remaining(),
                producer_retry_response_codes: Some(retry_response_codes),
                retry_not_store_ok: false,
            },
            &input,
        );

        if let Some(fault) = crate::common::retry_policy::producer_send_fault_decision(
            &input,
            mq_fault_strategy.is_start_detector_enable(),
        ) {
            mq_fault_strategy
                .update_fault_item(current_broker_name.clone(), cost, fault.isolation, fault.reachable)
                .await;
        }

        let terminal_error = crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
            input,
            Some(current_addr),
        );
        if action != RetryAction::Stop {
            match Self::execute_async_retry_action(
                action,
                mq_fault_strategy,
                topic_publish_info,
                instance,
                current_broker_name,
                current_addr,
                deadline,
                msg_topic,
            )
            .await
            {
                Ok(true) => {
                    *last_error = Some(terminal_error);
                    *retry_count = retry_count.saturating_add(1);
                    retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
                    return true;
                }
                Ok(false) => {}
                Err(input) => {
                    return Self::handle_async_route_refresh_failure(
                        input,
                        retry_request,
                        retry_count,
                        max_attempts,
                        attempt,
                        deadline,
                        last_error,
                        refresh_route_before_send,
                        current_addr,
                        callback_executor,
                        send_callback,
                        context_data,
                    )
                    .await;
                }
            }
        }

        Self::finish_async_retry_failure(terminal_error, callback_executor, send_callback, context_data).await;
        false
    }

    pub(super) async fn execute_async_retry_action(
        action: RetryAction,
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: &mut Option<TopicPublishInfo>,
        instance: Option<&Arc<MQClientInstance>>,
        current_broker_name: &mut CheetahString,
        current_addr: &mut CheetahString,
        deadline: RequestDeadline,
        msg_topic: &CheetahString,
    ) -> Result<bool, RetryInput> {
        match action {
            RetryAction::Stop => return Ok(false),
            RetryAction::RetryNow => {}
            RetryAction::RetryAfter(delay) => {
                if deadline.timeout(tokio::time::sleep(delay)).await.is_err() {
                    return Ok(false);
                }
            }
            RetryAction::RefreshRoute => {
                Self::refresh_async_retry_route(
                    mq_fault_strategy,
                    topic_publish_info,
                    instance,
                    current_broker_name,
                    current_addr,
                    deadline,
                    msg_topic,
                )
                .await?;
            }
            RetryAction::RefreshLeader => return Ok(false),
            RetryAction::SwitchBroker => {
                let Some((retry_addr, retry_broker_name)) = Self::select_async_retry_target(
                    mq_fault_strategy,
                    topic_publish_info.as_ref(),
                    instance,
                    current_broker_name,
                )
                .await
                else {
                    return Ok(false);
                };
                *current_addr = retry_addr;
                *current_broker_name = retry_broker_name;
            }
        }
        Ok(!deadline.is_expired())
    }

    pub(super) async fn refresh_async_retry_route(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: &mut Option<TopicPublishInfo>,
        instance: Option<&Arc<MQClientInstance>>,
        current_broker_name: &mut CheetahString,
        current_addr: &mut CheetahString,
        deadline: RequestDeadline,
        msg_topic: &CheetahString,
    ) -> Result<(), RetryInput> {
        let instance = instance.ok_or(RetryInput::BusinessError(RocketMQError::ClientNotStarted))?;
        let Some(refreshed) = instance.refresh_topic_route_info_once(msg_topic, deadline).await? else {
            return Err(RetryInput::RouteUnavailable);
        };
        *topic_publish_info = Some(refreshed);
        let mq = Self::select_async_retry_queue(mq_fault_strategy, topic_publish_info.as_ref(), current_broker_name)
            .ok_or(RetryInput::RouteUnavailable)?;
        let broker_name = instance.get_broker_name_from_message_queue(&mq).await;
        let broker_addr = instance
            .find_broker_address_in_publish(broker_name.as_ref())
            .ok_or(RetryInput::RouteUnavailable)?;
        *current_addr = broker_addr;
        *current_broker_name = broker_name;
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the retry executor retains the existing asynchronous callback context"
    )]
    pub(super) async fn handle_async_route_refresh_failure(
        input: RetryInput,
        retry_request: &mut AsyncRetryRequest,
        retry_count: &mut u32,
        max_attempts: u32,
        attempt: u32,
        deadline: RequestDeadline,
        last_error: &mut Option<RocketMQError>,
        refresh_route_before_send: &mut bool,
        current_addr: &CheetahString,
        callback_executor: &ClientCallbackExecutor,
        send_callback: &Option<ArcSendCallback>,
        context_data: &Option<AsyncSendHookContext>,
    ) -> bool {
        let action = RetryPolicy::decide(
            RetryContext {
                operation: RetryOperation::ProducerRouteRefresh,
                idempotency: RetryIdempotency::Idempotent,
                attempt,
                max_attempts,
                remaining: deadline.remaining(),
                producer_retry_response_codes: None,
                retry_not_store_ok: false,
            },
            &input,
        );
        let terminal_error = crate::producer::producer_impl::default_mq_producer_impl::producer_retry_input_error(
            input,
            Some(current_addr),
        );

        match action {
            RetryAction::RetryNow => {}
            RetryAction::RetryAfter(delay) => {
                if deadline.timeout(tokio::time::sleep(delay)).await.is_err() {
                    Self::finish_async_retry_failure(terminal_error, callback_executor, send_callback, context_data)
                        .await;
                    return false;
                }
            }
            RetryAction::Stop | RetryAction::RefreshRoute | RetryAction::RefreshLeader | RetryAction::SwitchBroker => {
                Self::finish_async_retry_failure(terminal_error, callback_executor, send_callback, context_data).await;
                return false;
            }
        }

        *last_error = Some(terminal_error);
        *refresh_route_before_send = true;
        *retry_count = retry_count.saturating_add(1);
        retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
        true
    }

    pub(super) async fn finish_async_retry_failure(
        error: RocketMQError,
        callback_executor: &ClientCallbackExecutor,
        send_callback: &Option<ArcSendCallback>,
        context_data: &Option<AsyncSendHookContext>,
    ) {
        let error = Arc::new(error);
        Self::execute_async_send_hook_after(context_data, None, Some(Arc::clone(&error)));
        Self::notify_send_callback_exception(callback_executor, send_callback, error.as_ref()).await;
    }

    pub(crate) async fn select_async_retry_target(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<&Arc<MQClientInstance>>,
        broker_name: &CheetahString,
    ) -> Option<(CheetahString, CheetahString)> {
        let mq_chosen = Self::select_async_retry_queue(mq_fault_strategy, topic_publish_info, broker_name)?;
        let instance = instance?;
        let retry_broker_name = instance.get_broker_name_from_message_queue(&mq_chosen).await;
        let retry_addr = instance.find_broker_address_in_publish(retry_broker_name.as_ref())?;
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

        if let Err(error) = spawn_client_task_with_context(service_context, thread_name, Box::pin(tracked_task)) {
            warn!("Failed to spawn {} background task: {}", thread_name, error);
        }
    }

    pub(super) async fn notify_send_callback_success(
        callback_executor: &ClientCallbackExecutor,
        send_callback: &Option<ArcSendCallback>,
        send_result: &SendResult,
    ) {
        let Some(callback) = send_callback.as_ref().cloned() else {
            return;
        };

        let _ = callback_executor
            .execute(async { callback.on_success(send_result) })
            .await;
    }

    pub(super) async fn notify_send_callback_exception(
        callback_executor: &ClientCallbackExecutor,
        send_callback: &Option<ArcSendCallback>,
        error: &RocketMQError,
    ) {
        let Some(callback) = send_callback.as_ref().cloned() else {
            return;
        };

        let _ = callback_executor.execute(async { callback.on_exception(error) }).await;
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
        Ok(SendResult {
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
        })
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
}
