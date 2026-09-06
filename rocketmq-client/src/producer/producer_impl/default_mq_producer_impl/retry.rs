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

use super::*;
#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    /// Core: send with retry logic
    pub(super) async fn send_with_retry<T>(
        &self,
        msg: &mut T,
        topic: &CheetahString,
        topic_publish_info: &TopicPublishInfo,
        send_callback: Option<ArcSendCallback>,
        ctx: SendContext,
        runtime: &ProducerRuntimeSnapshot,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let max_attempts = Self::get_retry_times(runtime, ctx.communication_mode);
        let mut retry_state = RetryState::new(max_attempts);
        let mut last_broker_name: Option<CheetahString> = None;
        let mut current_publish_info = topic_publish_info.clone();
        let mut queued_retry_queue: Option<MessageQueue> = None;

        for attempt_index in 0..max_attempts {
            if ctx.deadline.is_expired() {
                return Err(retry_state
                    .take_last_error()
                    .unwrap_or_else(|| rocketmq_error::RocketMQError::Timeout {
                        operation: "send_with_retry",
                        timeout_ms: ctx.deadline.budget_millis(),
                    }));
            }

            let attempt = attempt_index.saturating_add(1);
            let reset_index = attempt_index > 0;

            // Select message queue
            let mq = match self
                .select_or_refresh_route_for_attempt(
                    &mut queued_retry_queue,
                    topic,
                    &mut current_publish_info,
                    last_broker_name.as_ref(),
                    reset_index,
                    runtime.producer_config.as_ref(),
                    ctx.deadline,
                )
                .await
            {
                Ok(mq) => mq,
                Err(input) => {
                    self.apply_route_refresh_failure_policy(
                        input,
                        &ctx,
                        attempt,
                        max_attempts,
                        topic,
                        &mut retry_state,
                    )
                    .await?;
                    continue;
                }
            };

            retry_state.record_broker(attempt_index as usize, mq.broker_name());
            last_broker_name = Some(mq.broker_name().clone());

            // Prepare message for retry
            if attempt_index > 0 {
                Self::prepare_message_for_retry(runtime, msg, topic);
            }

            if ctx.deadline.is_expired() {
                return Err(retry_state
                    .take_last_error()
                    .unwrap_or_else(|| rocketmq_error::RocketMQError::Timeout {
                        operation: "send_with_retry",
                        timeout_ms: ctx.deadline.budget_millis(),
                    }));
            }

            // Send to broker
            let attempt_deadline = Self::send_deadline_for_attempt(runtime, ctx.deadline, attempt, max_attempts);
            let send_start = Instant::now();
            let result = self
                .send_kernel_impl_with_runtime(
                    msg,
                    &mq,
                    ctx.communication_mode,
                    send_callback.clone(),
                    Some(&current_publish_info),
                    attempt_deadline,
                    runtime,
                )
                .await;

            let elapsed = send_start.elapsed().as_millis() as u64;

            match result {
                Ok(result) => {
                    // Update fault item - success
                    self.update_fault_item(mq.broker_name(), elapsed, false, true).await;

                    let Some(send_result) = result else {
                        return Ok(None);
                    };
                    let input = RetryInput::SendStatus(send_result.send_status);
                    let action = RetryPolicy::decide(Self::retry_context(runtime, &ctx, attempt, max_attempts), &input);
                    if action == RetryAction::Stop {
                        return Ok(Some(send_result));
                    }
                    retry_state.record_send_result(send_result);
                    retry_state.set_error(producer_retry_input_error(input, None));
                    let completed = Self::execute_producer_retry_action(
                        self,
                        action,
                        &ctx,
                        topic,
                        &mut current_publish_info,
                        &mq,
                        &mut queued_retry_queue,
                    )
                    .await;
                    match completed {
                        Ok(true) => {}
                        Ok(false) => return Ok(retry_state.take_last_send_result()),
                        Err(input) => {
                            self.apply_route_refresh_failure_policy(
                                input,
                                &ctx,
                                attempt,
                                max_attempts,
                                topic,
                                &mut retry_state,
                            )
                            .await?;
                        }
                    }
                }
                Err(input) => {
                    // Handle send error
                    self.handle_send_error(&mq, &input, elapsed, ctx.invoke_id).await;

                    let action = RetryPolicy::decide(Self::retry_context(runtime, &ctx, attempt, max_attempts), &input);
                    let error = producer_retry_input_error(input, None);
                    if action == RetryAction::Stop {
                        return Err(error);
                    }
                    retry_state.set_error(error);
                    let completed = Self::execute_producer_retry_action(
                        self,
                        action,
                        &ctx,
                        topic,
                        &mut current_publish_info,
                        &mq,
                        &mut queued_retry_queue,
                    )
                    .await;
                    match completed {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(retry_state.take_failure_error(topic, ctx.elapsed() as u128));
                        }
                        Err(input) => {
                            self.apply_route_refresh_failure_policy(
                                input,
                                &ctx,
                                attempt,
                                max_attempts,
                                topic,
                                &mut retry_state,
                            )
                            .await?;
                        }
                    }
                }
            }
        }

        // All retries exhausted
        if let Some(send_result) = retry_state.take_last_send_result() {
            return Ok(Some(send_result));
        }

        Err(retry_state.take_failure_error(topic, ctx.elapsed() as u128))
    }

    /// Get retry times based on communication mode
    #[inline]
    pub(super) fn get_retry_times(runtime: &ProducerRuntimeSnapshot, mode: CommunicationMode) -> u32 {
        match mode {
            CommunicationMode::Sync => runtime.producer_config.retry_times_when_send_failed().saturating_add(1),
            CommunicationMode::Async | CommunicationMode::Oneway => 1,
        }
    }

    #[inline]
    pub(super) fn send_deadline_for_attempt(
        runtime: &ProducerRuntimeSnapshot,
        deadline: RequestDeadline,
        attempt: u32,
        retry_times: u32,
    ) -> RequestDeadline {
        let can_retry_again = attempt < retry_times;
        if can_retry_again {
            if let Some(max_timeout_per_request) = runtime.producer_config.send_msg_max_timeout_per_request() {
                return deadline.capped(Duration::from_millis(max_timeout_per_request as u64));
            }
        }
        deadline
    }

    /// Prepare message for retry (reset topic with namespace)
    pub(super) fn prepare_message_for_retry<T: MessageTrait>(
        runtime: &ProducerRuntimeSnapshot,
        msg: &mut T,
        topic: &CheetahString,
    ) {
        let namespace = runtime.client_config.resolved_namespace().unwrap_or_default();
        msg.set_topic(NamespaceUtil::wrap_namespace(namespace, topic));
    }
}
