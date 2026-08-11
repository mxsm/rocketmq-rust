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
        let retry_times = Self::get_retry_times(runtime, ctx.communication_mode);
        let mut retry_state = RetryState::new(retry_times);
        let mut last_broker_name: Option<CheetahString> = None;

        for attempt in 0..retry_times {
            let reset_index = attempt > 0;

            // Select message queue
            let mq = match self.select_one_message_queue(topic_publish_info, last_broker_name.as_ref(), reset_index) {
                Some(mq) => mq,
                None => break,
            };

            retry_state.record_broker(attempt as usize, mq.broker_name());
            last_broker_name = Some(mq.broker_name().clone());

            // Prepare message for retry
            if attempt > 0 {
                Self::prepare_message_for_retry(runtime, msg, topic);
            }

            // Check timeout
            ctx.check_timeout()?;

            // Send to broker
            let remaining_timeout = ctx.remaining_timeout();
            let request_timeout = Self::send_timeout_for_attempt(runtime, remaining_timeout, attempt, retry_times);
            let send_start = Instant::now();
            let result = self
                .send_kernel_impl_with_runtime(
                    msg,
                    &mq,
                    ctx.communication_mode,
                    send_callback.clone(),
                    Some(topic_publish_info),
                    request_timeout,
                    runtime,
                )
                .await;

            let elapsed = send_start.elapsed().as_millis() as u64;

            match result {
                Ok(result) => {
                    // Update fault item - success
                    self.update_fault_item(mq.broker_name(), elapsed, false, true).await;

                    // Check if need to retry based on send status
                    if Self::should_retry_on_result(runtime, &result, ctx.communication_mode) {
                        if let Some(result) = result {
                            retry_state.record_send_result(result);
                        }
                        retry_state.set_error(mq_client_err!("Send status not OK"));
                        continue;
                    }

                    return Ok(result);
                }
                Err(e) => {
                    let retry_decision = Self::retry_decision_on_error(runtime, &e);

                    // Handle send error
                    self.handle_send_error(&mq, &e, elapsed, ctx.invoke_id).await;

                    if !retry_decision.should_retry() {
                        return Err(e);
                    }

                    retry_state.set_error(e);
                }
            }
        }

        // All retries exhausted
        if let Some(send_result) = retry_state.take_last_send_result() {
            return Ok(Some(send_result));
        }

        Err(retry_state.build_failure_error(topic, ctx.elapsed() as u128))
    }

    /// Get retry times based on communication mode
    #[inline]
    pub(super) fn get_retry_times(runtime: &ProducerRuntimeSnapshot, mode: CommunicationMode) -> u32 {
        match mode {
            CommunicationMode::Sync => runtime.producer_config.retry_times_when_send_failed() + 1,
            CommunicationMode::Async | CommunicationMode::Oneway => 1,
        }
    }

    #[inline]
    pub(super) fn send_timeout_for_attempt(
        runtime: &ProducerRuntimeSnapshot,
        remaining_timeout: u64,
        attempt: u32,
        retry_times: u32,
    ) -> u64 {
        let can_retry_again = attempt < retry_times.saturating_sub(1);
        if can_retry_again {
            if let Some(max_timeout_per_request) = runtime.producer_config.send_msg_max_timeout_per_request() {
                return remaining_timeout.min(max_timeout_per_request as u64);
            }
        }
        remaining_timeout
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

    /// Handle send error - update fault item and log
    pub(super) async fn handle_send_error(
        &self,
        mq: &MessageQueue,
        error: &rocketmq_error::RocketMQError,
        elapsed: u64,
        invoke_id: u64,
    ) {
        let broker_name = mq.broker_name();

        let detector_enabled = self.mq_fault_strategy.read().is_start_detector_enable();
        let Some(fault_decision) = producer_send_fault_decision(error, detector_enabled) else {
            return;
        };

        self.update_fault_item(broker_name, elapsed, fault_decision.isolation, fault_decision.reachable)
            .await;

        if fault_decision.log_resend_immediately {
            warn!(
                "sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {:?}, {}",
                invoke_id, elapsed, mq, error
            );
        }
    }

    /// Build retry decision for producer send failures.
    #[inline]
    pub(super) fn retry_decision_on_error(
        runtime: &ProducerRuntimeSnapshot,
        error: &RocketMQError,
    ) -> ClientRetryDecision {
        producer_send_retry_decision(error, runtime.producer_config.retry_response_codes())
    }

    /// Check if should retry based on send result
    #[inline]
    pub(super) fn should_retry_on_result(
        runtime: &ProducerRuntimeSnapshot,
        result: &Option<SendResult>,
        mode: CommunicationMode,
    ) -> bool {
        if mode != CommunicationMode::Sync {
            return false;
        }

        result.as_ref().is_some_and(|r| {
            r.send_status != SendStatus::SendOk && runtime.producer_config.retry_another_broker_when_not_store_ok()
        })
    }

    #[inline]
    pub async fn update_fault_item(
        &self,
        broker_name: &CheetahString,
        current_latency: u64,
        isolation: bool,
        reachable: bool,
    ) {
        let strategy = self.mq_fault_strategy.read().clone();
        strategy
            .update_fault_item(broker_name.clone(), current_latency, isolation, reachable)
            .await;
    }
}
