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

use super::*;

impl DefaultMQProducerImpl {
    pub(super) async fn handle_send_error(&self, mq: &MessageQueue, input: &RetryInput, elapsed: u64, invoke_id: u64) {
        let broker_name = mq.broker_name();
        let detector_enabled = self.mq_fault_strategy.read().is_start_detector_enable();
        let Some(fault_decision) = producer_send_fault_decision(input, detector_enabled) else {
            return;
        };

        self.update_fault_item(broker_name, elapsed, fault_decision.isolation, fault_decision.reachable)
            .await;
        if fault_decision.log_resend_immediately {
            warn!(
                "sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {:?}",
                invoke_id, elapsed, mq
            );
        }
    }

    pub(super) fn retry_context<'a>(
        runtime: &'a ProducerRuntimeSnapshot,
        ctx: &SendContext,
        attempt: u32,
        max_attempts: u32,
    ) -> RetryContext<'a> {
        RetryContext {
            operation: RetryOperation::ProducerSend,
            idempotency: RetryIdempotency::NonIdempotent,
            attempt,
            max_attempts,
            remaining: ctx.deadline.remaining(),
            producer_retry_response_codes: Some(runtime.producer_config.retry_response_codes()),
            retry_not_store_ok: runtime.producer_config.retry_another_broker_when_not_store_ok(),
        }
    }

    fn route_refresh_retry_context(ctx: &SendContext, attempt: u32, max_attempts: u32) -> RetryContext<'static> {
        RetryContext {
            operation: RetryOperation::ProducerRouteRefresh,
            idempotency: RetryIdempotency::Idempotent,
            attempt,
            max_attempts,
            remaining: ctx.deadline.remaining(),
            producer_retry_response_codes: None,
            retry_not_store_ok: false,
        }
    }

    pub(super) async fn select_or_refresh_route_for_attempt(
        &self,
        queued_retry_queue: &mut Option<MessageQueue>,
        topic: &CheetahString,
        current_publish_info: &mut TopicPublishInfo,
        last_broker_name: Option<&CheetahString>,
        reset_index: bool,
        producer_config: &ProducerConfig,
        deadline: RequestDeadline,
    ) -> Result<MessageQueue, RetryInput> {
        if let Some(mq) = queued_retry_queue.take() {
            return Ok(mq);
        }

        if let Some(mq) = self.select_one_message_queue(current_publish_info, last_broker_name, reset_index) {
            return Ok(mq);
        }

        let instance = self.client_instance().map_err(RetryInput::BusinessError)?;
        let Some(publish_info) = instance
            .prepare_topic_publish_info_once(topic, producer_config, deadline)
            .await?
        else {
            return Err(RetryInput::RouteUnavailable);
        };
        *current_publish_info = publish_info;
        self.select_one_message_queue(current_publish_info, last_broker_name, reset_index)
            .ok_or(RetryInput::RouteUnavailable)
    }

    pub(super) async fn apply_route_refresh_failure_policy(
        &self,
        input: RetryInput,
        ctx: &SendContext,
        attempt: u32,
        max_attempts: u32,
        topic: &CheetahString,
        retry_state: &mut RetryState,
    ) -> rocketmq_error::RocketMQResult<()> {
        let action = RetryPolicy::decide(Self::route_refresh_retry_context(ctx, attempt, max_attempts), &input);
        retry_state.set_error(producer_retry_input_error(input, None));
        match action {
            RetryAction::RetryNow => Ok(()),
            RetryAction::RetryAfter(delay) => {
                if ctx.deadline.timeout(tokio::time::sleep(delay)).await.is_ok() {
                    Ok(())
                } else {
                    Err(retry_state.take_failure_error(topic, ctx.elapsed() as u128))
                }
            }
            RetryAction::Stop | RetryAction::RefreshRoute | RetryAction::RefreshLeader | RetryAction::SwitchBroker => {
                Err(retry_state.take_failure_error(topic, ctx.elapsed() as u128))
            }
        }
    }

    pub(super) async fn execute_producer_retry_action(
        &self,
        action: RetryAction,
        ctx: &SendContext,
        topic: &CheetahString,
        current_publish_info: &mut TopicPublishInfo,
        current_queue: &MessageQueue,
        queued_retry_queue: &mut Option<MessageQueue>,
    ) -> Result<bool, RetryInput> {
        match action {
            RetryAction::Stop => return Ok(false),
            RetryAction::RetryNow => *queued_retry_queue = Some(current_queue.clone()),
            RetryAction::RetryAfter(delay) => {
                if ctx.deadline.timeout(tokio::time::sleep(delay)).await.is_err() {
                    return Ok(false);
                }
                *queued_retry_queue = Some(current_queue.clone());
            }
            RetryAction::RefreshRoute => {
                let instance = self.client_instance().map_err(RetryInput::BusinessError)?;
                let Some(publish_info) = instance.refresh_topic_route_info_once(topic, ctx.deadline).await? else {
                    return Err(RetryInput::RouteUnavailable);
                };
                *current_publish_info = publish_info;
            }
            RetryAction::RefreshLeader => return Ok(false),
            RetryAction::SwitchBroker => {}
        }
        Ok(true)
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
