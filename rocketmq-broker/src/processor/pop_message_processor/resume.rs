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

use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rand::RngExt;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_model::common::pop_retry_policy::PopRetryPolicy;
use rocketmq_model::topic::TopicMessageType;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_store::GetMessageResult;
use rocketmq_store::GetMessageStatus;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::ResponsePlan;

use super::PopMessageProcessor;
use crate::long_polling::pop_deferred::service::ResumePop;
use crate::processor::pop_message_processor::capability::PopPolicy;
use crate::processor::response_plan::pop::attach_pop_response_header;
use crate::processor::response_plan::pop::pop_heap_response_parts;
use crate::processor::response_plan::pop::pop_segmented_response_parts;
use crate::processor::response_plan::pop::take_pop_body_segments;
use crate::processor::response_plan::BrokerResponseParts;

/// Allocation-free caller identity until a store offset commit needs ownership.
#[derive(Clone, Copy)]
pub(super) enum PopCallerHost<'a> {
    Network(SocketAddr),
    Retained(&'a CheetahString),
}

impl PopCallerHost<'_> {
    pub(super) fn to_owned(self) -> CheetahString {
        match self {
            Self::Network(address) => CheetahString::from_string(address.to_string()),
            Self::Retained(address) => address.clone(),
        }
    }
}

/// Trusted, Channel-free inputs for one actual POP store reread.
pub(super) struct PopStoreReadRequest<'a> {
    request_header: &'a PopMessageRequestHeader,
    topic_config: &'a TopicConfig,
    policy: &'a PopPolicy,
    retry_policy: &'a PopRetryPolicy,
    priority_factor: i32,
    message_filter: Option<ArcMessageFilter>,
    caller_host: PopCallerHost<'a>,
    opaque: i32,
}

impl<'a> PopStoreReadRequest<'a> {
    pub(super) fn new(
        request_header: &'a PopMessageRequestHeader,
        topic_config: &'a TopicConfig,
        policy: &'a PopPolicy,
        retry_policy: &'a PopRetryPolicy,
        priority_factor: i32,
        message_filter: Option<ArcMessageFilter>,
        caller_host: PopCallerHost<'a>,
        opaque: i32,
    ) -> Self {
        Self {
            request_header,
            topic_config,
            policy,
            retry_policy,
            priority_factor,
            message_filter,
            caller_host,
            opaque,
        }
    }
}

/// Store reread result before the initial legacy path decides whether to suspend.
pub(super) enum PopStoreReadOutcome {
    Found(BrokerResponseParts),
    Empty { head: RemotingCommand, rest_num: i64 },
}

struct PopResumeRequest {
    header: PopMessageRequestHeader,
    caller_host: CheetahString,
    filter: Option<ArcMessageFilter>,
}

impl PopResumeRequest {
    fn from_resume(resume: ResumePop) -> Self {
        let request = Self {
            header: resume.request().header().clone(),
            caller_host: resume.request().caller_host().clone(),
            filter: resume.filter().cloned(),
        };
        drop(resume);
        request
    }

    #[cfg(test)]
    fn for_test(header: PopMessageRequestHeader, caller_host: CheetahString, filter: Option<ArcMessageFilter>) -> Self {
        Self {
            header,
            caller_host,
            filter,
        }
    }
}

impl<MS> PopMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    /// Replays the real POP store read without retaining a Channel or connection context.
    pub(crate) async fn resume_pop(
        &self,
        resume: ResumePop,
        reason: DeferredWakeReason,
    ) -> rocketmq_error::RocketMQResult<ResponsePlan> {
        self.resume_pop_request(PopResumeRequest::from_resume(resume), reason)
            .await
    }

    async fn resume_pop_request(
        &self,
        request: PopResumeRequest,
        reason: DeferredWakeReason,
    ) -> rocketmq_error::RocketMQResult<ResponsePlan> {
        match reason {
            DeferredWakeReason::MessageArrived | DeferredWakeReason::Timeout | DeferredWakeReason::ForcedRefresh => {}
        }
        let Some(topic_config) = self.context.topics.select_topic_config(&request.header.topic) else {
            let head = self.context.command_factory.create_response_command_with_code_remark(
                ResponseCode::TopicNotExist,
                "POP topic is no longer available",
            );
            return BrokerResponseParts::command(head)?.into_response_plan();
        };
        let Some(group_config) = self
            .context
            .subscriptions
            .find_subscription_group_config(&request.header.consumer_group)
        else {
            let head = self.context.command_factory.create_response_command_with_code_remark(
                ResponseCode::SubscriptionGroupNotExist,
                "POP subscription group is no longer available",
            );
            return BrokerResponseParts::command(head)?.into_response_plan();
        };
        let policy = self.context.policy.snapshot();
        let retry_policy = self.retry_policy_for_group(&request.header.consumer_group);
        match self
            .read_pop_store(PopStoreReadRequest::new(
                &request.header,
                &topic_config,
                &policy,
                &retry_policy,
                group_config.priority_factor(),
                request.filter,
                PopCallerHost::Retained(&request.caller_host),
                0,
            ))
            .await?
        {
            PopStoreReadOutcome::Found(parts) => parts.into_response_plan(),
            PopStoreReadOutcome::Empty { mut head, .. } => {
                head.set_code_ref(ResponseCode::PollingTimeout);
                BrokerResponseParts::command(head)?.into_response_plan()
            }
        }
    }

    pub(super) async fn read_pop_store(
        &self,
        request: PopStoreReadRequest<'_>,
    ) -> rocketmq_error::RocketMQResult<PopStoreReadOutcome> {
        let PopStoreReadRequest {
            request_header,
            topic_config,
            policy,
            retry_policy,
            priority_factor,
            message_filter,
            caller_host,
            opaque,
        } = request;
        let revive_qid = if request_header.order.unwrap_or(false) {
            POP_ORDER_REVIVE_QUEUE
        } else {
            let revive_queue_num = policy.revive_queue_num as i64;
            let ck_num = self.ck_message_number.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            ((ck_num % revive_queue_num + revive_queue_num) % revive_queue_num) as i32
        };
        let mut get_message_result = GetMessageResult::new_result_size(request_header.max_msg_nums as usize);
        let random_sample = rand::rng().random_range(0..100);
        let use_priority_mode = topic_config.get_topic_message_type() == TopicMessageType::Priority
            && !request_header.order.unwrap_or(false)
            && random_sample < priority_factor;
        let retry_probability = if use_priority_mode {
            policy.pop_from_retry_probability_for_priority
        } else {
            policy.pop_from_retry_probability
        };
        let need_retry = random_sample < retry_probability;
        let random_q = if use_priority_mode { 0 } else { random_sample };
        let mut start_offset_info = String::with_capacity(64);
        let mut msg_offset_info = String::with_capacity(64);
        let mut order_count_info = if request_header.order.is_some() {
            String::with_capacity(64)
        } else {
            String::new()
        };
        let pop_time = current_millis();

        let mut rest_num = 0;
        if need_retry && !request_header.order.unwrap_or(false) {
            for retry_topic in retry_policy.read_topics(&request_header.topic, &request_header.consumer_group) {
                let retry_topic = CheetahString::from_string(retry_topic);
                rest_num = self
                    .pop_msg_from_topic_by_name(
                        &retry_topic,
                        true,
                        &mut get_message_result,
                        request_header,
                        revive_qid,
                        caller_host,
                        pop_time,
                        message_filter.clone(),
                        &mut start_offset_info,
                        &mut msg_offset_info,
                        &mut order_count_info,
                        random_q,
                        use_priority_mode.then_some(policy.priority_order_asc),
                        rest_num,
                    )
                    .await;
                if !get_message_result.message_mapped_list().is_empty() {
                    break;
                }
            }
        }
        rest_num = if request_header.queue_id < 0 {
            self.pop_msg_from_topic(
                topic_config,
                false,
                &mut get_message_result,
                request_header,
                revive_qid,
                caller_host,
                pop_time,
                message_filter.clone(),
                &mut start_offset_info,
                &mut msg_offset_info,
                &mut order_count_info,
                random_q,
                use_priority_mode.then_some(policy.priority_order_asc),
                rest_num,
            )
            .await
        } else {
            self.pop_msg_from_queue(
                &topic_config.topic_name.clone().unwrap_or_default(),
                &request_header.attempt_id.clone().unwrap_or_default(),
                false,
                &mut get_message_result,
                request_header,
                request_header.queue_id,
                rest_num,
                revive_qid,
                caller_host,
                pop_time,
                message_filter.clone(),
                &mut start_offset_info,
                &mut msg_offset_info,
                &mut order_count_info,
            )
            .await
        };
        if !need_retry
            && get_message_result.message_mapped_list().len() < request_header.max_msg_nums as usize
            && !request_header.order.unwrap_or(false)
        {
            for retry_topic in retry_policy.read_topics(&request_header.topic, &request_header.consumer_group) {
                let retry_topic = CheetahString::from_string(retry_topic);
                rest_num = self
                    .pop_msg_from_topic_by_name(
                        &retry_topic,
                        true,
                        &mut get_message_result,
                        request_header,
                        revive_qid,
                        caller_host,
                        pop_time,
                        message_filter.clone(),
                        &mut start_offset_info,
                        &mut msg_offset_info,
                        &mut order_count_info,
                        random_q,
                        use_priority_mode.then_some(policy.priority_order_asc),
                        rest_num,
                    )
                    .await;
                if !get_message_result.message_mapped_list().is_empty() {
                    break;
                }
            }
        }

        let mut head = self.context.command_factory.create_success_response_command();
        head.set_opaque_mut(opaque);
        if get_message_result.message_mapped_list().is_empty() {
            get_message_result.set_status(Some(GetMessageStatus::NoMessageInQueue));
            head.set_remark_mut(GetMessageStatus::NoMessageInQueue.to_string());
            return Ok(PopStoreReadOutcome::Empty { head, rest_num });
        }

        get_message_result.set_status(Some(GetMessageStatus::Found));
        if rest_num > 0 {
            if let Some(service) = self.pop_deferred_service() {
                let _ = service.latch_arrival(
                    &request_header.topic,
                    request_header.queue_id,
                    None,
                    current_millis() as i64,
                    None,
                    None,
                    service.fanout_cursor(),
                );
            }
        }
        let response_header = PopMessageResponseHeader {
            pop_time,
            invisible_time: request_header.invisible_time,
            revive_qid: revive_qid as u32,
            rest_num: rest_num as u64,
            start_offset_info: Some(CheetahString::from_string(start_offset_info)),
            msg_offset_info: Some(CheetahString::from_string(msg_offset_info)),
            order_count_info: if order_count_info.is_empty() {
                None
            } else {
                Some(CheetahString::from_string(order_count_info))
            },
        };
        head.set_remark_mut(GetMessageStatus::Found.to_string());
        let head = attach_pop_response_header(head, response_header);
        let parts = if policy.transfer_msg_by_heap {
            pop_heap_response_parts(
                head,
                self.read_get_message_result(
                    &get_message_result,
                    &request_header.consumer_group,
                    &request_header.topic,
                    request_header.queue_id,
                ),
            )?
        } else {
            pop_segmented_response_parts(head, take_pop_body_segments(get_message_result))?
        };
        Ok(PopStoreReadOutcome::Found(parts))
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/processor/pop_message/resume.rs"]
mod tests;
