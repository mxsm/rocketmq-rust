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

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::mix_all::MASTER_ID;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_protocol::code::response_code::RemotingSysResponseCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::request_source::RequestSource;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::protocol::topic::OffsetMovedEvent;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::GetMessageResult;
use rocketmq_store::GetMessageStatus;
use rocketmq_store::StatsType;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::long_polling::pull_deferred::PullHookMetadata;
use crate::long_polling::pull_deferred::PullSuspendTiming;
use crate::metrics::broker_metrics_manager::BrokerMetricsManager;
use crate::mqtrace::consume_message_context::ConsumeMessageContext;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::processor::pull_message_processor::capability::PullMessageProcessorContext;
use crate::processor::pull_message_processor::rewrite_response_for_static_topic;
use crate::processor::pull_message_processor::static_topic_rewrite_error_response;
use crate::processor::pull_message_processor::StaticTopicRewriteError;
use crate::processor::pull_message_result_handler::PullMessageResult;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::processor::pull_message_result_handler::PullResponseContext;
use crate::processor::pull_message_result_handler::PullSuspension;
use crate::processor::response_plan::store_response_parts;
use crate::processor::response_plan::BrokerResponseParts;

pub struct DefaultPullMessageResultHandler<MS: BrokerReadStore> {
    context: Arc<PullMessageProcessorContext<MS>>,
    consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    broker_metrics_manager: Option<Arc<BrokerMetricsManager>>,
}

impl<MS: BrokerReadStore> DefaultPullMessageResultHandler<MS> {
    pub fn new(
        consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
        context: Arc<PullMessageProcessorContext<MS>>,
        broker_metrics_manager: Option<Arc<BrokerMetricsManager>>,
    ) -> Self {
        Self {
            context,
            consume_message_hook_list,
            broker_metrics_manager,
        }
    }
}

impl<MS: BrokerReadStore> PullMessageResultHandler for DefaultPullMessageResultHandler<MS> {
    async fn handle(
        &self,
        get_message_result: GetMessageResult,
        request_header: PullMessageRequestHeader,
        subscription_data: SubscriptionData,
        subscription_group_config: &SubscriptionGroupConfig,
        message_filter: ArcMessageFilter,
        mut response: RemotingCommand,
        mut mapping_context: TopicQueueMappingContext,
        response_context: PullResponseContext<'_>,
    ) -> rocketmq_error::RocketMQResult<PullMessageResult> {
        let client_address = response_context.effective_peer.to_string();
        let policy = self.context.policy();
        let topic_config = self.context.topics().select_topic_config(request_header.topic.as_ref());
        let topic_sys_flag = topic_config.as_ref().map(|tc| tc.topic_sys_flag as i32).unwrap_or(0);
        Self::compose_response_header(
            &self.context,
            &request_header,
            &get_message_result,
            topic_sys_flag,
            subscription_group_config,
            &mut response,
            client_address.as_str(),
        );
        let code = From::from(response.code());
        self.execute_consume_message_hook_before(
            response_context.hook_metadata,
            &request_header,
            &get_message_result,
            response_context.allow_suspend,
            code,
        );
        {
            let Some(response_header) = response.read_custom_header_mut::<PullMessageResponseHeader>() else {
                return command_result(static_topic_rewrite_error_response(
                    self.context.command_factory(),
                    StaticTopicRewriteError::MissingResponseHeader,
                    &mapping_context,
                ));
            };
            match rewrite_response_for_static_topic(
                self.context.command_factory(),
                &request_header,
                response_header,
                &mut mapping_context,
                code,
            ) {
                Ok(Some(response)) => return command_result(response),
                Ok(None) => {}
                Err(error) => {
                    return command_result(static_topic_rewrite_error_response(
                        self.context.command_factory(),
                        error,
                        &mapping_context,
                    ));
                }
            }
        }
        self.update_broadcast_pulled_offset(
            request_header.topic.as_ref(),
            request_header.consumer_group.as_ref(),
            request_header.queue_id,
            &request_header,
            response_context.broadcast_client_resolver,
            Some(&mut response),
            get_message_result.next_begin_offset(),
        )?;
        self.try_commit_offset(
            response_context.allow_suspend,
            &request_header,
            get_message_result.next_begin_offset(),
            response_context.effective_peer,
        );

        match code {
            ResponseCode::Success => {
                let broker_stats = self.context.broker_stats();
                broker_stats.inc_group_get_nums(
                    request_header.consumer_group.as_str(),
                    request_header.topic.as_str(),
                    get_message_result.message_count(),
                );
                broker_stats.inc_group_get_size(
                    request_header.consumer_group.as_str(),
                    request_header.topic.as_str(),
                    get_message_result.buffer_total_size(),
                );
                broker_stats.inc_broker_get_nums(request_header.topic.as_str(), get_message_result.message_count());

                // Record BrokerMetrics for non-retry/dlq topics
                if let Some(metrics) = self.broker_metrics_manager.as_ref() {
                    let topic = request_header.topic.as_str();
                    let consumer_group = request_header.consumer_group.as_str();
                    let is_retry = topic.starts_with("%RETRY%") || topic.starts_with("%DLQ%");
                    if !is_retry {
                        metrics.inc_messages_out_total(
                            topic,
                            consumer_group,
                            get_message_result.message_count() as u64,
                            false,
                        );
                        metrics.inc_throughput_out_total(
                            topic,
                            consumer_group,
                            get_message_result.buffer_total_size() as u64,
                            false,
                        );
                    }
                }

                if policy.transfer_msg_by_heap {
                    let body = self.read_get_message_result(
                        &get_message_result,
                        request_header.consumer_group.as_str(),
                        request_header.topic.as_str(),
                        request_header.queue_id,
                    );
                    // Record group get latency
                    let latency = (current_millis() - response_context.begin_time_millis) as i32;
                    self.context.broker_stats().inc_group_get_latency(
                        request_header.consumer_group.as_str(),
                        request_header.topic.as_str(),
                        request_header.queue_id,
                        latency,
                    );
                    bytes_result(response, body.unwrap_or_default())
                } else {
                    store_result(response, get_message_result)
                }
            }
            ResponseCode::PullNotFound => {
                let has_suspend_flag = PullSysFlag::has_suspend_flag(request_header.sys_flag as u32);
                let suspend_timeout_millis_long = if has_suspend_flag {
                    request_header.suspend_timeout_millis
                } else {
                    0
                };
                if response_context.allow_suspend && has_suspend_flag {
                    let timing = PullSuspendTiming::from_policy(
                        current_millis(),
                        tokio::time::Instant::now(),
                        policy.long_polling_enable,
                        suspend_timeout_millis_long,
                        policy.short_polling_time_millis,
                    );
                    return Ok(PullMessageResult::Suspend(Box::new(PullSuspension {
                        timing,
                        request_header,
                        subscription_data,
                        message_filter,
                        fallback: BrokerResponseParts::command(response)?,
                    })));
                }
                command_result(response)
            }
            ResponseCode::PullRetryImmediately => command_result(response),
            ResponseCode::PullOffsetMoved => {
                if policy.broker_role != BrokerRole::Slave || policy.offset_check_in_slave {
                    let response_header = response.read_custom_header_mut::<PullMessageResponseHeader>().unwrap();
                    let mut mq = MessageQueue::new();
                    mq.set_topic(request_header.topic.clone());
                    mq.set_broker_name(policy.broker_name.clone());
                    mq.set_queue_id(request_header.queue_id);

                    let offset_moved_event = OffsetMovedEvent {
                        consumer_group: request_header.consumer_group.to_string(),
                        message_queue: mq,
                        offset_request: request_header.queue_offset,
                        offset_new: get_message_result.next_begin_offset(),
                    };
                    warn!(
                        "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, \
                         suggestBrokerId={}",
                        request_header.topic,
                        request_header.consumer_group,
                        offset_moved_event.offset_request,
                        offset_moved_event.offset_new,
                        response_header.suggest_which_broker_id
                    );
                } else {
                    let response_header = response.read_custom_header_mut::<PullMessageResponseHeader>().unwrap();
                    response_header.suggest_which_broker_id = subscription_group_config.broker_id();
                    response.set_code_ref(ResponseCode::PullRetryImmediately);
                    warn!(
                        "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, \
                         suggestBrokerId={}",
                        request_header.topic,
                        request_header.consumer_group,
                        request_header.queue_offset,
                        subscription_group_config.broker_id()
                    );
                }
                command_result(response)
            }
            _ => {
                warn!("[BUG] impossible result code of get message: {}", response.code());
                command_result(response)
            }
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn command_result(response: RemotingCommand) -> rocketmq_error::RocketMQResult<PullMessageResult> {
    Ok(PullMessageResult::Reply(BrokerResponseParts::command(response)?))
}

fn bytes_result(response: RemotingCommand, body: Bytes) -> rocketmq_error::RocketMQResult<PullMessageResult> {
    Ok(PullMessageResult::Reply(BrokerResponseParts::bytes(response, body)?))
}

fn store_result(
    response: RemotingCommand,
    get_message_result: GetMessageResult,
) -> rocketmq_error::RocketMQResult<PullMessageResult> {
    Ok(PullMessageResult::Reply(store_response_parts(
        response,
        get_message_result.message_mapped_vec(),
    )?))
}

#[cfg(test)]
pub(crate) fn pull_bytes_wire_fixture_parts(
    response: RemotingCommand,
    body: Bytes,
) -> rocketmq_error::RocketMQResult<BrokerResponseParts> {
    let PullMessageResult::Reply(parts) = bytes_result(response, body)? else {
        return Err(rocketmq_error::RocketMQError::invariant_violated(
            "the Pull bytes builder unexpectedly suspended",
        ));
    };
    Ok(parts)
}

#[cfg(test)]
pub(crate) fn pull_store_wire_fixture_parts(
    response: RemotingCommand,
    result: GetMessageResult,
) -> rocketmq_error::RocketMQResult<BrokerResponseParts> {
    let PullMessageResult::Reply(parts) = store_result(response, result)? else {
        return Err(rocketmq_error::RocketMQError::invariant_violated(
            "the Pull store-result builder unexpectedly suspended",
        ));
    };
    Ok(parts)
}

impl<MS: BrokerReadStore> DefaultPullMessageResultHandler<MS> {
    /// Read message result and return (body bytes, last store timestamp)
    fn read_get_message_result(
        &self,
        get_message_result: &GetMessageResult,
        group: &str,
        topic: &str,
        queue_id: i32,
    ) -> Option<Bytes> {
        let mut bytes_mut = BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);
        let mut store_timestamp: i64 = 0;

        for msg in get_message_result.message_mapped_list() {
            let data = msg.get_buffer();
            bytes_mut.extend_from_slice(data);

            // Parse storeTimestamp from the last message
            // The position depends on whether bornHost is IPv4 or IPv6
            if data.len() > MessageDecoder::SYSFLAG_POSITION + 4 {
                let sys_flag = i32::from_be_bytes([
                    data[MessageDecoder::SYSFLAG_POSITION],
                    data[MessageDecoder::SYSFLAG_POSITION + 1],
                    data[MessageDecoder::SYSFLAG_POSITION + 2],
                    data[MessageDecoder::SYSFLAG_POSITION + 3],
                ]);

                // bornHost: IPv4 = 8 bytes, IPv6 = 20 bytes
                let bornhost_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
                    8
                } else {
                    20
                };

                // storeTimestamp position = 4(TOTALSIZE) + 4(MAGICCODE) + 4(BODYCRC)
                //                         + 4(QUEUEID) + 4(FLAG) + 8(QUEUEOFFSET)
                //                         + 8(PHYSICALOFFSET) + 4(SYSFLAG) + 8(BORNTIMESTAMP)
                //                         + bornhost_length
                let store_timestamp_pos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhost_length;

                if data.len() > store_timestamp_pos + 8 {
                    store_timestamp = i64::from_be_bytes([
                        data[store_timestamp_pos],
                        data[store_timestamp_pos + 1],
                        data[store_timestamp_pos + 2],
                        data[store_timestamp_pos + 3],
                        data[store_timestamp_pos + 4],
                        data[store_timestamp_pos + 5],
                        data[store_timestamp_pos + 6],
                        data[store_timestamp_pos + 7],
                    ]);
                }
            }
        }

        // Record disk fall behind time
        if store_timestamp > 0 {
            let fall_behind_time = current_millis() as i64 - store_timestamp;
            self.context
                .broker_stats()
                .record_disk_fall_behind_time(group, topic, queue_id, fall_behind_time);
        }

        Some(bytes_mut.freeze())
    }

    fn execute_consume_message_hook_before(
        &self,
        hook_metadata: &PullHookMetadata,
        request_header: &PullMessageRequestHeader,
        get_message_result: &GetMessageResult,
        broker_allow_suspend: bool,
        response_code: ResponseCode,
    ) {
        if self.has_consume_message_hook() {
            let owner = hook_metadata.commercial_owner();
            let auth_type = hook_metadata.account_auth_type();
            let owner_parent = hook_metadata.account_owner_parent();
            let owner_self = hook_metadata.account_owner_self();

            let namespace =
                CheetahString::from_string(NamespaceUtil::get_namespace_from_resource(&request_header.topic));

            let mut context = ConsumeMessageContext {
                consumer_group: &request_header.consumer_group,
                topic: &request_header.topic,
                queue_id: Some(request_header.queue_id),
                client_host: None,
                store_host: None,
                message_ids: None,
                body_length: 0,
                success: false,
                status: None,
                topic_config: None,
                account_auth_type: auth_type,
                account_owner_parent: owner_parent,
                account_owner_self: owner_self,
                rcv_msg_num: 0,
                rcv_msg_size: 0,
                rcv_stat: StatsType::RcvSuccess,
                commercial_rcv_msg_num: 0,
                commercial_owner: None,
                commercial_rcv_stats: StatsType::RcvSuccess,
                commercial_rcv_times: 0,
                commercial_rcv_size: 0,
                namespace: &namespace,
            };

            match response_code {
                ResponseCode::Success => {
                    let commercial_base_count = self.context.policy().commercial_base_count;
                    let inc_value = get_message_result.msg_count4_commercial() * commercial_base_count;

                    context.commercial_rcv_stats = StatsType::RcvSuccess;
                    context.commercial_rcv_times = inc_value;
                    context.commercial_rcv_size = get_message_result.buffer_total_size();
                    context.commercial_owner = owner;

                    context.rcv_stat = StatsType::RcvSuccess;
                    context.rcv_msg_num = get_message_result.message_count();
                    context.rcv_msg_size = get_message_result.buffer_total_size();
                    context.commercial_rcv_msg_num = get_message_result.msg_count4_commercial();
                }
                ResponseCode::PullNotFound if !broker_allow_suspend => {
                    context.commercial_rcv_stats = StatsType::RcvEpolls;
                    context.commercial_rcv_times = 1;
                    context.commercial_owner = owner;

                    context.rcv_stat = StatsType::RcvEpolls;
                    context.rcv_msg_num = 0;
                    context.rcv_msg_size = 0;
                    context.commercial_rcv_msg_num = 0;
                }
                ResponseCode::PullRetryImmediately | ResponseCode::PullOffsetMoved => {
                    context.commercial_rcv_stats = StatsType::RcvEpolls;
                    context.commercial_rcv_times = 1;
                    context.commercial_owner = owner;

                    context.rcv_stat = StatsType::RcvEpolls;
                    context.rcv_msg_num = 0;
                    context.rcv_msg_size = 0;
                    context.commercial_rcv_msg_num = 0;
                }
                _ => {}
            }

            for hook in self.consume_message_hook_list.iter() {
                hook.consume_message_before(&mut context);
            }
        }
    }

    pub fn has_consume_message_hook(&self) -> bool {
        !self.consume_message_hook_list.is_empty()
    }
}

impl<MS: BrokerReadStore> DefaultPullMessageResultHandler<MS> {
    fn compose_response_header(
        context: &PullMessageProcessorContext<MS>,
        request_header: &PullMessageRequestHeader,
        get_message_result: &GetMessageResult,
        topic_sys_flag: i32,
        subscription_group_config: &SubscriptionGroupConfig,
        response: &mut RemotingCommand,
        client_address: &str,
    ) {
        let mut response_header = PullMessageResponseHeader::default();
        response.set_remark_mut(format!("{:?}", get_message_result.status()));
        response_header.next_begin_offset = get_message_result.next_begin_offset();
        response_header.min_offset = get_message_result.min_offset();
        response_header.max_offset = get_message_result.max_offset();
        response_header.topic_sys_flag = Some(topic_sys_flag);
        response_header.group_sys_flag = Some(subscription_group_config.group_sys_flag());

        match get_message_result.status().unwrap() {
            GetMessageStatus::Found => {
                response.set_code_ref(RemotingSysResponseCode::Success);
            }
            GetMessageStatus::MessageWasRemoving | GetMessageStatus::NoMatchedMessage => {
                response.set_code_ref(ResponseCode::PullRetryImmediately);
            }
            GetMessageStatus::NoMatchedLogicQueue | GetMessageStatus::NoMessageInQueue => {
                if request_header.queue_offset != 0 {
                    response.set_code_ref(ResponseCode::PullOffsetMoved);
                    info!(
                        "The broker stores no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} \
                         Consumer Group: {}",
                        request_header.queue_offset,
                        get_message_result.next_begin_offset(),
                        request_header.topic,
                        request_header.queue_id,
                        request_header.consumer_group
                    );
                } else {
                    response.set_code_ref(ResponseCode::PullNotFound);
                }
            }
            GetMessageStatus::OffsetFoundNull | GetMessageStatus::OffsetOverflowOne => {
                response.set_code_ref(ResponseCode::PullNotFound);
            }
            GetMessageStatus::OffsetOverflowBadly => {
                response.set_code_ref(ResponseCode::PullOffsetMoved);
                info!(
                    "The request offset: {} over flow badly, fix to {}, broker max offset: {}, consumer: {}",
                    request_header.queue_offset,
                    get_message_result.next_begin_offset(),
                    get_message_result.max_offset(),
                    client_address
                );
            }
            GetMessageStatus::OffsetReset => {
                response.set_code_ref(ResponseCode::PullOffsetMoved);
                info!(
                    "The queue under pulling was previously reset to start from {}",
                    get_message_result.next_begin_offset()
                );
            }
            GetMessageStatus::OffsetTooSmall => {
                response.set_code_ref(ResponseCode::PullOffsetMoved);
                info!(
                    "The request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, \
                     clientIp={}",
                    request_header.consumer_group,
                    request_header.topic,
                    request_header.queue_offset,
                    get_message_result.min_offset(),
                    client_address
                );
            }
        }

        let policy = context.policy();
        if policy.slave_read_enable && !policy.is_in_broker_container {
            if get_message_result.suggest_pulling_from_slave() {
                response_header.suggest_which_broker_id = subscription_group_config.which_broker_when_consume_slowly();
            } else {
                response_header.suggest_which_broker_id = subscription_group_config.broker_id();
            }
        } else {
            response_header.suggest_which_broker_id = MASTER_ID;
        }

        if policy.broker_id != MASTER_ID
            && !get_message_result.suggest_pulling_from_slave()
            && context.min_broker_id() == MASTER_ID
        {
            debug!(
                "slave redirect pullRequest to master, topic: {}, queueId: {}, consumer group: {}, next: {}, min: {}, \
                 max: {}",
                request_header.topic,
                request_header.queue_id,
                request_header.consumer_group,
                response_header.next_begin_offset,
                response_header.min_offset,
                response_header.max_offset
            );
            response_header.suggest_which_broker_id = MASTER_ID;
            if get_message_result.status() != Some(GetMessageStatus::Found) {
                response.set_code_ref(ResponseCode::PullRetryImmediately);
            }
        }
        response.set_command_custom_header_ref(response_header)
    }

    fn try_commit_offset(
        &self,
        broker_allow_suspend: bool,
        request_header: &PullMessageRequestHeader,
        next_offset: i64,
        client_address: SocketAddr,
    ) {
        self.context.commit_pull_offset(
            client_address,
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            request_header.queue_id,
            next_offset,
        );

        let mut store_offset_enable = broker_allow_suspend;
        let has_commit_offset_flag = PullSysFlag::has_commit_offset_flag(request_header.sys_flag as u32);
        store_offset_enable = store_offset_enable && has_commit_offset_flag;
        if store_offset_enable {
            self.context.commit_offset(
                client_address.to_string().into(),
                request_header.consumer_group.as_ref(),
                request_header.topic.as_ref(),
                request_header.queue_id,
                request_header.commit_offset,
            );
        }
    }

    fn update_broadcast_pulled_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        request_header: &PullMessageRequestHeader,
        client_resolver: &crate::processor::pull_message_result_handler::PullBroadcastClientResolver<'_>,
        response: Option<&mut RemotingCommand>,
        next_begin_offset: i64,
    ) -> rocketmq_error::RocketMQResult<()> {
        if response.is_none() || !self.context.policy().enable_broadcast_offset_store {
            return Ok(());
        }
        let proxy_pull_broadcast = request_header.request_source == Some(RequestSource::ProxyForBroadcast.get_value());
        let Some(client_id) = client_resolver(request_header)? else {
            return Ok(());
        };
        let mut offset = request_header.queue_offset;
        if let Some(response) = response {
            if ResponseCode::from(response.code()) == ResponseCode::PullOffsetMoved {
                offset = next_begin_offset;
            }
        }
        self.context
            .update_broadcast_offset(topic, group, queue_id, offset, client_id.as_str(), proxy_pull_broadcast);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::DefaultMappedFile;
    use rocketmq_store::MappedFile;
    use rocketmq_store::SelectMappedBufferResult;
    use rocketmq_transport::api::HandlerOutcome;
    use rocketmq_transport::api::ResponseBodyKind;
    use rocketmq_transport::api::ResponsePlan;

    fn response_head() -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
    }

    fn reply_plan(result: rocketmq_error::RocketMQResult<PullMessageResult>) -> ResponsePlan {
        let PullMessageResult::Reply(parts) = result.expect("valid Pull result") else {
            panic!("expected an immediate Pull reply");
        };
        let HandlerOutcome::Reply(plan) = parts.into_handler_outcome().expect("valid Pull response plan") else {
            panic!("immediate Pull parts must map to a Reply outcome");
        };
        plan
    }

    #[test]
    fn pull_result_preserves_explicit_immediate_reply() {
        let immediate = command_result(response_head()).expect("valid immediate Pull result");
        assert!(matches!(&immediate, PullMessageResult::Reply(_)));
        let PullMessageResult::Reply(parts) = immediate else {
            panic!("immediate Pull result must remain a reply");
        };
        let HandlerOutcome::Reply(plan) = parts.into_handler_outcome().expect("valid empty Pull plan") else {
            panic!("immediate Pull parts must map to a Reply outcome");
        };
        assert_eq!(plan.body_kind(), ResponseBodyKind::Empty);
    }

    #[test]
    fn heap_pull_reply_exposes_bytes_at_the_dispatch_seam() {
        let body = Bytes::from_static(b"heap-pull-body");
        let plan = reply_plan(bytes_result(response_head(), body));

        assert_eq!(plan.response_code(), ResponseCode::Success as i32);
        assert_eq!(plan.body_kind(), ResponseBodyKind::Bytes);
        assert_eq!(plan.body_len(), 14);
        assert_eq!(plan.body_part_count(), 1);
    }

    #[test]
    fn non_heap_pull_uses_file_regions_when_every_selection_has_a_range() {
        let directory = tempfile::tempdir().expect("temporary pull range directory");
        let path = directory.path().join("00000000000000000000");
        let mapped_file = DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64)
            .expect("mapped file");
        assert!(mapped_file.append_message_bytes(b"ordered-regions"));

        let first = mapped_file
            .try_file_range_selection(0, 8)
            .expect("first selection")
            .expect("first range");
        let second = mapped_file
            .try_file_range_selection(8, 7)
            .expect("second selection")
            .expect("second range");
        assert!(!first.has_byte_snapshot());
        assert!(!second.has_byte_snapshot());

        let mut result = GetMessageResult::new_result_size(2);
        result.add_message(first, 0, 1);
        result.add_message(second, 1, 1);
        assert!(result
            .message_mapped_list()
            .iter()
            .all(|selected| !selected.has_byte_snapshot()));

        let plan = reply_plan(store_result(response_head(), result));
        assert_eq!(plan.body_kind(), ResponseBodyKind::FileRegions);
        assert_eq!(plan.body_len(), 15);
        assert_eq!(plan.body_part_count(), 2);
    }

    #[test]
    fn non_heap_pull_falls_back_to_ordered_body_only_segments() {
        let directory = tempfile::tempdir().expect("temporary mixed pull directory");
        let path = directory.path().join("00000000000000000000");
        let mapped_file = DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64)
            .expect("mapped file");
        assert!(mapped_file.append_message_bytes(b"file-first"));

        let file_selection = mapped_file
            .try_file_range_selection(0, 10)
            .expect("file selection")
            .expect("published file range");
        let byte_selection =
            SelectMappedBufferResult::from_bytes(10, Bytes::from_static(b"bytes-second")).expect("byte selection");
        let mut result = GetMessageResult::new_result_size(2);
        result.add_message(file_selection, 0, 1);
        result.add_message(byte_selection, 1, 1);

        let plan = reply_plan(store_result(response_head(), result));
        assert_eq!(plan.body_kind(), ResponseBodyKind::Segments);
        assert_eq!(plan.body_len(), 22);
        assert_eq!(plan.body_part_count(), 2);
    }
}
