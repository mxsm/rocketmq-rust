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
pub(super) fn get_system_group_set() -> &'static HashSet<CheetahString> {
    SYSTEM_GROUP_SET.get_or_init(|| {
        let mut set = HashSet::new();
        set.insert(CheetahString::from(mix_all::DEFAULT_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::DEFAULT_PRODUCER_GROUP));
        set.insert(CheetahString::from(mix_all::TOOLS_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::SCHEDULE_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::FILTERSRV_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::MONITOR_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::CLIENT_INNER_PRODUCER_GROUP));
        set.insert(CheetahString::from(mix_all::SELF_TEST_PRODUCER_GROUP));
        set.insert(CheetahString::from(mix_all::SELF_TEST_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::ONS_HTTP_PROXY_GROUP));
        set.insert(CheetahString::from(mix_all::CID_ONSAPI_PERMISSION_GROUP));
        set.insert(CheetahString::from(mix_all::CID_ONSAPI_OWNER_GROUP));
        set.insert(CheetahString::from(mix_all::CID_ONSAPI_PULL_GROUP));
        set.insert(CheetahString::from(mix_all::CID_SYS_RMQ_TRANS));
        set
    })
}

pub(super) fn sync_pull_result_missing(operation: &'static str) -> RocketMQError {
    RocketMQError::ClientInvalidState {
        expected: "PullResultExt returned by sync pull_message",
        actual: format!("{operation} returned None"),
    }
}

pub(super) fn offset_to_java_long(operation: &'static str, offset: u64) -> rocketmq_error::RocketMQResult<i64> {
    i64::try_from(offset)
        .map_err(|_| RocketMQError::illegal_argument(format!("{operation} offset exceeds Java long range")))
}

pub(super) fn timestamp_to_java_long(operation: &'static str, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
    i64::try_from(timestamp)
        .map_err(|_| RocketMQError::illegal_argument(format!("{operation} timestamp exceeds Java long range")))
}

pub(super) fn timeout_millis_to_u64(timeout_millis: Duration) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(timeout_millis.as_millis()).map_err(|_| {
        RocketMQError::illegal_argument("DefaultMQAdminExt timeoutMillis exceeds Rust u64 millisecond range")
    })
}

pub(super) fn java_long_to_u64(
    operation: &'static str,
    field: &'static str,
    value: i64,
) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(value).map_err(|_| {
        RocketMQError::illegal_argument(format!(
            "{operation} {field} is negative and cannot be represented as Rust u64"
        ))
    })
}

pub(super) fn merge_consume_status_result(
    target: &mut HashMap<CheetahString, HashMap<MessageQueue, u64>>,
    source: HashMap<CheetahString, HashMap<MessageQueue, i64>>,
) -> rocketmq_error::RocketMQResult<()> {
    for (client_id, offsets) in source {
        let target_offsets = target.entry(client_id).or_default();
        for (mq, offset) in offsets {
            target_offsets.insert(mq, java_long_to_u64("getConsumeStatus", "offset", offset)?);
        }
    }
    Ok(())
}

pub(super) fn update_consume_offset_request_header(
    consume_group: CheetahString,
    mq: &MessageQueue,
    offset: u64,
) -> rocketmq_error::RocketMQResult<UpdateConsumerOffsetRequestHeader> {
    let commit_offset = offset_to_java_long("updateConsumeOffset", offset)?;

    Ok(UpdateConsumerOffsetRequestHeader {
        consumer_group: consume_group,
        topic: mq.topic().clone(),
        queue_id: mq.queue_id(),
        commit_offset,
        topic_request_header: Some(TopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: Some(mq.broker_name().clone()),
                oneway: None,
            }),
        }),
    })
}

pub(super) fn reset_offset_by_queue_id_request_headers(
    consumer_group: CheetahString,
    topic_name: CheetahString,
    queue_id: i32,
    reset_offset: u64,
) -> rocketmq_error::RocketMQResult<(UpdateConsumerOffsetRequestHeader, ResetOffsetRequestHeader)> {
    let reset_offset = offset_to_java_long("resetOffsetByQueueId", reset_offset)?;

    let update_header = UpdateConsumerOffsetRequestHeader {
        consumer_group: consumer_group.clone(),
        topic: topic_name.clone(),
        queue_id,
        commit_offset: reset_offset,
        topic_request_header: None,
    };

    let reset_header = ResetOffsetRequestHeader {
        topic: topic_name,
        group: consumer_group,
        queue_id,
        offset: Some(reset_offset),
        timestamp: 0,
        is_force: false,
        topic_request_header: None,
    };

    Ok((update_header, reset_header))
}

pub(super) fn lite_pull_update_consumer_offset_request_header(
    topic: CheetahString,
    group: CheetahString,
    queue_id: i32,
    offset: u64,
) -> rocketmq_error::RocketMQResult<UpdateConsumerOffsetRequestHeader> {
    let commit_offset = offset_to_java_long("updateLitePullConsumerOffset", offset)?;

    Ok(UpdateConsumerOffsetRequestHeader {
        consumer_group: group,
        topic,
        queue_id,
        commit_offset,
        topic_request_header: None,
    })
}

pub(super) fn update_group_forbidden_request_header(
    group_name: CheetahString,
    topic_name: CheetahString,
    readable: Option<bool>,
) -> UpdateGroupForbiddenRequestHeader {
    UpdateGroupForbiddenRequestHeader {
        group: group_name,
        topic: topic_name,
        readable,
        topic_request_header: None,
    }
}

impl DefaultMQAdminExtImpl {
    pub(super) async fn reset_offset_by_timestamp_old_on_broker(
        &self,
        broker_addr: CheetahString,
        queue_data: &QueueData,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let consume_stats = self
            .mq_client_api()?
            .get_consume_stats(
                &broker_addr,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: CheetahString::empty(),
                    topic_request_header: None,
                },
                self.remoting_timeout_millis()?,
            )
            .await?;

        let mut rollback_stats_list = Vec::new();
        let mut has_consumed = false;

        for (queue, offset_wrapper) in &consume_stats.offset_table {
            if queue.topic() == &topic {
                has_consumed = true;
                rollback_stats_list.push(
                    self.reset_offset_consume_offset(
                        broker_addr.clone(),
                        consumer_group.clone(),
                        queue.clone(),
                        offset_wrapper,
                        timestamp,
                        force,
                    )
                    .await?,
                );
            }
        }

        if !has_consumed {
            let topic_status = self
                .mq_client_api()?
                .get_topic_stats_info(
                    &broker_addr,
                    GetTopicStatsInfoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    },
                    self.remoting_timeout_millis()?,
                )
                .await?;

            for queue_id in 0..queue_data.read_queue_nums() {
                let queue = MessageQueue::from_parts(topic.clone(), queue_data.broker_name().clone(), queue_id as i32);
                let mut offset_wrapper = OffsetWrapper::new();
                let topic_offset = topic_status
                    .get_offset_table()
                    .get(&queue)
                    .cloned()
                    .unwrap_or_else(TopicOffset::new);
                offset_wrapper.set_broker_offset(topic_offset.get_max_offset());
                offset_wrapper.set_consumer_offset(topic_offset.get_min_offset());
                rollback_stats_list.push(
                    self.reset_offset_consume_offset(
                        broker_addr.clone(),
                        consumer_group.clone(),
                        queue,
                        &offset_wrapper,
                        timestamp,
                        force,
                    )
                    .await?,
                );
            }
        }

        Ok(rollback_stats_list)
    }

    pub(super) async fn reset_offset_consume_offset(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        queue: MessageQueue,
        offset_wrapper: &OffsetWrapper,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<RollbackStats> {
        let reset_offset = if timestamp == -1 {
            self.mq_client_api()?
                .get_max_offset(broker_addr.as_str(), &queue, self.remoting_timeout_millis()?)
                .await?
        } else {
            self.mq_client_api()?
                .search_offset_by_timestamp(
                    broker_addr.as_str(),
                    &queue,
                    timestamp,
                    rocketmq_model::common::boundary_type::BoundaryType::Lower,
                    self.remoting_timeout_millis()?,
                )
                .await?
        };

        let mut rollback_stats = RollbackStats {
            broker_name: queue.broker_name().clone(),
            queue_id: queue.queue_id() as i64,
            broker_offset: offset_wrapper.get_broker_offset(),
            consumer_offset: offset_wrapper.get_consumer_offset(),
            timestamp_offset: reset_offset,
            rollback_offset: offset_wrapper.get_consumer_offset(),
        };

        if force || reset_offset <= offset_wrapper.get_consumer_offset() {
            rollback_stats.rollback_offset = reset_offset;
            self.mq_client_api()?
                .update_consumer_offset(
                    &broker_addr,
                    UpdateConsumerOffsetRequestHeader {
                        consumer_group,
                        topic: queue.topic().clone(),
                        queue_id: queue.queue_id(),
                        commit_offset: reset_offset,
                        topic_request_header: None,
                    },
                    self.remoting_timeout_millis()?,
                )
                .await?;
        }

        Ok(rollback_stats)
    }

    pub(super) async fn message_consumed_by_group(
        &self,
        msg: &MessageExt,
        group: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let consume_stats = self
            .examine_consume_stats(group.clone(), None, None, None, None)
            .await?;
        let cluster_info = self.examine_broker_cluster_info().await?;

        Ok(is_message_consumed(msg, &consume_stats, &cluster_info))
    }
}

pub(super) fn select_consumer_direct_connection(
    consumer_group: &CheetahString,
    consumer_connection: &ConsumerConnection,
    requested_client_id: Option<&CheetahString>,
) -> rocketmq_error::RocketMQResult<(CheetahString, CheetahString)> {
    let requested = requested_client_id.filter(|client_id| !client_id.is_empty());
    let connection = consumer_connection
        .get_connection_set()
        .iter()
        .find(|connection| {
            requested
                .map(|client_id| connection.get_client_id() == *client_id)
                .unwrap_or_else(|| !connection.get_client_id().is_empty())
        })
        .ok_or_else(|| {
            let message = requested
                .map(|client_id| {
                    format!(
                        "Client `{}` was not found in consumer group `{}`",
                        client_id, consumer_group
                    )
                })
                .unwrap_or_else(|| format!("NO CONSUMER for consumer group `{}`", consumer_group));
            rocketmq_error::RocketMQError::IllegalArgument(message)
        })?;

    Ok((connection.get_client_id(), connection.get_client_addr()))
}

#[allow(deprecated)]
pub(super) fn build_message_track(consumer_group: &str) -> MessageTrack {
    MessageTrack {
        consumer_group: consumer_group.to_string(),
        track_type: Some(TrackType::Unknown),
        exception_desc: String::new(),
    }
}

#[allow(deprecated)]
pub(super) fn resolve_consumed_track_type(msg: &MessageExt, consumer_connection: &ConsumerConnection) -> TrackType {
    let Some(subscription_data) = consumer_connection.get_subscription_table().get(msg.topic()) else {
        return TrackType::Consumed;
    };

    let Some(message_tag) = msg.get_tags() else {
        return TrackType::Consumed;
    };

    if subscription_data.tags_set.is_empty()
        || subscription_data
            .tags_set
            .contains(&CheetahString::from_static_str(SubscriptionData::SUB_ALL))
        || subscription_data.tags_set.contains(&message_tag)
    {
        TrackType::Consumed
    } else {
        TrackType::ConsumedButFiltered
    }
}

pub(super) fn is_message_consumed(msg: &MessageExt, consume_stats: &ConsumeStats, cluster_info: &ClusterInfo) -> bool {
    consume_stats.get_offset_table().iter().any(|(queue, offset_wrapper)| {
        queue.topic() == msg.topic()
            && queue.queue_id() == msg.queue_id()
            && resolve_master_broker_addr(cluster_info, queue)
                .map(|broker_addr| {
                    broker_addr_matches_store_host(broker_addr, msg.store_host())
                        && offset_wrapper.get_consumer_offset() > msg.queue_offset()
                })
                .unwrap_or(false)
    })
}

pub(super) fn resolve_master_broker_addr<'a>(
    cluster_info: &'a ClusterInfo,
    queue: &MessageQueue,
) -> Option<&'a CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()?
        .get(queue.broker_name())?
        .broker_addrs()
        .get(&mix_all::MASTER_ID)
}

pub(super) fn broker_addr_matches_store_host(broker_addr: &CheetahString, store_host: std::net::SocketAddr) -> bool {
    broker_addr
        .parse::<std::net::SocketAddr>()
        .map(|parsed| parsed == store_host)
        .unwrap_or_else(|_| broker_addr.as_str() == store_host.to_string())
}

#[allow(deprecated)]
pub(super) fn apply_track_error(track: &mut MessageTrack, error: &RocketMQError) {
    if let Some(code) = response_code_from_error(error) {
        match code {
            ResponseCode::ConsumerNotOnline => track.set_track_type(TrackType::NotOnline),
            ResponseCode::BroadcastConsumption => track.set_track_type(TrackType::ConsumeBroadcasting),
            _ => {}
        }
    }

    track.set_exception_desc(track_exception_desc(error));
}

pub(super) fn response_code_from_error(error: &RocketMQError) -> Option<ResponseCode> {
    match error {
        RocketMQError::BrokerOperationFailed { code, .. } => Some(ResponseCode::from(*code)),
        RocketMQError::IllegalArgument(message) => parse_response_code_from_message(message),
        _ => None,
    }
}

pub(super) fn is_consumer_not_online_error(error: &RocketMQError) -> bool {
    response_code_from_error(error) == Some(ResponseCode::ConsumerNotOnline)
}

pub(super) fn map_topic_config_lookup_result<T>(
    result: rocketmq_error::RocketMQResult<T>,
) -> rocketmq_error::RocketMQResult<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(error) if response_code_from_error(&error) == Some(ResponseCode::TopicNotExist) => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) fn parse_response_code_from_message(message: &str) -> Option<ResponseCode> {
    let code_start = message.find("CODE:")?;
    let digits = message[code_start + "CODE:".len()..]
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '-')
        .collect::<String>();

    if digits.is_empty() {
        return None;
    }

    digits.parse::<i32>().ok().map(ResponseCode::from)
}

pub(super) fn track_exception_desc(error: &RocketMQError) -> String {
    match error {
        RocketMQError::BrokerOperationFailed { code, message, .. } => format!("CODE:{code} DESC:{message}"),
        _ => error.to_string(),
    }
}

pub(super) fn admin_result_code_for_error(error: &RocketMQError) -> AdminToolsResultCodeEnum {
    match response_code_from_error(error) {
        Some(ResponseCode::ConsumerNotOnline) => AdminToolsResultCodeEnum::ConsumerNotOnline,
        Some(ResponseCode::BroadcastConsumption) => AdminToolsResultCodeEnum::BroadcastConsumption,
        Some(_) => AdminToolsResultCodeEnum::MQBrokerError,
        None => AdminToolsResultCodeEnum::MQClientError,
    }
}

pub(super) fn filter_consume_stats(stats: &mut ConsumeStats, topic: Option<&CheetahString>, queue_id: Option<i32>) {
    if topic.is_none() && queue_id.is_none() {
        return;
    }

    stats.offset_table.retain(|queue, _| {
        let topic_matches = topic.is_none_or(|topic| queue.topic() == topic);
        let queue_matches = queue_id.is_none_or(|queue_id| queue.queue_id() == queue_id);
        topic_matches && queue_matches
    });
}
