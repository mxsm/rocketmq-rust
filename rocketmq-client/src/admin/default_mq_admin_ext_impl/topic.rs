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

use super::group::java_long_to_u64;
use super::group::offset_to_java_long;
use super::group::sync_pull_result_missing;
use super::group::timestamp_to_java_long;
use super::*;

pub(super) fn encode_topic_attributes(attributes: &HashMap<CheetahString, CheetahString>) -> Option<CheetahString> {
    if attributes.is_empty() {
        return None;
    }

    let serialized = AttributeParser::parse_to_string(
        &attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>(),
    );

    if serialized.is_empty() {
        None
    } else {
        Some(serialized.into())
    }
}

pub(super) fn master_flush_offset_to_java_long(master_flush_offset: u64) -> rocketmq_error::RocketMQResult<i64> {
    offset_to_java_long("resetMasterFlushOffset", master_flush_offset)
}

pub(super) fn query_consume_queue_index_to_java_long(index: u64) -> rocketmq_error::RocketMQResult<i64> {
    offset_to_java_long("queryConsumeQueue", index)
}

#[allow(clippy::too_many_arguments, reason = "mirrors the Java query-message wire header")]
pub(super) fn query_message_request_header(
    topic: &CheetahString,
    key: &CheetahString,
    max_num: i32,
    begin_timestamp: i64,
    end_timestamp: i64,
    key_type: &CheetahString,
    last_key: Option<&CheetahString>,
) -> rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader {
    rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader {
        topic: topic.clone(),
        key: key.clone(),
        max_num,
        begin_timestamp,
        end_timestamp,
        index_type: Some(key_type.clone()),
        last_key: last_key.cloned(),
        topic_request_header: None,
    }
}

pub(super) fn search_offset_timestamp_to_java_long(timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
    timestamp_to_java_long("searchOffset", timestamp)
}

pub(super) fn topic_list_from_lite_topic_names(
    broker_addr: Option<CheetahString>,
    topic_names: impl IntoIterator<Item = CheetahString>,
) -> TopicList {
    let mut topic_list = topic_names.into_iter().collect::<Vec<_>>();
    topic_list.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    topic_list.dedup();
    TopicList {
        topic_list,
        broker_addr,
    }
}

pub(super) fn lite_topic_list_from_broker_lite_info(
    broker_addr: Option<CheetahString>,
    lite_info: &GetBrokerLiteInfoResponseBody,
) -> TopicList {
    topic_list_from_lite_topic_names(broker_addr, lite_info.get_topic_meta().keys().cloned())
}

pub(super) fn lite_subscription_group_list_from_broker_lite_info(
    topic: &CheetahString,
    lite_info: &GetBrokerLiteInfoResponseBody,
) -> GroupList {
    GroupList::new(lite_info.get_group_meta().get(topic).cloned().unwrap_or_default())
}

pub(super) fn resolve_lite_pull_queue_num(
    field_name: &'static str,
    value: i32,
    fallback_queue_num: i32,
    allow_fallback: bool,
) -> rocketmq_error::RocketMQResult<u32> {
    let resolved = if value > 0 {
        value
    } else if allow_fallback && fallback_queue_num > 0 {
        fallback_queue_num
    } else {
        return Err(mq_client_err!(format!("{field_name} must be positive")));
    };
    u32::try_from(resolved).map_err(|error| mq_client_err!(format!("{field_name} is out of range: {error}")))
}

pub(super) fn lite_pull_topic_config(
    topic: CheetahString,
    queue_num: i32,
    topic_sys_flag: i32,
    read_queue_nums: i32,
    write_queue_nums: i32,
    update_existing: bool,
) -> rocketmq_error::RocketMQResult<TopicConfig> {
    if topic.is_empty() {
        return Err(mq_client_err!("Lite pull topic cannot be empty"));
    }
    if topic_sys_flag < 0 {
        return Err(mq_client_err!("topicSysFlag must be non-negative"));
    }

    let read_queue_nums = resolve_lite_pull_queue_num("readQueueNums", read_queue_nums, queue_num, !update_existing)?;
    let write_queue_nums =
        resolve_lite_pull_queue_num("writeQueueNums", write_queue_nums, queue_num, !update_existing)?;
    let topic_sys_flag = u32::try_from(topic_sys_flag)
        .map_err(|error| mq_client_err!(format!("topicSysFlag is out of range: {error}")))?;

    let mut config = TopicConfig::with_sys_flag(
        topic,
        read_queue_nums,
        write_queue_nums,
        PermName::PERM_READ | PermName::PERM_WRITE,
        topic_sys_flag,
    );
    config.attributes.insert(
        TopicAttributes::topic_message_type_attribute().name().clone(),
        CheetahString::from_static_str(TopicMessageType::Lite.as_str()),
    );
    Ok(config)
}

impl DefaultMQAdminExtImpl {
    pub(super) async fn query_topics_by_consumer_from_route(
        &self,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        let timeout = self.remoting_timeout_millis()?;
        let retry_topic: CheetahString = mix_all::get_retry_topic(&group).into();
        let topic_route = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&retry_topic, timeout)
            .await?;
        let Some(route_data) = topic_route else {
            return Ok(TopicList::default());
        };

        let mut result = TopicList::default();
        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let topic_list = self
                .mq_client_api()?
                .query_topics_by_consumer(&addr, QueryTopicsByConsumerRequestHeader::new(group.clone()), timeout)
                .await?;
            for topic in topic_list.topic_list {
                if !result.topic_list.contains(&topic) {
                    result.topic_list.push(topic);
                }
            }
        }

        Ok(result)
    }

    pub async fn pull_message_from_queue(
        &self,
        broker_addr: &str,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<crate::consumer::pull_result::PullResult> {
        let sys_flag = PullSysFlag::build_sys_flag(false, false, true, false);

        let request_header = PullMessageRequestHeader {
            consumer_group: CheetahString::from_static_str(mix_all::TOOLS_CONSUMER_GROUP),
            topic: mq.topic().clone(),
            lite_topic: None,
            queue_id: mq.queue_id(),
            queue_offset: offset,
            max_msg_nums: max_nums,
            sys_flag: sys_flag as i32,
            commit_offset: 0,
            suspend_timeout_millis: 0,
            sub_version: 0,
            subscription: Some(CheetahString::from(sub_expression)),
            expression_type: None,
            max_msg_bytes: None,
            request_source: None,
            proxy_forward_client_id: None,
            topic_request: None,
        };

        struct NoopPullCallback;
        impl PullCallback for NoopPullCallback {
            async fn on_success(&mut self, _pull_result: PullResultExt) {}
            fn on_exception(&mut self, _e: rocketmq_error::RocketMQError) {}
        }

        let api_impl = self.mq_client_api()?;

        let mut result = MQClientAPIImpl::pull_message(
            api_impl,
            CheetahString::from(broker_addr),
            request_header,
            timeout_millis,
            CommunicationMode::Sync,
            NoopPullCallback,
        )
        .await?
        .ok_or_else(|| sync_pull_result_missing("DefaultMQAdminExtImpl::pull_message_from_queue"))?;

        if result.pull_result.pull_status == PullStatus::Found {
            if let Some(mut message_binary) = result.message_binary.take() {
                let msg_vec = MessageDecoder::decodes_batch(&mut message_binary, true, true);
                result.pull_result.msg_found_list = Some(msg_vec);
            }
        }

        Ok(result.pull_result)
    }

    pub async fn query_message_by_key(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        key_type: CheetahString,
        last_key: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<crate::base::query_result::QueryResult> {
        self.query_message_by_key_internal(
            cluster_name,
            topic,
            key,
            max_num,
            begin_timestamp,
            end_timestamp,
            key_type,
            last_key,
            false,
        )
        .await
    }

    pub async fn query_message_by_unique_key(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        unique_key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> rocketmq_error::RocketMQResult<crate::base::query_result::QueryResult> {
        self.query_message_by_key_internal(
            cluster_name,
            topic,
            unique_key,
            max_num,
            begin_timestamp,
            end_timestamp,
            CheetahString::from_static_str(MessageConst::INDEX_UNIQUE_TYPE),
            None,
            true,
        )
        .await
    }

    pub(super) async fn query_message_by_key_internal(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        key_type: CheetahString,
        last_key: Option<CheetahString>,
        unique_key_flag: bool,
    ) -> rocketmq_error::RocketMQResult<crate::base::query_result::QueryResult> {
        let route_topic = cluster_name.unwrap_or_else(|| topic.clone());
        let topic_route_data = self
            .examine_topic_route_info(route_topic.clone())
            .await?
            .ok_or_else(|| admin_route_not_found(&route_topic))?;

        let mut message_list: Vec<MessageExt> = Vec::new();
        let mut index_last_update_timestamp: u64 = 0;

        let api_impl = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;

        for broker_data in &topic_route_data.broker_datas {
            let broker_addr = match broker_data.select_broker_addr() {
                Some(addr) => addr,
                None => continue,
            };

            let request_header = query_message_request_header(
                &topic,
                &key,
                max_num,
                begin_timestamp,
                end_timestamp,
                &key_type,
                last_key.as_ref(),
            );

            match MQClientAPIImpl::query_message(&api_impl, &broker_addr, request_header, unique_key_flag, timeout)
                .await
            {
                Ok(Some((response_header, body))) => {
                    if let Some(mut body_bytes) = body {
                        let msgs = MessageDecoder::decodes_batch(&mut body_bytes, true, true);
                        message_list.extend(msgs);
                    }
                    let response_index_timestamp = java_long_to_u64(
                        "queryMessage",
                        "indexLastUpdateTimestamp",
                        response_header.index_last_update_timestamp,
                    )?;
                    if response_index_timestamp > index_last_update_timestamp {
                        index_last_update_timestamp = response_index_timestamp;
                    }
                }
                Ok(None) => {
                    // No messages found on this broker, continue
                }
                Err(e) => {
                    tracing::warn!("Failed to query message by key from broker {}: {}", broker_addr, e);
                }
            }
        }

        Ok(crate::base::query_result::QueryResult::new(
            index_last_update_timestamp,
            message_list,
        ))
    }
}

pub(super) fn merge_order_conf_entries(existing: &str, value: &str) -> String {
    let mut entries = HashMap::new();
    for item in existing.split(';').filter(|item| !item.trim().is_empty()) {
        if let Some((broker_name, _)) = item.split_once(':') {
            entries.insert(broker_name.to_string(), item.to_string());
        }
    }
    if let Some((broker_name, _)) = value.split_once(':') {
        entries.insert(broker_name.to_string(), value.to_string());
    } else if !value.trim().is_empty() {
        entries.insert(value.to_string(), value.to_string());
    }

    let mut broker_names: Vec<String> = entries.keys().cloned().collect();
    broker_names.sort();
    broker_names
        .into_iter()
        .filter_map(|broker_name| entries.remove(&broker_name))
        .collect::<Vec<_>>()
        .join(";")
}

pub(super) fn retain_java_user_topic_config(
    topic_table: &mut HashMap<CheetahString, TopicConfig>,
    broker_system_topics: &[CheetahString],
    special_topic: bool,
) {
    topic_table.retain(|topic_name, topic_config| {
        let topic = topic_config
            .topic_name
            .as_ref()
            .map(CheetahString::as_str)
            .unwrap_or_else(|| topic_name.as_str());
        if broker_system_topics
            .iter()
            .any(|system_topic| system_topic.as_str() == topic)
            || TopicValidator::is_system_topic(topic)
        {
            return false;
        }
        if !special_topic && (topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX))
        {
            return false;
        }
        PermName::is_valid(topic_config.perm)
    });
}

pub(super) fn admin_route_not_found(route_topic: &CheetahString) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::route_not_found(route_topic.to_string())
}
