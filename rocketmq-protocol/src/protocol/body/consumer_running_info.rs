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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Display;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::message::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

use crate::protocol::admin::consume_stats::append_message_queue_object_key;
use crate::protocol::admin::consume_stats::normalize_nonstandard_offset_table_keys;
use crate::protocol::body::consume_status::ConsumeStatus;
use crate::protocol::body::pop_process_queue_info::PopProcessQueueInfo;
use crate::protocol::body::process_queue_info::ProcessQueueInfo;
use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;
use crate::protocol::RemotingDeserializable;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerRunningInfo {
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default)]
    pub subscription_set: BTreeSet<SubscriptionData>,
    #[serde(default, with = "any_key_map")]
    pub mq_table: BTreeMap<MessageQueue, ProcessQueueInfo>,
    #[serde(default, with = "any_key_map")]
    pub mq_pop_table: BTreeMap<MessageQueue, PopProcessQueueInfo>,
    #[serde(default)]
    pub status_table: BTreeMap<String, ConsumeStatus>,
    #[serde(default)]
    pub user_consumer_info: BTreeMap<String, String>,
    #[serde(skip)]
    pub consume_type: ConsumeType,
    #[serde(skip)]
    pub consume_orderly: bool,
    #[serde(skip)]
    pub prop_consumer_start_timestamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jstack: Option<String>,
}

impl ConsumerRunningInfo {
    pub const PROP_NAMESERVER_ADDR: &'static str = "PROP_NAMESERVER_ADDR";
    pub const PROP_THREADPOOL_CORE_SIZE: &'static str = "PROP_THREADPOOL_CORE_SIZE";
    pub const PROP_CONSUME_ORDERLY: &'static str = "PROP_CONSUMEORDERLY";
    pub const PROP_CONSUME_TYPE: &'static str = "PROP_CONSUME_TYPE";
    pub const PROP_CLIENT_VERSION: &'static str = "PROP_CLIENT_VERSION";
    pub const PROP_CONSUMER_START_TIMESTAMP: &'static str = "PROP_CONSUMER_START_TIMESTAMP";

    pub fn new() -> Self {
        ConsumerRunningInfo {
            properties: BTreeMap::new(),
            subscription_set: BTreeSet::new(),
            mq_table: BTreeMap::new(),
            mq_pop_table: BTreeMap::new(),
            status_table: BTreeMap::new(),
            user_consumer_info: BTreeMap::new(),
            consume_type: ConsumeType::ConsumePassively,
            consume_orderly: false,
            prop_consumer_start_timestamp: 0,
            jstack: None,
        }
    }

    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    pub fn encode_java_compatible(&self) -> RocketMQResult<Vec<u8>> {
        Ok(self.to_java_compatible_json()?.into_bytes())
    }

    pub fn to_java_compatible_json(&self) -> RocketMQResult<String> {
        let mut body = String::new();
        body.push_str("{\"properties\":");
        body.push_str(&serde_json::to_string(&self.properties)?);
        body.push_str(",\"subscriptionSet\":");
        body.push_str(&serde_json::to_string(&self.subscription_set)?);
        body.push_str(",\"mqTable\":{");
        append_process_queue_map(&mut body, &self.mq_table)?;
        body.push_str("},\"mqPopTable\":{");
        append_pop_process_queue_map(&mut body, &self.mq_pop_table)?;
        body.push_str("},\"statusTable\":");
        body.push_str(&serde_json::to_string(&self.status_table)?);
        body.push_str(",\"userConsumerInfo\":");
        body.push_str(&serde_json::to_string(&self.user_consumer_info)?);
        if let Some(jstack) = &self.jstack {
            body.push_str(",\"jstack\":");
            body.push_str(&serde_json::to_string(jstack)?);
        }
        body.push('}');
        Ok(body)
    }

    pub fn decode(body: &[u8]) -> RocketMQResult<Self> {
        match <Self as RemotingDeserializable>::decode(body) {
            Ok(mut info) => {
                info.sync_derived_fields_from_properties();
                Ok(info)
            }
            Err(error) => {
                let Ok(raw_body) = std::str::from_utf8(body) else {
                    return Err(error);
                };
                let normalized = normalize_consumer_running_info_message_queue_keys(raw_body);
                if normalized == raw_body {
                    return Err(error);
                }
                let mut info = <Self as RemotingDeserializable>::decode_str(&normalized)?;
                info.sync_derived_fields_from_properties();
                Ok(info)
            }
        }
    }

    pub fn sync_properties_from_derived_fields(&mut self) {
        self.set_property(Self::PROP_CONSUME_TYPE, java_consume_type_name(self.consume_type));
        self.set_property(Self::PROP_CONSUME_ORDERLY, self.consume_orderly.to_string());
        self.set_property(
            Self::PROP_CONSUMER_START_TIMESTAMP,
            self.prop_consumer_start_timestamp.to_string(),
        );
    }

    pub fn sync_derived_fields_from_properties(&mut self) {
        if let Some(value) = self.properties.get(Self::PROP_CONSUME_TYPE) {
            self.consume_type = match value.as_str() {
                "CONSUME_PASSIVELY" | "ConsumePassively" => ConsumeType::ConsumePassively,
                "CONSUME_POP" | "ConsumePop" => ConsumeType::ConsumePop,
                _ => ConsumeType::ConsumeActively,
            };
        }
        if let Some(value) = self.properties.get(Self::PROP_CONSUME_ORDERLY) {
            self.consume_orderly = value == "true";
        }
        if let Some(value) = self.properties.get(Self::PROP_CONSUMER_START_TIMESTAMP) {
            if let Ok(timestamp) = value.parse::<u64>() {
                self.prop_consumer_start_timestamp = timestamp;
            }
        }
    }
}

fn java_consume_type_name(consume_type: ConsumeType) -> &'static str {
    match consume_type {
        ConsumeType::ConsumeActively => "CONSUME_ACTIVELY",
        ConsumeType::ConsumePassively => "CONSUME_PASSIVELY",
        ConsumeType::ConsumePop => "CONSUME_POP",
    }
}

fn append_process_queue_map(
    output: &mut String,
    table: &BTreeMap<MessageQueue, ProcessQueueInfo>,
) -> RocketMQResult<()> {
    for (index, (queue, info)) in table.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        append_message_queue_object_key(output, queue)?;
        output.push(':');
        output.push_str(&serde_json::to_string(info)?);
    }
    Ok(())
}

fn append_pop_process_queue_map(
    output: &mut String,
    table: &BTreeMap<MessageQueue, PopProcessQueueInfo>,
) -> RocketMQResult<()> {
    for (index, (queue, info)) in table.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        append_message_queue_object_key(output, queue)?;
        output.push(':');
        output.push_str(&serde_json::to_string(info)?);
    }
    Ok(())
}

fn normalize_consumer_running_info_message_queue_keys(input: &str) -> String {
    normalize_field_with_message_queue_keys(&normalize_field_with_message_queue_keys(input, "mqTable"), "mqPopTable")
}

fn normalize_field_with_message_queue_keys(input: &str, field: &str) -> String {
    let marker = format!("\"{field}\"");
    if !input.contains(&marker) {
        return input.to_string();
    }

    let offset_marker = "\"offsetTable\"";
    let rewritten = input.replacen(&marker, offset_marker, 1);
    let normalized = normalize_nonstandard_offset_table_keys(&rewritten);
    if normalized == rewritten {
        return input.to_string();
    }
    normalized.replacen(offset_marker, &marker, 1)
}

impl Display for ConsumerRunningInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut sb = String::new();
        sb.push_str("\n\n#Consumer Subscription#\n");
        let mut i = 0;
        for subscription in &self.subscription_set {
            i += 1;
            let item = format!(
                "{} Topic: {} ClassFilter: {} SubExpression: {}\n",
                i, subscription.topic, subscription.class_filter_mode, subscription.sub_string
            );

            sb.push_str(&item);
        }
        sb.push_str("\n\n#Consumer Offset#\n");
        sb.push_str("#Topic #Broker Name #QID #Consumer Offset\n");

        for (k, v) in &self.mq_table {
            let item = format!(
                "{}  {}  {}  {}\n",
                k.topic_str(),
                k.broker_name(),
                k.queue_id(),
                v.commit_offset
            );

            sb.push_str(&item);
        }

        sb.push_str("\n\n#Consumer MQ Detail#\n");
        sb.push_str("#Topic #Broker Name #QID #ProcessQueueInfo\n");

        for (k, v) in &self.mq_table {
            let item = format!("{}  {}  {}  {}\n", k.topic_str(), k.broker_name(), k.queue_id(), v);

            sb.push_str(&item);
        }

        sb.push_str("\n\n#Consumer Pop Detail#\n");
        sb.push_str("#Topic #Broker Name #QID #ProcessQueueInfo\n");
        for (k, v) in &self.mq_pop_table {
            let item = format!("{}  {}  {}  {}\n", k.topic_str(), k.broker_name(), k.queue_id(), v);

            sb.push_str(&item);
        }
        sb.push_str("\n\n#Consumer RT&TPS#\n");
        sb.push_str(
            "#Topic #Pull RT #Pull TPS #Consume RT #ConsumeOK TPS #ConsumeFailed TPS #ConsumeFailedMsgsInHour\n",
        );

        for (k, v) in &self.status_table {
            let item = format!(
                "{} {} {} {} {} {} {}\n",
                k, v.pull_rt, v.pull_tps, v.consume_rt, v.consume_ok_tps, v.consume_failed_tps, v.consume_failed_msgs
            );

            sb.push_str(&item);
        }

        sb.push_str("\n\n#User Consume Info#\n");
        for (k, v) in &self.user_consumer_info {
            let item = format!("{}: {}\n", k, v);
            sb.push_str(&item);
        }
        f.write_str(&sb)
    }
}

impl ConsumerRunningInfo {
    pub fn is_push_type(&self) -> bool {
        matches!(self.consume_type, ConsumeType::ConsumePassively)
    }

    pub fn analyze_subscription_at(
        cri_table: BTreeMap<String /* clientId */, ConsumerRunningInfo>,
        now_millis: u64,
    ) -> RocketMQResult<()> {
        let first = cri_table.first_key_value().ok_or_else(|| {
            RocketMQError::response_process_failed("analyze_subscription", "consumer running info table is empty")
        })?;
        let prev = first.1;

        let push = matches!(prev.consume_type, ConsumeType::ConsumePassively);

        let start_for_a_while = (now_millis - prev.prop_consumer_start_timestamp) > (1000 * 60 * 2);

        if push && start_for_a_while {
            let mut prev = prev.clone();
            for v in cri_table.values() {
                if v.subscription_set != prev.subscription_set {
                    // Different subscription in the same group of consumer
                    return Err(RocketMQError::response_process_failed(
                        "analyze_subscription",
                        "different subscription in the same consumer group",
                    ));
                }

                prev = v.clone();
            }
        }
        Ok(())
    }

    pub fn analyze_process_queue_at(
        client_id: String,
        info: ConsumerRunningInfo,
        now_millis: u64,
    ) -> RocketMQResult<String> {
        let mut sb = String::new();
        let push = matches!(info.consume_type, ConsumeType::ConsumePassively);

        let order_msg = info.consume_orderly;

        if push {
            for (k, v) in &info.mq_table {
                if order_msg {
                    if !v.locked {
                        sb.push_str(&format!(
                            "{} {} can't lock for a while, {}ms\n",
                            client_id,
                            k,
                            now_millis - v.last_lock_timestamp
                        ));
                    } else if v.droped && v.try_unlock_times > 0 {
                        sb.push_str(&format!(
                            "{} {} unlock {} times, still failed\n",
                            client_id, k, v.try_unlock_times
                        ));
                    }
                } else {
                    let diff = now_millis - v.last_consume_timestamp;

                    if diff > (1000 * 60) && v.cached_msg_count > 0 {
                        sb.push_str(&format!(
                            "{} {} can't consume for a while, maybe blocked, {}ms\n",
                            client_id, k, diff
                        ));
                    }
                }
            }
        }

        Ok(sb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyze_subscription_empty_table_uses_response_process_error() {
        let error = ConsumerRunningInfo::analyze_subscription_at(BTreeMap::new(), 0)
            .expect_err("empty consumer running info should be rejected");

        assert_eq!(error.kind(), rocketmq_error::ErrorKind::ResponseProcessFailed);
        assert!(matches!(
            error,
            RocketMQError::ResponseProcessFailed {
                operation: "analyze_subscription",
                ..
            }
        ));
    }

    #[test]
    fn consumer_running_info_default() {
        let info = ConsumerRunningInfo::new();
        assert!(info.subscription_set.is_empty());
        assert!(info.mq_table.is_empty());
        assert!(info.mq_pop_table.is_empty());
        assert!(info.status_table.is_empty());
        assert!(info.user_consumer_info.is_empty());
        assert_eq!(info.consume_type, ConsumeType::ConsumePassively);
        assert!(!info.consume_orderly);
        assert_eq!(info.prop_consumer_start_timestamp, 0);
    }

    #[test]
    fn consumer_running_info_display() {
        let mut info = ConsumerRunningInfo::new();
        let subscription_data = SubscriptionData {
            topic: "topic".into(),
            sub_string: "*".into(),
            ..Default::default()
        };
        info.subscription_set.insert(subscription_data);

        let mq = MessageQueue::from_parts("topic", "broker", 1);
        let process_queue_info = ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 0,
            cached_msg_max_offset: 100,
            cached_msg_count: 100,
            cached_msg_size_in_mib: 0,
            transaction_msg_min_offset: 0,
            transaction_msg_max_offset: 0,
            transaction_msg_count: 0,
            locked: false,
            try_unlock_times: 0,
            last_lock_timestamp: 0,
            droped: false,
            last_pull_timestamp: 0,
            last_consume_timestamp: 0,
        };
        info.mq_table.insert(mq.clone(), process_queue_info);

        let pop_process_queue_info = PopProcessQueueInfo::new(1, true, 2);
        info.mq_pop_table.insert(mq.clone(), pop_process_queue_info);

        let status = ConsumeStatus {
            pull_rt: 1.0,
            pull_tps: 2.0,
            consume_rt: 3.0,
            consume_ok_tps: 4.0,
            consume_failed_tps: 5.0,
            consume_failed_msgs: 6,
        };

        info.status_table.insert("clientId".to_string(), status);
        info.user_consumer_info
            .insert("userKey".to_string(), "userValue".to_string());

        let display = format!("{}", info);
        assert!(display.contains("#Consumer Subscription#"));
        assert!(display.contains("Topic: topic"));
        assert!(display.contains("#Consumer Offset#"));
        assert!(display.contains("broker"));
        assert!(display.contains("100"));
        assert!(display.contains("#Consumer MQ Detail#"));
        assert!(display.contains("#Consumer Pop Detail#"));
        assert!(display.contains("#Consumer RT&TPS#"));
        assert!(display.contains("clientId"));
        assert!(display.contains("#User Consume Info#"));
        assert!(display.contains("userKey: userValue"));
    }

    #[test]
    fn consumer_running_info_java_compatible_encode_decode_preserves_mq_tables() {
        let mut info = ConsumerRunningInfo::new();
        info.consume_type = ConsumeType::ConsumePassively;
        info.consume_orderly = true;
        info.prop_consumer_start_timestamp = 12345;
        info.sync_properties_from_derived_fields();

        let mq = MessageQueue::from_parts("topic", "broker", 1);
        let process_queue_info = ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 10,
            cached_msg_max_offset: 100,
            cached_msg_count: 3,
            cached_msg_size_in_mib: 1,
            transaction_msg_min_offset: 0,
            transaction_msg_max_offset: 0,
            transaction_msg_count: 0,
            locked: true,
            try_unlock_times: 0,
            last_lock_timestamp: 7,
            droped: false,
            last_pull_timestamp: 8,
            last_consume_timestamp: 9,
        };
        info.mq_table.insert(mq.clone(), process_queue_info);
        info.mq_pop_table
            .insert(mq.clone(), PopProcessQueueInfo::new(2, false, 11));

        let encoded = info
            .encode_java_compatible()
            .expect("consumer running info should encode");
        let encoded_text = std::str::from_utf8(&encoded).expect("encoded body should be utf8");
        assert!(encoded_text.contains("\"mqTable\":{{\"topic\":\"topic\""));

        let decoded = ConsumerRunningInfo::decode(&encoded).expect("consumer running info should decode");
        assert_eq!(decoded.mq_table.get(&mq).map(|item| item.commit_offset), Some(100));
        assert_eq!(decoded.mq_pop_table.get(&mq).map(|item| item.wait_ack_count()), Some(2));
        assert_eq!(
            decoded
                .properties
                .get(ConsumerRunningInfo::PROP_CONSUME_TYPE)
                .map(String::as_str),
            Some("CONSUME_PASSIVELY")
        );
        assert!(decoded.consume_orderly);
        assert_eq!(decoded.prop_consumer_start_timestamp, 12345);
    }
}
