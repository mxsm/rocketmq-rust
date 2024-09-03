/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::code::request_code::RequestCode;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::remoting_command::RemotingCommand;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeader {
    pub producer_group: String,
    pub topic: String,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
    pub queue_id: Option<i32>,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: Option<String>,
    pub reconsume_times: Option<i32>,
    pub unit_mode: Option<bool>,
    pub batch: Option<bool>,
    pub max_reconsume_times: Option<i32>,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl SendMessageRequestHeader {
    pub const PRODUCER_GROUP: &'static str = "producerGroup";
    pub const TOPIC: &'static str = "topic";
    pub const DEFAULT_TOPIC: &'static str = "defaultTopic";
    pub const DEFAULT_TOPIC_QUEUE_NUMS: &'static str = "defaultTopicQueueNums";
    pub const QUEUE_ID: &'static str = "queueId";
    pub const SYS_FLAG: &'static str = "sysFlag";
    pub const BORN_TIMESTAMP: &'static str = "bornTimestamp";
    pub const FLAG: &'static str = "flag";
    pub const PROPERTIES: &'static str = "properties";
    pub const RECONSUME_TIMES: &'static str = "reconsumeTimes";
    pub const UNIT_MODE: &'static str = "unitMode";
    pub const BATCH: &'static str = "batch";
    pub const MAX_RECONSUME_TIMES: &'static str = "maxReconsumeTimes";
}

impl CommandCustomHeader for SendMessageRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::from([
            (
                Self::PRODUCER_GROUP.to_string(),
                self.producer_group.clone(),
            ),
            (Self::TOPIC.to_string(), self.topic.clone()),
            (Self::DEFAULT_TOPIC.to_string(), self.default_topic.clone()),
            (
                Self::DEFAULT_TOPIC_QUEUE_NUMS.to_string(),
                self.default_topic_queue_nums.to_string(),
            ),
            (Self::SYS_FLAG.to_string(), self.sys_flag.to_string()),
            (
                Self::BORN_TIMESTAMP.to_string(),
                self.born_timestamp.to_string(),
            ),
            (Self::FLAG.to_string(), self.flag.to_string()),
        ]);
        if let Some(ref queue_id) = self.queue_id {
            map.insert(Self::QUEUE_ID.to_string(), queue_id.to_string());
        }
        if let Some(ref properties) = self.properties {
            map.insert(Self::PROPERTIES.to_string(), properties.clone());
        }
        if let Some(ref reconsume_times) = self.reconsume_times {
            map.insert(
                Self::RECONSUME_TIMES.to_string(),
                reconsume_times.to_string(),
            );
        }
        if let Some(ref unit_mode) = self.unit_mode {
            map.insert(Self::UNIT_MODE.to_string(), unit_mode.to_string());
        }
        if let Some(ref batch) = self.batch {
            map.insert(Self::BATCH.to_string(), batch.to_string());
        }
        if let Some(ref max_reconsume_times) = self.max_reconsume_times {
            map.insert(
                Self::MAX_RECONSUME_TIMES.to_string(),
                max_reconsume_times.to_string(),
            );
        }
        if let Some(ref value) = self.topic_request_header {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }
}

impl FromMap for SendMessageRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(SendMessageRequestHeader {
            producer_group: map.get(Self::PRODUCER_GROUP)?.clone(),
            topic: map.get(Self::TOPIC)?.clone(),
            default_topic: map.get(Self::DEFAULT_TOPIC)?.clone(),
            default_topic_queue_nums: map.get(Self::DEFAULT_TOPIC_QUEUE_NUMS)?.parse().ok()?,
            queue_id: map.get(Self::QUEUE_ID).and_then(|v| v.parse().ok()),
            sys_flag: map.get(Self::SYS_FLAG)?.parse().ok()?,
            born_timestamp: map.get(Self::BORN_TIMESTAMP)?.parse().ok()?,
            flag: map.get(Self::FLAG)?.parse().ok()?,
            properties: map.get(Self::PROPERTIES).cloned(),
            reconsume_times: map.get(Self::RECONSUME_TIMES).and_then(|v| v.parse().ok()),
            unit_mode: map.get(Self::UNIT_MODE).and_then(|v| v.parse().ok()),
            batch: map.get(Self::BATCH).and_then(|v| v.parse().ok()),
            max_reconsume_times: map
                .get(Self::MAX_RECONSUME_TIMES)
                .and_then(|v| v.parse().ok()),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

impl TopicRequestHeaderTrait for SendMessageRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        match self.topic_request_header {
            None => None,
            Some(ref value) => value.lo,
        }
    }

    fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    fn topic(&self) -> &str {
        self.topic.as_str()
    }

    fn broker_name(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .broker_name
            .as_deref()
    }

    fn set_broker_name(&mut self, broker_name: String) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .namespace
            .as_deref()
    }

    fn set_namespace(&mut self, namespace: String) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .namespaced
            .as_ref()
            .cloned()
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .oneway
            .as_ref()
            .cloned()
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespaced = Some(oneway);
    }

    fn queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.queue_id = queue_id;
    }
}

pub fn parse_request_header(
    request: &RemotingCommand,
    request_code: RequestCode,
) -> Option<SendMessageRequestHeader> {
    let mut request_header_v2 = None;
    if RequestCode::SendMessageV2 == request_code || RequestCode::SendBatchMessage == request_code {
        request_header_v2 = request.decode_command_custom_header::<SendMessageRequestHeaderV2>();
    }

    match request_header_v2 {
        Some(header) => {
            Some(SendMessageRequestHeaderV2::create_send_message_request_header_v1(&header))
        }
        None => request.decode_command_custom_header::<SendMessageRequestHeader>(),
    }
}

#[cfg(test)]
mod send_message_request_header_tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn from_map_creates_instance_with_all_fields() {
        let mut map = HashMap::new();
        map.insert("producerGroup".to_string(), "ProducerGroup".to_string());
        map.insert("topic".to_string(), "TopicName".to_string());
        map.insert("defaultTopic".to_string(), "DefaultTopic".to_string());
        map.insert("defaultTopicQueueNums".to_string(), "4".to_string());
        map.insert("queueId".to_string(), "1".to_string());
        map.insert("sysFlag".to_string(), "0".to_string());
        map.insert("bornTimestamp".to_string(), "1622547600000".to_string());
        map.insert("flag".to_string(), "0".to_string());
        map.insert("properties".to_string(), "key:value".to_string());
        map.insert("reconsumeTimes".to_string(), "0".to_string());
        map.insert("unitMode".to_string(), "false".to_string());
        map.insert("batch".to_string(), "false".to_string());
        map.insert("maxReconsumeTimes".to_string(), "3".to_string());

        let header = <SendMessageRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.producer_group, "ProducerGroup");
        assert_eq!(header.topic, "TopicName");
        assert_eq!(header.default_topic, "DefaultTopic");
        assert_eq!(header.default_topic_queue_nums, 4);
        assert_eq!(header.queue_id, Some(1));
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.born_timestamp, 1622547600000);
        assert_eq!(header.flag, 0);
        assert_eq!(header.properties, Some("key:value".to_string()));
        assert_eq!(header.reconsume_times, Some(0));
        assert_eq!(header.unit_mode, Some(false));
        assert_eq!(header.batch, Some(false));
        assert_eq!(header.max_reconsume_times, Some(3));
    }

    #[test]
    fn to_map_returns_map_with_all_fields() {
        let header = SendMessageRequestHeader {
            producer_group: "ProducerGroup".to_string(),
            topic: "TopicName".to_string(),
            default_topic: "DefaultTopic".to_string(),
            default_topic_queue_nums: 4,
            queue_id: Some(1),
            sys_flag: 0,
            born_timestamp: 1622547600000,
            flag: 0,
            properties: Some("key:value".to_string()),
            reconsume_times: Some(0),
            unit_mode: Some(false),
            batch: Some(false),
            max_reconsume_times: Some(3),
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();

        assert_eq!(map.get("producerGroup").unwrap(), "ProducerGroup");
        assert_eq!(map.get("topic").unwrap(), "TopicName");
        assert_eq!(map.get("defaultTopic").unwrap(), "DefaultTopic");
        assert_eq!(map.get("defaultTopicQueueNums").unwrap(), "4");
        assert_eq!(map.get("queueId").unwrap(), "1");
        assert_eq!(map.get("sysFlag").unwrap(), "0");
        assert_eq!(map.get("bornTimestamp").unwrap(), "1622547600000");
        assert_eq!(map.get("flag").unwrap(), "0");
        assert_eq!(map.get("properties").unwrap(), "key:value");
        assert_eq!(map.get("reconsumeTimes").unwrap(), "0");
        assert_eq!(map.get("unitMode").unwrap(), "false");
        assert_eq!(map.get("batch").unwrap(), "false");
        assert_eq!(map.get("maxReconsumeTimes").unwrap(), "3");
    }

    #[test]
    fn from_map_with_missing_fields_returns_none() {
        let map = HashMap::new(); // Empty map

        let header = <SendMessageRequestHeader as FromMap>::from(&map);

        assert!(header.is_none());
    }

    #[test]
    fn from_map_with_invalid_values_returns_none() {
        let mut map = HashMap::new();
        map.insert("defaultTopicQueueNums".to_string(), "invalid".to_string()); // Invalid integer

        let header = <SendMessageRequestHeader as FromMap>::from(&map);

        assert!(header.is_none());
    }
}
