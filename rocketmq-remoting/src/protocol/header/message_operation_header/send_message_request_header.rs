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

use cheetah_string::CheetahString;
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
    pub producer_group: CheetahString,
    pub topic: CheetahString,
    pub default_topic: CheetahString,
    pub default_topic_queue_nums: i32,
    pub queue_id: Option<i32>,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: Option<CheetahString>,
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
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::from([
            (
                CheetahString::from_static_str(Self::PRODUCER_GROUP),
                self.producer_group.clone(),
            ),
            (
                CheetahString::from_static_str(Self::TOPIC),
                self.topic.clone(),
            ),
            (
                CheetahString::from_static_str(Self::DEFAULT_TOPIC),
                self.default_topic.clone(),
            ),
            (
                CheetahString::from_static_str(Self::DEFAULT_TOPIC_QUEUE_NUMS),
                CheetahString::from_string(self.default_topic_queue_nums.to_string()),
            ),
            (
                CheetahString::from_static_str(Self::SYS_FLAG),
                CheetahString::from_string(self.sys_flag.to_string()),
            ),
            (
                CheetahString::from_static_str(Self::BORN_TIMESTAMP),
                CheetahString::from_string(self.born_timestamp.to_string()),
            ),
            (
                CheetahString::from_static_str(Self::FLAG),
                CheetahString::from_string(self.flag.to_string()),
            ),
        ]);
        if let Some(ref queue_id) = self.queue_id {
            map.insert(
                CheetahString::from_static_str(Self::QUEUE_ID),
                CheetahString::from_string(queue_id.to_string()),
            );
        }
        if let Some(ref properties) = self.properties {
            map.insert(
                CheetahString::from_static_str(Self::PROPERTIES),
                properties.clone(),
            );
        }
        if let Some(ref reconsume_times) = self.reconsume_times {
            map.insert(
                CheetahString::from_static_str(Self::RECONSUME_TIMES),
                CheetahString::from_string(reconsume_times.to_string()),
            );
        }
        if let Some(ref unit_mode) = self.unit_mode {
            map.insert(
                CheetahString::from_static_str(Self::UNIT_MODE),
                CheetahString::from_string(unit_mode.to_string()),
            );
        }
        if let Some(ref batch) = self.batch {
            map.insert(
                CheetahString::from_static_str(Self::BATCH),
                CheetahString::from_string(batch.to_string()),
            );
        }
        if let Some(ref max_reconsume_times) = self.max_reconsume_times {
            map.insert(
                CheetahString::from_static_str(Self::MAX_RECONSUME_TIMES),
                CheetahString::from_string(max_reconsume_times.to_string()),
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

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(SendMessageRequestHeader {
            producer_group: map
                .get(&CheetahString::from_static_str(Self::PRODUCER_GROUP))?
                .clone(),
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))?
                .clone(),
            default_topic: map
                .get(&CheetahString::from_static_str(Self::DEFAULT_TOPIC))?
                .clone(),
            default_topic_queue_nums: map
                .get(&CheetahString::from_static_str(
                    Self::DEFAULT_TOPIC_QUEUE_NUMS,
                ))?
                .parse()
                .ok()?,
            queue_id: map
                .get(&CheetahString::from_static_str(Self::QUEUE_ID))
                .and_then(|v| v.parse().ok()),
            sys_flag: map
                .get(&CheetahString::from_static_str(Self::SYS_FLAG))?
                .parse()
                .ok()?,
            born_timestamp: map
                .get(&CheetahString::from_static_str(Self::BORN_TIMESTAMP))?
                .parse()
                .ok()?,
            flag: map
                .get(&CheetahString::from_static_str(Self::FLAG))?
                .parse()
                .ok()?,
            properties: map
                .get(&CheetahString::from_static_str(Self::PROPERTIES))
                .cloned(),
            reconsume_times: map
                .get(&CheetahString::from_static_str(Self::RECONSUME_TIMES))
                .and_then(|v| v.parse().ok()),
            unit_mode: map
                .get(&CheetahString::from_static_str(Self::UNIT_MODE))
                .and_then(|v| v.parse().ok()),
            batch: map
                .get(&CheetahString::from_static_str(Self::BATCH))
                .and_then(|v| v.parse().ok()),
            max_reconsume_times: map
                .get(&CheetahString::from_static_str(Self::MAX_RECONSUME_TIMES))
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

    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .broker_name
            .as_ref()
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
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

    fn set_namespace(&mut self, namespace: CheetahString) {
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
