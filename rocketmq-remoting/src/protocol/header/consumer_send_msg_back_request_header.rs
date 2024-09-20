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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSendMsgBackRequestHeader {
    pub offset: i64,
    pub group: String,
    pub delay_level: i32,
    pub origin_msg_id: Option<String>,
    pub origin_topic: Option<String>,
    pub unit_mode: bool,
    pub max_reconsume_times: Option<i32>,
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl ConsumerSendMsgBackRequestHeader {
    pub const OFFSET: &'static str = "offset";
    pub const GROUP: &'static str = "group";
    pub const DELAY_LEVEL: &'static str = "delayLevel";
    pub const ORIGIN_MSG_ID: &'static str = "originMsgId";
    pub const ORIGIN_TOPIC: &'static str = "originTopic";
    pub const UNIT_MODE: &'static str = "unitMode";
    pub const MAX_RECONSUME_TIMES: &'static str = "maxReconsumeTimes";
}

impl CommandCustomHeader for ConsumerSendMsgBackRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
        let mut map = std::collections::HashMap::new();
        map.insert(Self::OFFSET.to_string(), self.offset.to_string());
        map.insert(Self::GROUP.to_string(), self.group.clone());
        map.insert(Self::DELAY_LEVEL.to_string(), self.delay_level.to_string());
        if let Some(value) = &self.origin_msg_id {
            map.insert(Self::ORIGIN_MSG_ID.to_string(), value.clone());
        }
        if let Some(value) = &self.origin_topic {
            map.insert(Self::ORIGIN_TOPIC.to_string(), value.clone());
        }
        map.insert(Self::UNIT_MODE.to_string(), self.unit_mode.to_string());
        if let Some(value) = self.max_reconsume_times {
            map.insert(Self::MAX_RECONSUME_TIMES.to_string(), value.to_string());
        }
        if let Some(ref rpc) = self.rpc_request_header {
            if let Some(rpc_map) = rpc.to_map() {
                map.extend(rpc_map);
            }
        }
        Some(map)
    }
}

impl FromMap for ConsumerSendMsgBackRequestHeader {
    type Target = Self;

    fn from(map: &std::collections::HashMap<String, String>) -> Option<Self::Target> {
        Some(ConsumerSendMsgBackRequestHeader {
            offset: map.get(Self::OFFSET)?.parse().ok()?,
            group: map.get(Self::GROUP)?.clone(),
            delay_level: map.get(Self::DELAY_LEVEL)?.parse().ok()?,
            origin_msg_id: map.get(Self::ORIGIN_MSG_ID).cloned(),
            origin_topic: map.get(Self::ORIGIN_TOPIC).cloned(),
            unit_mode: map.get(Self::UNIT_MODE)?.parse().ok()?,
            max_reconsume_times: map.get(Self::MAX_RECONSUME_TIMES)?.parse().ok(),
            rpc_request_header: <RpcRequestHeader as FromMap>::from(map),
        })
    }
}
