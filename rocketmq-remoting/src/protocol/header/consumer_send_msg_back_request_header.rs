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
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSendMsgBackRequestHeader {
    pub offset: i64,
    pub group: CheetahString,
    pub delay_level: i32,
    pub origin_msg_id: Option<CheetahString>,
    pub origin_topic: Option<CheetahString>,
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
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::OFFSET),
            CheetahString::from_string(self.offset.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::GROUP),
            self.group.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::DELAY_LEVEL),
            CheetahString::from_string(self.delay_level.to_string()),
        );
        if let Some(value) = &self.origin_msg_id {
            map.insert(
                CheetahString::from_static_str(Self::ORIGIN_MSG_ID),
                value.clone(),
            );
        }
        if let Some(value) = &self.origin_topic {
            map.insert(
                CheetahString::from_static_str(Self::ORIGIN_TOPIC),
                value.clone(),
            );
        }
        map.insert(
            CheetahString::from_static_str(Self::UNIT_MODE),
            CheetahString::from_string(self.unit_mode.to_string()),
        );
        if let Some(value) = self.max_reconsume_times {
            map.insert(
                CheetahString::from_static_str(Self::MAX_RECONSUME_TIMES),
                CheetahString::from_string(value.to_string()),
            );
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
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(ConsumerSendMsgBackRequestHeader {
            offset: map
                .get(&CheetahString::from_static_str(Self::OFFSET))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Missing offset".to_string(),
                ))?
                .parse()
                .map_err(|_| Self::Error::RemotingCommandError("Invalid offset".to_string()))?,
            group: map
                .get(&CheetahString::from_static_str(Self::GROUP))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Missing group".to_string(),
                ))?,
            delay_level: map
                .get(&CheetahString::from_static_str(Self::DELAY_LEVEL))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Missing delay level".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::RemotingCommandError("Invalid delay level".to_string())
                })?,
            origin_msg_id: map
                .get(&CheetahString::from_static_str(Self::ORIGIN_MSG_ID))
                .cloned(),
            origin_topic: map
                .get(&CheetahString::from_static_str(Self::ORIGIN_TOPIC))
                .cloned(),
            unit_mode: map
                .get(&CheetahString::from_static_str(Self::UNIT_MODE))
                .cloned()
                .unwrap_or(CheetahString::from_static_str("false"))
                .parse()
                .unwrap_or(false),
            max_reconsume_times: map
                .get(&CheetahString::from_static_str(Self::MAX_RECONSUME_TIMES))
                .and_then(|value| value.parse().ok()),
            rpc_request_header: Some(<RpcRequestHeader as FromMap>::from(map)?),
        })
    }
}
