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

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TopicRequestHeader {
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
    pub lo: Option<bool>,
}

impl TopicRequestHeader {
    pub const LO: &'static str = "lo";

    pub fn get_lo(&self) -> Option<&bool> {
        self.lo.as_ref()
    }

    pub fn set_lo(&mut self, lo: bool) {
        self.lo = Some(lo);
    }

    pub fn get_broker_name(&self) -> Option<&CheetahString> {
        self.rpc_request_header
            .as_ref()
            .and_then(|v| v.broker_name.as_ref())
    }
}

impl FromMap for TopicRequestHeader {
    type Error = rocketmq_error::RocketMQError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(TopicRequestHeader {
            lo: map
                .get(&CheetahString::from_static_str(Self::LO))
                .and_then(|v| v.parse().ok()),
            rpc_request_header: Some(<RpcRequestHeader as FromMap>::from(map)?),
        })
    }
}

impl CommandCustomHeader for TopicRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        if let Some(ref lo) = self.lo {
            map.insert(
                CheetahString::from_static_str(Self::LO),
                CheetahString::from_string(lo.to_string()),
            );
        }
        if let Some(value) = self.rpc_request_header.as_ref() {
            if let Some(rpc_map) = value.to_map() {
                map.extend(rpc_map);
            }
        }
        Some(map)
    }
}
