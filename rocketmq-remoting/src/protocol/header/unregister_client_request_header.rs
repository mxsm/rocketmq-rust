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

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct UnregisterClientRequestHeader {
    #[serde(rename = "clientID")]
    pub client_id: CheetahString,
    pub producer_group: Option<CheetahString>,
    pub consumer_group: Option<CheetahString>,
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl UnregisterClientRequestHeader {
    pub const CLIENT_ID: &'static str = "clientID";
    pub const CONSUMER_GROUP: &'static str = "consumerGroup";
    pub const PRODUCER_GROUP: &'static str = "producerGroup";
}

impl FromMap for UnregisterClientRequestHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(UnregisterClientRequestHeader {
            client_id: map
                .get(&CheetahString::from_static_str(
                    UnregisterClientRequestHeader::CLIENT_ID,
                ))
                .cloned()
                .unwrap_or_default(),
            producer_group: map
                .get(&CheetahString::from_static_str(
                    UnregisterClientRequestHeader::PRODUCER_GROUP,
                ))
                .cloned(),
            consumer_group: map
                .get(&CheetahString::from_static_str(
                    UnregisterClientRequestHeader::CONSUMER_GROUP,
                ))
                .cloned(),
            rpc_request_header: Some(<RpcRequestHeader as FromMap>::from(map)?),
        })
    }
}

impl CommandCustomHeader for UnregisterClientRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(UnregisterClientRequestHeader::CLIENT_ID),
            self.client_id.clone(),
        );
        if let Some(producer_group) = &self.producer_group {
            map.insert(
                CheetahString::from_static_str(UnregisterClientRequestHeader::PRODUCER_GROUP),
                producer_group.clone(),
            );
        }
        if let Some(consumer_group) = &self.consumer_group {
            map.insert(
                CheetahString::from_static_str(UnregisterClientRequestHeader::CONSUMER_GROUP),
                consumer_group.clone(),
            );
        }
        if let Some(rpc_request_header) = &self.rpc_request_header {
            map.extend(rpc_request_header.to_map()?);
        }
        Some(map)
    }
}
