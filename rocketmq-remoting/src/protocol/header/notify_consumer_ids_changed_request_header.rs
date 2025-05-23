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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NotifyConsumerIdsChangedRequestHeader {
    pub consumer_group: CheetahString,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl NotifyConsumerIdsChangedRequestHeader {
    pub const CONSUMER_GROUP: &'static str = "consumerGroup";
}

impl CommandCustomHeader for NotifyConsumerIdsChangedRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::CONSUMER_GROUP),
            self.consumer_group.clone(),
        );
        if let Some(value) = self.rpc_request_header.as_ref() {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }
}

impl FromMap for NotifyConsumerIdsChangedRequestHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        let consumer_group = map
            .get(&CheetahString::from_static_str(Self::CONSUMER_GROUP))
            .cloned()
            .unwrap_or_default();
        let rpc_request_header = <RpcRequestHeader as FromMap>::from(map)?;
        Ok(NotifyConsumerIdsChangedRequestHeader {
            consumer_group,
            rpc_request_header: Some(rpc_request_header),
        })
    }
}
