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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerListByGroupRequestHeader {
    pub consumer_group: CheetahString,
    #[serde(flatten)]
    pub rpc: Option<RpcRequestHeader>,
}

impl GetConsumerListByGroupRequestHeader {
    pub const CONSUMER_GROUP: &'static str = "consumerGroup";
}

impl CommandCustomHeader for GetConsumerListByGroupRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::CONSUMER_GROUP),
            self.consumer_group.clone(),
        );
        if let Some(ref rpc) = self.rpc {
            if let Some(rpc_map) = rpc.to_map() {
                map.extend(rpc_map);
            }
        }
        Some(map)
    }
}
impl FromMap for GetConsumerListByGroupRequestHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(GetConsumerListByGroupRequestHeader {
            consumer_group: map
                .get(&CheetahString::from_static_str(Self::CONSUMER_GROUP))
                .cloned()
                .unwrap_or_default(),
            rpc: Some(<RpcRequestHeader as FromMap>::from(map)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn get_consumer_list_by_group_request_header_to_map() {
        let header = GetConsumerListByGroupRequestHeader {
            consumer_group: "test_group".into(),
            rpc: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(GetConsumerListByGroupRequestHeader::CONSUMER_GROUP),
            Some(&"test_group".into())
        );
    }

    #[test]
    fn get_consumer_list_by_group_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(
            GetConsumerListByGroupRequestHeader::CONSUMER_GROUP.into(),
            "test_group".into(),
        );

        let header = <GetConsumerListByGroupRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_group");
    }
}
