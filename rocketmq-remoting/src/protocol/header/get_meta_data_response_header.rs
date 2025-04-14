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

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::CheetahString;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetMetaDataResponseHeader {
    pub group: Option<CheetahString>,
    pub controller_leader_id: Option<CheetahString>,
    pub controller_leader_address: Option<CheetahString>,
    pub is_leader: Option<bool>,
    pub peers: Option<CheetahString>,
}

impl GetMetaDataResponseHeader {
    pub const GROUP: &'static str = "group";
    pub const CONTROLLER_LEADER_ID: &'static str = "controllerLeaderId";
    pub const CONTROLLER_LEADER_ADDRESS: &'static str = "controllerLeaderAddress";
    pub const IS_LEADER: &'static str = "isLeader";
    pub const PEERS: &'static str = "peers";
}

impl CommandCustomHeader for GetMetaDataResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        if let Some(value) = self.group.as_ref() {
            map.insert(CheetahString::from_static_str(Self::GROUP), value.clone());
        }
        if let Some(value) = self.controller_leader_id.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::CONTROLLER_LEADER_ID),
                value.clone(),
            );
        }
        if let Some(value) = self.controller_leader_address.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::CONTROLLER_LEADER_ADDRESS),
                value.clone(),
            );
        }
        if let Some(value) = self.is_leader.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::IS_LEADER),
                CheetahString::from(value.to_string()),
            );
        }
        if let Some(value) = self.peers.as_ref() {
            map.insert(CheetahString::from_static_str(Self::PEERS), value.clone());
        }
        Some(map)
    }
}

impl FromMap for GetMetaDataResponseHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;
    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(GetMetaDataResponseHeader {
            group: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::GROUP,
                ))
                .cloned(),
            controller_leader_id: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::CONTROLLER_LEADER_ID,
                ))
                .cloned(),
            controller_leader_address: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::CONTROLLER_LEADER_ADDRESS,
                ))
                .cloned(),
            is_leader: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::IS_LEADER,
                ))
                .and_then(|s| s.parse::<bool>().ok()),
            peers: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::PEERS,
                ))
                .cloned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_meta_data_response_header_serializes_correctly() {
        let header = GetMetaDataResponseHeader {
            group: Some(CheetahString::from_static_str("test_group")),
            controller_leader_id: Some(CheetahString::from_static_str("1")),
            controller_leader_address: Some(CheetahString::from_static_str("192.168.1.1:9876")),
            is_leader: Some(true),
            peers: Some(CheetahString::from_static_str(
                "192.168.1.1:9876,192.168.1.2:9876",
            )),
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                GetMetaDataResponseHeader::GROUP
            ))
            .unwrap(),
            "test_group"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                GetMetaDataResponseHeader::CONTROLLER_LEADER_ID
            ))
            .unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                GetMetaDataResponseHeader::CONTROLLER_LEADER_ADDRESS
            ))
            .unwrap(),
            "192.168.1.1:9876"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                GetMetaDataResponseHeader::IS_LEADER
            ))
            .unwrap(),
            "true"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                GetMetaDataResponseHeader::PEERS
            ))
            .unwrap(),
            "192.168.1.1:9876,192.168.1.2:9876"
        );
    }

    #[test]
    fn get_meta_data_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(GetMetaDataResponseHeader::GROUP),
            CheetahString::from("test_group"),
        );
        map.insert(
            CheetahString::from_static_str(GetMetaDataResponseHeader::CONTROLLER_LEADER_ID),
            CheetahString::from("1"),
        );
        map.insert(
            CheetahString::from_static_str(GetMetaDataResponseHeader::CONTROLLER_LEADER_ADDRESS),
            CheetahString::from("192.168.1.1:9876"),
        );
        map.insert(
            CheetahString::from_static_str(GetMetaDataResponseHeader::IS_LEADER),
            CheetahString::from("true"),
        );
        map.insert(
            CheetahString::from_static_str(GetMetaDataResponseHeader::PEERS),
            CheetahString::from("192.168.1.1:9876,192.168.1.2:9876"),
        );
        let header = <GetMetaDataResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(
            header.group,
            Some(CheetahString::from_static_str("test_group"))
        );
        assert_eq!(
            header.controller_leader_id,
            Some(CheetahString::from_static_str("1"))
        );
        assert_eq!(
            header.controller_leader_address,
            Some(CheetahString::from_static_str("192.168.1.1:9876"))
        );
        assert_eq!(header.is_leader, Some(true));
        assert_eq!(
            header.peers,
            Some(CheetahString::from_static_str(
                "192.168.1.1:9876,192.168.1.2:9876"
            ))
        );
    }

    #[test]
    fn get_meta_data_response_header_handles_missing_optional_fields() {
        let map = HashMap::new();
        let header = <GetMetaDataResponseHeader as FromMap>::from(&map).unwrap();
        assert!(header.group.is_none());
        assert!(header.controller_leader_id.is_none());
        assert!(header.controller_leader_address.is_none());
        assert!(header.is_leader.is_none());
        assert!(header.peers.is_none());
    }
}
