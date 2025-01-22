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
use serde::{Serialize, Deserialize};

use crate::protocol::CheetahString;
use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetMetaDataResponseHeader {
    pub group: CheetahString,
    pub controller_leader_id: CheetahString,
    pub controller_leader_address: CheetahString,
    pub is_leader: bool,
    pub peers: CheetahString,
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
        map.insert(
            CheetahString::from_static_str(Self::GROUP),
            self.group.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::CONTROLLER_LEADER_ID),
            self.controller_leader_id.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::CONTROLLER_LEADER_ADDRESS),
            self.controller_leader_address.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::IS_LEADER),
            CheetahString::from(self.is_leader.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::PEERS),
            self.peers.clone(),
        );
        Some(map)
    }
}

impl FromMap for GetMetaDataResponseHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;
    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(GetMetaDataResponseHeader {
            group: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::GROUP,
                ))
                .cloned()
                .unwrap_or_default(),
            controller_leader_id: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::CONTROLLER_LEADER_ID,
                ))
                .cloned()
                .unwrap_or_default(),
            controller_leader_address: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::CONTROLLER_LEADER_ADDRESS,
                ))
                .cloned()
                .unwrap_or_default(),
            is_leader: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::IS_LEADER,
                ))
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or_default(),
            peers: map
                .get(&CheetahString::from_static_str(
                    GetMetaDataResponseHeader::PEERS,
                ))
                .cloned()
                .unwrap_or_default(),
        })
    }
}