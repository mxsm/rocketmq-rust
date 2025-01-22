use std::collections::HashMap;

use cheetah_string::CheetahString;
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
use serde::{Serialize, Deserialize};

use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};


#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ElectMasterResponseHeader {
    pub master_broker_id: u64,
    pub master_address: CheetahString,
    pub master_epoch: u32,
    pub sync_state_set_epoch: u32,
}

impl ElectMasterResponseHeader {
    pub const MASTER_BROKER_ID: &'static str = "masterBrokerId";
    pub const MASTER_ADDRESS: &'static str = "masterAddress";
    pub const MASTER_EPOCH: &'static str = "masterEpoch";
    pub const SYNC_STATE_SET_EPOCH: &'static str = "syncStateSetEpoch";
}

impl CommandCustomHeader for ElectMasterResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::MASTER_BROKER_ID),
            CheetahString::from_string(self.master_broker_id.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::MASTER_ADDRESS),
            self.master_address.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::MASTER_EPOCH),
            CheetahString::from_string(self.master_epoch.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::SYNC_STATE_SET_EPOCH),
            CheetahString::from_string(self.sync_state_set_epoch.to_string()),
        );
        Some(map)
    }
}

impl FromMap for ElectMasterResponseHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;
    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(ElectMasterResponseHeader {
            master_broker_id: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::MASTER_BROKER_ID,
                ))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or_default(),
            master_address: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::MASTER_ADDRESS,
                ))
                .cloned()
                .unwrap_or_default(),
            master_epoch: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::MASTER_EPOCH,
                ))
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or_default(),
            sync_state_set_epoch: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::SYNC_STATE_SET_EPOCH,
                ))
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or_default(),
        })
    }
}