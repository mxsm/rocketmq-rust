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

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ElectMasterResponseHeader {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<CheetahString>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
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
        if let Some(value) = self.master_broker_id.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::MASTER_BROKER_ID),
                CheetahString::from(value.to_string()),
            );
        }
        if let Some(value) = self.master_address.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::MASTER_ADDRESS),
                value.clone(),
            );
        }
        if let Some(value) = self.master_epoch.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::MASTER_EPOCH),
                CheetahString::from(value.to_string()),
            );
        }
        if let Some(value) = self.sync_state_set_epoch.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::SYNC_STATE_SET_EPOCH),
                CheetahString::from(value.to_string()),
            );
        }
        Some(map)
    }
}

impl FromMap for ElectMasterResponseHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;
    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(ElectMasterResponseHeader {
            master_broker_id: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::MASTER_BROKER_ID,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            master_address: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::MASTER_ADDRESS,
                ))
                .cloned(),
            master_epoch: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::MASTER_EPOCH,
                ))
                .and_then(|s| s.parse::<i32>().ok()),
            sync_state_set_epoch: map
                .get(&CheetahString::from_static_str(
                    ElectMasterResponseHeader::SYNC_STATE_SET_EPOCH,
                ))
                .and_then(|s| s.parse::<i32>().ok()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elect_master_response_header_serializes_correctly() {
        let header = ElectMasterResponseHeader {
            master_broker_id: Some(1),
            master_address: Some(CheetahString::from_static_str("test_address")),
            master_epoch: Some(2),
            sync_state_set_epoch: Some(3),
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                ElectMasterResponseHeader::MASTER_BROKER_ID
            ))
            .unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                ElectMasterResponseHeader::MASTER_ADDRESS
            ))
            .unwrap(),
            "test_address"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                ElectMasterResponseHeader::MASTER_EPOCH
            ))
            .unwrap(),
            "2"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                ElectMasterResponseHeader::SYNC_STATE_SET_EPOCH
            ))
            .unwrap(),
            "3"
        );
    }

    #[test]

    fn elect_master_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(ElectMasterResponseHeader::MASTER_BROKER_ID),
            CheetahString::from("1"),
        );
        map.insert(
            CheetahString::from_static_str(ElectMasterResponseHeader::MASTER_ADDRESS),
            CheetahString::from("test_address"),
        );
        map.insert(
            CheetahString::from_static_str(ElectMasterResponseHeader::MASTER_EPOCH),
            CheetahString::from("2"),
        );
        map.insert(
            CheetahString::from_static_str(ElectMasterResponseHeader::SYNC_STATE_SET_EPOCH),
            CheetahString::from("3"),
        );
        let header = <ElectMasterResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.master_broker_id, Some(1));
        assert_eq!(
            header.master_address,
            Some(CheetahString::from_static_str("test_address"))
        );
        assert_eq!(header.master_epoch, Some(2));
        assert_eq!(header.sync_state_set_epoch, Some(3));
    }

    #[test]
    fn elect_master_response_header_handles_missing_optional_fields() {
        let map = HashMap::new();
        let header = <ElectMasterResponseHeader as FromMap>::from(&map).unwrap();
        assert!(header.master_broker_id.is_none());
        assert!(header.master_address.is_none());
        assert!(header.master_epoch.is_none());
        assert!(header.sync_state_set_epoch.is_none());
    }
}
