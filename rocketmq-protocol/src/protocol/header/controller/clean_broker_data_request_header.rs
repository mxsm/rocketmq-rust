// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

fn default_invoke_time() -> u64 {
    rocketmq_model::time::current_millis()
}

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader",
    fast
)]
#[serde(rename_all = "camelCase")]
pub struct CleanBrokerDataRequestHeader {
    pub cluster_name: Option<CheetahString>,
    #[header(required)]
    pub broker_name: CheetahString,
    pub broker_controller_ids_to_clean: Option<CheetahString>,
    #[serde(rename = "isCleanLivingBroker", alias = "cleanLivingBroker")]
    #[header(
        key = "isCleanLivingBroker",
        alias = "cleanLivingBroker",
        alias_conflict = "prefer_canonical",
        default,
        default_semantic = "literal:false"
    )]
    pub clean_living_broker: bool,
    #[serde(default = "default_invoke_time")]
    #[header(
        default_with = "default_invoke_time",
        default_semantic = "dynamic:current_time_millis",
        range = "i64"
    )]
    pub invoke_time: u64,
}

pub type CleanControllerBrokerDataRequestHeader = CleanBrokerDataRequestHeader;

impl Default for CleanBrokerDataRequestHeader {
    fn default() -> Self {
        Self {
            cluster_name: None,
            broker_name: CheetahString::new(),
            broker_controller_ids_to_clean: None,
            clean_living_broker: false,
            invoke_time: default_invoke_time(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn clean_broker_data_request_header_serializes_correctly() {
        let header = CleanBrokerDataRequestHeader {
            cluster_name: Some(CheetahString::from_static_str("test_cluster")),
            broker_name: CheetahString::from_static_str("test_broker"),
            broker_controller_ids_to_clean: Some(CheetahString::from_static_str("1;2")),
            clean_living_broker: true,
            invoke_time: 1234567890,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("clusterName")).unwrap(),
            "test_cluster"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerName")).unwrap(),
            "test_broker"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerControllerIdsToClean"))
                .unwrap(),
            "1;2"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("isCleanLivingBroker")).unwrap(),
            "true"
        );
    }

    #[test]
    fn clean_broker_data_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("clusterName"),
            CheetahString::from_static_str("test_cluster"),
        );
        map.insert(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("test_broker"),
        );
        map.insert(
            CheetahString::from_static_str("brokerControllerIdsToClean"),
            CheetahString::from_static_str("1;2"),
        );
        map.insert(
            CheetahString::from_static_str("isCleanLivingBroker"),
            CheetahString::from_static_str("false"),
        );
        map.insert(
            CheetahString::from_static_str("invokeTime"),
            CheetahString::from_static_str("1234567890"),
        );

        let header = <CleanBrokerDataRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(
            header.cluster_name,
            Some(CheetahString::from_static_str("test_cluster"))
        );
        assert_eq!(header.broker_name, CheetahString::from_static_str("test_broker"));
        assert_eq!(
            header.broker_controller_ids_to_clean,
            Some(CheetahString::from_static_str("1;2"))
        );
        assert!(!header.clean_living_broker);
        assert_eq!(header.invoke_time, 1234567890);
    }

    #[test]
    fn missing_invoke_time_uses_the_java_dynamic_default() {
        let before = rocketmq_model::time::current_millis();
        let map = HashMap::from([(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("broker-a"),
        )]);

        let header = <CleanBrokerDataRequestHeader as FromMap>::from(&map).unwrap();
        let after = rocketmq_model::time::current_millis();

        assert!((before..=after).contains(&header.invoke_time));
        assert!(!header.clean_living_broker);
    }

    #[test]
    fn clean_broker_data_request_header_default() {
        let before = rocketmq_model::time::current_millis();
        let header = CleanBrokerDataRequestHeader::default();
        let after = rocketmq_model::time::current_millis();

        assert!(header.cluster_name.is_none());
        assert_eq!(header.broker_name, "");
        assert!(header.broker_controller_ids_to_clean.is_none());
        assert!(!header.clean_living_broker);
        assert!((before..=after).contains(&header.invoke_time));
    }

    #[test]
    fn canonical_clean_living_key_wins_the_legacy_alias() {
        let map = HashMap::from([
            (
                CheetahString::from_static_str("brokerName"),
                CheetahString::from_static_str("broker-a"),
            ),
            (
                CheetahString::from_static_str("isCleanLivingBroker"),
                CheetahString::from_static_str("true"),
            ),
            (
                CheetahString::from_static_str("cleanLivingBroker"),
                CheetahString::from_static_str("false"),
            ),
        ]);

        let header = <CleanBrokerDataRequestHeader as FromMap>::from(&map).unwrap();

        assert!(header.clean_living_broker);
    }
}
