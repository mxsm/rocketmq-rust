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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader",
    java_class = "org.apache.rocketmq.remoting.rpc.RpcRequestHeader"
)]
pub struct RpcRequestHeader {
    // the namespace name
    #[serde(rename = "ns", alias = "namespace")]
    #[header(key = "ns", alias = "namespace", alias_conflict = "prefer_canonical")]
    pub namespace: Option<CheetahString>,
    // if the data has been namespaced
    #[serde(rename = "nsd", alias = "namespaced")]
    #[header(key = "nsd", alias = "namespaced", alias_conflict = "prefer_canonical")]
    pub namespaced: Option<bool>,
    // the abstract remote addr name, usually the physical broker name
    #[serde(rename = "bname", alias = "brokerName")]
    #[header(key = "bname", alias = "brokerName", alias_conflict = "prefer_canonical")]
    pub broker_name: Option<CheetahString>,
    // oneway
    #[serde(rename = "oway", alias = "oneway")]
    #[header(key = "oway", alias = "oneway", alias_conflict = "prefer_canonical")]
    pub oneway: Option<bool>,
}

impl RpcRequestHeader {
    pub fn new(
        namespace: Option<CheetahString>,
        namespaced: Option<bool>,
        broker_name: Option<CheetahString>,
        oneway: Option<bool>,
    ) -> Self {
        Self {
            namespace,
            namespaced,
            broker_name,
            oneway,
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
    fn encodes_java_canonical_short_keys() {
        let header = RpcRequestHeader::new(Some("tenant".into()), Some(true), Some("broker-a".into()), Some(false));

        let map = header.to_map().unwrap();
        assert_eq!(map.get("ns"), Some(&"tenant".into()));
        assert_eq!(map.get("nsd"), Some(&"true".into()));
        assert_eq!(map.get("bname"), Some(&"broker-a".into()));
        assert_eq!(map.get("oway"), Some(&"false".into()));
        for legacy_key in ["namespace", "namespaced", "brokerName", "oneway"] {
            assert!(!map.contains_key(legacy_key));
        }
    }

    #[test]
    fn decodes_legacy_long_keys() {
        let map = HashMap::from([
            ("namespace".into(), "tenant".into()),
            ("namespaced".into(), "true".into()),
            ("brokerName".into(), "broker-a".into()),
            ("oneway".into(), "false".into()),
        ]);

        let header = <RpcRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.namespace.as_deref(), Some("tenant"));
        assert_eq!(header.namespaced, Some(true));
        assert_eq!(header.broker_name.as_deref(), Some("broker-a"));
        assert_eq!(header.oneway, Some(false));
    }

    #[test]
    fn canonical_keys_win_alias_conflicts_independent_of_insertion_order() {
        let entries = [
            ("ns", "canonical-ns"),
            ("namespace", "legacy-ns"),
            ("nsd", "true"),
            ("namespaced", "false"),
            ("bname", "canonical-broker"),
            ("brokerName", "legacy-broker"),
            ("oway", "false"),
            ("oneway", "true"),
        ];

        for reverse in [false, true] {
            let mut map = HashMap::new();
            let ordered: Vec<_> = if reverse {
                entries.iter().rev().copied().collect()
            } else {
                entries.to_vec()
            };
            for (key, value) in ordered {
                map.insert(key.into(), value.into());
            }

            let header = <RpcRequestHeader as FromMap>::from(&map).unwrap();
            assert_eq!(header.namespace.as_deref(), Some("canonical-ns"));
            assert_eq!(header.namespaced, Some(true));
            assert_eq!(header.broker_name.as_deref(), Some("canonical-broker"));
            assert_eq!(header.oneway, Some(false));
        }
    }
}
