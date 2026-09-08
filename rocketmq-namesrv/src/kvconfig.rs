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

//! Namespaced key-value configuration storage and persistence.
//!
//! [`kvconfig_mananger::KVConfigManager`] manages configuration values, while
//! [`KVConfigSerializeWrapper`] defines their serialized representation.

use std::collections::HashMap;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

pub mod kvconfig_mananger;
pub(crate) mod persistence;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVConfigSerializeWrapper {
    #[serde(rename = "configTable")]
    pub config_table: Option<
        dashmap::DashMap<
            CheetahString, /* Namespace */
            HashMap<CheetahString /* Key */, CheetahString /* Value */>,
        >,
    >,
}

impl KVConfigSerializeWrapper {
    pub fn new_with_config_table(
        config_table: dashmap::DashMap<CheetahString, HashMap<CheetahString, CheetahString>>,
    ) -> KVConfigSerializeWrapper {
        KVConfigSerializeWrapper {
            config_table: Some(config_table),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type ConfigTable = dashmap::DashMap<CheetahString, HashMap<CheetahString, CheetahString>>;

    fn namespace(entries: &[(&'static str, &'static str)]) -> HashMap<CheetahString, CheetahString> {
        entries
            .iter()
            .map(|(key, value)| {
                (
                    CheetahString::from_static_str(key),
                    CheetahString::from_static_str(value),
                )
            })
            .collect()
    }

    fn sample_config_table() -> ConfigTable {
        let config_table = ConfigTable::new();
        config_table.insert(
            CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
            namespace(&[("orders-eu", "broker-a:4;broker-b:4")]),
        );
        config_table.insert(
            CheetahString::from_static_str("PROJECT_CONFIG"),
            namespace(&[("tenant-a", "project-alpha"), ("tenant-unicode", "生产")]),
        );
        config_table
    }

    /// Order-independent view of the wrapper, mirroring how persistence snapshots the table.
    fn snapshot(wrapper: &KVConfigSerializeWrapper) -> HashMap<CheetahString, HashMap<CheetahString, CheetahString>> {
        wrapper
            .config_table
            .as_ref()
            .expect("wrapper should carry a config table")
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    #[test]
    fn new_with_config_table_stores_the_provided_table() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(sample_config_table());

        let table = snapshot(&wrapper);
        assert_eq!(table.len(), 2);
        assert_eq!(
            table["ORDER_TOPIC_CONFIG"].get("orders-eu"),
            Some(&CheetahString::from_static_str("broker-a:4;broker-b:4"))
        );
        assert_eq!(
            table["PROJECT_CONFIG"].get("tenant-a"),
            Some(&CheetahString::from_static_str("project-alpha"))
        );
    }

    #[test]
    fn serialization_uses_the_java_config_table_field_name() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(sample_config_table());

        let json = serde_json::to_string(&wrapper).expect("wrapper should serialize");

        assert!(json.contains("\"configTable\""), "unexpected JSON: {json}");
        assert!(
            !json.contains("config_table"),
            "Rust field name leaked into JSON: {json}"
        );
    }

    #[test]
    fn round_trip_preserves_namespaces_keys_and_values() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(sample_config_table());

        let json = serde_json::to_string(&wrapper).expect("wrapper should serialize");
        let decoded: KVConfigSerializeWrapper = serde_json::from_str(&json).expect("wrapper should deserialize");

        assert_eq!(snapshot(&decoded), snapshot(&wrapper));
    }

    #[test]
    fn absent_config_table_round_trips_as_null() {
        let wrapper = KVConfigSerializeWrapper { config_table: None };

        let json = serde_json::to_string(&wrapper).expect("wrapper should serialize");
        assert_eq!(json, r#"{"configTable":null}"#);

        let decoded: KVConfigSerializeWrapper = serde_json::from_str(&json).expect("wrapper should deserialize");
        assert!(decoded.config_table.is_none());
    }

    #[test]
    fn empty_config_table_round_trips_as_an_empty_object() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(ConfigTable::new());

        let json = serde_json::to_string(&wrapper).expect("wrapper should serialize");
        assert_eq!(json, r#"{"configTable":{}}"#);

        let decoded: KVConfigSerializeWrapper = serde_json::from_str(&json).expect("wrapper should deserialize");
        assert!(snapshot(&decoded).is_empty());
    }

    #[test]
    fn java_config_table_fixture_decodes_into_the_wrapper() {
        let fixture = include_str!("../tests/fixtures/kv/java-config-table.json");

        let decoded: KVConfigSerializeWrapper = serde_json::from_str(fixture).expect("Java KV fixture should decode");

        let table = snapshot(&decoded);
        assert_eq!(table.len(), 2);
        assert_eq!(
            table["ORDER_TOPIC_CONFIG"].get("orders-eu"),
            Some(&CheetahString::from_static_str("broker-a:4;broker-b:4"))
        );
        assert_eq!(
            table["PROJECT_CONFIG"].get("tenant-a"),
            Some(&CheetahString::from_static_str("project-alpha"))
        );
        assert_eq!(
            table["PROJECT_CONFIG"].get("tenant-unicode"),
            Some(&CheetahString::from_static_str("生产"))
        );
    }
}
