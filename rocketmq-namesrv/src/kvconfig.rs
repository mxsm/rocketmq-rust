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
    use rocketmq_protocol::protocol::RemotingSerializable;

    use super::*;

    type ConfigTable = dashmap::DashMap<CheetahString, HashMap<CheetahString, CheetahString>>;

    fn namespace_entries(entries: &[(&'static str, &'static str)]) -> HashMap<CheetahString, CheetahString> {
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

    fn sample_table() -> ConfigTable {
        let table = ConfigTable::new();
        table.insert(
            CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
            namespace_entries(&[("topic-a", "broker-a:4"), ("topic-b", "broker-b:8")]),
        );
        table.insert(
            CheetahString::from_static_str("PROJECT_CONFIG"),
            namespace_entries(&[("tenant-a", "project-alpha"), ("tenant-unicode", "café")]),
        );
        table
    }

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
    fn new_with_config_table_stores_all_namespaces_and_keys() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(sample_table());
        let table = wrapper
            .config_table
            .expect("constructor should store the provided table");

        assert_eq!(table.len(), 2);
        let order_namespace = table
            .get("ORDER_TOPIC_CONFIG")
            .expect("ORDER_TOPIC_CONFIG namespace should be stored");
        assert_eq!(order_namespace.len(), 2);
        assert_eq!(order_namespace.get("topic-a").cloned().as_deref(), Some("broker-a:4"));
        assert_eq!(order_namespace.get("topic-b").cloned().as_deref(), Some("broker-b:8"));
        let project_namespace = table
            .get("PROJECT_CONFIG")
            .expect("PROJECT_CONFIG namespace should be stored");
        assert_eq!(
            project_namespace.get("tenant-a").cloned().as_deref(),
            Some("project-alpha")
        );
    }

    #[test]
    fn serialized_json_uses_the_config_table_rename() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(sample_table());
        let serialized = serde_json::to_string(&wrapper).expect("wrapper should serialize to JSON");

        assert!(
            serialized.contains("\"configTable\""),
            "serialized wrapper must use the persisted `configTable` field name"
        );
        assert!(
            !serialized.contains("config_table"),
            "serialized wrapper must never leak the Rust-side `config_table` name"
        );
        let value: serde_json::Value =
            serde_json::from_str(&serialized).expect("serialized wrapper should be valid JSON");
        let object = value.as_object().expect("wrapper should serialize to a JSON object");
        assert_eq!(object.len(), 1);
        assert_eq!(
            object
                .get("configTable")
                .and_then(serde_json::Value::as_object)
                .map(serde_json::Map::len),
            Some(2)
        );
    }

    #[test]
    fn round_trip_preserves_namespaces_keys_and_values() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(sample_table());
        let serialized = serde_json::to_string(&wrapper).expect("wrapper should serialize to JSON");
        let deserialized: KVConfigSerializeWrapper =
            serde_json::from_str(&serialized).expect("wrapper should deserialize from its JSON");
        assert_eq!(snapshot(&deserialized), snapshot(&wrapper));
        let table = deserialized
            .config_table
            .expect("round trip should keep the config table");

        assert_eq!(table.len(), 2);
        assert_eq!(
            table.get("ORDER_TOPIC_CONFIG").map(|namespace| namespace.len()),
            Some(2)
        );
        assert_eq!(
            table
                .get("ORDER_TOPIC_CONFIG")
                .and_then(|namespace| namespace.get("topic-a").cloned())
                .as_deref(),
            Some("broker-a:4")
        );
        assert_eq!(
            table
                .get("ORDER_TOPIC_CONFIG")
                .and_then(|namespace| namespace.get("topic-b").cloned())
                .as_deref(),
            Some("broker-b:8")
        );
        assert_eq!(
            table
                .get("PROJECT_CONFIG")
                .and_then(|namespace| namespace.get("tenant-a").cloned())
                .as_deref(),
            Some("project-alpha")
        );
    }

    #[test]
    fn none_table_serializes_as_null_and_round_trips() {
        let wrapper = KVConfigSerializeWrapper { config_table: None };
        let serialized = serde_json::to_string(&wrapper).expect("wrapper without a table should serialize to JSON");

        assert_eq!(serialized, "{\"configTable\":null}");

        let deserialized: KVConfigSerializeWrapper =
            serde_json::from_str(&serialized).expect("a null config table should deserialize");
        assert!(deserialized.config_table.is_none());
    }

    #[test]
    fn empty_table_serializes_as_an_empty_object_and_round_trips() {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(ConfigTable::new());
        let serialized = serde_json::to_string(&wrapper).expect("an empty table should serialize to JSON");

        assert_eq!(serialized, "{\"configTable\":{}}");

        let deserialized: KVConfigSerializeWrapper =
            serde_json::from_str(&serialized).expect("an empty table should deserialize from its JSON");
        let table = deserialized
            .config_table
            .expect("an empty table should remain present after the round trip");
        assert!(table.is_empty());
    }

    #[test]
    fn pretty_persistence_format_matches_kv_config_json_layout() {
        let table = ConfigTable::new();
        table.insert(
            CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
            namespace_entries(&[("orders-eu", "broker-a:4")]),
        );
        let persisted = KVConfigSerializeWrapper::new_with_config_table(table)
            .serialize_json_pretty()
            .expect("persistence path should pretty print the wrapper");

        assert_eq!(
            persisted,
            r#"{
  "configTable": {
    "ORDER_TOPIC_CONFIG": {
      "orders-eu": "broker-a:4"
    }
  }
}"#
        );

        let reloaded: KVConfigSerializeWrapper =
            serde_json::from_str(&persisted).expect("persisted pretty JSON should reload");
        assert_eq!(
            reloaded
                .config_table
                .expect("persisted pretty JSON should keep the table")
                .get("ORDER_TOPIC_CONFIG")
                .and_then(|namespace| namespace.get("orders-eu").cloned())
                .as_deref(),
            Some("broker-a:4")
        );
    }

    #[test]
    fn java_config_table_fixture_deserializes_into_namespaces() {
        let fixture = include_str!("../tests/fixtures/kv/java-config-table.json");
        let wrapper: KVConfigSerializeWrapper =
            serde_json::from_str(fixture).expect("Java KV fixture should deserialize");

        let table = wrapper
            .config_table
            .expect("Java KV fixture should carry a config table");
        assert_eq!(table.len(), 2);
        assert_eq!(
            table
                .get("ORDER_TOPIC_CONFIG")
                .and_then(|namespace| namespace.get("orders-eu").cloned())
                .as_deref(),
            Some("broker-a:4;broker-b:4")
        );
        assert_eq!(
            table
                .get("PROJECT_CONFIG")
                .and_then(|namespace| namespace.get("tenant-a").cloned())
                .as_deref(),
            Some("project-alpha")
        );
        assert_eq!(
            table
                .get("PROJECT_CONFIG")
                .and_then(|namespace| namespace.get("tenant-unicode").cloned())
                .as_deref(),
            Some("\u{751f}\u{4ea7}")
        );
    }
}
