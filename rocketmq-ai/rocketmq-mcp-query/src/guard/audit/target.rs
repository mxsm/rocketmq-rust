// Copyright 2026 The RocketMQ Rust Authors
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

//! The logical names an audited call asked about.

use rmcp::model::JsonObject;
use serde::Serialize;
use serde_json::Value;

use crate::model::identifier;
use crate::resources::uri::ResourceKind;

/// The most names of one kind that a record holds.
const MAX_TARGET_NAMES: usize = 8;

/// The logical names a Tool call or a Resource read asked about.
///
/// Only a name that passes the check of its kind is held, so a value that is no RocketMQ name,
/// such as an address, a credential, or free text, cannot reach the audit trail through this
/// type. What names a message (its identifier, a key) and what a caller searches with (a filter,
/// a completion prefix) is never held.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AuditTarget {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    producer_group: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    broker_names: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    proxy_name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    controller_names: Vec<String>,
    /// The call named more Brokers or Controllers than a record holds.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
}

impl AuditTarget {
    /// Reads the names out of the arguments of a Tool call.
    ///
    /// The Guard sees the arguments before the input schema is checked, so nothing here relies
    /// on that check: a value of another type, or one that is not a name, is left out.
    pub(crate) fn of_tool_arguments(arguments: &JsonObject) -> Self {
        let (broker_names, more_brokers) = aliases(arguments, &["broker_name", "broker_names"]);
        let (controller_names, more_controllers) = aliases(arguments, &["controller_names"]);
        Self {
            topic: name(arguments, "topic", identifier::is_topic),
            // The configuration state Tool calls the same thing `group`.
            consumer_group: name(arguments, "consumer_group", identifier::is_consumer_group)
                .or_else(|| name(arguments, "group", identifier::is_consumer_group)),
            producer_group: name(arguments, "producer_group", identifier::is_consumer_group),
            broker_names,
            proxy_name: name(arguments, "proxy_name", identifier::is_logical_alias),
            controller_names,
            truncated: more_brokers || more_controllers,
        }
    }

    /// Reads the names out of the address of a Resource.
    pub(crate) fn of_resource(kind: &ResourceKind) -> Self {
        let topic = |topic: &str| checked(topic, identifier::is_topic);
        let group = |group: &str| checked(group, identifier::is_consumer_group);
        match kind {
            ResourceKind::Topic(name)
            | ResourceKind::TopicRoute(name)
            | ResourceKind::TopicStats(name)
            | ResourceKind::TopicConfig(name) => Self {
                topic: topic(name),
                ..Self::default()
            },
            ResourceKind::ConsumerGroup(name) | ResourceKind::ConsumerProgress(name) => Self {
                consumer_group: group(name),
                ..Self::default()
            },
            ResourceKind::ConsumerLag {
                group: group_name,
                topic: topic_name,
            } => Self {
                topic: topic(topic_name),
                consumer_group: group(group_name),
                ..Self::default()
            },
            ResourceKind::Broker(name)
            | ResourceKind::BrokerDiagnostics(name)
            | ResourceKind::BrokerConfigSummary(name) => Self {
                broker_names: checked(name, identifier::is_logical_alias).into_iter().collect(),
                ..Self::default()
            },
            ResourceKind::Capabilities
            | ResourceKind::SystemRuntimeV1
            | ResourceKind::SystemObservabilityV1
            | ResourceKind::Overview
            | ResourceKind::Topics
            | ResourceKind::Brokers
            | ResourceKind::ConsumerGroups => Self::default(),
        }
    }

    /// Whether the call named nothing that a record holds.
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    /// Passes every name through `sanitize`, the way the other fields of a record are written.
    pub(super) fn sanitized(self, sanitize: impl Fn(&str) -> String) -> Self {
        let one = |name: Option<String>| name.as_deref().map(&sanitize);
        let many = |names: Vec<String>| names.iter().map(|name| sanitize(name)).collect();
        Self {
            topic: one(self.topic),
            consumer_group: one(self.consumer_group),
            producer_group: one(self.producer_group),
            broker_names: many(self.broker_names),
            proxy_name: one(self.proxy_name),
            controller_names: many(self.controller_names),
            truncated: self.truncated,
        }
    }
}

/// Collects the logical aliases under `keys`, each of which holds one name or a list of them.
///
/// A name is held once, in the order the arguments give it. The flag says that the arguments
/// hold more names than [`MAX_TARGET_NAMES`]: a record does not grow with a list of arguments.
fn aliases(arguments: &JsonObject, keys: &[&str]) -> (Vec<String>, bool) {
    let mut names = Vec::new();
    for value in keys.iter().filter_map(|key| arguments.get(*key)) {
        let listed = match value {
            Value::Array(values) => values.as_slice(),
            single => std::slice::from_ref(single),
        };
        let listed = listed
            .iter()
            .filter_map(Value::as_str)
            .filter_map(|name| checked(name, identifier::is_logical_alias));
        for name in listed {
            if names.contains(&name) {
                continue;
            }
            if names.len() == MAX_TARGET_NAMES {
                return (names, true);
            }
            names.push(name);
        }
    }
    (names, false)
}

/// The name under `key`, when it is a string that passes `is_name`.
fn name(arguments: &JsonObject, key: &str, is_name: fn(&str) -> bool) -> Option<String> {
    match arguments.get(key) {
        Some(Value::String(value)) => checked(value, is_name),
        _ => None,
    }
}

/// A Tool trims a name before it uses it, so the name is checked the way it would be used.
fn checked(value: &str, is_name: fn(&str) -> bool) -> Option<String> {
    let value = value.trim();
    is_name(value).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(value: Value) -> JsonObject {
        value.as_object().expect("an object of arguments").clone()
    }

    fn written(target: &AuditTarget) -> Value {
        serde_json::to_value(target).expect("an audit target is JSON")
    }

    #[test]
    fn a_tool_call_is_recorded_with_the_names_it_asks_about() {
        let target = AuditTarget::of_tool_arguments(&arguments(serde_json::json!({
            "cluster": "production-a",
            "topic": " orders ",
            "consumer_group": "billing",
            "producer_group": "checkout",
            "broker_name": "broker-a",
            "proxy_name": "proxy-a",
            "controller_names": ["controller-a", "controller-b"],
            "limit": 20,
        })));

        // The cluster has its own field in a record, and a name is written as a Tool uses it.
        assert_eq!(
            written(&target),
            serde_json::json!({
                "topic": "orders",
                "consumer_group": "billing",
                "producer_group": "checkout",
                "broker_names": ["broker-a"],
                "proxy_name": "proxy-a",
                "controller_names": ["controller-a", "controller-b"],
            })
        );

        // The configuration state Tool names its consumer group `group`.
        let target = AuditTarget::of_tool_arguments(&arguments(serde_json::json!({
            "group": "billing",
            "broker_names": ["broker-b", "broker-a", "broker-b"],
        })));
        assert_eq!(
            written(&target),
            serde_json::json!({ "consumer_group": "billing", "broker_names": ["broker-b", "broker-a"] })
        );
    }

    #[test]
    fn what_is_not_a_name_and_what_names_a_message_is_never_recorded() {
        let target = AuditTarget::of_tool_arguments(&arguments(serde_json::json!({
            // Values that are no RocketMQ name.
            "topic": "token=secret",
            "consumer_group": "10.24.7.9:9876",
            "producer_group": "orders/../../etc",
            "broker_name": "10.24.7.9",
            "proxy_name": { "name": "proxy-a" },
            "broker_names": ["broker a", 7, null, "user@host"],
            "controller_names": "controller=1",
            // Values that are never read, whatever they hold.
            "message_id": "7F000001-ABCD",
            "key": "order-4711",
            "filter": "orders",
            "client_alias": "client-1",
            "trace_topic": "RMQ_SYS_TRACE_TOPIC",
            "logger": "RocketmqBroker",
        })));

        assert!(target.is_empty(), "{target:?}");
        assert_eq!(written(&target), serde_json::json!({}));
        assert!(AuditTarget::of_tool_arguments(&JsonObject::new()).is_empty());
    }

    #[test]
    fn a_long_list_of_names_is_cut_and_the_record_says_so() {
        let names = (0..MAX_TARGET_NAMES + 3)
            .map(|index| format!("broker-{index}"))
            .collect::<Vec<_>>();
        let target = AuditTarget::of_tool_arguments(&arguments(serde_json::json!({ "broker_names": names })));

        assert_eq!(
            written(&target),
            serde_json::json!({ "broker_names": names[..MAX_TARGET_NAMES], "truncated": true })
        );

        // As many names as a record holds are all of them: nothing is missing.
        let target = AuditTarget::of_tool_arguments(&arguments(
            serde_json::json!({ "broker_names": names[..MAX_TARGET_NAMES] }),
        ));
        assert_eq!(
            written(&target),
            serde_json::json!({ "broker_names": names[..MAX_TARGET_NAMES] })
        );
    }

    #[test]
    fn a_resource_read_is_recorded_with_the_names_in_its_address() {
        for (kind, expected) in [
            (
                ResourceKind::TopicRoute("orders".to_string()),
                serde_json::json!({ "topic": "orders" }),
            ),
            (
                ResourceKind::ConsumerProgress("billing".to_string()),
                serde_json::json!({ "consumer_group": "billing" }),
            ),
            (
                ResourceKind::ConsumerLag {
                    group: "billing".to_string(),
                    topic: "orders".to_string(),
                },
                serde_json::json!({ "topic": "orders", "consumer_group": "billing" }),
            ),
            (
                ResourceKind::BrokerDiagnostics("broker-a".to_string()),
                serde_json::json!({ "broker_names": ["broker-a"] }),
            ),
            // A list and a system document name nothing.
            (ResourceKind::Topics, serde_json::json!({})),
            (ResourceKind::SystemRuntimeV1, serde_json::json!({})),
            // An address is parsed before it gets here; a name that is none is still left out.
            (ResourceKind::Broker("10.24.7.9".to_string()), serde_json::json!({})),
        ] {
            assert_eq!(written(&AuditTarget::of_resource(&kind)), expected, "{kind:?}");
        }
    }
}
