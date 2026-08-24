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

//! Protocol-independent Broker inventory, runtime, and configuration models.

use std::{collections::BTreeMap, fmt};

use serde::{ser::SerializeStruct as _, Deserialize, Serialize};

use crate::{EndpointAvailability, Observed};

/// Request for cluster home page data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHomePageRequest {
    /// Forces the backend to bypass a cached inventory response.
    pub force_refresh: bool,
}

/// Request for broker configuration data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterBrokerConfigRequest {
    /// Concrete Broker address queried by the backend.
    pub broker_addr: String,
}

/// Request for broker status data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterBrokerStatusRequest {
    /// Concrete Broker address queried by the backend.
    pub broker_addr: String,
}

/// Complete Broker identity. Broker name alone is never a unique target.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerIdentity {
    /// Cluster reported by NameServer inventory.
    pub cluster: String,
    /// Logical Broker name.
    pub broker_name: String,
    /// Numeric Broker ID.
    pub broker_id: u64,
    /// Concrete target address.
    pub address: String,
}

/// Encodes every Broker identity field into a collision-safe local History series key.
pub fn broker_history_series_identity(identity: &BrokerIdentity) -> String {
    format!(
        "{}:{}|{}:{}|{}|{}:{}",
        identity.cluster.len(),
        identity.cluster,
        identity.broker_name.len(),
        identity.broker_name,
        identity.broker_id,
        identity.address.len(),
        identity.address
    )
}

/// Broker role derived only from the Admin response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerRole {
    /// Master Broker.
    Master,
    /// Slave Broker.
    Slave,
    /// The response did not provide a recognized role.
    #[default]
    Unknown,
}

impl BrokerRole {
    /// Classifies a provider role without inventing a role for unknown strings.
    pub fn classify(value: &str) -> Self {
        if value.eq_ignore_ascii_case("master") {
            Self::Master
        } else if value.eq_ignore_ascii_case("slave") {
            Self::Slave
        } else {
            Self::Unknown
        }
    }
}

/// A row in the real Broker inventory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInventoryItem {
    /// Collision-safe Broker identity.
    pub identity: BrokerIdentity,
    /// Provider-reported role classification.
    pub role: BrokerRole,
    /// Version is unknown when the runtime request omitted it.
    pub version: Observed<String>,
    /// Availability is never inferred from list membership.
    pub availability: EndpointAvailability,
    /// Produce TPS is observed only when the runtime payload contained its key.
    pub produce_tps: Observed<f64>,
    /// Consume TPS is observed only when the runtime payload contained its key.
    pub consume_tps: Observed<f64>,
}

/// Current Broker metric used by Dashboard ranking and history collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerCurrentMetric {
    /// Collision-safe Broker identity.
    pub identity: BrokerIdentity,
    /// Version reported by runtime data.
    pub version: Observed<String>,
    /// Real reachability state.
    pub availability: EndpointAvailability,
    /// Produce TPS returned by the runtime response.
    pub produce_tps: Observed<f64>,
    /// Consume TPS returned by the runtime response.
    pub consume_tps: Observed<f64>,
    /// Sum only when both component rates were observed.
    pub combined_tps: Observed<f64>,
}

impl BrokerCurrentMetric {
    /// Creates a metric whose runtime values are unproven.
    pub fn unknown(identity: BrokerIdentity, availability: EndpointAvailability) -> Self {
        Self {
            identity,
            version: Observed::Unknown,
            availability,
            produce_tps: Observed::Unknown,
            consume_tps: Observed::Unknown,
            combined_tps: Observed::Unknown,
        }
    }

    /// Creates a metric from values present in a successful runtime response.
    pub fn observed(identity: BrokerIdentity, version: String, produce_tps: f64, consume_tps: f64) -> Self {
        Self {
            identity,
            version: Observed::Observed(version),
            availability: EndpointAvailability::Available,
            produce_tps: Observed::Observed(produce_tps),
            consume_tps: Observed::Observed(consume_tps),
            combined_tps: Observed::Observed(produce_tps + consume_tps),
        }
    }
}

/// Broker list filtering criteria.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrokerInventoryFilter {
    /// Case-insensitive match across identity fields.
    pub keyword: String,
    /// Exact cluster selection.
    pub cluster: Option<String>,
    /// Exact role selection.
    pub role: Option<BrokerRole>,
}

/// Supported local Broker inventory sort orders.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BrokerInventorySort {
    /// Cluster, name, ID, then address.
    #[default]
    Identity,
    /// Broker name, cluster, ID, then address.
    BrokerName,
    /// Role followed by identity.
    Role,
    /// Availability followed by identity.
    Availability,
}

/// Filters, sorts, and returns one bounded page of Broker inventory.
pub fn broker_inventory_page(
    items: &[BrokerInventoryItem],
    filter: &BrokerInventoryFilter,
    sort: BrokerInventorySort,
    page: usize,
    page_size: usize,
) -> Vec<BrokerInventoryItem> {
    let keyword = filter.keyword.trim().to_ascii_lowercase();
    let mut filtered = items
        .iter()
        .filter(|item| {
            (keyword.is_empty()
                || item.identity.cluster.to_ascii_lowercase().contains(&keyword)
                || item.identity.broker_name.to_ascii_lowercase().contains(&keyword)
                || item.identity.address.to_ascii_lowercase().contains(&keyword))
                && filter
                    .cluster
                    .as_deref()
                    .is_none_or(|cluster| item.identity.cluster == cluster)
                && filter.role.is_none_or(|role| item.role == role)
        })
        .cloned()
        .collect::<Vec<_>>();
    filtered.sort_by(|left, right| {
        let identity = || left.identity.cmp(&right.identity);
        match sort {
            BrokerInventorySort::Identity => identity(),
            BrokerInventorySort::BrokerName => left
                .identity
                .broker_name
                .cmp(&right.identity.broker_name)
                .then(left.identity.cmp(&right.identity)),
            BrokerInventorySort::Role => role_rank(left.role)
                .cmp(&role_rank(right.role))
                .then(left.identity.cmp(&right.identity)),
            BrokerInventorySort::Availability => availability_rank(left.availability)
                .cmp(&availability_rank(right.availability))
                .then(left.identity.cmp(&right.identity)),
        }
    });
    let page_size = page_size.max(1);
    filtered
        .into_iter()
        .skip(page.saturating_mul(page_size))
        .take(page_size)
        .collect()
}

/// Returns the number of Brokers that match an inventory filter without applying pagination.
pub fn broker_inventory_count(items: &[BrokerInventoryItem], filter: &BrokerInventoryFilter) -> usize {
    let keyword = filter.keyword.trim().to_ascii_lowercase();
    items
        .iter()
        .filter(|item| {
            (keyword.is_empty()
                || item.identity.cluster.to_ascii_lowercase().contains(&keyword)
                || item.identity.broker_name.to_ascii_lowercase().contains(&keyword)
                || item.identity.address.to_ascii_lowercase().contains(&keyword))
                && filter
                    .cluster
                    .as_deref()
                    .is_none_or(|cluster| item.identity.cluster == cluster)
                && filter.role.is_none_or(|role| item.role == role)
        })
        .count()
}

fn role_rank(role: BrokerRole) -> u8 {
    match role {
        BrokerRole::Master => 0,
        BrokerRole::Slave => 1,
        BrokerRole::Unknown => 2,
    }
}

fn availability_rank(availability: EndpointAvailability) -> u8 {
    match availability {
        EndpointAvailability::Available => 0,
        EndpointAvailability::Unavailable => 1,
        EndpointAvailability::Unknown => 2,
    }
}

/// A runtime key/value with explicit sensitivity and copy policy.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeEntry {
    /// Runtime key.
    pub key: String,
    /// Original value, retained only for rendering or explicit safe copy.
    value: String,
    /// Whether the key is sensitive.
    sensitive: bool,
}

impl Serialize for RuntimeEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RuntimeEntry", 3)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", self.display_value())?;
        state.serialize_field("sensitive", &self.sensitive)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RuntimeEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RuntimeEntryWire {
            key: String,
            value: String,
        }

        let wire = RuntimeEntryWire::deserialize(deserializer)?;
        Ok(Self::new(wire.key, wire.value))
    }
}

impl RuntimeEntry {
    /// Creates an entry and classifies its key.
    pub fn new(key: String, mut value: String) -> Self {
        let sensitive = is_sensitive_key(&key);
        if sensitive {
            value = "<redacted>".into();
        }
        Self { key, value, sensitive }
    }

    /// Returns the safe display value.
    pub fn display_value(&self) -> &str {
        if self.sensitive {
            "<redacted>"
        } else {
            &self.value
        }
    }

    /// Returns a copyable value only for non-sensitive entries.
    pub fn copy_value(&self) -> Option<&str> {
        (!self.sensitive).then_some(self.value.as_str())
    }

    /// Returns whether the key classifier erased the value.
    pub fn is_sensitive(&self) -> bool {
        self.sensitive
    }
}

impl fmt::Debug for RuntimeEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeEntry")
            .field("key", &self.key)
            .field("value", &self.display_value())
            .field("sensitive", &self.sensitive)
            .finish()
    }
}

/// Converts and sorts runtime entries without losing redaction policy.
pub fn runtime_entries(entries: BTreeMap<String, String>) -> Vec<RuntimeEntry> {
    entries
        .into_iter()
        .map(|(key, value)| RuntimeEntry::new(key, value))
        .collect()
}

/// Returns a case-insensitive key-filtered runtime view.
pub fn filter_runtime_entries<'a>(entries: &'a [RuntimeEntry], filter: &str) -> Vec<&'a RuntimeEntry> {
    let filter = filter.trim().to_ascii_lowercase();
    entries
        .iter()
        .filter(|entry| filter.is_empty() || entry.key.to_ascii_lowercase().contains(&filter))
        .collect()
}

/// Erases values for sensitive configuration keys before they cross a UI service boundary.
pub fn redact_sensitive_entries(mut entries: BTreeMap<String, String>) -> BTreeMap<String, String> {
    for (key, value) in &mut entries {
        if is_sensitive_key(key) {
            *value = "<redacted>".into();
        }
    }
    entries
}

/// Broker configuration paired with its generation CAS token.
#[derive(Clone, PartialEq, Eq)]
pub struct BrokerConfigSnapshot {
    /// Collision-safe target.
    pub identity: BrokerIdentity,
    /// Generation returned by the mutation adapter.
    pub generation: u64,
    /// Full response kept behind the application service boundary.
    entries: BTreeMap<String, String>,
}

impl BrokerConfigSnapshot {
    /// Builds a snapshot whose sensitive values are irreversibly erased.
    pub fn new(identity: BrokerIdentity, generation: u64, entries: BTreeMap<String, String>) -> Self {
        Self {
            identity,
            generation,
            entries: redact_sensitive_entries(entries),
        }
    }

    /// Returns the already-redacted configuration entries.
    pub fn entries(&self) -> &BTreeMap<String, String> {
        &self.entries
    }
}

impl Serialize for BrokerConfigSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BrokerConfigSnapshot", 3)?;
        state.serialize_field("identity", &self.identity)?;
        state.serialize_field("generation", &self.generation)?;
        state.serialize_field("entries", &redact_sensitive_entries(self.entries.clone()))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for BrokerConfigSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct BrokerConfigSnapshotWire {
            identity: BrokerIdentity,
            generation: u64,
            entries: BTreeMap<String, String>,
        }

        let wire = BrokerConfigSnapshotWire::deserialize(deserializer)?;
        Ok(Self::new(wire.identity, wire.generation, wire.entries))
    }
}

impl fmt::Debug for BrokerConfigSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerConfigSnapshot")
            .field("identity", &self.identity)
            .field("generation", &self.generation)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

/// One non-sensitive changed configuration key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfigChange {
    /// Changed key.
    pub key: String,
    /// Previously loaded value.
    pub previous_value: String,
    /// Draft replacement value.
    pub next_value: String,
}

impl fmt::Debug for BrokerConfigChange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerConfigChange")
            .field("key", &self.key)
            .finish()
    }
}

/// Generation-aware patch containing only actual non-sensitive changes.
#[derive(Clone, PartialEq, Eq)]
pub struct BrokerConfigPatch {
    /// Collision-safe target.
    pub identity: BrokerIdentity,
    /// Generation observed with the editable snapshot.
    pub expected_generation: u64,
    /// Changed key/value pairs only.
    entries: BTreeMap<String, String>,
}

impl BrokerConfigPatch {
    /// Builds a patch and discards every sensitive key before it crosses a service seam.
    pub fn new(identity: BrokerIdentity, expected_generation: u64, mut entries: BTreeMap<String, String>) -> Self {
        entries.retain(|key, _| !is_sensitive_key(key));
        Self {
            identity,
            expected_generation,
            entries,
        }
    }

    /// Returns the safe non-sensitive changed entries.
    pub fn entries(&self) -> &BTreeMap<String, String> {
        &self.entries
    }

    /// Consumes the patch and returns its safe non-sensitive changed entries.
    pub fn into_entries(self) -> BTreeMap<String, String> {
        self.entries
    }
}

impl Serialize for BrokerConfigPatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BrokerConfigPatch", 3)?;
        state.serialize_field("identity", &self.identity)?;
        state.serialize_field("expectedGeneration", &self.expected_generation)?;
        let entries = self
            .entries
            .iter()
            .filter(|(key, _)| !is_sensitive_key(key))
            .collect::<BTreeMap<_, _>>();
        state.serialize_field("entries", &entries)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for BrokerConfigPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct BrokerConfigPatchWire {
            identity: BrokerIdentity,
            expected_generation: u64,
            entries: BTreeMap<String, String>,
        }

        let wire = BrokerConfigPatchWire::deserialize(deserializer)?;
        Ok(Self::new(wire.identity, wire.expected_generation, wire.entries))
    }
}

impl fmt::Debug for BrokerConfigPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerConfigPatch")
            .field("identity", &self.identity)
            .field("expected_generation", &self.expected_generation)
            .field("keys", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Calculates a sorted diff and excludes every sensitive key.
pub fn broker_config_diff(
    snapshot: &BrokerConfigSnapshot,
    draft: &BTreeMap<String, String>,
) -> Vec<BrokerConfigChange> {
    draft
        .iter()
        .filter(|(key, next)| !is_sensitive_key(key) && snapshot.entries.get(*key) != Some(*next))
        .map(|(key, next_value)| BrokerConfigChange {
            key: key.clone(),
            previous_value: snapshot.entries.get(key).cloned().unwrap_or_default(),
            next_value: next_value.clone(),
        })
        .collect()
}

/// Builds a CAS patch from the reviewed diff.
pub fn broker_config_patch(snapshot: &BrokerConfigSnapshot, diff: &[BrokerConfigChange]) -> BrokerConfigPatch {
    BrokerConfigPatch::new(
        snapshot.identity.clone(),
        snapshot.generation,
        diff.iter()
            .map(|change| (change.key.clone(), change.next_value.clone()))
            .collect(),
    )
}

/// Classifies credential, token, key material, and certificate fields.
pub fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    [
        "password",
        "passwd",
        "secret",
        "token",
        "credential",
        "accesskey",
        "privatekey",
        "certificate",
        "keystore",
        "truststore",
        "tls",
        "ssl",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(name: &str, id: u64) -> BrokerIdentity {
        BrokerIdentity {
            cluster: "cluster-a".into(),
            broker_name: name.into(),
            broker_id: id,
            address: format!("{name}:{id}"),
        }
    }

    fn item(name: &str, id: u64, role: BrokerRole) -> BrokerInventoryItem {
        BrokerInventoryItem {
            identity: identity(name, id),
            role,
            version: Observed::Unknown,
            availability: EndpointAvailability::Unknown,
            produce_tps: Observed::Unknown,
            consume_tps: Observed::Unknown,
        }
    }

    #[test]
    fn existing_request_fields_remain_wire_compatible() {
        assert_eq!(
            serde_json::to_value(ClusterHomePageRequest { force_refresh: true }).expect("serialize"),
            serde_json::json!({ "forceRefresh": true })
        );
        assert_eq!(
            serde_json::to_value(ClusterBrokerConfigRequest {
                broker_addr: "127.0.0.1:10911".into(),
            })
            .expect("serialize"),
            serde_json::json!({ "brokerAddr": "127.0.0.1:10911" })
        );
        assert_eq!(
            serde_json::to_value(ClusterBrokerStatusRequest {
                broker_addr: "127.0.0.1:10911".into(),
            })
            .expect("serialize"),
            serde_json::json!({ "brokerAddr": "127.0.0.1:10911" })
        );
    }

    #[test]
    fn inventory_filter_sort_and_page_preserve_complete_identity() {
        let items = vec![
            item("broker-b", 1, BrokerRole::Slave),
            item("broker-a", 0, BrokerRole::Master),
            item("broker-a", 1, BrokerRole::Slave),
        ];
        let page = broker_inventory_page(
            &items,
            &BrokerInventoryFilter {
                keyword: "broker".into(),
                cluster: Some("cluster-a".into()),
                role: None,
            },
            BrokerInventorySort::BrokerName,
            0,
            2,
        );
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].identity, identity("broker-a", 0));
        assert_eq!(page[1].identity, identity("broker-a", 1));
    }

    #[test]
    fn runtime_redaction_disables_copy_and_debug_output() {
        let secret = RuntimeEntry::new("accessKey".into(), "do-not-leak".into());
        assert!(secret.sensitive);
        assert_eq!(secret.display_value(), "<redacted>");
        assert_eq!(secret.copy_value(), None);
        assert!(!format!("{secret:?}").contains("do-not-leak"));
        assert!(!serde_json::to_string(&secret)
            .expect("serialize redacted runtime")
            .contains("do-not-leak"));

        let normal = RuntimeEntry::new("brokerVersion".into(), "5.3".into());
        assert_eq!(normal.copy_value(), Some("5.3"));
    }

    #[test]
    fn config_diff_contains_only_real_non_sensitive_changes() {
        let snapshot = BrokerConfigSnapshot {
            identity: identity("broker-a", 0),
            generation: 7,
            entries: BTreeMap::from([
                ("flushInterval".into(), "1000".into()),
                ("accessKey".into(), "secret-old".into()),
            ]),
        };
        let draft = BTreeMap::from([
            ("flushInterval".into(), "2000".into()),
            ("accessKey".into(), "secret-new".into()),
        ]);
        let diff = broker_config_diff(&snapshot, &draft);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].key, "flushInterval");
        let patch = broker_config_patch(&snapshot, &diff);
        assert_eq!(patch.expected_generation, 7);
        assert_eq!(patch.entries, BTreeMap::from([("flushInterval".into(), "2000".into())]));
        assert!(!format!("{snapshot:?}{diff:?}{patch:?}").contains("secret"));

        let redacted = redact_sensitive_entries(snapshot.entries.clone());
        assert_eq!(redacted["accessKey"], "<redacted>");
        assert_eq!(redacted["flushInterval"], "1000");
        assert!(!serde_json::to_string(&redacted)
            .expect("serialize redacted config")
            .contains("secret-old"));
    }

    #[test]
    fn history_identity_is_unambiguous_and_contains_every_identity_field() {
        let left = BrokerIdentity {
            cluster: "a".into(),
            broker_name: "bc".into(),
            broker_id: 7,
            address: "127.0.0.1:10911".into(),
        };
        let right = BrokerIdentity {
            cluster: "ab".into(),
            broker_name: "c".into(),
            broker_id: 7,
            address: "127.0.0.1:10911".into(),
        };
        let series = broker_history_series_identity(&left);
        assert_ne!(series, broker_history_series_identity(&right));
        assert!(series.contains("127.0.0.1:10911"));
        assert!(series.contains("|7|"));
    }
}
