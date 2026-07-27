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

use std::collections::BTreeSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::AssetSnapshotId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TopologyEdgeId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::ControlPlaneError;

const MAX_ASSETS_PER_SNAPSHOT: usize = 20_000;
const MAX_EDGES_PER_SNAPSHOT: usize = 50_000;
const MAX_ATTRIBUTE_BYTES: usize = 64 * 1024;

/// Asset kinds exposed by the Phase 1 normalized inventory.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AssetKind {
    Cluster,
    NameServer,
    Controller,
    Broker,
    Proxy,
    Store,
    Topic,
    Queue,
    ConsumerGroup,
    ProducerGroup,
    Producer,
    Consumer,
    Connection,
    Client,
    Pod,
    Node,
    PersistentVolumeClaim,
    PodDisruptionBudget,
    ConfigVersion,
}

impl AssetKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::NameServer => "name_server",
            Self::Controller => "controller",
            Self::Broker => "broker",
            Self::Proxy => "proxy",
            Self::Store => "store",
            Self::Topic => "topic",
            Self::Queue => "queue",
            Self::ConsumerGroup => "consumer_group",
            Self::ProducerGroup => "producer_group",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
            Self::Connection => "connection",
            Self::Client => "client",
            Self::Pod => "pod",
            Self::Node => "node",
            Self::PersistentVolumeClaim => "persistent_volume_claim",
            Self::PodDisruptionBudget => "pod_disruption_budget",
            Self::ConfigVersion => "config_version",
        }
    }

    pub(super) const fn dashboard_segment(self) -> &'static str {
        match self {
            Self::Cluster => "clusters",
            Self::NameServer => "nameservers",
            Self::Controller => "controllers",
            Self::Broker => "brokers",
            Self::Proxy => "proxies",
            Self::Store => "stores",
            Self::Topic => "topics",
            Self::Queue => "queues",
            Self::ConsumerGroup => "consumer-groups",
            Self::ProducerGroup => "producer-groups",
            Self::Producer => "producers",
            Self::Consumer => "consumers",
            Self::Connection => "connections",
            Self::Client => "clients",
            Self::Pod => "pods",
            Self::Node => "nodes",
            Self::PersistentVolumeClaim => "persistent-volume-claims",
            Self::PodDisruptionBudget => "pod-disruption-budgets",
            Self::ConfigVersion => "config-versions",
        }
    }
}

impl Display for AssetKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AssetKind {
    type Err = ControlPlaneError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cluster" => Ok(Self::Cluster),
            "name_server" => Ok(Self::NameServer),
            "controller" => Ok(Self::Controller),
            "broker" => Ok(Self::Broker),
            "proxy" => Ok(Self::Proxy),
            "store" => Ok(Self::Store),
            "topic" => Ok(Self::Topic),
            "queue" => Ok(Self::Queue),
            "consumer_group" => Ok(Self::ConsumerGroup),
            "producer_group" => Ok(Self::ProducerGroup),
            "producer" => Ok(Self::Producer),
            "consumer" => Ok(Self::Consumer),
            "connection" => Ok(Self::Connection),
            "client" => Ok(Self::Client),
            "pod" => Ok(Self::Pod),
            "node" => Ok(Self::Node),
            "persistent_volume_claim" => Ok(Self::PersistentVolumeClaim),
            "pod_disruption_budget" => Ok(Self::PodDisruptionBudget),
            "config_version" => Ok(Self::ConfigVersion),
            _ => Err(invalid_stored_inventory("asset kind")),
        }
    }
}

/// Read-only inventory sources accepted from the connector.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AssetSource {
    Mcp,
    Admin,
    Kubernetes,
    Runtime,
    Topology,
}

impl AssetSource {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Mcp => "mcp",
            Self::Admin => "admin",
            Self::Kubernetes => "kubernetes",
            Self::Runtime => "runtime",
            Self::Topology => "topology",
        }
    }
}

impl FromStr for AssetSource {
    type Err = ControlPlaneError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "mcp" => Ok(Self::Mcp),
            "admin" => Ok(Self::Admin),
            "kubernetes" => Ok(Self::Kubernetes),
            "runtime" => Ok(Self::Runtime),
            "topology" => Ok(Self::Topology),
            _ => Err(invalid_stored_inventory("asset source")),
        }
    }
}

/// Stable compound identity within a tenant and cluster.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct AssetKey {
    pub kind: AssetKind,
    pub external_key: String,
}

impl AssetKey {
    pub(crate) fn new(kind: AssetKind, external_key: impl Into<String>) -> Result<Self, ControlPlaneError> {
        let key = Self {
            kind,
            external_key: external_key.into(),
        };
        key.validate()?;
        Ok(key)
    }

    pub(super) fn canonical(&self) -> String {
        format!("{}:{}", self.kind.as_str(), self.external_key)
    }

    pub(super) fn parse_canonical(value: &str) -> Result<Self, ControlPlaneError> {
        let (kind, external_key) = value
            .split_once(':')
            .ok_or_else(|| invalid_stored_inventory("asset key"))?;
        Self::new(kind.parse()?, external_key)
    }

    fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_text("asset external key", &self.external_key, 512)
    }
}

/// Directed relationship between two normalized assets.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopologyRelation {
    Contains,
    RegistersWith,
    ControlledBy,
    RoutesTo,
    StoresOn,
    RunsOn,
    ProducesTo,
    ConsumesFrom,
    ConnectedTo,
    Configures,
}

impl TopologyRelation {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Contains => "contains",
            Self::RegistersWith => "registers_with",
            Self::ControlledBy => "controlled_by",
            Self::RoutesTo => "routes_to",
            Self::StoresOn => "stores_on",
            Self::RunsOn => "runs_on",
            Self::ProducesTo => "produces_to",
            Self::ConsumesFrom => "consumes_from",
            Self::ConnectedTo => "connected_to",
            Self::Configures => "configures",
        }
    }
}

impl FromStr for TopologyRelation {
    type Err = ControlPlaneError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "contains" => Ok(Self::Contains),
            "registers_with" => Ok(Self::RegistersWith),
            "controlled_by" => Ok(Self::ControlledBy),
            "routes_to" => Ok(Self::RoutesTo),
            "stores_on" => Ok(Self::StoresOn),
            "runs_on" => Ok(Self::RunsOn),
            "produces_to" => Ok(Self::ProducesTo),
            "consumes_from" => Ok(Self::ConsumesFrom),
            "connected_to" => Ok(Self::ConnectedTo),
            "configures" => Ok(Self::Configures),
            _ => Err(invalid_stored_inventory("topology relation")),
        }
    }
}

/// Connector-provided asset observation before canonical hashing.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AssetObservation {
    pub kind: AssetKind,
    pub external_key: String,
    pub display_name: String,
    pub source: AssetSource,
    pub attributes: Value,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    #[serde(default)]
    pub partial: bool,
}

impl AssetObservation {
    pub(super) fn validate(&self) -> Result<(), ControlPlaneError> {
        AssetKey::new(self.kind, self.external_key.clone())?;
        validate_text("asset display name", &self.display_name, 512)?;
        validate_attributes(self.kind, &self.attributes)
    }
}

/// Connector-provided topology edge before canonical hashing.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct TopologyObservation {
    pub from: AssetKey,
    pub to: AssetKey,
    pub relation: TopologyRelation,
    pub source: AssetSource,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    #[serde(default)]
    pub partial: bool,
}

/// One coherent, bounded inventory observation.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct IngestInventoryRequest {
    pub cluster_id: ClusterId,
    pub observed_at: DateTime<Utc>,
    #[serde(default)]
    pub partial: bool,
    #[serde(default)]
    pub assets: Vec<AssetObservation>,
    #[serde(default)]
    pub edges: Vec<TopologyObservation>,
}

impl IngestInventoryRequest {
    pub(super) fn validate(&self) -> Result<(), ControlPlaneError> {
        if self.assets.len() > MAX_ASSETS_PER_SNAPSHOT || self.edges.len() > MAX_EDGES_PER_SNAPSHOT {
            return Err(ControlPlaneError::validation(
                "output_too_large",
                "inventory snapshot exceeds the supported asset or edge bound",
            ));
        }
        let mut assets = BTreeSet::new();
        for asset in &self.assets {
            asset.validate()?;
            let key = AssetKey::new(asset.kind, asset.external_key.clone())?;
            if !assets.insert(key) {
                return Err(ControlPlaneError::validation(
                    "invalid_request",
                    "inventory snapshot contains a duplicate asset identity",
                ));
            }
        }
        let mut edges = BTreeSet::new();
        for edge in &self.edges {
            edge.from.validate()?;
            edge.to.validate()?;
            if edge.from == edge.to {
                return Err(ControlPlaneError::validation(
                    "invalid_request",
                    "topology self-edges are not supported",
                ));
            }
            let identity = (edge.from.clone(), edge.to.clone(), edge.relation);
            if !edges.insert(identity) {
                return Err(ControlPlaneError::validation(
                    "invalid_request",
                    "inventory snapshot contains a duplicate topology edge",
                ));
            }
            if !self.partial && (!assets.contains(&edge.from) || !assets.contains(&edge.to)) {
                return Err(ControlPlaneError::validation(
                    "invalid_request",
                    "complete topology snapshots must contain both edge endpoints",
                ));
            }
        }
        Ok(())
    }
}

/// Canonically hashed asset record stored in PostgreSQL.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct NormalizedAsset {
    pub id: AssetSnapshotId,
    pub key: AssetKey,
    pub display_name: String,
    pub source: AssetSource,
    pub attributes: Value,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    pub content_hash: String,
}

/// Canonically hashed topology relationship stored in PostgreSQL.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NormalizedTopologyEdge {
    pub id: TopologyEdgeId,
    pub from: AssetKey,
    pub to: AssetKey,
    pub relation: TopologyRelation,
    pub source: AssetSource,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    pub content_hash: String,
}

/// Persisted point-in-time inventory and topology graph.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct InventorySnapshot {
    pub id: Uuid,
    pub tenant_id: rocketmq_sre_contracts::TenantId,
    pub cluster_id: ClusterId,
    pub sources: Vec<AssetSource>,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    pub content_hash: String,
    pub assets: Vec<NormalizedAsset>,
    pub edges: Vec<NormalizedTopologyEdge>,
}

/// Entity type represented by a topology diff entry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiffEntity {
    Asset,
    Edge,
}

/// Stable, bounded representation of one addition, removal, or change.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TopologyDiffEntry {
    pub entity: DiffEntity,
    pub key: String,
    pub previous_hash: Option<String>,
    pub current_hash: Option<String>,
}

/// Deterministic difference between adjacent inventory snapshots.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct TopologyDiff {
    pub id: Uuid,
    pub tenant_id: rocketmq_sre_contracts::TenantId,
    pub cluster_id: ClusterId,
    pub previous_snapshot_id: Option<Uuid>,
    pub current_snapshot_id: Uuid,
    pub previous_observed_at: Option<DateTime<Utc>>,
    pub current_observed_at: DateTime<Utc>,
    pub additions: Vec<TopologyDiffEntry>,
    pub removals: Vec<TopologyDiffEntry>,
    pub changes: Vec<TopologyDiffEntry>,
    pub partial: bool,
    pub suppressed_removals: u32,
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

/// Bounded inventory list request.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AssetListQuery {
    pub cluster_id: ClusterId,
    pub kind: Option<AssetKind>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl AssetListQuery {
    pub(super) fn bounded_limit(&self) -> Result<u32, ControlPlaneError> {
        let limit = self.limit.unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "asset page limit must be between 1 and 500",
            ));
        }
        Ok(limit)
    }
}

/// One bounded page from the latest inventory snapshot.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AssetPage {
    pub snapshot_id: Option<Uuid>,
    pub observed_at: Option<DateTime<Utc>>,
    pub items: Vec<NormalizedAsset>,
    pub next_cursor: Option<String>,
    pub partial: bool,
}

pub(crate) fn invalid_stored_inventory(field: &str) -> ControlPlaneError {
    ControlPlaneError::validation("source_unavailable", format!("stored inventory {field} is invalid"))
}

fn validate_text(name: &str, value: &str, max_chars: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control) {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            format!("{name} must be non-empty, bounded, and contain no control characters"),
        ));
    }
    Ok(())
}

fn validate_attributes(kind: AssetKind, attributes: &Value) -> Result<(), ControlPlaneError> {
    if !attributes.is_object() {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "asset attributes must be a JSON object",
        ));
    }
    let encoded = serde_json::to_vec(attributes)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "asset attributes cannot be serialized"))?;
    if encoded.len() > MAX_ATTRIBUTE_BYTES {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "asset attributes exceed the supported byte bound",
        ));
    }
    reject_sensitive_attributes(attributes, 0)?;
    if kind == AssetKind::ConfigVersion {
        let digest = attributes.get("digest").and_then(Value::as_str).ok_or_else(|| {
            ControlPlaneError::validation(
                "invalid_request",
                "config version assets require a content digest instead of raw configuration",
            )
        })?;
        if !is_sha256(digest) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "config version digest must use sha256:<hex>",
            ));
        }
    }
    Ok(())
}

fn reject_sensitive_attributes(value: &Value, depth: usize) -> Result<(), ControlPlaneError> {
    if depth > 12 {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "asset attributes exceed the supported nesting depth",
        ));
    }
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                let normalized = key.to_ascii_lowercase().replace('-', "_");
                if is_sensitive_key(&normalized) {
                    return Err(ControlPlaneError::validation(
                        "sensitive_data_rejected",
                        "asset attributes contain a prohibited sensitive field",
                    ));
                }
                reject_sensitive_attributes(value, depth + 1)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_sensitive_attributes(value, depth + 1)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key,
        "password"
            | "secret"
            | "token"
            | "access_key"
            | "secret_key"
            | "private_key"
            | "tls_material"
            | "acl"
            | "message_body"
            | "body"
            | "raw_config"
            | "config_content"
            | "client_ip"
            | "remote_address"
            | "internal_address"
    ) || key.ends_with("_password")
        || key.ends_with("_secret")
        || key.ends_with("_token")
}

fn is_sha256(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const PHASE1_INVENTORY: &str = include_str!("../../../../tests/fixtures/e2e/phase1-inventory.json");

    #[test]
    fn phase_one_fixture_covers_the_required_asset_and_topology_paths() {
        let request: IngestInventoryRequest =
            serde_json::from_str(PHASE1_INVENTORY).expect("Phase 1 inventory fixture");
        request.validate().expect("complete fixture");
        let kinds = request.assets.iter().map(|asset| asset.kind).collect::<BTreeSet<_>>();
        for kind in [
            AssetKind::NameServer,
            AssetKind::Controller,
            AssetKind::Broker,
            AssetKind::Proxy,
            AssetKind::Store,
            AssetKind::Pod,
            AssetKind::Node,
            AssetKind::PersistentVolumeClaim,
            AssetKind::PodDisruptionBudget,
            AssetKind::Topic,
            AssetKind::Queue,
            AssetKind::Producer,
            AssetKind::Consumer,
            AssetKind::Connection,
        ] {
            assert!(kinds.contains(&kind), "{kind} is missing");
        }
        assert!(
            request
                .edges
                .iter()
                .any(|edge| edge.from.kind == AssetKind::Topic && edge.to.kind == AssetKind::Queue)
        );
        assert!(
            request
                .edges
                .iter()
                .any(|edge| edge.from.kind == AssetKind::Connection && edge.to.kind == AssetKind::Broker)
        );
    }

    #[test]
    fn canonical_asset_key_round_trips_external_colons() {
        let key = AssetKey::new(AssetKind::Topic, "tenant:orders").expect("valid key");
        assert_eq!(AssetKey::parse_canonical(&key.canonical()).expect("canonical key"), key);
    }

    #[test]
    fn sensitive_and_raw_config_attributes_are_rejected() {
        let mut asset = AssetObservation {
            kind: AssetKind::Broker,
            external_key: "broker-a".to_owned(),
            display_name: "Broker A".to_owned(),
            source: AssetSource::Admin,
            attributes: json!({"token": "must-not-persist"}),
            observed_at: Utc::now(),
            freshness_seconds: 0,
            partial: false,
        };
        assert!(asset.validate().is_err());

        asset.kind = AssetKind::ConfigVersion;
        asset.attributes = json!({"version": "v1", "raw_config": "hidden"});
        assert!(asset.validate().is_err());
        asset.attributes = json!({"version": "v1", "digest": format!("sha256:{}", "a".repeat(64))});
        assert!(asset.validate().is_ok());
    }

    #[test]
    fn complete_snapshot_requires_edge_endpoints() {
        let request = IngestInventoryRequest {
            cluster_id: ClusterId::new(),
            observed_at: Utc::now(),
            partial: false,
            assets: Vec::new(),
            edges: vec![TopologyObservation {
                from: AssetKey::new(AssetKind::Broker, "a").expect("key"),
                to: AssetKey::new(AssetKind::Topic, "orders").expect("key"),
                relation: TopologyRelation::RoutesTo,
                source: AssetSource::Topology,
                observed_at: Utc::now(),
                freshness_seconds: 0,
                partial: false,
            }],
        };
        assert!(request.validate().is_err());
    }
}
