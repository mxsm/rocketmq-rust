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

mod client_connections;
mod coverage;
mod k8s;
mod rocketmq;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use self::coverage::InventoryCoverage;
use super::admin_query::AdminQuerySource;
use super::common::CancelSignal;
use super::kubernetes::KubernetesSource;
use super::mcp::McpSource;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::mcp::McpGateway;

const INVENTORY_UPLOAD_MAX_BYTES: usize = 512 * 1024;
const MAX_EDGE_MULTIPLIER: usize = 2;

/// Connector-owned wire representation of the Control Plane inventory ingest
/// contract. It intentionally carries no configuration bodies or addresses.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct InventoryUpload {
    pub cluster_id: ClusterId,
    pub observed_at: DateTime<Utc>,
    pub partial: bool,
    pub assets: Vec<AssetObservation>,
    pub edges: Vec<TopologyObservation>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AssetObservation {
    pub(super) kind: &'static str,
    pub(super) external_key: String,
    pub(super) display_name: String,
    pub(super) source: &'static str,
    pub(super) attributes: Value,
    pub(super) observed_at: DateTime<Utc>,
    pub(super) freshness_seconds: u64,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct AssetKey {
    pub(super) kind: &'static str,
    pub(super) external_key: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TopologyObservation {
    pub(super) from: AssetKey,
    pub(super) to: AssetKey,
    pub(super) relation: &'static str,
    pub(super) source: &'static str,
    pub(super) observed_at: DateTime<Utc>,
    pub(super) freshness_seconds: u64,
    pub(super) partial: bool,
}

type EdgeIdentity = (AssetKey, AssetKey, &'static str);

pub(super) struct InventoryAccumulator {
    cluster_id: ClusterId,
    cluster_key: AssetKey,
    observed_at: DateTime<Utc>,
    partial: bool,
    assets: BTreeMap<AssetKey, AssetObservation>,
    edges: BTreeMap<EdgeIdentity, TopologyObservation>,
    coverage: InventoryCoverage,
}

impl InventoryAccumulator {
    fn new(cluster_id: ClusterId, external_cluster: &str, observed_at: DateTime<Utc>) -> Self {
        let cluster_key = key("cluster", external_cluster);
        let cluster = asset(
            "cluster",
            external_cluster,
            external_cluster,
            "mcp",
            json!({"effective_access": "read_only"}),
            observed_at,
            0,
            false,
        );
        Self {
            cluster_id,
            cluster_key: cluster_key.clone(),
            observed_at,
            partial: false,
            assets: BTreeMap::from([(cluster_key, cluster)]),
            edges: BTreeMap::new(),
            coverage: InventoryCoverage::new(),
        }
    }

    pub(super) fn cluster_key(&self) -> &AssetKey {
        &self.cluster_key
    }

    pub(super) fn mark_source(&mut self, kind: &'static str, partial: bool) {
        self.coverage.mark_source(kind, partial);
        self.partial |= partial;
    }

    pub(super) fn mark_gap(&mut self, kind: &'static str, reason: &'static str) {
        self.coverage.mark_gap(kind, reason);
        self.partial = true;
    }

    pub(super) fn mark_partial(&mut self, reason: &'static str) {
        self.coverage.mark_inventory_gap(reason);
        self.partial = true;
    }

    pub(super) fn merge_cluster_attributes(&mut self, attributes: Value) {
        self.merge_asset_attributes(&self.cluster_key.clone(), attributes);
    }

    pub(super) fn merge_asset_attributes(&mut self, key: &AssetKey, attributes: Value) {
        let Some(asset) = self.assets.get_mut(key) else {
            return;
        };
        let (Some(current), Some(additions)) = (asset.attributes.as_object_mut(), attributes.as_object()) else {
            return;
        };
        for (field, value) in additions {
            current.insert(field.clone(), value.clone());
        }
    }

    pub(super) fn contains_asset(&self, key: &AssetKey) -> bool {
        self.assets.contains_key(key)
    }

    pub(super) fn remaining_asset_capacity(&self, max_rows: usize) -> usize {
        max_rows.saturating_sub(self.assets.len())
    }

    pub(super) fn insert_asset(&mut self, observation: AssetObservation) -> AssetKey {
        let key = key(observation.kind, &observation.external_key);
        let replace = self
            .assets
            .get(&key)
            .is_some_and(|existing| existing.partial && !observation.partial);
        if replace || !self.assets.contains_key(&key) {
            self.assets.insert(key.clone(), observation);
        }
        key
    }

    pub(super) fn insert_edge(&mut self, observation: TopologyObservation) {
        let identity = (observation.from.clone(), observation.to.clone(), observation.relation);
        let replace = self
            .edges
            .get(&identity)
            .is_some_and(|existing| existing.partial && !observation.partial);
        if replace || !self.edges.contains_key(&identity) {
            self.edges.insert(identity, observation);
        }
    }

    fn finish(mut self, max_rows: usize, max_bytes: usize) -> Result<InventoryUpload, ConnectorError> {
        self.coverage
            .mark_gap_if_unqueried("producer", "no_read_only_producer_inventory_source");
        self.coverage
            .mark_gap_if_unqueried("connection", "no_verified_per_client_broker_connection_source");
        let mut upload = InventoryUpload {
            cluster_id: self.cluster_id,
            observed_at: self.observed_at,
            partial: self.partial,
            assets: self.assets.into_values().collect(),
            edges: self.edges.into_values().collect(),
        };
        upload.sort();
        upload.bound(max_rows, max_bytes.min(INVENTORY_UPLOAD_MAX_BYTES), &self.coverage)?;
        Ok(upload)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "inventory observations keep every provenance field explicit"
)]
pub(super) fn asset(
    kind: &'static str,
    external_key: &str,
    display_name: &str,
    source: &'static str,
    attributes: Value,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> AssetObservation {
    AssetObservation {
        kind,
        external_key: external_key.to_owned(),
        display_name: display_name.to_owned(),
        source,
        attributes,
        observed_at,
        freshness_seconds,
        partial,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "topology observations keep every provenance field explicit"
)]
pub(super) fn edge(
    from: AssetKey,
    to: AssetKey,
    relation: &'static str,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> TopologyObservation {
    TopologyObservation {
        from,
        to,
        relation,
        source,
        observed_at,
        freshness_seconds,
        partial,
    }
}

pub(super) fn key(kind: &'static str, external_key: &str) -> AssetKey {
    AssetKey {
        kind,
        external_key: external_key.to_owned(),
    }
}

impl InventoryUpload {
    fn sort(&mut self) {
        self.assets.sort_by(|left, right| {
            asset_kind_order(left.kind)
                .cmp(&asset_kind_order(right.kind))
                .then(left.external_key.cmp(&right.external_key))
        });
        self.edges.sort_by(|left, right| {
            left.from
                .cmp(&right.from)
                .then(left.to.cmp(&right.to))
                .then(left.relation.cmp(right.relation))
        });
    }

    fn bound(&mut self, max_rows: usize, max_bytes: usize, coverage: &InventoryCoverage) -> Result<(), ConnectorError> {
        let asset_limit = max_rows.max(1);
        let edge_limit = max_rows.saturating_mul(MAX_EDGE_MULTIPLIER).max(1);
        let mut bounded = false;
        if self.assets.len() > asset_limit {
            self.assets.truncate(asset_limit);
            self.partial = true;
            bounded = true;
            self.retain_observed_edges();
        }
        if self.edges.len() > edge_limit {
            self.edges.truncate(edge_limit);
            self.partial = true;
            bounded = true;
        }
        self.update_coverage(coverage, bounded);

        while serde_json::to_vec(self).map_err(|_| schema_mismatch())?.len() > max_bytes {
            if !self.edges.is_empty() {
                self.edges.pop();
            } else if self.assets.len() > 1 {
                self.assets.pop();
                self.retain_observed_edges();
            } else {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::OutputTooLarge,
                    false,
                    "minimum inventory upload exceeds the configured byte bound",
                ));
            }
            self.partial = true;
            bounded = true;
            self.update_coverage(coverage, bounded);
        }
        Ok(())
    }

    fn retain_observed_edges(&mut self) {
        let retained = self
            .assets
            .iter()
            .map(|asset| (asset.kind, asset.external_key.clone()))
            .collect::<BTreeSet<_>>();
        self.edges.retain(|edge| {
            retained.contains(&(edge.from.kind, edge.from.external_key.clone()))
                && retained.contains(&(edge.to.kind, edge.to.external_key.clone()))
        });
    }

    fn update_coverage(&mut self, coverage: &InventoryCoverage, bounded: bool) {
        let (coverage_value, has_gap) = coverage.render(&self.assets, &self.edges, bounded);
        self.partial |= has_gap;
        if let Some(cluster) = self.assets.first_mut()
            && let Some(attributes) = cluster.attributes.as_object_mut()
        {
            attributes.insert("inventory_coverage".to_owned(), coverage_value);
        }
    }
}

fn asset_kind_order(kind: &str) -> u8 {
    match kind {
        "cluster" => 0,
        "name_server" => 1,
        "controller" => 2,
        "broker" => 3,
        "proxy" => 4,
        "store" => 5,
        "topic" => 6,
        "queue" => 7,
        "producer" => 8,
        "consumer" => 9,
        "connection" => 10,
        "pod" => 11,
        "node" => 12,
        "persistent_volume_claim" => 13,
        "pod_disruption_budget" => 14,
        _ => u8::MAX,
    }
}

pub(super) fn validate_inventory_name(value: &str) -> Result<(), ConnectorError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(schema_mismatch());
    }
    Ok(())
}

pub(super) fn schema_mismatch() -> ConnectorError {
    ConnectorError::capability(
        ConnectorErrorCode::CapabilityMismatch,
        "inventory source response does not match the supported read-only schema",
    )
}

pub(super) fn recoverable_gap(error: &ConnectorError) -> bool {
    matches!(
        error.code,
        ConnectorErrorCode::SourceUnavailable
            | ConnectorErrorCode::MissingRequiredFeature
            | ConnectorErrorCode::DeadlineExceeded
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "inventory collection keeps all source, security, and resource bounds explicit"
)]
pub(super) async fn collect<G>(
    mcp: &McpSource<G>,
    admin: &AdminQuerySource,
    kubernetes: &KubernetesSource,
    cluster_id: ClusterId,
    external_cluster: &str,
    max_rows: usize,
    max_bytes: usize,
    pseudonymization_key: &[u8],
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
) -> Result<InventoryUpload, ConnectorError>
where
    G: McpGateway,
{
    let observed_at = Utc::now();
    let mut inventory = InventoryAccumulator::new(cluster_id, external_cluster, observed_at);
    let consumer_groups = rocketmq::collect(&mut inventory, mcp, admin, external_cluster, max_rows, deadline, cancel)
        .await
        .inspect_err(|error| log_collection_error("rocketmq", "collect", error))?;
    client_connections::collect(
        &mut inventory,
        admin,
        external_cluster,
        &consumer_groups,
        max_rows,
        pseudonymization_key,
        deadline,
        cancel,
    )
    .await
    .inspect_err(|error| log_collection_error("rocketmq", "client_connections", error))?;
    k8s::collect(
        &mut inventory,
        kubernetes,
        external_cluster,
        max_rows,
        max_bytes,
        pseudonymization_key,
        deadline,
        cancel,
    )
    .await
    .inspect_err(|error| log_collection_error("kubernetes", "collect", error))?;
    inventory
        .finish(max_rows, max_bytes)
        .inspect_err(|error| log_collection_error("inventory", "bound", error))
}

fn log_collection_error(source: &'static str, stage: &'static str, error: &ConnectorError) {
    tracing::warn!(
        source,
        stage,
        code = error.code.as_str(),
        retryable = error.retryable,
        "read-only inventory collection stage failed"
    );
}

#[cfg(test)]
mod tests;
