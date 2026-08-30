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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_sre_contracts::AssetSnapshotId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TopologyEdgeId;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

use super::AssetKey;
use super::AssetObservation;
use super::AssetSource;
use super::DiffEntity;
use super::IngestInventoryRequest;
use super::InventorySnapshot;
use super::NormalizedAsset;
use super::NormalizedTopologyEdge;
use super::TopologyDiff;
use super::TopologyDiffEntry;
use super::TopologyObservation;
use super::TopologyRelation;
use crate::ControlPlaneError;

#[derive(Serialize)]
struct AssetHashMaterial<'a> {
    key: &'a AssetKey,
    display_name: &'a str,
    source: AssetSource,
    attributes: Value,
}

#[derive(Serialize)]
struct EdgeHashMaterial<'a> {
    from: &'a AssetKey,
    to: &'a AssetKey,
    relation: TopologyRelation,
    source: AssetSource,
}

#[derive(Serialize)]
struct SnapshotHashMaterial<'a> {
    cluster_id: rocketmq_sre_contracts::ClusterId,
    assets: Vec<(&'a AssetKey, &'a str)>,
    edges: Vec<(String, &'a str)>,
}

#[derive(Serialize)]
struct DiffHashMaterial<'a> {
    cluster_id: rocketmq_sre_contracts::ClusterId,
    previous_snapshot_id: Option<Uuid>,
    current_snapshot_id: Uuid,
    additions: &'a [TopologyDiffEntry],
    removals: &'a [TopologyDiffEntry],
    changes: &'a [TopologyDiffEntry],
    partial: bool,
    suppressed_removals: u32,
}

pub(crate) fn materialize_snapshot(
    tenant_id: TenantId,
    request: &IngestInventoryRequest,
) -> Result<InventorySnapshot, ControlPlaneError> {
    request.validate()?;
    let mut assets = request
        .assets
        .iter()
        .map(normalize_asset)
        .collect::<Result<Vec<_>, _>>()?;
    assets.sort_by(|left, right| left.key.cmp(&right.key));
    let mut edges = request
        .edges
        .iter()
        .map(normalize_edge)
        .collect::<Result<Vec<_>, _>>()?;
    edges.sort_by(edge_order);
    let sources = assets
        .iter()
        .map(|asset| asset.source)
        .chain(edges.iter().map(|edge| edge.source))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let freshness_seconds = assets
        .iter()
        .map(|asset| asset.freshness_seconds)
        .chain(edges.iter().map(|edge| edge.freshness_seconds))
        .max()
        .unwrap_or_default();
    let partial = request.partial || assets.iter().any(|asset| asset.partial) || edges.iter().any(|edge| edge.partial);
    let mut snapshot = InventorySnapshot {
        id: Uuid::new_v4(),
        tenant_id,
        cluster_id: request.cluster_id,
        sources,
        observed_at: request.observed_at,
        freshness_seconds,
        partial,
        content_hash: String::new(),
        assets,
        edges,
    };
    snapshot.content_hash = compute_snapshot_hash(&snapshot)?;
    Ok(snapshot)
}

pub(crate) fn verify_snapshot(snapshot: &InventorySnapshot) -> Result<(), ControlPlaneError> {
    for asset in &snapshot.assets {
        if asset.content_hash != compute_asset_hash(asset)? {
            return Err(super::invalid_stored_inventory("asset content hash"));
        }
    }
    for edge in &snapshot.edges {
        if edge.content_hash != compute_edge_hash(edge)? {
            return Err(super::invalid_stored_inventory("topology edge content hash"));
        }
    }
    if snapshot.content_hash != compute_snapshot_hash(snapshot)? {
        return Err(super::invalid_stored_inventory("snapshot content hash"));
    }
    Ok(())
}

pub(crate) fn calculate_diff(
    previous: Option<&InventorySnapshot>,
    current: &InventorySnapshot,
) -> Result<TopologyDiff, ControlPlaneError> {
    let previous_assets = previous
        .map(|snapshot| {
            snapshot
                .assets
                .iter()
                .map(|asset| (asset.key.canonical(), asset))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let current_assets = current
        .assets
        .iter()
        .map(|asset| (asset.key.canonical(), asset))
        .collect::<BTreeMap<_, _>>();
    let previous_edges = previous
        .map(|snapshot| {
            snapshot
                .edges
                .iter()
                .map(|edge| (edge_identity(edge), edge))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let current_edges = current
        .edges
        .iter()
        .map(|edge| (edge_identity(edge), edge))
        .collect::<BTreeMap<_, _>>();

    let mut additions = Vec::new();
    let mut removals = Vec::new();
    let mut changes = Vec::new();
    compare_maps(
        DiffEntity::Asset,
        &previous_assets,
        &current_assets,
        |asset| asset.content_hash.as_str(),
        &mut additions,
        &mut removals,
        &mut changes,
    );
    compare_maps(
        DiffEntity::Edge,
        &previous_edges,
        &current_edges,
        |edge| edge.content_hash.as_str(),
        &mut additions,
        &mut removals,
        &mut changes,
    );

    let suppressed_removals = if current.partial {
        let count = u32::try_from(removals.len()).map_err(|_| {
            ControlPlaneError::validation("output_too_large", "topology diff exceeds the supported bound")
        })?;
        removals.clear();
        count
    } else {
        0
    };
    let mut diff = TopologyDiff {
        id: Uuid::new_v4(),
        tenant_id: current.tenant_id,
        cluster_id: current.cluster_id,
        previous_snapshot_id: previous.map(|snapshot| snapshot.id),
        current_snapshot_id: current.id,
        previous_observed_at: previous.map(|snapshot| snapshot.observed_at),
        current_observed_at: current.observed_at,
        additions,
        removals,
        changes,
        partial: current.partial || previous.is_some_and(|snapshot| snapshot.partial),
        suppressed_removals,
        content_hash: String::new(),
        created_at: Utc::now(),
    };
    diff.content_hash = compute_diff_hash(&diff)?;
    Ok(diff)
}

pub(crate) fn verify_diff(diff: &TopologyDiff) -> Result<(), ControlPlaneError> {
    if diff.content_hash != compute_diff_hash(diff)? {
        return Err(super::invalid_stored_inventory("topology diff content hash"));
    }
    Ok(())
}

fn normalize_asset(observation: &AssetObservation) -> Result<NormalizedAsset, ControlPlaneError> {
    let mut asset = NormalizedAsset {
        id: AssetSnapshotId::new(),
        key: AssetKey::new(observation.kind, observation.external_key.clone())?,
        display_name: observation.display_name.trim().to_owned(),
        source: observation.source,
        attributes: sorted_json(&observation.attributes),
        observed_at: observation.observed_at,
        freshness_seconds: observation.freshness_seconds,
        partial: observation.partial,
        content_hash: String::new(),
    };
    asset.content_hash = compute_asset_hash(&asset)?;
    Ok(asset)
}

fn normalize_edge(observation: &TopologyObservation) -> Result<NormalizedTopologyEdge, ControlPlaneError> {
    let mut edge = NormalizedTopologyEdge {
        id: TopologyEdgeId::new(),
        from: observation.from.clone(),
        to: observation.to.clone(),
        relation: observation.relation,
        source: observation.source,
        observed_at: observation.observed_at,
        freshness_seconds: observation.freshness_seconds,
        partial: observation.partial,
        content_hash: String::new(),
    };
    edge.content_hash = compute_edge_hash(&edge)?;
    Ok(edge)
}

fn compute_asset_hash(asset: &NormalizedAsset) -> Result<String, ControlPlaneError> {
    canonical_digest(&AssetHashMaterial {
        key: &asset.key,
        display_name: &asset.display_name,
        source: asset.source,
        attributes: sorted_json(&asset.attributes),
    })
}

fn compute_edge_hash(edge: &NormalizedTopologyEdge) -> Result<String, ControlPlaneError> {
    canonical_digest(&EdgeHashMaterial {
        from: &edge.from,
        to: &edge.to,
        relation: edge.relation,
        source: edge.source,
    })
}

fn compute_snapshot_hash(snapshot: &InventorySnapshot) -> Result<String, ControlPlaneError> {
    let mut assets = snapshot
        .assets
        .iter()
        .map(|asset| (&asset.key, asset.content_hash.as_str()))
        .collect::<Vec<_>>();
    assets.sort_by(|left, right| left.0.cmp(right.0));
    let mut edges = snapshot
        .edges
        .iter()
        .map(|edge| (edge_identity(edge), edge.content_hash.as_str()))
        .collect::<Vec<_>>();
    edges.sort_by(|left, right| left.0.cmp(&right.0));
    canonical_digest(&SnapshotHashMaterial {
        cluster_id: snapshot.cluster_id,
        assets,
        edges,
    })
}

fn compute_diff_hash(diff: &TopologyDiff) -> Result<String, ControlPlaneError> {
    canonical_digest(&DiffHashMaterial {
        cluster_id: diff.cluster_id,
        previous_snapshot_id: diff.previous_snapshot_id,
        current_snapshot_id: diff.current_snapshot_id,
        additions: &diff.additions,
        removals: &diff.removals,
        changes: &diff.changes,
        partial: diff.partial,
        suppressed_removals: diff.suppressed_removals,
    })
}

fn compare_maps<'a, T, F>(
    entity: DiffEntity,
    previous: &BTreeMap<String, &'a T>,
    current: &BTreeMap<String, &'a T>,
    hash: F,
    additions: &mut Vec<TopologyDiffEntry>,
    removals: &mut Vec<TopologyDiffEntry>,
    changes: &mut Vec<TopologyDiffEntry>,
) where
    F: Fn(&T) -> &str,
{
    for (key, current_value) in current {
        match previous.get(key) {
            None => additions.push(TopologyDiffEntry {
                entity,
                key: key.clone(),
                previous_hash: None,
                current_hash: Some(hash(current_value).to_owned()),
            }),
            Some(previous_value) if hash(previous_value) != hash(current_value) => {
                changes.push(TopologyDiffEntry {
                    entity,
                    key: key.clone(),
                    previous_hash: Some(hash(previous_value).to_owned()),
                    current_hash: Some(hash(current_value).to_owned()),
                });
            }
            Some(_) => {}
        }
    }
    for (key, previous_value) in previous {
        if !current.contains_key(key) {
            removals.push(TopologyDiffEntry {
                entity,
                key: key.clone(),
                previous_hash: Some(hash(previous_value).to_owned()),
                current_hash: None,
            });
        }
    }
}

fn edge_identity(edge: &NormalizedTopologyEdge) -> String {
    let from = edge.from.canonical();
    let to = edge.to.canonical();
    format!("{}:{from}|{}|{}:{to}", from.len(), edge.relation.as_str(), to.len())
}

fn edge_order(left: &NormalizedTopologyEdge, right: &NormalizedTopologyEdge) -> std::cmp::Ordering {
    (&left.from, &left.to, left.relation).cmp(&(&right.from, &right.to, right.relation))
}

fn canonical_digest<T: Serialize>(value: &T) -> Result<String, ControlPlaneError> {
    let serialized = serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "inventory content cannot be serialized"))?;
    let bytes = serde_json::to_vec(&sorted_json(&serialized))
        .map_err(|_| ControlPlaneError::validation("invalid_request", "inventory content cannot be canonicalized"))?;
    Ok(format!(
        "sha256:{}",
        rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(bytes))
    ))
}

fn sorted_json(value: &Value) -> Value {
    match value {
        Value::Object(values) => {
            let mut sorted = values.iter().collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.0.cmp(right.0));
            let mut result = Map::new();
            for (key, value) in sorted {
                result.insert(key.clone(), sorted_json(value));
            }
            Value::Object(result)
        }
        Value::Array(values) => Value::Array(values.iter().map(sorted_json).collect()),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use rocketmq_sre_contracts::ClusterId;
    use serde_json::json;

    use super::*;
    use crate::assets::AssetKind;

    fn request(attributes: Value, partial: bool) -> IngestInventoryRequest {
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 27, 8, 0, 0).single().expect("timestamp");
        IngestInventoryRequest {
            cluster_id: ClusterId::new(),
            observed_at,
            partial,
            assets: vec![AssetObservation {
                kind: AssetKind::Broker,
                external_key: "broker-a".to_owned(),
                display_name: "Broker A".to_owned(),
                source: AssetSource::Admin,
                attributes,
                observed_at,
                freshness_seconds: 3,
                partial,
            }],
            edges: Vec::new(),
        }
    }

    #[test]
    fn asset_hash_ignores_json_key_order_and_collection_metadata() {
        let tenant = TenantId::new();
        let first = materialize_snapshot(tenant, &request(json!({"a": 1, "b": 2}), false)).expect("snapshot");
        let mut changed_request = request(json!({"b": 2, "a": 1}), true);
        changed_request.cluster_id = first.cluster_id;
        changed_request.assets[0].freshness_seconds = 90;
        let second = materialize_snapshot(tenant, &changed_request).expect("snapshot");

        assert_eq!(first.assets[0].content_hash, second.assets[0].content_hash);
        assert_ne!(first.id, second.id);
        assert!(second.partial);
        verify_snapshot(&second).expect("hash should verify");
    }

    #[test]
    fn diff_reports_add_remove_change_and_suppresses_partial_removals() {
        let tenant = TenantId::new();
        let mut first_request = request(json!({"role": "master"}), false);
        first_request.assets.push(AssetObservation {
            kind: AssetKind::Topic,
            external_key: "orders".to_owned(),
            display_name: "orders".to_owned(),
            source: AssetSource::Mcp,
            attributes: json!({"queues": 8}),
            observed_at: first_request.observed_at,
            freshness_seconds: 0,
            partial: false,
        });
        let first = materialize_snapshot(tenant, &first_request).expect("first");

        let mut second_request = request(json!({"role": "replica"}), false);
        second_request.cluster_id = first.cluster_id;
        second_request.assets.push(AssetObservation {
            kind: AssetKind::Pod,
            external_key: "broker-a-0".to_owned(),
            display_name: "broker-a-0".to_owned(),
            source: AssetSource::Kubernetes,
            attributes: json!({"phase": "running"}),
            observed_at: second_request.observed_at,
            freshness_seconds: 0,
            partial: false,
        });
        let second = materialize_snapshot(tenant, &second_request).expect("second");
        let diff = calculate_diff(Some(&first), &second).expect("diff");
        assert_eq!(diff.additions.len(), 1);
        assert_eq!(diff.removals.len(), 1);
        assert_eq!(diff.changes.len(), 1);
        verify_diff(&diff).expect("diff hash");

        second_request.partial = true;
        let partial = materialize_snapshot(tenant, &second_request).expect("partial");
        let partial_diff = calculate_diff(Some(&first), &partial).expect("partial diff");
        assert!(partial_diff.removals.is_empty());
        assert_eq!(partial_diff.suppressed_removals, 1);
        assert!(partial_diff.partial);
    }
}
