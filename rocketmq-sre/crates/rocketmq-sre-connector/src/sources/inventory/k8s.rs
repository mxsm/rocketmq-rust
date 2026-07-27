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

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::AssetKey;
use super::InventoryAccumulator;
use super::asset;
use super::edge;
use super::key;
use super::recoverable_gap;
use super::schema_mismatch;
use super::validate_inventory_name;
use crate::ConnectorError;
use crate::sources::common::CancelSignal;
use crate::sources::common::SourceOutput;
use crate::sources::common::pseudonymize_identifier;
use crate::sources::kubernetes::KubernetesSource;

const SERVICE_LABEL: &str = "rocketmq.apache.org/service";
const BROKER_NAME_LABEL: &str = "rocketmq.apache.org/broker-name";

#[derive(Deserialize)]
struct KubernetesWire<T> {
    kind: String,
    namespace: String,
    items: Vec<T>,
}

#[derive(Deserialize)]
struct KubernetesPodWire {
    name: Option<String>,
    namespace: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    phase: Option<String>,
    node_name: Option<String>,
    #[serde(default)]
    persistent_volume_claims: Vec<String>,
    #[serde(default)]
    containers: Vec<KubernetesContainerWire>,
}

#[derive(Deserialize)]
struct KubernetesContainerWire {
    ready: Option<bool>,
    restart_count: Option<u64>,
}

#[derive(Deserialize)]
struct KubernetesNodeWire {
    name: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    unschedulable: Option<bool>,
    pod_capacity: Option<Value>,
    pod_allocatable: Option<Value>,
    #[serde(default)]
    conditions: Vec<Value>,
}

#[derive(Deserialize)]
struct KubernetesPvcWire {
    name: Option<String>,
    namespace: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    phase: Option<String>,
    storage_class: Option<String>,
    #[serde(default)]
    access_modes: Vec<String>,
    requested_storage: Option<Value>,
    capacity_storage: Option<Value>,
}

#[derive(Deserialize)]
struct KubernetesPdbWire {
    name: Option<String>,
    namespace: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    selector_match_labels: BTreeMap<String, String>,
    selector_has_match_expressions: bool,
    min_available: Option<Value>,
    max_unavailable: Option<Value>,
    current_healthy: Option<u64>,
    desired_healthy: Option<u64>,
    expected_pods: Option<u64>,
    disruptions_allowed: Option<u64>,
}

#[derive(Clone)]
struct PodRecord {
    key: AssetKey,
    labels: BTreeMap<String, String>,
}

pub(super) async fn collect(
    inventory: &mut InventoryAccumulator,
    kubernetes: &KubernetesSource,
    cluster: &str,
    max_rows: usize,
    max_bytes: usize,
    pseudonymization_key: &[u8],
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
) -> Result<(), ConnectorError> {
    if !kubernetes.configured() {
        for (kind, reason) in [
            ("name_server", "kubernetes_component_inventory_not_configured"),
            ("controller", "kubernetes_component_inventory_not_configured"),
            ("proxy", "kubernetes_component_inventory_not_configured"),
            ("pod", "kubernetes_inventory_not_configured"),
            ("node", "kubernetes_inventory_not_configured"),
            ("persistent_volume_claim", "kubernetes_inventory_not_configured"),
            ("pod_disruption_budget", "kubernetes_inventory_not_configured"),
        ] {
            inventory.mark_gap(kind, reason);
        }
        return Ok(());
    }

    let pods = match query(kubernetes, cluster, "pods", max_rows, max_bytes, deadline, cancel).await {
        Ok(output) => add_pods(inventory, output, pseudonymization_key)?,
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("pod", "kubernetes_pod_inventory_unavailable");
            inventory.mark_partial("kubernetes_pod_query_incomplete");
            Vec::new()
        }
        Err(error) => return Err(error),
    };

    match query(kubernetes, cluster, "nodes", max_rows, max_bytes, deadline, cancel).await {
        Ok(output) => add_nodes(inventory, output, pseudonymization_key)?,
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("node", "kubernetes_node_inventory_unavailable");
            inventory.mark_partial("kubernetes_node_query_incomplete");
        }
        Err(error) => return Err(error),
    }

    match query(
        kubernetes,
        cluster,
        "persistentvolumeclaims",
        max_rows,
        max_bytes,
        deadline,
        cancel,
    )
    .await
    {
        Ok(output) => add_pvcs(inventory, output)?,
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("persistent_volume_claim", "kubernetes_pvc_inventory_unavailable");
            inventory.mark_partial("kubernetes_pvc_query_incomplete");
        }
        Err(error) => return Err(error),
    }

    match query(
        kubernetes,
        cluster,
        "poddisruptionbudgets",
        max_rows,
        max_bytes,
        deadline,
        cancel,
    )
    .await
    {
        Ok(output) => add_pdbs(inventory, output, &pods)?,
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("pod_disruption_budget", "kubernetes_pdb_inventory_unavailable");
            inventory.mark_partial("kubernetes_pdb_query_incomplete");
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

async fn query(
    kubernetes: &KubernetesSource,
    cluster: &str,
    resource: &str,
    max_rows: usize,
    max_bytes: usize,
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
) -> Result<SourceOutput, ConnectorError> {
    kubernetes
        .query(cluster, resource, max_rows, max_bytes, deadline, cancel)
        .await
}

fn add_pods(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    pseudonymization_key: &[u8],
) -> Result<Vec<PodRecord>, ConnectorError> {
    let pods: KubernetesWire<KubernetesPodWire> = decode(output.content)?;
    if pods.kind != "pods" {
        return Err(schema_mismatch());
    }
    let mut identities = BTreeSet::new();
    let mut records = Vec::new();
    for pod in pods.items {
        let Some(name) = pod.name.filter(|name| !name.is_empty()) else {
            inventory.mark_partial("kubernetes_pod_identity_missing");
            continue;
        };
        let namespace = pod.namespace.unwrap_or_else(|| pods.namespace.clone());
        validate_inventory_name(&name)?;
        validate_inventory_name(&namespace)?;
        let external_key = format!("{namespace}/{name}");
        if !identities.insert(external_key.clone()) {
            return Err(schema_mismatch());
        }
        let ready_containers = pod
            .containers
            .iter()
            .filter(|container| container.ready == Some(true))
            .count();
        let unknown_containers = pod
            .containers
            .iter()
            .filter(|container| container.ready.is_none())
            .count();
        let restart_count = pod
            .containers
            .iter()
            .filter_map(|container| container.restart_count)
            .fold(0_u64, u64::saturating_add);
        let pod_partial = output.partial || pod.phase.is_none() || unknown_containers > 0;
        let node_ref = pod
            .node_name
            .as_deref()
            .map(|node| normalized_node_identity(node, pseudonymization_key));
        let pod_key = inventory.insert_asset(asset(
            "pod",
            &external_key,
            &name,
            "kubernetes",
            json!({
                "namespace": namespace.clone(),
                "labels": pod.labels.clone(),
                "phase": pod.phase,
                "container_count": pod.containers.len(),
                "ready_container_count": ready_containers,
                "unknown_container_readiness_count": unknown_containers,
                "restart_count": restart_count,
                "node_ref": node_ref.clone(),
                "persistent_volume_claim_count": pod.persistent_volume_claims.len(),
            }),
            output.observed_at,
            output.freshness_seconds,
            pod_partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            pod_key.clone(),
            "contains",
            "kubernetes",
            output.observed_at,
            output.freshness_seconds,
            pod_partial,
        ));
        inventory.mark_source("pod", pod_partial);

        if let Some(node_ref) = node_ref {
            let node_key = inventory.insert_asset(asset(
                "node",
                &node_ref,
                &node_ref,
                "kubernetes",
                json!({
                    "identity": node_ref.clone(),
                    "observed_via": "pod_scheduling",
                }),
                output.observed_at,
                output.freshness_seconds,
                true,
            ));
            inventory.insert_edge(edge(
                pod_key.clone(),
                node_key,
                "runs_on",
                "kubernetes",
                output.observed_at,
                output.freshness_seconds,
                true,
            ));
            inventory.mark_source("node", true);
        }

        let component = pod
            .labels
            .get(SERVICE_LABEL)
            .and_then(|service| component_kind(service));
        let store_key = if let Some(kind) = component {
            let component_partial = pod_partial || (kind == "broker" && !pod.labels.contains_key(BROKER_NAME_LABEL));
            inventory.mark_source(kind, component_partial);
            add_component(
                inventory,
                kind,
                &namespace,
                &name,
                &pod.labels,
                &pod_key,
                output.observed_at,
                output.freshness_seconds,
                pod_partial,
            )
        } else {
            None
        };

        for claim in &pod.persistent_volume_claims {
            validate_inventory_name(claim)?;
            let claim_external_key = format!("{namespace}/{claim}");
            let claim_key = inventory.insert_asset(asset(
                "persistent_volume_claim",
                &claim_external_key,
                claim,
                "kubernetes",
                json!({
                    "namespace": namespace,
                    "observed_via": "pod_volume",
                }),
                output.observed_at,
                output.freshness_seconds,
                true,
            ));
            inventory.insert_edge(edge(
                pod_key.clone(),
                claim_key.clone(),
                "stores_on",
                "kubernetes",
                output.observed_at,
                output.freshness_seconds,
                true,
            ));
            if let Some(store_key) = &store_key {
                inventory.insert_edge(edge(
                    store_key.clone(),
                    claim_key,
                    "stores_on",
                    "kubernetes",
                    output.observed_at,
                    output.freshness_seconds,
                    pod_partial,
                ));
            }
            inventory.mark_source("persistent_volume_claim", true);
        }
        records.push(PodRecord {
            key: pod_key,
            labels: pod.labels,
        });
    }
    if records.is_empty() {
        inventory.mark_source("pod", output.partial);
    }
    Ok(records)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Kubernetes component observations retain explicit provenance and freshness"
)]
fn add_component(
    inventory: &mut InventoryAccumulator,
    kind: &'static str,
    namespace: &str,
    pod_name: &str,
    labels: &BTreeMap<String, String>,
    pod_key: &AssetKey,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> Option<AssetKey> {
    let explicit_broker_name = (kind == "broker")
        .then(|| labels.get(BROKER_NAME_LABEL))
        .flatten()
        .filter(|name| !name.is_empty());
    let component_external_key = explicit_broker_name
        .cloned()
        .unwrap_or_else(|| format!("kubernetes/{namespace}/{pod_name}"));
    let identity_partial = partial || (kind == "broker" && explicit_broker_name.is_none());
    let component_key = key(kind, &component_external_key);
    if !inventory.contains_asset(&component_key) {
        inventory.insert_asset(asset(
            kind,
            &component_external_key,
            pod_name,
            "kubernetes",
            json!({
                "namespace": namespace,
                "pod_name": pod_name,
                "service": labels.get(SERVICE_LABEL),
                "identity_scope": if explicit_broker_name.is_some() {
                    "rocketmq_logical_name"
                } else {
                    "kubernetes_pod"
                },
            }),
            observed_at,
            freshness_seconds,
            identity_partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            component_key.clone(),
            "contains",
            "kubernetes",
            observed_at,
            freshness_seconds,
            identity_partial,
        ));
    }
    inventory.insert_edge(edge(
        component_key.clone(),
        pod_key.clone(),
        "runs_on",
        "kubernetes",
        observed_at,
        freshness_seconds,
        identity_partial,
    ));

    if kind != "broker" {
        return (kind == "store").then_some(component_key);
    }
    let store_external_key = explicit_broker_name
        .map(|broker_name| format!("broker/{broker_name}"))
        .unwrap_or_else(|| format!("kubernetes/{namespace}/{pod_name}"));
    let store_key = key("store", &store_external_key);
    if !inventory.contains_asset(&store_key) {
        inventory.insert_asset(asset(
            "store",
            &store_external_key,
            &format!("{pod_name} store"),
            "kubernetes",
            json!({
                "namespace": namespace,
                "pod_name": pod_name,
                "implementation": "broker_embedded_store",
                "identity_scope": if explicit_broker_name.is_some() {
                    "rocketmq_logical_name"
                } else {
                    "kubernetes_pod"
                },
            }),
            observed_at,
            freshness_seconds,
            identity_partial,
        ));
    }
    inventory.insert_edge(edge(
        component_key,
        store_key.clone(),
        "stores_on",
        "kubernetes",
        observed_at,
        freshness_seconds,
        identity_partial,
    ));
    inventory.insert_edge(edge(
        store_key.clone(),
        pod_key.clone(),
        "runs_on",
        "kubernetes",
        observed_at,
        freshness_seconds,
        identity_partial,
    ));
    inventory.mark_source("store", identity_partial);
    Some(store_key)
}

fn component_kind(service: &str) -> Option<&'static str> {
    match service.to_ascii_lowercase().as_str() {
        "namesrv" | "name-server" | "nameserver" => Some("name_server"),
        "controller" => Some("controller"),
        "broker" => Some("broker"),
        "proxy" => Some("proxy"),
        "store" => Some("store"),
        _ => None,
    }
}

fn add_nodes(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    pseudonymization_key: &[u8],
) -> Result<(), ConnectorError> {
    let nodes: KubernetesWire<KubernetesNodeWire> = decode(output.content)?;
    if nodes.kind != "nodes" {
        return Err(schema_mismatch());
    }
    let mut identities = BTreeSet::new();
    for node in nodes.items {
        let Some(name) = node.name.filter(|name| !name.is_empty()) else {
            inventory.mark_partial("kubernetes_node_identity_missing");
            continue;
        };
        validate_inventory_name(&name)?;
        let pseudonym = normalized_node_identity(&name, pseudonymization_key);
        if !identities.insert(pseudonym.clone()) {
            return Err(schema_mismatch());
        }
        inventory.insert_asset(asset(
            "node",
            &pseudonym,
            &pseudonym,
            "kubernetes",
            json!({
                "identity": pseudonym.clone(),
                "labels": node.labels,
                "unschedulable": node.unschedulable,
                "pod_capacity": node.pod_capacity,
                "pod_allocatable": node.pod_allocatable,
                "conditions": node.conditions,
            }),
            output.observed_at,
            output.freshness_seconds,
            output.partial,
        ));
    }
    inventory.mark_source("node", output.partial);
    Ok(())
}

fn add_pvcs(inventory: &mut InventoryAccumulator, output: SourceOutput) -> Result<(), ConnectorError> {
    let claims: KubernetesWire<KubernetesPvcWire> = decode(output.content)?;
    if claims.kind != "persistent_volume_claims" {
        return Err(schema_mismatch());
    }
    let mut identities = BTreeSet::new();
    for claim in claims.items {
        let Some(name) = claim.name.filter(|name| !name.is_empty()) else {
            inventory.mark_partial("kubernetes_pvc_identity_missing");
            continue;
        };
        let namespace = claim.namespace.unwrap_or_else(|| claims.namespace.clone());
        validate_inventory_name(&name)?;
        validate_inventory_name(&namespace)?;
        let external_key = format!("{namespace}/{name}");
        if !identities.insert(external_key.clone()) {
            return Err(schema_mismatch());
        }
        inventory.insert_asset(asset(
            "persistent_volume_claim",
            &external_key,
            &name,
            "kubernetes",
            json!({
                "namespace": namespace,
                "labels": claim.labels,
                "phase": claim.phase,
                "storage_class": claim.storage_class,
                "access_modes": claim.access_modes,
                "requested_storage": claim.requested_storage,
                "capacity_storage": claim.capacity_storage,
            }),
            output.observed_at,
            output.freshness_seconds,
            output.partial,
        ));
    }
    inventory.mark_source("persistent_volume_claim", output.partial);
    Ok(())
}

fn add_pdbs(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    pods: &[PodRecord],
) -> Result<(), ConnectorError> {
    let budgets: KubernetesWire<KubernetesPdbWire> = decode(output.content)?;
    if budgets.kind != "pod_disruption_budgets" {
        return Err(schema_mismatch());
    }
    let mut identities = BTreeSet::new();
    let mut any_partial = output.partial;
    for budget in budgets.items {
        let Some(name) = budget.name.filter(|name| !name.is_empty()) else {
            inventory.mark_partial("kubernetes_pdb_identity_missing");
            continue;
        };
        let namespace = budget.namespace.unwrap_or_else(|| budgets.namespace.clone());
        validate_inventory_name(&name)?;
        validate_inventory_name(&namespace)?;
        let external_key = format!("{namespace}/{name}");
        if !identities.insert(external_key.clone()) {
            return Err(schema_mismatch());
        }
        let partial = output.partial || budget.selector_has_match_expressions;
        any_partial |= partial;
        let pdb_key = inventory.insert_asset(asset(
            "pod_disruption_budget",
            &external_key,
            &name,
            "kubernetes",
            json!({
                "namespace": namespace,
                "labels": budget.labels,
                "selector_match_labels": budget.selector_match_labels.clone(),
                "selector_has_match_expressions": budget.selector_has_match_expressions,
                "min_available": budget.min_available,
                "max_unavailable": budget.max_unavailable,
                "current_healthy": budget.current_healthy,
                "desired_healthy": budget.desired_healthy,
                "expected_pods": budget.expected_pods,
                "disruptions_allowed": budget.disruptions_allowed,
            }),
            output.observed_at,
            output.freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            pdb_key.clone(),
            "contains",
            "kubernetes",
            output.observed_at,
            output.freshness_seconds,
            partial,
        ));
        if !budget.selector_has_match_expressions {
            for pod in pods
                .iter()
                .filter(|pod| selector_matches(&budget.selector_match_labels, &pod.labels))
            {
                inventory.insert_edge(edge(
                    pdb_key.clone(),
                    pod.key.clone(),
                    "configures",
                    "kubernetes",
                    output.observed_at,
                    output.freshness_seconds,
                    partial,
                ));
            }
        }
    }
    inventory.mark_source("pod_disruption_budget", any_partial);
    Ok(())
}

fn normalized_node_identity(value: &str, pseudonymization_key: &[u8]) -> String {
    let already_pseudonymized = value
        .strip_prefix("sha256:")
        .is_some_and(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    if already_pseudonymized {
        value.to_owned()
    } else {
        pseudonymize_identifier(value, pseudonymization_key)
    }
}

fn selector_matches(selector: &BTreeMap<String, String>, labels: &BTreeMap<String, String>) -> bool {
    selector.iter().all(|(key, value)| labels.get(key) == Some(value))
}

fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, ConnectorError> {
    serde_json::from_value(value).map_err(|_| schema_mismatch())
}

#[cfg(test)]
pub(super) fn fixture_add_pods(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    pseudonymization_key: &[u8],
) -> Result<(), ConnectorError> {
    add_pods(inventory, output, pseudonymization_key).map(|_| ())
}

#[cfg(test)]
pub(super) fn fixture_add_nodes(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    pseudonymization_key: &[u8],
) -> Result<(), ConnectorError> {
    add_nodes(inventory, output, pseudonymization_key)
}

#[cfg(test)]
pub(super) fn fixture_add_pvcs(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
) -> Result<(), ConnectorError> {
    add_pvcs(inventory, output)
}
