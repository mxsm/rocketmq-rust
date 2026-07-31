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

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_core::diagnostics::DiagnosticPack;
use rocketmq_sre_core::diagnostics::EvidenceRequirement;
use rocketmq_sre_core::diagnostics::full_registry;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::api::AppState;
use crate::auth::AuthContext;

const PACK_ORDER: [&str; 32] = [
    "cluster-topology.v1",
    "consumer-lag.v2",
    "consumer-runtime.v1",
    "producer-connectivity.v1",
    "broker-health.v1",
    "message-path.v1",
    "telemetry-pipeline.v1",
    "deployment-drift.v1",
    "store-pressure.v1",
    "store-integrity.v1",
    "rocksdb-health.v1",
    "tiered-store.v1",
    "broker-ha.v1",
    "controller-ha.v1",
    "namesrv-route.v1",
    "send-latency.v1",
    "proxy-connectivity.v1",
    "retry-dlq.v1",
    "transaction-message.v1",
    "pop-revive.v1",
    "timer-backlog.v1",
    "queue-hotspot.v1",
    "static-topic-route.v1",
    "topic-subscription-config.v1",
    "auth-failure.v1",
    "runtime-saturation.v1",
    "upgrade-readiness.v1",
    "capacity-runway.v1",
    "cold-data-flow.v1",
    "dr-readiness.v1",
    "security-posture.v1",
    "change-regression.v1",
];

const SOURCE_ORDER: [&str; 9] = [
    "rocketmq-mcp",
    "admin-query",
    "prometheus",
    "loki",
    "tempo",
    "kubernetes",
    "runtime",
    "topology",
    "alertmanager",
];

#[derive(Debug, Default, Deserialize)]
pub(crate) struct CoverageQuery {
    pub(crate) cluster_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoverageCellStatus {
    Queryable,
    ImplementedLocal,
    InProcessOnly,
    MissingInstrumentation,
    NotProductionVerified,
}

#[derive(Debug)]
struct SourceState {
    cluster_id: ClusterId,
    source: String,
    schema_major: i32,
    status: String,
    limits: Value,
    last_success_at: Option<DateTime<Utc>>,
    latency_millis: Option<i64>,
    freshness_seconds: Option<i64>,
    observed_at: DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoverageMatrix {
    generated_at: String,
    semantic_signal_count: usize,
    semantic_owner_count: usize,
    cluster_id: Option<ClusterId>,
    packs: Vec<PackView>,
    rows: Vec<RowView>,
    selected: SelectionView,
    source_capabilities: Vec<SourceCapabilityView>,
    pack_coverage: Vec<PackCoverageView>,
}

#[derive(Serialize)]
struct PackView {
    id: String,
    label: &'static str,
}

#[derive(Serialize)]
struct RowView {
    component: &'static str,
    cells: BTreeMap<String, CoverageCellStatus>,
}

#[derive(Serialize)]
struct SelectionView {
    component: &'static str,
    pack: String,
    status: CoverageCellStatus,
    requirements: Vec<RequirementView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RequirementView {
    id: String,
    signal_type: &'static str,
    registry_reference: String,
    freshness: String,
    expected_attributes: Vec<String>,
    sensitivity: &'static str,
    missing_behavior: &'static str,
    evidence_field: String,
    owner: String,
    purpose: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceCapabilityView {
    cluster_id: ClusterId,
    source: String,
    schema_major: i32,
    status: String,
    limits: Value,
    last_success_at: Option<DateTime<Utc>>,
    latency_millis: Option<i64>,
    freshness_seconds: Option<i64>,
    observed_at: DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PackCoverageView {
    pack_id: String,
    required_total: usize,
    required_queryable: usize,
    optional_total: usize,
    optional_queryable: usize,
    missing_required: Vec<String>,
}

pub(crate) async fn matrix(
    state: &AppState,
    auth: &AuthContext,
    query: CoverageQuery,
) -> Result<Value, ControlPlaneError> {
    let cluster_ids = selected_clusters(auth, query.cluster_id.as_deref())?;
    let source_states = latest_source_states(state, auth, &cluster_ids).await?;
    let states = source_states
        .iter()
        .map(|state| (state.source.as_str(), state))
        .collect::<BTreeMap<_, _>>();
    let registry = full_registry().map_err(|error| {
        ControlPlaneError::configuration(format!("built-in diagnostic pack registry is invalid: {error}"))
    })?;
    let packs = PACK_ORDER
        .iter()
        .map(|id| {
            registry
                .resolve(id)
                .ok_or_else(|| ControlPlaneError::configuration(format!("built-in diagnostic pack `{id}` is missing")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let pack_views = packs
        .iter()
        .map(|pack| PackView {
            id: pack.qualified_id(),
            label: pack_label(pack.id()),
        })
        .collect::<Vec<_>>();
    let rows = SOURCE_ORDER
        .iter()
        .map(|source| RowView {
            component: source_label(source),
            cells: packs
                .iter()
                .map(|pack| {
                    (
                        pack.qualified_id(),
                        cell_status(*pack, source, states.get(source).copied()),
                    )
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let pack_coverage = packs.iter().map(|pack| pack_coverage(*pack, &states)).collect();

    let selected_pack = packs.first().ok_or_else(|| {
        ControlPlaneError::configuration("built-in diagnostic pack registry contains no active packs")
    })?;
    let selected_source = "topology";
    let selected_requirements = matching_requirements(*selected_pack, selected_source)
        .map(|requirement| requirement_view(requirement, states.get(selected_source).copied()))
        .collect();
    let semantic_signal_count = state.documents.coverage_matrix["semanticSignalCount"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    let semantic_owner_count = state.documents.coverage_matrix["semanticOwnerCount"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    serde_json::to_value(CoverageMatrix {
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        semantic_signal_count,
        semantic_owner_count,
        cluster_id: cluster_ids.first().copied(),
        packs: pack_views,
        rows,
        selected: SelectionView {
            component: source_label(selected_source),
            pack: selected_pack.qualified_id(),
            status: cell_status(*selected_pack, selected_source, states.get(selected_source).copied()),
            requirements: selected_requirements,
        },
        source_capabilities: source_states.into_iter().map(SourceCapabilityView::from).collect(),
        pack_coverage,
    })
    .map_err(|error| ControlPlaneError::configuration(format!("coverage response cannot be encoded: {error}")))
}

fn selected_clusters(auth: &AuthContext, requested: Option<&str>) -> Result<Vec<ClusterId>, ControlPlaneError> {
    if let Some(requested) = requested {
        let cluster_id = requested.parse::<ClusterId>().map_err(|_| {
            ControlPlaneError::validation("cluster_not_allowed", "coverage cluster identifier must be a UUID")
        })?;
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "coverage cluster is outside the authenticated scope",
            ));
        }
        Ok(vec![cluster_id])
    } else {
        Ok(auth.clusters.iter().copied().collect())
    }
}

async fn latest_source_states(
    state: &AppState,
    auth: &AuthContext,
    cluster_ids: &[ClusterId],
) -> Result<Vec<SourceState>, ControlPlaneError> {
    if cluster_ids.is_empty() {
        return Ok(Vec::new());
    }
    let cluster_uuids = cluster_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT DISTINCT ON (source)
            cluster_id, source, schema_major, status, limits,
            last_success_at, latency_millis, freshness_seconds, observed_at
         FROM source_capability_history
         WHERE tenant_id = $1 AND cluster_id = ANY($2)
         ORDER BY source, observed_at DESC, sequence_id DESC",
    )
    .bind(auth.tenant_id.as_uuid())
    .bind(&cluster_uuids)
    .fetch_all(&state.repository.pool)
    .await?;
    rows.iter()
        .map(|row| {
            let cluster_id: Uuid = row.try_get("cluster_id")?;
            Ok(SourceState {
                cluster_id: ClusterId::from_uuid(cluster_id),
                source: row.try_get("source")?,
                schema_major: row.try_get("schema_major")?,
                status: row.try_get("status")?,
                limits: row.try_get("limits")?,
                last_success_at: row.try_get("last_success_at")?,
                latency_millis: row.try_get("latency_millis")?,
                freshness_seconds: row.try_get("freshness_seconds")?,
                observed_at: row.try_get("observed_at")?,
            })
        })
        .collect()
}

fn cell_status(pack: &dyn DiagnosticPack, source: &str, source_state: Option<&SourceState>) -> CoverageCellStatus {
    if matching_requirements(pack, source).next().is_none() {
        return CoverageCellStatus::NotProductionVerified;
    }
    match source_state.map(|state| state.status.as_str()) {
        Some("queryable") => CoverageCellStatus::Queryable,
        Some("degraded") => CoverageCellStatus::InProcessOnly,
        Some("missing") => CoverageCellStatus::MissingInstrumentation,
        Some("unsupported") => CoverageCellStatus::ImplementedLocal,
        Some(_) | None => CoverageCellStatus::NotProductionVerified,
    }
}

fn pack_coverage(pack: &dyn DiagnosticPack, states: &BTreeMap<&str, &SourceState>) -> PackCoverageView {
    let required_queryable = pack
        .required_evidence()
        .iter()
        .filter(|requirement| source_is_queryable(requirement.source, states))
        .count();
    let optional_queryable = pack
        .optional_evidence()
        .iter()
        .filter(|requirement| source_is_queryable(requirement.source, states))
        .count();
    let missing_required = pack
        .required_evidence()
        .iter()
        .filter(|requirement| !source_is_queryable(requirement.source, states))
        .map(|requirement| requirement.key.to_owned())
        .collect();
    PackCoverageView {
        pack_id: pack.qualified_id(),
        required_total: pack.required_evidence().len(),
        required_queryable,
        optional_total: pack.optional_evidence().len(),
        optional_queryable,
        missing_required,
    }
}

fn source_is_queryable(source: &str, states: &BTreeMap<&str, &SourceState>) -> bool {
    states
        .get(capability_source(source))
        .is_some_and(|state| state.status == "queryable")
}

fn matching_requirements<'a>(
    pack: &'a dyn DiagnosticPack,
    source: &'a str,
) -> impl Iterator<Item = &'a EvidenceRequirement> {
    pack.required_evidence()
        .iter()
        .chain(pack.optional_evidence())
        .filter(move |requirement| capability_source(requirement.source) == source)
}

fn requirement_view(requirement: &EvidenceRequirement, state: Option<&SourceState>) -> RequirementView {
    RequirementView {
        id: requirement.key.to_owned(),
        signal_type: "resource",
        registry_reference: format!("{}:{}", requirement.source, requirement.resource_prefix),
        freshness: state.and_then(|state| state.freshness_seconds).map_or_else(
            || "not_production_verified".to_owned(),
            |seconds| format!("≤{seconds}s"),
        ),
        expected_attributes: Vec::new(),
        sensitivity: "internal",
        missing_behavior: "missing",
        evidence_field: format!("content.{}", requirement.key),
        owner: capability_source(requirement.source).to_owned(),
        purpose: requirement.purpose.to_owned(),
    }
}

fn capability_source(source: &str) -> &str {
    match source {
        "mcp" => "rocketmq-mcp",
        other => other,
    }
}

fn pack_label(id: &str) -> &'static str {
    match id {
        "store-pressure" => "Store pressure",
        "store-integrity" => "Store integrity",
        "rocksdb-health" => "RocksDB health",
        "tiered-store" => "Tiered Store",
        "broker-ha" => "Broker HA",
        "controller-ha" => "Controller HA",
        "namesrv-route" => "NameServer route",
        "send-latency" => "Send latency",
        "proxy-connectivity" => "Proxy connectivity",
        "retry-dlq" => "Retry and DLQ",
        "transaction-message" => "Transaction messages",
        "pop-revive" => "POP revive",
        "timer-backlog" => "Timer backlog",
        "queue-hotspot" => "Queue hotspot",
        "static-topic-route" => "Static Topic route",
        "topic-subscription-config" => "Topic and subscription config",
        "auth-failure" => "Auth failure",
        "runtime-saturation" => "Runtime saturation",
        "upgrade-readiness" => "Upgrade readiness",
        "capacity-runway" => "Capacity runway",
        "cold-data-flow" => "Cold-data flow",
        "dr-readiness" => "DR readiness",
        "security-posture" => "Security posture",
        "change-regression" => "Change regression",
        "cluster-topology" => "集群拓扑",
        "consumer-lag" => "消费积压",
        "consumer-runtime" => "消费者运行态",
        "producer-connectivity" => "生产者连通性",
        "broker-health" => "Broker 健康",
        "message-path" => "消息链路",
        "telemetry-pipeline" => "遥测管道",
        "deployment-drift" => "部署漂移",
        _ => "未知诊断包",
    }
}

fn source_label(source: &str) -> &'static str {
    match source {
        "rocketmq-mcp" => "MCP",
        "admin-query" => "Admin Query",
        "prometheus" => "Prometheus",
        "loki" => "Loki",
        "tempo" => "Tempo",
        "kubernetes" => "Kubernetes",
        "runtime" => "Runtime",
        "topology" => "Topology",
        "alertmanager" => "Alertmanager",
        _ => "Unknown",
    }
}

impl From<SourceState> for SourceCapabilityView {
    fn from(state: SourceState) -> Self {
        Self {
            cluster_id: state.cluster_id,
            source: state.source,
            schema_major: state.schema_major,
            status: state.status,
            limits: state.limits,
            last_success_at: state.last_success_at,
            latency_millis: state.latency_millis,
            freshness_seconds: state.freshness_seconds,
            observed_at: state.observed_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn wave_a_matrix_contains_all_pack_and_source_contracts() {
        let registry = full_registry().expect("built-in registry");
        let packs = PACK_ORDER
            .iter()
            .map(|id| registry.resolve(id).expect("registered pack"))
            .collect::<Vec<_>>();
        assert_eq!(packs.len(), 32);
        let declared_sources = packs
            .iter()
            .flat_map(|pack| {
                pack.required_evidence()
                    .iter()
                    .chain(pack.optional_evidence())
                    .map(|requirement| capability_source(requirement.source))
            })
            .collect::<BTreeSet<_>>();
        assert!(declared_sources.iter().all(|source| SOURCE_ORDER.contains(source)));
    }

    #[test]
    fn missing_source_is_never_reported_as_zero_or_queryable() {
        let registry = full_registry().expect("built-in registry");
        let pack = registry.resolve("consumer-lag.v2").expect("consumer lag");
        assert!(matches!(
            cell_status(pack, "rocketmq-mcp", None),
            CoverageCellStatus::NotProductionVerified
        ));
    }
}
