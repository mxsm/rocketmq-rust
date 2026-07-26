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
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use chrono::SecondsFormat;
use chrono::Utc;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_sre_contracts::ClusterId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use subtle::ConstantTimeEq;

use crate::CapabilitySnapshot;
use crate::Cluster;
use crate::ControlPlaneConfig;
use crate::ControlPlaneError;
use crate::HandshakeRequest;
use crate::OffboardRequest;
use crate::OnboardClusterRequest;
use crate::PostgresRepository;
use crate::model::HandshakeOutcome;
use crate::model::OnboardOutcome;
use crate::repository::ClusterRepository;

const CAPABILITY_CATALOG: &str = include_str!("../../../config/capabilities/rocketmq-capability-catalog.v1.yaml");
const COVERAGE: &str = include_str!("../../../config/evidence/capability-to-signal-coverage.v1.yaml");
const DATA_CLASSIFICATION: &str = include_str!("../../../config/evidence/data-classification.v1.yaml");
const REQUIRED_SOURCE_PROFILES: &str = include_str!("../../../config/evidence/required-source-profiles.v1.yaml");
const BROKER_SIGNALS: &str = include_str!("../../../config/observability/required-signals/broker.yaml");
const NAMESERVER_SIGNALS: &str = include_str!("../../../config/observability/required-signals/nameserver.yaml");
const CONTROLLER_SIGNALS: &str = include_str!("../../../config/observability/required-signals/controller.yaml");
const PROXY_SIGNALS: &str = include_str!("../../../config/observability/required-signals/proxy.yaml");
const MCP_SIGNALS: &str = include_str!("../../../config/observability/required-signals/mcp.yaml");
const RUNTIME_SIGNALS: &str = include_str!("../../../config/observability/required-signals/runtime.yaml");
const TELEMETRY_REGISTRY: &str = include_str!("../../../../scripts/telemetry-semantic-registry.json");
const REQUIRED_SIGNAL_SCHEMA_VERSION: &str = "rocketmq.sre.required-signals.v1";
const PACK_ORDER: [&str; 6] = [
    "cluster_health",
    "route_health",
    "consumer_lag",
    "broker_runtime",
    "controller_stability",
    "mcp_runtime",
];

/// Immutable capability documents bundled with the control-plane image.
#[derive(Clone, Debug)]
pub struct CapabilityDocuments {
    catalog: Arc<Value>,
    coverage_matrix: Arc<Value>,
    data_classification: Arc<Value>,
    required_source_profiles: Arc<Value>,
}

impl CapabilityDocuments {
    /// Parses the checked-in YAML artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error if a committed capability artifact is malformed.
    pub fn embedded() -> Result<Self, ControlPlaneError> {
        Ok(Self {
            catalog: Arc::new(parse_yaml("capability catalog", CAPABILITY_CATALOG)?),
            coverage_matrix: Arc::new(build_coverage_matrix()?),
            data_classification: Arc::new(parse_yaml("data classification", DATA_CLASSIFICATION)?),
            required_source_profiles: Arc::new(parse_yaml("required source profiles", REQUIRED_SOURCE_PROFILES)?),
        })
    }
}

#[derive(Clone, Deserialize)]
struct CoverageDocument {
    semantic_registry_owners: Vec<SemanticRegistryOwnerCoverage>,
    diagnostic_packs: BTreeMap<String, DiagnosticPack>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
struct SemanticRegistryOwnerCoverage {
    owner: String,
    component_surface: String,
    exposure: String,
    backlog: String,
    #[serde(default)]
    notable_sources: Vec<NotableSourceCoverage>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
struct NotableSourceCoverage {
    name: String,
    source_path: String,
    source_symbol: String,
    exposure: String,
}

#[derive(Clone, Deserialize)]
struct DiagnosticPack {
    #[serde(default)]
    required_signals: Vec<String>,
    #[serde(default)]
    optional_signals: Vec<String>,
}

#[derive(Deserialize)]
struct RequiredSignalManifest {
    schema_version: String,
    component: String,
    signals: Vec<RequiredSignal>,
}

#[derive(Clone, Deserialize)]
struct RequiredSignal {
    requirement_id: String,
    purpose: String,
    owner: String,
    registry_reference: String,
    signal_type: String,
    status: String,
    freshness_seconds: Option<u64>,
    #[serde(default)]
    expected_attributes: Vec<String>,
    sensitivity: String,
    missing_behavior: String,
    evidence_field: String,
}

#[derive(Deserialize)]
struct TelemetryRegistry {
    signals: Vec<TelemetrySignal>,
}

#[derive(Deserialize)]
struct TelemetrySignal {
    owner: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoverageMatrixResponse {
    generated_at: String,
    semantic_signal_count: usize,
    semantic_owner_count: usize,
    owner_surfaces: Vec<SemanticRegistryOwnerCoverage>,
    packs: Vec<CoveragePackView>,
    rows: Vec<CoverageRowView>,
    selected: CoverageSelectionView,
}

#[derive(Serialize)]
struct CoveragePackView {
    id: String,
    label: &'static str,
}

#[derive(Serialize)]
struct CoverageRowView {
    component: &'static str,
    cells: BTreeMap<String, CoverageCellStatus>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoverageCellStatus {
    Queryable,
    ImplementedLocal,
    InProcessOnly,
    MissingInstrumentation,
    NotProductionVerified,
}

#[derive(Serialize)]
struct CoverageSelectionView {
    component: &'static str,
    pack: &'static str,
    status: CoverageCellStatus,
    requirements: Vec<CoverageRequirementView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoverageRequirementView {
    id: String,
    signal_type: String,
    registry_reference: String,
    freshness: String,
    expected_attributes: Vec<String>,
    sensitivity: String,
    missing_behavior: String,
    evidence_field: String,
    owner: String,
    purpose: String,
}

fn build_coverage_matrix() -> Result<Value, ControlPlaneError> {
    let coverage: CoverageDocument =
        serde_yaml::from_str(COVERAGE).map_err(|error| ControlPlaneError::CapabilityDocument {
            detail: format!("capability coverage cannot be parsed: {error}"),
        })?;
    let manifests = [
        parse_signal_manifest("broker", BROKER_SIGNALS)?,
        parse_signal_manifest("nameserver", NAMESERVER_SIGNALS)?,
        parse_signal_manifest("controller", CONTROLLER_SIGNALS)?,
        parse_signal_manifest("proxy", PROXY_SIGNALS)?,
        parse_signal_manifest("mcp", MCP_SIGNALS)?,
        parse_signal_manifest("runtime", RUNTIME_SIGNALS)?,
    ];
    let registry: TelemetryRegistry =
        serde_json::from_str(TELEMETRY_REGISTRY).map_err(|error| ControlPlaneError::CapabilityDocument {
            detail: format!("telemetry semantic registry cannot be parsed: {error}"),
        })?;
    let owners = validate_semantic_registry_owners(&registry, &coverage.semantic_registry_owners)?;

    let packs = PACK_ORDER
        .iter()
        .map(|id| {
            if !coverage.diagnostic_packs.contains_key(*id) {
                return Err(ControlPlaneError::CapabilityDocument {
                    detail: format!("capability coverage is missing diagnostic pack `{id}`"),
                });
            }
            Ok(CoveragePackView {
                id: (*id).to_owned(),
                label: pack_label(id),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let rows = manifests
        .iter()
        .map(|manifest| {
            let cells = PACK_ORDER
                .iter()
                .map(|pack_id| {
                    let pack = &coverage.diagnostic_packs[*pack_id];
                    ((*pack_id).to_owned(), status_for(manifest, pack))
                })
                .collect();
            CoverageRowView {
                component: component_label(&manifest.component),
                cells,
            }
        })
        .collect();

    let selected_manifest = manifests
        .iter()
        .find(|manifest| manifest.component == "controller")
        .ok_or_else(|| ControlPlaneError::CapabilityDocument {
            detail: "controller required-signal manifest is missing".to_owned(),
        })?;
    let selected_pack = &coverage.diagnostic_packs["controller_stability"];
    let selected_requirements = matching_signals(selected_manifest, selected_pack)
        .into_iter()
        .map(CoverageRequirementView::from)
        .collect();
    serde_json::to_value(CoverageMatrixResponse {
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        semantic_signal_count: registry.signals.len(),
        semantic_owner_count: owners.len(),
        owner_surfaces: coverage.semantic_registry_owners,
        packs,
        rows,
        selected: CoverageSelectionView {
            component: "Controller",
            pack: "controller_stability",
            status: status_for(selected_manifest, selected_pack),
            requirements: selected_requirements,
        },
    })
    .map_err(|error| ControlPlaneError::CapabilityDocument {
        detail: format!("coverage matrix cannot be represented as JSON: {error}"),
    })
}

fn validate_semantic_registry_owners(
    registry: &TelemetryRegistry,
    configured: &[SemanticRegistryOwnerCoverage],
) -> Result<BTreeSet<String>, ControlPlaneError> {
    let registry_owners = registry
        .signals
        .iter()
        .map(|signal| signal.owner.clone())
        .collect::<BTreeSet<_>>();
    let mut configured_owners = BTreeSet::new();
    for owner in configured {
        if owner.component_surface.trim().is_empty()
            || owner.exposure.trim().is_empty()
            || owner.backlog.trim().is_empty()
            || owner.notable_sources.is_empty()
            || owner.notable_sources.iter().any(|source| {
                source.name.trim().is_empty()
                    || source.source_path.trim().is_empty()
                    || source.source_symbol.trim().is_empty()
                    || source.exposure.trim().is_empty()
            })
        {
            return Err(ControlPlaneError::CapabilityDocument {
                detail: format!(
                    "semantic registry owner `{}` must record a component surface, current exposure, backlog, and \
                     complete notable-source metadata",
                    owner.owner
                ),
            });
        }
        if owner.owner != "mcp"
            && (owner.exposure == "queryable"
                || owner
                    .notable_sources
                    .iter()
                    .any(|source| source.exposure == "queryable"))
        {
            return Err(ControlPlaneError::CapabilityDocument {
                detail: format!(
                    "semantic registry owner `{}` cannot claim remote queryability in Phase 00",
                    owner.owner
                ),
            });
        }
        if !configured_owners.insert(owner.owner.clone()) {
            return Err(ControlPlaneError::CapabilityDocument {
                detail: format!("semantic registry owner `{}` is mapped more than once", owner.owner),
            });
        }
    }

    if configured_owners != registry_owners {
        let missing = registry_owners
            .difference(&configured_owners)
            .cloned()
            .collect::<Vec<_>>();
        let unknown = configured_owners
            .difference(&registry_owners)
            .cloned()
            .collect::<Vec<_>>();
        return Err(ControlPlaneError::CapabilityDocument {
            detail: format!(
                "semantic registry owner coverage must map every owner exactly once; missing={missing:?}, \
                 unknown={unknown:?}"
            ),
        });
    }
    Ok(registry_owners)
}

fn parse_signal_manifest(name: &str, input: &str) -> Result<RequiredSignalManifest, ControlPlaneError> {
    let manifest: RequiredSignalManifest =
        serde_yaml::from_str(input).map_err(|error| ControlPlaneError::CapabilityDocument {
            detail: format!("{name} required-signal manifest cannot be parsed: {error}"),
        })?;
    if manifest.schema_version != REQUIRED_SIGNAL_SCHEMA_VERSION {
        return Err(ControlPlaneError::CapabilityDocument {
            detail: format!(
                "{name} required-signal schema `{}` does not equal `{REQUIRED_SIGNAL_SCHEMA_VERSION}`",
                manifest.schema_version
            ),
        });
    }
    Ok(manifest)
}

fn status_for(manifest: &RequiredSignalManifest, pack: &DiagnosticPack) -> CoverageCellStatus {
    let signals = matching_signals(manifest, pack);
    if signals.is_empty() {
        return CoverageCellStatus::NotProductionVerified;
    }
    signals
        .into_iter()
        .map(|signal| match signal.status.as_str() {
            "queryable" => CoverageCellStatus::Queryable,
            "existing" => CoverageCellStatus::ImplementedLocal,
            "in_process_only" => CoverageCellStatus::InProcessOnly,
            "missing_instrumentation" => CoverageCellStatus::MissingInstrumentation,
            _ => CoverageCellStatus::NotProductionVerified,
        })
        .max_by_key(|status| match status {
            CoverageCellStatus::Queryable => 0,
            CoverageCellStatus::ImplementedLocal => 1,
            CoverageCellStatus::InProcessOnly => 2,
            CoverageCellStatus::NotProductionVerified => 3,
            CoverageCellStatus::MissingInstrumentation => 4,
        })
        .unwrap_or(CoverageCellStatus::NotProductionVerified)
}

fn matching_signals<'a>(manifest: &'a RequiredSignalManifest, pack: &DiagnosticPack) -> Vec<&'a RequiredSignal> {
    let ids = pack
        .required_signals
        .iter()
        .chain(&pack.optional_signals)
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    manifest
        .signals
        .iter()
        .filter(|signal| ids.contains(signal.requirement_id.as_str()))
        .collect()
}

impl From<&RequiredSignal> for CoverageRequirementView {
    fn from(signal: &RequiredSignal) -> Self {
        Self {
            id: signal.requirement_id.clone(),
            signal_type: signal.signal_type.clone(),
            registry_reference: signal.registry_reference.clone(),
            freshness: signal
                .freshness_seconds
                .map_or_else(|| "按请求".to_owned(), |seconds| format!("≤ {seconds}s")),
            expected_attributes: signal.expected_attributes.clone(),
            sensitivity: signal.sensitivity.clone(),
            missing_behavior: signal.missing_behavior.clone(),
            evidence_field: signal.evidence_field.clone(),
            owner: signal.owner.clone(),
            purpose: signal.purpose.clone(),
        }
    }
}

fn pack_label(id: &str) -> &'static str {
    match id {
        "cluster_health" => "集群健康",
        "route_health" => "路由异常",
        "consumer_lag" => "消费堆积",
        "broker_runtime" => "Broker 运行态",
        "controller_stability" => "控制器选举",
        "mcp_runtime" => "MCP 自身状态",
        _ => "未知诊断包",
    }
}

fn component_label(component: &str) -> &'static str {
    match component {
        "broker" => "Broker",
        "nameserver" => "NameServer",
        "controller" => "Controller",
        "proxy" => "Proxy",
        "mcp" => "MCP",
        "runtime" => "Runtime",
        _ => "Unknown",
    }
}

fn parse_yaml(name: &str, input: &str) -> Result<Value, ControlPlaneError> {
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(input).map_err(|error| ControlPlaneError::CapabilityDocument {
            detail: format!("{name} cannot be parsed: {error}"),
        })?;
    serde_json::to_value(yaml).map_err(|error| ControlPlaneError::CapabilityDocument {
        detail: format!("{name} cannot be represented as JSON: {error}"),
    })
}

#[derive(Clone)]
struct AppState {
    repository: PostgresRepository,
    documents: CapabilityDocuments,
    internal_token: Arc<str>,
}

/// Builds the production API router.
pub fn build_router(
    repository: PostgresRepository,
    documents: CapabilityDocuments,
    internal_token: impl Into<Arc<str>>,
) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/v1/clusters/onboard", post(onboard))
        .route("/v1/clusters", get(list_clusters))
        .route("/v1/clusters/{id}", get(get_cluster))
        .route("/v1/clusters/{id}/handshake", post(handshake))
        .route("/v1/clusters/{id}/capabilities", get(get_capability))
        .route("/v1/clusters/{id}/offboard", post(offboard))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/capabilities/coverage", get(coverage))
        .with_state(AppState {
            repository,
            documents,
            internal_token: internal_token.into(),
        })
}

/// Connects the production repository, binds the HTTP endpoint, and drains on
/// process shutdown.
///
/// # Errors
///
/// Returns a configuration, database, bind, or serving error.
pub async fn run(config: ControlPlaneConfig, service_context: ChildServiceContext) -> Result<(), ControlPlaneError> {
    let repository = PostgresRepository::connect(config.database_url(), config.database_max_connections()).await?;
    let documents = CapabilityDocuments::embedded()?;
    let listener = tokio::net::TcpListener::bind(config.bind_addr()).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        bind_addr = %local_addr,
        scope = service_context.name(),
        effective_access = "read_only",
        "RocketMQ AI SRE control plane is ready"
    );
    axum::serve(
        listener,
        build_router(repository, documents, Arc::<str>::from(config.internal_token())),
    )
    .with_graceful_shutdown(async {
        if let Err(error) = wait_for_signal_result().await {
            tracing::warn!(error = %error, "shutdown signal watcher failed");
        }
    })
    .await
    .map_err(ControlPlaneError::Io)
}

#[derive(Serialize)]
struct ServiceStatus {
    status: &'static str,
}

async fn health() -> Json<ServiceStatus> {
    Json(ServiceStatus { status: "healthy" })
}

async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ServiceStatus>) {
    readiness(&state.repository).await
}

async fn readiness<R>(repository: &R) -> (StatusCode, Json<ServiceStatus>)
where
    R: ClusterRepository,
{
    match repository.ping().await {
        Ok(()) => (StatusCode::OK, Json(ServiceStatus { status: "ready" })),
        Err(error) => {
            tracing::warn!(error = %error, "control-plane readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ServiceStatus { status: "not_ready" }),
            )
        }
    }
}

async fn onboard(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<OnboardClusterRequest>,
) -> Result<(StatusCode, Json<OnboardOutcome>), ControlPlaneError> {
    authorize_mutation(&headers, &state.internal_token)?;
    request.validate()?;
    let outcome = state.repository.onboard(&request).await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(outcome)))
}

async fn list_clusters(State(state): State<AppState>) -> Result<Json<Vec<Cluster>>, ControlPlaneError> {
    state.repository.list().await.map(Json)
}

async fn get_cluster(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, ControlPlaneError> {
    state.repository.get(parse_cluster_id(&id)?).await.map(Json)
}

async fn handshake(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<HandshakeRequest>,
) -> Result<Json<HandshakeOutcome>, ControlPlaneError> {
    authorize_mutation(&headers, &state.internal_token)?;
    let id = parse_cluster_id(&id)?;
    let decision = request.validate()?;
    state.repository.handshake(id, &request, &decision).await.map(Json)
}

async fn get_capability(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CapabilitySnapshot>, ControlPlaneError> {
    state.repository.capability(parse_cluster_id(&id)?).await.map(Json)
}

async fn offboard(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<OffboardRequest>,
) -> Result<Json<Cluster>, ControlPlaneError> {
    authorize_mutation(&headers, &state.internal_token)?;
    state
        .repository
        .offboard(parse_cluster_id(&id)?, &request)
        .await
        .map(Json)
}

async fn capabilities(State(state): State<AppState>) -> Json<Value> {
    let providers = rocketmq_sre_model_gateway::phase00_provider_descriptors();
    Json(json!({
        "schema_version": "rocketmq-sre.capabilities.v1",
        "phase": "00",
        "effective_access_profile": "read_only",
        "execution_supported": false,
        "approval_supported": false,
        "provider_network_calls_supported": false,
        "providers": providers,
        "catalog": state.documents.catalog.as_ref(),
        "data_classification": state.documents.data_classification.as_ref(),
        "required_source_profiles": state.documents.required_source_profiles.as_ref()
    }))
}

async fn coverage(State(state): State<AppState>) -> Json<Value> {
    Json(state.documents.coverage_matrix.as_ref().clone())
}

fn parse_cluster_id(value: &str) -> Result<ClusterId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("cluster_not_allowed", "cluster identifier must be a UUID"))
}

fn authorize_mutation(headers: &HeaderMap, expected_token: &str) -> Result<(), ControlPlaneError> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    let matches = token.len() == expected_token.len() && bool::from(token.as_bytes().ct_eq(expected_token.as_bytes()));
    if matches {
        Ok(())
    } else {
        Err(ControlPlaneError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::memory::InMemoryRepository;

    #[test]
    fn embedded_capability_documents_are_valid() {
        let documents = CapabilityDocuments::embedded().expect("committed YAML should parse");
        let coverage = parse_yaml("capability coverage", COVERAGE).expect("coverage YAML should parse");
        let registry: TelemetryRegistry =
            serde_json::from_str(TELEMETRY_REGISTRY).expect("semantic registry should match its contract");
        assert_eq!(coverage["schema_version"], "rocketmq.sre.capability-signal-coverage.v1");
        assert!(documents.catalog["capabilities"].is_array());
        assert_eq!(documents.coverage_matrix["semanticSignalCount"], registry.signals.len());
        assert_eq!(documents.coverage_matrix["semanticOwnerCount"], 16);
        assert_eq!(
            documents.coverage_matrix["ownerSurfaces"]
                .as_array()
                .expect("owner surfaces should be an array")
                .len(),
            16
        );
        assert_eq!(
            documents.coverage_matrix["selected"]["status"],
            "missing_instrumentation"
        );
        assert_eq!(
            documents.coverage_matrix["packs"]
                .as_array()
                .expect("packs should be an array")
                .len(),
            6
        );
    }

    #[test]
    fn semantic_registry_owners_are_mapped_exactly_once() {
        let mut coverage: CoverageDocument =
            serde_yaml::from_str(COVERAGE).expect("coverage YAML should match its contract");
        let registry: TelemetryRegistry =
            serde_json::from_str(TELEMETRY_REGISTRY).expect("semantic registry should match its contract");

        let owners = validate_semantic_registry_owners(&registry, &coverage.semantic_registry_owners)
            .expect("every semantic registry owner should be mapped exactly once");
        assert_eq!(owners.len(), 16);

        let mut incomplete = coverage.clone();
        incomplete.semantic_registry_owners[0].backlog.clear();
        let error = validate_semantic_registry_owners(&registry, &incomplete.semantic_registry_owners)
            .expect_err("missing exposure metadata must fail closed");
        assert!(matches!(
            error,
            ControlPlaneError::CapabilityDocument { detail }
                if detail.contains("current exposure, backlog")
        ));

        let mut false_remote = coverage.clone();
        false_remote.semantic_registry_owners[0].exposure = "queryable".to_owned();
        let error = validate_semantic_registry_owners(&registry, &false_remote.semantic_registry_owners)
            .expect_err("non-MCP remote queryability must fail closed");
        assert!(matches!(
            error,
            ControlPlaneError::CapabilityDocument { detail }
                if detail.contains("cannot claim remote queryability")
        ));

        coverage.semantic_registry_owners[1].owner = coverage.semantic_registry_owners[0].owner.clone();
        let error = validate_semantic_registry_owners(&registry, &coverage.semantic_registry_owners)
            .expect_err("duplicate owner mapping must fail closed");
        assert!(matches!(
            error,
            ControlPlaneError::CapabilityDocument { detail }
                if detail.contains("mapped more than once")
        ));
    }

    #[test]
    fn owner_coverage_records_phase00_local_source_backlog() {
        let coverage: CoverageDocument =
            serde_yaml::from_str(COVERAGE).expect("coverage YAML should match its contract");
        let notable_sources = coverage
            .semantic_registry_owners
            .iter()
            .flat_map(|owner| owner.notable_sources.iter())
            .map(|source| source.name.as_str())
            .collect::<BTreeSet<_>>();

        for required in [
            "StoreHealthSnapshot",
            "RecoveryReport",
            "BackgroundIndexRebuildSnapshot",
            "AuthMetricsSnapshot",
            "AdmissionSnapshot",
            "ProxyMetricsSnapshot",
            "RuntimeDiagnosticsViewV1",
        ] {
            assert!(notable_sources.contains(required), "{required} coverage is missing");
        }

        let remotely_queryable = coverage
            .semantic_registry_owners
            .iter()
            .filter(|owner| {
                owner.exposure == "queryable"
                    || owner
                        .notable_sources
                        .iter()
                        .any(|source| source.exposure == "queryable")
            })
            .map(|owner| owner.owner.as_str())
            .collect::<Vec<_>>();
        assert_eq!(remotely_queryable, ["mcp"]);
    }

    #[test]
    fn required_signal_manifests_use_the_canonical_schema_name() {
        let legacy = BROKER_SIGNALS.replace(REQUIRED_SIGNAL_SCHEMA_VERSION, "rocketmq-sre.required-signals.v1");
        let error = parse_signal_manifest("broker", &legacy)
            .err()
            .expect("legacy required-signal schema spelling must fail closed");
        assert!(matches!(
            error,
            ControlPlaneError::CapabilityDocument { detail }
                if detail.contains("does not equal `rocketmq.sre.required-signals.v1`")
        ));
    }

    #[tokio::test]
    async fn readiness_requires_repository_ping() {
        let repository = InMemoryRepository::default();
        let (status, body) = readiness(&repository).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.0.status, "ready");
    }

    #[test]
    fn mutation_authorization_is_fail_closed() {
        let mut headers = HeaderMap::new();
        assert!(matches!(
            authorize_mutation(&headers, "expected"),
            Err(ControlPlaneError::Unauthorized)
        ));
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer wrong".parse().expect("header"),
        );
        assert!(matches!(
            authorize_mutation(&headers, "expected"),
            Err(ControlPlaneError::Unauthorized)
        ));
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer expected".parse().expect("header"),
        );
        assert!(authorize_mutation(&headers, "expected").is_ok());
    }
}
