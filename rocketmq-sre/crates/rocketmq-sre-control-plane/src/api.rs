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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get;
use axum::routing::post;
use chrono::SecondsFormat;
use chrono::Utc;
use rocketmq_observability::ObservabilityStatusHandle;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::Phase2ContractManifest;
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
use crate::alerting::AlertingService;
use crate::alerting::NotificationOutboxWorker;
use crate::assets::AssetTopologyService;
use crate::assets::DashboardDeepLinkPolicy;
use crate::auth::AuthService;
use crate::connector_channel;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceBlobStore;
use crate::evidence::EvidenceService;
use crate::forecast::ForecastService;
use crate::inspection::InspectionService;
use crate::knowledge::KnowledgeService;
use crate::model::HandshakeOutcome;
use crate::model::OnboardOutcome;
use crate::models::ModelGatewayService;
use crate::observability::ConnectorHealthSample;
use crate::observability::DatabaseHealthSample;
use crate::observability::DependencyStatus;
use crate::observability::HealthAggregator;
use crate::observability::HealthReasonCode;
use crate::observability::ProviderFamilyLabel;
use crate::observability::ProviderHealthSample;
use crate::observability::SreHealthViewV1;
use crate::observability::SreMetrics;
use crate::observability::SreObservability;
use crate::repository::ClusterRepository;
use crate::slo::SloService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

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
    pub(crate) coverage_matrix: Arc<Value>,
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
pub(crate) struct AppState {
    pub(crate) repository: PostgresRepository,
    pub(crate) documents: CapabilityDocuments,
    pub(crate) internal_token: Arc<str>,
    pub(crate) auth: AuthService,
    pub(crate) alerting: AlertingService,
    pub(crate) assets: AssetTopologyService,
    pub(crate) connector_channel: PostgresConnectorChannelService,
    pub(crate) evidence: EvidenceService,
    pub(crate) lease_authority: crate::execution_authority::LeaseAuthorityService,
    pub(crate) forecast: ForecastService,
    pub(crate) knowledge: KnowledgeService,
    pub(crate) model_gateway: ModelGatewayService,
    pub(crate) observability: SreObservability,
    pub(crate) observability_status: ObservabilityStatusHandle,
    pub(crate) operations: crate::operator_workbench::OperatorWorkbenchService,
    pub(crate) postmortems: crate::postmortem::PostmortemService,
    pub(crate) sre_metrics: Arc<SreMetrics>,
    pub(crate) slo: SloService,
    pub(crate) supervised_execution: crate::supervised_execution::SupervisedExecutionService,
    pub(crate) workflow: WorkflowService,
}

/// Builds the production API router.
pub fn build_router(
    repository: PostgresRepository,
    documents: CapabilityDocuments,
    internal_token: impl Into<Arc<str>>,
) -> Result<Router, ControlPlaneError> {
    let internal_token = internal_token.into();
    let auth = AuthService::development(internal_token.clone());
    let model_gateway = ModelGatewayService::disabled(repository.clone());
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(1_024));
    let evidence_blobs = EvidenceBlobStore::from_env(true)?;
    Ok(build_routers_with_auth(
        repository,
        documents,
        internal_token.clone(),
        internal_token.clone(),
        internal_token,
        crate::supervised_execution::ExecutorSubmissionClient::disabled(),
        auth,
        evidence_blobs,
        DashboardDeepLinkPolicy::disabled(),
        model_gateway,
        workflow,
    )?
    .public)
}

struct ControlPlaneRouters {
    public: Router,
    connector: Router,
    forecast: ForecastService,
    slo: SloService,
}

fn build_routers_with_auth(
    repository: PostgresRepository,
    documents: CapabilityDocuments,
    internal_token: Arc<str>,
    grant_signing_key: Arc<str>,
    agent_ack_verification_key: Arc<str>,
    executor_client: crate::supervised_execution::ExecutorSubmissionClient,
    auth: AuthService,
    evidence_blobs: EvidenceBlobStore,
    dashboard_links: DashboardDeepLinkPolicy,
    model_gateway: ModelGatewayService,
    workflow: WorkflowService,
) -> Result<ControlPlaneRouters, ControlPlaneError> {
    let alerting = AlertingService::new(repository.clone(), workflow.clone())?;
    let evidence = EvidenceService::new(repository.clone(), evidence_blobs);
    let assets = AssetTopologyService::new(repository.clone(), dashboard_links);
    let knowledge = KnowledgeService::new(repository.clone());
    let postmortems = crate::postmortem::PostmortemService::new(
        repository.clone(),
        evidence.clone(),
        model_gateway.clone(),
        workflow.clone(),
    );
    let operations = crate::operator_workbench::OperatorWorkbenchService::new(repository.clone());
    let connector_channel = PostgresConnectorChannelService::postgres(repository.clone(), internal_token.clone())?;
    let slo = SloService::new(
        repository.clone(),
        connector_channel.clone(),
        evidence.clone(),
        alerting.clone(),
    )?;
    let forecast = ForecastService::new(
        repository.clone(),
        connector_channel.clone(),
        evidence.clone(),
        assets.clone(),
        slo.clone(),
    )?;
    let supervised_execution = crate::supervised_execution::SupervisedExecutionService::new_with_executor(
        repository.clone(),
        workflow.clone(),
        grant_signing_key.as_bytes(),
        model_gateway.clone(),
        executor_client,
    )?;
    let lease_authority = crate::execution_authority::LeaseAuthorityService::new(
        repository.pool.clone(),
        grant_signing_key.as_bytes(),
        agent_ack_verification_key.as_bytes(),
    )?;
    let connector_routes = connector_channel::router::<AppState>(connector_channel.clone());
    let connector_control_routes = Router::new()
        .route(
            "/internal/v1/connectors/v1/clusters/{id}/handshake",
            post(connector_handshake),
        )
        .route("/internal/v1/connectors/v1/clusters/{id}", get(connector_cluster_state));
    let (observability, sre_metrics) = SreObservability::with_prometheus_metrics();
    let state = AppState {
        repository,
        documents,
        internal_token,
        auth,
        alerting,
        assets,
        connector_channel,
        evidence,
        lease_authority,
        forecast: forecast.clone(),
        knowledge,
        model_gateway,
        observability,
        observability_status: ObservabilityStatusHandle::default(),
        operations,
        postmortems,
        sre_metrics,
        slo: slo.clone(),
        supervised_execution,
        workflow,
    };
    let public = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/clusters/onboard", post(onboard))
        .route("/v1/clusters", get(list_clusters))
        .route("/v1/clusters/{id}", get(get_cluster))
        .route("/v1/clusters/{id}/handshake", post(handshake))
        .route("/v1/clusters/{id}/capabilities", get(get_capability))
        .route("/v1/clusters/{id}/offboard", post(offboard))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/capabilities/coverage", get(coverage))
        .route("/v1/capabilities/phase2-contract", get(phase2_contract_manifest))
        .merge(crate::phase1_api::public_routes())
        .merge(crate::operator_workbench::routes())
        .merge(crate::postmortem::routes())
        .merge(crate::supervised_execution::routes())
        .merge(crate::execution_authority::routes())
        .merge(crate::execution_verification::routes())
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            crate::read_audit::middleware,
        ));
    let connector = connector_routes
        .merge(connector_control_routes)
        .merge(crate::phase1_api::connector_ingest_routes())
        .with_state(state);
    Ok(ControlPlaneRouters {
        public,
        connector,
        forecast,
        slo,
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
    let auth = AuthService::from_config(&config).await?;
    let evidence_blobs = EvidenceBlobStore::from_env(config.dev_auth_enabled())?;
    let dashboard_links = DashboardDeepLinkPolicy::from_allowlist(config.dashboard_deep_link_origins().iter())?;
    let model_gateway = ModelGatewayService::from_env(
        repository.clone(),
        config.dev_auth_enabled(),
        service_context.metadata_io().clone(),
    )?;
    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(1_024));
    let notification_worker = NotificationOutboxWorker::new(repository.clone())?;
    let notification_tasks = service_context.scheduled_tasks("notification-outbox");
    let mut notification_schedule =
        ScheduledTaskConfig::fixed_rate_no_overlap("phase2-notification-outbox", std::time::Duration::from_secs(5));
    notification_schedule.initial_delay = std::time::Duration::from_secs(2);
    notification_schedule.max_run_time = Some(std::time::Duration::from_secs(20));
    notification_schedule.shutdown_timeout = config.shutdown_timeout();
    notification_tasks
        .schedule_fixed_rate_no_overlap(notification_schedule, move || {
            let worker = notification_worker.clone();
            async move {
                worker.run_due().await;
            }
        })
        .map_err(|error| {
            ControlPlaneError::configuration(format!("notification outbox worker could not be started: {error}"))
        })?;
    let todo_repository = repository.clone();
    let todo_tasks = service_context.scheduled_tasks("postmortem-operator-todos");
    let mut todo_schedule = ScheduledTaskConfig::fixed_rate_no_overlap(
        "phase2-postmortem-operator-todos",
        std::time::Duration::from_secs(60),
    );
    todo_schedule.initial_delay = std::time::Duration::from_secs(11);
    todo_schedule.max_run_time = Some(std::time::Duration::from_secs(30));
    todo_schedule.shutdown_timeout = config.shutdown_timeout();
    todo_tasks
        .schedule_fixed_rate_no_overlap(todo_schedule, move || {
            let repository = todo_repository.clone();
            async move {
                if let Err(error) = crate::postmortem::materialize_due_operator_todos(&repository).await {
                    tracing::warn!(
                        error = %error,
                        "postmortem operator todo scan failed"
                    );
                }
            }
        })
        .map_err(|error| {
            ControlPlaneError::configuration(format!(
                "postmortem operator todo scheduler could not be started: {error}"
            ))
        })?;
    let scheduler_evidence = EvidenceService::new(repository.clone(), evidence_blobs.clone());
    let scheduled_inspections = InspectionService::new(repository.clone(), workflow.clone(), scheduler_evidence)?;
    let scheduled_tasks = service_context.scheduled_tasks("inspection-scheduler");
    let mut schedule =
        ScheduledTaskConfig::fixed_rate_no_overlap("phase1-inspection-scan", std::time::Duration::from_secs(30));
    schedule.initial_delay = std::time::Duration::from_secs(5);
    schedule.max_run_time = Some(std::time::Duration::from_secs(25));
    schedule.shutdown_timeout = config.shutdown_timeout();
    scheduled_tasks
        .schedule_fixed_rate_no_overlap(schedule, move || {
            let inspections = scheduled_inspections.clone();
            async move {
                inspections.run_due().await;
            }
        })
        .map_err(|error| {
            ControlPlaneError::configuration(format!("inspection scheduler could not be started: {error}"))
        })?;
    let public_listener = tokio::net::TcpListener::bind(config.bind_addr()).await?;
    let public_addr = public_listener.local_addr()?;
    let connector_listener = tokio::net::TcpListener::bind(config.connector_bind_addr()).await?;
    let connector_addr = connector_listener.local_addr()?;
    tracing::info!(
        bind_addr = %public_addr,
        scope = service_context.name(),
        effective_access = "human_approved_supervised",
        "RocketMQ AI SRE control plane is ready"
    );
    tracing::info!(
        bind_addr = %connector_addr,
        scope = service_context.name(),
        transport_boundary = "mtls_proxy_only",
        "RocketMQ AI SRE Connector-only listener is ready"
    );
    let executor_client = match (config.executor_url(), config.executor_token()) {
        (Some(url), Some(token)) => crate::supervised_execution::ExecutorSubmissionClient::http(
            url.clone(),
            Arc::<str>::from(token),
            config.executor_timeout(),
            config.executor_allow_insecure_http(),
        )?,
        (None, None) => crate::supervised_execution::ExecutorSubmissionClient::disabled(),
        _ => {
            return Err(ControlPlaneError::configuration(
                "Executor URL and token configuration is incomplete",
            ));
        }
    };
    let routers = build_routers_with_auth(
        repository,
        documents,
        Arc::<str>::from(config.internal_token()),
        Arc::<str>::from(config.grant_signing_key()),
        Arc::<str>::from(config.agent_ack_verification_key()),
        executor_client,
        auth,
        evidence_blobs,
        dashboard_links,
        model_gateway,
        workflow,
    )?;
    let slo_worker = routers.slo.clone();
    let slo_tasks = service_context.scheduled_tasks("slo-evaluator");
    let mut slo_schedule =
        ScheduledTaskConfig::fixed_rate_no_overlap("phase2-slo-evaluator", slo_worker.worker_interval());
    slo_schedule.initial_delay = std::time::Duration::from_secs(3);
    slo_schedule.max_run_time = Some(std::time::Duration::from_secs(5 * 60));
    slo_schedule.shutdown_timeout = config.shutdown_timeout();
    slo_tasks
        .schedule_fixed_rate_no_overlap(slo_schedule, move || {
            let slo = slo_worker.clone();
            async move {
                slo.run_due().await;
            }
        })
        .map_err(|error| ControlPlaneError::configuration(format!("SLO evaluator could not be started: {error}")))?;
    let forecast_worker = routers.forecast.clone();
    let forecast_tasks = service_context.scheduled_tasks("forecast-evaluator");
    let mut forecast_schedule =
        ScheduledTaskConfig::fixed_rate_no_overlap("phase2-forecast-evaluator", forecast_worker.worker_interval());
    forecast_schedule.initial_delay = std::time::Duration::from_secs(7);
    forecast_schedule.max_run_time = Some(std::time::Duration::from_secs(10 * 60));
    forecast_schedule.shutdown_timeout = config.shutdown_timeout();
    forecast_tasks
        .schedule_fixed_rate_no_overlap(forecast_schedule, move || {
            let forecast = forecast_worker.clone();
            async move {
                forecast.run_due().await;
            }
        })
        .map_err(|error| {
            ControlPlaneError::configuration(format!("forecast evaluator could not be started: {error}"))
        })?;
    let ControlPlaneRouters {
        public,
        connector,
        forecast: _,
        slo: _,
    } = routers;
    let connector_shutdown = service_context.task_group().cancellation_token();
    let connector_failure = connector_shutdown.clone();
    let connector_failed = Arc::new(AtomicBool::new(false));
    let connector_failed_task = connector_failed.clone();
    service_context
        .spawn_service("rocketmq-sre-control-plane.connector-listener", async move {
            let result = axum::serve(connector_listener, connector)
                .with_graceful_shutdown(async move {
                    connector_shutdown.cancelled().await;
                })
                .await;
            if let Err(error) = result {
                connector_failed_task.store(true, Ordering::Release);
                tracing::error!(
                    error = %error,
                    "Connector-only listener failed and is stopping the control plane"
                );
                connector_failure.cancel();
            }
        })
        .map_err(|error| {
            ControlPlaneError::configuration(format!(
                "Connector-only listener could not be owned by TaskGroup: {error}"
            ))
        })?;
    let public_shutdown = service_context.task_group().cancellation_token();
    let server_result = axum::serve(public_listener, public)
        .with_graceful_shutdown(async move {
            tokio::select! {
                () = public_shutdown.cancelled() => {}
                result = wait_for_signal_result() => {
                    if let Err(error) = result {
                        tracing::warn!(error = %error, "shutdown signal watcher failed");
                    }
                }
            }
        })
        .await;
    service_context.task_group().cancel();
    let report = service_context.task_group().shutdown(config.shutdown_timeout()).await;
    report.log_if_unhealthy();
    if connector_failed.load(Ordering::Acquire) {
        return Err(ControlPlaneError::Io(std::io::Error::other(
            "Connector-only listener failed",
        )));
    }
    server_result.map_err(ControlPlaneError::Io)
}

#[derive(Serialize)]
struct ServiceStatus {
    status: &'static str,
}

async fn health() -> Json<ServiceStatus> {
    Json(ServiceStatus { status: "healthy" })
}

async fn ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    if !state.auth.ready().await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "schemaVersion": SreHealthViewV1::SCHEMA_VERSION,
                "ready": false,
                "overallStatus": "unavailable",
                "reason": "authentication_failed",
                "rulesOnlyAvailable": false,
                "evidenceCollectionAvailable": false,
            })),
        );
    }
    let started = std::time::Instant::now();
    let database = match state.repository.ping().await {
        Ok(()) => DatabaseHealthSample::healthy(
            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            state
                .repository
                .pool
                .size()
                .saturating_sub(u32::try_from(state.repository.pool.num_idle()).unwrap_or(u32::MAX)),
            u32::try_from(state.repository.pool.num_idle()).unwrap_or(u32::MAX),
            state.repository.pool.options().get_max_connections(),
        ),
        Err(error) => {
            tracing::warn!(error = %error, "control-plane readiness database probe failed");
            DatabaseHealthSample::unavailable(HealthReasonCode::QueryFailed)
        }
    };
    let connector_samples = match state.connector_channel.health_samples(256).await {
        Ok(samples) => samples,
        Err(error) => {
            tracing::warn!(error = %error, "control-plane readiness connector probe failed");
            vec![ConnectorHealthSample::new(
                DependencyStatus::Unavailable,
                None,
                0,
                Some(HealthReasonCode::QueryFailed),
            )]
        }
    };
    let provider_samples = match state.model_gateway.health_samples(256).await {
        Ok(samples) => samples,
        Err(error) => {
            tracing::warn!(error = %error, "control-plane readiness provider probe failed");
            vec![ProviderHealthSample::new(
                ProviderFamilyLabel::Other,
                DependencyStatus::Unavailable,
                None,
                Some(HealthReasonCode::QueryFailed),
            )]
        }
    };
    let view = HealthAggregator::aggregate(
        database,
        provider_samples,
        connector_samples,
        state.observability_status.view(),
    );
    let status = if view.ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    match serde_json::to_value(view) {
        Ok(value) => (status, Json(value)),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "schemaVersion": SreHealthViewV1::SCHEMA_VERSION,
                "ready": false,
                "overallStatus": "unavailable",
                "reason": "unknown",
            })),
        ),
    }
}

async fn metrics(State(state): State<AppState>) -> ([(&'static str, &'static str); 1], String) {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.sre_metrics.render_prometheus(),
    )
}

#[cfg(test)]
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
    let auth = state.auth.authorize(&headers, None).await?;
    if !auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "rocketmq:onboard" | "rocketmq:sre" | "sre-admin" | "admin"
        )
    }) {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "cluster onboarding requires the rocketmq:onboard role",
        ));
    }
    if request.tenant_id != auth.tenant_id.to_string() || request.actor_subject != auth.subject {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "onboarding tenant and actor must match the authenticated identity",
        ));
    }
    request.validate()?;
    let outcome = state.repository.onboard(&request).await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(outcome)))
}

async fn list_clusters(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Cluster>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let clusters = state
        .repository
        .list()
        .await?
        .into_iter()
        .filter(|cluster| cluster.tenant_id == auth.tenant_id.to_string() && auth.clusters.contains(&cluster.id))
        .collect();
    Ok(Json(clusters))
}

async fn get_cluster(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Cluster>, ControlPlaneError> {
    let id = parse_cluster_id(&id)?;
    let auth = state.auth.authorize(&headers, Some(id)).await?;
    let cluster = state.repository.get(id).await?;
    ensure_cluster_authorized(&auth, &cluster)?;
    Ok(Json(cluster))
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

async fn connector_handshake(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<HandshakeRequest>,
) -> Result<Json<HandshakeOutcome>, ControlPlaneError> {
    let principal = state.connector_channel.authenticate(&headers)?;
    if request.connector_subject != principal.subject || request.connector_issuer != principal.issuer {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector handshake identity does not match the mTLS principal",
        ));
    }
    let id = parse_cluster_id(&id)?;
    let decision = request.validate()?;
    state.repository.handshake(id, &request, &decision).await.map(Json)
}

async fn connector_cluster_state(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Cluster>, ControlPlaneError> {
    let principal = state.connector_channel.authenticate(&headers)?;
    let id = parse_cluster_id(&id)?;
    if !state
        .repository
        .connector_identity_known(id, &principal.subject, &principal.issuer)
        .await?
    {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector identity is not registered for the requested cluster",
        ));
    }
    state.repository.get(id).await.map(Json)
}

async fn get_capability(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<CapabilitySnapshot>, ControlPlaneError> {
    let id = parse_cluster_id(&id)?;
    let auth = state.auth.authorize(&headers, Some(id)).await?;
    let cluster = state.repository.get(id).await?;
    ensure_cluster_authorized(&auth, &cluster)?;
    state.repository.capability(id).await.map(Json)
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

async fn capabilities(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ControlPlaneError> {
    let _auth = state.auth.authorize(&headers, None).await?;
    let providers = rocketmq_sre_model_gateway::phase00_provider_descriptors();
    Ok(Json(json!({
        "schema_version": "rocketmq-sre.capabilities.v1",
        "phase": "03",
        "effective_access_profile": "human_approved_supervised",
        "execution_supported": false,
        "execution_submission_supported": true,
        "approval_supported": true,
        "unattended_execution_supported": false,
        "arbitrary_mutation_supported": false,
        "provider_network_calls_supported": true,
        "providers": providers,
        "catalog": state.documents.catalog.as_ref(),
        "data_classification": state.documents.data_classification.as_ref(),
        "required_source_profiles": state.documents.required_source_profiles.as_ref()
    })))
}

async fn phase2_contract_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Phase2ContractManifest>, ControlPlaneError> {
    let _auth = state.auth.authorize(&headers, None).await?;
    Ok(Json(Phase2ContractManifest::default()))
}

async fn coverage(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<crate::coverage::CoverageQuery>,
) -> Result<Json<Value>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    crate::coverage::matrix(&state, &auth, query).await.map(Json)
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

fn ensure_cluster_authorized(auth: &crate::auth::AuthContext, cluster: &Cluster) -> Result<(), ControlPlaneError> {
    if cluster.tenant_id != auth.tenant_id.to_string() || !auth.clusters.contains(&cluster.id) {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "cluster is outside the authenticated tenant scope",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

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

    #[tokio::test]
    async fn public_listener_does_not_mount_connector_internal_routes() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .expect("lazy PostgreSQL pool");
        let repository = PostgresRepository::from_pool(pool);
        let documents = CapabilityDocuments::embedded().expect("capability documents");
        let internal_token = Arc::<str>::from("test-internal-token");
        let auth = AuthService::development(internal_token.clone());
        let model_gateway = ModelGatewayService::disabled(repository.clone());
        let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(16));
        let routers = build_routers_with_auth(
            repository,
            documents,
            internal_token.clone(),
            internal_token.clone(),
            internal_token,
            crate::supervised_execution::ExecutorSubmissionClient::disabled(),
            auth,
            EvidenceBlobStore::in_memory(64 * 1024),
            DashboardDeepLinkPolicy::disabled(),
            model_gateway,
            workflow,
        )
        .expect("router pair");
        for (method, path) in [
            ("POST", "/internal/v1/connectors/v1/register"),
            (
                "POST",
                "/internal/v1/connectors/v1/clusters/00000000-0000-4000-8000-000000000001/handshake",
            ),
            (
                "GET",
                "/internal/v1/connectors/v1/clusters/00000000-0000-4000-8000-000000000001",
            ),
            ("POST", "/internal/v1/inventory"),
            ("POST", "/internal/v1/evidence"),
        ] {
            let request = || {
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .expect("request")
            };

            let public_response = routers
                .public
                .clone()
                .oneshot(request())
                .await
                .expect("public response");
            assert_eq!(public_response.status(), StatusCode::NOT_FOUND, "{path}");

            let connector_response = routers
                .connector
                .clone()
                .oneshot(request())
                .await
                .expect("connector response");
            assert_ne!(connector_response.status(), StatusCode::NOT_FOUND, "{path}");
        }
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
