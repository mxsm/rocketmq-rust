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

use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use axum::routing::get;
use axum::routing::post;
use rocketmq_sre_contracts::ConversationId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::InvestigationId;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::RecommendationId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::alerting::AlertIngestionOutcome;
use crate::alerting::AlertmanagerWebhook;
use crate::alerting::IncidentNoteRequest;
use crate::alerting::IncidentTopologyView;
use crate::alerting::IntegrationEventRequest;
use crate::alerting::NotificationTestRequest;
use crate::alerting::NotificationTestResponse;
use crate::api::AppState;
use crate::assets::AssetKey;
use crate::assets::AssetKind;
use crate::assets::AssetListQuery;
use crate::assets::AssetPage;
use crate::assets::DashboardDeepLink;
use crate::assets::IngestInventoryRequest;
use crate::assets::InventorySnapshot;
use crate::assets::TopologyDiff;
use crate::evidence::EvidenceListQuery;
use crate::evidence::EvidencePage;
use crate::evidence::PersistEvidenceRequest;
use crate::inspection::InspectionReport;
use crate::inspection::InspectionService;
use crate::knowledge::ImportKnowledgeRequest;
use crate::knowledge::KnowledgeFeedbackRequest;
use crate::knowledge::KnowledgeImportResult;
use crate::knowledge::KnowledgeListQuery;
use crate::knowledge::KnowledgePage;
use crate::knowledge::KnowledgeReviewRequest;
use crate::knowledge::KnowledgeSearchPage;
use crate::knowledge::KnowledgeSearchQuery;
use crate::models::ModelCapabilitiesStatus;
use crate::models::ModelInvocationListQuery;
use crate::models::ModelInvocationPage;
use crate::observability::CORRELATION_ID_HEADER;
use crate::observability::CorrelationContext;
use crate::orchestrator::DiagnosisResponse;
use crate::orchestrator::OrchestratorService;
use crate::workflow::ConversationCreateRequest;
use crate::workflow::ConversationView;
use crate::workflow::IncidentCreateRequest;
use crate::workflow::IncidentView;
use crate::workflow::InspectionCreateRequest;
use crate::workflow::InspectionView;
use crate::workflow::InvestigationCreateRequest;
use crate::workflow::InvestigationView;
use crate::workflow::PromoteInvestigationRequest;
use crate::workflow::RecommendationDispositionRequest;
use crate::workflow::WorkflowListQuery;
use crate::workflow::WorkflowPage;

pub(crate) fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/conversations", get(list_conversations).post(create_conversation))
        .route("/v1/conversations/{id}", get(get_conversation))
        .route(
            "/v1/investigations",
            get(list_investigations).post(create_investigation),
        )
        .route("/v1/investigations/{id}", get(get_investigation))
        .route("/v1/investigations/{id}/promote", post(promote_investigation))
        .route("/v1/incidents", get(list_incidents).post(create_incident))
        .route("/v1/incidents/{id}", get(get_incident))
        .route("/v1/incidents/{id}/diagnose", post(diagnose_incident))
        .route("/v1/incidents/{id}/timeline", get(get_incident_timeline))
        .route("/v1/incidents/{id}/topology", get(get_incident_topology))
        .route(
            "/v1/incidents/{id}/notes",
            post(add_incident_note).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/integrations/alertmanager/events",
            post(ingest_alertmanager).layer(DefaultBodyLimit::max(256 * 1024)),
        )
        .route(
            "/v1/integrations/events",
            post(ingest_integration_event).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/integrations/webhook/test",
            post(test_notification_webhook).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/v1/clusters/{id}/slo", get(get_cluster_slo))
        .route("/v1/clusters/{id}/health", get(get_cluster_health))
        .route("/v1/fleet/health", get(get_fleet_health))
        .route("/v1/inspections", get(list_inspections).post(create_inspection))
        .route("/v1/inspections/{id}", get(get_inspection))
        .route("/v1/inspections/{id}/run", post(run_inspection))
        .route("/v1/inspections/{id}/report", get(get_inspection_report))
        .route("/v1/recommendations", get(list_recommendations))
        .route("/v1/recommendations/{id}/disposition", post(disposition_recommendation))
        .route("/v1/inventory/{id}", get(get_inventory_snapshot))
        .route("/v1/clusters/{id}/inventory/latest", get(get_latest_inventory))
        .route("/v1/clusters/{id}/connector", get(get_connector_status))
        .route("/v1/assets", get(list_assets))
        .route("/v1/assets/dashboard-link", get(get_dashboard_link))
        .route("/v1/topology", get(get_latest_topology))
        .route("/v1/topology/diff", get(get_latest_topology_diff))
        .route("/v1/evidence", get(list_evidence))
        .route("/v1/evidence/{id}", get(get_evidence))
        .route("/v1/evidence/{id}/content", get(get_evidence_content))
        .route("/v1/message-journeys", get(get_message_journey))
        .route("/v1/knowledge/import", post(import_knowledge))
        .route("/v1/knowledge", get(list_knowledge))
        .route("/v1/knowledge/search", get(search_knowledge))
        .route("/v1/knowledge/{id}", get(get_knowledge))
        .route("/v1/knowledge/{id}/review", post(review_knowledge))
        .route("/v1/knowledge/{id}/feedback", post(feedback_knowledge))
        .route("/v1/events/stream", get(event_stream))
        .route("/v1/models/capabilities", get(model_capabilities))
        .route("/v1/models/status", get(model_status))
        .route("/v1/models/invocations", get(model_invocations))
        .route("/v1/openapi.json", get(openapi))
}

pub(crate) fn connector_ingest_routes() -> Router<AppState> {
    Router::new()
        .route("/internal/v1/inventory", post(ingest_inventory))
        .route("/internal/v1/evidence", post(persist_evidence))
}

async fn get_connector_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ControlPlaneError> {
    let cluster_id = parse_cluster_id(&id)?;
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    let status = state.connector_channel.status(auth.tenant_id, cluster_id).await?;
    Ok(Json(json!({
        "schemaVersion": "rocketmq-sre.connector-status.v1",
        "clusterId": cluster_id,
        "status": status,
    })))
}

async fn ingest_inventory(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IngestInventoryRequest>,
) -> Result<Json<(InventorySnapshot, TopologyDiff)>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.assets.ingest(&auth, &request).await.map(Json)
}

async fn get_inventory_snapshot(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<InventorySnapshot>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .assets
        .snapshot(&auth, parse_uuid(&id, "inventory snapshot")?)
        .await
        .map(Json)
}

async fn get_latest_inventory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Option<InventorySnapshot>>, ControlPlaneError> {
    let cluster_id = parse_cluster_id(&id)?;
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    state.assets.latest(&auth, cluster_id).await.map(Json)
}

async fn list_assets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AssetListQuery>,
) -> Result<Json<AssetPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.assets.assets(&auth, &query).await.map(Json)
}

#[derive(Deserialize)]
struct TopologyQuery {
    cluster_id: rocketmq_sre_contracts::ClusterId,
}

async fn get_latest_topology(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TopologyQuery>,
) -> Result<Json<Option<InventorySnapshot>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.assets.latest(&auth, query.cluster_id).await.map(Json)
}

async fn get_latest_topology_diff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TopologyQuery>,
) -> Result<Json<Option<TopologyDiff>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.assets.latest_diff(&auth, query.cluster_id).await.map(Json)
}

#[derive(Deserialize)]
struct DashboardLinkQuery {
    cluster_id: rocketmq_sre_contracts::ClusterId,
    kind: AssetKind,
    external_key: String,
}

async fn get_dashboard_link(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DashboardLinkQuery>,
) -> Result<Json<Option<DashboardDeepLink>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    let key = AssetKey::new(query.kind, query.external_key)?;
    state.assets.dashboard_link(&auth, query.cluster_id, &key).map(Json)
}

async fn create_conversation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ConversationCreateRequest>,
) -> Result<Json<ConversationView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .workflow
        .create_conversation(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_conversations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<WorkflowPage<ConversationView>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.workflow.list_conversations(&auth, &query).await.map(Json)
}

async fn get_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ConversationView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .workflow
        .conversation(&auth, parse_conversation_id(&id)?)
        .await
        .map(Json)
}

async fn import_knowledge(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ImportKnowledgeRequest>,
) -> Result<Json<KnowledgeImportResult>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.knowledge.import(&auth, request).await.map(Json)
}

async fn search_knowledge(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<KnowledgeSearchQuery>,
) -> Result<Json<KnowledgeSearchPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.knowledge.search(&auth, &query).await.map(Json)
}

async fn list_knowledge(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<KnowledgeListQuery>,
) -> Result<Json<KnowledgePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.knowledge.list(&auth, &query).await.map(Json)
}

async fn get_knowledge(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<KnowledgeItem>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.knowledge.item(&auth, parse_knowledge_id(&id)?).await.map(Json)
}

async fn review_knowledge(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<KnowledgeReviewRequest>,
) -> Result<Json<KnowledgeItem>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .knowledge
        .review(&auth, parse_knowledge_id(&id)?, &request)
        .await
        .map(Json)
}

async fn feedback_knowledge(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<KnowledgeFeedbackRequest>,
) -> Result<Json<KnowledgeItem>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .knowledge
        .feedback(&auth, parse_knowledge_id(&id)?, &request)
        .await
        .map(Json)
}

async fn persist_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PersistEvidenceRequest>,
) -> Result<Json<EvidenceSnapshot>, ControlPlaneError> {
    let auth = state
        .auth
        .authorize(&headers, Some(request.evidence.cluster_id))
        .await?;
    state.evidence.persist(&auth, request).await.map(Json)
}

async fn list_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EvidenceListQuery>,
) -> Result<Json<EvidencePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.evidence.list(&auth, &query).await.map(Json)
}

async fn get_evidence(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<EvidenceSnapshot>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.evidence.get(&auth, parse_evidence_id(&id)?).await.map(Json)
}

async fn get_evidence_content(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let content = state.evidence.content(&auth, parse_evidence_id(&id)?).await?;
    Ok(([(header::CONTENT_TYPE, "application/json")], Body::from(content)).into_response())
}

#[derive(Deserialize)]
struct MessageJourneyQuery {
    cluster_id: rocketmq_sre_contracts::ClusterId,
    query: String,
}

#[derive(Serialize)]
struct MessageJourney {
    schema_version: &'static str,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    trace_fingerprint: String,
    topic: Option<String>,
    queue_id: Option<u32>,
    message_body_available: bool,
    partial: bool,
    warnings: Vec<String>,
    hops: Vec<MessageJourneyHop>,
}

#[derive(Serialize)]
struct MessageJourneyHop {
    stage: String,
    component: String,
    observed_at: chrono::DateTime<chrono::Utc>,
    status: String,
    latency_ms: Option<u64>,
    evidence_id: EvidenceId,
    detail: String,
}

async fn get_message_journey(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MessageJourneyQuery>,
) -> Result<Json<MessageJourney>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    let fingerprint = message_fingerprint(auth.tenant_id, query.cluster_id, &query.query)?;
    let evidence = state
        .repository
        .message_journey_evidence(&auth, query.cluster_id, &fingerprint)
        .await?;
    let mut topic = None;
    let mut queue_id = None;
    let mut hops = Vec::new();
    let mut warnings = Vec::new();
    for snapshot in evidence {
        let EvidenceContent::Inline(content) = &snapshot.content else {
            warnings.push("referenced_message_metadata_not_loaded".to_owned());
            continue;
        };
        if topic.is_none() {
            topic = bounded_text(content.get("topic"), 255);
        }
        if queue_id.is_none() {
            queue_id = content
                .get("queue_id")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok());
        }
        let Some(values) = content.get("hops").and_then(Value::as_array) else {
            continue;
        };
        for value in values.iter().take(64_usize.saturating_sub(hops.len())) {
            if let Some(hop) = parse_message_hop(value, snapshot.evidence_id) {
                hops.push(hop);
            }
        }
    }
    if hops.is_empty() {
        warnings.push("message_journey_evidence_missing".to_owned());
    }
    warnings.sort();
    warnings.dedup();
    warnings.truncate(8);
    Ok(Json(MessageJourney {
        schema_version: "rocketmq-sre.message-journey.v1",
        cluster_id: query.cluster_id,
        trace_fingerprint: fingerprint,
        topic,
        queue_id,
        message_body_available: false,
        partial: !warnings.is_empty(),
        warnings,
        hops,
    }))
}

fn message_fingerprint(
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    query: &str,
) -> Result<String, ControlPlaneError> {
    let query = query.trim();
    if query.is_empty()
        || query.len() > 512
        || !query
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
    {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "message or trace identifier must be a bounded opaque identifier, not message content",
        ));
    }
    if let Some(digest) = query.strip_prefix("sha256:")
        && digest.len() == 64
        && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(format!("sha256:{}", digest.to_ascii_lowercase()));
    }
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_uuid().as_bytes());
    hasher.update(cluster_id.as_uuid().as_bytes());
    hasher.update(query.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn parse_message_hop(value: &Value, evidence_id: EvidenceId) -> Option<MessageJourneyHop> {
    let stage = bounded_text(value.get("stage"), 32)?;
    if !matches!(stage.as_str(), "producer" | "proxy" | "broker" | "store" | "consumer") {
        return None;
    }
    let status = bounded_text(value.get("status"), 16)?;
    if !matches!(status.as_str(), "observed" | "partial" | "missing") {
        return None;
    }
    let observed_at = value
        .get("observed_at")?
        .as_str()?
        .parse::<chrono::DateTime<chrono::Utc>>()
        .ok()?;
    Some(MessageJourneyHop {
        stage,
        component: bounded_text(value.get("component"), 128)?,
        observed_at,
        status,
        latency_ms: value.get("latency_ms").and_then(Value::as_u64),
        evidence_id,
        detail: bounded_text(value.get("detail"), 512)?,
    })
}

fn bounded_text(value: Option<&Value>, max_chars: usize) -> Option<String> {
    let value = value?.as_str()?.trim();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.to_owned())
}

async fn create_investigation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InvestigationCreateRequest>,
) -> Result<Json<InvestigationView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .workflow
        .create_investigation(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_investigations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<WorkflowPage<InvestigationView>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.workflow.list_investigations(&auth, &query).await.map(Json)
}

async fn get_investigation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<InvestigationView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .workflow
        .investigation(&auth, parse_investigation_id(&id)?)
        .await
        .map(Json)
}

async fn promote_investigation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PromoteInvestigationRequest>,
) -> Result<Json<IncidentView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .workflow
        .promote_investigation(&auth, parse_investigation_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn create_incident(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IncidentCreateRequest>,
) -> Result<Json<IncidentView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .workflow
        .create_incident(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_incidents(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<WorkflowPage<IncidentView>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.workflow.list_incidents(&auth, &query).await.map(Json)
}

async fn get_incident(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<IncidentView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.workflow.incident(&auth, parse_incident_id(&id)?).await.map(Json)
}

async fn ingest_alertmanager(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AlertmanagerWebhook>,
) -> Result<Json<Vec<AlertIngestionOutcome>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .alerting
        .ingest_alertmanager(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn ingest_integration_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IntegrationEventRequest>,
) -> Result<Json<AlertIngestionOutcome>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .alerting
        .ingest_integration_event(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn get_incident_timeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Vec<rocketmq_sre_contracts::TimelineEvent>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.alerting.timeline(&auth, parse_incident_id(&id)?).await.map(Json)
}

async fn get_incident_topology(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<IncidentTopologyView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.alerting.topology(&auth, parse_incident_id(&id)?).await.map(Json)
}

async fn add_incident_note(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<IncidentNoteRequest>,
) -> Result<Json<rocketmq_sre_contracts::TimelineEvent>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .alerting
        .add_note(&auth, parse_incident_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn get_cluster_slo(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<rocketmq_sre_contracts::ClusterHealthReport>, ControlPlaneError> {
    let cluster_id = parse_cluster_id(&id)?;
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    state.slo.cluster_report(&auth, cluster_id).await.map(Json)
}

async fn get_cluster_health(
    state: State<AppState>,
    path: Path<String>,
    headers: HeaderMap,
) -> Result<Json<rocketmq_sre_contracts::ClusterHealthReport>, ControlPlaneError> {
    get_cluster_slo(state, path, headers).await
}

#[derive(Deserialize)]
struct FleetHealthQuery {
    region: Option<String>,
}

async fn get_fleet_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FleetHealthQuery>,
) -> Result<Json<rocketmq_sre_contracts::FleetHealthReport>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.slo.fleet_report(&auth, query.region.as_deref()).await.map(Json)
}

async fn test_notification_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<NotificationTestRequest>,
) -> Result<Json<NotificationTestResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.alerting.test_notification(&auth, &request).await.map(Json)
}

async fn diagnose_incident(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<DiagnosisResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let orchestrator = OrchestratorService::new(
        state.workflow.clone(),
        state.evidence.clone(),
        state.observability.clone(),
    )?
    .with_connector_channel(state.connector_channel.clone())
    .with_model_gateway(state.model_gateway.clone());
    orchestrator
        .diagnose(&auth, parse_incident_id(&id)?, correlation_id(&headers))
        .await
        .map(Json)
}

async fn create_inspection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InspectionCreateRequest>,
) -> Result<Json<InspectionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    let inspections = InspectionService::new(state.repository.clone(), state.workflow.clone(), state.evidence.clone())?;
    inspections
        .create(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_inspections(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<WorkflowPage<InspectionView>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.workflow.list_inspections(&auth, &query).await.map(Json)
}

async fn get_inspection(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<InspectionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .workflow
        .inspection(&auth, parse_inspection_id(&id)?)
        .await
        .map(Json)
}

async fn run_inspection(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<InspectionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let inspections = InspectionService::new(state.repository.clone(), state.workflow.clone(), state.evidence.clone())?;
    inspections
        .execute(&auth, parse_inspection_id(&id)?, correlation_id(&headers))
        .await
        .map(Json)
}

#[derive(Deserialize)]
struct InspectionReportQuery {
    #[serde(default = "default_report_format")]
    format: String,
}

fn default_report_format() -> String {
    "markdown".to_owned()
}

async fn get_inspection_report(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<InspectionReportQuery>,
) -> Result<Json<InspectionReport>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let inspections = InspectionService::new(state.repository.clone(), state.workflow.clone(), state.evidence.clone())?;
    inspections
        .report(&auth, parse_inspection_id(&id)?, &query.format)
        .await
        .map(Json)
}

async fn disposition_recommendation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RecommendationDispositionRequest>,
) -> Result<Json<rocketmq_sre_contracts::Recommendation>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .workflow
        .disposition_recommendation(&auth, parse_recommendation_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_recommendations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<WorkflowPage<rocketmq_sre_contracts::Recommendation>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.workflow.list_recommendations(&auth, &query).await.map(Json)
}

async fn event_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let tenant_id = auth.tenant_id;
    let clusters = auth.clusters;
    let stream = BroadcastStream::new(state.workflow.subscribe()).filter_map(move |result| match result {
        Ok(event) if event.tenant_id == tenant_id && clusters.contains(&event.cluster_id) => {
            let payload = serde_json::to_string(&event).ok()?;
            Some(Ok(Event::default().event(event.event_type).data(payload)))
        }
        Ok(_) | Err(_) => None,
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("rocketmq-sre")))
}

async fn model_capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ModelCapabilitiesStatus>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.model_gateway.capabilities_status(&auth).await.map(Json)
}

async fn model_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ModelCapabilitiesStatus>, ControlPlaneError> {
    model_capabilities(State(state), headers).await
}

async fn model_invocations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ModelInvocationListQuery>,
) -> Result<Json<ModelInvocationPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.model_gateway.invocations(&auth, &query).await.map(Json)
}

async fn openapi(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ControlPlaneError> {
    let _auth = state.auth.authorize(&headers, None).await?;
    Ok(Json(crate::openapi::document()))
}

fn correlation_id(headers: &HeaderMap) -> CorrelationId {
    CorrelationContext::from_optional_header(headers.get(CORRELATION_ID_HEADER).and_then(|value| value.to_str().ok()))
        .id()
}

fn parse_conversation_id(value: &str) -> Result<ConversationId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "conversation identifier must be a UUID"))
}

fn parse_cluster_id(value: &str) -> Result<rocketmq_sre_contracts::ClusterId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("cluster_not_allowed", "cluster identifier must be a UUID"))
}

fn parse_uuid(value: &str, name: &str) -> Result<Uuid, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", format!("{name} identifier must be a UUID")))
}

fn parse_investigation_id(value: &str) -> Result<InvestigationId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "investigation identifier must be a UUID"))
}

fn parse_incident_id(value: &str) -> Result<IncidentId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "incident identifier must be a UUID"))
}

fn parse_evidence_id(value: &str) -> Result<EvidenceId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "evidence identifier must be a UUID"))
}

fn parse_knowledge_id(value: &str) -> Result<KnowledgeItemId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "knowledge identifier must be a UUID"))
}

fn parse_inspection_id(value: &str) -> Result<InspectionRunId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "inspection identifier must be a UUID"))
}

fn parse_recommendation_id(value: &str) -> Result<RecommendationId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "recommendation identifier must be a UUID"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_identifier_is_pseudonymized_and_control_characters_are_rejected() {
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = rocketmq_sre_contracts::ClusterId::new();
        let fingerprint = message_fingerprint(tenant_id, cluster_id, "raw-message-id").expect("fingerprint");

        assert!(fingerprint.starts_with("sha256:"));
        assert_eq!(fingerprint.len(), 71);
        assert!(!fingerprint.contains("raw-message-id"));
        assert!(message_fingerprint(tenant_id, cluster_id, "body\nsecret").is_err());
    }

    #[test]
    fn message_hop_parser_accepts_only_explicit_bounded_metadata() {
        let evidence_id = EvidenceId::new();
        let hop = parse_message_hop(
            &json!({
                "stage": "broker",
                "component": "broker-a",
                "observed_at": "2026-07-27T10:00:00Z",
                "status": "observed",
                "latency_ms": 4,
                "detail": "stored"
            }),
            evidence_id,
        )
        .expect("valid hop");
        assert_eq!(hop.evidence_id, evidence_id);
        assert!(parse_message_hop(&json!({"stage": "message_body"}), evidence_id).is_none());
    }
}
