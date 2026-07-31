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

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::get;
use axum::routing::post;
use chrono::Utc;
use rocketmq_sre_contracts::AUTOMATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::AutomationBudget;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EnterpriseIntegrationEventKind;
use rocketmq_sre_contracts::IntegrationDeliveryId;
use rocketmq_sre_contracts::IntegrationDescriptor;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_contracts::PreventiveAutomationRequest;
use rocketmq_sre_contracts::PreventiveRiskFamily;
use rocketmq_sre_contracts::ReleaseId;

use super::model::CompleteRollbackRequest;
use super::model::CreateReleaseRequest;
use super::model::EnterpriseEventListQuery;
use super::model::EnterpriseEventPage;
use super::model::EnterpriseIngressAuthorization;
use super::model::EnterpriseIngressRequest;
use super::model::EnterpriseIngressView;
use super::model::ExternalApprovalRequest;
use super::model::ExternalApprovalView;
use super::model::IntegrationDeliveryListQuery;
use super::model::IntegrationDeliveryPage;
use super::model::IntegrationHealthView;
use super::model::IntegrationTargetListQuery;
use super::model::IntegrationTargetPage;
use super::model::IntegrationTargetView;
use super::model::PrepareReleaseRequest;
use super::model::RecordReleaseObservationRequest;
use super::model::RegisterIntegrationTargetRequest;
use super::model::ReleaseDetail;
use super::model::ReleaseExecutionRequest;
use super::model::ReleaseExecutionView;
use super::model::ReleaseListQuery;
use super::model::ReleasePage;
use super::model::ReleasePreparationView;
use super::model::ReleaseTransitionRequest;
use super::model::ReplayIntegrationDeliveryRequest;
use super::model::ReplayIntegrationDeliveryView;
use super::model::RotateIntegrationSecretRequest;
use super::model::SetIntegrationTargetStateRequest;
use crate::ControlPlaneError;
use crate::api::AppState;
use crate::observability::CORRELATION_ID_HEADER;

const ENTERPRISE_EVENT_TIMESTAMP_HEADER: &str = "x-rocketmq-sre-event-timestamp";
const ENTERPRISE_EVENT_NONCE_HEADER: &str = "x-rocketmq-sre-event-nonce";
const ENTERPRISE_EVENT_SIGNATURE_HEADER: &str = "x-rocketmq-sre-signature";

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/integrations/descriptors", get(integration_descriptors))
        .route(
            "/v1/integrations/targets",
            post(register_integration_target)
                .get(list_integration_targets)
                .layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route("/v1/integrations/targets/{id}", get(get_integration_target))
        .route(
            "/v1/integrations/targets/{id}/events",
            post(ingest_enterprise_event)
                .get(list_enterprise_events)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/integrations/targets/{id}/config-test",
            post(test_integration_config),
        )
        .route("/v1/integrations/targets/{id}/health", get(get_integration_health))
        .route(
            "/v1/integrations/targets/{id}/state",
            post(set_integration_target_state).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/integrations/targets/{id}/secret-reference/rotate",
            post(rotate_integration_secret).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route("/v1/integrations/deliveries", get(list_integration_deliveries))
        .route(
            "/v1/integrations/deliveries/{id}/replay",
            post(replay_integration_delivery).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/integrations/approvals/external",
            post(apply_external_approval).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/v1/releases",
            post(create_release)
                .get(list_releases)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route("/v1/releases/{id}", get(get_release))
        .route(
            "/v1/releases/{id}/prepare",
            post(prepare_release).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/releases/{id}/start",
            post(start_release).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/releases/{id}/observations",
            post(record_release_observation).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/releases/{id}/pause",
            post(pause_release).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/releases/{id}/resume",
            post(resume_release).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route("/v1/releases/{id}/verification/start", post(begin_release_verification))
        .route("/v1/releases/{id}/complete", post(complete_release))
        .route(
            "/v1/releases/{id}/rollback/start",
            post(start_release_rollback).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/releases/{id}/rollback/complete",
            post(complete_release_rollback).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/releases/{id}/manual-takeover",
            post(manual_release_takeover).layer(DefaultBodyLimit::max(8 * 1024)),
        )
}

async fn integration_descriptors(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<IntegrationDescriptor>>, ControlPlaneError> {
    state.auth.authorize(&headers, None).await?;
    Ok(Json(super::ReleaseManagementService::integration_descriptors()))
}

async fn register_integration_target(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RegisterIntegrationTargetRequest>,
) -> Result<Json<IntegrationTargetView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .release_management
        .register_integration_target(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_integration_targets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<IntegrationTargetListQuery>,
) -> Result<Json<IntegrationTargetPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .release_management
        .integration_targets(&auth, &query)
        .await
        .map(Json)
}

async fn get_integration_target(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<IntegrationTargetView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .integration_target(&auth, parse_target_id(&id)?)
        .await
        .map(Json)
}

async fn ingest_enterprise_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<EnterpriseIngressRequest>,
) -> Result<Json<EnterpriseIngressView>, ControlPlaneError> {
    let cluster_id = request.payload.cluster_id();
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    let target_id = parse_target_id(&id)?;
    let authorization = enterprise_authorization(&headers)?;
    let mut view = state
        .release_management
        .ingest_enterprise_event(&auth, target_id, &authorization, &request)
        .await?;
    if view.followup_id.is_none()
        && matches!(
            view.event.event_kind,
            EnterpriseIntegrationEventKind::ReleaseStarted
                | EnterpriseIntegrationEventKind::ReleaseCanary
                | EnterpriseIntegrationEventKind::ReleasePromoted
        )
    {
        let run = state
            .preventive_automation
            .submit(
                &auth,
                &PreventiveAutomationRequest {
                    schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
                    id: AutomationRunId::new(),
                    tenant_id: auth.tenant_id,
                    cluster_id,
                    correlation_id: correlation_id(&headers),
                    risk_family: PreventiveRiskFamily::Upgrade,
                    idempotency_key: format!("cicd-readiness:{}", view.event.id),
                    budget: AutomationBudget {
                        max_model_calls: 0,
                        max_output_bytes: 64 * 1_024,
                        timeout_seconds: 120,
                    },
                    requested_by: auth.subject.clone(),
                    requested_at: Utc::now(),
                },
            )
            .await?;
        state
            .release_management
            .record_enterprise_followup(&auth, view.event.id, run.id.as_uuid())
            .await?;
        view.followup_id = Some(run.id.as_uuid());
    }
    Ok(Json(view))
}

async fn list_enterprise_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<EnterpriseEventListQuery>,
) -> Result<Json<EnterpriseEventPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .enterprise_events(&auth, parse_target_id(&id)?, &query)
        .await
        .map(Json)
}

async fn test_integration_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<IntegrationHealthView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .test_integration_config(&auth, parse_target_id(&id)?)
        .await
        .map(Json)
}

async fn get_integration_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<IntegrationHealthView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .integration_health(&auth, parse_target_id(&id)?)
        .await
        .map(Json)
}

async fn set_integration_target_state(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<SetIntegrationTargetStateRequest>,
) -> Result<Json<IntegrationTargetView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .set_integration_target_state(&auth, parse_target_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn rotate_integration_secret(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RotateIntegrationSecretRequest>,
) -> Result<Json<IntegrationTargetView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .rotate_integration_secret(&auth, parse_target_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_integration_deliveries(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<IntegrationDeliveryListQuery>,
) -> Result<Json<IntegrationDeliveryPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .release_management
        .integration_deliveries(&auth, &query)
        .await
        .map(Json)
}

async fn replay_integration_delivery(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReplayIntegrationDeliveryRequest>,
) -> Result<Json<ReplayIntegrationDeliveryView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .replay_integration_delivery(&auth, parse_delivery_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn apply_external_approval(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ExternalApprovalRequest>,
) -> Result<Json<ExternalApprovalView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .apply_external_approval(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn create_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateReleaseRequest>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .release_management
        .create_release(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_releases(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ReleaseListQuery>,
) -> Result<Json<ReleasePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.release_management.releases(&auth, &query).await.map(Json)
}

async fn get_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .release(&auth, parse_release_id(&id)?)
        .await
        .map(Json)
}

async fn prepare_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<PrepareReleaseRequest>,
) -> Result<Json<ReleasePreparationView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .prepare_release(&auth, parse_release_id(&id)?, &request)
        .await
        .map(Json)
}

async fn start_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReleaseExecutionRequest>,
) -> Result<Json<ReleaseExecutionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .start_release(&auth, parse_release_id(&id)?, &request)
        .await
        .map(Json)
}

async fn record_release_observation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RecordReleaseObservationRequest>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .record_release_observation(&auth, parse_release_id(&id)?, request)
        .await
        .map(Json)
}

async fn pause_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReleaseTransitionRequest>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    transition_release_request(state, headers, id, request, ReleaseTransitionKind::Pause).await
}

async fn resume_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReleaseTransitionRequest>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    transition_release_request(state, headers, id, request, ReleaseTransitionKind::Resume).await
}

async fn begin_release_verification(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .begin_release_verification(&auth, parse_release_id(&id)?)
        .await
        .map(Json)
}

async fn complete_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .complete_release(&auth, parse_release_id(&id)?)
        .await
        .map(Json)
}

async fn start_release_rollback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReleaseExecutionRequest>,
) -> Result<Json<ReleaseExecutionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .start_release_rollback(&auth, parse_release_id(&id)?, &request)
        .await
        .map(Json)
}

async fn complete_release_rollback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<CompleteRollbackRequest>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .release_management
        .complete_release_rollback(&auth, parse_release_id(&id)?, request)
        .await
        .map(Json)
}

async fn manual_release_takeover(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReleaseTransitionRequest>,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    transition_release_request(state, headers, id, request, ReleaseTransitionKind::ManualTakeover).await
}

enum ReleaseTransitionKind {
    Pause,
    Resume,
    ManualTakeover,
}

async fn transition_release_request(
    state: AppState,
    headers: HeaderMap,
    id: String,
    request: ReleaseTransitionRequest,
    kind: ReleaseTransitionKind,
) -> Result<Json<ReleaseDetail>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    let release_id = parse_release_id(&id)?;
    match kind {
        ReleaseTransitionKind::Pause => {
            state
                .release_management
                .pause_release(&auth, release_id, &request)
                .await
        }
        ReleaseTransitionKind::Resume => {
            state
                .release_management
                .resume_release(&auth, release_id, &request)
                .await
        }
        ReleaseTransitionKind::ManualTakeover => {
            state
                .release_management
                .manual_release_takeover(&auth, release_id, &request)
                .await
        }
    }
    .map(Json)
}

fn parse_target_id(value: &str) -> Result<IntegrationTargetId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "integration target identifier must be a UUID"))
}

fn parse_delivery_id(value: &str) -> Result<IntegrationDeliveryId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "integration delivery identifier must be a UUID"))
}

fn parse_release_id(value: &str) -> Result<ReleaseId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "release identifier must be a UUID"))
}

fn correlation_id(headers: &HeaderMap) -> CorrelationId {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn enterprise_authorization(headers: &HeaderMap) -> Result<EnterpriseIngressAuthorization, ControlPlaneError> {
    Ok(EnterpriseIngressAuthorization {
        timestamp: required_header(headers, ENTERPRISE_EVENT_TIMESTAMP_HEADER)?,
        nonce: required_header(headers, ENTERPRISE_EVENT_NONCE_HEADER)?,
        signature: required_header(headers, ENTERPRISE_EVENT_SIGNATURE_HEADER)?,
    })
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ControlPlaneError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty() && value.len() <= 512)
        .map(str::to_owned)
        .ok_or_else(|| {
            ControlPlaneError::validation(
                "integration_signature_invalid",
                "signed integration headers are required",
            )
        })
}
