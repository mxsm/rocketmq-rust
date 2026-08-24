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
use crate::error::DashboardError;
use crate::middleware::AuditTerminalFactSink;
use crate::middleware::successful_mutation_audit_event;
use crate::model::ApiResponse;
use crate::model::AuditAction;
use crate::model::AuditResourceType;
use crate::model::AuthenticatedActor;
use crate::model::ConsumerMonitorMutationResult;
use crate::model::ConsumerMonitorUpsertRequest;
use crate::model::ConsumerMonitorView;
use crate::model::MonitorDeleteQuery;
use crate::model::MonitorEnvironmentQuery;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;

pub async fn list_consumer_monitors(
    State(state): State<AppState>,
    Query(scope): Query<MonitorEnvironmentQuery>,
) -> Result<Json<ApiResponse<Vec<ConsumerMonitorView>>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::list_consumer_monitors(&state, &scope.environment_id).await?,
    )))
}

pub async fn create_consumer_monitor(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Extension(actor): Extension<AuthenticatedActor>,
    Json(payload): Json<ConsumerMonitorUpsertRequest>,
) -> Result<Json<ApiResponse<ConsumerMonitorMutationResult>>, DashboardError> {
    let resource_name = payload.consumer_group.clone();
    let environment_id = Some(payload.environment_id.clone());
    let atomic_audit = successful_mutation_audit_event(
        &actor,
        AuditAction::MonitorUpsert,
        AuditResourceType::Monitor,
        Some(&resource_name),
        environment_id.clone(),
    );
    let result = service::create_or_update_consumer_monitor(&state, payload, Some(atomic_audit)).await?;
    audit
        .record_persisted_success(Some(&resource_name), environment_id)
        .await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn delete_consumer_monitor(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Extension(actor): Extension<AuthenticatedActor>,
    Path(consumer_group): Path<String>,
    Query(query): Query<MonitorDeleteQuery>,
) -> Result<Json<ApiResponse<ConsumerMonitorMutationResult>>, DashboardError> {
    let environment_id = Some(query.environment_id.clone());
    let atomic_audit = successful_mutation_audit_event(
        &actor,
        AuditAction::MonitorDelete,
        AuditResourceType::Monitor,
        Some(&consumer_group),
        environment_id.clone(),
    );
    let result = service::delete_consumer_monitor(
        &state,
        &query.environment_id,
        &consumer_group,
        query.expected_revision,
        Some(atomic_audit),
    )
    .await?;
    if result.message.ends_with("deleted") {
        audit
            .record_persisted_success(Some(&consumer_group), environment_id)
            .await;
    }
    Ok(Json(ApiResponse::success(result)))
}
