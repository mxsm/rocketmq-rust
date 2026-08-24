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
use crate::model::ApiResponse;
use crate::model::StorageStatusView;
use crate::service::storage_status;
use crate::state::AppState;
use axum::Json;
use axum::extract::State;
use rocketmq_observability::DashboardStorageOperation;
use std::time::Instant;

/// Returns the safe storage status for an authenticated operator.
pub async fn storage_status_handler(State(state): State<AppState>) -> Json<ApiResponse<StorageStatusView>> {
    let started_at = Instant::now();
    let view = storage_status(&state.persistence).await;
    state.storage_metrics.record_status(&view);
    let operation_result: Result<(), DashboardError> = Ok(());
    state.storage_metrics.record_dashboard_operation(
        view.backend,
        DashboardStorageOperation::Status,
        &operation_result,
        started_at.elapsed(),
    );
    Json(ApiResponse::success(view))
}
