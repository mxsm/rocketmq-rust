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
use crate::model::ApiResponse;
use crate::model::HealthStatus;
use crate::model::MinimalHealthStatus;
use crate::service::liveness_status;
use crate::service::readiness_status;
use crate::state::AppState;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<ApiResponse<MinimalHealthStatus>>) {
    readiness_response(
        readiness_status(
            &state.persistence,
            state.history_runtime.health().await,
            state.session_audit_cleanup_runtime.health().await,
        )
        .await,
    )
}

pub async fn live() -> Json<ApiResponse<MinimalHealthStatus>> {
    let _ = liveness_status();
    Json(ApiResponse::success(MinimalHealthStatus { status: "UP" }))
}

pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ApiResponse<MinimalHealthStatus>>) {
    readiness_response(
        readiness_status(
            &state.persistence,
            state.history_runtime.health().await,
            state.session_audit_cleanup_runtime.health().await,
        )
        .await,
    )
}

fn readiness_response(status: HealthStatus) -> (StatusCode, Json<ApiResponse<MinimalHealthStatus>>) {
    let response = MinimalHealthStatus {
        status: if status.status == "UP" { "UP" } else { "DOWN" },
    };
    if status.status == "UP" {
        (StatusCode::OK, Json(ApiResponse::success(response)))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiResponse::failure_with_data(
                "STORAGE_UNAVAILABLE",
                "Service is not ready",
                response,
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::readiness_response;
    use crate::model::HealthStatus;
    use crate::model::StorageBackend;
    use crate::persistence::StorageHealth;
    use crate::persistence::StorageMode;
    use crate::persistence::StorageStatus;
    use axum::http::StatusCode;

    #[test]
    fn unavailable_storage_returns_a_not_ready_response() {
        let (status, body) = readiness_response(HealthStatus {
            status: "DOWN".to_string(),
            storage: Some(StorageHealth {
                backend: StorageBackend::MySql,
                mode: StorageMode::MultiNode,
                status: StorageStatus::Unavailable,
                schema_version: None,
                last_successful_write_at: None,
                available_bytes: None,
                pool_size: None,
                idle_connections: None,
            }),
            history: None,
            session_audit_cleanup: None,
        });

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.0.success);
        assert_eq!(body.0.code, "STORAGE_UNAVAILABLE");
        assert_eq!(body.0.data.unwrap().status, "DOWN");
    }
}
