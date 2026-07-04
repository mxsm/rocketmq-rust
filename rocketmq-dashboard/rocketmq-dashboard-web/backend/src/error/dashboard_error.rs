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
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rocketmq_error::RocketMQError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DashboardError {
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    Config(String),
    #[error(transparent)]
    RocketMq(#[from] RocketMQError),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Auth(String),
    #[error("{0}")]
    NotImplemented(String),
    #[error("{0}")]
    Internal(String),
}

impl DashboardError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Validation(_) => "VALIDATION_ERROR",
            Self::Config(_) => "CONFIG_ERROR",
            Self::RocketMq(_) => "ROCKETMQ_ERROR",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Auth(_) => "AUTH_ERROR",
            Self::NotImplemented(_) => "NOT_IMPLEMENTED",
            Self::Internal(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::Config(_) => StatusCode::BAD_REQUEST,
            Self::RocketMq(_) => StatusCode::BAD_GATEWAY,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Auth(_) => StatusCode::UNAUTHORIZED,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let body = ApiResponse::failure(self.code(), self.to_string());
        (status, Json(body)).into_response()
    }
}
