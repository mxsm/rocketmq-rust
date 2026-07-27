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

use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use rocketmq_sre_contracts::CorrelationId;
use serde::Serialize;
use thiserror::Error;

/// Sanitized control-plane failure.
#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("configuration is invalid")]
    Configuration { detail: String },
    #[error("request is invalid")]
    Validation { code: &'static str, detail: String },
    #[error("request is unauthorized")]
    Unauthorized,
    #[error("request is forbidden")]
    Forbidden { code: &'static str, detail: String },
    #[error("resource was not found")]
    NotFound,
    #[error("operation conflicts with current state")]
    Conflict { code: &'static str, detail: String },
    #[error("persistent state is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("identity provider is unavailable")]
    IdentityProvider(#[source] reqwest::Error),
    #[error("change executor is unavailable")]
    Executor(#[source] reqwest::Error),
    #[error("evidence object storage is unavailable")]
    ObjectStore,
    #[error("capability document is invalid")]
    CapabilityDocument { detail: String },
    #[error("service I/O failed")]
    Io(#[source] std::io::Error),
}

impl ControlPlaneError {
    pub(crate) fn configuration(detail: impl Into<String>) -> Self {
        Self::Configuration { detail: detail.into() }
    }

    pub(crate) fn validation(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Validation {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn conflict(detail: impl Into<String>) -> Self {
        Self::Conflict {
            code: "capability_mismatch",
            detail: detail.into(),
        }
    }

    pub(crate) fn conflict_code(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Conflict {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn forbidden(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Forbidden {
            code,
            detail: detail.into(),
        }
    }

    fn status_and_code(&self) -> (StatusCode, &'static str, bool) {
        match self {
            Self::Configuration { .. } | Self::CapabilityDocument { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, "source_unavailable", false)
            }
            Self::Validation { code, .. } => (StatusCode::BAD_REQUEST, code, false),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized_scope", false),
            Self::Forbidden { code, .. } => (StatusCode::FORBIDDEN, code, false),
            Self::NotFound => (StatusCode::NOT_FOUND, "source_unavailable", false),
            Self::Conflict { code, .. } => (StatusCode::CONFLICT, code, false),
            Self::Database(_) | Self::IdentityProvider(_) | Self::Executor(_) | Self::ObjectStore | Self::Io(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "source_unavailable", true)
            }
        }
    }

    fn safe_message(&self) -> String {
        match self {
            Self::Configuration { detail }
            | Self::Validation { detail, .. }
            | Self::Forbidden { detail, .. }
            | Self::Conflict { detail, .. }
            | Self::CapabilityDocument { detail } => detail.clone(),
            Self::NotFound => "resource was not found".to_owned(),
            Self::Unauthorized => "an authenticated internal identity is required".to_owned(),
            Self::Database(_) => "persistent state is temporarily unavailable".to_owned(),
            Self::IdentityProvider(_) => "identity provider is temporarily unavailable".to_owned(),
            Self::Executor(_) => "Change Executor is temporarily unavailable".to_owned(),
            Self::ObjectStore => "evidence object storage is temporarily unavailable".to_owned(),
            Self::Io(_) => "service endpoint is temporarily unavailable".to_owned(),
        }
    }
}

impl From<sqlx::Error> for ControlPlaneError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<std::io::Error> for ControlPlaneError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<reqwest::Error> for ControlPlaneError {
    fn from(error: reqwest::Error) -> Self {
        Self::IdentityProvider(error)
    }
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    schema_version: &'static str,
    code: &'static str,
    message: String,
    retryable: bool,
    correlation_id: CorrelationId,
}

impl IntoResponse for ControlPlaneError {
    fn into_response(self) -> Response {
        let (status, code, retryable) = self.status_and_code();
        let body = ErrorEnvelope {
            schema_version: "rocketmq-sre.error.v1",
            code,
            message: self.safe_message(),
            retryable,
            correlation_id: CorrelationId::new(),
        };
        (status, Json(body)).into_response()
    }
}
