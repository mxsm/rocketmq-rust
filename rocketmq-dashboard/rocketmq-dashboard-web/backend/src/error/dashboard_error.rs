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
use rocketmq_admin_core::core::AdminError;
use rocketmq_dashboard_common::DashboardCommonError;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::RocketMQError;
use std::borrow::Cow;
use std::error::Error as StdError;
use thiserror::Error;

type DashboardErrorSource = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum DashboardError {
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    Config(String),
    #[error("{message}")]
    ConfigSource {
        message: &'static str,
        #[source]
        source: DashboardErrorSource,
    },
    #[error(transparent)]
    RocketMq(#[from] RocketMQError),
    #[error(transparent)]
    Admin(AdminError),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Auth(String),
    #[error("{0}")]
    NotImplemented(String),
    #[error("{0}")]
    Internal(String),
    #[error("{message}")]
    InternalSource {
        message: &'static str,
        #[source]
        source: DashboardErrorSource,
    },
}

impl From<DashboardCommonError> for DashboardError {
    fn from(error: DashboardCommonError) -> Self {
        match error {
            DashboardCommonError::Validation(message) | DashboardCommonError::ParseInt { message, .. } => {
                Self::Validation(message)
            }
            DashboardCommonError::Store(message) => Self::Config(message),
            DashboardCommonError::Runtime(message) => Self::Internal(message),
        }
    }
}

impl From<AdminError> for DashboardError {
    fn from(error: AdminError) -> Self {
        match error {
            AdminError::InvalidArgument { field, reason } => Self::Validation(format!("{field}: {reason}")),
            AdminError::NotFound { resource, name } => Self::NotFound(format!("{resource} `{name}` was not found")),
            error => Self::Admin(error),
        }
    }
}

impl DashboardError {
    pub fn config_source<E>(message: &'static str, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::ConfigSource {
            message,
            source: Box::new(source),
        }
    }

    pub fn internal_source<E>(message: &'static str, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::InternalSource {
            message,
            source: Box::new(source),
        }
    }

    pub fn code(&self) -> Cow<'_, str> {
        match self {
            Self::Validation(_) => Cow::Borrowed("VALIDATION_ERROR"),
            Self::Config(_) | Self::ConfigSource { .. } => Cow::Borrowed("CONFIG_ERROR"),
            Self::RocketMq(error) => Cow::Borrowed(error.boundary_view().code().as_str()),
            Self::Admin(error) => Cow::Owned(error.code().unwrap_or("ADMIN_ERROR").to_string()),
            Self::NotFound(_) => Cow::Borrowed("NOT_FOUND"),
            Self::Auth(_) => Cow::Borrowed("AUTH_ERROR"),
            Self::NotImplemented(_) => Cow::Borrowed("NOT_IMPLEMENTED"),
            Self::Internal(_) | Self::InternalSource { .. } => Cow::Borrowed("INTERNAL_ERROR"),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::Config(_) | Self::ConfigSource { .. } => StatusCode::BAD_REQUEST,
            Self::RocketMq(error) => status_code_from_spec(error.boundary_view().http().status),
            Self::Admin(error) => error
                .http_status()
                .and_then(|status| StatusCode::from_u16(status).ok())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Auth(_) => StatusCode::UNAUTHORIZED,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Internal(_) | Self::InternalSource { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn response_message(&self) -> String {
        match self {
            Self::Validation(message)
            | Self::Config(message)
            | Self::NotFound(message)
            | Self::Auth(message)
            | Self::NotImplemented(message)
            | Self::Internal(message) => message.clone(),
            Self::ConfigSource { message, .. } | Self::InternalSource { message, .. } => (*message).to_string(),
            Self::RocketMq(error) => rocketmq_response_message(error),
            Self::Admin(error) => admin_response_message(error),
        }
    }
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let body = ApiResponse::failure(self.code().into_owned(), self.response_message());
        (status, Json(body)).into_response()
    }
}

fn status_code_from_spec(status: HttpStatusCode) -> StatusCode {
    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn rocketmq_response_message(error: &RocketMQError) -> String {
    let view = error.boundary_view();
    if view.context().is_empty() {
        view.message().to_string()
    } else {
        format!("{} ({})", view.message(), view.context())
    }
}

fn admin_response_message(error: &AdminError) -> String {
    match error {
        AdminError::Backend { reason, context, .. } => context
            .as_deref()
            .filter(|context| !context.is_empty())
            .map_or_else(|| reason.clone(), |context| format!("{reason} ({context})")),
        _ => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::DashboardError;
    use crate::model::ApiResponse;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use rocketmq_admin_core::core::AdminError;
    use rocketmq_error::REDACTED;
    use rocketmq_error::RocketMQError;
    use serde_json::Value;
    use std::io;

    async fn failure_response(error: DashboardError) -> (StatusCode, ApiResponse<Value>) {
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body = serde_json::from_slice(&bytes).expect("deserialize error response");
        (status, body)
    }

    #[tokio::test]
    async fn rocketmq_error_uses_central_http_spec_and_code() {
        let error = RocketMQError::route_not_found("TopicA");
        let public_message = error.public_message();

        let (status, body) = failure_response(DashboardError::from(error)).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!body.success);
        assert_eq!(body.code, "ROUTE_NOT_FOUND");
        assert!(body.message.starts_with(public_message));
        assert!(body.message.contains("topic=TopicA"));
    }

    #[tokio::test]
    async fn rocketmq_error_response_uses_redacted_context() {
        let (status, body) = failure_response(DashboardError::from(RocketMQError::Internal(
            "password=plain-text".to_string(),
        )))
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.code, "INTERNAL");
        assert!(body.message.contains(REDACTED));
        assert!(!body.message.contains("password=plain-text"));
    }

    #[tokio::test]
    async fn admin_error_preserves_boundary_code_status_and_context() {
        let error = AdminError::backend_view(
            "query",
            "ROUTE_NOT_FOUND",
            "No route info",
            Some("topic=Orders".to_string()),
            404,
            false,
        );

        let (status, body) = failure_response(DashboardError::from(error)).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.code, "ROUTE_NOT_FOUND");
        assert_eq!(body.message, "No route info (topic=Orders)");
    }

    #[tokio::test]
    async fn admin_not_found_uses_existing_dashboard_contract() {
        let (status, body) = failure_response(DashboardError::from(AdminError::not_found("topic", "Orders"))).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.code, "NOT_FOUND");
        assert_eq!(body.message, "topic `Orders` was not found");
    }

    #[tokio::test]
    async fn local_source_error_response_hides_source_detail() {
        let error = DashboardError::config_source(
            "Failed to read config file",
            io::Error::new(io::ErrorKind::PermissionDenied, "token=plain-text"),
        );

        let (status, body) = failure_response(error).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.code, "CONFIG_ERROR");
        assert_eq!(body.message, "Failed to read config file");
        assert!(!body.message.contains("token=plain-text"));
    }

    #[test]
    fn local_error_codes_are_stable() {
        let cases = [
            (
                DashboardError::Validation("invalid".to_string()),
                "VALIDATION_ERROR",
                StatusCode::BAD_REQUEST,
            ),
            (
                DashboardError::Config("bad config".to_string()),
                "CONFIG_ERROR",
                StatusCode::BAD_REQUEST,
            ),
            (
                DashboardError::NotFound("missing".to_string()),
                "NOT_FOUND",
                StatusCode::NOT_FOUND,
            ),
            (
                DashboardError::Auth("denied".to_string()),
                "AUTH_ERROR",
                StatusCode::UNAUTHORIZED,
            ),
            (
                DashboardError::NotImplemented("todo".to_string()),
                "NOT_IMPLEMENTED",
                StatusCode::NOT_IMPLEMENTED,
            ),
            (
                DashboardError::Internal("failed".to_string()),
                "INTERNAL_ERROR",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];

        for (error, code, status) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.status_code(), status);
        }
    }
}
