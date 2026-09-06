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
use crate::persistence::error::PersistenceError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rocketmq_admin_core::core::AdminError;
use rocketmq_dashboard_common::DashboardCommonError;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RocketMQError;
use rocketmq_error::ViewValueRef;
use rocketmq_error::descriptor_by_code;
use rocketmq_runtime::RuntimeError;
use serde::Serialize;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
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
    Admin(#[from] AdminError),
    #[error("dashboard common error")]
    Common(#[from] DashboardCommonError),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Auth(String),
    /// More than one supported request credential was supplied with different
    /// values. Keep this response value-free so session tokens never cross the
    /// HTTP error boundary.
    #[error("Ambiguous session credentials")]
    AuthTokenAmbiguous,
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotImplemented(String),
    #[error("{0}")]
    Internal(String),
    #[error(transparent)]
    Storage(#[from] PersistenceError),
    #[error("{message}")]
    InternalSource {
        message: &'static str,
        #[source]
        source: DashboardErrorSource,
    },
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
        Cow::Borrowed(self.http_projection().code)
    }

    pub fn status_code(&self) -> StatusCode {
        self.http_projection().status
    }

    fn http_projection(&self) -> DashboardHttpProjection {
        match self {
            Self::Validation(_) => {
                DashboardHttpProjection::fixed(StatusCode::BAD_REQUEST, "VALIDATION_ERROR", "Request validation failed")
            }
            Self::Config(_) | Self::ConfigSource { .. } => DashboardHttpProjection::fixed(
                StatusCode::INTERNAL_SERVER_ERROR,
                "CONFIG_ERROR",
                "Dashboard configuration is invalid",
            ),
            Self::RocketMq(error) => rocketmq_http_projection(error),
            Self::Admin(error) => admin_http_projection(error),
            Self::Common(error) => common_http_projection(error),
            Self::NotFound(_) => {
                DashboardHttpProjection::fixed(StatusCode::NOT_FOUND, "NOT_FOUND", "Requested resource was not found")
            }
            Self::Auth(_) => {
                DashboardHttpProjection::fixed(StatusCode::UNAUTHORIZED, "AUTH_ERROR", "Authentication failed")
            }
            Self::AuthTokenAmbiguous => DashboardHttpProjection::fixed(
                StatusCode::UNAUTHORIZED,
                "AUTH_TOKEN_AMBIGUOUS",
                "Ambiguous session credentials",
            ),
            Self::Forbidden(_) => {
                DashboardHttpProjection::fixed(StatusCode::FORBIDDEN, "FORBIDDEN", "Permission was denied")
            }
            Self::NotImplemented(_) => DashboardHttpProjection::fixed(
                StatusCode::NOT_IMPLEMENTED,
                "NOT_IMPLEMENTED",
                "Requested operation is not implemented",
            ),
            Self::Internal(_) | Self::InternalSource { .. } => DashboardHttpProjection::unknown(),
            Self::Storage(error) => storage_http_projection(error),
        }
    }
}

fn common_http_projection(error: &DashboardCommonError) -> DashboardHttpProjection {
    match error {
        DashboardCommonError::Validation(_) | DashboardCommonError::ParseInt { .. } => {
            DashboardHttpProjection::fixed(StatusCode::BAD_REQUEST, "VALIDATION_ERROR", "Request validation failed")
        }
        DashboardCommonError::Store(_) => DashboardHttpProjection::fixed(
            StatusCode::INTERNAL_SERVER_ERROR,
            "CONFIG_ERROR",
            "Dashboard configuration is invalid",
        ),
        DashboardCommonError::Runtime(_) => DashboardHttpProjection::unknown(),
    }
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> axum::response::Response {
        let projection = self.http_projection();
        let status = projection.status;
        (status, Json(DashboardErrorResponse::from(projection))).into_response()
    }
}

#[derive(Debug)]
struct DashboardHttpProjection {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    details: BTreeMap<String, Value>,
}

impl DashboardHttpProjection {
    fn fixed(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            details: BTreeMap::new(),
        }
    }

    fn unknown() -> Self {
        Self::fixed(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal error")
    }
}

#[derive(Debug, Serialize)]
struct DashboardErrorResponse {
    success: bool,
    code: &'static str,
    message: &'static str,
    data: Option<Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    details: BTreeMap<String, Value>,
}

impl From<DashboardHttpProjection> for DashboardErrorResponse {
    fn from(projection: DashboardHttpProjection) -> Self {
        Self {
            success: false,
            code: projection.code,
            message: projection.message,
            data: None,
            details: projection.details,
        }
    }
}

fn rocketmq_http_projection(error: &RocketMQError) -> DashboardHttpProjection {
    if let Some(descriptor) = metadata_io_descriptor(error) {
        return public_view_projection(PublicErrorView::descriptor_only(descriptor))
            .unwrap_or_else(DashboardHttpProjection::unknown);
    }

    let context = error.context();
    PublicErrorView::try_new(error.descriptor(), &context)
        .ok()
        .and_then(public_view_projection)
        .unwrap_or_else(DashboardHttpProjection::unknown)
}

fn metadata_io_source(error: &RocketMQError) -> Option<&RuntimeError> {
    match error {
        RocketMQError::IO(error) => error.get_ref()?.downcast_ref::<RuntimeError>(),
        _ => None,
    }
}

fn metadata_io_descriptor(error: &RocketMQError) -> Option<&'static ErrorDescriptor> {
    Some(metadata_io_source(error)?.descriptor())
}

fn public_view_projection(view: PublicErrorView<'_>) -> Option<DashboardHttpProjection> {
    let status = StatusCode::from_u16(view.projection().http().status.as_u16()).ok()?;
    let mut details = BTreeMap::new();
    for field in view.fields() {
        let value = match field.value() {
            ViewValueRef::Text(value) => Value::String(value.to_string()),
            ViewValueRef::I64(value) => Value::from(value),
            ViewValueRef::U64(value) => Value::from(value),
            ViewValueRef::Bool(value) => Value::from(value),
            ViewValueRef::Redacted => continue,
        };
        details.insert(field.name().to_string(), value);
    }
    Some(DashboardHttpProjection {
        status,
        code: view.code().as_str(),
        message: view.message(),
        details,
    })
}

fn admin_http_projection(error: &AdminError) -> DashboardHttpProjection {
    match error {
        AdminError::InvalidArgument { .. } => DashboardHttpProjection::fixed(
            StatusCode::BAD_REQUEST,
            "ADMIN_INVALID_ARGUMENT",
            "Admin request is invalid",
        ),
        AdminError::NotFound { .. } => {
            DashboardHttpProjection::fixed(StatusCode::NOT_FOUND, "ADMIN_NOT_FOUND", "Admin resource was not found")
        }
        AdminError::SessionClosed => DashboardHttpProjection::fixed(
            StatusCode::SERVICE_UNAVAILABLE,
            "ADMIN_SESSION_CLOSED",
            "Admin service is unavailable",
        ),
        AdminError::Backend {
            code: Some(code),
            http_status: Some(http_status),
            ..
        } => catalog_admin_projection(code, *http_status)
            .or_else(|| fixed_admin_backend_projection(code, *http_status))
            .unwrap_or_else(DashboardHttpProjection::unknown),
        AdminError::Backend { .. } => DashboardHttpProjection::unknown(),
    }
}

fn catalog_admin_projection(code: &str, http_status: u16) -> Option<DashboardHttpProjection> {
    let descriptor = descriptor_by_code(code)?;
    if descriptor.projection().http().status.as_u16() != http_status {
        return None;
    }
    public_view_projection(PublicErrorView::descriptor_only(descriptor))
}

fn fixed_admin_backend_projection(code: &str, http_status: u16) -> Option<DashboardHttpProjection> {
    let (status, message) = match (code, http_status) {
        (
            "CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED"
            | "HA_OBSERVATION_TARGET_LIMIT_EXCEEDED"
            | "TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED"
            | "TOPIC_PRODUCER_TARGET_LIMIT_EXCEEDED",
            422,
        ) => (StatusCode::UNPROCESSABLE_ENTITY, "Admin target limit was exceeded"),
        ("TARGET_DRIFT", 409) => (StatusCode::CONFLICT, "Admin target state changed"),
        ("ADMIN_QUERY_ALL_SOURCES_FAILED", 503) => {
            (StatusCode::SERVICE_UNAVAILABLE, "Admin data sources are unavailable")
        }
        ("INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE", 429) => {
            (StatusCode::TOO_MANY_REQUESTS, "Admin data source is unavailable")
        }
        ("INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE", 500) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "Admin data source is unavailable")
        }
        ("INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE", 503) => {
            (StatusCode::SERVICE_UNAVAILABLE, "Admin data source is unavailable")
        }
        ("INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE", 504) => {
            (StatusCode::GATEWAY_TIMEOUT, "Admin data source is unavailable")
        }
        _ => return None,
    };
    Some(DashboardHttpProjection::fixed(
        status,
        match code {
            "CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED" => "CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED",
            "HA_OBSERVATION_TARGET_LIMIT_EXCEEDED" => "HA_OBSERVATION_TARGET_LIMIT_EXCEEDED",
            "TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED" => "TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED",
            "TOPIC_PRODUCER_TARGET_LIMIT_EXCEEDED" => "TOPIC_PRODUCER_TARGET_LIMIT_EXCEEDED",
            "TARGET_DRIFT" => "TARGET_DRIFT",
            "ADMIN_QUERY_ALL_SOURCES_FAILED" => "ADMIN_QUERY_ALL_SOURCES_FAILED",
            "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE" => "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
            _ => return None,
        },
        message,
    ))
}

fn storage_http_projection(error: &PersistenceError) -> DashboardHttpProjection {
    let (status, message) = match error {
        PersistenceError::InvalidConfig(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Storage configuration is invalid"),
        PersistenceError::NotFound => (StatusCode::NOT_FOUND, "Storage record was not found"),
        PersistenceError::Capacity => (StatusCode::INSUFFICIENT_STORAGE, "Storage capacity is insufficient"),
        PersistenceError::LockUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Storage data directory is already in use",
        ),
        PersistenceError::ConnectionUnavailable => (StatusCode::SERVICE_UNAVAILABLE, "Storage backend is unavailable"),
        PersistenceError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "Storage operation timed out"),
        PersistenceError::Conflict => (StatusCode::CONFLICT, "Storage write conflict"),
        PersistenceError::UnsupportedLayout => (StatusCode::INTERNAL_SERVER_ERROR, "Storage layout is unsupported"),
        PersistenceError::CorruptedData => (StatusCode::INTERNAL_SERVER_ERROR, "Storage data is corrupted"),
        PersistenceError::MigrationFailed => (StatusCode::INTERNAL_SERVER_ERROR, "Storage migration failed"),
        PersistenceError::Io(_)
        | PersistenceError::Serialization(_)
        | PersistenceError::Query(_)
        | PersistenceError::Runtime(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Storage operation failed"),
    };
    DashboardHttpProjection::fixed(status, error.stable_code(), message)
}

#[cfg(test)]
fn invalid_public_context_projection(
    descriptor: &'static ErrorDescriptor,
    context: &rocketmq_error::ErrorContext,
) -> DashboardHttpProjection {
    PublicErrorView::try_new(descriptor, context)
        .ok()
        .and_then(public_view_projection)
        .unwrap_or_else(DashboardHttpProjection::unknown)
}

#[cfg(test)]
mod tests {
    use super::DashboardError;
    use super::DashboardErrorResponse;
    use super::invalid_public_context_projection;
    use crate::model::ApiResponse;
    use crate::persistence::error::PersistenceError;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use rocketmq_admin_core::core::AdminError;
    use rocketmq_dashboard_common::DashboardCommonError;
    use rocketmq_error::ErrorContext;
    use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;
    use rocketmq_error::RocketMQError;
    use rocketmq_error::fields;
    use rocketmq_runtime::RuntimeError;
    use serde::Deserialize;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::error::Error as _;
    use std::io;

    #[derive(Debug, Deserialize)]
    struct TestErrorResponse {
        success: bool,
        code: String,
        message: String,
        data: Option<Value>,
        #[serde(default)]
        details: BTreeMap<String, Value>,
    }

    async fn failure_response(error: DashboardError) -> (StatusCode, TestErrorResponse, Vec<u8>) {
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body = serde_json::from_slice(&bytes).expect("deserialize error response");
        (status, body, bytes.to_vec())
    }

    #[test]
    fn success_response_json_shape_does_not_gain_error_details() {
        let body = serde_json::to_value(ApiResponse::success("UP")).expect("serialize success response");

        assert_eq!(
            body,
            serde_json::json!({
                "success": true,
                "code": "OK",
                "message": "success",
                "data": "UP"
            })
        );
        assert!(body.get("details").is_none());
    }

    #[tokio::test]
    async fn rocketmq_error_uses_public_view_code_message_status_and_details() {
        let error = RocketMQError::route_not_found("TopicA");
        let public_message = error.public_message();

        let (status, body, _) = failure_response(DashboardError::from(error)).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!body.success);
        assert_eq!(body.code, "route.topic.not_found");
        assert_eq!(body.message, public_message);
        assert_eq!(body.data, None);
        assert_eq!(body.details.get("topic"), Some(&Value::String("TopicA".to_string())));
    }

    #[tokio::test]
    async fn rocketmq_internal_error_response_omits_diagnostic_context() {
        let (status, body, bytes) = failure_response(DashboardError::from(RocketMQError::internal(
            "run dashboard request",
            std::io::Error::other("password=plain-text"),
        )))
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.code, "core.internal.failure");
        assert_eq!(body.message, "Internal error");
        assert!(body.details.is_empty());
        let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(!serialized.contains("run dashboard request"));
        assert!(!serialized.contains("password=plain-text"));
    }

    #[tokio::test]
    async fn catalog_admin_error_uses_descriptor_only_public_projection() {
        let error = AdminError::backend_view(
            "query",
            "route.topic.not_found",
            "password=plain-text\r\nNo route info",
            Some("token=secret; topic=Orders".to_string()),
            404,
            false,
        );

        let (status, body, bytes) = failure_response(DashboardError::from(error)).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.code, "route.topic.not_found");
        assert_eq!(body.message, "Topic route was not found");
        assert!(body.details.is_empty());
        let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(!serialized.contains("password=plain-text"));
        assert!(!serialized.contains("token=secret"));
        assert!(!serialized.contains("Orders"));
    }

    #[tokio::test]
    async fn admin_not_found_uses_fixed_value_free_contract() {
        let (status, body, bytes) = failure_response(DashboardError::from(AdminError::not_found(
            "topic",
            "Orders\r\ntoken=secret",
        )))
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.code, "ADMIN_NOT_FOUND");
        assert_eq!(body.message, "Admin resource was not found");
        assert!(!String::from_utf8(bytes).expect("UTF-8 JSON").contains("Orders"));
    }

    #[tokio::test]
    async fn admin_typed_and_fixed_allowlist_statuses_are_explicit() {
        let cases = [
            (
                AdminError::invalid_argument("topic", "password=plain-text"),
                StatusCode::BAD_REQUEST,
                "ADMIN_INVALID_ARGUMENT",
                "Admin request is invalid",
            ),
            (
                AdminError::SessionClosed,
                StatusCode::SERVICE_UNAVAILABLE,
                "ADMIN_SESSION_CLOSED",
                "Admin service is unavailable",
            ),
            (
                AdminError::backend_view(
                    "mutation",
                    "TARGET_DRIFT",
                    "token=secret",
                    Some("C:\\private\\target".to_string()),
                    409,
                    false,
                ),
                StatusCode::CONFLICT,
                "TARGET_DRIFT",
                "Admin target state changed",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED",
                    "password=plain-text",
                    None,
                    422,
                    false,
                ),
                StatusCode::UNPROCESSABLE_ENTITY,
                "TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED",
                "Admin target limit was exceeded",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED",
                    "password=plain-text",
                    None,
                    422,
                    false,
                ),
                StatusCode::UNPROCESSABLE_ENTITY,
                "CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED",
                "Admin target limit was exceeded",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "HA_OBSERVATION_TARGET_LIMIT_EXCEEDED",
                    "password=plain-text",
                    None,
                    422,
                    false,
                ),
                StatusCode::UNPROCESSABLE_ENTITY,
                "HA_OBSERVATION_TARGET_LIMIT_EXCEEDED",
                "Admin target limit was exceeded",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "TOPIC_PRODUCER_TARGET_LIMIT_EXCEEDED",
                    "password=plain-text",
                    None,
                    422,
                    false,
                ),
                StatusCode::UNPROCESSABLE_ENTITY,
                "TOPIC_PRODUCER_TARGET_LIMIT_EXCEEDED",
                "Admin target limit was exceeded",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "ADMIN_QUERY_ALL_SOURCES_FAILED",
                    "password=plain-text",
                    None,
                    503,
                    true,
                ),
                StatusCode::SERVICE_UNAVAILABLE,
                "ADMIN_QUERY_ALL_SOURCES_FAILED",
                "Admin data sources are unavailable",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                    "token=secret",
                    None,
                    429,
                    true,
                ),
                StatusCode::TOO_MANY_REQUESTS,
                "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                "Admin data source is unavailable",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                    "token=secret",
                    None,
                    500,
                    false,
                ),
                StatusCode::INTERNAL_SERVER_ERROR,
                "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                "Admin data source is unavailable",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                    "token=secret",
                    None,
                    503,
                    true,
                ),
                StatusCode::SERVICE_UNAVAILABLE,
                "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                "Admin data source is unavailable",
            ),
            (
                AdminError::backend_view(
                    "query",
                    "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                    "token=secret",
                    None,
                    504,
                    true,
                ),
                StatusCode::GATEWAY_TIMEOUT,
                "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
                "Admin data source is unavailable",
            ),
        ];

        for (error, expected_status, expected_code, expected_message) in cases {
            let (status, body, bytes) = failure_response(DashboardError::from(error)).await;
            assert_eq!(status, expected_status);
            assert_eq!(body.code, expected_code);
            assert_eq!(body.message, expected_message);
            assert!(body.details.is_empty());
            let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
            assert!(!serialized.contains("plain-text"));
            assert!(!serialized.contains("token=secret"));
            assert!(!serialized.contains("private"));
        }
    }

    #[tokio::test]
    async fn admin_reason_cannot_amplify_the_public_response() {
        let error = AdminError::backend_view(
            "query",
            "ADMIN_QUERY_ALL_SOURCES_FAILED",
            format!("password=plain-text\r\nC:\\private\\admin\0{}", "x".repeat(65_536)),
            Some("token=secret".to_string()),
            503,
            true,
        );

        let (_, _, bytes) = failure_response(DashboardError::from(error)).await;
        let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(serialized.len() < 512);
        assert!(!serialized.contains("plain-text"));
        assert!(!serialized.contains("private"));
        assert!(!serialized.contains("xxxx"));
    }

    #[tokio::test]
    async fn local_source_error_response_hides_source_detail() {
        let error = DashboardError::config_source(
            "Failed to read config file",
            io::Error::new(io::ErrorKind::PermissionDenied, "token=plain-text"),
        );

        let (status, body, bytes) = failure_response(error).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.code, "CONFIG_ERROR");
        assert_eq!(body.message, "Dashboard configuration is invalid");
        let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(!serialized.contains("Failed to read config file"));
        assert!(!serialized.contains("token=plain-text"));
    }

    #[tokio::test]
    async fn common_parse_error_keeps_typed_source_but_hides_dynamic_text() {
        let parse_source = "secret-value".parse::<u64>().expect_err("invalid integer");
        let error = DashboardError::from(DashboardCommonError::parse_int(
            "password=plain-text\r\nC:\\private\\value",
            parse_source,
        ));
        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<DashboardCommonError>())
                .is_some()
        );

        let (status, body, bytes) = failure_response(error).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.code, "VALIDATION_ERROR");
        assert_eq!(body.message, "Request validation failed");
        let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(!serialized.contains("plain-text"));
        assert!(!serialized.contains("private"));
        assert!(!serialized.contains("secret-value"));
    }

    #[tokio::test]
    async fn metadata_io_saturation_uses_canonical_capacity_projection() {
        let error = RocketMQError::IO(io::Error::other(RuntimeError::capacity(
            rocketmq_runtime::RuntimeOperation::MetadataIo,
        )));

        let (status, body, _) = failure_response(DashboardError::from(error)).await;

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body.code, "runtime.capacity.exhausted");
        assert_eq!(body.message, "Runtime capacity is exhausted");
        assert!(body.details.is_empty());
    }

    #[tokio::test]
    async fn storage_conflict_is_a_stable_redacted_409_response() {
        let (status, body, _) = failure_response(DashboardError::from(PersistenceError::Conflict)).await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.code, "STORAGE_CONFLICT");
        assert_eq!(body.message, "Storage write conflict");
        assert!(!body.message.contains(':'));
        assert!(!body.message.contains('/'));
    }

    #[tokio::test]
    async fn storage_dynamic_and_source_text_never_crosses_http_boundary() {
        let cases = [
            DashboardError::from(PersistenceError::InvalidConfig(
                "password=plain-text\r\nC:\\private\\storage".to_string(),
            )),
            DashboardError::from(PersistenceError::Io(io::Error::other("token=secret /private/storage"))),
        ];

        for error in cases {
            let (status, body, bytes) = failure_response(error).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(body.details.is_empty());
            let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
            assert!(!serialized.contains("plain-text"));
            assert!(!serialized.contains("token=secret"));
            assert!(!serialized.contains("private"));
        }
    }

    #[tokio::test]
    async fn storage_http_status_ledger_covers_every_source_free_category() {
        let cases = [
            (PersistenceError::LockUnavailable, StatusCode::SERVICE_UNAVAILABLE),
            (PersistenceError::UnsupportedLayout, StatusCode::INTERNAL_SERVER_ERROR),
            (PersistenceError::CorruptedData, StatusCode::INTERNAL_SERVER_ERROR),
            (PersistenceError::NotFound, StatusCode::NOT_FOUND),
            (PersistenceError::Capacity, StatusCode::INSUFFICIENT_STORAGE),
            (PersistenceError::ConnectionUnavailable, StatusCode::SERVICE_UNAVAILABLE),
            (PersistenceError::MigrationFailed, StatusCode::INTERNAL_SERVER_ERROR),
            (PersistenceError::Timeout, StatusCode::GATEWAY_TIMEOUT),
            (PersistenceError::Conflict, StatusCode::CONFLICT),
        ];

        for (error, expected_status) in cases {
            let expected_code = error.stable_code();
            let (status, body, _) = failure_response(DashboardError::from(error)).await;
            assert_eq!(status, expected_status);
            assert_eq!(body.code, expected_code);
            assert!(body.details.is_empty());
        }
    }

    #[tokio::test]
    async fn ambiguous_session_credentials_preserve_the_fixed_unauthorized_contract() {
        let (status, body, _) = failure_response(DashboardError::AuthTokenAmbiguous).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body.code, "AUTH_TOKEN_AMBIGUOUS");
        assert_eq!(body.message, "Ambiguous session credentials");
        assert!(!body.message.contains("token"));
    }

    #[tokio::test]
    async fn local_dynamic_text_is_never_exposed_or_response_amplified() {
        let hostile = format!("password=plain-text\r\nC:\\private\\config\0{}", "x".repeat(65_536));
        let cases = [
            DashboardError::Validation(hostile.clone()),
            DashboardError::Config(hostile.clone()),
            DashboardError::NotFound(hostile.clone()),
            DashboardError::Auth(hostile.clone()),
            DashboardError::Forbidden(hostile.clone()),
            DashboardError::NotImplemented(hostile.clone()),
            DashboardError::Internal(hostile),
        ];

        for error in cases {
            let (_, _, bytes) = failure_response(error).await;
            let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
            assert!(serialized.len() < 512);
            assert!(!serialized.contains("password=plain-text"));
            assert!(!serialized.contains("private"));
            assert!(!serialized.contains("xxxx"));
        }
    }

    #[tokio::test]
    async fn unknown_or_inconsistent_admin_metadata_uses_exact_generic_500() {
        let cases = [
            AdminError::backend_view(
                "query",
                "EVIL\r\nCODE",
                "password=plain-text",
                Some("C:\\private\\admin".to_string()),
                200,
                false,
            ),
            AdminError::backend_view(
                "query",
                "route.topic.not_found",
                "secret",
                Some("token=secret".to_string()),
                503,
                false,
            ),
            AdminError::backend("query", "password=plain-text"),
        ];

        for error in cases {
            let (status, body, bytes) = failure_response(DashboardError::from(error)).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(body.code, "INTERNAL_ERROR");
            assert_eq!(body.message, "Internal error");
            assert!(body.details.is_empty());
            let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
            assert!(!serialized.contains("plain-text"));
            assert!(!serialized.contains("private"));
            assert!(!serialized.contains("token=secret"));
        }
    }

    #[tokio::test]
    async fn rocketmq_details_exclude_diagnostic_and_secret_fields() {
        let error = RocketMQError::RouteInconsistent {
            topic: "Orders\r\nInjected".to_string(),
            reason: "password=plain-text C:\\private\\route".to_string(),
        };

        let (status, body, bytes) = failure_response(DashboardError::from(error)).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.code, "route.topic.inconsistent");
        assert_eq!(body.message, "Topic route data is inconsistent");
        let topic = body.details.get("topic").and_then(Value::as_str).expect("public topic");
        assert!(!topic.contains('\r'));
        assert!(!topic.contains('\n'));
        let serialized = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(!serialized.contains("password=plain-text"));
        assert!(!serialized.contains("private"));
        assert!(!serialized.contains("reason"));
    }

    #[tokio::test]
    async fn rocketmq_public_details_preserve_json_scalar_types() {
        let cases = [
            (
                RocketMQError::QueueNotExist {
                    topic: "Orders".to_string(),
                    queue_id: -7,
                },
                "queue_id",
                serde_json::json!(-7),
            ),
            (
                RocketMQError::MessageTooLarge { actual: 7, limit: 9 },
                "actual_bytes",
                serde_json::json!(7),
            ),
        ];

        for (error, field, expected) in cases {
            let (_, body, _) = failure_response(DashboardError::from(error)).await;
            assert_eq!(body.details.get(field), Some(&expected));
        }

        let error = RocketMQError::from(rocketmq_error::ObservabilityError::SubscriberInstallFailed {
            attempted: true,
            installed: false,
        });
        let (_, body, _) = failure_response(DashboardError::from(error)).await;
        assert!(!body.details.contains_key("attempted"));
        assert!(!body.details.contains_key("installed"));
    }

    #[test]
    fn invalid_public_context_uses_exact_unknown_projection() {
        let context = ErrorContext::new().with_text(fields::GROUP, "secret-group");
        let projection = invalid_public_context_projection(&ROUTE_TOPIC_NOT_FOUND, &context);
        let body = serde_json::to_value(DashboardErrorResponse::from(projection)).expect("serialize response");

        assert_eq!(
            body,
            serde_json::json!({
                "success": false,
                "code": "INTERNAL_ERROR",
                "message": "Internal error",
                "data": null
            })
        );
        assert!(!body.to_string().contains("secret-group"));
    }

    #[test]
    fn local_http_status_ledger_is_explicit() {
        let cases = [
            (
                DashboardError::Validation("invalid".to_string()),
                "VALIDATION_ERROR",
                StatusCode::BAD_REQUEST,
            ),
            (
                DashboardError::Config("bad config".to_string()),
                "CONFIG_ERROR",
                StatusCode::INTERNAL_SERVER_ERROR,
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
                DashboardError::Forbidden("denied".to_string()),
                "FORBIDDEN",
                StatusCode::FORBIDDEN,
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
