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

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rocketmq_sre_contracts::CorrelationId;
use serde::Serialize;

/// Stable, non-error control-plane failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlPlaneFailure {
    Configuration,
    Validation,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    Data,
    Database,
    IdentityProvider,
    Executor,
    ObjectStore,
    CapabilityDocument,
    Io,
}

/// Boundary-safe HTTP projection of a control-plane failure.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ControlPlaneErrorView {
    pub schema_version: &'static str,
    pub code: &'static str,
    pub message: &'static str,
    pub retryable: bool,
    pub correlation_id: CorrelationId,
}

/// Closed, source-free classification for an expected request rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlPlaneRejectionKind {
    Validation,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
}

/// Expected request refusal. This is deliberately a value and never an
/// implementation of [`std::error::Error`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlPlaneRejection {
    kind: ControlPlaneRejectionKind,
    code: &'static str,
}

impl ControlPlaneRejection {
    fn validation(code: &'static str) -> Self {
        Self {
            kind: ControlPlaneRejectionKind::Validation,
            code,
        }
    }

    fn unauthorized() -> Self {
        Self {
            kind: ControlPlaneRejectionKind::Unauthorized,
            code: "unauthorized_scope",
        }
    }

    fn forbidden(code: &'static str) -> Self {
        Self {
            kind: ControlPlaneRejectionKind::Forbidden,
            code,
        }
    }

    fn not_found() -> Self {
        Self {
            kind: ControlPlaneRejectionKind::NotFound,
            code: "source_unavailable",
        }
    }

    fn conflict(code: &'static str) -> Self {
        Self {
            kind: ControlPlaneRejectionKind::Conflict,
            code,
        }
    }

    fn status_and_message(self) -> (StatusCode, &'static str) {
        match self.kind {
            ControlPlaneRejectionKind::Validation => (StatusCode::BAD_REQUEST, "request is invalid"),
            ControlPlaneRejectionKind::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "an authenticated internal identity is required",
            ),
            ControlPlaneRejectionKind::Forbidden => (StatusCode::FORBIDDEN, "request is forbidden"),
            ControlPlaneRejectionKind::NotFound => (StatusCode::NOT_FOUND, "resource was not found"),
            ControlPlaneRejectionKind::Conflict => (StatusCode::CONFLICT, "operation conflicts with current state"),
        }
    }

    fn view(self) -> ControlPlaneErrorView {
        let (_, message) = self.status_and_message();
        ControlPlaneErrorView {
            schema_version: "rocketmq-sre.error.v1",
            code: self.code,
            message,
            retryable: false,
            correlation_id: CorrelationId::new(),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn kind(self) -> ControlPlaneRejectionKind {
        self.kind
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        self.code
    }
}

/// HTTP boundary union separating expected request refusals from operational
/// failures. It intentionally does not implement [`std::error::Error`].
#[derive(Clone, Debug)]
pub enum ControlPlaneRequestFailure {
    Rejected(ControlPlaneRejection),
    Operational(ControlPlaneError),
}

impl ControlPlaneRequestFailure {
    pub(crate) fn configuration(_detail: impl Into<String>) -> Self {
        Self::Operational(ControlPlaneError::configuration(
            "request-independent configuration failure",
        ))
    }

    pub(crate) fn validation(code: &'static str, _detail: impl Into<String>) -> Self {
        Self::Rejected(ControlPlaneRejection::validation(code))
    }

    pub(crate) fn validation_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Operational(ControlPlaneError::validation_source(code, source))
    }

    pub(crate) fn unauthorized() -> Self {
        Self::Rejected(ControlPlaneRejection::unauthorized())
    }

    pub(crate) fn forbidden(code: &'static str, _detail: impl Into<String>) -> Self {
        Self::Rejected(ControlPlaneRejection::forbidden(code))
    }

    pub(crate) fn not_found() -> Self {
        Self::Rejected(ControlPlaneRejection::not_found())
    }

    pub(crate) fn conflict_code(code: &'static str, _detail: impl Into<String>) -> Self {
        Self::Rejected(ControlPlaneRejection::conflict(code))
    }

    pub(crate) fn configuration_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Operational(ControlPlaneError::configuration_source(source))
    }

    pub(crate) fn operational_validation_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::validation_source(code, source)
    }

    pub(crate) fn unavailable_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Operational(ControlPlaneError::from_source(
            ControlPlaneFailure::Io,
            "source_unavailable",
            source,
        ))
    }

    pub(crate) fn state(code: &'static str, _detail: impl Into<String>) -> Self {
        Self::Operational(ControlPlaneError::state(code, "invalid persisted state"))
    }

    pub(crate) fn state_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Operational(ControlPlaneError::state_source(code, source))
    }

    pub(crate) fn conflict_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Operational(ControlPlaneError::conflict_source(code, source))
    }

    pub(crate) fn contract(
        failure: ControlPlaneFailure,
        code: &'static str,
        source: rocketmq_sre_contracts::SreContractError,
    ) -> Self {
        if source.source().is_some() {
            return Self::Operational(ControlPlaneError::contract(failure, code, source));
        }
        match failure {
            ControlPlaneFailure::Validation => Self::validation(code, "contract rejected"),
            ControlPlaneFailure::Unauthorized => Self::unauthorized(),
            ControlPlaneFailure::Forbidden => Self::forbidden(code, "contract rejected"),
            ControlPlaneFailure::NotFound => Self::not_found(),
            ControlPlaneFailure::Conflict => Self::conflict_code(code, "contract rejected"),
            ControlPlaneFailure::Configuration
            | ControlPlaneFailure::Data
            | ControlPlaneFailure::Database
            | ControlPlaneFailure::IdentityProvider
            | ControlPlaneFailure::Executor
            | ControlPlaneFailure::ObjectStore
            | ControlPlaneFailure::CapabilityDocument
            | ControlPlaneFailure::Io => Self::Operational(ControlPlaneError::contract(failure, code, source)),
        }
    }

    #[must_use]
    pub(crate) fn failure(&self) -> ControlPlaneFailure {
        match self {
            Self::Rejected(rejection) => match rejection.kind {
                ControlPlaneRejectionKind::Validation => ControlPlaneFailure::Validation,
                ControlPlaneRejectionKind::Unauthorized => ControlPlaneFailure::Unauthorized,
                ControlPlaneRejectionKind::Forbidden => ControlPlaneFailure::Forbidden,
                ControlPlaneRejectionKind::NotFound => ControlPlaneFailure::NotFound,
                ControlPlaneRejectionKind::Conflict => ControlPlaneFailure::Conflict,
            },
            Self::Operational(error) => error.failure(),
        }
    }

    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Rejected(rejection) => rejection.code,
            Self::Operational(error) => error.code(),
        }
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected(_) => None,
            Self::Operational(error) => error.source(),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn rejection(&self) -> Option<ControlPlaneRejection> {
        match self {
            Self::Rejected(rejection) => Some(*rejection),
            Self::Operational(_) => None,
        }
    }
}

impl From<ControlPlaneError> for ControlPlaneRequestFailure {
    fn from(error: ControlPlaneError) -> Self {
        if error.inner.source.is_some() {
            return Self::Operational(error);
        }
        match error.failure() {
            ControlPlaneFailure::Validation => Self::validation(error.code(), "rejected"),
            ControlPlaneFailure::Unauthorized => Self::unauthorized(),
            ControlPlaneFailure::Forbidden => Self::forbidden(error.code(), "rejected"),
            ControlPlaneFailure::NotFound => Self::not_found(),
            ControlPlaneFailure::Conflict => Self::conflict_code(error.code(), "rejected"),
            ControlPlaneFailure::Configuration
            | ControlPlaneFailure::Database
            | ControlPlaneFailure::IdentityProvider
            | ControlPlaneFailure::Executor
            | ControlPlaneFailure::ObjectStore
            | ControlPlaneFailure::CapabilityDocument
            | ControlPlaneFailure::Data
            | ControlPlaneFailure::Io => Self::Operational(error),
        }
    }
}

impl From<ControlPlaneRejection> for ControlPlaneRequestFailure {
    fn from(rejection: ControlPlaneRejection) -> Self {
        Self::Rejected(rejection)
    }
}

impl From<sqlx::Error> for ControlPlaneRequestFailure {
    fn from(source: sqlx::Error) -> Self {
        Self::Operational(ControlPlaneError::database(source))
    }
}

impl Display for ControlPlaneRequestFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(rejection) => formatter.write_str(rejection.status_and_message().1),
            Self::Operational(error) => Display::fmt(error, formatter),
        }
    }
}

impl IntoResponse for ControlPlaneRequestFailure {
    fn into_response(self) -> Response {
        match self {
            Self::Rejected(rejection) => {
                let (status, _) = rejection.status_and_message();
                (status, Json(rejection.view())).into_response()
            }
            Self::Operational(error) => {
                let (status, _) = error.status_and_retryable();
                (status, Json(error.view())).into_response()
            }
        }
    }
}

#[derive(Clone)]
struct ControlPlaneErrorInner {
    failure: ControlPlaneFailure,
    code: &'static str,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

/// Opaque, redacted control-plane operational error facade.
#[derive(Clone)]
pub struct ControlPlaneError {
    inner: Arc<ControlPlaneErrorInner>,
}

impl ControlPlaneError {
    fn from_failure(failure: ControlPlaneFailure, code: &'static str) -> Self {
        Self {
            inner: Arc::new(ControlPlaneErrorInner {
                failure,
                code,
                source: None,
            }),
        }
    }

    pub(crate) fn from_source<E>(failure: ControlPlaneFailure, code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(ControlPlaneErrorInner {
                failure,
                code,
                source: Some(Arc::new(source)),
            }),
        }
    }

    pub(crate) fn configuration(_detail: impl Into<String>) -> Self {
        Self::from_failure(ControlPlaneFailure::Configuration, "source_unavailable")
    }

    pub(crate) fn configuration_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ControlPlaneFailure::Configuration, "source_unavailable", source)
    }

    pub(crate) fn validation_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ControlPlaneFailure::Validation, code, source)
    }

    pub(crate) fn state(code: &'static str, _detail: impl Into<String>) -> Self {
        Self::from_failure(ControlPlaneFailure::Data, code)
    }

    pub(crate) fn state_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ControlPlaneFailure::Data, code, source)
    }

    pub(crate) fn conflict_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ControlPlaneFailure::Conflict, code, source)
    }

    pub(crate) fn reclassify(mut self, failure: ControlPlaneFailure, code: &'static str) -> Self {
        let inner = Arc::make_mut(&mut self.inner);
        inner.failure = failure;
        inner.code = code;
        self
    }

    pub(crate) fn contract(
        failure: ControlPlaneFailure,
        code: &'static str,
        source: rocketmq_sre_contracts::SreContractError,
    ) -> Self {
        Self::from_source(failure, code, source)
    }

    pub(crate) fn database(source: sqlx::Error) -> Self {
        Self::from_source(ControlPlaneFailure::Database, "source_unavailable", source)
    }

    pub(crate) fn identity_provider(source: reqwest::Error) -> Self {
        Self::from_source(ControlPlaneFailure::IdentityProvider, "source_unavailable", source)
    }

    pub(crate) fn executor(source: reqwest::Error) -> Self {
        Self::from_source(ControlPlaneFailure::Executor, "source_unavailable", source)
    }

    pub(crate) fn object_store_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ControlPlaneFailure::ObjectStore, "source_unavailable", source)
    }

    pub(crate) fn capability_document(_detail: impl Into<String>) -> Self {
        Self::from_failure(ControlPlaneFailure::CapabilityDocument, "source_unavailable")
    }

    pub(crate) fn capability_document_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ControlPlaneFailure::CapabilityDocument, "source_unavailable", source)
    }

    pub(crate) fn io(source: std::io::Error) -> Self {
        Self::from_source(ControlPlaneFailure::Io, "source_unavailable", source)
    }

    #[must_use]
    pub(crate) fn failure(&self) -> ControlPlaneFailure {
        self.inner.failure
    }

    #[must_use]
    pub fn code(&self) -> &'static str {
        self.inner.code
    }

    fn status_and_retryable(&self) -> (StatusCode, bool) {
        match self.inner.failure {
            ControlPlaneFailure::Configuration
            | ControlPlaneFailure::CapabilityDocument
            | ControlPlaneFailure::Data => (StatusCode::INTERNAL_SERVER_ERROR, false),
            ControlPlaneFailure::Validation => (StatusCode::BAD_REQUEST, false),
            ControlPlaneFailure::Unauthorized => (StatusCode::UNAUTHORIZED, false),
            ControlPlaneFailure::Forbidden => (StatusCode::FORBIDDEN, false),
            ControlPlaneFailure::NotFound => (StatusCode::NOT_FOUND, false),
            ControlPlaneFailure::Conflict => (StatusCode::CONFLICT, false),
            ControlPlaneFailure::Database
            | ControlPlaneFailure::IdentityProvider
            | ControlPlaneFailure::Executor
            | ControlPlaneFailure::ObjectStore
            | ControlPlaneFailure::Io => (StatusCode::SERVICE_UNAVAILABLE, true),
        }
    }

    fn safe_message(&self) -> &'static str {
        match self.inner.failure {
            ControlPlaneFailure::Configuration => "service configuration is unavailable",
            ControlPlaneFailure::Validation => "request is invalid",
            ControlPlaneFailure::Unauthorized => "an authenticated internal identity is required",
            ControlPlaneFailure::Forbidden => "request is forbidden",
            ControlPlaneFailure::NotFound => "resource was not found",
            ControlPlaneFailure::Conflict => "operation conflicts with current state",
            ControlPlaneFailure::Data => "persisted state is invalid",
            ControlPlaneFailure::Database => "persistent state is temporarily unavailable",
            ControlPlaneFailure::IdentityProvider => "identity provider is temporarily unavailable",
            ControlPlaneFailure::Executor => "Change Executor is temporarily unavailable",
            ControlPlaneFailure::ObjectStore => "evidence object storage is temporarily unavailable",
            ControlPlaneFailure::CapabilityDocument => "capability document is invalid",
            ControlPlaneFailure::Io => "service endpoint is temporarily unavailable",
        }
    }

    #[must_use]
    pub fn view(&self) -> ControlPlaneErrorView {
        let (_, retryable) = self.status_and_retryable();
        ControlPlaneErrorView {
            schema_version: "rocketmq-sre.error.v1",
            code: self.inner.code,
            message: self.safe_message(),
            retryable,
            correlation_id: CorrelationId::new(),
        }
    }
}

impl From<sqlx::Error> for ControlPlaneError {
    fn from(source: sqlx::Error) -> Self {
        Self::database(source)
    }
}

impl From<std::io::Error> for ControlPlaneError {
    fn from(source: std::io::Error) -> Self {
        Self::io(source)
    }
}

impl From<reqwest::Error> for ControlPlaneError {
    fn from(source: reqwest::Error) -> Self {
        Self::identity_provider(source)
    }
}

impl Debug for ControlPlaneError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPlaneError")
            .field("failure", &self.inner.failure)
            .field("code", &self.inner.code)
            .finish()
    }
}

impl Display for ControlPlaneError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

impl Error for ControlPlaneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner
            .source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use axum::body::to_bytes;
    use serde_json::Value;

    use super::*;

    #[test]
    fn facade_is_small_send_sync_and_keeps_typed_source() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ControlPlaneError>();
        assert_eq!(std::mem::size_of::<ControlPlaneError>(), std::mem::size_of::<usize>());
        let error = ControlPlaneError::io(std::io::Error::other("secret path and token"));
        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .is_some()
        );
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("path"));
            assert!(!rendered.contains("token"));
        }
    }

    #[test]
    fn http_view_is_fixed_and_does_not_project_detail() {
        let rejection = ControlPlaneRequestFailure::validation("invalid_request", "secret tenant path");
        let ControlPlaneRequestFailure::Rejected(rejection) = rejection else {
            panic!("validation must be a closed rejection")
        };
        let view = rejection.view();
        assert_eq!(rejection.status_and_message().0, StatusCode::BAD_REQUEST);
        assert_eq!(view.schema_version, "rocketmq-sre.error.v1");
        assert_eq!(view.code, "invalid_request");
        assert_eq!(view.message, "request is invalid");
        assert!(!view.retryable);
        assert!(!format!("{rejection:?}").contains("secret tenant path"));
    }

    #[test]
    fn reclassification_keeps_the_original_leaf_and_fixed_http_projection() {
        let error = ControlPlaneError::validation_source(
            "original_code",
            std::io::Error::other("secret-token tenant/resource /private/path"),
        )
        .reclassify(ControlPlaneFailure::Conflict, "execution_snapshot_invalid");
        assert!(error.source().unwrap().is::<std::io::Error>());
        assert_eq!(error.status_and_retryable(), (StatusCode::CONFLICT, false));
        assert_eq!(error.code(), "execution_snapshot_invalid");
        for text in [
            error.to_string(),
            format!("{error:?}"),
            serde_json::to_string(&error.view()).unwrap(),
        ] {
            assert!(!text.contains("secret-token"));
            assert!(!text.contains("tenant/resource"));
            assert!(!text.contains("/private/path"));
        }
    }

    #[test]
    fn deterministic_contract_rejections_are_closed_non_error_values() {
        let failure = ControlPlaneRequestFailure::contract(
            ControlPlaneFailure::Validation,
            "invalid_content_hash",
            rocketmq_sre_contracts::SreContractError::new(rocketmq_sre_contracts::PublicErrorCode::InvalidContentHash),
        );
        assert!(failure.source().is_none());
        assert_eq!(failure.code(), "invalid_content_hash");
        assert!(matches!(failure, ControlPlaneRequestFailure::Rejected(_)));
    }

    #[test]
    fn source_bearing_contract_failures_remain_operational() {
        let failure = ControlPlaneRequestFailure::contract(
            ControlPlaneFailure::Validation,
            "invalid_content_hash",
            rocketmq_sre_contracts::SreContractError::with_source(
                rocketmq_sre_contracts::PublicErrorCode::SourceUnavailable,
                std::io::Error::other("private codec state"),
            ),
        );
        let ControlPlaneRequestFailure::Operational(error) = failure else {
            panic!("source-bearing contract failure must remain operational");
        };
        assert!(
            error
                .source()
                .is_some_and(|source| source.is::<rocketmq_sre_contracts::SreContractError>())
        );
    }

    #[test]
    fn validation_source_remains_a_typed_operational_failure() {
        let failure = ControlPlaneRequestFailure::validation_source(
            "invalid_persisted_state",
            std::io::Error::other("private persisted state"),
        );
        let ControlPlaneRequestFailure::Operational(error) = failure else {
            panic!("source-bearing validation must remain operational");
        };
        assert!(error.source().is_some_and(|source| source.is::<std::io::Error>()));
    }

    #[test]
    fn unavailable_source_is_retryable_and_not_a_bad_request() {
        let failure = ControlPlaneRequestFailure::unavailable_source(std::io::Error::other("private timeout"));
        let ControlPlaneRequestFailure::Operational(error) = failure else {
            panic!("source unavailability must remain operational");
        };
        assert_eq!(error.status_and_retryable(), (StatusCode::SERVICE_UNAVAILABLE, true));
        assert!(error.source().is_some_and(|source| source.is::<std::io::Error>()));
    }

    #[test]
    fn boundary_conversion_separates_source_free_rejections_from_typed_failures() {
        let rejection = ControlPlaneRequestFailure::forbidden("cluster_not_allowed", "private cluster");
        assert_eq!(
            rejection.rejection().map(ControlPlaneRejection::kind),
            Some(ControlPlaneRejectionKind::Forbidden)
        );

        let operational = ControlPlaneRequestFailure::from(ControlPlaneError::validation_source(
            "invalid_persisted_state",
            std::io::Error::other("private path"),
        ));
        let ControlPlaneRequestFailure::Operational(error) = operational else {
            panic!("source-bearing failure must remain operational");
        };
        assert!(error.source().is_some_and(|source| source.is::<std::io::Error>()));
    }

    async fn response_json(failure: ControlPlaneRequestFailure) -> (StatusCode, Value, String) {
        let response = failure.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let raw = String::from_utf8(bytes.to_vec()).unwrap();
        let value = serde_json::from_str(&raw).unwrap();
        (status, value, raw)
    }

    #[tokio::test]
    async fn request_rejections_have_fixed_protocol_statuses_and_redacted_bodies() {
        let cases = [
            (
                ControlPlaneRequestFailure::validation("invalid_request", "secret\r\nheader"),
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "request is invalid",
            ),
            (
                ControlPlaneRequestFailure::unauthorized(),
                StatusCode::UNAUTHORIZED,
                "unauthorized_scope",
                "an authenticated internal identity is required",
            ),
            (
                ControlPlaneRequestFailure::forbidden("cluster_not_allowed", "secret-token"),
                StatusCode::FORBIDDEN,
                "cluster_not_allowed",
                "request is forbidden",
            ),
            (
                ControlPlaneRequestFailure::not_found(),
                StatusCode::NOT_FOUND,
                "source_unavailable",
                "resource was not found",
            ),
            (
                ControlPlaneRequestFailure::conflict_code("state_conflict", "private/path"),
                StatusCode::CONFLICT,
                "state_conflict",
                "operation conflicts with current state",
            ),
        ];

        for (failure, expected_status, expected_code, expected_message) in cases {
            let (status, body, raw) = response_json(failure).await;
            assert_eq!(status, expected_status);
            assert_eq!(body["schema_version"], "rocketmq-sre.error.v1");
            assert_eq!(body["code"], expected_code);
            assert_eq!(body["message"], expected_message);
            assert_eq!(body["retryable"], false);
            assert!(body["correlation_id"].as_str().is_some_and(|value| !value.is_empty()));
            for sensitive in ["secret", "token", "private", "path", "\r", "\n"] {
                assert!(!raw.contains(sensitive));
            }
        }
    }

    #[tokio::test]
    async fn operational_failures_keep_fixed_500_and_503_projections() {
        let data = ControlPlaneRequestFailure::state("invalid_persisted_state", "secret row");
        let database = ControlPlaneRequestFailure::from(sqlx::Error::RowNotFound);

        let (data_status, data_body, _) = response_json(data).await;
        assert_eq!(data_status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(data_body["code"], "invalid_persisted_state");
        assert_eq!(data_body["message"], "persisted state is invalid");
        assert_eq!(data_body["retryable"], false);

        let (database_status, database_body, _) = response_json(database).await;
        assert_eq!(database_status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(database_body["code"], "source_unavailable");
        assert_eq!(database_body["message"], "persistent state is temporarily unavailable");
        assert_eq!(database_body["retryable"], true);
    }
}
