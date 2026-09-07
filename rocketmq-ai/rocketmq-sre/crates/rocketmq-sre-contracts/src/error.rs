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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::CorrelationId;

/// Stable machine-readable errors shared by SRE APIs.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[schemars(rename = "ErrorCode")]
pub enum PublicErrorCode {
    UnsupportedSchemaFamily,
    UnsupportedSchemaMajor,
    MissingRequiredFeature,
    InvalidTimeRange,
    InvalidContentHash,
    InvalidStateTransition,
    InvalidDescriptor,
    DescriptorAlreadyExists,
    DescriptorNotFound,
    DescriptorVersionConflict,
    CapabilityMismatch,
    SchemaDigestMismatch,
    UnauthorizedScope,
    TenantMismatch,
    ClusterNotAllowed,
    OutputTooLarge,
    SourceUnavailable,
    ExecutionDisabled,
}

/// Sanitized API error envelope.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[schemars(rename = "SreError")]
pub struct PublicErrorView {
    pub code: PublicErrorCode,
    pub message: String,
    pub retryable: bool,
    pub correlation_id: CorrelationId,
}

/// Contract failure with a closed public classification and private operational source.
#[derive(Clone)]
pub struct SreContractError {
    code: PublicErrorCode,
    source: Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>,
}

impl SreContractError {
    #[must_use]
    pub const fn new(code: PublicErrorCode) -> Self {
        Self { code, source: None }
    }

    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::with_source(PublicErrorCode::SourceUnavailable, source)
    }

    pub fn with_source(code: PublicErrorCode, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            code,
            source: Some(std::sync::Arc::new(source)),
        }
    }

    #[must_use]
    pub const fn code(&self) -> PublicErrorCode {
        self.code
    }

    #[must_use]
    pub fn into_public_view(self, correlation_id: CorrelationId) -> PublicErrorView {
        PublicErrorView {
            code: self.code,
            message: self.to_string(),
            retryable: false,
            correlation_id,
        }
    }
}

impl std::fmt::Display for SreContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.code {
            PublicErrorCode::SourceUnavailable => "SRE contract operation failed",
            PublicErrorCode::UnsupportedSchemaFamily
            | PublicErrorCode::UnsupportedSchemaMajor
            | PublicErrorCode::MissingRequiredFeature
            | PublicErrorCode::InvalidTimeRange
            | PublicErrorCode::InvalidContentHash
            | PublicErrorCode::InvalidStateTransition
            | PublicErrorCode::InvalidDescriptor
            | PublicErrorCode::DescriptorAlreadyExists
            | PublicErrorCode::DescriptorNotFound
            | PublicErrorCode::DescriptorVersionConflict
            | PublicErrorCode::CapabilityMismatch
            | PublicErrorCode::SchemaDigestMismatch
            | PublicErrorCode::UnauthorizedScope
            | PublicErrorCode::TenantMismatch
            | PublicErrorCode::ClusterNotAllowed
            | PublicErrorCode::OutputTooLarge
            | PublicErrorCode::ExecutionDisabled => "SRE contract was rejected",
        })
    }
}

impl std::fmt::Debug for SreContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for SreContractError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("private-token tenant/resource /secret/path"))
        }
    }

    #[test]
    fn canonical_failure_preserves_typed_source_and_redacts_every_projection() {
        let error = crate::canonical_sha256(&Unserializable).unwrap_err();
        assert_eq!(error.code(), PublicErrorCode::SourceUnavailable);
        assert!(error.source().unwrap().is::<serde_json::Error>());
        let clone = error.clone();
        assert!(clone.source().unwrap().is::<serde_json::Error>());
        for projection in [
            error.to_string(),
            format!("{error:?}"),
            serde_json::to_string(&error.into_public_view(CorrelationId::new())).unwrap(),
        ] {
            assert!(!projection.contains("private-token"));
            assert!(!projection.contains("tenant/resource"));
            assert!(!projection.contains("/secret/path"));
        }
    }

    #[test]
    fn public_error_view_preserves_the_wire_shape() {
        let correlation_id = CorrelationId::new();
        let value = SreContractError::new(PublicErrorCode::InvalidDescriptor).into_public_view(correlation_id);
        assert_eq!(
            serde_json::to_value(&value).unwrap(),
            serde_json::json!({
                "code": "invalid_descriptor",
                "message": "SRE contract was rejected",
                "retryable": false,
                "correlation_id": correlation_id,
            })
        );
        assert_eq!(
            serde_json::from_value::<PublicErrorView>(serde_json::to_value(&value).unwrap()).unwrap(),
            value
        );
    }
}
