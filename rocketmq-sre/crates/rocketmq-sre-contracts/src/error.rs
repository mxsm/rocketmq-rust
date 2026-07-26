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
use thiserror::Error;

use crate::CorrelationId;

/// Stable machine-readable errors shared by SRE APIs.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
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
pub struct SreError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub correlation_id: CorrelationId,
}

/// Typed contract validation failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ContractError {
    #[error("unsupported schema family `{actual}`; expected `{supported}`")]
    UnsupportedSchemaFamily { actual: String, supported: String },
    #[error("unsupported major {actual} for `{family}`; expected {supported}")]
    UnsupportedSchemaMajor {
        family: String,
        actual: u16,
        supported: u16,
    },
    #[error("required feature `{feature}` is not supported")]
    MissingRequiredFeature { feature: String },
    #[error("time range starts after it ends")]
    InvalidTimeRange,
    #[error("evidence content hash is missing or does not match")]
    InvalidContentHash,
    #[error("transition from `{from}` to `{to}` is not allowed")]
    InvalidStateTransition { from: String, to: String },
    #[error("descriptor is invalid: {reason}")]
    InvalidDescriptor { reason: String },
}

impl ContractError {
    /// Converts a validation error into a stable, sanitized API envelope.
    #[must_use]
    pub fn into_sre_error(self, correlation_id: CorrelationId) -> SreError {
        let code = match self {
            Self::UnsupportedSchemaFamily { .. } => ErrorCode::UnsupportedSchemaFamily,
            Self::UnsupportedSchemaMajor { .. } => ErrorCode::UnsupportedSchemaMajor,
            Self::MissingRequiredFeature { .. } => ErrorCode::MissingRequiredFeature,
            Self::InvalidTimeRange => ErrorCode::InvalidTimeRange,
            Self::InvalidContentHash => ErrorCode::InvalidContentHash,
            Self::InvalidStateTransition { .. } => ErrorCode::InvalidStateTransition,
            Self::InvalidDescriptor { .. } => ErrorCode::InvalidDescriptor,
        };
        SreError {
            code,
            message: self.to_string(),
            retryable: false,
            correlation_id,
        }
    }
}
