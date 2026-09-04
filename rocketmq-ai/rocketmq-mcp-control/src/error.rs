// Copyright 2026 The RocketMQ Rust Authors
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

use std::borrow::Cow;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

pub const ERROR_SCHEMA_VERSION: &str = "rocketmq-mcp-control.error.v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum ControlErrorSchemaVersion {
    #[serde(rename = "rocketmq-mcp-control.error.v2")]
    V2,
}

impl JsonSchema for ControlErrorSchemaVersion {
    fn schema_name() -> Cow<'static, str> {
        "ControlErrorSchemaVersion".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string", "const": "rocketmq-mcp-control.error.v2"})
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlErrorCode {
    InvalidConfig,
    RequestRejected,
    Unauthorized,
    PermissionDenied,
    ClusterNotAllowed,
    OperationNotAllowed,
    MutationDisabled,
    OperationUnavailable,
    ConfirmationRequired,
    InvalidArgument,
    AuditUnavailable,
    PreconditionConflict,
    PartialApply,
    VerificationFailed,
    Timeout,
    Cancelled,
    ExecutionFailed,
    ShutdownFailed,
}

impl ControlErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::RequestRejected => "request_rejected",
            Self::Unauthorized => "unauthorized",
            Self::PermissionDenied => "permission_denied",
            Self::ClusterNotAllowed => "cluster_not_allowed",
            Self::OperationNotAllowed => "operation_not_allowed",
            Self::MutationDisabled => "mutation_disabled",
            Self::OperationUnavailable => "operation_unavailable",
            Self::ConfirmationRequired => "confirmation_required",
            Self::InvalidArgument => "invalid_argument",
            Self::AuditUnavailable => "audit_unavailable",
            Self::PreconditionConflict => "precondition_conflict",
            Self::PartialApply => "partial_apply",
            Self::VerificationFailed => "verification_failed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::ExecutionFailed => "execution_failed",
            Self::ShutdownFailed => "shutdown_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlErrorEnvelope {
    pub schema_version: ControlErrorSchemaVersion,
    pub code: ControlErrorCode,
    pub message: Cow<'static, str>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ControlError {
    code: ControlErrorCode,
    message: &'static str,
    retryable: bool,
}

impl ControlError {
    pub const fn new(code: ControlErrorCode, message: &'static str, retryable: bool) -> Self {
        Self {
            code,
            message,
            retryable,
        }
    }

    pub const fn code(&self) -> ControlErrorCode {
        self.code
    }

    pub const fn envelope(&self) -> ControlErrorEnvelope {
        ControlErrorEnvelope {
            schema_version: ControlErrorSchemaVersion::V2,
            code: self.code,
            message: Cow::Borrowed(self.message),
            retryable: self.retryable,
        }
    }

    pub const fn invalid_config() -> Self {
        Self::new(
            ControlErrorCode::InvalidConfig,
            "control configuration is invalid",
            false,
        )
    }

    pub const fn unauthorized() -> Self {
        Self::new(ControlErrorCode::Unauthorized, "authentication is required", false)
    }

    pub const fn request_rejected() -> Self {
        Self::new(ControlErrorCode::RequestRejected, "request was rejected", false)
    }

    pub const fn permission_denied() -> Self {
        Self::new(
            ControlErrorCode::PermissionDenied,
            "write permission is required",
            false,
        )
    }

    pub const fn cluster_not_allowed() -> Self {
        Self::new(
            ControlErrorCode::ClusterNotAllowed,
            "mutation cluster is not allowed",
            false,
        )
    }

    pub const fn operation_not_allowed() -> Self {
        Self::new(
            ControlErrorCode::OperationNotAllowed,
            "mutation operation is not allowed",
            false,
        )
    }

    pub const fn mutation_disabled() -> Self {
        Self::new(
            ControlErrorCode::MutationDisabled,
            "mutation execution is disabled",
            false,
        )
    }

    pub const fn operation_unavailable() -> Self {
        Self::new(
            ControlErrorCode::OperationUnavailable,
            "mutation operation is unavailable",
            false,
        )
    }

    pub const fn confirmation_required() -> Self {
        Self::new(
            ControlErrorCode::ConfirmationRequired,
            "explicit mutation confirmation is required",
            false,
        )
    }

    pub const fn invalid_argument() -> Self {
        Self::new(ControlErrorCode::InvalidArgument, "mutation argument is invalid", false)
    }

    pub const fn audit_unavailable() -> Self {
        Self::new(
            ControlErrorCode::AuditUnavailable,
            "reliable audit storage is unavailable",
            true,
        )
    }

    pub const fn precondition_conflict() -> Self {
        Self::new(
            ControlErrorCode::PreconditionConflict,
            "mutation precondition conflict",
            false,
        )
    }

    pub const fn partial_apply() -> Self {
        Self::new(
            ControlErrorCode::PartialApply,
            "mutation applied to only part of the target set",
            false,
        )
    }

    pub const fn verification_failed() -> Self {
        Self::new(
            ControlErrorCode::VerificationFailed,
            "mutation result could not be verified",
            true,
        )
    }

    pub const fn timeout() -> Self {
        Self::new(ControlErrorCode::Timeout, "mutation timed out", true)
    }

    pub const fn cancelled() -> Self {
        Self::new(ControlErrorCode::Cancelled, "mutation was cancelled", true)
    }

    pub const fn execution_failed() -> Self {
        Self::new(ControlErrorCode::ExecutionFailed, "mutation execution failed", false)
    }

    pub const fn shutdown_failed() -> Self {
        Self::new(
            ControlErrorCode::ShutdownFailed,
            "mutation session shutdown failed",
            true,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_error_vocabulary_is_closed_and_uses_only_v2_names() {
        let required = [
            ControlError::mutation_disabled(),
            ControlError::confirmation_required(),
            ControlError::permission_denied(),
            ControlError::cluster_not_allowed(),
            ControlError::operation_not_allowed(),
            ControlError::invalid_argument(),
            ControlError::precondition_conflict(),
            ControlError::partial_apply(),
            ControlError::verification_failed(),
            ControlError::audit_unavailable(),
        ];
        let names = required.iter().map(|error| error.code().as_str()).collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "mutation_disabled",
                "confirmation_required",
                "permission_denied",
                "cluster_not_allowed",
                "operation_not_allowed",
                "invalid_argument",
                "precondition_conflict",
                "partial_apply",
                "verification_failed",
                "audit_unavailable",
            ]
        );
        for error in required {
            let envelope = serde_json::to_value(error.envelope()).unwrap();
            assert_eq!(envelope["schema_version"], ERROR_SCHEMA_VERSION);
            assert!(envelope.get("operator").is_none());
            assert!(envelope.get("reason").is_none());
        }
        assert!(serde_json::from_str::<ControlErrorCode>(r#""invalid_arguments""#).is_err());
        assert!(serde_json::from_str::<ControlErrorCode>(r#""conflict""#).is_err());

        let encoded = serde_json::to_string(&ControlError::invalid_argument().envelope()).unwrap();
        let decoded: ControlErrorEnvelope = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.code, ControlErrorCode::InvalidArgument);
        let mut unknown = serde_json::to_value(decoded).unwrap();
        unknown["operator"] = serde_json::json!("must-not-be-public");
        assert!(serde_json::from_value::<ControlErrorEnvelope>(unknown).is_err());
    }

    #[test]
    fn public_error_envelope_schema_snapshot() {
        insta::assert_json_snapshot!(
            "control_error_envelope_schema_v2",
            schemars::schema_for!(ControlErrorEnvelope)
        );
    }
}
