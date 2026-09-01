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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

pub const ERROR_SCHEMA_VERSION: &str = "rocketmq-mcp-control.error.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlErrorCode {
    InvalidConfig,
    RequestRejected,
    Unauthorized,
    PermissionDenied,
    OperationUnavailable,
    InvalidArguments,
    AuditUnavailable,
    Conflict,
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
            Self::OperationUnavailable => "operation_unavailable",
            Self::InvalidArguments => "invalid_arguments",
            Self::AuditUnavailable => "audit_unavailable",
            Self::Conflict => "conflict",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::ExecutionFailed => "execution_failed",
            Self::ShutdownFailed => "shutdown_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlErrorEnvelope {
    pub schema_version: &'static str,
    pub code: ControlErrorCode,
    pub message: &'static str,
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
            schema_version: ERROR_SCHEMA_VERSION,
            code: self.code,
            message: self.message,
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
        Self::new(ControlErrorCode::PermissionDenied, "mutation is not authorized", false)
    }

    pub const fn operation_unavailable() -> Self {
        Self::new(
            ControlErrorCode::OperationUnavailable,
            "mutation operation is unavailable",
            false,
        )
    }

    pub const fn invalid_arguments() -> Self {
        Self::new(
            ControlErrorCode::InvalidArguments,
            "mutation arguments are invalid",
            false,
        )
    }

    pub const fn audit_unavailable() -> Self {
        Self::new(
            ControlErrorCode::AuditUnavailable,
            "reliable audit storage is unavailable",
            true,
        )
    }

    pub const fn conflict() -> Self {
        Self::new(ControlErrorCode::Conflict, "mutation precondition conflict", false)
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
