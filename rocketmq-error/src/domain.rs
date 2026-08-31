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

//! Canonical metadata contract implemented by errors that cross domain boundaries.

use std::error::Error as StdError;

use crate::descriptor::ErrorCode;
use crate::fields;
use crate::AuthError;
use crate::BoundaryErrorView;
use crate::ControllerError;
use crate::ErrorContext;
use crate::ErrorKind;
use crate::ErrorSeverity;
use crate::ErrorSpec;
use crate::FilterCompileError;
use crate::FilterError;
use crate::NetworkError;
use crate::ObservabilityError;
use crate::ProtocolError;
use crate::RecoverySpec;
use crate::RedactionPolicy;
use crate::RetryClass;
use crate::RocketMQError;
use crate::RpcClientError;
use crate::SerializationError;
use crate::ToolsError;
use crate::UnifiedServiceError;

/// Stable metadata exposed by every error that may cross a domain boundary.
///
/// Implementations keep their typed [`StdError::source`] chain for diagnostics.
/// Boundary adapters consume [`Self::boundary_view`] exactly once and must not
/// infer behavior from `Display` output.
pub trait DomainError: StdError + Send + Sync + 'static {
    /// Returns the stable error kind.
    fn kind(&self) -> ErrorKind;

    /// Returns redaction-aware context safe for boundary adapters.
    fn context(&self) -> ErrorContext {
        ErrorContext::new().with_secret_presence(fields::DOMAIN_ERROR_PRESENT)
    }

    /// Returns the immutable policy record for this error.
    fn spec(&self) -> &'static ErrorSpec {
        self.kind().spec()
    }

    /// Returns the stable machine-readable error code.
    fn code(&self) -> ErrorCode {
        self.spec().code
    }

    /// Returns the recovery policy.
    fn recovery(&self) -> RecoverySpec {
        self.spec().recovery
    }

    /// Returns the retry classification.
    fn retry(&self) -> RetryClass {
        self.recovery().retry
    }

    /// Returns the observability severity.
    fn severity(&self) -> ErrorSeverity {
        self.spec().observe.severity
    }

    /// Returns the external redaction policy.
    fn redaction(&self) -> RedactionPolicy {
        self.spec().redact
    }

    /// Builds the sole redaction-aware input consumed by protocol adapters.
    fn boundary_view(&self) -> BoundaryErrorView {
        let spec = self.spec();
        BoundaryErrorView::new(
            spec.kind,
            self.code(),
            spec.category,
            spec.public_message,
            self.context(),
            spec.remoting,
            spec.grpc,
            spec.http,
            spec.cli,
            spec.recovery,
            spec.observe,
        )
    }
}

impl DomainError for RocketMQError {
    fn kind(&self) -> ErrorKind {
        RocketMQError::kind(self)
    }

    fn context(&self) -> ErrorContext {
        RocketMQError::context(self)
    }

    fn boundary_view(&self) -> BoundaryErrorView {
        RocketMQError::boundary_view(self)
    }
}

impl DomainError for AuthError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Authentication
    }
}

impl DomainError for NetworkError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Network
    }
}

impl DomainError for SerializationError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Serialization
    }
}

impl DomainError for ProtocolError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Protocol
    }
}

impl DomainError for RpcClientError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Rpc
    }
}

impl DomainError for ControllerError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Io(_) => ErrorKind::Io,
            Self::Raft(_) | Self::RaftSource { .. } => ErrorKind::ControllerRaftError,
            Self::NotLeader { .. } => ErrorKind::ControllerNotLeader,
            Self::MetadataNotFound { .. } => ErrorKind::QueryNotFound,
            Self::InvalidRequest(_) | Self::InvalidRequestSource { .. } => ErrorKind::IllegalArgument,
            Self::BrokerRegistrationFailed(_) => ErrorKind::BrokerRegistrationFailed,
            Self::NotInitialized(_) => ErrorKind::NotInitialized,
            Self::ConfigError(_) => ErrorKind::ConfigInvalidValue,
            Self::SerializationError(_) | Self::SerializationSource { .. } => ErrorKind::Serialization,
            Self::StorageError(_) | Self::StorageSource { .. } => ErrorKind::StorageWriteFailed,
            Self::NetworkError(_) => ErrorKind::Network,
            Self::Timeout { .. } => ErrorKind::ControllerConsensusTimeout,
            Self::InitializationFailed | Self::RuntimeError(_) | Self::RuntimeSource { .. } | Self::Shutdown => {
                ErrorKind::Controller
            }
        }
    }
}

impl DomainError for FilterError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Filter
    }

    fn context(&self) -> ErrorContext {
        match self {
            FilterError::Compile(error) => error.context(),
            _ => ErrorContext::new().with_secret_presence(fields::DOMAIN_ERROR_PRESENT),
        }
    }
}

impl DomainError for FilterCompileError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Filter
    }

    fn context(&self) -> ErrorContext {
        FilterCompileError::context(self)
    }
}

impl DomainError for ObservabilityError {
    fn kind(&self) -> ErrorKind {
        ObservabilityError::kind(self)
    }

    fn context(&self) -> ErrorContext {
        ObservabilityError::context(self)
    }
}

impl DomainError for ToolsError {
    fn kind(&self) -> ErrorKind {
        ToolsError::kind(self)
    }

    fn context(&self) -> ErrorContext {
        ToolsError::context(self)
    }
}

impl DomainError for UnifiedServiceError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Service
    }
}
