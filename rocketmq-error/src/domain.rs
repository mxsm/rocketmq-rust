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

use crate::AuthError;
use crate::BoundaryErrorView;
use crate::ErrorCode;
use crate::ErrorContext;
use crate::ErrorDescriptor;
use crate::ErrorKind;
use crate::ErrorSeverity;
use crate::Exposure;
use crate::FilterCompileError;
use crate::FilterError;
use crate::NetworkError;
use crate::ObservabilityError;
use crate::ProtocolError;
use crate::RecoveryHint;
use crate::RocketMQError;
use crate::RpcClientError;
use crate::SerializationError;
use crate::ToolsError;
use crate::UnifiedServiceError;
use crate::CORE_SERVICE_FAILED;

/// Stable descriptor metadata exposed by errors that cross domain boundaries.
///
/// Implementations keep their typed [`StdError::source`] chain for diagnostics.
/// Boundary adapters consume [`Self::boundary_view`] and derive all identity,
/// projection, recovery, and exposure policy from [`Self::descriptor`].
pub trait DomainError: StdError + Send + Sync + 'static {
    /// Returns the structural legacy error kind.
    fn kind(&self) -> ErrorKind;

    /// Returns the authoritative catalog descriptor.
    fn descriptor(&self) -> &'static ErrorDescriptor;

    /// Returns descriptor-valid diagnostic context.
    ///
    /// The context is not itself a public-boundary projection. Boundary
    /// adapters should consume [`Self::boundary_view`] so exposure and field
    /// visibility are applied.
    fn context(&self) -> ErrorContext {
        ErrorContext::new()
    }

    /// Returns the stable canonical error code.
    fn code(&self) -> ErrorCode {
        self.descriptor().code()
    }

    /// Returns the catalog recovery hint.
    fn recovery_hint(&self) -> RecoveryHint {
        self.descriptor().recovery_hint()
    }

    /// Returns the catalog severity.
    fn severity(&self) -> ErrorSeverity {
        self.descriptor().severity()
    }

    /// Returns the catalog exposure policy.
    fn exposure(&self) -> Exposure {
        self.descriptor().exposure()
    }

    /// Builds the sole redaction-aware input consumed by protocol adapters.
    fn boundary_view(&self) -> BoundaryErrorView {
        BoundaryErrorView::new(self.descriptor(), self.context())
    }
}

impl DomainError for RocketMQError {
    fn kind(&self) -> ErrorKind {
        RocketMQError::kind(self)
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        RocketMQError::descriptor(self)
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

    fn descriptor(&self) -> &'static ErrorDescriptor {
        AuthError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        AuthError::context(self)
    }
}

impl DomainError for NetworkError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Network
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        NetworkError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        NetworkError::context(self)
    }
}

impl DomainError for SerializationError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Serialization
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        SerializationError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        SerializationError::context(self)
    }
}

impl DomainError for ProtocolError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Protocol
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        ProtocolError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        ProtocolError::context(self)
    }
}

impl DomainError for RpcClientError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Rpc
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        RpcClientError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        RpcClientError::context(self)
    }
}

impl DomainError for FilterError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Filter
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        FilterError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        FilterError::context(self)
    }
}

impl DomainError for FilterCompileError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Filter
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        FilterCompileError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        FilterCompileError::context(self)
    }
}

impl DomainError for ObservabilityError {
    fn kind(&self) -> ErrorKind {
        ObservabilityError::kind(self)
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        ObservabilityError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        ObservabilityError::context(self)
    }
}

impl DomainError for ToolsError {
    fn kind(&self) -> ErrorKind {
        ToolsError::kind(self)
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        ToolsError::descriptor(self)
    }

    fn context(&self) -> ErrorContext {
        ToolsError::context(self)
    }
}

impl DomainError for UnifiedServiceError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Service
    }

    fn descriptor(&self) -> &'static ErrorDescriptor {
        &CORE_SERVICE_FAILED
    }
}
