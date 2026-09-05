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

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use rocketmq_error::CanonicalCondition;
use rocketmq_error::DiagnosticView;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::ErrorSeverity;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::ViewContextViolation;

type SharedError = Arc<dyn StdError + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TransportOperation {
    Start,
    Dispatch,
    Resume,
    Respond,
    Push,
    RequestRegister,
    RequestWrite,
    RequestAwaitResponse,
    CloseClientBindingRetired,
    CloseHeartbeatTimeout,
    CloseAdministrative,
    CloseServiceShutdown,
    CloseSessionEnded,
    CloseClientShutdown,
}

impl TransportOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Dispatch => "dispatch",
            Self::Resume => "resume",
            Self::Respond => "respond",
            Self::Push => "push",
            Self::RequestRegister => "request_register",
            Self::RequestWrite => "request_write",
            Self::RequestAwaitResponse => "request_await_response",
            Self::CloseClientBindingRetired => "close_client_binding_retired",
            Self::CloseHeartbeatTimeout => "close_heartbeat_timeout",
            Self::CloseAdministrative => "close_administrative",
            Self::CloseServiceShutdown => "close_service_shutdown",
            Self::CloseSessionEnded => "close_session_ended",
            Self::CloseClientShutdown => "close_client_shutdown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RequestOperation {
    Register,
    Write,
    AwaitResponse,
}

impl RequestOperation {
    const fn transport_operation(self) -> TransportOperation {
        match self {
            Self::Register => TransportOperation::RequestRegister,
            Self::Write => TransportOperation::RequestWrite,
            Self::AwaitResponse => TransportOperation::RequestAwaitResponse,
        }
    }
}

/// Operational failure at the Transport ownership boundary.
///
/// Stable identity and policy are supplied exclusively by the canonical error
/// catalog. The operation is closed diagnostic context, and a typed cause is
/// retained without rendering source text or request and session identifiers.
#[derive(Clone)]
pub struct TransportError {
    descriptor: &'static ErrorDescriptor,
    operation: TransportOperation,
    context: ErrorContext,
    source: SharedError,
}

impl TransportError {
    pub(crate) fn start(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_START_FAILED,
            TransportOperation::Start,
            source,
        )
    }

    pub(crate) fn dispatch(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_DISPATCH_FAILED,
            TransportOperation::Dispatch,
            source,
        )
    }

    pub(crate) fn resume(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_DISPATCH_FAILED,
            TransportOperation::Resume,
            source,
        )
    }

    pub(crate) fn response(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_RESPONSE_FAILED,
            TransportOperation::Respond,
            source,
        )
    }

    pub(crate) fn push(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_SESSION_FAILED,
            TransportOperation::Push,
            source,
        )
    }

    pub(crate) fn request_failed(operation: RequestOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_SESSION_FAILED,
            operation.transport_operation(),
            source,
        )
    }

    pub(crate) fn request_timeout(operation: RequestOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(
            &rocketmq_error::TRANSPORT_REQUEST_TIMEOUT,
            operation.transport_operation(),
            source,
        )
    }

    pub(crate) fn close(
        cause: crate::server::SessionCloseCause,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let operation = match cause {
            crate::server::SessionCloseCause::ClientBindingRetired => TransportOperation::CloseClientBindingRetired,
            crate::server::SessionCloseCause::HeartbeatTimeout => TransportOperation::CloseHeartbeatTimeout,
            crate::server::SessionCloseCause::Administrative => TransportOperation::CloseAdministrative,
            crate::server::SessionCloseCause::ServiceShutdown => TransportOperation::CloseServiceShutdown,
            crate::server::SessionCloseCause::SessionEnded => TransportOperation::CloseSessionEnded,
            crate::server::SessionCloseCause::ClientShutdown => TransportOperation::CloseClientShutdown,
        };
        Self::new(&rocketmq_error::TRANSPORT_SESSION_FAILED, operation, source)
    }

    fn new(
        descriptor: &'static ErrorDescriptor,
        operation: TransportOperation,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let context = ErrorContext::new()
            .with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, operation.as_str())
            .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT);
        Self {
            descriptor,
            operation,
            context,
            source: Arc::new(source),
        }
    }

    /// Returns the catalog descriptor that owns this failure's identity.
    #[must_use]
    pub const fn descriptor(&self) -> &'static ErrorDescriptor {
        self.descriptor
    }

    /// Returns the stable dotted catalog code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.descriptor.code()
    }

    /// Returns the protocol-independent condition.
    #[must_use]
    pub const fn condition(&self) -> CanonicalCondition {
        self.descriptor.condition()
    }

    /// Returns the catalog-owned severity.
    #[must_use]
    pub const fn severity(&self) -> ErrorSeverity {
        self.descriptor.severity()
    }

    /// Returns the catalog-owned recovery hint.
    #[must_use]
    pub const fn recovery_hint(&self) -> RecoveryHint {
        self.descriptor.recovery_hint()
    }

    /// Creates the descriptor-validated public projection.
    ///
    /// # Errors
    ///
    /// Returns a schema violation if the catalog and internally generated
    /// Transport context become inconsistent.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, ViewContextViolation> {
        PublicErrorView::try_new(self.descriptor, &self.context)
    }

    /// Creates the descriptor-validated controlled diagnostic projection.
    ///
    /// # Errors
    ///
    /// Returns a schema violation if the catalog and internally generated
    /// Transport context become inconsistent.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, ViewContextViolation> {
        DiagnosticView::try_new(self.descriptor, &self.context)
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.descriptor.public_message())
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportError")
            .field("code", &self.code())
            .field("condition", &self.condition())
            .field("operation", &self.operation)
            .field("source_present", &true)
            .finish()
    }
}

impl StdError for TransportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}
