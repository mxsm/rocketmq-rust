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

//! Catalog-backed error surface for shared dashboard code.

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;
use std::num::ParseIntError;
use std::panic::Location;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::DiagnosticView;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::SharedError;
use thiserror::Error;

/// Result returned by shared dashboard operations.
pub type DashboardCommonResult<T> = std::result::Result<T, DashboardCommonError>;

/// A closed dashboard operation identifier retained outside catalog policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DashboardOperation {
    /// Normalizes a NameServer address.
    NormalizeNameServerAddress,
    /// Normalizes a Proxy address.
    NormalizeProxyAddress,
    /// Normalizes an endpoint whose concrete family is caller-defined.
    NormalizeEndpointAddress,
    /// Normalizes a NameServer selection.
    NormalizeNameServerSelection,
    /// Normalizes a Proxy selection.
    NormalizeProxySelection,
    /// Normalizes an endpoint selection whose concrete family is caller-defined.
    NormalizeEndpointSelection,
    /// Adds a configured endpoint.
    AddEndpoint,
    /// Selects a configured endpoint.
    SwitchEndpoint,
    /// Removes a configured endpoint.
    RemoveEndpoint,
    /// Selects the active NameServer.
    UpdateNameServer,
    /// Adds a NameServer address.
    AddNameServer,
    /// Removes a NameServer address.
    DeleteNameServer,
    /// Canonicalizes a NameServer snapshot.
    CanonicalizeNameServerSnapshot,
    /// Canonicalizes a Proxy snapshot.
    CanonicalizeProxySnapshot,
    /// Accesses the local NameServer configuration store.
    NameServerStorage,
    /// Applies a NameServer snapshot to the running dashboard.
    ApplyNameServerSnapshot,
}

impl DashboardOperation {
    const fn diagnostic_label(self) -> &'static str {
        match self {
            Self::NormalizeNameServerAddress => "normalize-nameserver-address",
            Self::NormalizeProxyAddress => "normalize-proxy-address",
            Self::NormalizeEndpointAddress => "normalize-endpoint-address",
            Self::NormalizeNameServerSelection => "normalize-nameserver-selection",
            Self::NormalizeProxySelection => "normalize-proxy-selection",
            Self::NormalizeEndpointSelection => "normalize-endpoint-selection",
            Self::AddEndpoint => "add-endpoint",
            Self::SwitchEndpoint => "switch-endpoint",
            Self::RemoveEndpoint => "remove-endpoint",
            Self::UpdateNameServer => "update-nameserver",
            Self::AddNameServer => "add-nameserver",
            Self::DeleteNameServer => "delete-nameserver",
            Self::CanonicalizeNameServerSnapshot => "canonicalize-nameserver-snapshot",
            Self::CanonicalizeProxySnapshot => "canonicalize-proxy-snapshot",
            Self::NameServerStorage => "nameserver-storage",
            Self::ApplyNameServerSnapshot => "apply-nameserver-snapshot",
        }
    }
}

/// A closed endpoint family used by dashboard contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DashboardEndpointKind {
    /// An endpoint whose concrete family is supplied by the caller.
    Endpoint,
    /// A RocketMQ NameServer endpoint.
    NameServer,
    /// A RocketMQ Proxy endpoint.
    Proxy,
}

impl fmt::Display for DashboardEndpointKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Endpoint => "endpoint",
            Self::NameServer => "NameServer endpoint",
            Self::Proxy => "Proxy endpoint",
        })
    }
}

/// A deterministic dashboard input or state contract violation.
///
/// Variants retain the exact failed rule without carrying endpoint values or
/// other caller-provided text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DashboardContractViolation {
    /// The endpoint does not use the required `host:port` shape.
    #[error("{kind} must use host:port format")]
    EndpointFormat {
        /// The affected endpoint family.
        kind: DashboardEndpointKind,
    },
    /// The endpoint host is empty.
    #[error("{kind} host must not be empty")]
    EndpointHostEmpty {
        /// The affected endpoint family.
        kind: DashboardEndpointKind,
    },
    /// The endpoint host contains whitespace.
    #[error("{kind} host must not contain whitespace")]
    EndpointHostContainsWhitespace {
        /// The affected endpoint family.
        kind: DashboardEndpointKind,
    },
    /// The normalized endpoint is already configured.
    #[error("{kind} is already configured")]
    EndpointAlreadyConfigured {
        /// The affected endpoint family.
        kind: DashboardEndpointKind,
    },
    /// The requested endpoint is not configured.
    #[error("{kind} is not configured")]
    EndpointNotConfigured {
        /// The affected endpoint family.
        kind: DashboardEndpointKind,
    },
    /// An active endpoint replacement is not another configured endpoint.
    #[error("active endpoint replacement must be another configured endpoint")]
    ActiveEndpointReplacementInvalid,
    /// Active endpoint removal omitted the required replacement.
    #[error("active endpoint removal requires an explicit replacement")]
    ActiveEndpointReplacementRequired,
    /// The selected endpoint does not exist in its configured list.
    #[error("selected {kind} must exist in the configured endpoint list")]
    SelectedEndpointNotConfigured {
        /// The affected endpoint family.
        kind: DashboardEndpointKind,
    },
    /// The caller attempted to delete the active NameServer.
    #[error("the active NameServer cannot be deleted")]
    ActiveNameServerDeletion,
}

/// Typed leaf for an endpoint port that cannot be parsed as a `u16`.
#[derive(Debug, Error)]
#[error("{kind} port is invalid")]
pub struct DashboardEndpointPortError {
    kind: DashboardEndpointKind,
    #[source]
    source: ParseIntError,
}

impl DashboardEndpointPortError {
    /// Returns the endpoint family whose port failed to parse.
    #[must_use]
    pub const fn kind(&self) -> DashboardEndpointKind {
        self.kind
    }
}

/// Opaque, catalog-backed shared dashboard failure.
///
/// The facade keeps one canonical error instance behind [`SharedError`] so
/// clones share descriptor identity, provenance, backtrace, and the typed
/// source. Callers classify failures through descriptor metadata instead of
/// matching dashboard-local variants.
#[derive(Clone)]
pub struct DashboardCommonError {
    error: SharedError,
    operation: DashboardOperation,
}

impl DashboardCommonError {
    #[track_caller]
    fn caused_by(
        descriptor: &'static ErrorDescriptor,
        operation: DashboardOperation,
        context: ErrorContext,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            error: Arc::new(CanonicalError::caused_by(descriptor, source).with_context(context)),
            operation,
        }
    }

    /// Creates a deterministic dashboard contract failure.
    #[must_use]
    #[track_caller]
    pub fn contract(operation: DashboardOperation, violation: DashboardContractViolation) -> Self {
        Self::caused_by(
            &rocketmq_error::CORE_ARGUMENT_INVALID,
            operation,
            ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
            violation,
        )
    }

    /// Creates an endpoint-port failure while retaining the parse source.
    #[must_use]
    #[track_caller]
    pub fn endpoint_port(kind: DashboardEndpointKind, source: ParseIntError) -> Self {
        let operation = match kind {
            DashboardEndpointKind::NameServer => DashboardOperation::NormalizeNameServerAddress,
            DashboardEndpointKind::Proxy => DashboardOperation::NormalizeProxyAddress,
            DashboardEndpointKind::Endpoint => DashboardOperation::NormalizeEndpointAddress,
        };
        Self::caused_by(
            &rocketmq_error::CORE_ARGUMENT_INVALID,
            operation,
            ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
            DashboardEndpointPortError { kind, source },
        )
    }

    /// Creates a local dashboard storage failure with its typed source.
    #[must_use]
    #[track_caller]
    pub fn storage(operation: DashboardOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::STORE_OPERATION, operation.diagnostic_label())
            .with_text(fields::STORE_COMPONENT, "dashboard")
            .with_secret_presence(fields::STORE_DETAIL_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT);
        Self::caused_by(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, operation, context, source)
    }

    /// Creates a dashboard runtime failure with its typed source.
    #[must_use]
    #[track_caller]
    pub fn runtime(operation: DashboardOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        let context = ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation.diagnostic_label())
            .with_secret_presence(fields::SOURCE_PRESENT);
        Self::caused_by(&rocketmq_error::RUNTIME_INTERNAL_FAILURE, operation, context, source)
    }

    /// Returns the catalog descriptor that owns identity and policy.
    #[must_use]
    pub fn descriptor(&self) -> &'static ErrorDescriptor {
        self.error.descriptor()
    }

    /// Returns the stable catalog code.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        self.error.code()
    }

    /// Returns the descriptor-owned canonical condition.
    #[must_use]
    pub fn condition(&self) -> CanonicalCondition {
        self.error.condition()
    }

    /// Returns the descriptor-owned recovery hint.
    #[must_use]
    pub fn recovery_hint(&self) -> RecoveryHint {
        self.error.recovery_hint()
    }

    /// Returns the closed dashboard operation.
    #[must_use]
    pub const fn operation(&self) -> DashboardOperation {
        self.operation
    }

    /// Returns descriptor-validated bounded context.
    #[must_use]
    pub fn context(&self) -> &ErrorContext {
        self.error.context()
    }

    /// Returns the first canonical promotion location.
    #[must_use]
    pub fn location(&self) -> &'static Location<'static> {
        self.error.location()
    }

    /// Returns the catalog-controlled backtrace, when captured.
    #[must_use]
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.error.backtrace()
    }

    /// Creates a descriptor-validated public projection.
    ///
    /// # Errors
    ///
    /// Returns an error only if the facade built context that violates its
    /// selected descriptor.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, rocketmq_error::ViewContextViolation> {
        self.error.public_view()
    }

    /// Creates a descriptor-validated diagnostic projection.
    ///
    /// # Errors
    ///
    /// Returns an error only if the facade built context that violates its
    /// selected descriptor.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, rocketmq_error::ViewContextViolation> {
        self.error.diagnostic_view()
    }
}

impl fmt::Display for DashboardCommonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.error.as_ref(), formatter)
    }
}

impl fmt::Debug for DashboardCommonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DashboardCommonError")
            .field("code", &self.code())
            .field("condition", &self.condition())
            .field("operation", &self.operation)
            .field("has_source", &self.error.source().is_some())
            .finish()
    }
}

impl StdError for DashboardCommonError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use rocketmq_error::CliExitCode;
    use rocketmq_error::GrpcStatusCode;
    use rocketmq_error::HttpStatusCode;
    use rocketmq_error::RemotingResponseCode;

    use super::*;

    #[test]
    fn contract_failure_uses_canonical_argument_policy_and_closed_source() {
        let error = DashboardCommonError::contract(
            DashboardOperation::NormalizeProxyAddress,
            DashboardContractViolation::EndpointHostEmpty {
                kind: DashboardEndpointKind::Proxy,
            },
        );

        let projection = error.descriptor().projection();
        assert_eq!(error.code(), rocketmq_error::CORE_ARGUMENT_INVALID.code());
        assert_eq!(error.condition(), CanonicalCondition::InvalidArgument);
        assert_eq!(projection.http().status, HttpStatusCode::BAD_REQUEST);
        assert_eq!(projection.grpc().status, GrpcStatusCode::InvalidArgument);
        assert_eq!(projection.cli().exit_code, CliExitCode::USAGE);
        assert_eq!(projection.remoting().code, RemotingResponseCode::InvalidParameter);
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<DashboardContractViolation>())
            .is_some());
    }

    #[test]
    fn endpoint_port_failure_retains_parse_int_source_without_rendering_input() {
        const SENTINEL: &str = "secret-port-value";
        let parse_source = SENTINEL.parse::<u16>().expect_err("invalid integer");
        let error = DashboardCommonError::endpoint_port(DashboardEndpointKind::NameServer, parse_source);

        let leaf = error
            .source()
            .and_then(|source| source.downcast_ref::<DashboardEndpointPortError>())
            .expect("typed endpoint port source");
        assert_eq!(leaf.kind(), DashboardEndpointKind::NameServer);
        assert!(leaf
            .source()
            .and_then(|source| source.downcast_ref::<ParseIntError>())
            .is_some());
        assert!(!error.to_string().contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));
    }

    #[test]
    fn storage_failure_uses_unavailable_projection_and_shares_typed_source() {
        const SENTINEL: &str = "C:\\private\\dashboard.db";
        let caller_line = line!() + 1;
        let error = DashboardCommonError::storage(DashboardOperation::NameServerStorage, io::Error::other(SENTINEL));
        let cloned = error.clone();

        let projection = error.descriptor().projection();
        assert_eq!(error.code(), rocketmq_error::STORAGE_BACKEND_UNAVAILABLE.code());
        assert_eq!(projection.http().status, HttpStatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(projection.grpc().status, GrpcStatusCode::Unavailable);
        assert_eq!(projection.cli().exit_code, CliExitCode::UNAVAILABLE);
        assert!(std::ptr::eq(
            error.source().expect("storage source"),
            cloned.source().expect("cloned storage source")
        ));
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        assert_eq!(error.location().line(), caller_line);
        assert!(!error.to_string().contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));
    }

    #[test]
    fn runtime_failure_uses_canonical_runtime_policy() {
        let error = DashboardCommonError::runtime(
            DashboardOperation::ApplyNameServerSnapshot,
            io::Error::other("runtime source detail"),
        );

        let projection = error.descriptor().projection();
        assert_eq!(error.code(), rocketmq_error::RUNTIME_INTERNAL_FAILURE.code());
        assert_eq!(projection.http().status, HttpStatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(projection.grpc().status, GrpcStatusCode::Internal);
        assert_eq!(projection.cli().exit_code, CliExitCode::SOFTWARE);
        assert!(error.public_view().is_ok());
        assert!(error.diagnostic_view().is_ok());
    }
}
