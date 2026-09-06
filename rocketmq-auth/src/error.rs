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

//! Stable operational error boundary for authentication and authorization services.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::RocketMQError;
use rocketmq_error::AUTH_CONFIGURATION_INVALID;
use rocketmq_error::AUTH_OPERATION_FAILED;
use rocketmq_error::CORE_ARGUMENT_INVALID;
use rocketmq_error::CORE_SERIALIZATION_FAILED;
use rocketmq_error::STORAGE_BACKEND_UNAVAILABLE;
use rocketmq_error::STORAGE_READ_FAILED;
use rocketmq_error::STORAGE_WRITE_FAILED;

/// Closed, low-cardinality classification of an authentication service failure.
///
/// Policy denials are decisions and must not be represented by this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFailureKind {
    InvalidInput,
    InvalidConfiguration,
    NotFound,
    Conflict,
    Expired,
    Unauthenticated,
    Timeout,
    Unavailable,
    InvalidData,
    Unsupported,
    Internal,
}

impl AuthFailureKind {
    const fn public_message(self) -> &'static str {
        match self {
            Self::InvalidInput => "Authentication service input is invalid",
            Self::InvalidConfiguration => "Authentication service configuration is invalid",
            Self::NotFound => "Authentication service resource was not found",
            Self::Conflict => "Authentication service state conflicts with the request",
            Self::Expired => "Authentication service request has expired",
            Self::Unauthenticated => "Authentication could not be verified",
            Self::Timeout => "Authentication service operation timed out",
            Self::Unavailable => "Authentication service is unavailable",
            Self::InvalidData => "Authentication service data is invalid",
            Self::Unsupported => "Authentication service operation is unsupported",
            Self::Internal => "Authentication service operation failed",
        }
    }
}

/// Closed, low-cardinality operation associated with an authentication service failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOperation {
    Initialize,
    InitializeProvider,
    BuildContext,
    Authenticate,
    Authorize,
    ReadMetadata,
    WriteMetadata,
    LockMetadata,
    EncodeMetadata,
    DecodeMetadata,
    ManageMetadata,
    Bootstrap,
    RotateCredential,
    MaintainService,
    LoadSecret,
    Audit,
}

impl AuthOperation {
    /// Returns the fixed low-cardinality operation identifier used by diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Initialize => "initialize",
            Self::InitializeProvider => "initialize-provider",
            Self::BuildContext => "build-context",
            Self::Authenticate => "authenticate",
            Self::Authorize => "authorize",
            Self::ReadMetadata => "read-metadata",
            Self::WriteMetadata => "write-metadata",
            Self::LockMetadata => "lock-metadata",
            Self::EncodeMetadata => "encode-metadata",
            Self::DecodeMetadata => "decode-metadata",
            Self::ManageMetadata => "manage-metadata",
            Self::Bootstrap => "bootstrap",
            Self::RotateCredential => "rotate-credential",
            Self::MaintainService => "maintain-service",
            Self::LoadSecret => "load-secret",
            Self::Audit => "audit",
        }
    }
}

/// Typed operational failure returned by authentication and authorization services.
///
/// Public formatting deliberately excludes the operation and source. The typed source
/// remains available to trusted diagnostics through [`Error::source`].
pub struct AuthServiceError {
    operation: AuthOperation,
    kind: AuthFailureKind,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl AuthServiceError {
    #[must_use]
    pub const fn new(operation: AuthOperation, kind: AuthFailureKind) -> Self {
        Self {
            operation,
            kind,
            source: None,
        }
    }

    pub fn with_source<E>(operation: AuthOperation, kind: AuthFailureKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            operation,
            kind,
            source: Some(Box::new(source)),
        }
    }

    #[must_use]
    pub const fn operation(&self) -> AuthOperation {
        self.operation
    }

    #[must_use]
    pub const fn kind(&self) -> AuthFailureKind {
        self.kind
    }

    #[must_use]
    pub const fn source_present(&self) -> bool {
        self.source.is_some()
    }

    pub(crate) fn invalid_context<T>(_diagnostic: T) -> Self {
        Self::new(AuthOperation::BuildContext, AuthFailureKind::InvalidInput)
    }

    pub(crate) fn invalid_context_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(AuthOperation::BuildContext, AuthFailureKind::InvalidInput, source)
    }

    pub(crate) fn configuration_error<T>(_diagnostic: T) -> Self {
        Self::new(AuthOperation::Initialize, AuthFailureKind::InvalidConfiguration)
    }

    pub(crate) fn subject_not_found<T>(_diagnostic: T) -> Self {
        Self::new(AuthOperation::ManageMetadata, AuthFailureKind::NotFound)
    }

    pub(crate) fn not_initialized<T>(_diagnostic: T) -> Self {
        Self::new(AuthOperation::Initialize, AuthFailureKind::Unavailable)
    }

    pub(crate) fn storage_lock_failed<T>(_diagnostic: T) -> Self {
        Self::new(AuthOperation::LockMetadata, AuthFailureKind::Unavailable)
    }

    pub(crate) fn metadata_io(source: rocketmq_runtime::RuntimeError) -> Self {
        let kind = match source.condition() {
            rocketmq_error::CanonicalCondition::DeadlineExceeded => AuthFailureKind::Timeout,
            rocketmq_error::CanonicalCondition::Unavailable => AuthFailureKind::Unavailable,
            _ => AuthFailureKind::Internal,
        };
        Self::with_source(AuthOperation::WriteMetadata, kind, source)
    }

    pub(crate) fn provider_failed<E>(operation: AuthOperation, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(operation, AuthFailureKind::Unavailable, source)
    }

    pub(crate) fn storage_read_failed<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(AuthOperation::ReadMetadata, AuthFailureKind::Unavailable, source)
    }

    pub(crate) fn storage_write_failed<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(AuthOperation::WriteMetadata, AuthFailureKind::Unavailable, source)
    }

    pub(crate) const fn storage_conflict() -> Self {
        Self::new(AuthOperation::WriteMetadata, AuthFailureKind::Conflict)
    }

    pub(crate) const fn serialization_failed(operation: AuthOperation) -> Self {
        Self::new(operation, AuthFailureKind::InvalidData)
    }
}

impl fmt::Display for AuthServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.public_message())
    }
}

impl fmt::Debug for AuthServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthServiceError")
            .field("operation", &self.operation)
            .field("kind", &self.kind)
            .field("source_present", &self.source_present())
            .finish()
    }
}

impl Error for AuthServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn Error + 'static))
    }
}

impl From<AuthServiceError> for RocketMQError {
    fn from(error: AuthServiceError) -> Self {
        let operation = error.operation();
        let kind = error.kind();
        match (operation, kind) {
            (_, AuthFailureKind::InvalidInput | AuthFailureKind::NotFound) => project_canonical(
                error,
                &CORE_ARGUMENT_INVALID,
                ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
            ),
            (_, AuthFailureKind::InvalidConfiguration | AuthFailureKind::Unsupported) => project_canonical(
                error,
                &AUTH_CONFIGURATION_INVALID,
                ErrorContext::new()
                    .with_text(fields::KEY, "auth.authorization")
                    .with_secret_presence(fields::REASON_PRESENT),
            ),
            (AuthOperation::Initialize, _) => project_canonical(
                error,
                &AUTH_CONFIGURATION_INVALID,
                ErrorContext::new()
                    .with_text(fields::KEY, "auth.authorization")
                    .with_secret_presence(fields::REASON_PRESENT),
            ),
            (AuthOperation::ReadMetadata, _) => project_storage(error, &STORAGE_READ_FAILED, "read"),
            (AuthOperation::WriteMetadata, _) => project_storage(error, &STORAGE_WRITE_FAILED, "write"),
            (AuthOperation::LockMetadata, _) => project_storage(error, &STORAGE_BACKEND_UNAVAILABLE, "lock"),
            (AuthOperation::EncodeMetadata, _) => project_serialization(error, "encode"),
            (AuthOperation::DecodeMetadata, _) => project_serialization(error, "decode"),
            (_, AuthFailureKind::Unauthenticated | AuthFailureKind::Expired) => project_canonical(
                error,
                &rocketmq_error::AUTH_CREDENTIALS_INVALID,
                ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT),
            ),
            _ => project_canonical(
                error,
                &AUTH_OPERATION_FAILED,
                ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation.as_str()),
            ),
        }
    }
}

fn project_storage(
    error: AuthServiceError,
    descriptor: &'static ErrorDescriptor,
    operation: &'static str,
) -> RocketMQError {
    project_canonical(
        error,
        descriptor,
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, operation)
            .with_text(fields::STORE_COMPONENT, "auth-metadata"),
    )
}

fn project_serialization(error: AuthServiceError, operation: &'static str) -> RocketMQError {
    project_canonical(
        error,
        &CORE_SERIALIZATION_FAILED,
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_text(fields::FORMAT, "auth-metadata"),
    )
}

fn project_canonical(
    error: AuthServiceError,
    descriptor: &'static ErrorDescriptor,
    context: ErrorContext,
) -> RocketMQError {
    let has_source = error.source_present();
    let context = if has_source
        && descriptor
            .fields()
            .iter()
            .any(|schema| *schema == fields::SOURCE_PRESENT.schema())
    {
        context.with_secret_presence(fields::SOURCE_PRESENT)
    } else {
        context
    };
    let canonical = if has_source {
        CanonicalError::caused_by(descriptor, error)
    } else {
        CanonicalError::new(descriptor)
    };
    RocketMQError::Shared(Arc::new(canonical.with_context(context)))
}

/// Result returned by authentication and authorization service operations.
pub type AuthServiceResult<T> = Result<T, AuthServiceError>;

#[cfg(test)]
mod tests {
    use std::io;
    use std::mem::size_of;

    use super::*;

    #[test]
    fn public_formatting_never_exposes_source_text() {
        let error = AuthServiceError::with_source(
            AuthOperation::ReadMetadata,
            AuthFailureKind::Unavailable,
            io::Error::other("secret\r\n/injected/private/path"),
        );

        assert_eq!(error.to_string(), "Authentication service is unavailable");
        let debug = format!("{error:?}");
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("private/path"));
        assert!(debug.contains("source_present: true"));
    }

    #[test]
    fn typed_source_is_retained_for_trusted_diagnostics() {
        let error = AuthServiceError::with_source(
            AuthOperation::ReadMetadata,
            AuthFailureKind::Unavailable,
            io::Error::other("disk unavailable"),
        );

        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
    }

    #[test]
    fn service_error_remains_small_enough_for_result_boundaries() {
        assert!(size_of::<AuthServiceError>() <= 4 * size_of::<usize>());
    }

    #[test]
    fn rocketmq_projection_uses_fixed_messages_and_retains_operational_sources() {
        let invalid: RocketMQError = AuthServiceError::with_source(
            AuthOperation::BuildContext,
            AuthFailureKind::InvalidInput,
            io::Error::other("secret\r\npath"),
        )
        .into();
        assert_eq!(invalid.descriptor(), &CORE_ARGUMENT_INVALID);
        assert!(!invalid.to_string().contains("secret"));

        let operational: RocketMQError = AuthServiceError::with_source(
            AuthOperation::ReadMetadata,
            AuthFailureKind::Unavailable,
            io::Error::other("diagnostic cause"),
        )
        .into();
        assert_eq!(operational.descriptor(), &STORAGE_READ_FAILED);
        let RocketMQError::Shared(canonical) = operational else {
            panic!("canonical storage projection must use the shared carrier")
        };
        let auth = canonical.source().expect("auth facade source must be retained");
        assert!(auth.downcast_ref::<AuthServiceError>().is_some());
        let io = auth.source().expect("I/O source must be retained");
        assert!(io.downcast_ref::<io::Error>().is_some());
    }

    #[test]
    fn rocketmq_projection_preserves_frozen_error_categories() {
        let cases = [
            (
                AuthServiceError::new(AuthOperation::Initialize, AuthFailureKind::InvalidConfiguration),
                &AUTH_CONFIGURATION_INVALID,
            ),
            (
                AuthServiceError::new(AuthOperation::ReadMetadata, AuthFailureKind::Unavailable),
                &STORAGE_READ_FAILED,
            ),
            (
                AuthServiceError::new(AuthOperation::WriteMetadata, AuthFailureKind::Conflict),
                &STORAGE_WRITE_FAILED,
            ),
            (
                AuthServiceError::new(AuthOperation::LockMetadata, AuthFailureKind::Unavailable),
                &STORAGE_BACKEND_UNAVAILABLE,
            ),
            (
                AuthServiceError::new(AuthOperation::EncodeMetadata, AuthFailureKind::InvalidData),
                &CORE_SERIALIZATION_FAILED,
            ),
            (
                AuthServiceError::new(AuthOperation::Authorize, AuthFailureKind::Internal),
                &AUTH_OPERATION_FAILED,
            ),
            (
                AuthServiceError::with_source(
                    AuthOperation::Authorize,
                    AuthFailureKind::Unavailable,
                    io::Error::other("provider unavailable"),
                ),
                &AUTH_OPERATION_FAILED,
            ),
        ];

        for (error, descriptor) in cases {
            assert_eq!(RocketMQError::from(error).descriptor(), descriptor);
        }
    }
}
