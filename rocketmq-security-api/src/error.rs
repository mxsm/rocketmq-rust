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

//! Security contract and provider failure types.

use std::error::Error as StdError;
use std::fmt;

use crate::SecurityBootstrapMaterial;

type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Closed validation rules for provider identifiers and logical secret names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecurityIdentifierRule {
    /// An identifier must contain at least one byte.
    NotEmpty,
    /// An identifier must not exceed the documented length bound.
    MaximumLength,
    /// An identifier must use only the documented canonical character set.
    CanonicalCharacters,
}

impl fmt::Display for SecurityIdentifierRule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotEmpty => "identifier-not-empty",
            Self::MaximumLength => "identifier-maximum-length",
            Self::CanonicalCharacters => "identifier-canonical-characters",
        })
    }
}

/// Closed semantic rules for maintenance policies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenancePolicyRule {
    /// The schema version must be supported.
    SchemaVersion,
    /// The policy identifier must be canonical.
    PolicyIdentifier,
    /// The policy version must be non-zero.
    PolicyVersion,
    /// Authentication must be required.
    AuthenticationRequired,
    /// Authorization must be required.
    AuthorizationRequired,
    /// Fencing tokens must be required.
    FencingTokenRequired,
    /// Request lifetime must be within the supported range.
    RequestLifetime,
    /// Every resource limit must be non-zero.
    ResourceLimits,
    /// Every bound principal must be canonical.
    PrincipalIdentifier,
    /// A principal may be bound only once.
    UniquePrincipalBinding,
    /// Every bound principal must have at least one role.
    PrincipalRoleRequired,
    /// At least one release operator must be bound.
    ReleaseOperatorRequired,
    /// A role may have only one grant entry.
    UniqueRoleGrant,
    /// Every role grant must contain at least one capability.
    RoleCapabilityRequired,
    /// Administrators must not receive the release-checkpoint capability.
    AdministratorCheckpointForbidden,
    /// Release operators must receive the release-checkpoint capability.
    ReleaseCheckpointRequired,
}

impl fmt::Display for MaintenancePolicyRule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SchemaVersion => "maintenance-schema-version",
            Self::PolicyIdentifier => "maintenance-policy-identifier",
            Self::PolicyVersion => "maintenance-policy-version",
            Self::AuthenticationRequired => "maintenance-authentication-required",
            Self::AuthorizationRequired => "maintenance-authorization-required",
            Self::FencingTokenRequired => "maintenance-fencing-token-required",
            Self::RequestLifetime => "maintenance-request-lifetime",
            Self::ResourceLimits => "maintenance-resource-limits",
            Self::PrincipalIdentifier => "maintenance-principal-identifier",
            Self::UniquePrincipalBinding => "maintenance-unique-principal-binding",
            Self::PrincipalRoleRequired => "maintenance-principal-role-required",
            Self::ReleaseOperatorRequired => "maintenance-release-operator-required",
            Self::UniqueRoleGrant => "maintenance-unique-role-grant",
            Self::RoleCapabilityRequired => "maintenance-role-capability-required",
            Self::AdministratorCheckpointForbidden => "maintenance-administrator-checkpoint-forbidden",
            Self::ReleaseCheckpointRequired => "maintenance-release-checkpoint-required",
        })
    }
}

/// Deterministic violation of a security input, configuration, or policy contract.
///
/// Variants contain only closed identifiers. Secret values, paths, principals,
/// provider identifiers, and arbitrary reason strings are deliberately absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SecurityContractViolation {
    /// A provider identifier or logical secret name is invalid.
    #[error("security identifier violates {rule}")]
    InvalidIdentifier {
        /// The violated identifier rule.
        rule: SecurityIdentifierRule,
    },
    /// Secret material is empty.
    #[error("secret material must not be empty")]
    EmptySecretMaterial,
    /// Bootstrap fields were configured without selecting a profile.
    #[error("security bootstrap profile is required")]
    BootstrapProfileRequired,
    /// A configured security profile is unknown.
    #[error("security profile is unknown")]
    UnknownSecurityProfile,
    /// A bootstrap environment value is not valid Unicode.
    #[error("security bootstrap environment value is not valid Unicode")]
    BootstrapEnvironmentEncoding,
    /// Required bootstrap material was not configured.
    #[error("security bootstrap is missing {material}")]
    BootstrapMaterialRequired {
        /// The missing material class.
        material: SecurityBootstrapMaterial,
    },
    /// Configured bootstrap material is not a regular file.
    #[error("security bootstrap {material} is not a regular file")]
    BootstrapMaterialNotRegularFile {
        /// The invalid material class.
        material: SecurityBootstrapMaterial,
    },
    /// Configured bootstrap material is empty.
    #[error("security bootstrap {material} is empty")]
    BootstrapMaterialEmpty {
        /// The empty material class.
        material: SecurityBootstrapMaterial,
    },
    /// Secure bootstrap has no configured secret provider.
    #[error("secure bootstrap requires a secret provider")]
    SecretProviderRequired,
    /// The configured secret-provider capability is unsupported.
    #[error("configured secret provider is unsupported")]
    SecretProviderUnsupported,
    /// An insecure development profile was configured with a non-loopback listener.
    #[error("development-insecure profile requires loopback listeners")]
    DevelopmentListenerNotLoopback,
    /// Provider configuration violates a closed contract.
    #[error("secret provider configuration is invalid")]
    ProviderConfigurationInvalid,
    /// Secret storage permissions are not owner-only.
    #[error("secret storage permissions are not owner-only")]
    SecretStoragePermissionsInsecure,
    /// Secure transport was configured without a signing provider.
    #[error("secure transport requires a signing provider")]
    SigningProviderRequired,
    /// A maintenance policy violates a closed semantic rule.
    #[error("maintenance policy violates {rule}")]
    MaintenancePolicy {
        /// The violated maintenance-policy rule.
        rule: MaintenancePolicyRule,
    },
}

/// Stable category of a security provider failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecurityProviderFailure {
    /// Requested secret material does not exist.
    NotFound,
    /// Provider state conflicts with the requested update.
    Conflict,
    /// The requested provider capability is unsupported.
    Unsupported,
    /// Provider data is malformed, corrupted, or cannot be authenticated.
    InvalidData,
    /// A deterministic security contract was violated.
    ContractViolation,
    /// The provider or a required dependency is unavailable.
    Unavailable,
    /// The provider failed for another operational reason.
    OperationFailed,
}

impl fmt::Display for SecurityProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::InvalidData => "invalid_data",
            Self::ContractViolation => "contract_violation",
            Self::Unavailable => "unavailable",
            Self::OperationFailed => "operation_failed",
        })
    }
}

/// Closed operation performed at a security-provider boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecurityOperation {
    /// Validate deterministic security input or configuration.
    Validate,
    /// Resolve or select a provider.
    ResolveProvider,
    /// Register a provider.
    RegisterProvider,
    /// Inspect bootstrap material.
    InspectBootstrapMaterial,
    /// Read secret material.
    ReadSecret,
    /// Write secret material.
    WriteSecret,
    /// Inspect secret storage metadata.
    InspectSecret,
    /// Encrypt secret material.
    EncryptSecret,
    /// Decrypt secret material.
    DecryptSecret,
    /// Synchronize durable secret-provider state.
    SynchronizeSecret,
    /// Sign an outbound request.
    SignRequest,
    /// Load a security or maintenance policy.
    LoadPolicy,
    /// Record a security audit event.
    RecordAudit,
    /// Rotate credentials.
    RotateCredentials,
}

impl SecurityOperation {
    /// Returns the stable diagnostic label for this operation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validate => "validate",
            Self::ResolveProvider => "resolve_provider",
            Self::RegisterProvider => "register_provider",
            Self::InspectBootstrapMaterial => "inspect_bootstrap_material",
            Self::ReadSecret => "read_secret",
            Self::WriteSecret => "write_secret",
            Self::InspectSecret => "inspect_secret",
            Self::EncryptSecret => "encrypt_secret",
            Self::DecryptSecret => "decrypt_secret",
            Self::SynchronizeSecret => "synchronize_secret",
            Self::SignRequest => "sign_request",
            Self::LoadPolicy => "load_policy",
            Self::RecordAudit => "record_audit",
            Self::RotateCredentials => "rotate_credentials",
        }
    }
}

impl fmt::Display for SecurityOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Operational failure at a security-provider boundary.
///
/// Formatting is intentionally fixed and never renders the typed source. The
/// source remains available for internal downcasting through [`Self::source`]
/// and [`std::error::Error::source`].
pub struct SecurityProviderError {
    kind: SecurityProviderFailure,
    operation: SecurityOperation,
    source: Option<BoxError>,
}

impl SecurityProviderError {
    /// Creates a source-free provider failure.
    #[must_use]
    pub const fn new(kind: SecurityProviderFailure, operation: SecurityOperation) -> Self {
        Self {
            kind,
            operation,
            source: None,
        }
    }

    /// Creates a provider failure while retaining its typed source.
    #[must_use]
    pub fn caused_by(
        kind: SecurityProviderFailure,
        operation: SecurityOperation,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            operation,
            source: Some(Box::new(source)),
        }
    }

    /// Wraps a deterministic contract violation for a specific operation.
    #[must_use]
    pub fn contract(operation: SecurityOperation, violation: SecurityContractViolation) -> Self {
        Self::caused_by(SecurityProviderFailure::ContractViolation, operation, violation)
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> SecurityProviderFailure {
        self.kind
    }

    /// Returns the closed operation identifier.
    #[must_use]
    pub const fn operation(&self) -> SecurityOperation {
        self.operation
    }

    /// Returns the typed source without formatting it.
    #[must_use]
    pub fn source(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl From<SecurityContractViolation> for SecurityProviderError {
    fn from(violation: SecurityContractViolation) -> Self {
        Self::contract(SecurityOperation::Validate, violation)
    }
}

impl fmt::Display for SecurityProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            SecurityProviderFailure::NotFound => "security material was not found",
            SecurityProviderFailure::Conflict => "security provider state conflict",
            SecurityProviderFailure::Unsupported => "security provider operation is unsupported",
            SecurityProviderFailure::InvalidData => "security provider data is invalid",
            SecurityProviderFailure::ContractViolation => "security provider contract was violated",
            SecurityProviderFailure::Unavailable => "security provider is unavailable",
            SecurityProviderFailure::OperationFailed => "security provider operation failed",
        })
    }
}

impl fmt::Debug for SecurityProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityProviderError")
            .field("kind", &self.kind)
            .field("operation", &self.operation)
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl StdError for SecurityProviderError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::mem::size_of;

    use super::*;

    #[test]
    fn provider_error_redacts_and_retains_typed_source() {
        let sentinel = "secret-provider-sentinel";
        let error = SecurityProviderError::caused_by(
            SecurityProviderFailure::Unavailable,
            SecurityOperation::ReadSecret,
            io::Error::other(sentinel),
        );

        assert!(!error.to_string().contains(sentinel));
        assert!(!format!("{error:?}").contains(sentinel));
        assert_eq!(error.kind(), SecurityProviderFailure::Unavailable);
        assert_eq!(error.operation(), SecurityOperation::ReadSecret);
        assert!(Error::source(&error).unwrap().downcast_ref::<io::Error>().is_some());
    }

    #[test]
    fn contract_conversion_is_value_free_and_small() {
        let error = SecurityProviderError::from(SecurityContractViolation::EmptySecretMaterial);

        assert_eq!(error.kind(), SecurityProviderFailure::ContractViolation);
        assert_eq!(error.operation(), SecurityOperation::Validate);
        assert!(error
            .source()
            .unwrap()
            .downcast_ref::<SecurityContractViolation>()
            .is_some());
        assert!(size_of::<SecurityProviderError>() <= 4 * size_of::<usize>());
    }
}
