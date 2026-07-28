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

//! Fail-closed authorization for production maintenance operations.
//!
//! Maintenance authorization is intentionally independent from ordinary
//! RocketMQ `Admin` actions. A successfully authenticated administrator still
//! needs an explicit [`MaintenanceRole::ReleaseOperator`] binding before a
//! release checkpoint request is accepted.

use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_error::Sensitive;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

/// Current on-disk maintenance policy schema.
pub const MAINTENANCE_POLICY_SCHEMA_VERSION: u16 = 1;

/// A privileged production operation controlled by the maintenance policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceCapability {
    /// Create, verify, or restore-verify a release checkpoint set.
    ReleaseCheckpoint,
}

/// A role that can be bound by the maintenance policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceRole {
    /// An ordinary administrator. This role never implies release checkpoint access.
    Administrator,
    /// A separately delegated production release operator.
    ReleaseOperator,
}

/// Classification applied to every maintenance request before dispatch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceRequestClass {
    /// A fail-closed operation that requires a dedicated role and capability.
    PrivilegedMaintenance,
}

/// Bounded resources available to one authorized checkpoint operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MaintenanceResourceBudget {
    /// Maximum uncompressed bytes accepted for one checkpoint artifact.
    pub max_checkpoint_bytes: u64,
    /// Maximum number of Store members in one checkpoint set.
    pub max_store_members: u32,
    /// Maximum number of maintenance operations admitted at once.
    pub max_concurrent_operations: u32,
}

/// A principal-to-role binding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MaintenancePrincipalBinding {
    /// Authenticated access-key identity.
    pub principal: String,
    /// Explicit maintenance roles assigned to this identity.
    pub roles: BTreeSet<MaintenanceRole>,
}

/// Capabilities delegated to one maintenance role.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MaintenanceRoleGrant {
    /// Role receiving the capability set.
    pub role: MaintenanceRole,
    /// Capabilities granted to the role.
    pub capabilities: BTreeSet<MaintenanceCapability>,
}

/// Versioned fail-closed maintenance policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MaintenancePolicy {
    /// On-disk schema version.
    pub schema_version: u16,
    /// Stable policy identifier used by audit records.
    pub policy_id: String,
    /// Monotonically increasing policy version.
    pub policy_version: u64,
    /// Authentication must remain enabled for maintenance.
    pub require_authentication: bool,
    /// Authorization must remain enabled for maintenance.
    pub require_authorization: bool,
    /// Every state-changing request must carry a non-zero fencing token.
    pub require_fencing_token: bool,
    /// Maximum allowed request lifetime from authorization time.
    pub max_request_lifetime_millis: u64,
    /// Resource budget enforced by checkpoint coordinators.
    pub resource_budget: MaintenanceResourceBudget,
    /// Authenticated identities and their independent maintenance roles.
    pub principal_bindings: Vec<MaintenancePrincipalBinding>,
    /// Role-to-capability grants.
    pub role_grants: Vec<MaintenanceRoleGrant>,
}

impl MaintenancePolicy {
    /// Validates the complete policy before any listener is bound.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenancePolicyError::InvalidPolicy`] when the schema,
    /// fail-closed requirements, role bindings, grants, or resource limits are
    /// incomplete or contradictory.
    pub fn validate(&self) -> Result<(), MaintenancePolicyError> {
        if self.schema_version != MAINTENANCE_POLICY_SCHEMA_VERSION {
            return Err(MaintenancePolicyError::InvalidPolicy(format!(
                "schema_version must be {MAINTENANCE_POLICY_SCHEMA_VERSION}"
            )));
        }
        if !is_canonical_identifier(&self.policy_id) {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "policy_id must be a canonical lowercase identifier".to_string(),
            ));
        }
        if self.policy_version == 0 {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "policy_version must be greater than zero".to_string(),
            ));
        }
        if !self.require_authentication || !self.require_authorization {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "maintenance policy must require authentication and authorization".to_string(),
            ));
        }
        if !self.require_fencing_token {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "maintenance policy must require fencing tokens".to_string(),
            ));
        }
        if !(1_000..=86_400_000).contains(&self.max_request_lifetime_millis) {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "max_request_lifetime_millis must be between 1000 and 86400000".to_string(),
            ));
        }
        if self.resource_budget.max_checkpoint_bytes == 0
            || self.resource_budget.max_store_members == 0
            || self.resource_budget.max_concurrent_operations == 0
        {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "maintenance resource limits must be greater than zero".to_string(),
            ));
        }

        let mut principals = BTreeSet::new();
        let mut has_release_operator = false;
        for binding in &self.principal_bindings {
            if !is_canonical_principal(&binding.principal) {
                return Err(MaintenancePolicyError::InvalidPolicy(
                    "principal bindings must use non-empty canonical identities".to_string(),
                ));
            }
            if !principals.insert(binding.principal.as_str()) {
                return Err(MaintenancePolicyError::InvalidPolicy(format!(
                    "principal '{}' is bound more than once",
                    binding.principal
                )));
            }
            if binding.roles.is_empty() {
                return Err(MaintenancePolicyError::InvalidPolicy(format!(
                    "principal '{}' has no maintenance role",
                    binding.principal
                )));
            }
            has_release_operator |= binding.roles.contains(&MaintenanceRole::ReleaseOperator);
        }
        if !has_release_operator {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "at least one principal must be bound to release_operator".to_string(),
            ));
        }

        let mut granted_roles = BTreeSet::new();
        let mut release_checkpoint_granted = false;
        for grant in &self.role_grants {
            if !granted_roles.insert(grant.role) {
                return Err(MaintenancePolicyError::InvalidPolicy(format!(
                    "role '{:?}' is granted more than once",
                    grant.role
                )));
            }
            if grant.capabilities.is_empty() {
                return Err(MaintenancePolicyError::InvalidPolicy(format!(
                    "role '{:?}' has no capability",
                    grant.role
                )));
            }
            if grant.role == MaintenanceRole::Administrator
                && grant.capabilities.contains(&MaintenanceCapability::ReleaseCheckpoint)
            {
                return Err(MaintenancePolicyError::InvalidPolicy(
                    "administrator cannot be granted release_checkpoint".to_string(),
                ));
            }
            release_checkpoint_granted |= grant.role == MaintenanceRole::ReleaseOperator
                && grant.capabilities.contains(&MaintenanceCapability::ReleaseCheckpoint);
        }
        if !release_checkpoint_granted {
            return Err(MaintenancePolicyError::InvalidPolicy(
                "release_operator must be granted release_checkpoint".to_string(),
            ));
        }

        Ok(())
    }

    fn roles_for(&self, principal: &str) -> Option<&BTreeSet<MaintenanceRole>> {
        self.principal_bindings
            .iter()
            .find(|binding| binding.principal == principal)
            .map(|binding| &binding.roles)
    }

    fn role_allows(&self, role: MaintenanceRole, capability: MaintenanceCapability) -> bool {
        self.role_grants
            .iter()
            .find(|grant| grant.role == role)
            .is_some_and(|grant| grant.capabilities.contains(&capability))
    }
}

/// Immutable reference used to pin a policy by path, version, and SHA-256.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MaintenancePolicyReference {
    /// Absolute path or path relative to the service configuration root.
    pub path: PathBuf,
    /// Expected policy version.
    pub version: u64,
    /// Expected lowercase SHA-256 of the exact policy bytes.
    pub sha256: String,
}

impl MaintenancePolicyReference {
    /// Loads and validates a policy relative to `configuration_root`.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the reference is unsafe, the file cannot be
    /// read, the digest or version does not match, JSON decoding fails, or the
    /// policy is not fail closed.
    pub fn load_from(
        &self,
        configuration_root: impl AsRef<Path>,
    ) -> Result<LoadedMaintenancePolicy, MaintenancePolicyError> {
        validate_sha256(&self.sha256)?;
        if self.version == 0 {
            return Err(MaintenancePolicyError::InvalidReference(
                "policy reference version must be greater than zero".to_string(),
            ));
        }
        if self.path.as_os_str().is_empty() {
            return Err(MaintenancePolicyError::InvalidReference(
                "policy reference path is empty".to_string(),
            ));
        }
        if self.path.is_relative()
            && self.path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(MaintenancePolicyError::InvalidReference(
                "relative policy path cannot escape the configuration root".to_string(),
            ));
        }

        let resolved_path = if self.path.is_absolute() {
            self.path.clone()
        } else {
            configuration_root.as_ref().join(&self.path)
        };
        let bytes = fs::read(&resolved_path).map_err(|source| MaintenancePolicyError::Read {
            path: resolved_path.clone(),
            source,
        })?;
        let actual_sha256 = sha256_hex(&bytes);
        if actual_sha256 != self.sha256 {
            return Err(MaintenancePolicyError::DigestMismatch {
                expected: self.sha256.clone(),
                actual: actual_sha256,
            });
        }

        let policy: MaintenancePolicy =
            serde_json::from_slice(&bytes).map_err(|source| MaintenancePolicyError::Decode {
                path: resolved_path.clone(),
                source,
            })?;
        policy.validate()?;
        if policy.policy_version != self.version {
            return Err(MaintenancePolicyError::VersionMismatch {
                expected: self.version,
                actual: policy.policy_version,
            });
        }

        Ok(LoadedMaintenancePolicy {
            policy,
            reference: self.clone(),
            resolved_path,
        })
    }
}

/// A policy whose bytes, version, and semantic invariants were validated.
#[derive(Clone, Debug)]
pub struct LoadedMaintenancePolicy {
    policy: MaintenancePolicy,
    reference: MaintenancePolicyReference,
    resolved_path: PathBuf,
}

impl LoadedMaintenancePolicy {
    /// Returns the validated policy.
    pub const fn policy(&self) -> &MaintenancePolicy {
        &self.policy
    }

    /// Returns the immutable policy reference.
    pub const fn reference(&self) -> &MaintenancePolicyReference {
        &self.reference
    }

    /// Returns the resolved file used during validation.
    pub fn resolved_path(&self) -> &Path {
        &self.resolved_path
    }
}

/// Authenticated request facts supplied by the service composition root.
#[derive(Clone, Eq, PartialEq)]
pub struct MaintenanceAuthorizationContext {
    /// Whether the authentication runtime is enabled.
    pub authentication_enabled: bool,
    /// Whether authorization is enabled for the service.
    pub authorization_enabled: bool,
    /// Identity produced by successful credential verification.
    pub principal: Option<String>,
    /// Privileged classification assigned by the protocol router.
    pub request_class: MaintenanceRequestClass,
    /// Capability requested by the operation.
    pub capability: MaintenanceCapability,
    /// Absolute request deadline.
    pub deadline_unix_millis: u64,
    /// Non-zero lease fencing token.
    pub fencing_token: Option<u64>,
}

impl fmt::Debug for MaintenanceAuthorizationContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceAuthorizationContext")
            .field("authentication_enabled", &self.authentication_enabled)
            .field("authorization_enabled", &self.authorization_enabled)
            .field("principal", &Sensitive::new(self.principal.as_deref()))
            .field("request_class", &self.request_class)
            .field("capability", &self.capability)
            .field("deadline_unix_millis", &self.deadline_unix_millis)
            .field("fencing_token", &Sensitive::new(self.fencing_token))
            .finish()
    }
}

/// Auditable authorization result carried into a checkpoint operation.
#[derive(Clone, Eq, PartialEq)]
pub struct MaintenanceAuthorizationGrant {
    /// Authenticated identity.
    principal: String,
    /// Dedicated role authorizing the operation.
    role: MaintenanceRole,
    /// Authorized capability.
    capability: MaintenanceCapability,
    /// Policy version used for the decision.
    policy_version: u64,
    /// Request deadline.
    deadline_unix_millis: u64,
    /// Lease fencing token.
    fencing_token: u64,
    /// Resource budget pinned by the authorizing policy.
    resource_budget: MaintenanceResourceBudget,
}

impl fmt::Debug for MaintenanceAuthorizationGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceAuthorizationGrant")
            .field("principal", &Sensitive::new(&self.principal))
            .field("role", &self.role)
            .field("capability", &self.capability)
            .field("policy_version", &self.policy_version)
            .field("deadline_unix_millis", &self.deadline_unix_millis)
            .field("fencing_token", &Sensitive::new(self.fencing_token))
            .field("resource_budget", &self.resource_budget)
            .finish()
    }
}

impl MaintenanceAuthorizationGrant {
    /// Returns the authenticated identity.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the dedicated maintenance role.
    pub const fn role(&self) -> MaintenanceRole {
        self.role
    }

    /// Returns the authorized capability.
    pub const fn capability(&self) -> MaintenanceCapability {
        self.capability
    }

    /// Returns the policy version used for the decision.
    pub const fn policy_version(&self) -> u64 {
        self.policy_version
    }

    /// Returns the absolute request deadline.
    pub const fn deadline_unix_millis(&self) -> u64 {
        self.deadline_unix_millis
    }

    /// Returns the non-zero Lease fencing token.
    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Returns the resource budget pinned by the authorizing policy.
    pub const fn resource_budget(&self) -> &MaintenanceResourceBudget {
        &self.resource_budget
    }
}

/// Stateless fail-closed maintenance authorization engine.
#[derive(Clone, Debug)]
pub struct MaintenanceAuthorizer {
    loaded_policy: LoadedMaintenancePolicy,
}

impl MaintenanceAuthorizer {
    /// Creates an authorizer from an already validated policy.
    pub const fn new(loaded_policy: LoadedMaintenancePolicy) -> Self {
        Self { loaded_policy }
    }

    /// Returns the validated policy used for decisions and resource budgets.
    pub const fn policy(&self) -> &MaintenancePolicy {
        self.loaded_policy.policy()
    }

    /// Authorizes one privileged maintenance request.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenanceAuthorizationError`] for a missing context, disabled
    /// auth, anonymous or unbound identity, ordinary administrator, absent
    /// capability, invalid deadline, or missing fencing token.
    pub fn authorize(
        &self,
        context: Option<&MaintenanceAuthorizationContext>,
        now_unix_millis: u64,
    ) -> Result<MaintenanceAuthorizationGrant, MaintenanceAuthorizationError> {
        let context = context.ok_or(MaintenanceAuthorizationError::MissingAuthorizationContext)?;
        if !context.authentication_enabled {
            return Err(MaintenanceAuthorizationError::AuthenticationDisabled);
        }
        if !context.authorization_enabled {
            return Err(MaintenanceAuthorizationError::AuthorizationDisabled);
        }
        if context.request_class != MaintenanceRequestClass::PrivilegedMaintenance {
            return Err(MaintenanceAuthorizationError::InvalidRequestClass);
        }
        let principal = context
            .principal
            .as_deref()
            .filter(|principal| !principal.trim().is_empty())
            .ok_or(MaintenanceAuthorizationError::Anonymous)?;
        let roles = self
            .policy()
            .roles_for(principal)
            .ok_or_else(|| MaintenanceAuthorizationError::PrincipalUnbound(principal.to_string()))?;
        if !roles.contains(&MaintenanceRole::ReleaseOperator) {
            return Err(MaintenanceAuthorizationError::MissingRole {
                principal: principal.to_string(),
                role: MaintenanceRole::ReleaseOperator,
            });
        }
        if !self
            .policy()
            .role_allows(MaintenanceRole::ReleaseOperator, context.capability)
        {
            return Err(MaintenanceAuthorizationError::CapabilityDenied(context.capability));
        }
        if context.deadline_unix_millis <= now_unix_millis {
            return Err(MaintenanceAuthorizationError::DeadlineExpired);
        }
        let lifetime = context.deadline_unix_millis - now_unix_millis;
        if lifetime > self.policy().max_request_lifetime_millis {
            return Err(MaintenanceAuthorizationError::DeadlineTooFar {
                requested_millis: lifetime,
                maximum_millis: self.policy().max_request_lifetime_millis,
            });
        }
        let fencing_token = context
            .fencing_token
            .filter(|token| *token != 0)
            .ok_or(MaintenanceAuthorizationError::MissingFencingToken)?;

        Ok(MaintenanceAuthorizationGrant {
            principal: principal.to_string(),
            role: MaintenanceRole::ReleaseOperator,
            capability: context.capability,
            policy_version: self.policy().policy_version,
            deadline_unix_millis: context.deadline_unix_millis,
            fencing_token,
            resource_budget: self.policy().resource_budget.clone(),
        })
    }
}

/// Policy loading and semantic validation failure.
#[derive(Debug, Error)]
pub enum MaintenancePolicyError {
    #[error("invalid maintenance policy reference: {0}")]
    InvalidReference(String),
    #[error("failed to read maintenance policy {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode maintenance policy {path}: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("maintenance policy SHA-256 mismatch: expected {expected}, actual {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("maintenance policy version mismatch: expected {expected}, actual {actual}")]
    VersionMismatch { expected: u64, actual: u64 },
    #[error("invalid maintenance policy: {0}")]
    InvalidPolicy(String),
}

/// Fail-closed maintenance authorization denial.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MaintenanceAuthorizationError {
    #[error("maintenance authorization context is missing")]
    MissingAuthorizationContext,
    #[error("maintenance authentication is disabled")]
    AuthenticationDisabled,
    #[error("maintenance authorization is disabled")]
    AuthorizationDisabled,
    #[error("maintenance request class is not privileged maintenance")]
    InvalidRequestClass,
    #[error("maintenance caller is anonymous")]
    Anonymous,
    #[error("maintenance principal '{0}' is not bound by policy")]
    PrincipalUnbound(String),
    #[error("maintenance principal '{principal}' is missing role {role:?}")]
    MissingRole { principal: String, role: MaintenanceRole },
    #[error("maintenance capability {0:?} is not granted")]
    CapabilityDenied(MaintenanceCapability),
    #[error("maintenance request deadline has expired")]
    DeadlineExpired,
    #[error("maintenance request lifetime {requested_millis}ms exceeds policy maximum {maximum_millis}ms")]
    DeadlineTooFar { requested_millis: u64, maximum_millis: u64 },
    #[error("maintenance request is missing a non-zero fencing token")]
    MissingFencingToken,
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_sha256(value: &str) -> Result<(), MaintenancePolicyError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(MaintenancePolicyError::InvalidReference(
            "policy SHA-256 must be 64 lowercase hexadecimal characters".to_string(),
        ))
    }
}

fn is_canonical_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-'))
}

fn is_canonical_principal(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@' | b':' | b'/'))
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    #[test]
    fn maintenance_authorization_debug_redacts_identity_and_fencing() {
        let context = MaintenanceAuthorizationContext {
            authentication_enabled: true,
            authorization_enabled: true,
            principal: Some("principal-secret".to_string()),
            request_class: MaintenanceRequestClass::PrivilegedMaintenance,
            capability: MaintenanceCapability::ReleaseCheckpoint,
            deadline_unix_millis: 1_800_000_000_000,
            fencing_token: Some(987_654_321),
        };
        let grant = MaintenanceAuthorizationGrant {
            principal: "principal-secret".to_string(),
            role: MaintenanceRole::ReleaseOperator,
            capability: MaintenanceCapability::ReleaseCheckpoint,
            policy_version: 7,
            deadline_unix_millis: 1_800_000_000_000,
            fencing_token: 987_654_321,
            resource_budget: MaintenanceResourceBudget {
                max_checkpoint_bytes: 1024,
                max_store_members: 3,
                max_concurrent_operations: 1,
            },
        };

        for debug in [format!("{context:?}"), format!("{grant:?}")] {
            assert!(debug.contains("<redacted>"));
            assert!(!debug.contains("principal-secret"));
            assert!(!debug.contains("987654321"));
        }
    }
}
