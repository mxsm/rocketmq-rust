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

//! Runtime-neutral, fail-closed authorization for maintenance operations.

use std::collections::BTreeSet;
use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::Secret;

/// Current maintenance policy schema.
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
    /// Policy schema version.
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
    /// Validates every policy invariant without granting authorization.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenancePolicyError`] when the policy could authorize an
    /// operation without authentication, authorization, fencing, or a bounded
    /// resource budget.
    pub fn validate(&self) -> Result<(), MaintenancePolicyError> {
        validate_policy(self)
    }

    /// Consumes a completely validated policy for use by an authorizer.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenancePolicyError`] for the same fail-closed invariants
    /// checked by [`Self::validate`].
    pub fn into_validated(self) -> Result<ValidatedMaintenancePolicy, MaintenancePolicyError> {
        validate_policy(&self)?;
        Ok(ValidatedMaintenancePolicy(self))
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

/// A maintenance policy whose complete semantic contract has been validated.
#[derive(Clone, Debug)]
pub struct ValidatedMaintenancePolicy(MaintenancePolicy);

impl ValidatedMaintenancePolicy {
    /// Returns the immutable validated policy.
    pub const fn policy(&self) -> &MaintenancePolicy {
        &self.0
    }
}

/// Authenticated request facts supplied by a service composition root.
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
            .field("principal", &Secret::new(self.principal.as_deref()))
            .field("request_class", &self.request_class)
            .field("capability", &self.capability)
            .field("deadline_unix_millis", &self.deadline_unix_millis)
            .field("fencing_token", &Secret::new(self.fencing_token))
            .finish()
    }
}

/// Auditable authorization result carried into a maintenance operation.
///
/// All fields are private and there is intentionally no unchecked constructor.
/// A grant can only be produced by [`MaintenanceAuthorizer::authorize`].
#[derive(Clone, Eq, PartialEq)]
pub struct MaintenanceAuthorizationGrant {
    principal: String,
    role: MaintenanceRole,
    capability: MaintenanceCapability,
    policy_version: u64,
    deadline_unix_millis: u64,
    fencing_token: u64,
    resource_budget: MaintenanceResourceBudget,
}

impl fmt::Debug for MaintenanceAuthorizationGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceAuthorizationGrant")
            .field("principal", &Secret::new(&self.principal))
            .field("role", &self.role)
            .field("capability", &self.capability)
            .field("policy_version", &self.policy_version)
            .field("deadline_unix_millis", &self.deadline_unix_millis)
            .field("fencing_token", &Secret::new(self.fencing_token))
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

    /// Returns the non-zero lease fencing token.
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
    policy: ValidatedMaintenancePolicy,
}

impl MaintenanceAuthorizer {
    /// Creates an authorizer from a policy that has already passed validation.
    pub fn new(policy: impl Into<ValidatedMaintenancePolicy>) -> Self {
        Self { policy: policy.into() }
    }

    /// Returns the validated policy used for decisions and resource budgets.
    pub const fn policy(&self) -> &MaintenancePolicy {
        self.policy.policy()
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

/// Semantic maintenance policy validation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MaintenancePolicyError {
    /// A fail-closed policy invariant was not satisfied.
    #[error("invalid maintenance policy: {0}")]
    InvalidPolicy(String),
}

/// Fail-closed maintenance authorization denial.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MaintenanceAuthorizationError {
    #[error("maintenance authorization context is missing")]
    /// Represents the missing authorization context case.
    MissingAuthorizationContext,
    #[error("maintenance authentication is disabled")]
    /// Represents the authentication disabled case.
    AuthenticationDisabled,
    #[error("maintenance authorization is disabled")]
    /// Represents the authorization disabled case.
    AuthorizationDisabled,
    #[error("maintenance request class is not privileged maintenance")]
    /// Represents the invalid request class case.
    InvalidRequestClass,
    #[error("maintenance caller is anonymous")]
    /// Represents the anonymous case.
    Anonymous,
    #[error("maintenance principal '{0}' is not bound by policy")]
    /// Represents the principal unbound case.
    PrincipalUnbound(String),
    #[error("maintenance principal '{principal}' is missing role {role:?}")]
    /// Represents the missing role case.
    MissingRole {
        /// The principal value.
        principal: String,
        /// The role value.
        role: MaintenanceRole,
    },
    #[error("maintenance capability {0:?} is not granted")]
    /// Represents the capability denied case.
    CapabilityDenied(MaintenanceCapability),
    #[error("maintenance request deadline has expired")]
    /// Represents the deadline expired case.
    DeadlineExpired,
    #[error("maintenance request lifetime {requested_millis}ms exceeds policy maximum {maximum_millis}ms")]
    /// Represents the deadline too far case.
    DeadlineTooFar {
        /// The requested duration in milliseconds.
        requested_millis: u64,
        /// The maximum duration in milliseconds.
        maximum_millis: u64,
    },
    #[error("maintenance request is missing a non-zero fencing token")]
    /// Represents the missing fencing token case.
    MissingFencingToken,
}

fn validate_policy(policy: &MaintenancePolicy) -> Result<(), MaintenancePolicyError> {
    if policy.schema_version != MAINTENANCE_POLICY_SCHEMA_VERSION {
        return invalid_policy(format!("schema_version must be {MAINTENANCE_POLICY_SCHEMA_VERSION}"));
    }
    if !is_canonical_identifier(&policy.policy_id) {
        return invalid_policy("policy_id must be a canonical lowercase identifier");
    }
    if policy.policy_version == 0 {
        return invalid_policy("policy_version must be greater than zero");
    }
    if !policy.require_authentication || !policy.require_authorization {
        return invalid_policy("maintenance policy must require authentication and authorization");
    }
    if !policy.require_fencing_token {
        return invalid_policy("maintenance policy must require fencing tokens");
    }
    if !(1_000..=86_400_000).contains(&policy.max_request_lifetime_millis) {
        return invalid_policy("max_request_lifetime_millis must be between 1000 and 86400000");
    }
    if policy.resource_budget.max_checkpoint_bytes == 0
        || policy.resource_budget.max_store_members == 0
        || policy.resource_budget.max_concurrent_operations == 0
    {
        return invalid_policy("maintenance resource limits must be greater than zero");
    }

    let mut principals = BTreeSet::new();
    let mut has_release_operator = false;
    for binding in &policy.principal_bindings {
        if !is_canonical_principal(&binding.principal) {
            return invalid_policy("principal bindings must use non-empty canonical identities");
        }
        if !principals.insert(binding.principal.as_str()) {
            return invalid_policy(format!("principal '{}' is bound more than once", binding.principal));
        }
        if binding.roles.is_empty() {
            return invalid_policy(format!("principal '{}' has no maintenance role", binding.principal));
        }
        has_release_operator |= binding.roles.contains(&MaintenanceRole::ReleaseOperator);
    }
    if !has_release_operator {
        return invalid_policy("at least one principal must be bound to release_operator");
    }

    let mut granted_roles = BTreeSet::new();
    let mut release_checkpoint_granted = false;
    for grant in &policy.role_grants {
        if !granted_roles.insert(grant.role) {
            return invalid_policy(format!("role '{:?}' is granted more than once", grant.role));
        }
        if grant.capabilities.is_empty() {
            return invalid_policy(format!("role '{:?}' has no capability", grant.role));
        }
        if grant.role == MaintenanceRole::Administrator
            && grant.capabilities.contains(&MaintenanceCapability::ReleaseCheckpoint)
        {
            return invalid_policy("administrator cannot be granted release_checkpoint");
        }
        release_checkpoint_granted |= grant.role == MaintenanceRole::ReleaseOperator
            && grant.capabilities.contains(&MaintenanceCapability::ReleaseCheckpoint);
    }
    if !release_checkpoint_granted {
        return invalid_policy("release_operator must be granted release_checkpoint");
    }
    Ok(())
}

fn invalid_policy<T>(reason: impl Into<String>) -> Result<T, MaintenancePolicyError> {
    Err(MaintenancePolicyError::InvalidPolicy(reason.into()))
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
mod tests {
    use super::*;

    fn policy() -> MaintenancePolicy {
        MaintenancePolicy {
            schema_version: MAINTENANCE_POLICY_SCHEMA_VERSION,
            policy_id: "rocketmq.production-maintenance".to_string(),
            policy_version: 7,
            require_authentication: true,
            require_authorization: true,
            require_fencing_token: true,
            max_request_lifetime_millis: 30_000,
            resource_budget: MaintenanceResourceBudget {
                max_checkpoint_bytes: 1024,
                max_store_members: 3,
                max_concurrent_operations: 1,
            },
            principal_bindings: vec![MaintenancePrincipalBinding {
                principal: "release-operator".to_string(),
                roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
            }],
            role_grants: vec![MaintenanceRoleGrant {
                role: MaintenanceRole::ReleaseOperator,
                capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
            }],
        }
    }

    fn context() -> MaintenanceAuthorizationContext {
        MaintenanceAuthorizationContext {
            authentication_enabled: true,
            authorization_enabled: true,
            principal: Some("release-operator".to_string()),
            request_class: MaintenanceRequestClass::PrivilegedMaintenance,
            capability: MaintenanceCapability::ReleaseCheckpoint,
            deadline_unix_millis: 120_000,
            fencing_token: Some(42),
        }
    }

    #[test]
    fn grant_pins_policy_deadline_fencing_and_budget() {
        let authorizer = MaintenanceAuthorizer::new(policy().into_validated().expect("valid policy"));
        let grant = authorizer.authorize(Some(&context()), 100_000).expect("authorized");

        assert_eq!(grant.policy_version(), 7);
        assert_eq!(grant.deadline_unix_millis(), 120_000);
        assert_eq!(grant.fencing_token(), 42);
        assert_eq!(grant.resource_budget().max_checkpoint_bytes, 1024);
    }

    #[test]
    fn grant_cannot_bypass_fail_closed_inputs() {
        let authorizer = MaintenanceAuthorizer::new(policy().into_validated().expect("valid policy"));
        let mut request = context();
        request.authentication_enabled = false;
        assert_eq!(
            authorizer.authorize(Some(&request), 100_000),
            Err(MaintenanceAuthorizationError::AuthenticationDisabled)
        );
        request.authentication_enabled = true;
        request.fencing_token = None;
        assert_eq!(
            authorizer.authorize(Some(&request), 100_000),
            Err(MaintenanceAuthorizationError::MissingFencingToken)
        );
        request.fencing_token = Some(42);
        request.deadline_unix_millis = 100_000;
        assert_eq!(
            authorizer.authorize(Some(&request), 100_000),
            Err(MaintenanceAuthorizationError::DeadlineExpired)
        );
    }
}
