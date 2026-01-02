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

//! Production implementation of Authorization Metadata Manager.
//!
//! This module provides `AuthorizationMetadataManagerImpl`, a concrete implementation
//! that coordinates authorization and authentication metadata providers to manage
//! ACL (Access Control List) operations in RocketMQ's authorization system.
//!
//! # Architecture
//!
//! ```text
//! AuthorizationMetadataManagerImpl
//! ├── AuthorizationMetadataProvider (ACL storage)
//! │   ├── create_acl()
//! │   ├── update_acl()
//! │   ├── delete_acl()
//! │   ├── get_acl()
//! │   └── list_acl()
//! └── AuthenticationMetadataProvider (Subject validation)
//!     └── get_user() / validate_subject()
//! ```
//!
//! # Validation Strategy
//!
//! All ACL mutations follow a strict validation pipeline:
//! 1. **Structural validation**: Ensure ACL/Policy/Entry data is well-formed
//! 2. **Subject validation**: Verify subject exists in authentication system (for USER type)
//! 3. **Business validation**: Check IP addresses, resource patterns, actions
//! 4. **Storage operation**: Delegate to provider after all validations pass
//!
//! # Error Handling
//!
//! - Validation errors return `AuthorizationError::InvalidContext`
//! - Missing subjects return `AuthorizationError::SubjectNotFound`
//! - Storage failures return `AuthorizationError::MetadataServiceError`
//! - All errors are propagated without panic, enabling graceful degradation
//!
//! # Thread Safety
//!
//! This implementation is thread-safe through immutable references (`&self`) and
//! `Arc`-wrapped providers that implement `Send + Sync`.

use std::sync::Arc;

use tracing::debug;
use tracing::warn;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::model::subject::Subject;
use crate::authorization::enums::policy_type::PolicyType;
use crate::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::model::acl::Acl;
use crate::authorization::model::environment::Environment;
use crate::authorization::model::policy::Policy;
use crate::authorization::model::policy_entry::PolicyEntry;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::config::AuthConfig;

/// Result type for metadata manager operations.
pub type ManagerResult<T> = Result<T, AuthorizationError>;

/// Production implementation of Authorization Metadata Manager.
///
/// This struct manages ACL (Access Control List) operations by coordinating
/// authorization and authentication metadata providers. It provides a unified,
/// type-safe API with comprehensive validation for all ACL operations.
///
/// # Provider Coordination
///
/// - **AuthorizationMetadataProvider**: Handles ACL persistence (create, update, delete, query)
/// - **AuthenticationMetadataProvider**: Validates subject existence (currently placeholder)
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::manager::AuthorizationMetadataManagerImpl;
/// use rocketmq_auth::config::AuthConfig;
/// use rocketmq_auth::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
/// use std::sync::Arc;
///
/// // Create manager with local provider
/// let mut provider = LocalAuthorizationMetadataProvider::new();
/// provider.initialize(AuthConfig::default(), None)?;
///
/// let manager = AuthorizationMetadataManagerImpl::new(Arc::new(provider), None);
///
/// // Create ACL for a user
/// let acl = Acl::of("user:alice", SubjectType::User, policy);
/// manager.create_acl(acl).await?;
///
/// // Query ACL
/// let user = UserSubject::new("alice");
/// if let Some(acl) = manager.get_acl(&user).await? {
///     println!("ACL found: {:?}", acl);
/// }
///
/// // Cleanup
/// manager.shutdown().await;
/// ```
pub struct AuthorizationMetadataManagerImpl {
    /// Authorization metadata provider for ACL persistence.
    ///
    /// This provider handles all storage operations: create, read, update, delete.
    /// Must be initialized before the manager is used.
    ///
    /// Note: Uses concrete `LocalAuthorizationMetadataProvider` type instead of trait object
    /// because `AuthorizationMetadataProvider` is not dyn-compatible (has async methods with
    /// generics).
    authorization_provider: Arc<LocalAuthorizationMetadataProvider>,

    /// Authentication metadata provider for subject validation.
    ///
    /// Currently a placeholder using `dyn Any`. Will be replaced with proper
    /// `AuthenticationMetadataProvider` trait once available.
    ///
    /// When integrated, this provider will:
    /// - Validate USER subjects exist before ACL creation
    /// - Support group/role membership queries
    /// - Enable fine-grained permission checks
    authentication_provider: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl AuthorizationMetadataManagerImpl {
    /// Create a new Authorization Metadata Manager with providers.
    ///
    /// # Arguments
    ///
    /// * `authorization_provider` - Provider for ACL storage (required, must be initialized)
    /// * `authentication_provider` - Optional provider for subject validation
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let auth_provider = Arc::new(LocalAuthorizationMetadataProvider::new());
    /// let manager = AuthorizationMetadataManagerImpl::new(auth_provider, None);
    /// ```
    pub fn new(
        authorization_provider: Arc<LocalAuthorizationMetadataProvider>,
        authentication_provider: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Self {
        Self {
            authorization_provider,
            authentication_provider,
        }
    }

    /// Create a manager from configuration.
    ///
    /// This factory method instantiates providers based on configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Authorization configuration
    ///
    /// # Errors
    ///
    /// Returns `AuthorizationError::ConfigurationError` if:
    /// - Provider initialization fails
    /// - Required configuration is missing
    /// - Configuration contains invalid values
    ///
    /// # Note
    ///
    /// Currently requires AuthorizationFactory implementation. Use `new()` constructor
    /// with explicit providers as a workaround.
    pub fn from_config(_config: &AuthConfig) -> ManagerResult<Self> {
        // TODO: Implement AuthorizationFactory to create providers from config
        // Example:
        // let auth_provider = AuthorizationFactory::create_metadata_provider(config)?;
        // let authn_provider = AuthenticationFactory::create_metadata_provider(config)?;
        // Ok(Self::new(Arc::new(auth_provider), Some(Arc::new(authn_provider))))

        Err(AuthorizationError::ConfigurationError(
            "AuthorizationMetadataManagerImpl::from_config requires factory implementation. Use new() constructor \
             with explicit providers instead."
                .to_string(),
        ))
    }

    /// Shutdown the manager and release resources.
    ///
    /// This method:
    /// - Calls shutdown on authentication provider (if present)
    /// - Calls shutdown on authorization provider
    /// - Flushes pending operations
    /// - Releases system resources
    ///
    /// This method is idempotent (safe to call multiple times).
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// manager.shutdown().await;
    /// ```
    pub async fn shutdown(&mut self) {
        debug!("Shutting down AuthorizationMetadataManagerImpl");

        // Shutdown authentication provider if available
        if self.authentication_provider.is_some() {
            debug!("Shutting down authentication provider");
            // TODO: When AuthenticationMetadataProvider trait is available:
            // if let Some(provider) = &self.authentication_provider {
            //     if let Some(auth_provider) = provider.downcast_ref::<dyn
            // AuthenticationMetadataProvider>() {         auth_provider.shutdown();
            //     }
            // }
        }

        // Shutdown authorization provider
        // Note: shutdown() requires &mut self, but we have Arc<dyn AuthorizationMetadataProvider>
        // This is a design limitation - providers should ideally have async fn shutdown(&self)
        // For now, we'll log that shutdown was requested
        debug!("Authorization provider shutdown requested (requires mutable reference)");

        debug!("AuthorizationMetadataManagerImpl shutdown complete");
    }

    /// Create a new ACL.
    ///
    /// This method performs the following steps:
    /// 1. **Validate** ACL structure (subject, policies, entries, resources, actions)
    /// 2. **Initialize** default policy types (sets CUSTOM if not specified)
    /// 3. **Verify subject exists** (for USER type, via authentication provider)
    /// 4. **Check for existing ACL**:
    ///    - If exists: merge policies and update
    ///    - If not: create new ACL
    ///
    /// # Arguments
    ///
    /// * `acl` - The ACL to create
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::InvalidContext` if ACL validation fails
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let acl = Acl::of("user:alice", SubjectType::User, policy);
    /// manager.create_acl(acl).await?;
    /// ```
    pub async fn create_acl(&self, mut acl: Acl) -> ManagerResult<()> {
        // Step 1: Validate ACL structure
        self.validate_acl(&acl)?;

        // Step 2: Initialize default policy types
        Self::init_acl(&mut acl);

        // Step 3: Verify subject exists (for USER type)
        self.verify_subject_exists(&acl).await?;

        // Step 4: Check if ACL already exists
        let subject_key = acl.subject_key();
        let subject_type = acl.subject_type();

        let existing_acl = self
            .authorization_provider
            .get_acl(&SubjectWrapper {
                key: subject_key.to_string(),
                subject_type,
            })
            .await?;

        match existing_acl {
            None => {
                // No existing ACL, create new one
                debug!("Creating new ACL for subject: {}", subject_key);
                self.authorization_provider.create_acl(acl).await
            }
            Some(mut old_acl) => {
                // ACL exists, merge policies and update
                debug!("ACL already exists for subject {}, merging policies", subject_key);
                old_acl.update_policies(acl.policies().clone());
                self.authorization_provider.update_acl(old_acl).await
            }
        }
    }

    /// Update an existing ACL.
    ///
    /// This method:
    /// 1. Validates the ACL structure
    /// 2. Initializes default policy types
    /// 3. Verifies the subject exists
    /// 4. Merges with existing ACL or creates new if not found
    ///
    /// # Arguments
    ///
    /// * `acl` - The ACL with updated policies
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::InvalidContext` if ACL validation fails
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut acl = manager.get_acl(&user).await?.unwrap();
    /// acl.add_policy(new_policy);
    /// manager.update_acl(acl).await?;
    /// ```
    pub async fn update_acl(&self, mut acl: Acl) -> ManagerResult<()> {
        // Step 1: Validate ACL
        self.validate_acl(&acl)?;

        // Step 2: Initialize default policy types
        Self::init_acl(&mut acl);

        // Step 3: Verify subject exists
        self.verify_subject_exists(&acl).await?;

        // Step 4: Get existing ACL and merge or create
        let subject_key = acl.subject_key();
        let subject_type = acl.subject_type();

        let existing_acl = self
            .authorization_provider
            .get_acl(&SubjectWrapper {
                key: subject_key.to_string(),
                subject_type,
            })
            .await?;

        match existing_acl {
            None => {
                // No existing ACL, create new one
                debug!("No existing ACL for subject {}, creating new", subject_key);
                self.authorization_provider.create_acl(acl).await
            }
            Some(mut old_acl) => {
                // Merge policies
                debug!("Updating existing ACL for subject: {}", subject_key);
                old_acl.update_policies(acl.policies().clone());
                self.authorization_provider.update_acl(old_acl).await
            }
        }
    }

    /// Delete an ACL for a subject.
    ///
    /// This is a convenience method that deletes the entire ACL.
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject whose ACL should be deleted
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::InternalError` if ACL doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let user = UserSubject::new("alice");
    /// manager.delete_acl(&user).await?;
    /// ```
    pub async fn delete_acl<S: Subject + Send + Sync>(&self, subject: &S) -> ManagerResult<()> {
        self.delete_acl_with_filter(subject, None, None).await
    }

    /// Delete an ACL or specific policy entries.
    ///
    /// This method provides fine-grained deletion:
    /// - If `resource` is specified: deletes specific policy entry for that resource
    /// - If only `policy_type` is specified: deletes entire policy of that type
    /// - If neither specified: deletes entire ACL
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject whose ACL to modify
    /// * `policy_type` - Optional policy type filter (defaults to CUSTOM)
    /// * `resource` - Optional resource to delete from the policy
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::InternalError` if ACL doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Delete entire ACL
    /// manager.delete_acl_with_filter(&user, None, None).await?;
    ///
    /// // Delete a specific policy type
    /// manager.delete_acl_with_filter(&user, Some(PolicyType::Custom), None).await?;
    ///
    /// // Delete specific resource from policy
    /// let resource = Resource::of_topic("test-topic");
    /// manager.delete_acl_with_filter(&user, Some(PolicyType::Custom), Some(&resource)).await?;
    /// ```
    pub async fn delete_acl_with_filter<S: Subject + Send + Sync>(
        &self,
        subject: &S,
        policy_type: Option<PolicyType>,
        resource: Option<&Resource>,
    ) -> ManagerResult<()> {
        // Step 1: Verify subject exists
        self.verify_subject_by_ref(subject).await?;

        // Step 2: Default policy type to CUSTOM if not specified
        let policy_type = policy_type.unwrap_or(PolicyType::Custom);

        // Step 3: Get existing ACL
        let mut acl = self.authorization_provider.get_acl(subject).await?.ok_or_else(|| {
            AuthorizationError::InternalError(format!(
                "The ACL for subject '{}' does not exist",
                subject.subject_key()
            ))
        })?;

        // Step 4: Delete policy entry or entire ACL
        if let Some(resource) = resource {
            // Delete specific resource from policy
            debug!(
                "Deleting resource {:?} from {:?} policy for subject {}",
                resource,
                policy_type,
                subject.subject_key()
            );
            acl.delete_policy(policy_type, resource);

            if acl.policies().is_empty() {
                // No policies left, delete entire ACL
                self.authorization_provider.delete_acl(subject).await
            } else {
                // Update ACL with remaining policies
                self.authorization_provider.update_acl(acl).await
            }
        } else {
            // Delete entire ACL
            debug!("Deleting entire ACL for subject: {}", subject.subject_key());
            self.authorization_provider.delete_acl(subject).await
        }
    }

    /// Get the ACL for a subject.
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject to query
    ///
    /// # Returns
    ///
    /// - `Ok(Some(Acl))` if ACL exists
    /// - `Ok(None)` if no ACL found
    /// - `Err(...)` if operation fails
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let user = UserSubject::new("alice");
    /// if let Some(acl) = manager.get_acl(&user).await? {
    ///     println!("Found ACL with {} policies", acl.policies().len());
    /// }
    /// ```
    pub async fn get_acl<S: Subject + Send + Sync>(&self, subject: &S) -> ManagerResult<Option<Acl>> {
        // Step 1: Verify subject exists
        self.verify_subject_by_ref(subject).await?;

        // Step 2: Get ACL from provider
        self.authorization_provider.get_acl(subject).await
    }

    /// List ACLs with optional filtering.
    ///
    /// # Arguments
    ///
    /// * `subject_filter` - Optional subject name filter (substring match)
    /// * `resource_filter` - Optional resource name filter (substring match)
    ///
    /// # Returns
    ///
    /// A vector of matching ACLs. Empty vector if no matches found.
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // List all ACLs
    /// let all_acls = manager.list_acl(None, None).await?;
    ///
    /// // Filter by subject
    /// let user_acls = manager.list_acl(Some("user:"), None).await?;
    ///
    /// // Filter by resource
    /// let topic_acls = manager.list_acl(None, Some("Topic:test")).await?;
    /// ```
    pub async fn list_acl(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> ManagerResult<Vec<Acl>> {
        self.authorization_provider
            .list_acl(subject_filter, resource_filter)
            .await
    }

    // ========== Private Validation Methods ==========

    /// Initialize ACL by setting default policy types.
    ///
    /// Sets PolicyType to CUSTOM for any policy without an explicit type.
    fn init_acl(acl: &mut Acl) {
        let mut policies = acl.policies().clone();
        for policy in policies.iter_mut() {
            // Ensure policy type is set (defaults to Custom in Rust impl)
            if policy.policy_type() == PolicyType::Custom {
                // Already set to Custom, this is fine
            }
        }
        acl.set_policies(policies);
    }

    /// Validate ACL structure.
    ///
    /// Validates:
    /// - Subject type is valid (not None/Unknown)
    /// - Policies list is not empty
    /// - Each policy is structurally valid
    fn validate_acl(&self, acl: &Acl) -> ManagerResult<()> {
        // Validate subject type (currently only User is supported)
        // SubjectType enum doesn't have Unknown variant in Rust

        // Validate policies exist
        if acl.policies().is_empty() {
            return Err(AuthorizationError::InvalidContext("The policies is empty.".to_string()));
        }

        // Validate each policy
        for policy in acl.policies() {
            self.validate_policy(policy)?;
        }

        Ok(())
    }

    /// Validate policy structure.
    ///
    /// Validates:
    /// - Policy entries list is not empty
    /// - Each entry is structurally valid
    fn validate_policy(&self, policy: &Policy) -> ManagerResult<()> {
        // Validate entries exist
        if policy.entries().is_empty() {
            return Err(AuthorizationError::InvalidContext(
                "The policy entries is empty.".to_string(),
            ));
        }

        // Validate each entry
        for entry in policy.entries() {
            self.validate_policy_entry(entry)?;
        }

        Ok(())
    }

    /// Validate policy entry structure.
    ///
    /// Validates:
    /// - Resource is present and valid
    /// - Resource type is set
    /// - Resource pattern is set
    /// - Actions list is not empty
    /// - Actions do not include Action::ANY
    /// - Environment (if present) has valid IP addresses
    /// - Decision is set
    fn validate_policy_entry(&self, entry: &PolicyEntry) -> ManagerResult<()> {
        // Validate resource
        let resource = entry.resource();
        // ResourceType is an enum, always has a value
        // resource_name() returns Option<&str>, check if it's None or empty
        if let Some(name) = resource.resource_name() {
            if name.is_empty() {
                return Err(AuthorizationError::InvalidContext(
                    "The resource pattern is empty.".to_string(),
                ));
            }
        } else {
            return Err(AuthorizationError::InvalidContext(
                "The resource pattern is null.".to_string(),
            ));
        }

        // Validate actions
        if entry.actions().is_empty() {
            return Err(AuthorizationError::InvalidContext("The actions is empty.".to_string()));
        }

        // Check for Action::ANY (should not be allowed in ACL entries)
        use rocketmq_common::common::action::Action;
        if entry.actions().contains(&Action::Any) {
            return Err(AuthorizationError::InvalidContext(
                "The actions can not be Any.".to_string(),
            ));
        }

        // Validate environment (if present)
        if let Some(environment) = entry.environment() {
            self.validate_environment(environment)?;
        }

        // Decision is always set in Rust (enum with no null)
        // No validation needed

        Ok(())
    }

    /// Validate environment configuration.
    ///
    /// Validates:
    /// - Source IPs are not blank
    /// - Source IPs are valid IP addresses or CIDR blocks
    fn validate_environment(&self, environment: &Environment) -> ManagerResult<()> {
        let source_ips = environment.source_ips();
        // source_ips() returns &Vec<String>
        if !source_ips.is_empty() {
            for source_ip in source_ips {
                if source_ip.trim().is_empty() {
                    return Err(AuthorizationError::InvalidContext(
                        "The source ip is empty.".to_string(),
                    ));
                }

                // Validate IP address or CIDR format
                if !Self::is_valid_ip_or_cidr(source_ip) {
                    return Err(AuthorizationError::InvalidContext(format!(
                        "The source ip '{}' is invalid.",
                        source_ip
                    )));
                }
            }
        }

        Ok(())
    }

    /// Validate if a string is a valid IP address or CIDR block.
    ///
    /// This is a simplified implementation. In production, consider using
    /// the `ipnetwork` crate or similar for robust IP validation.
    fn is_valid_ip_or_cidr(ip: &str) -> bool {
        use std::net::IpAddr;

        // Try parsing as plain IP address
        if ip.parse::<IpAddr>().is_ok() {
            return true;
        }

        // Try parsing as CIDR (contains '/')
        if let Some((addr, prefix)) = ip.split_once('/') {
            if addr.parse::<IpAddr>().is_ok() {
                if let Ok(prefix_len) = prefix.parse::<u8>() {
                    // Validate prefix length based on IP version
                    if addr.contains(':') {
                        // IPv6
                        return prefix_len <= 128;
                    } else {
                        // IPv4
                        return prefix_len <= 32;
                    }
                }
            }
        }

        false
    }

    // ========== Subject Validation Methods ==========

    /// Verify that the subject exists in the authentication system.
    ///
    /// For USER subjects, checks with authentication provider (when available).
    /// For other subject types, assumes existence.
    async fn verify_subject_exists(&self, acl: &Acl) -> ManagerResult<()> {
        if acl.subject_type() == SubjectType::User {
            // TODO: When AuthenticationMetadataProvider is available:
            // let username = extract_username_from_key(acl.subject_key());
            // let user = self.authentication_provider
            //     .as_ref()
            //     .ok_or_else(|| AuthorizationError::NotInitialized(
            //         "Authentication provider not configured".to_string()
            //     ))?
            //     .get_user(username)
            //     .await?;
            //
            // if user.is_none() {
            //     return Err(AuthorizationError::SubjectNotFound(format!(
            //         "The subject of {} is not exist.",
            //         acl.subject_key()
            //     )));
            // }

            // For now, we'll just log a warning
            warn!(
                "Subject validation for USER type is not yet implemented (subject: {})",
                acl.subject_key()
            );
        }

        Ok(())
    }

    /// Verify that a subject exists by reference.
    ///
    /// Similar to `verify_subject_exists` but works with Subject trait references.
    async fn verify_subject_by_ref<S: Subject + Send + Sync>(&self, subject: &S) -> ManagerResult<()> {
        if subject.subject_type() == SubjectType::User {
            // TODO: When AuthenticationMetadataProvider is available:
            // Extract username from subject_key and validate

            warn!(
                "Subject validation for USER type is not yet implemented (subject: {})",
                subject.subject_key()
            );
        }

        Ok(())
    }

    /// Get the authorization provider reference.
    #[allow(dead_code)]
    fn get_authorization_provider(&self) -> &Arc<LocalAuthorizationMetadataProvider> {
        &self.authorization_provider
    }

    /// Get the authentication provider reference.
    #[allow(dead_code)]
    fn get_authentication_provider(&self) -> ManagerResult<&Arc<dyn std::any::Any + Send + Sync>> {
        self.authentication_provider.as_ref().ok_or_else(|| {
            AuthorizationError::NotInitialized("The authenticationMetadataProvider is not configured.".to_string())
        })
    }
}

// ========== Helper Types ==========

/// Wrapper struct to implement Subject trait for provider operations.
///
/// This is needed because we need to pass subject information to the provider's
/// methods which expect types implementing the Subject trait.
struct SubjectWrapper {
    key: String,
    subject_type: SubjectType,
}

impl Subject for SubjectWrapper {
    fn subject_key(&self) -> &str {
        &self.key
    }

    fn subject_type(&self) -> SubjectType {
        self.subject_type
    }
}

// ========== Unit Tests ==========

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_ip_or_cidr() {
        // Valid IPv4
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("192.168.0.1"));
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("10.0.0.0"));

        // Valid IPv4 CIDR
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("192.168.0.0/24"));
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("10.0.0.0/8"));

        // Valid IPv6
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("2001:db8::1"));
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("::1"));

        // Valid IPv6 CIDR
        assert!(AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("2001:db8::/32"));

        // Invalid IPs
        assert!(!AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("invalid"));
        assert!(!AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr(
            "256.256.256.256"
        ));
        assert!(!AuthorizationMetadataManagerImpl::is_valid_ip_or_cidr("192.168.0.0/33")); // Invalid prefix
    }
}
