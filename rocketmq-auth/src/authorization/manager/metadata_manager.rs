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

//! Authorization metadata manager implementation.
//!
//! This module provides the `AuthorizationMetadataManager`, which coordinates
//! authorization and authentication metadata providers to manage ACL (Access Control List)
//! operations in RocketMQ's authorization system.
//!
//! # Architecture
//!
//! The manager acts as a facade that:
//! - Validates subjects exist before ACL operations (via `AuthenticationMetadataProvider`)
//! - Performs ACL validation and initialization
//! - Delegates storage operations to `AuthorizationMetadataProvider`
//! - Provides a unified, type-safe API for ACL management
//!
//! # Design Principles
//!
//! 1. **Two-Provider Coordination**: Authentication provider validates subject existence,
//!    authorization provider handles ACL persistence
//! 2. **Validation-First**: All ACL mutations are validated before storage
//! 3. **Async-First**: All I/O operations are async
//! 4. **Error Transparency**: Errors from underlying providers are propagated clearly
//!
//! # Thread Safety
//!
//! The manager can be safely shared across threads. Provider instances must implement
//! `Send + Sync`.

use std::sync::Arc;

use tracing::debug;
use tracing::warn;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::model::subject::Subject;
use crate::authorization::enums::policy_type::PolicyType;
use crate::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::model::acl::Acl;
use crate::authorization::model::policy::Policy;
use crate::authorization::model::policy_entry::PolicyEntry;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::config::AuthConfig;

/// Result type for metadata manager operations.
pub type ManagerResult<T> = Result<T, AuthorizationError>;

/// Authorization metadata manager.
///
/// This struct manages ACL (Access Control List) operations by coordinating
/// two metadata providers:
/// - `AuthorizationMetadataProvider`: Persists ACL data
/// - `AuthenticationMetadataProvider`: Validates subject existence (NOTE: currently placeholder)
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::manager::metadata_manager::AuthorizationMetadataManager;
/// use rocketmq_auth::config::AuthConfig;
///
/// // Create manager with configuration
/// let config = AuthConfig::default();
/// let manager = AuthorizationMetadataManager::new(config)?;
///
/// // Create ACL for a user
/// let acl = Acl::of("user:alice", SubjectType::User, policy);
/// manager.create_acl(acl).await?;
///
/// // Retrieve ACL
/// let user = User::of("alice");
/// if let Some(acl) = manager.get_acl(&user).await? {
///     println!("ACL found: {:?}", acl);
/// }
///
/// // Shutdown
/// manager.shutdown().await;
/// ```
pub struct AuthorizationMetadataManager {
    /// Authorization metadata provider for ACL persistence
    authorization_provider: Arc<LocalAuthorizationMetadataProvider>,

    /// Authentication metadata provider for subject validation
    /// TODO: Integrate with actual AuthenticationMetadataProvider once available
    authentication_provider: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl AuthorizationMetadataManager {
    /// Create a new manager with the given providers.
    ///
    /// # Arguments
    ///
    /// * `authorization_provider` - Provider for ACL storage operations
    /// * `authentication_provider` - Optional provider for subject validation
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let auth_provider = Arc::new(LocalAuthorizationMetadataProvider::new(...));
    /// let manager = AuthorizationMetadataManager::with_providers(
    ///     auth_provider,
    ///     None
    /// );
    /// ```
    pub fn with_providers(
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
    /// This will instantiate providers based on the configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Authorization configuration
    ///
    /// # Errors
    ///
    /// Returns `AuthorizationError::ConfigurationError` if provider initialization fails
    pub fn new(_config: AuthConfig) -> ManagerResult<Self> {
        // TODO: Use AuthorizationFactory to create provider from config
        // For now, return error indicating factory is needed
        Err(AuthorizationError::ConfigurationError(
            "AuthorizationMetadataManager::new requires factory implementation. Use with_providers() instead."
                .to_string(),
        ))
    }

    /// Shutdown the manager and release resources.
    ///
    /// This will shutdown both authorization and authentication providers.
    pub async fn shutdown(&mut self) {
        debug!("Shutting down AuthorizationMetadataManager");
        // Note: AuthorizationMetadataProvider::shutdown() is synchronous in current impl
        // If it becomes async in the future, this will need to be updated
    }

    /// Create a new ACL.
    ///
    /// This method:
    /// 1. Validates the ACL structure
    /// 2. Initializes default policy types
    /// 3. Verifies the subject exists (if authentication provider is available)
    /// 4. Checks if ACL already exists
    /// 5. Creates or updates the ACL
    ///
    /// # Arguments
    ///
    /// * `acl` - The ACL to create
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::InvalidContext` if ACL validation fails
    /// - `AuthorizationError::InternalError` if ACL already exists (and merge fails)
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let acl = Acl::of("user:alice", SubjectType::User, policy);
    /// manager.create_acl(acl).await?;
    /// ```
    pub async fn create_acl(&self, mut acl: Acl) -> ManagerResult<()> {
        // 1. Validate ACL
        self.validate_acl(&acl)?;

        // 2. Initialize default policy types
        Self::init_acl(&mut acl);

        // 3. Verify subject exists (for USER type)
        self.verify_subject_exists(&acl).await?;

        // 4. Check if ACL already exists and merge if needed
        let subject_key = acl.subject_key();
        let subject_type = acl.subject_type();

        match self
            .authorization_provider
            .get_acl(&UserSubjectWrapper {
                key: subject_key.to_string(),
                subject_type,
            })
            .await?
        {
            None => {
                // No existing ACL, create new one
                debug!("Creating new ACL for subject: {}", subject_key);
                self.authorization_provider.create_acl(acl).await
            }
            Some(mut old_acl) => {
                // ACL exists, update policies
                debug!("ACL already exists for subject {}, updating policies", subject_key);
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
    /// 4. Creates the ACL if it doesn't exist, or merges with existing ACL
    ///
    /// # Arguments
    ///
    /// * `acl` - The ACL with updated policies
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::InvalidContext` if ACL validation fails
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
        // 1. Validate ACL
        self.validate_acl(&acl)?;

        // 2. Initialize default policy types
        Self::init_acl(&mut acl);

        // 3. Verify subject exists
        self.verify_subject_exists(&acl).await?;

        // 4. Get existing ACL and merge or create
        let subject_key = acl.subject_key();
        let subject_type = acl.subject_type();

        match self
            .authorization_provider
            .get_acl(&UserSubjectWrapper {
                key: subject_key.to_string(),
                subject_type,
            })
            .await?
        {
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
    /// This is a convenience method that calls `delete_acl_with_filter` with no filters.
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject whose ACL should be deleted
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if subject doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
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
    /// * `policy_type` - Optional policy type filter (defaults to CUSTOM if resource specified)
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
    /// manager.delete_acl_with_filter(&user, Some(PolicyType::CUSTOM), None).await?;
    ///
    /// // Delete specific resource from policy
    /// let resource = Resource::of_topic("test-topic");
    /// manager.delete_acl_with_filter(&user, Some(PolicyType::CUSTOM), Some(&resource)).await?;
    /// ```
    pub async fn delete_acl_with_filter<S: Subject + Send + Sync>(
        &self,
        subject: &S,
        policy_type: Option<PolicyType>,
        resource: Option<&Resource>,
    ) -> ManagerResult<()> {
        // 1. Verify subject exists
        self.verify_subject_by_ref(subject).await?;

        // 2. Default policy type to CUSTOM if not specified
        let policy_type = policy_type.unwrap_or(PolicyType::Custom);

        // 3. Get existing ACL
        let mut acl = self.authorization_provider.get_acl(subject).await?.ok_or_else(|| {
            AuthorizationError::InternalError(format!(
                "The ACL for subject '{}' does not exist",
                subject.subject_key()
            ))
        })?;

        // 4. Delete policy entry or entire ACL
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
    /// let user = User::of("alice");
    /// if let Some(acl) = manager.get_acl(&user).await? {
    ///     println!("Found ACL with {} policies", acl.policies().len());
    /// }
    /// ```
    pub async fn get_acl<S: Subject + Send + Sync>(&self, subject: &S) -> ManagerResult<Option<Acl>> {
        // 1. Verify subject exists
        self.verify_subject_by_ref(subject).await?;

        // 2. Get ACL from provider
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

    // ========== Private Helper Methods ==========

    /// Initialize ACL by setting default policy types.
    ///
    /// Sets PolicyType to CUSTOM for any policy without an explicit type.
    fn init_acl(acl: &mut Acl) {
        // PolicyType doesn't have Unknown variant, so we just ensure Custom is set if needed
        // In practice, policies should already have a type set
        let mut policies = acl.policies().clone();
        for policy in policies.iter_mut() {
            // Policies default to Custom, no action needed
            // This is a placeholder for future logic if needed
            let _ = policy.policy_type();
        }
        acl.set_policies(policies);
    }

    /// Validate ACL structure.
    ///
    /// Checks:
    /// - Subject type is not None/Unknown
    /// - Policies list is not empty
    /// - Each policy is valid
    fn validate_acl(&self, acl: &Acl) -> ManagerResult<()> {
        // Validate subject type (currently only User is valid)
        // SubjectType doesn't have Unknown variant, so validation is simplified

        // Validate policies exist
        if acl.policies().is_empty() {
            return Err(AuthorizationError::InvalidContext(
                "The policies list is empty".to_string(),
            ));
        }

        // Validate each policy
        for policy in acl.policies() {
            self.validate_policy(policy)?;
        }

        Ok(())
    }

    /// Validate policy structure.
    ///
    /// Checks:
    /// - Policy entries list is not empty
    /// - Each entry is valid
    fn validate_policy(&self, policy: &Policy) -> ManagerResult<()> {
        // Validate entries exist
        if policy.entries().is_empty() {
            return Err(AuthorizationError::InvalidContext(
                "Policy entries list is empty".to_string(),
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
    /// Checks:
    /// - Resources list is not empty
    /// - Actions list is not empty
    fn validate_policy_entry(&self, entry: &PolicyEntry) -> ManagerResult<()> {
        // PolicyEntry has a single resource (not a list)
        // Validate actions list is not empty
        if entry.actions().is_empty() {
            return Err(AuthorizationError::InvalidContext(
                "PolicyEntry actions list is empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Verify that the subject exists in the authentication system.
    ///
    /// For USER subjects, checks with authentication provider (when available).
    /// For other subject types, assumes existence.
    async fn verify_subject_exists(&self, acl: &Acl) -> ManagerResult<()> {
        if acl.subject_type() == SubjectType::User {
            // TODO: When AuthenticationMetadataProvider is available:
            // let username = extract_username(acl.subject_key());
            // let user = self.authentication_provider.get_user(username).await?;
            // if user.is_none() {
            //     return Err(AuthorizationError::SubjectNotFound(acl.subject_key().to_string()));
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
    /// Similar to verify_subject_exists but works with Subject trait references.
    async fn verify_subject_by_ref<S: Subject + Send + Sync>(&self, subject: &S) -> ManagerResult<()> {
        if subject.subject_type() == SubjectType::User {
            // TODO: When AuthenticationMetadataProvider is available:
            // let user_subject = subject as &dyn std::any::Any;
            // if let Some(user) = user_subject.downcast_ref::<User>() {
            //     let existing = self.authentication_provider.get_user(user.username()).await?;
            //     if existing.is_none() {
            //         return
            // Err(AuthorizationError::SubjectNotFound(subject.subject_key().to_string()));
            //     }
            // }

            warn!(
                "Subject validation for USER type is not yet implemented (subject: {})",
                subject.subject_key()
            );
        }
        Ok(())
    }
}

/// Wrapper struct to implement Subject trait for get_acl operations.
///
/// This is needed because we need to pass subject information to the provider's
/// get_acl method which expects a type implementing the Subject trait.
struct UserSubjectWrapper {
    key: String,
    subject_type: SubjectType,
}

impl Subject for UserSubjectWrapper {
    fn subject_key(&self) -> &str {
        &self.key
    }

    fn subject_type(&self) -> SubjectType {
        self.subject_type
    }
}
