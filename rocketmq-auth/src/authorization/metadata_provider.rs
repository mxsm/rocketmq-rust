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

//! Authorization metadata provider abstraction.
//!
//! This module provides the core trait for managing authorization metadata (ACLs)
//! in RocketMQ. Implementations can store metadata in various backends such as:
//! - Local files (YAML, JSON)
//! - Embedded databases (RocksDB, SQLite)
//! - Remote databases (MySQL, PostgreSQL)
//! - Distributed configuration services (etcd, Consul)

// Sub-modules
pub mod local;

use std::any::Any;

// Re-export commonly used types
pub use local::LocalAuthorizationMetadataProvider;

use crate::authentication::model::subject::Subject;
use crate::authorization::model::acl::Acl;
use crate::authorization::provider::AuthorizationError;
use crate::config::AuthConfig;

/// Result type for authorization metadata operations.
pub type MetadataResult<T> = Result<T, AuthorizationError>;

/// Authorization metadata provider trait.
///
/// This trait defines the interface for managing Access Control List (ACL) metadata
/// in RocketMQ's authorization system. It provides CRUD operations for ACLs and
/// supports querying and filtering metadata.
///
/// # Design Principles
///
/// - **Async-first**: All I/O operations are async to support scalable implementations
/// - **Flexible storage**: Implementations can use any backend (files, databases, remote services)
/// - **Query support**: Built-in filtering capabilities for listing ACLs
/// - **Error handling**: Comprehensive error reporting through `AuthorizationError`
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support concurrent access from multiple
/// RocketMQ components (broker, namesrv, proxy).
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::metadata_provider::{AuthorizationMetadataProvider, MetadataResult};
/// use rocketmq_auth::authorization::model::acl::Acl;
/// use rocketmq_auth::authentication::model::subject::Subject;
/// use rocketmq_auth::config::AuthConfig;
///
/// struct MyMetadataProvider {
///     // Storage implementation
/// }
///
/// impl AuthorizationMetadataProvider for MyMetadataProvider {
///     fn initialize(
///         &mut self,
///         config: AuthConfig,
///         metadata_service: Option<Box<dyn Any + Send + Sync>>,
///     ) -> MetadataResult<()> {
///         // Initialize storage backend
///         Ok(())
///     }
///
///     async fn create_acl(&self, acl: Acl) -> MetadataResult<()> {
///         // Store ACL in backend
///         Ok(())
///     }
///
///     async fn get_acl<S: Subject>(&self, subject: &S) -> MetadataResult<Option<Acl>> {
///         // Retrieve ACL from backend
///         Ok(None)
///     }
///
///     // ... implement other methods
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait AuthorizationMetadataProvider: Send + Sync {
    /// Initialize the metadata provider with configuration.
    ///
    /// This method is called once during provider setup. Implementations should:
    /// - Initialize storage backend (open database, connect to remote service, etc.)
    /// - Load initial configuration and validate parameters
    /// - Set up caching layers if needed
    /// - Establish connection pools or prepare resources
    ///
    /// # Arguments
    ///
    /// * `config` - Authorization configuration containing provider-specific settings
    /// * `metadata_service` - Optional metadata service for advanced integration scenarios. Can be
    ///   downcast to implementation-specific types if needed.
    ///
    /// # Errors
    ///
    /// Returns `AuthorizationError::ConfigurationError` if:
    /// - Configuration parameters are invalid
    /// - Storage backend cannot be initialized
    /// - Required resources are unavailable
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut provider = MyMetadataProvider::new();
    /// provider.initialize(auth_config, None)?;
    /// ```
    fn initialize(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Box<dyn Any + Send + Sync>>,
    ) -> MetadataResult<()>;

    /// Shutdown the metadata provider and release resources.
    ///
    /// This method should:
    /// - Close database connections
    /// - Flush pending writes
    /// - Release file handles
    /// - Cleanup temporary resources
    ///
    /// Implementations should be idempotent (safe to call multiple times).
    fn shutdown(&mut self);

    /// Create a new ACL for a subject.
    ///
    /// If an ACL already exists for the subject, implementations should return an error.
    /// Use `update_acl` to modify existing ACLs.
    ///
    /// # Arguments
    ///
    /// * `acl` - The ACL to create, containing subject information and policies
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::InternalError` if ACL already exists
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    /// - `AuthorizationError::InvalidContext` if ACL data is malformed
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let acl = Acl::of("user:alice", SubjectType::User, policy);
    /// provider.create_acl(acl).await?;
    /// ```
    async fn create_acl(&self, acl: Acl) -> MetadataResult<()>;

    /// Delete an ACL for a subject.
    ///
    /// If the ACL does not exist, implementations should not return an error
    /// (idempotent operation).
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject whose ACL should be deleted
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let user = User::new("alice");
    /// provider.delete_acl(&user).await?;
    /// ```
    async fn delete_acl<S: Subject + Send + Sync>(&self, subject: &S) -> MetadataResult<()>;

    /// Update an existing ACL.
    ///
    /// This operation completely replaces the existing ACL. To perform partial updates,
    /// implementations should first retrieve the ACL, modify it, then call this method.
    ///
    /// # Arguments
    ///
    /// * `acl` - The updated ACL data
    ///
    /// # Errors
    ///
    /// - `AuthorizationError::SubjectNotFound` if ACL doesn't exist
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    /// - `AuthorizationError::InvalidContext` if ACL data is malformed
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut acl = provider.get_acl(&user).await?.unwrap();
    /// acl.add_policy(new_policy);
    /// provider.update_acl(acl).await?;
    /// ```
    async fn update_acl(&self, acl: Acl) -> MetadataResult<()>;

    /// Get the ACL for a specific subject.
    ///
    /// Returns `None` if no ACL exists for the subject.
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
    /// - `AuthorizationError::MetadataServiceError` if storage operation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let user = User::new("alice");
    /// if let Some(acl) = provider.get_acl(&user).await? {
    ///     println!("ACL found: {:?}", acl);
    /// }
    /// ```
    fn get_acl<S: Subject + Send + Sync>(
        &self,
        subject: &S,
    ) -> impl std::future::Future<Output = MetadataResult<Option<Acl>>> + Send;

    /// List ACLs with optional filtering.
    ///
    /// This method supports filtering by subject name pattern and resource pattern.
    /// Both filters are optional and use substring matching.
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
    /// let all_acls = provider.list_acl(None, None).await?;
    ///
    /// // Filter by subject
    /// let user_acls = provider.list_acl(Some("user:"), None).await?;
    ///
    /// // Filter by resource
    /// let topic_acls = provider.list_acl(None, Some("Topic:test")).await?;
    ///
    /// // Both filters
    /// let filtered = provider.list_acl(Some("user:alice"), Some("Topic:")).await?;
    /// ```
    async fn list_acl(&self, subject_filter: Option<&str>, resource_filter: Option<&str>) -> MetadataResult<Vec<Acl>>;
}

/// No-op authorization metadata provider for testing and disabled scenarios.
///
/// This provider accepts all operations but maintains no state. Useful for:
/// - Testing authorization logic without persistence
/// - Disabling authorization in development environments
/// - Placeholder implementations during development
#[derive(Debug, Default)]
pub struct NoopMetadataProvider;

impl NoopMetadataProvider {
    /// Create a new no-op metadata provider.
    pub fn new() -> Self {
        Self
    }
}

impl AuthorizationMetadataProvider for NoopMetadataProvider {
    fn initialize(
        &mut self,
        _config: AuthConfig,
        _metadata_service: Option<Box<dyn Any + Send + Sync>>,
    ) -> MetadataResult<()> {
        Ok(())
    }

    fn shutdown(&mut self) {
        // No resources to clean up
    }

    async fn create_acl(&self, _acl: Acl) -> MetadataResult<()> {
        Ok(())
    }

    async fn delete_acl<S: Subject + Send + Sync>(&self, _subject: &S) -> MetadataResult<()> {
        Ok(())
    }

    async fn update_acl(&self, _acl: Acl) -> MetadataResult<()> {
        Ok(())
    }

    async fn get_acl<S: Subject + Send + Sync>(&self, _subject: &S) -> MetadataResult<Option<Acl>> {
        Ok(None)
    }

    async fn list_acl(
        &self,
        _subject_filter: Option<&str>,
        _resource_filter: Option<&str>,
    ) -> MetadataResult<Vec<Acl>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::action::Action;

    use super::*;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authentication::model::user::User;
    use crate::authorization::enums::decision::Decision;
    use crate::authorization::enums::policy_type::PolicyType;
    use crate::authorization::model::environment::Environment;
    use crate::authorization::model::policy::Policy;
    use crate::authorization::model::policy_entry::PolicyEntry;
    use crate::authorization::model::resource::Resource;

    #[tokio::test]
    async fn test_noop_provider_initialization() {
        let mut provider = NoopMetadataProvider::new();
        let config = AuthConfig::default();

        let result = provider.initialize(config, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_provider_create_acl() {
        let provider = NoopMetadataProvider::new();

        let resource = Resource::of_topic("test-topic");
        let entry = PolicyEntry::of(
            resource,
            vec![Action::Pub],
            Environment::of("192.168.1.1"),
            Decision::Allow,
        );
        let policy = Policy::of_entries(PolicyType::Custom, vec![entry]);
        let acl = Acl::of("user:test", SubjectType::User, policy);

        let result = provider.create_acl(acl).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_provider_get_acl_returns_none() {
        let provider = NoopMetadataProvider::new();
        let user = User::of("test");

        let result = provider.get_acl(&user).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_noop_provider_list_acl_returns_empty() {
        let provider = NoopMetadataProvider::new();

        let result = provider.list_acl(None, None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_noop_provider_update_acl() {
        let provider = NoopMetadataProvider::new();

        let resource = Resource::of_topic("test-topic");
        let entry = PolicyEntry::of(
            resource,
            vec![Action::Pub],
            Environment::of("192.168.1.1"),
            Decision::Allow,
        );
        let policy = Policy::of_entries(PolicyType::Custom, vec![entry]);
        let acl = Acl::of("user:test", SubjectType::User, policy);

        let result = provider.update_acl(acl).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_provider_delete_acl() {
        let provider = NoopMetadataProvider::new();
        let user = User::of("test");

        let result = provider.delete_acl(&user).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_noop_provider_shutdown() {
        let mut provider = NoopMetadataProvider::new();
        provider.shutdown();
        // Should not panic or cause issues
    }

    #[tokio::test]
    async fn test_noop_provider_list_with_filters() {
        let provider = NoopMetadataProvider::new();

        let result = provider.list_acl(Some("user:test"), Some("Topic:test")).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
