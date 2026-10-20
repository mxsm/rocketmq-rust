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

//! Authorization metadata access and the built-in local ACL provider.
//!
//! [`LocalAuthorizationMetadataProvider`] keeps ACLs in memory, with an optional `acls.json`
//! snapshot selected by [`AuthConfig::auth_config_path`]. YAML ACL files are imported by the
//! auth runtime; they are not the local provider's snapshot format. [`NoopMetadataProvider`]
//! accepts mutations without retaining metadata and always returns empty query results.
//!
//! Custom runtime providers are supplied through [`crate::ProviderBundle`], using
//! [`crate::AclMetadataPort`] and [`crate::UserMetadataPort`] for concurrent operations and
//! [`crate::ProviderControl`] for initialization, flushing, and closure. ACL file imports and
//! v1 migration additionally require [`crate::AclSnapshotImport`].

// Sub-modules
pub mod local;

use std::any::Any;

// Re-export commonly used types
pub use local::LocalAuthorizationMetadataProvider;

use crate::authentication::model::subject::Subject;
use crate::authorization::model::acl::Acl;
use crate::config::AuthConfig;
use crate::AuthServiceResult;

/// ACL metadata operations used by the local provider and compatibility adapters.
///
/// ACL mutations and queries are asynchronous. [`initialize`](Self::initialize) and
/// [`shutdown`](Self::shutdown) are synchronous lifecycle hooks; local initialization can block
/// while reading a configured JSON snapshot. [`crate::AuthRuntimeBuilder`] loads the built-in
/// providers on the service's bounded metadata I/O lane.
///
/// Runtime extension uses [`crate::ProviderBundle`] to separate metadata operations from
/// lifecycle control. Implementing this trait alone does not supply a runtime provider bundle.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support concurrent access from multiple
/// RocketMQ components (broker, namesrv, proxy).
///
/// # Examples
///
/// ```
/// use rocketmq_auth::{
///     AuthConfig, AuthorizationMetadataProvider, LocalAuthorizationMetadataProvider, User,
/// };
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> rocketmq_auth::AuthServiceResult<()> {
/// let mut provider = LocalAuthorizationMetadataProvider::new();
/// provider.initialize(AuthConfig::default(), None)?;
///
/// let user = User::of("alice");
/// assert!(provider.get_acl(&user).await?.is_none());
/// provider.shutdown();
/// # Ok(())
/// # }
/// ```
#[allow(async_fn_in_trait)]
pub trait AuthorizationMetadataProvider: Send + Sync {
    /// Synchronously initialize the metadata provider with configuration.
    ///
    /// The local implementation applies the ACL cache settings and loads `acls.json` when
    /// `auth_config_path` is set. An empty path selects in-memory storage. Snapshot loading
    /// blocks the calling thread; service integrations use [`crate::AuthRuntimeBuilder`] to
    /// perform that work on the bounded metadata I/O lane.
    ///
    /// # Arguments
    ///
    /// * `config` - Authorization configuration containing provider-specific settings
    /// * `metadata_service` - Implementation-specific integration value. The local provider ignores it.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific [`crate::AuthServiceError`] if initialization fails.
    /// The local provider reports snapshot read or decode failures and synchronization errors.
    fn initialize(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Box<dyn Any + Send + Sync>>,
    ) -> AuthServiceResult<()>;

    /// Synchronously release the provider's resources.
    ///
    /// The local implementation clears its in-memory ACLs and cache and marks itself uninitialized.
    /// Repeated calls are safe. Runtime-managed flushing and closure use [`crate::ProviderControl`].
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
    /// Returns a provider-specific [`crate::AuthServiceError`]. The local provider reports an
    /// uninitialized provider, a duplicate subject, synchronization errors, or snapshot persistence
    /// failures.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let acl = Acl::of("user:alice", SubjectType::User, policy);
    /// provider.create_acl(acl).await?;
    /// ```
    async fn create_acl(&self, acl: Acl) -> AuthServiceResult<()>;

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
    /// Returns a provider-specific [`crate::AuthServiceError`]. The local provider reports an
    /// uninitialized provider, synchronization errors, or snapshot persistence failures.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let user = User::of("alice");
    /// provider.delete_acl(&user).await?;
    /// ```
    async fn delete_acl<S: Subject + Send + Sync>(&self, subject: &S) -> AuthServiceResult<()>;

    /// Write an ACL for a subject.
    ///
    /// The local implementation inserts a missing ACL or completely replaces an existing one.
    /// To perform partial updates, retrieve the ACL, modify it, then call this method.
    ///
    /// # Arguments
    ///
    /// * `acl` - The updated ACL data
    ///
    /// # Errors
    ///
    /// Returns a provider-specific [`crate::AuthServiceError`]. The local provider reports an
    /// uninitialized provider, synchronization errors, or snapshot persistence failures.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut acl = provider.get_acl(&user).await?.unwrap();
    /// acl.add_policy(new_policy);
    /// provider.update_acl(acl).await?;
    /// ```
    async fn update_acl(&self, acl: Acl) -> AuthServiceResult<()>;

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
    /// Propagates metadata lookup failures. The local provider reports initialization or
    /// synchronization errors.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let user = User::of("alice");
    /// if let Some(acl) = provider.get_acl(&user).await? {
    ///     println!("ACL found: {:?}", acl);
    /// }
    /// ```
    fn get_acl<S: Subject + Send + Sync>(
        &self,
        subject: &S,
    ) -> impl std::future::Future<Output = AuthServiceResult<Option<Acl>>> + Send;

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
    /// Propagates metadata listing failures. The local provider requires initialization and a
    /// readable canonical ACL map.
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
    async fn list_acl(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> AuthServiceResult<Vec<Acl>>;
}

/// Empty-state authorization metadata provider for tests.
///
/// Mutations succeed without retaining ACLs, lookups return `None`, and listings return an empty
/// vector. This provider does not disable authorization or grant access to protected resources.
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
    ) -> AuthServiceResult<()> {
        Ok(())
    }

    fn shutdown(&mut self) {
        // No resources to clean up
    }

    async fn create_acl(&self, _acl: Acl) -> AuthServiceResult<()> {
        Ok(())
    }

    async fn delete_acl<S: Subject + Send + Sync>(&self, _subject: &S) -> AuthServiceResult<()> {
        Ok(())
    }

    async fn update_acl(&self, _acl: Acl) -> AuthServiceResult<()> {
        Ok(())
    }

    async fn get_acl<S: Subject + Send + Sync>(&self, _subject: &S) -> AuthServiceResult<Option<Acl>> {
        Ok(None)
    }

    async fn list_acl(
        &self,
        _subject_filter: Option<&str>,
        _resource_filter: Option<&str>,
    ) -> AuthServiceResult<Vec<Acl>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_security_api::Action;

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
