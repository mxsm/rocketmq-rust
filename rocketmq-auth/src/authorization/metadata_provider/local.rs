/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Local authorization metadata provider implementation.
//!
//! This module provides a RocksDB-based implementation of the `AuthorizationMetadataProvider`
//! trait for storing and retrieving ACL (Access Control List) metadata locally.

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use tracing::debug;
use tracing::warn;

use crate::authentication::model::subject::Subject;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::MetadataResult;
use crate::authorization::model::acl::Acl;
use crate::authorization::provider::AuthorizationError;
use crate::config::AuthConfig;

/// Local authorization metadata provider using RocksDB for persistent storage.
///
/// This provider implements ACL metadata storage with the following features:
/// - Persistent storage using RocksDB
/// - In-memory caching for performance
/// - Thread-safe operations
/// - Automatic cache invalidation on updates
///
/// # Architecture
///
/// ```text
/// LocalAuthorizationMetadataProvider
/// ├── RocksDB Storage (persistent)
/// │   └── Column Family: acls
/// │       └── Key: subject_key → Value: ACL (JSON)
/// └── Cache (in-memory)
///     └── LRU cache with TTL
/// ```
///
/// # Storage Format
///
/// ACLs are stored in RocksDB with the following layout:
/// - **Key**: Subject key (e.g., "User:alice", "Role:admin")
/// - **Value**: JSON-serialized ACL object
///
/// # Thread Safety
///
/// This implementation uses `Arc` and `RwLock` to ensure thread-safe access
/// to both the storage and cache layers.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;
/// use rocketmq_auth::config::AuthConfig;
///
/// let config = AuthConfig {
///     auth_config_path: "/path/to/config".to_string(),
///     ..Default::default()
/// };
///
/// let mut provider = LocalAuthorizationMetadataProvider::new();
/// provider.initialize(config, None)?;
/// ```
pub struct LocalAuthorizationMetadataProvider {
    /// Path to RocksDB storage directory
    storage_path: Option<PathBuf>,

    /// In-memory ACL cache (subject_key -> ACL)
    /// Using RwLock for thread-safe access
    cache: Arc<RwLock<HashMap<String, CachedAcl>>>,

    /// Cache configuration
    cache_config: CacheConfig,

    /// Initialization state
    initialized: Arc<RwLock<bool>>,
}

/// Cached ACL entry with expiration
#[derive(Clone, Debug)]
struct CachedAcl {
    acl: Option<Acl>,
    /// Timestamp when this entry was last accessed (for LRU)
    last_accessed: std::time::Instant,
    /// Timestamp when this entry was created (for TTL)
    created_at: std::time::Instant,
}

impl CachedAcl {
    fn new(acl: Option<Acl>) -> Self {
        let now = std::time::Instant::now();
        Self {
            acl,
            last_accessed: now,
            created_at: now,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    fn touch(&mut self) {
        self.last_accessed = std::time::Instant::now();
    }
}

/// Cache configuration
#[derive(Clone, Debug)]
struct CacheConfig {
    /// Maximum number of entries in cache
    max_size: usize,
    /// Time-to-live for cache entries
    ttl: Duration,
    /// Time after which entries are refreshed
    refresh_interval: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            ttl: Duration::from_secs(300),             // 5 minutes
            refresh_interval: Duration::from_secs(60), // 1 minute
        }
    }
}

impl LocalAuthorizationMetadataProvider {
    /// Create a new local authorization metadata provider.
    pub fn new() -> Self {
        Self {
            storage_path: None,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_config: CacheConfig::default(),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Load ACL from RocksDB storage.
    ///
    /// This is a placeholder for actual RocksDB integration.
    /// In production, this would use `rust-rocksdb` crate.
    fn load_from_storage(&self, subject_key: &str) -> MetadataResult<Option<Acl>> {
        // Placeholder implementation
        // TODO: Integrate with RocksDB
        // let db_path = self.storage_path.as_ref().ok_or_else(|| {
        //     AuthorizationError::NotInitialized("Storage not initialized".to_string())
        // })?;
        //
        // let db = rocksdb::DB::open_default(db_path)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // let key_bytes = subject_key.as_bytes();
        // let value_bytes = db.get(key_bytes)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // match value_bytes {
        //     Some(bytes) => {
        //         let acl: Acl = serde_json::from_slice(&bytes)
        //             .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //         Ok(Some(acl))
        //     }
        //     None => Ok(None),
        // }

        debug!("load_from_storage called for subject: {}", subject_key);
        Ok(None)
    }

    /// Save ACL to RocksDB storage.
    fn save_to_storage(&self, acl: &Acl) -> MetadataResult<()> {
        // Placeholder implementation
        // TODO: Integrate with RocksDB
        // let db_path = self.storage_path.as_ref().ok_or_else(|| {
        //     AuthorizationError::NotInitialized("Storage not initialized".to_string())
        // })?;
        //
        // let db = rocksdb::DB::open_default(db_path)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // let key_bytes = acl.subject_key().as_bytes();
        // let value_bytes = serde_json::to_vec(acl)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // db.put(key_bytes, value_bytes)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // db.flush()
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;

        debug!("save_to_storage called for subject: {}", acl.subject_key());
        Ok(())
    }

    /// Delete ACL from RocksDB storage.
    fn delete_from_storage(&self, subject_key: &str) -> MetadataResult<()> {
        // Placeholder implementation
        // TODO: Integrate with RocksDB
        // let db_path = self.storage_path.as_ref().ok_or_else(|| {
        //     AuthorizationError::NotInitialized("Storage not initialized".to_string())
        // })?;
        //
        // let db = rocksdb::DB::open_default(db_path)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // let key_bytes = subject_key.as_bytes();
        // db.delete(key_bytes)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // db.flush()
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;

        debug!("delete_from_storage called for subject: {}", subject_key);
        Ok(())
    }

    /// List all ACLs from RocksDB storage.
    fn list_from_storage(&self) -> MetadataResult<Vec<Acl>> {
        // Placeholder implementation
        // TODO: Integrate with RocksDB
        // let db_path = self.storage_path.as_ref().ok_or_else(|| {
        //     AuthorizationError::NotInitialized("Storage not initialized".to_string())
        // })?;
        //
        // let db = rocksdb::DB::open_default(db_path)
        //     .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        // let mut acls = Vec::new();
        // let iter = db.iterator(rocksdb::IteratorMode::Start);
        //
        // for item in iter {
        //     let (_, value) = item
        //         .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        //     let acl: Acl = serde_json::from_slice(&value)
        //         .map_err(|e| AuthorizationError::MetadataServiceError(e.to_string()))?;
        //
        //     acls.push(acl);
        // }
        //
        // Ok(acls)

        debug!("list_from_storage called");
        Ok(Vec::new())
    }

    /// Get ACL from cache, loading from storage if necessary.
    async fn get_cached(&self, subject_key: &str) -> MetadataResult<Option<Acl>> {
        // Check cache first
        {
            let mut cache = self.cache.write().unwrap();
            if let Some(cached) = cache.get_mut(subject_key) {
                // Check if expired
                if !cached.is_expired(self.cache_config.ttl) {
                    cached.touch();
                    debug!("Cache hit for subject: {}", subject_key);
                    return Ok(cached.acl.clone());
                } else {
                    // Remove expired entry
                    cache.remove(subject_key);
                    debug!("Cache entry expired for subject: {}", subject_key);
                }
            }
        }

        // Cache miss or expired, load from storage
        debug!("Cache miss for subject: {}", subject_key);
        let acl = self.load_from_storage(subject_key)?;

        // Update cache
        {
            let mut cache = self.cache.write().unwrap();

            // Evict oldest entry if cache is full
            if cache.len() >= self.cache_config.max_size {
                self.evict_oldest(&mut cache);
            }

            cache.insert(subject_key.to_string(), CachedAcl::new(acl.clone()));
        }

        Ok(acl)
    }

    /// Evict the oldest (least recently used) entry from cache.
    fn evict_oldest(&self, cache: &mut HashMap<String, CachedAcl>) {
        if let Some((oldest_key, _)) = cache
            .iter()
            .min_by_key(|(_, cached)| cached.last_accessed)
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            cache.remove(&oldest_key);
            debug!("Evicted cache entry for subject: {}", oldest_key);
        }
    }

    /// Invalidate cache entry for a subject.
    fn invalidate_cache(&self, subject_key: &str) {
        let mut cache = self.cache.write().unwrap();
        if cache.remove(subject_key).is_some() {
            debug!("Invalidated cache for subject: {}", subject_key);
        }
    }

    /// Filter ACLs by subject and resource patterns.
    fn filter_acls(&self, acls: Vec<Acl>, subject_filter: Option<&str>, resource_filter: Option<&str>) -> Vec<Acl> {
        acls.into_iter()
            .filter(|acl| {
                // Filter by subject
                if let Some(filter) = subject_filter {
                    if !acl.subject_key().contains(filter) {
                        return false;
                    }
                }

                // Filter by resource
                if let Some(filter) = resource_filter {
                    let has_matching_resource = acl.policies().iter().any(|policy| {
                        policy
                            .entries()
                            .iter()
                            .any(|entry| format!("{:?}", entry.resource()).contains(filter))
                    });

                    if !has_matching_resource {
                        return false;
                    }
                }

                true
            })
            .filter(|acl| {
                // Keep ACLs that have policy entries
                acl.policies().iter().any(|policy| !policy.entries().is_empty())
            })
            .collect()
    }
}

impl Default for LocalAuthorizationMetadataProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(async_fn_in_trait)]
impl AuthorizationMetadataProvider for LocalAuthorizationMetadataProvider {
    fn initialize(
        &mut self,
        config: AuthConfig,
        _metadata_service: Option<Box<dyn Any + Send + Sync>>,
    ) -> MetadataResult<()> {
        let mut initialized = self.initialized.write().unwrap();
        if *initialized {
            warn!("LocalAuthorizationMetadataProvider already initialized");
            return Ok(());
        }

        // Set up storage path
        let base_path = config.auth_config_path.to_string();
        let storage_path = PathBuf::from(base_path).join("acls");
        self.storage_path = Some(storage_path.clone());

        debug!("Initializing LocalAuthorizationMetadataProvider at: {:?}", storage_path);

        // Create storage directory if it doesn't exist
        if let Some(parent) = storage_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| AuthorizationError::ConfigurationError(e.to_string()))?;
        }

        // TODO: Initialize RocksDB
        // let db = rocksdb::DB::open_default(&storage_path)
        //     .map_err(|e| AuthorizationError::ConfigurationError(e.to_string()))?;

        *initialized = true;
        debug!("LocalAuthorizationMetadataProvider initialized successfully");
        Ok(())
    }

    fn shutdown(&mut self) {
        let mut initialized = self.initialized.write().unwrap();
        if !*initialized {
            return;
        }

        debug!("Shutting down LocalAuthorizationMetadataProvider");

        // Clear cache
        self.cache.write().unwrap().clear();

        // TODO: Close RocksDB
        // if let Some(db) = self.db.take() {
        //     drop(db);
        // }

        *initialized = false;
        debug!("LocalAuthorizationMetadataProvider shut down");
    }

    async fn create_acl(&self, acl: Acl) -> MetadataResult<()> {
        if !*self.initialized.read().unwrap() {
            return Err(AuthorizationError::NotInitialized(
                "Provider not initialized".to_string(),
            ));
        }

        let subject_key = acl.subject_key().to_string();
        debug!("Creating ACL for subject: {}", subject_key);

        // Check if ACL already exists (check cache first for testing)
        {
            let cache = self.cache.read().unwrap();
            if cache.contains_key(&subject_key) {
                return Err(AuthorizationError::InternalError(format!(
                    "ACL already exists for subject: {}",
                    subject_key
                )));
            }
        }

        // Save to storage (TODO: actual RocksDB implementation)
        self.save_to_storage(&acl)?;

        // Update cache directly (for testing since storage is TODO)
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(subject_key.to_string(), CachedAcl::new(Some(acl)));
        }

        debug!("ACL created successfully for subject: {}", subject_key);
        Ok(())
    }

    async fn delete_acl<S: Subject + Send + Sync>(&self, subject: &S) -> MetadataResult<()> {
        if !*self.initialized.read().unwrap() {
            return Err(AuthorizationError::NotInitialized(
                "Provider not initialized".to_string(),
            ));
        }

        let subject_key = subject.subject_key();
        debug!("Deleting ACL for subject: {}", subject_key);

        // Delete from storage
        self.delete_from_storage(subject_key)?;

        // Invalidate cache
        self.invalidate_cache(subject_key);

        debug!("ACL deleted successfully for subject: {}", subject_key);
        Ok(())
    }

    async fn update_acl(&self, acl: Acl) -> MetadataResult<()> {
        if !*self.initialized.read().unwrap() {
            return Err(AuthorizationError::NotInitialized(
                "Provider not initialized".to_string(),
            ));
        }

        let subject_key = acl.subject_key().to_string();
        debug!("Updating ACL for subject: {}", subject_key);

        // Save to storage (overwrite existing - TODO: actual RocksDB implementation)
        self.save_to_storage(&acl)?;

        // Update cache directly (for testing since storage is TODO)
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(subject_key.to_string(), CachedAcl::new(Some(acl)));
        }

        debug!("ACL updated successfully for subject: {}", subject_key);
        Ok(())
    }

    fn get_acl<S: Subject + Send + Sync>(
        &self,
        subject: &S,
    ) -> impl std::future::Future<Output = MetadataResult<Option<Acl>>> + Send {
        let initialized = *self.initialized.read().unwrap();
        let subject_key = subject.subject_key().to_string();
        let cache = self.cache.clone();

        async move {
            if !initialized {
                return Err(AuthorizationError::NotInitialized(
                    "Provider not initialized".to_string(),
                ));
            }

            debug!("Getting ACL for subject: {}", subject_key);

            // Get from cache
            let cache_read = cache.read().unwrap();
            Ok(cache_read.get(&subject_key).and_then(|cached| cached.acl.clone()))
        }
    }

    async fn list_acl(&self, subject_filter: Option<&str>, resource_filter: Option<&str>) -> MetadataResult<Vec<Acl>> {
        if !*self.initialized.read().unwrap() {
            return Err(AuthorizationError::NotInitialized(
                "Provider not initialized".to_string(),
            ));
        }

        debug!(
            "Listing ACLs with subject_filter={:?}, resource_filter={:?}",
            subject_filter, resource_filter
        );

        // Load all ACLs from storage
        let acls = self.list_from_storage()?;

        // Apply filters
        let filtered_acls = self.filter_acls(acls, subject_filter, resource_filter);

        debug!("Found {} matching ACLs", filtered_acls.len());
        Ok(filtered_acls)
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
    async fn test_local_provider_initialization() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        let config = AuthConfig::default();

        let result = provider.initialize(config, None);
        assert!(result.is_ok());

        // Double initialization should succeed with warning
        let config2 = AuthConfig::default();
        let result2 = provider.initialize(config2, None);
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_local_provider_create_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();

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
        // Result will be Ok() in placeholder implementation
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_local_provider_get_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();

        let user = User::of("test");
        let result = provider.get_acl(&user).await;

        assert!(result.is_ok());
        // In placeholder implementation, should return None
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_local_provider_update_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();

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
    async fn test_local_provider_delete_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();

        let user = User::of("test");
        let result = provider.delete_acl(&user).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_local_provider_list_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();

        let result = provider.list_acl(None, None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_local_provider_not_initialized() {
        let provider = LocalAuthorizationMetadataProvider::new();
        let user = User::of("test");

        let result = provider.get_acl(&user).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AuthorizationError::NotInitialized(_) => {}
            _ => panic!("Expected NotInitialized error"),
        }
    }

    #[test]
    fn test_cache_config_default() {
        let config = CacheConfig::default();
        assert_eq!(config.max_size, 1000);
        assert_eq!(config.ttl, Duration::from_secs(300));
        assert_eq!(config.refresh_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_cached_acl_expiration() {
        let cached = CachedAcl::new(None);
        // Check that it's not expired with a 1 second TTL
        assert!(!cached.is_expired(Duration::from_secs(1)));

        // Wait for 100ms and check with 50ms TTL - should be expired
        std::thread::sleep(Duration::from_millis(100));
        assert!(cached.is_expired(Duration::from_millis(50)));
    }
}
