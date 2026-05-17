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

//! Local authorization metadata provider implementation.
//!
//! This module provides a local implementation of the `AuthorizationMetadataProvider`
//! trait for storing and retrieving ACL (Access Control List) metadata.

use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::time::Duration;

use rocketmq_common::common::action::Action;
use serde::Deserialize;
use serde::Serialize;
use tracing::debug;
use tracing::warn;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::model::subject::Subject;
use crate::authorization::enums::decision::Decision;
use crate::authorization::enums::policy_type::PolicyType;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::MetadataResult;
use crate::authorization::model::acl::Acl;
use crate::authorization::model::environment::Environment;
use crate::authorization::model::policy::Policy;
use crate::authorization::model::policy_entry::PolicyEntry;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::config::AuthConfig;

/// Local authorization metadata provider backed by an in-memory snapshot and an optional JSON
/// snapshot file.
///
/// This provider implements ACL metadata storage with the following features:
/// - Persistent local snapshot storage when `auth_config_path` is configured
/// - In-memory caching for performance
/// - Thread-safe operations
/// - Automatic cache invalidation on updates
///
/// # Architecture
///
/// ```text
/// LocalAuthorizationMetadataProvider
/// |- JSON snapshot file (persistent, optional)
/// |  `- acls.json
/// `- Cache (in-memory)
///    `- subject_key -> ACL
/// ```
///
/// # Storage Format
///
/// ACLs are stored in a JSON snapshot with the following layout:
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
    /// Path to the JSON snapshot file.
    storage_path: Option<PathBuf>,
    storage: Arc<RwLock<HashMap<String, Acl>>>,

    /// In-memory ACL cache (subject_key -> ACL)
    /// Using RwLock for thread-safe access
    cache: Arc<RwLock<HashMap<String, CachedAcl>>>,

    /// Cache configuration
    cache_config: CacheConfig,

    /// Initialization state
    initialized: Arc<RwLock<bool>>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
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

impl CacheConfig {
    fn from_auth_config(config: &AuthConfig) -> Self {
        Self {
            max_size: config.acl_cache_max_num as usize,
            ttl: Duration::from_secs(config.acl_cache_expired_second as u64),
            refresh_interval: Duration::from_secs(config.acl_cache_refresh_second as u64),
        }
    }
}

impl LocalAuthorizationMetadataProvider {
    /// Create a new local authorization metadata provider.
    pub fn new() -> Self {
        Self {
            storage_path: None,
            storage: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_config: CacheConfig::default(),
            initialized: Arc::new(RwLock::new(false)),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn load_from_storage(&self, subject_key: &str) -> MetadataResult<Option<Acl>> {
        let storage = self.storage_read()?;
        Ok(storage.get(subject_key).cloned())
    }

    async fn persist_storage_snapshot(&self, snapshot: &HashMap<String, Acl>) -> MetadataResult<()> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let path = path.clone();
        let snapshot = snapshot.clone();
        tokio::task::spawn_blocking(move || write_acl_snapshot(&path, &snapshot))
            .await
            .map_err(|error| AuthorizationError::MetadataServiceError(format!("ACL snapshot task failed: {error}")))?
    }

    fn replace_storage(&self, snapshot: HashMap<String, Acl>) -> MetadataResult<()> {
        let mut storage = self.storage_write()?;
        *storage = snapshot;
        Ok(())
    }

    fn list_from_storage(&self) -> MetadataResult<Vec<Acl>> {
        let storage = self.storage_read()?;
        Ok(storage.values().cloned().collect())
    }

    /// Get ACL from cache, loading from storage if necessary.
    async fn get_cached(&self, subject_key: &str) -> MetadataResult<Option<Acl>> {
        if self.cache_config.max_size == 0 {
            return self.load_from_storage(subject_key);
        }

        // Check cache first
        {
            let mut cache = self.cache_write()?;
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
            let mut cache = self.cache_write()?;

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
    fn invalidate_cache(&self, subject_key: &str) -> MetadataResult<()> {
        let mut cache = self.cache_write()?;
        if cache.remove(subject_key).is_some() {
            debug!("Invalidated cache for subject: {}", subject_key);
        }
        Ok(())
    }

    fn store_cache_entry(&self, subject_key: &str, acl: Option<Acl>) -> MetadataResult<()> {
        if self.cache_config.max_size == 0 {
            return Ok(());
        }

        let mut cache = self.cache_write()?;
        if !cache.contains_key(subject_key) && cache.len() >= self.cache_config.max_size {
            self.evict_oldest(&mut cache);
        }
        cache.insert(subject_key.to_string(), CachedAcl::new(acl));
        Ok(())
    }

    fn ensure_initialized(&self) -> MetadataResult<()> {
        if *self.initialized_read()? {
            Ok(())
        } else {
            Err(AuthorizationError::NotInitialized(
                "Provider not initialized".to_string(),
            ))
        }
    }

    fn initialized_read(&self) -> MetadataResult<RwLockReadGuard<'_, bool>> {
        self.initialized
            .read()
            .map_err(|_| AuthorizationError::MetadataServiceError("ACL initialized lock is poisoned".to_string()))
    }

    fn initialized_write(&self) -> MetadataResult<RwLockWriteGuard<'_, bool>> {
        self.initialized
            .write()
            .map_err(|_| AuthorizationError::MetadataServiceError("ACL initialized lock is poisoned".to_string()))
    }

    fn storage_read(&self) -> MetadataResult<RwLockReadGuard<'_, HashMap<String, Acl>>> {
        self.storage
            .read()
            .map_err(|_| AuthorizationError::MetadataServiceError("ACL storage lock is poisoned".to_string()))
    }

    fn storage_write(&self) -> MetadataResult<RwLockWriteGuard<'_, HashMap<String, Acl>>> {
        self.storage
            .write()
            .map_err(|_| AuthorizationError::MetadataServiceError("ACL storage lock is poisoned".to_string()))
    }

    fn cache_write(&self) -> MetadataResult<RwLockWriteGuard<'_, HashMap<String, CachedAcl>>> {
        self.cache
            .write()
            .map_err(|_| AuthorizationError::MetadataServiceError("ACL cache lock is poisoned".to_string()))
    }

    fn cache_read(&self) -> MetadataResult<RwLockReadGuard<'_, HashMap<String, CachedAcl>>> {
        self.cache
            .read()
            .map_err(|_| AuthorizationError::MetadataServiceError("ACL cache lock is poisoned".to_string()))
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
                        policy.entries().iter().any(|entry| {
                            entry
                                .resource()
                                .resource_key()
                                .is_some_and(|resource| resource.contains(filter))
                        })
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

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredAclSnapshot {
    acls: Vec<StoredAclRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredAclRecord {
    subject: String,
    policies: Vec<StoredPolicyRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredPolicyRecord {
    policy_type: String,
    entries: Vec<StoredPolicyEntryRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredPolicyEntryRecord {
    resource: String,
    actions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    source_ips: Vec<String>,
    decision: String,
}

fn auth_metadata_snapshot_path(config: &AuthConfig, file_name: &str) -> Option<PathBuf> {
    let raw_path = config.auth_config_path.as_str().trim();
    if raw_path.is_empty() {
        return None;
    }
    let path = PathBuf::from(raw_path);
    let root = if path.extension().is_some() {
        path.with_extension("")
    } else {
        path
    };
    Some(root.join(file_name))
}

fn read_acl_snapshot(path: &Path) -> MetadataResult<HashMap<String, Acl>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(storage_error(path, error)),
    };
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(HashMap::new());
    }
    let snapshot: StoredAclSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| AuthorizationError::MetadataServiceError(format!("{}: {error}", path.display())))?;
    let mut acls = HashMap::new();
    for record in snapshot.acls {
        let acl = acl_from_record(record)?;
        acls.insert(acl.subject_key().to_string(), acl);
    }
    Ok(acls)
}

fn write_acl_snapshot(path: &Path, acls: &HashMap<String, Acl>) -> MetadataResult<()> {
    let mut records = acls.values().map(acl_to_record).collect::<Vec<_>>();
    records.sort_by(|left, right| left.subject.cmp(&right.subject));
    let snapshot = StoredAclSnapshot { acls: records };
    let content = serde_json::to_vec_pretty(&snapshot)
        .map_err(|error| AuthorizationError::MetadataServiceError(error.to_string()))?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| storage_error(parent, error))?;
    }

    let temp_file = temp_snapshot_path(path);
    std::fs::write(&temp_file, content).map_err(|error| storage_error(&temp_file, error))?;
    match std::fs::rename(&temp_file, path) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            std::fs::copy(&temp_file, path).map_err(|error| {
                AuthorizationError::MetadataServiceError(format!(
                    "{}: {error}; rename failed first: {rename_error}",
                    path.display()
                ))
            })?;
            let _ = std::fs::remove_file(&temp_file);
            Ok(())
        }
    }
}

fn acl_to_record(acl: &Acl) -> StoredAclRecord {
    StoredAclRecord {
        subject: acl.subject_key().to_string(),
        policies: acl
            .policies()
            .iter()
            .map(|policy| StoredPolicyRecord {
                policy_type: policy.policy_type().name().to_string(),
                entries: policy.entries().iter().filter_map(policy_entry_to_record).collect(),
            })
            .collect(),
    }
}

fn policy_entry_to_record(entry: &PolicyEntry) -> Option<StoredPolicyEntryRecord> {
    Some(StoredPolicyEntryRecord {
        resource: entry.resource().resource_key()?,
        actions: entry.actions().iter().map(|action| action.name().to_string()).collect(),
        source_ips: entry
            .environment()
            .map(|environment| environment.source_ips().clone())
            .unwrap_or_default(),
        decision: entry.decision().name().to_string(),
    })
}

fn acl_from_record(record: StoredAclRecord) -> MetadataResult<Acl> {
    let subject_type = subject_type_from_key(&record.subject)?;
    let policies = record
        .policies
        .into_iter()
        .map(policy_from_record)
        .collect::<MetadataResult<Vec<_>>>()?;
    Ok(Acl::of_with_policies(record.subject, subject_type, policies))
}

fn policy_from_record(record: StoredPolicyRecord) -> MetadataResult<Policy> {
    let policy_type = PolicyType::get_by_name(&record.policy_type).ok_or_else(|| {
        AuthorizationError::MetadataServiceError(format!("Invalid policy type '{}'", record.policy_type))
    })?;
    let entries = record
        .entries
        .into_iter()
        .map(policy_entry_from_record)
        .collect::<MetadataResult<Vec<_>>>()?;
    Ok(Policy::of_entries(policy_type, entries))
}

fn policy_entry_from_record(record: StoredPolicyEntryRecord) -> MetadataResult<PolicyEntry> {
    let resource = Resource::of_str(&record.resource)
        .ok_or_else(|| AuthorizationError::MetadataServiceError(format!("Invalid resource '{}'", record.resource)))?;
    let actions = record
        .actions
        .iter()
        .map(|action| {
            Action::get_by_name(action)
                .ok_or_else(|| AuthorizationError::MetadataServiceError(format!("Invalid action '{action}'")))
        })
        .collect::<MetadataResult<Vec<_>>>()?;
    let decision = Decision::get_by_name(&record.decision)
        .ok_or_else(|| AuthorizationError::MetadataServiceError(format!("Invalid decision '{}'", record.decision)))?;
    let environment = Environment::of_list(record.source_ips);
    Ok(PolicyEntry::of(resource, actions, environment, decision))
}

fn subject_type_from_key(subject_key: &str) -> MetadataResult<SubjectType> {
    let Some((subject_type, _)) = subject_key.split_once(':') else {
        return Ok(SubjectType::User);
    };
    SubjectType::get_by_name(subject_type)
        .ok_or_else(|| AuthorizationError::MetadataServiceError(format!("Invalid subject type '{subject_type}'")))
}

fn storage_error(path: &Path, error: std::io::Error) -> AuthorizationError {
    AuthorizationError::MetadataServiceError(format!("{}: {error}", path.display()))
}

fn temp_snapshot_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(|value| value.to_str()).unwrap_or("acls.json");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    path.with_file_name(format!(".{file_name}.{nanos}.tmp"))
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
        {
            let initialized = self.initialized_read()?;
            if *initialized {
                warn!("LocalAuthorizationMetadataProvider already initialized");
                return Ok(());
            }
        }

        self.cache_config = CacheConfig::from_auth_config(&config);
        let storage_path = auth_metadata_snapshot_path(&config, "acls.json");
        self.storage_path = storage_path.clone();

        debug!("Initializing LocalAuthorizationMetadataProvider at: {:?}", storage_path);

        if let Some(path) = storage_path {
            let snapshot = read_acl_snapshot(&path)?;
            self.replace_storage(snapshot)?;
        }

        let mut initialized = self.initialized_write()?;
        *initialized = true;
        debug!("LocalAuthorizationMetadataProvider initialized successfully");
        Ok(())
    }

    fn shutdown(&mut self) {
        let Ok(mut initialized) = self.initialized.write() else {
            warn!("LocalAuthorizationMetadataProvider initialized lock is poisoned during shutdown");
            return;
        };
        if !*initialized {
            return;
        }

        debug!("Shutting down LocalAuthorizationMetadataProvider");

        match self.cache.write() {
            Ok(mut cache) => cache.clear(),
            Err(_) => warn!("LocalAuthorizationMetadataProvider cache lock is poisoned during shutdown"),
        }
        match self.storage.write() {
            Ok(mut storage) => storage.clear(),
            Err(_) => warn!("LocalAuthorizationMetadataProvider storage lock is poisoned during shutdown"),
        }

        *initialized = false;
        debug!("LocalAuthorizationMetadataProvider shut down");
    }

    async fn create_acl(&self, acl: Acl) -> MetadataResult<()> {
        self.ensure_initialized()?;

        let subject_key = acl.subject_key().to_string();
        debug!("Creating ACL for subject: {}", subject_key);

        let _write_guard = self.write_lock.lock().await;
        let mut snapshot = {
            let storage = self.storage_read()?;
            storage.clone()
        };
        if snapshot.contains_key(&subject_key) {
            return Err(AuthorizationError::InternalError(format!(
                "ACL already exists for subject: {}",
                subject_key
            )));
        }
        snapshot.insert(subject_key.clone(), acl.clone());

        self.persist_storage_snapshot(&snapshot).await?;
        self.replace_storage(snapshot)?;
        self.store_cache_entry(&subject_key, Some(acl))?;

        debug!("ACL created successfully for subject: {}", subject_key);
        Ok(())
    }

    async fn delete_acl<S: Subject + Send + Sync>(&self, subject: &S) -> MetadataResult<()> {
        self.ensure_initialized()?;

        let subject_key = subject.subject_key();
        debug!("Deleting ACL for subject: {}", subject_key);

        let _write_guard = self.write_lock.lock().await;
        let mut snapshot = {
            let storage = self.storage_read()?;
            storage.clone()
        };
        snapshot.remove(subject_key);
        self.persist_storage_snapshot(&snapshot).await?;
        self.replace_storage(snapshot)?;
        self.invalidate_cache(subject_key)?;

        debug!("ACL deleted successfully for subject: {}", subject_key);
        Ok(())
    }

    async fn update_acl(&self, acl: Acl) -> MetadataResult<()> {
        self.ensure_initialized()?;

        let subject_key = acl.subject_key().to_string();
        debug!("Updating ACL for subject: {}", subject_key);

        let _write_guard = self.write_lock.lock().await;
        let mut snapshot = {
            let storage = self.storage_read()?;
            storage.clone()
        };
        snapshot.insert(subject_key.clone(), acl.clone());
        self.persist_storage_snapshot(&snapshot).await?;
        self.replace_storage(snapshot)?;
        self.store_cache_entry(&subject_key, Some(acl))?;

        debug!("ACL updated successfully for subject: {}", subject_key);
        Ok(())
    }

    fn get_acl<S: Subject + Send + Sync>(
        &self,
        subject: &S,
    ) -> impl std::future::Future<Output = MetadataResult<Option<Acl>>> + Send {
        let initialized = self.ensure_initialized();
        let subject_key = subject.subject_key().to_string();
        let provider = self;

        async move {
            initialized?;

            debug!("Getting ACL for subject: {}", subject_key);
            provider.get_cached(&subject_key).await
        }
    }

    async fn list_acl(&self, subject_filter: Option<&str>, resource_filter: Option<&str>) -> MetadataResult<Vec<Acl>> {
        self.ensure_initialized()?;

        debug!(
            "Listing ACLs with subject_filter={:?}, resource_filter={:?}",
            subject_filter, resource_filter
        );

        // Load all ACLs from storage, then overlay live cache entries.
        let mut acls = self.list_from_storage()?;
        let cache_snapshot: Vec<Acl> = self
            .cache_read()?
            .values()
            .filter_map(|cached| cached.acl.clone())
            .collect();
        for cached_acl in cache_snapshot {
            if let Some(existing) = acls
                .iter_mut()
                .find(|existing_acl| existing_acl.subject_key() == cached_acl.subject_key())
            {
                *existing = cached_acl;
            } else {
                acls.push(cached_acl);
            }
        }

        // Apply filters
        let filtered_acls = self.filter_acls(acls, subject_filter, resource_filter);

        debug!("Found {} matching ACLs", filtered_acls.len());
        Ok(filtered_acls)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::action::Action;
    use tempfile::TempDir;

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
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_local_provider_get_acl() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(AuthConfig::default(), None).unwrap();

        let user = User::of("test");
        let result = provider.get_acl(&user).await;

        assert!(result.is_ok());
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
    fn initialize_applies_acl_cache_config() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider
            .initialize(
                AuthConfig {
                    acl_cache_max_num: 2,
                    acl_cache_expired_second: 3,
                    acl_cache_refresh_second: 4,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();

        assert_eq!(provider.cache_config.max_size, 2);
        assert_eq!(provider.cache_config.ttl, Duration::from_secs(3));
        assert_eq!(provider.cache_config.refresh_interval, Duration::from_secs(4));
    }

    #[tokio::test]
    async fn zero_acl_cache_max_num_disables_cache_entries() {
        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider
            .initialize(
                AuthConfig {
                    acl_cache_max_num: 0,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();

        let acl = Acl::of(
            "cache-disabled",
            SubjectType::User,
            Policy::of(
                vec![Resource::of_topic("topic-a")],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ),
        );
        provider.create_acl(acl).await.unwrap();

        let user = User::of("cache-disabled");
        assert!(provider.get_acl(&user).await.unwrap().is_some());
        assert!(provider.cache.read().unwrap().is_empty());
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

    #[tokio::test]
    async fn local_provider_persists_acls_across_reinitialization() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(config.clone(), None).unwrap();
        let acl = Acl::of(
            "alice",
            SubjectType::User,
            Policy::of(
                vec![Resource::of_topic("topic-a")],
                vec![Action::Pub],
                Environment::of("192.168.0.1"),
                Decision::Allow,
            ),
        );
        provider.create_acl(acl).await.unwrap();

        let mut restarted = LocalAuthorizationMetadataProvider::new();
        restarted.initialize(config, None).unwrap();
        let user = User::of("alice");
        let restored = restarted.get_acl(&user).await.unwrap().unwrap();

        assert_eq!(restored.subject_key(), "User:alice");
        assert_eq!(restored.policies().len(), 1);
        assert_eq!(restored.policies()[0].entries().len(), 1);
        assert_eq!(
            restored.policies()[0].entries()[0].resource().resource_key().as_deref(),
            Some("Topic:topic-a")
        );
    }

    #[tokio::test]
    async fn local_provider_persists_acl_update_and_delete_across_reinitialization() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let mut provider = LocalAuthorizationMetadataProvider::new();
        provider.initialize(config.clone(), None).unwrap();
        provider
            .create_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("topic-a")],
                    vec![Action::Pub],
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .unwrap();
        provider
            .update_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("topic-b")],
                    vec![Action::Sub],
                    None,
                    Decision::Deny,
                ),
            ))
            .await
            .unwrap();

        let mut restarted = LocalAuthorizationMetadataProvider::new();
        restarted.initialize(config.clone(), None).unwrap();
        let user = User::of("alice");
        let restored = restarted.get_acl(&user).await.unwrap().unwrap();
        let entry = &restored.policies()[0].entries()[0];
        assert_eq!(entry.resource().resource_key().as_deref(), Some("Topic:topic-b"));
        assert_eq!(entry.actions(), &vec![Action::Sub]);
        assert_eq!(entry.decision(), Decision::Deny);

        restarted.delete_acl(&user).await.unwrap();

        let mut deleted_restart = LocalAuthorizationMetadataProvider::new();
        deleted_restart.initialize(config, None).unwrap();
        assert!(deleted_restart.get_acl(&user).await.unwrap().is_none());
    }

    #[test]
    fn local_provider_rejects_corrupted_acl_snapshot() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };
        let snapshot = temp.path().join("auth").join("acls.json");
        fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        fs::write(&snapshot, b"{not valid json").unwrap();

        let mut provider = LocalAuthorizationMetadataProvider::new();
        let error = provider.initialize(config, None).unwrap_err();

        assert!(error.to_string().contains("acls.json"));
    }
}
