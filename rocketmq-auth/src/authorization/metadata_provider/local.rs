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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::time::Duration;

use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoDurabilityOutcome;
use rocketmq_runtime::MonotonicClock;
use rocketmq_runtime::SystemMonotonicClock;
use rocketmq_security_api::Action;
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
use crate::runtime_bridge::AuthBlockingExecutor;

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
/// use rocketmq_auth::LocalAuthorizationMetadataProvider;
/// use rocketmq_auth::AuthConfig;
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
    clock: Arc<dyn MonotonicClock>,
    /// Generation of the canonical storage snapshot. Writers publish storage before advancing the
    /// generation with `Release`; refill readers use `Acquire` before reading and while holding the
    /// cache publication lock.
    storage_generation: AtomicU64,

    /// Initialization state
    initialized: Arc<RwLock<bool>>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
    blocking: AuthBlockingExecutor,
    metadata_io: Option<MetadataIoActor>,
    #[cfg(test)]
    refill_hook: std::sync::Mutex<Option<CacheRefillHook>>,
    #[cfg(test)]
    cache_lock_attempt_hook: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>,
    #[cfg(test)]
    canonical_read_count: AtomicU64,
}

enum CacheCommit {
    Store { subject_key: String, acl: Acl },
    Remove { subject_key: String },
    Clear,
}

#[cfg(test)]
struct CacheRefillHook {
    storage_read: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

/// Cached ACL entry with expiration
#[derive(Clone, Debug)]
struct CachedAcl {
    acl: Option<Acl>,
    /// Timestamp when this entry was last accessed (for LRU)
    last_accessed: Duration,
    /// Timestamp when this entry was created (for TTL)
    created_at: Duration,
}

impl CachedAcl {
    fn new(acl: Option<Acl>, now: Duration) -> Self {
        Self {
            acl,
            last_accessed: now,
            created_at: now,
        }
    }

    /// A cached value is never returned beyond either configured bound. A zero refresh interval
    /// therefore makes every lookup read through to the canonical in-memory storage snapshot.
    fn requires_reload(&self, now: Duration, config: &CacheConfig) -> bool {
        let age = now.saturating_sub(self.created_at);
        age >= config.ttl || age >= config.refresh_interval
    }

    fn touch(&mut self, now: Duration) {
        self.last_accessed = now;
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
            clock: Arc::new(SystemMonotonicClock::new()),
            storage_generation: AtomicU64::new(0),
            initialized: Arc::new(RwLock::new(false)),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            blocking: AuthBlockingExecutor::default(),
            metadata_io: None,
            #[cfg(test)]
            refill_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            cache_lock_attempt_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            canonical_read_count: AtomicU64::new(0),
        }
    }

    pub fn with_metadata_io(metadata_io: MetadataIoActor) -> Self {
        Self {
            metadata_io: Some(metadata_io),
            ..Self::new()
        }
    }

    #[cfg(test)]
    fn with_clock(clock: Arc<dyn MonotonicClock>) -> Self {
        Self { clock, ..Self::new() }
    }

    #[cfg(test)]
    fn install_refill_hook(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let storage_read = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *self.refill_hook.lock().unwrap() = Some(CacheRefillHook {
            storage_read: storage_read.clone(),
            resume: resume.clone(),
        });
        (storage_read, resume)
    }

    #[cfg(test)]
    fn install_cache_lock_attempt_hook(&self) -> Arc<tokio::sync::Notify> {
        let attempted = Arc::new(tokio::sync::Notify::new());
        *self.cache_lock_attempt_hook.lock().unwrap() = Some(attempted.clone());
        attempted
    }

    #[cfg(test)]
    fn signal_cache_lock_attempt(&self) {
        if let Some(attempted) = self.cache_lock_attempt_hook.lock().unwrap().take() {
            attempted.notify_one();
        }
    }

    #[cfg(test)]
    async fn pause_after_storage_read(&self) {
        let hook = self.refill_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook.storage_read.notify_one();
            hook.resume.notified().await;
        }
    }

    fn load_from_storage(&self, subject_key: &str) -> MetadataResult<Option<Acl>> {
        #[cfg(test)]
        self.canonical_read_count.fetch_add(1, Ordering::Relaxed);
        let storage = self.storage_read()?;
        Ok(storage.get(subject_key).cloned())
    }

    async fn persist_storage_snapshot(&self, snapshot: &HashMap<String, Acl>) -> MetadataResult<()> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let content = encode_acl_snapshot(snapshot)?;
        if let Some(metadata_io) = &self.metadata_io {
            match metadata_io
                .submit_next_durable(
                    "auth.authorization-acls",
                    path,
                    content,
                    MetadataDeadline::after(Duration::from_secs(5)),
                )
                .await
                .map_err(AuthorizationError::MetadataIo)?
            {
                MetadataIoDurabilityOutcome::Durable(_) => return Ok(()),
                MetadataIoDurabilityOutcome::TargetConflict(request) => {
                    return Err(AuthorizationError::StorageWriteFailed {
                        path: request.target().display().to_string(),
                        reason: "metadata resource target conflict".to_owned(),
                    });
                }
            }
        }
        let path = path.clone();
        let path_display = path.display().to_string();
        self.blocking
            .spawn_io("auth.authorization.write_acl_snapshot", move || {
                write_acl_snapshot(&path, &content)
            })
            .await
            .map_err(|error| AuthorizationError::StorageWriteFailed {
                path: path_display,
                reason: format!("ACL snapshot task failed: {error}"),
            })?
    }

    fn replace_storage(&self, snapshot: HashMap<String, Acl>) -> MetadataResult<()> {
        self.commit_storage_snapshot(snapshot, CacheCommit::Clear)
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

        // Check cache first. Refresh is access-triggered, so no background task or second
        // lifecycle owner is required.
        {
            #[cfg(test)]
            self.signal_cache_lock_attempt();
            let mut cache = self.cache_write()?;
            let now = self.clock.now();
            if let Some(cached) = cache.get_mut(subject_key) {
                if !cached.requires_reload(now, &self.cache_config) {
                    cached.touch(now);
                    debug!("Cache hit for subject: {}", subject_key);
                    return Ok(cached.acl.clone());
                } else {
                    cache.remove(subject_key);
                    debug!(
                        "Cache entry reached its refresh or expiry bound for subject: {}",
                        subject_key
                    );
                }
            }
        }

        // Cache miss or expired, load from storage
        debug!("Cache miss for subject: {}", subject_key);
        let observed_generation = self.storage_generation.load(Ordering::Acquire);
        let acl = self.load_from_storage(subject_key)?;

        #[cfg(test)]
        self.pause_after_storage_read().await;

        // A mutation commits while holding the same cache lock, so equality here proves that the
        // canonical value read above still belongs to the current storage generation.
        {
            let mut cache = self.cache_write()?;
            if self.storage_generation.load(Ordering::Acquire) == observed_generation {
                self.publish_cache_entry(&mut cache, subject_key, acl.clone());
                return Ok(acl);
            }
        }

        // A refill that overlaps a mutation waits once for the existing mutation lock, then reads
        // and publishes while holding the cache commit boundary. This is bounded and cannot spin
        // under repeated writer contention.
        let _write_guard = self.write_lock.lock().await;
        let mut cache = self.cache_write()?;
        let acl = self.load_from_storage(subject_key)?;
        self.publish_cache_entry(&mut cache, subject_key, acl.clone());

        Ok(acl)
    }

    /// Commit invariant: every canonical replacement holds the cache publication lock, replaces
    /// storage, and advances `storage_generation` with `Release` before applying its final cache
    /// state. A refill either publishes first and is overwritten by this commit, or observes the
    /// new generation with `Acquire` and performs a settled reread.
    fn commit_storage_snapshot(&self, snapshot: HashMap<String, Acl>, cache_commit: CacheCommit) -> MetadataResult<()> {
        let mut cache = self.cache_write()?;
        {
            let mut storage = self.storage_write()?;
            *storage = snapshot;
            self.storage_generation.fetch_add(1, Ordering::Release);
        }

        match cache_commit {
            CacheCommit::Store { subject_key, acl } => {
                self.publish_cache_entry(&mut cache, &subject_key, Some(acl));
            }
            CacheCommit::Remove { subject_key } => {
                if cache.remove(&subject_key).is_some() {
                    debug!("Invalidated cache for subject: {}", subject_key);
                }
            }
            CacheCommit::Clear => cache.clear(),
        }
        Ok(())
    }

    fn publish_cache_entry(&self, cache: &mut HashMap<String, CachedAcl>, subject_key: &str, acl: Option<Acl>) {
        if self.cache_config.max_size == 0 {
            cache.remove(subject_key);
            return;
        }
        if !cache.contains_key(subject_key) && cache.len() >= self.cache_config.max_size {
            self.evict_oldest(cache);
        }
        let now = self.clock.now();
        cache.insert(subject_key.to_string(), CachedAcl::new(acl, now));
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
            .map_err(|_| AuthorizationError::StorageLockFailed("auth.authorization.initialized".to_string()))
    }

    fn initialized_write(&self) -> MetadataResult<RwLockWriteGuard<'_, bool>> {
        self.initialized
            .write()
            .map_err(|_| AuthorizationError::StorageLockFailed("auth.authorization.initialized".to_string()))
    }

    fn storage_read(&self) -> MetadataResult<RwLockReadGuard<'_, HashMap<String, Acl>>> {
        self.storage
            .read()
            .map_err(|_| AuthorizationError::StorageLockFailed("auth.authorization.storage".to_string()))
    }

    fn storage_write(&self) -> MetadataResult<RwLockWriteGuard<'_, HashMap<String, Acl>>> {
        self.storage
            .write()
            .map_err(|_| AuthorizationError::StorageLockFailed("auth.authorization.storage".to_string()))
    }

    fn cache_write(&self) -> MetadataResult<RwLockWriteGuard<'_, HashMap<String, CachedAcl>>> {
        self.cache
            .write()
            .map_err(|_| AuthorizationError::StorageLockFailed("auth.authorization.cache".to_string()))
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
        Err(error) => return Err(storage_read_error(path, error)),
    };
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(HashMap::new());
    }
    let snapshot: StoredAclSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| snapshot_decode_error(format!("{}: {error}", path.display())))?;
    let mut acls = HashMap::new();
    for record in snapshot.acls {
        let acl = acl_from_record(record)?;
        acls.insert(acl.subject_key().to_string(), acl);
    }
    Ok(acls)
}

fn encode_acl_snapshot(acls: &HashMap<String, Acl>) -> MetadataResult<Vec<u8>> {
    let mut records = acls.values().map(acl_to_record).collect::<Vec<_>>();
    records.sort_by(|left, right| left.subject.cmp(&right.subject));
    let snapshot = StoredAclSnapshot { acls: records };
    serde_json::to_vec_pretty(&snapshot).map_err(|error| snapshot_encode_error(error.to_string()))
}

fn write_acl_snapshot(path: &Path, content: &[u8]) -> MetadataResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| storage_write_error(parent, error))?;
    }

    let temp_file = temp_snapshot_path(path);
    std::fs::write(&temp_file, content).map_err(|error| storage_write_error(&temp_file, error))?;
    match std::fs::rename(&temp_file, path) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            std::fs::copy(&temp_file, path).map_err(|error| AuthorizationError::StorageWriteFailed {
                path: path.display().to_string(),
                reason: format!("{error}; rename failed first: {rename_error}"),
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
    let policy_type = PolicyType::get_by_name(&record.policy_type)
        .ok_or_else(|| snapshot_decode_error(format!("Invalid policy type '{}'", record.policy_type)))?;
    let entries = record
        .entries
        .into_iter()
        .map(policy_entry_from_record)
        .collect::<MetadataResult<Vec<_>>>()?;
    Ok(Policy::of_entries(policy_type, entries))
}

fn policy_entry_from_record(record: StoredPolicyEntryRecord) -> MetadataResult<PolicyEntry> {
    let resource = Resource::of_str(&record.resource)
        .ok_or_else(|| snapshot_decode_error(format!("Invalid resource '{}'", record.resource)))?;
    let actions = record
        .actions
        .iter()
        .map(|action| {
            Action::get_by_name(action).ok_or_else(|| snapshot_decode_error(format!("Invalid action '{action}'")))
        })
        .collect::<MetadataResult<Vec<_>>>()?;
    let decision = Decision::get_by_name(&record.decision)
        .ok_or_else(|| snapshot_decode_error(format!("Invalid decision '{}'", record.decision)))?;
    let environment = Environment::of_list(record.source_ips);
    Ok(PolicyEntry::of(resource, actions, environment, decision))
}

fn subject_type_from_key(subject_key: &str) -> MetadataResult<SubjectType> {
    let Some((subject_type, _)) = subject_key.split_once(':') else {
        return Ok(SubjectType::User);
    };
    SubjectType::get_by_name(subject_type)
        .ok_or_else(|| snapshot_decode_error(format!("Invalid subject type '{subject_type}'")))
}

fn storage_read_error(path: &Path, error: std::io::Error) -> AuthorizationError {
    AuthorizationError::StorageReadFailed {
        path: path.display().to_string(),
        reason: error.to_string(),
    }
}

fn storage_write_error(path: &Path, error: std::io::Error) -> AuthorizationError {
    AuthorizationError::StorageWriteFailed {
        path: path.display().to_string(),
        reason: error.to_string(),
    }
}

fn snapshot_encode_error(reason: impl Into<String>) -> AuthorizationError {
    AuthorizationError::SerializationFailed {
        operation: "encode",
        format: "JSON",
        reason: reason.into(),
    }
}

fn snapshot_decode_error(reason: impl Into<String>) -> AuthorizationError {
    AuthorizationError::SerializationFailed {
        operation: "decode",
        format: "JSON",
        reason: reason.into(),
    }
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

        let snapshot = match storage_path {
            Some(path) => read_acl_snapshot(&path)?,
            None => HashMap::new(),
        };
        self.replace_storage(snapshot)?;

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

        if let Err(error) = self.commit_storage_snapshot(HashMap::new(), CacheCommit::Clear) {
            warn!("LocalAuthorizationMetadataProvider storage cleanup failed during shutdown: {error}");
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
            return Err(AuthorizationError::InvalidContext(format!(
                "ACL already exists for subject: {}",
                subject_key
            )));
        }
        snapshot.insert(subject_key.clone(), acl.clone());

        self.persist_storage_snapshot(&snapshot).await?;
        self.commit_storage_snapshot(
            snapshot,
            CacheCommit::Store {
                subject_key: subject_key.clone(),
                acl,
            },
        )?;

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
        self.commit_storage_snapshot(
            snapshot,
            CacheCommit::Remove {
                subject_key: subject_key.to_string(),
            },
        )?;

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
        self.commit_storage_snapshot(
            snapshot,
            CacheCommit::Store {
                subject_key: subject_key.clone(),
                acl,
            },
        )?;

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

        // Canonical storage is the source of truth. Mixing it with a separately sampled cache can
        // resurrect a value that a concurrent update or revocation already replaced.
        let filtered_acls = self.filter_acls(self.list_from_storage()?, subject_filter, resource_filter);

        debug!("Found {} matching ACLs", filtered_acls.len());
        Ok(filtered_acls)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Barrier;

    use cheetah_string::CheetahString;
    use rocketmq_security_api::Action;
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

    #[derive(Default)]
    struct ManualClock {
        now_millis: AtomicU64,
    }

    impl ManualClock {
        fn advance(&self, duration: Duration) {
            let millis = u64::try_from(duration.as_millis()).unwrap();
            self.now_millis.fetch_add(millis, Ordering::Relaxed);
        }
    }

    impl MonotonicClock for ManualClock {
        fn now(&self) -> Duration {
            Duration::from_millis(self.now_millis.load(Ordering::Relaxed))
        }
    }

    fn acl_for_topic(subject: &str, topic: &str) -> Acl {
        Acl::of(
            subject,
            SubjectType::User,
            Policy::of(
                vec![Resource::of_topic(topic)],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ),
        )
    }

    fn metadata_io_actor(name: &str) -> MetadataIoActor {
        let context = rocketmq_runtime::RuntimeContext::try_from_current(name).unwrap();
        rocketmq_runtime::MetadataIoConfig::default()
            .into_plan()
            .expect("default metadata I/O config is valid")
            .start(&context.service_context("auth.authorization-metadata"))
            .unwrap()
    }

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

    #[tokio::test]
    async fn acl_cache_refreshes_from_storage_at_the_configured_boundary() {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock.clone());
        provider
            .initialize(
                AuthConfig {
                    acl_cache_expired_second: 10,
                    acl_cache_refresh_second: 2,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();

        let first = acl_for_topic("refresh-bound", "topic-a");
        provider.create_acl(first.clone()).await.unwrap();
        let user = User::of("refresh-bound");
        let reads_before = provider.canonical_read_count.load(Ordering::Relaxed);

        clock.advance(Duration::from_millis(1_999));
        assert_eq!(provider.get_acl(&user).await.unwrap(), Some(first.clone()));
        assert_eq!(provider.canonical_read_count.load(Ordering::Relaxed), reads_before);
        assert_eq!(
            provider.cache.read().unwrap()[first.subject_key()].created_at,
            Duration::ZERO
        );

        clock.advance(Duration::from_millis(1));
        assert_eq!(provider.get_acl(&user).await.unwrap(), Some(first.clone()));
        assert_eq!(provider.canonical_read_count.load(Ordering::Relaxed), reads_before + 1);
        assert_eq!(
            provider.cache.read().unwrap()[first.subject_key()].created_at,
            Duration::from_secs(2)
        );
        assert_eq!(provider.list_acl(None, None).await.unwrap(), vec![first]);
    }

    #[tokio::test]
    async fn zero_acl_cache_refresh_interval_reads_through_on_every_lookup() {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock.clone());
        provider
            .initialize(
                AuthConfig {
                    acl_cache_expired_second: 10,
                    acl_cache_refresh_second: 0,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();

        let first = acl_for_topic("refresh-zero", "topic-a");
        let second = acl_for_topic("refresh-zero", "topic-b");
        let third = acl_for_topic("refresh-zero", "topic-c");
        let user = User::of("refresh-zero");
        provider.create_acl(first).await.unwrap();

        provider.update_acl(second.clone()).await.unwrap();
        let reads_before = provider.canonical_read_count.load(Ordering::Relaxed);
        assert_eq!(provider.get_acl(&user).await.unwrap(), Some(second));
        assert_eq!(provider.canonical_read_count.load(Ordering::Relaxed), reads_before + 1);

        provider.update_acl(third.clone()).await.unwrap();
        assert_eq!(provider.get_acl(&user).await.unwrap(), Some(third.clone()));
        assert_eq!(provider.canonical_read_count.load(Ordering::Relaxed), reads_before + 2);
        assert_eq!(provider.list_acl(None, None).await.unwrap(), vec![third]);
        assert_eq!(clock.now(), Duration::ZERO);
    }

    #[tokio::test]
    async fn acl_cache_expires_at_the_exact_ttl_boundary() {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock.clone());
        provider
            .initialize(
                AuthConfig {
                    acl_cache_expired_second: 1,
                    acl_cache_refresh_second: 10,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();

        let first = acl_for_topic("expiry-bound", "topic-a");
        provider.create_acl(first.clone()).await.unwrap();
        let user = User::of("expiry-bound");
        let reads_before = provider.canonical_read_count.load(Ordering::Relaxed);

        clock.advance(Duration::from_millis(999));
        assert_eq!(provider.get_acl(&user).await.unwrap(), Some(first.clone()));
        assert_eq!(provider.canonical_read_count.load(Ordering::Relaxed), reads_before);
        assert_eq!(
            provider.cache.read().unwrap()[first.subject_key()].created_at,
            Duration::ZERO
        );

        clock.advance(Duration::from_millis(1));
        assert_eq!(provider.get_acl(&user).await.unwrap(), Some(first.clone()));
        assert_eq!(provider.canonical_read_count.load(Ordering::Relaxed), reads_before + 1);
        assert_eq!(
            provider.cache.read().unwrap()[first.subject_key()].created_at,
            Duration::from_secs(1)
        );
    }

    async fn assert_cache_wait_samples_boundary(ttl: Duration, refresh_interval: Duration, boundary: Duration) {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock.clone());
        provider
            .initialize(
                AuthConfig {
                    acl_cache_expired_second: u32::try_from(ttl.as_secs()).unwrap(),
                    acl_cache_refresh_second: u32::try_from(refresh_interval.as_secs()).unwrap(),
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();

        let acl = acl_for_topic("lock-boundary", "topic-a");
        provider.create_acl(acl.clone()).await.unwrap();
        let provider = Arc::new(provider);
        let cache = provider.cache.clone();
        let cache_locked = Arc::new(Barrier::new(2));
        let release_cache = Arc::new(Barrier::new(2));
        let holder = {
            let cache_locked = cache_locked.clone();
            let release_cache = release_cache.clone();
            std::thread::spawn(move || {
                let _cache_guard = cache.write().unwrap();
                cache_locked.wait();
                release_cache.wait();
            })
        };
        cache_locked.wait();

        let attempted = provider.install_cache_lock_attempt_hook();
        let reader = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.get_acl(&User::of("lock-boundary")).await })
        };
        attempted.notified().await;
        clock.advance(boundary);
        release_cache.wait();
        holder.join().unwrap();

        assert_eq!(reader.await.unwrap().unwrap(), Some(acl.clone()));
        assert_eq!(provider.cache.read().unwrap()[acl.subject_key()].created_at, boundary);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_freshness_samples_clock_after_lock_wait_at_refresh_and_ttl_boundaries() {
        assert_cache_wait_samples_boundary(Duration::from_secs(10), Duration::from_secs(2), Duration::from_secs(2))
            .await;
        assert_cache_wait_samples_boundary(Duration::from_secs(1), Duration::from_secs(10), Duration::from_secs(1))
            .await;
    }

    #[tokio::test]
    async fn stale_refill_cannot_overwrite_or_return_after_concurrent_update() {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock.clone());
        provider
            .initialize(
                AuthConfig {
                    acl_cache_expired_second: 10,
                    acl_cache_refresh_second: 1,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();
        let old_acl = acl_for_topic("update-race", "topic-old");
        let new_acl = acl_for_topic("update-race", "topic-new");
        provider.create_acl(old_acl).await.unwrap();
        clock.advance(Duration::from_secs(1));
        let (storage_read, resume) = provider.install_refill_hook();
        let provider = Arc::new(provider);
        let reader = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.get_acl(&User::of("update-race")).await })
        };

        storage_read.notified().await;
        provider.update_acl(new_acl.clone()).await.unwrap();
        resume.notify_one();

        assert_eq!(reader.await.unwrap().unwrap(), Some(new_acl.clone()));
        assert_eq!(
            provider.cache.read().unwrap()[new_acl.subject_key()].acl,
            Some(new_acl.clone())
        );
        let reads_after_race = provider.canonical_read_count.load(Ordering::Relaxed);
        assert_eq!(
            provider.get_acl(&User::of("update-race")).await.unwrap(),
            Some(new_acl.clone())
        );
        assert_eq!(
            provider.canonical_read_count.load(Ordering::Relaxed),
            reads_after_race,
            "the immediate post-race lookup must hit the non-expired cache"
        );
        assert_eq!(provider.list_acl(None, None).await.unwrap(), vec![new_acl]);
    }

    #[tokio::test]
    async fn stale_refill_cannot_resurrect_or_return_after_concurrent_delete() {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock.clone());
        provider
            .initialize(
                AuthConfig {
                    acl_cache_expired_second: 10,
                    acl_cache_refresh_second: 1,
                    ..AuthConfig::default()
                },
                None,
            )
            .unwrap();
        provider
            .create_acl(acl_for_topic("delete-race", "topic-old"))
            .await
            .unwrap();
        clock.advance(Duration::from_secs(1));
        let (storage_read, resume) = provider.install_refill_hook();
        let provider = Arc::new(provider);
        let reader = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.get_acl(&User::of("delete-race")).await })
        };

        storage_read.notified().await;
        provider.delete_acl(&User::of("delete-race")).await.unwrap();
        resume.notify_one();

        assert_eq!(reader.await.unwrap().unwrap(), None);
        assert_eq!(provider.cache.read().unwrap()["User:delete-race"].acl, None);
        let reads_after_race = provider.canonical_read_count.load(Ordering::Relaxed);
        assert_eq!(provider.get_acl(&User::of("delete-race")).await.unwrap(), None);
        assert_eq!(
            provider.canonical_read_count.load(Ordering::Relaxed),
            reads_after_race,
            "the immediate post-revocation lookup must hit the negative cache"
        );
        assert!(provider.list_acl(None, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_uses_canonical_storage_and_never_overlays_a_stale_cache_snapshot() {
        let clock = Arc::new(ManualClock::default());
        let mut provider = LocalAuthorizationMetadataProvider::with_clock(clock);
        provider.initialize(AuthConfig::default(), None).unwrap();
        let old_acl = acl_for_topic("list-consistency", "topic-old");
        let new_acl = acl_for_topic("list-consistency", "topic-new");
        let subject_key = old_acl.subject_key().to_string();
        provider.create_acl(old_acl.clone()).await.unwrap();
        provider.update_acl(new_acl.clone()).await.unwrap();
        provider
            .cache
            .write()
            .unwrap()
            .insert(subject_key.clone(), CachedAcl::new(Some(old_acl), Duration::ZERO));

        assert_eq!(provider.list_acl(None, None).await.unwrap(), vec![new_acl.clone()]);

        provider.delete_acl(&User::of("list-consistency")).await.unwrap();
        provider
            .cache
            .write()
            .unwrap()
            .insert(subject_key, CachedAcl::new(Some(new_acl), Duration::ZERO));
        assert!(provider.list_acl(None, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn local_provider_persists_acls_across_reinitialization() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.json").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let metadata_io = metadata_io_actor("auth-authorization-persistence-test");
        let mut provider = LocalAuthorizationMetadataProvider::with_metadata_io(metadata_io.clone());
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

        let mut restarted = LocalAuthorizationMetadataProvider::with_metadata_io(metadata_io);
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

        let metadata_io = metadata_io_actor("auth-authorization-update-delete-test");
        let mut provider = LocalAuthorizationMetadataProvider::with_metadata_io(metadata_io.clone());
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

        let mut restarted = LocalAuthorizationMetadataProvider::with_metadata_io(metadata_io.clone());
        restarted.initialize(config.clone(), None).unwrap();
        let user = User::of("alice");
        let restored = restarted.get_acl(&user).await.unwrap().unwrap();
        let entry = &restored.policies()[0].entries()[0];
        assert_eq!(entry.resource().resource_key().as_deref(), Some("Topic:topic-b"));
        assert_eq!(entry.actions(), &vec![Action::Sub]);
        assert_eq!(entry.decision(), Decision::Deny);

        restarted.delete_acl(&user).await.unwrap();

        let mut deleted_restart = LocalAuthorizationMetadataProvider::with_metadata_io(metadata_io);
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
        assert!(matches!(error, AuthorizationError::SerializationFailed { .. }));
    }
}
