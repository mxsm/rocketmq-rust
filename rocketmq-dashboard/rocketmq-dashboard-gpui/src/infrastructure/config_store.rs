// Copyright 2026 The RocketMQ Rust Authors
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

//! Versioned, non-sensitive desktop configuration persisted on the storage-I/O lane.

use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use rocketmq_dashboard_common::{
    ConnectionScope, ConnectionSnapshot, CredentialSourceKind, TransportSettings, normalize_nameserver_selection,
    normalize_proxy_selection,
};
use rocketmq_runtime::ChildServiceContext;
use serde::{Deserialize, Serialize};

/// Current on-disk schema understood by this delivery.
pub const CONFIG_SCHEMA_VERSION: u32 = 1;
/// Environment override used by tests, packaged builds, and portable deployments.
pub const CONFIG_PATH_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_CONFIG_PATH";

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Authentication settings that never contain credential values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthConfig {
    /// Whether local dashboard sign-in is required.
    pub enabled: bool,
    /// Source category for RocketMQ Admin credentials.
    pub credential_source: CredentialSourceKind,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            credential_source: CredentialSourceKind::None,
        }
    }
}

/// Maximum accepted per-series History retention.
pub const HISTORY_MAX_POINTS_CAP: usize = 100_000;
/// Maximum accepted distinct History series.
pub const HISTORY_MAX_SERIES_CAP: usize = 4_096;
/// Maximum accepted observations across the complete History file.
pub const HISTORY_MAX_TOTAL_POINTS_CAP: usize = 100_000;

/// Feature and bounded-lifecycle settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct FoundationFlags {
    /// Enables the owned History collector.
    pub history_enabled: bool,
    /// Collection interval in seconds. Zero disables collection even when the flag is enabled.
    pub history_interval_seconds: u64,
    /// Maximum retained observations for each metric series.
    pub history_max_points_per_series: usize,
    /// Maximum retained metric series across the complete History file.
    pub history_max_series: usize,
    /// Maximum retained observations across the complete History file.
    pub history_max_total_points: usize,
    /// Enables the local Monitor store without exposing a Monitor page in Delivery 02.
    pub monitor_enabled: bool,
}

impl Default for FoundationFlags {
    fn default() -> Self {
        Self {
            history_enabled: false,
            history_interval_seconds: 60,
            history_max_points_per_series: 1_440,
            history_max_series: 512,
            history_max_total_points: 20_000,
            monitor_enabled: false,
        }
    }
}

/// Complete non-sensitive desktop configuration.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct DesktopConfig {
    /// Schema discriminator. Unknown versions are never overwritten.
    pub schema_version: u32,
    /// Monotonic persisted revision used to invalidate sessions and requests.
    pub revision: u64,
    /// Configured normalized NameServer endpoints.
    pub nameservers: Vec<String>,
    /// Selected NameServer endpoint.
    pub current_nameserver: Option<String>,
    /// Admin transport settings.
    pub transport: TransportSettings,
    /// Configured normalized Proxy endpoints.
    pub proxies: Vec<String>,
    /// Selected Proxy endpoint.
    pub current_proxy: Option<String>,
    /// Active query scope.
    pub scope: ConnectionScope,
    /// Non-sensitive authentication configuration.
    pub auth: AuthConfig,
    /// Delivery-foundation feature flags.
    pub foundations: FoundationFlags,
}

impl fmt::Debug for DesktopConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopConfig")
            .field("schema_version", &self.schema_version)
            .field("revision", &self.revision)
            .field("nameserver_count", &self.nameservers.len())
            .field("current_nameserver_configured", &self.current_nameserver.is_some())
            .field("transport", &self.transport)
            .field("proxy_count", &self.proxies.len())
            .field("current_proxy_configured", &self.current_proxy.is_some())
            .field("scope", &self.scope)
            .field("auth", &self.auth)
            .field("foundations", &self.foundations)
            .finish()
    }
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            revision: 0,
            nameservers: Vec::new(),
            current_nameserver: None,
            transport: TransportSettings::default(),
            proxies: Vec::new(),
            current_proxy: None,
            scope: ConnectionScope::NameServer,
            auth: AuthConfig::default(),
            foundations: FoundationFlags::default(),
        }
    }
}

impl DesktopConfig {
    /// Validates and normalizes the complete persisted compatibility surface.
    pub fn normalize(mut self) -> Result<Self, ConfigStoreError> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigStoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: CONFIG_SCHEMA_VERSION,
            });
        }
        (self.nameservers, self.current_nameserver) =
            normalize_nameserver_selection(&self.nameservers, self.current_nameserver.as_deref())
                .map_err(|error| ConfigStoreError::Validation(error.to_string()))?;
        (self.proxies, self.current_proxy) = normalize_proxy_selection(&self.proxies, self.current_proxy.as_deref())
            .map_err(|error| ConfigStoreError::Validation(error.to_string()))?;
        if self.scope == ConnectionScope::Proxy && self.current_proxy.is_none() {
            return Err(ConfigStoreError::Validation(
                "Proxy scope requires a selected Proxy endpoint".to_owned(),
            ));
        }
        self.foundations.history_max_points_per_series = self
            .foundations
            .history_max_points_per_series
            .min(HISTORY_MAX_POINTS_CAP);
        self.foundations.history_max_series = self.foundations.history_max_series.min(HISTORY_MAX_SERIES_CAP);
        self.foundations.history_max_total_points = self
            .foundations
            .history_max_total_points
            .min(HISTORY_MAX_TOTAL_POINTS_CAP);
        Ok(self)
    }

    /// Projects the immutable provider input without any credential values.
    pub fn connection_snapshot(&self) -> ConnectionSnapshot {
        ConnectionSnapshot {
            revision: self.revision,
            nameserver: self.current_nameserver.clone(),
            proxy: self.current_proxy.clone(),
            scope: self.scope,
            transport: self.transport,
            credential_source: self.auth.credential_source,
        }
    }
}

/// Recoverable configuration failure. Its display text never includes file contents.
#[derive(Debug, thiserror::Error)]
pub enum ConfigStoreError {
    /// A filesystem operation failed.
    #[error("configuration {operation} failed at {path}: {source}")]
    Io {
        /// Stable operation category.
        operation: &'static str,
        /// Exact affected path for recovery actions.
        path: PathBuf,
        /// Platform I/O failure.
        #[source]
        source: io::Error,
    },
    /// JSON parsing failed. The malformed content is deliberately omitted.
    #[error("configuration at {path} is not valid JSON: {summary}")]
    InvalidDocument {
        /// Exact affected path.
        path: PathBuf,
        /// Parser category/position without source bytes.
        summary: String,
    },
    /// A newer or incompatible schema was found.
    #[error("unsupported configuration schema {found}; this build supports {supported}")]
    UnsupportedSchema {
        /// Schema found on disk.
        found: u32,
        /// Schema supported by this build.
        supported: u32,
    },
    /// Domain validation rejected a requested change.
    #[error("configuration validation failed: {0}")]
    Validation(String),
    /// A caller attempted to persist from a stale revision.
    #[error("configuration revision changed; reload before saving")]
    StaleRevision,
    /// The revision cannot advance further.
    #[error("configuration revision is exhausted")]
    RevisionExhausted,
    /// A damaged or unknown-schema original remains protected from overwrite.
    #[error("the existing configuration is protected; recover it before saving")]
    ProtectedOriginal,
    /// Runtime infrastructure rejected storage-lane work.
    #[error("configuration storage runtime is unavailable: {0}")]
    Runtime(String),
}

/// File-backed store with serialized updates and explicit corrupt-original protection.
pub struct DesktopConfigStore {
    path: PathBuf,
    context: ChildServiceContext,
    io_gate: tokio::sync::Mutex<()>,
    known_revision: AtomicU64,
    protected_original: AtomicBool,
}

impl DesktopConfigStore {
    /// Resolves the configured path, preferring [`CONFIG_PATH_ENV`].
    pub fn from_environment(context: ChildServiceContext) -> Result<Arc<Self>, ConfigStoreError> {
        let path = match std::env::var_os(CONFIG_PATH_ENV) {
            Some(path) if !path.is_empty() => PathBuf::from(path),
            _ => dirs::config_dir()
                .ok_or_else(|| ConfigStoreError::Validation("the user configuration directory is unavailable".into()))?
                .join("rocketmq-dashboard")
                .join("gpui")
                .join("config.json"),
        };
        Ok(Self::new(path, context))
    }

    /// Creates a store at an injected path. Tests use this to avoid user directories.
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Arc::new(Self {
            path,
            context,
            io_gate: tokio::sync::Mutex::new(()),
            known_revision: AtomicU64::new(0),
            protected_original: AtomicBool::new(false),
        })
    }

    /// Returns the exact non-secret storage path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads, parses, validates, and records the on-disk revision.
    pub async fn load(&self) -> Result<DesktopConfig, ConfigStoreError> {
        let _guard = self.io_gate.lock().await;
        let path = self.path.clone();
        let result = self
            .context
            .storage_io()
            .spawn_io("gpui-config-load", move || load_file(&path))
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?;
        match result {
            Ok(config) => {
                self.known_revision.store(config.revision, Ordering::Release);
                self.protected_original.store(false, Ordering::Release);
                Ok(config)
            }
            Err(error @ (ConfigStoreError::InvalidDocument { .. } | ConfigStoreError::UnsupportedSchema { .. })) => {
                self.protected_original.store(true, Ordering::Release);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically persists a validated next revision.
    pub async fn save_next(&self, config: DesktopConfig) -> Result<DesktopConfig, ConfigStoreError> {
        let _guard = self.io_gate.lock().await;
        if self.protected_original.load(Ordering::Acquire) {
            return Err(ConfigStoreError::ProtectedOriginal);
        }
        let known_revision = self.known_revision.load(Ordering::Acquire);
        if config.revision != known_revision {
            return Err(ConfigStoreError::StaleRevision);
        }
        let mut config = config.normalize()?;
        config.revision = config
            .revision
            .checked_add(1)
            .ok_or(ConfigStoreError::RevisionExhausted)?;
        let path = self.path.clone();
        let persisted = config.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-config-save", move || save_file(&path, &persisted))
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))??;
        self.known_revision.store(config.revision, Ordering::Release);
        Ok(config)
    }
}

fn load_file(path: &Path) -> Result<DesktopConfig, ConfigStoreError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(DesktopConfig::default()),
        Err(source) => {
            return Err(ConfigStoreError::Io {
                operation: "read",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let config =
        serde_json::from_slice::<DesktopConfig>(&bytes).map_err(|error| ConfigStoreError::InvalidDocument {
            path: path.to_path_buf(),
            summary: format!(
                "{:?} at line {}, column {}",
                error.classify(),
                error.line(),
                error.column()
            ),
        })?;
    config.normalize()
}

fn save_file(path: &Path, config: &DesktopConfig) -> Result<(), ConfigStoreError> {
    write_json_atomically(path, config)
}

pub(super) fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> Result<(), ConfigStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| ConfigStoreError::Validation("configuration path has no parent".into()))?;
    fs::create_dir_all(parent).map_err(|source| ConfigStoreError::Io {
        operation: "create directory",
        path: parent.to_path_buf(),
        source,
    })?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path.file_name().and_then(|name| name.to_str()).unwrap_or("config.json");
    let temporary = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), sequence));
    // Keep a synced temporary file on failure. In particular, Windows replacement failures may
    // require both the original/backup and replacement file for operator recovery.
    write_and_replace(&temporary, path, value)
}

fn write_and_replace<T: Serialize>(temporary: &Path, target: &Path, value: &T) -> Result<(), ConfigStoreError> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| ConfigStoreError::Validation(format!("configuration serialization failed: {error}")))?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|source| ConfigStoreError::Io {
            operation: "create temporary file",
            path: temporary.to_path_buf(),
            source,
        })?;
    file.write_all(&bytes).map_err(|source| ConfigStoreError::Io {
        operation: "write temporary file",
        path: temporary.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| ConfigStoreError::Io {
        operation: "sync temporary file",
        path: temporary.to_path_buf(),
        source,
    })?;
    drop(file);
    atomic_replace(temporary, target).map_err(|source| ConfigStoreError::Io {
        operation: "replace",
        path: target.to_path_buf(),
        source,
    })?;
    sync_parent(target)?;
    Ok(())
}

#[cfg(not(windows))]
fn atomic_replace(temporary: &Path, target: &Path) -> io::Result<()> {
    fs::rename(temporary, target)
}

#[cfg(windows)]
fn atomic_replace(temporary: &Path, target: &Path) -> io::Result<()> {
    atomic_replace_with(temporary, target, &SystemWindowsReplace)
}

#[cfg(windows)]
trait WindowsReplace {
    fn replace(&self, target: &Path, replacement: &Path, backup: &Path) -> io::Result<()>;
}

#[cfg(windows)]
struct SystemWindowsReplace;

#[cfg(windows)]
impl WindowsReplace for SystemWindowsReplace {
    fn replace(&self, target: &Path, replacement: &Path, backup: &Path) -> io::Result<()> {
        use std::os::windows::ffi::OsStrExt as _;

        #[link(name = "Kernel32")]
        unsafe extern "system" {
            fn ReplaceFileW(
                replaced_file_name: *const u16,
                replacement_file_name: *const u16,
                backup_file_name: *const u16,
                replace_flags: u32,
                exclude: *mut std::ffi::c_void,
                reserved: *mut std::ffi::c_void,
            ) -> i32;
        }

        let replaced = target.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
        let replacement = replacement.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
        let backup = backup.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
        // SAFETY: all path buffers are NUL-terminated and live for the call; reserved pointer
        // arguments are null, and ReplaceFileW does not retain any supplied pointer.
        let replaced_ok = unsafe {
            ReplaceFileW(
                replaced.as_ptr(),
                replacement.as_ptr(),
                backup.as_ptr(),
                0x0000_0002,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if replaced_ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
fn atomic_replace_with(temporary: &Path, target: &Path, api: &dyn WindowsReplace) -> io::Result<()> {
    if !target.exists() {
        return fs::rename(temporary, target);
    }
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.json");
    let backup = target.with_file_name(format!(
        ".{file_name}.{}.{}.replacement-backup",
        std::process::id(),
        sequence
    ));
    let recovery = target.with_file_name(format!(
        ".{file_name}.{}.{}.pre-replacement-recovery",
        std::process::id(),
        sequence
    ));
    fs::hard_link(target, &recovery)?;

    match api.replace(target, temporary, &backup) {
        Ok(()) => {
            if backup.exists() {
                let _ = fs::remove_file(&backup);
            }
            let _ = fs::remove_file(&recovery);
            Ok(())
        }
        Err(replace_error) => {
            if !target.exists() {
                let restore_result = if backup.exists() {
                    fs::rename(&backup, target)
                } else {
                    fs::copy(&recovery, target).map(|_| ())
                };
                if let Err(restore_error) = restore_result {
                    return Err(io::Error::new(
                        restore_error.kind(),
                        format!("replacement failed ({replace_error}); backup restore failed ({restore_error})"),
                    ));
                }
            }
            Err(replace_error)
        }
    }
}

#[cfg(not(windows))]
fn sync_parent(target: &Path) -> Result<(), ConfigStoreError> {
    use std::fs::File;

    let parent = target
        .parent()
        .ok_or_else(|| ConfigStoreError::Validation("configuration path has no parent".into()))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ConfigStoreError::Io {
            operation: "sync directory",
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(windows)]
fn sync_parent(_target: &Path) -> Result<(), ConfigStoreError> {
    // ReplaceFileW completes the same-volume atomic replacement. Windows does not expose
    // directory handles through std::fs::File, while the temporary file itself is synced above.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    fn runtime() -> RuntimeOwner {
        RuntimeOwner::plan(RuntimeConfig::for_parallelism("gpui-config-test", 1))
            .expect("test runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory limit"))
            .build()
            .expect("test runtime")
    }

    #[test]
    fn load_save_reload_advances_revision_and_never_serializes_secrets() {
        let directory = tempfile::tempdir().expect("temp directory");
        let runtime = runtime();
        let store = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("config"),
        );
        runtime.block_on(async {
            let mut config = store.load().await.expect("default config");
            config.nameservers.push("LOCALHOST:9876".into());
            config.current_nameserver = Some("localhost:9876".into());
            config.auth.enabled = true;
            config.auth.credential_source = CredentialSourceKind::Environment;
            let saved = store.save_next(config).await.expect("save");
            assert_eq!(saved.revision, 1);
            let reloaded = store.load().await.expect("reload");
            assert_eq!(reloaded, saved);
        });
        let persisted = fs::read_to_string(store.path()).expect("persisted config");
        for forbidden in ["password", "accessKey", "secretKey", "securityToken", "sessionId"] {
            assert!(!persisted.contains(forbidden));
        }
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn invalid_and_unknown_schema_files_are_recoverable_and_protected() {
        for contents in ["{broken", r#"{"schemaVersion":99,"revision":4}"#] {
            let directory = tempfile::tempdir().expect("temp directory");
            let path = directory.path().join("config.json");
            fs::write(&path, contents).expect("fixture");
            let runtime = runtime();
            let store = DesktopConfigStore::new(path.clone(), runtime.root_context().component("config"));
            runtime.block_on(async {
                assert!(store.load().await.is_err());
                assert!(matches!(
                    store.save_next(DesktopConfig::default()).await,
                    Err(ConfigStoreError::ProtectedOriginal)
                ));
            });
            assert_eq!(fs::read_to_string(path).expect("protected original"), contents);
            runtime.shutdown_runtime_blocking().expect("shutdown");
        }
    }

    #[test]
    fn stale_revision_cannot_overwrite_a_newer_save() {
        let directory = tempfile::tempdir().expect("temp directory");
        let runtime = runtime();
        let store = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("config"),
        );
        runtime.block_on(async {
            let original = store.load().await.expect("load");
            let saved = store.save_next(original.clone()).await.expect("first save");
            assert_eq!(saved.revision, 1);
            assert!(matches!(
                store.save_next(original).await,
                Err(ConfigStoreError::StaleRevision)
            ));
        });
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[cfg(windows)]
    #[test]
    fn windows_existing_target_is_replaced_atomically() {
        let directory = tempfile::tempdir().expect("temp directory");
        let target = directory.path().join("config.json");
        let replacement = directory.path().join("replacement.tmp");
        fs::write(&target, b"old").expect("old target");
        fs::write(&replacement, b"new").expect("new replacement");

        atomic_replace(&replacement, &target).expect("atomic replacement");

        assert_eq!(fs::read(&target).expect("target"), b"new");
        assert!(!replacement.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_move_replacement_failure_restores_original_and_preserves_replacement() {
        struct MoveReplacementFailure;

        impl WindowsReplace for MoveReplacementFailure {
            fn replace(&self, target: &Path, _replacement: &Path, backup: &Path) -> io::Result<()> {
                fs::rename(target, backup)?;
                Err(io::Error::from_raw_os_error(1177))
            }
        }

        let directory = tempfile::tempdir().expect("temp directory");
        let target = directory.path().join("config.json");
        let replacement = directory.path().join("replacement.tmp");
        fs::write(&target, b"old").expect("old target");
        fs::write(&replacement, b"new").expect("new replacement");

        let error = atomic_replace_with(&replacement, &target, &MoveReplacementFailure)
            .expect_err("injected replacement failure");

        assert_eq!(error.raw_os_error(), Some(1177));
        assert_eq!(fs::read(&target).expect("restored target"), b"old");
        assert_eq!(fs::read(&replacement).expect("preserved replacement"), b"new");
    }

    #[cfg(windows)]
    #[test]
    fn windows_remove_replaced_failure_retains_both_original_names() {
        struct RemoveReplacedFailure;

        impl WindowsReplace for RemoveReplacedFailure {
            fn replace(&self, _target: &Path, _replacement: &Path, _backup: &Path) -> io::Result<()> {
                Err(io::Error::from_raw_os_error(1175))
            }
        }

        assert_windows_replacement_failure_preserves_files(&RemoveReplacedFailure, 1175);
    }

    #[cfg(windows)]
    #[test]
    fn windows_move_replacement_failure_restores_from_pre_replacement_recovery() {
        struct MoveReplacementFailure;

        impl WindowsReplace for MoveReplacementFailure {
            fn replace(&self, target: &Path, _replacement: &Path, _backup: &Path) -> io::Result<()> {
                fs::remove_file(target)?;
                Err(io::Error::from_raw_os_error(1176))
            }
        }

        assert_windows_replacement_failure_preserves_files(&MoveReplacementFailure, 1176);
    }

    #[cfg(windows)]
    fn assert_windows_replacement_failure_preserves_files(api: &dyn WindowsReplace, code: i32) {
        let directory = tempfile::tempdir().expect("temp directory");
        let target = directory.path().join("config.json");
        let replacement = directory.path().join("replacement.tmp");
        fs::write(&target, b"old").expect("old target");
        fs::write(&replacement, b"new").expect("new replacement");

        let error = atomic_replace_with(&replacement, &target, api).expect_err("injected replacement failure");

        assert_eq!(error.raw_os_error(), Some(code));
        assert_eq!(fs::read(&target).expect("retained/restored target"), b"old");
        assert_eq!(fs::read(&replacement).expect("preserved replacement"), b"new");
    }
}
