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
    error::Error as StdError,
    fmt, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use rocketmq_dashboard_common::{
    ConnectionScope, ConnectionSnapshot, CredentialSourceKind, EnvironmentConnectionSnapshot, OperationActor,
    OperationContext, OperationResource, TransportSettings, normalize_nameserver_selection, normalize_proxy_selection,
};
use rocketmq_runtime::ChildServiceContext;
use serde::{Deserialize, Serialize};

pub(super) use super::atomic_file::write_json_atomically;

use super::{
    environment_store::{
        DesktopDocument, EnvironmentCatalog, EnvironmentChange, EnvironmentProfile, EnvironmentValidation,
        LocalOperationKind, LocalOperationRecord, StoredMonitorRule, load_document, new_environment_id,
    },
    storage_writer::StorageWriter,
};

/// Current on-disk schema understood by this delivery.
pub const CONFIG_SCHEMA_VERSION: u32 = 4;

#[path = "monitor_commit.rs"]
mod monitor_commit;
/// Environment override used by tests, packaged builds, and portable deployments.
pub const CONFIG_PATH_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_CONFIG_PATH";

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
    pub fn normalize(mut self) -> Result<Self, ConfigStoreFailure> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigStoreFailure::unsupported_schema(
                self.schema_version,
                CONFIG_SCHEMA_VERSION,
            ));
        }
        (self.nameservers, self.current_nameserver) =
            normalize_nameserver_selection(&self.nameservers, self.current_nameserver.as_deref())
                .map_err(ConfigStoreFailure::validation)?;
        (self.proxies, self.current_proxy) = normalize_proxy_selection(&self.proxies, self.current_proxy.as_deref())
            .map_err(ConfigStoreFailure::validation)?;
        if self.scope == ConnectionScope::Proxy && self.current_proxy.is_none() {
            return Err(ConfigStoreFailure::validation(ConfigValidation::ProxyRequiresSelection));
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

/// Opaque recoverable storage failure with a fixed safe projection.
#[derive(Clone)]
pub(crate) struct ConfigStoreFailure {
    kind: ConfigStoreFailureKind,
}

#[derive(Clone)]
enum ConfigStoreFailureKind {
    Rejected(ConfigStoreRejection),
    Operational(ConfigStoreError),
}

#[derive(Clone, Debug)]
enum ConfigStoreRejection {
    UnsupportedSchema { found: u32, supported: u32 },
    StaleRevision,
    RevisionExhausted,
    ProtectedOriginal,
    WriterUnavailable,
}

#[derive(Clone)]
pub(crate) struct ConfigStoreError {
    inner: Arc<ConfigStoreErrorInner>,
}

struct ConfigStoreErrorInner {
    condition: ConfigStoreCondition,
    source: Arc<dyn StdError + Send + Sync>,
    // Retained for recovery and diagnostics, but never exposed by Display or Debug.
    path: Option<PathBuf>,
    operation: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConfigStoreCondition {
    Io,
    InvalidDocument,
    Validation,
    Runtime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConfigStoreFailureCode {
    Io,
    InvalidDocument,
    UnsupportedSchema,
    Validation,
    StaleRevision,
    RevisionExhausted,
    ProtectedOriginal,
    Runtime,
    WriterUnavailable,
}

#[derive(Debug, thiserror::Error)]
enum ConfigValidation {
    #[error("the user configuration directory is unavailable")]
    UserDirectoryUnavailable,
    #[error("Proxy scope requires a selected Proxy endpoint")]
    ProxyRequiresSelection,
}

impl ConfigStoreFailure {
    fn rejected(rejection: ConfigStoreRejection) -> Self {
        Self {
            kind: ConfigStoreFailureKind::Rejected(rejection),
        }
    }

    fn operational(inner: ConfigStoreErrorInner) -> Self {
        Self {
            kind: ConfigStoreFailureKind::Operational(ConfigStoreError { inner: Arc::new(inner) }),
        }
    }

    fn caused_by(
        condition: ConfigStoreCondition,
        path: Option<PathBuf>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::operational(ConfigStoreErrorInner {
            condition,
            source: Arc::new(source),
            path,
            operation: None,
        })
    }

    pub(super) fn io(operation: &'static str, path: PathBuf, source: io::Error) -> Self {
        Self::operational(ConfigStoreErrorInner {
            condition: ConfigStoreCondition::Io,
            source: Arc::new(source),
            path: Some(path),
            operation: Some(operation),
        })
    }

    pub(super) fn invalid_document(path: PathBuf, source: serde_json::Error) -> Self {
        Self::caused_by(ConfigStoreCondition::InvalidDocument, Some(path), source)
    }

    pub(super) fn validation(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::caused_by(ConfigStoreCondition::Validation, None, source)
    }

    pub(super) fn runtime(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::caused_by(ConfigStoreCondition::Runtime, None, source)
    }

    fn code(&self) -> ConfigStoreFailureCode {
        match &self.kind {
            ConfigStoreFailureKind::Operational(error) => match error.inner.condition {
                ConfigStoreCondition::Io => ConfigStoreFailureCode::Io,
                ConfigStoreCondition::InvalidDocument => ConfigStoreFailureCode::InvalidDocument,
                ConfigStoreCondition::Validation => ConfigStoreFailureCode::Validation,
                ConfigStoreCondition::Runtime => ConfigStoreFailureCode::Runtime,
            },
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::UnsupportedSchema { .. }) => {
                ConfigStoreFailureCode::UnsupportedSchema
            }
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::StaleRevision) => {
                ConfigStoreFailureCode::StaleRevision
            }
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::RevisionExhausted) => {
                ConfigStoreFailureCode::RevisionExhausted
            }
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::ProtectedOriginal) => {
                ConfigStoreFailureCode::ProtectedOriginal
            }
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::WriterUnavailable) => {
                ConfigStoreFailureCode::WriterUnavailable
            }
        }
    }

    pub(super) fn unsupported_schema(found: u32, supported: u32) -> Self {
        Self::rejected(ConfigStoreRejection::UnsupportedSchema { found, supported })
    }

    pub(super) fn writer_unavailable() -> Self {
        Self::rejected(ConfigStoreRejection::WriterUnavailable)
    }

    pub(super) fn stale_revision() -> Self {
        Self::rejected(ConfigStoreRejection::StaleRevision)
    }

    fn protects_original(&self) -> bool {
        matches!(
            &self.kind,
            ConfigStoreFailureKind::Operational(error)
                if error.inner.condition == ConfigStoreCondition::InvalidDocument
        ) || matches!(
            &self.kind,
            ConfigStoreFailureKind::Rejected(
                ConfigStoreRejection::UnsupportedSchema { .. } | ConfigStoreRejection::WriterUnavailable
            )
        )
    }

    pub(crate) fn into_operational(self) -> Option<ConfigStoreError> {
        match self.kind {
            ConfigStoreFailureKind::Operational(error) => Some(error),
            ConfigStoreFailureKind::Rejected(_) => None,
        }
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match &self.kind {
            ConfigStoreFailureKind::Rejected(
                ConfigStoreRejection::StaleRevision
                | ConfigStoreRejection::ProtectedOriginal
                | ConfigStoreRejection::WriterUnavailable,
            ) => true,
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::UnsupportedSchema { .. })
            | ConfigStoreFailureKind::Rejected(ConfigStoreRejection::RevisionExhausted) => false,
            ConfigStoreFailureKind::Operational(_) => true,
        }
    }

    pub(crate) fn safe_summary(&self) -> &'static str {
        match &self.kind {
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::WriterUnavailable) => {
                "Another Dashboard instance owns this data directory. Close it before opening this instance."
            }
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::UnsupportedSchema { .. }) => {
                "The local dashboard data uses an unsupported schema version. The original files were preserved."
            }
            _ => "The local dashboard configuration could not be loaded or updated.",
        }
    }
}

impl fmt::Debug for ConfigStoreFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigStoreFailure")
            .field("code", &self.code())
            .field(
                "operational",
                &matches!(&self.kind, ConfigStoreFailureKind::Operational(_)),
            )
            .field(
                "schema_available",
                &matches!(
                    &self.kind,
                    ConfigStoreFailureKind::Rejected(ConfigStoreRejection::UnsupportedSchema {
                        found,
                        supported,
                    }) if *found > 0 || *supported > 0
                ),
            )
            .finish()
    }
}

impl fmt::Debug for ConfigStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigStoreError")
            .field("condition", &self.inner.condition)
            .field("path_available", &self.inner.path.is_some())
            .field("operation_available", &self.inner.operation.is_some())
            .finish()
    }
}

impl fmt::Display for ConfigStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.inner.condition {
            ConfigStoreCondition::Io => "The local dashboard data could not be read or written.",
            ConfigStoreCondition::InvalidDocument => "The local dashboard data is not valid JSON.",
            ConfigStoreCondition::Validation => "The local dashboard data failed validation.",
            ConfigStoreCondition::Runtime => "The local dashboard storage runtime is unavailable.",
        })
    }
}

impl StdError for ConfigStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.source.as_ref())
    }
}

/// File-backed store with serialized updates and explicit corrupt-original protection.
pub struct DesktopConfigStore {
    path: PathBuf,
    context: ChildServiceContext,
    io_gate: tokio::sync::Mutex<()>,
    protected_original: AtomicBool,
    writer: Arc<parking_lot::Mutex<Option<StorageWriter>>>,
    document: parking_lot::RwLock<Option<DesktopDocument>>,
}

impl DesktopConfigStore {
    /// Resolves the configured path, preferring [`CONFIG_PATH_ENV`].
    pub fn from_environment(context: ChildServiceContext) -> Result<Arc<Self>, ConfigStoreFailure> {
        let path = match std::env::var_os(CONFIG_PATH_ENV) {
            Some(path) if !path.is_empty() => PathBuf::from(path),
            _ => dirs::config_dir()
                .ok_or_else(|| ConfigStoreFailure::validation(ConfigValidation::UserDirectoryUnavailable))?
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
            protected_original: AtomicBool::new(false),
            writer: Arc::new(parking_lot::Mutex::new(None)),
            document: parking_lot::RwLock::new(None),
        })
    }

    /// Returns the exact non-secret storage path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn authentication_enabled(&self) -> Result<bool, ConfigStoreFailure> {
        self.document
            .read()
            .as_ref()
            .map(|document| document.settings.authentication_enabled)
            .ok_or_else(|| ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile))
    }

    pub(super) fn audit_snapshot(&self) -> Result<super::audit_store::AuditJournal, ConfigStoreFailure> {
        self.document
            .read()
            .as_ref()
            .map(|document| document.audit.clone())
            .ok_or_else(|| ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile))
    }

    pub(super) async fn update_audit(
        &self,
        name: &'static str,
        update: impl FnOnce(&mut super::audit_store::AuditJournal) -> Result<(), ConfigStoreFailure> + Send + 'static,
    ) -> Result<(), ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        if self.protected_original.load(Ordering::Acquire) || self.document.read().is_none() {
            return Err(ConfigStoreFailure::writer_unavailable());
        }
        let path = self.path.clone();
        let document = self
            .context
            .storage_io()
            .spawn_io(name, move || {
                let (mut document, _) = load_document(&path)?;
                let before = document.audit.clone();
                update(&mut document.audit)?;
                document.validate()?;
                if document.audit != before {
                    write_json_atomically(&path, &document)?;
                }
                Ok::<_, ConfigStoreFailure>(document)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)??;
        *self.document.write() = Some(document);
        Ok(())
    }

    /// Loads, parses, validates, and records the on-disk revision.
    pub async fn load(&self) -> Result<DesktopConfig, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let path = self.path.clone();
        let writer = Arc::clone(&self.writer);
        let result = self
            .context
            .storage_io()
            .spawn_io("gpui-config-load", move || {
                let mut writer = writer.lock();
                if writer.is_none() {
                    *writer = Some(StorageWriter::acquire(&path)?);
                }
                drop(writer);
                let (document, migrated) = load_document(&path)?;
                document.validate()?;
                if migrated {
                    write_json_atomically(&path, &document)?;
                }
                Ok::<_, ConfigStoreFailure>(document)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)?;
        match result {
            Ok(document) => {
                let config = document.config()?;

                self.protected_original.store(false, Ordering::Release);
                *self.document.write() = Some(document);
                Ok(config)
            }
            Err(error) if error.protects_original() => {
                self.protected_original.store(true, Ordering::Release);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically persists a validated next revision.
    pub async fn save_next(&self, config: DesktopConfig) -> Result<DesktopConfig, ConfigStoreFailure> {
        if self.protected_original.load(Ordering::Acquire) {
            return Err(ConfigStoreFailure::rejected(ConfigStoreRejection::ProtectedOriginal));
        }
        let context = self.operation_context(
            config.revision,
            OperationActor::System,
            OperationResource::Configuration,
        )?;
        self.save_next_with_context(config, context).await
    }

    /// Saves the active profile and its accepted-operation record in one replacement.
    pub async fn save_next_with_context(
        &self,
        config: DesktopConfig,
        context: OperationContext,
    ) -> Result<DesktopConfig, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        if self.protected_original.load(Ordering::Acquire) {
            return Err(ConfigStoreFailure::rejected(ConfigStoreRejection::ProtectedOriginal));
        }
        let config = config.normalize()?;
        let path = self.path.clone();
        let document = self
            .context
            .storage_io()
            .spawn_io("gpui-config-save", move || {
                let (mut document, _) = load_document(&path)?;
                if document
                    .local_operations
                    .iter()
                    .any(|record| record.context.request_id == context.request_id)
                {
                    return Ok(document);
                }
                if config.revision != document.settings.revision
                    || context.configuration_revision != config.revision
                    || context.environment_id != document.active_environment_id
                {
                    return Err(ConfigStoreFailure::stale_revision());
                }
                let mut config = config;
                config.revision = config
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| ConfigStoreFailure::rejected(ConfigStoreRejection::RevisionExhausted))?;
                document.apply_config(&config)?;
                persist_local_document(&path, document, context, LocalOperationKind::ConfigurationSaved)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)??;
        let config = document.config()?;

        *self.document.write() = Some(document);
        Ok(config)
    }

    /// Returns profiles from the same committed document as the configuration projection.
    pub fn environments(&self) -> Option<EnvironmentCatalog> {
        self.document.read().as_ref().map(DesktopDocument::catalog)
    }

    pub fn history_settings(&self) -> Option<FoundationFlags> {
        self.document
            .read()
            .as_ref()
            .and_then(|document| document.config().ok())
            .map(|config| config.foundations)
    }

    pub fn environment_snapshot(&self) -> Result<EnvironmentConnectionSnapshot, ConfigStoreFailure> {
        let document = self.document.read();
        let document = document
            .as_ref()
            .ok_or_else(|| ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile))?;
        Ok(EnvironmentConnectionSnapshot {
            environment_id: document.active_environment_id.clone(),
            connection: document.config()?.connection_snapshot(),
        })
    }

    /// Resolves attribution at acceptance; caller-supplied form data cannot choose an actor.
    pub fn operation_context(
        &self,
        revision: u64,
        actor: OperationActor,
        resource: OperationResource,
    ) -> Result<OperationContext, ConfigStoreFailure> {
        let snapshot = self.environment_snapshot()?;
        if snapshot.connection.revision != revision {
            return Err(ConfigStoreFailure::stale_revision());
        }
        Ok(OperationContext {
            environment_id: snapshot.environment_id,
            configuration_revision: revision,
            request_id: format!("request-{:032x}", rand::random::<u128>()),
            actor,
            resource,
        })
    }

    /// Creates an empty profile or selects a profile explicitly. Endpoint edits never call this.
    pub async fn change_environment(
        &self,
        change: EnvironmentChange,
        context: OperationContext,
    ) -> Result<DesktopConfig, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let path = self.path.clone();
        let document = self
            .context
            .storage_io()
            .spawn_io("gpui-environment-change", move || {
                let (mut document, _) = load_document(&path)?;
                if document
                    .local_operations
                    .iter()
                    .any(|record| record.context.request_id == context.request_id)
                {
                    return Ok(document);
                }
                if context.configuration_revision != document.settings.revision
                    || context.environment_id != document.active_environment_id
                {
                    return Err(ConfigStoreFailure::stale_revision());
                }
                match change {
                    EnvironmentChange::Create { name } => {
                        if name.trim().is_empty() {
                            return Err(ConfigStoreFailure::validation(EnvironmentValidation::InvalidProfile));
                        }
                        let id = new_environment_id()?;
                        document.profiles.push(EnvironmentProfile::from_config(
                            id.clone(),
                            name.trim().into(),
                            &DesktopConfig::default(),
                        ));
                        document.active_environment_id = id;
                    }
                    EnvironmentChange::Select { id: selected } => {
                        if !document.profiles.iter().any(|profile| profile.id == selected) {
                            return Err(ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile));
                        }
                        document.active_environment_id = selected;
                    }
                }
                document.settings.revision = document
                    .settings
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| ConfigStoreFailure::rejected(ConfigStoreRejection::RevisionExhausted))?;
                persist_local_document(&path, document, context, LocalOperationKind::ConfigurationSaved)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)??;
        let config = document.config()?;

        *self.document.write() = Some(document);
        Ok(config)
    }

    pub async fn monitor_rules(&self) -> Result<Vec<StoredMonitorRule>, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-monitor-list", move || {
                load_document(&path).map(|(document, _)| document.monitors.rules)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)?
    }

    /// Commits a Monitor edit and its operation record under the configuration writer lock.
    #[cfg(test)]
    pub async fn save_monitor(
        &self,
        rule: Option<StoredMonitorRule>,
        id: String,
        context: OperationContext,
    ) -> Result<bool, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let path = self.path.clone();
        let (document, removed) = self
            .context
            .storage_io()
            .spawn_io("gpui-monitor-commit", move || {
                let (mut document, _) = load_document(&path)?;
                if document
                    .local_operations
                    .iter()
                    .any(|record| record.context.request_id == context.request_id)
                {
                    return Ok((document, false));
                }
                if context.configuration_revision != document.settings.revision
                    || context.environment_id != document.active_environment_id
                {
                    return Err(ConfigStoreFailure::stale_revision());
                }
                let old_len = document.monitors.rules.len();
                document.monitors.rules.retain(|existing| {
                    !(existing.environment_id.as_ref() == Some(&context.environment_id) && existing.rule.id == id)
                });
                let removed = old_len != document.monitors.rules.len();
                let kind = if let Some(rule) = rule {
                    document.monitors.rules.push(rule);
                    LocalOperationKind::MonitorSaved
                } else {
                    LocalOperationKind::MonitorDeleted
                };
                let document = persist_local_document(&path, document, context, kind)?;
                Ok::<_, ConfigStoreFailure>((document, removed))
            })
            .await
            .map_err(ConfigStoreFailure::runtime)??;
        *self.document.write() = Some(document);
        Ok(removed)
    }
}

fn persist_local_document(
    path: &Path,
    mut document: DesktopDocument,
    context: OperationContext,
    kind: LocalOperationKind,
) -> Result<DesktopDocument, ConfigStoreFailure> {
    let committed_at_epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(ConfigStoreFailure::validation)?;
    let committed_at_epoch_ms =
        u64::try_from(committed_at_epoch_ms.as_millis()).map_err(ConfigStoreFailure::validation)?;
    document.local_operations.push(LocalOperationRecord {
        context: context.clone(),
        kind,
        committed_at_epoch_ms,
    });
    let action = match kind {
        LocalOperationKind::ConfigurationSaved => rocketmq_dashboard_common::AuditAction::ConfigurationSaved,
        LocalOperationKind::MonitorSaved => rocketmq_dashboard_common::AuditAction::MonitorSaved,
        LocalOperationKind::MonitorDeleted => rocketmq_dashboard_common::AuditAction::MonitorDeleted,
    };
    document.audit.finish_local(context, action, committed_at_epoch_ms)?;
    if document.local_operations.len() > 10_000 {
        document
            .local_operations
            .drain(..document.local_operations.len() - 10_000);
    }
    document.validate()?;
    write_json_atomically(path, &document)?;
    Ok(document)
}

#[cfg(test)]
mod tests {
    use std::fs;

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
                let error = store
                    .save_next(DesktopConfig::default())
                    .await
                    .expect_err("protected original");
                assert_eq!(error.code(), ConfigStoreFailureCode::ProtectedOriginal);
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
            let error = store.save_next(original).await.expect_err("stale revision");
            assert_eq!(error.code(), ConfigStoreFailureCode::StaleRevision);
        });
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn storage_error_retains_typed_source_and_redacts_path_and_source_text() {
        let path = PathBuf::from("C:/private/token/config.json");
        let failure = ConfigStoreFailure::io(
            "read",
            path,
            io::Error::new(io::ErrorKind::PermissionDenied, "password=private-password"),
        );
        let error = failure.into_operational().expect("I/O is operational");

        assert!(
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<io::Error>())
                .is_some()
        );
        for projection in [format!("{error}"), format!("{error:?}")] {
            assert!(!projection.contains("private/token"));
            assert!(!projection.contains("private-password"));
        }
    }

    #[test]
    fn malformed_document_retains_serde_source_while_schema_rejection_is_non_operational() {
        let serde_source =
            serde_json::from_slice::<DesktopConfig>(br#"{"password":"secret""#).expect_err("invalid JSON");
        let failure = ConfigStoreFailure::invalid_document(PathBuf::from("secret.json"), serde_source);
        let error = failure.into_operational().expect("parse failure is operational");
        assert!(
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<serde_json::Error>())
                .is_some()
        );

        let schema = ConfigStoreFailure::unsupported_schema(99, CONFIG_SCHEMA_VERSION);
        assert!(schema.into_operational().is_none());
    }
}
