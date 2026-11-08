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

//! Safe native storage failures and compatibility re-exports.

#[cfg(test)]
use std::io;
use std::{error::Error as StdError, fmt, path::PathBuf, sync::Arc};

#[cfg(test)]
use rocketmq_dashboard_common::CredentialSourceKind;
pub use rocketmq_dashboard_storage::{
    AuthConfig, CONFIG_SCHEMA_VERSION, DesktopConfig, FoundationFlags, HISTORY_MAX_POINTS_CAP, HISTORY_MAX_SERIES_CAP,
    HISTORY_MAX_TOTAL_POINTS_CAP,
};
use rocketmq_dashboard_storage::{StorageError, StorageErrorKind};

#[path = "config_authority.rs"]
mod authority;
pub use authority::DesktopConfigStore;
pub(crate) use authority::NativeConfigurationChange;
pub(crate) use authority::{NativeStorageSetup, SelectionSource, WorkspaceSetupAction};

/// Existing portable configuration path override.
pub const CONFIG_PATH_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_CONFIG_PATH";
/// Explicit native authority, defaulting to file when no choice is supplied.
pub const STORAGE_ENGINE_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_STORAGE_ENGINE";
/// External database connection input. Its value is never persisted or formatted.
pub const DATABASE_URL_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_DATABASE_URL";

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
    Uninitialized,
    Closed,
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
    Storage,
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
    Storage,
    Uninitialized,
}

#[derive(Debug, thiserror::Error)]
enum ConfigValidation {
    #[error("the user configuration directory is unavailable")]
    UserDirectoryUnavailable,
    #[error("select file, sqlite, mysql, or postgres as the native storage engine")]
    StorageEngine,
    #[error("external database connection configuration is required for the selected engine")]
    DatabaseConnection,
    #[error("the external storage engine override must be changed before saving a local choice")]
    StorageEngineOverride,
    #[error("the selected engine was not compiled into this application")]
    StorageDriverUnavailable,
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

    #[cfg(test)]
    pub(super) fn io(operation: &'static str, path: PathBuf, source: io::Error) -> Self {
        Self::operational(ConfigStoreErrorInner {
            condition: ConfigStoreCondition::Io,
            source: Arc::new(source),
            path: Some(path),
            operation: Some(operation),
        })
    }

    #[cfg(test)]
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
                ConfigStoreCondition::Storage => ConfigStoreFailureCode::Storage,
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
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::Uninitialized) => {
                ConfigStoreFailureCode::Uninitialized
            }
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::Closed) => ConfigStoreFailureCode::Storage,
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
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::Uninitialized) => true,
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::UnsupportedSchema { .. })
            | ConfigStoreFailureKind::Rejected(ConfigStoreRejection::RevisionExhausted) => false,
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::Closed) => false,
            ConfigStoreFailureKind::Operational(_) => true,
        }
    }

    pub(crate) fn safe_summary(&self) -> &'static str {
        match &self.kind {
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::Closed) => "The selected native storage is closed.",
            ConfigStoreFailureKind::Rejected(ConfigStoreRejection::Uninitialized) => {
                "The selected database has no native Dashboard data. Initialize it or import existing files explicitly."
            }
            ConfigStoreFailureKind::Operational(error) if error.inner.condition == ConfigStoreCondition::Storage => {
                match error
                    .inner
                    .source
                    .downcast_ref::<StorageError>()
                    .map(StorageError::kind)
                {
                    Some(StorageErrorKind::DriverUnavailable { .. }) => {
                        "The selected native storage engine is unavailable in this application build."
                    }
                    Some(StorageErrorKind::Connection | StorageErrorKind::Query | StorageErrorKind::Closed) => {
                        "The selected native database is unavailable. Check its external connection configuration and retry."
                    }
                    Some(StorageErrorKind::LeaseLost) => {
                        "History collection ownership changed; the sample was not persisted."
                    }
                    Some(StorageErrorKind::EmptySource) => "The import source has no native Dashboard configuration.",
                    Some(StorageErrorKind::AlreadyInitialized) => {
                        "The selected storage already contains native Dashboard data."
                    }
                    _ => "The selected native storage could not complete the operation.",
                }
            }
            ConfigStoreFailureKind::Operational(error) if error.inner.condition == ConfigStoreCondition::Validation => {
                match error.inner.source.downcast_ref::<ConfigValidation>() {
                    Some(ConfigValidation::StorageEngine) => {
                        "Select file, sqlite, mysql, or postgres as the native storage engine."
                    }
                    Some(ConfigValidation::DatabaseConnection) => {
                        "External database connection configuration is required for the selected engine."
                    }
                    Some(ConfigValidation::StorageEngineOverride) => {
                        "The engine is controlled by ROCKETMQ_DASHBOARD_GPUI_STORAGE_ENGINE. Change or remove that external override and restart."
                    }
                    Some(ConfigValidation::StorageDriverUnavailable) => {
                        "The selected native storage engine is unavailable in this application build."
                    }
                    _ => "The local dashboard configuration could not be loaded or updated.",
                }
            }
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

impl fmt::Display for ConfigStoreFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_summary())
    }
}

impl StdError for ConfigStoreFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.kind {
            ConfigStoreFailureKind::Operational(error) => Some(error),
            ConfigStoreFailureKind::Rejected(_) => None,
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
            ConfigStoreCondition::Storage => "The selected native Dashboard storage is unavailable.",
        })
    }
}

impl StdError for ConfigStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.source.as_ref())
    }
}

impl From<StorageError> for ConfigStoreFailure {
    fn from(error: StorageError) -> Self {
        match error.kind() {
            StorageErrorKind::UnsupportedSchema { found, supported } => Self::unsupported_schema(found, supported),
            StorageErrorKind::Conflict => Self::stale_revision(),
            StorageErrorKind::WriterUnavailable => Self::writer_unavailable(),
            StorageErrorKind::Uninitialized => Self::rejected(ConfigStoreRejection::Uninitialized),
            StorageErrorKind::RevisionExhausted => Self::rejected(ConfigStoreRejection::RevisionExhausted),
            StorageErrorKind::InvalidDocument => Self::caused_by(ConfigStoreCondition::InvalidDocument, None, error),
            StorageErrorKind::Io => Self::caused_by(ConfigStoreCondition::Io, None, error),
            StorageErrorKind::Validation => Self::validation(error),
            StorageErrorKind::Runtime => Self::runtime(error),
            StorageErrorKind::DriverUnavailable { .. }
            | StorageErrorKind::Connection
            | StorageErrorKind::Query
            | StorageErrorKind::EmptySource
            | StorageErrorKind::AlreadyInitialized
            | StorageErrorKind::LeaseLost
            | StorageErrorKind::Closed => Self::caused_by(ConfigStoreCondition::Storage, None, error),
        }
    }
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
    fn injected_native_authority_is_the_same_handle_and_never_opens_a_second_writer() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let path = directory.path().join("config.json");
        let context = runtime.root_context().component("native");
        runtime.block_on(async {
            let native = Arc::new(
                rocketmq_dashboard_storage::DashboardStore::open(
                    rocketmq_dashboard_storage::StorageOptions::File {
                        config_path: path.clone(),
                    },
                    context.clone(),
                )
                .await
                .unwrap(),
            );
            let store = DesktopConfigStore::with_native_storage(path, context, Arc::clone(&native));
            store.load().await.unwrap();
            assert!(Arc::ptr_eq(&native, &store.native_storage().await.unwrap()));
            assert!(store.owns_writer());
            store.close().await.unwrap();
            assert!(!store.owns_writer());
            assert!(store.load().await.is_err());
            assert_eq!(native.status().await.unwrap_err().kind(), StorageErrorKind::Closed);
        });
        runtime.shutdown_runtime_blocking().unwrap();
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
