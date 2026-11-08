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

//! One selected native authority, with a safe synchronous projection for existing views.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use rocketmq_dashboard_common::{
    AuditAcceptedOperation, EnvironmentConnectionSnapshot, OperationActor, OperationContext, OperationResource,
};
use rocketmq_dashboard_storage::{
    DashboardStore, DatabaseConnection, DesktopSnapshot, StorageEngine, StorageIoOperation, StorageOptions,
};
use rocketmq_runtime::ChildServiceContext;

use super::{
    CONFIG_PATH_ENV, CONFIG_SCHEMA_VERSION, ConfigStoreFailure, ConfigStoreRejection, ConfigValidation,
    DATABASE_URL_ENV, DesktopConfig, FoundationFlags, STORAGE_ENGINE_ENV,
};
use crate::infrastructure::{
    audit_store::AuditJournal,
    environment_store::{
        DesktopDocument, EnvironmentCatalog, EnvironmentChange, EnvironmentProfile, EnvironmentValidation,
        LocalOperationKind, LocalOperationRecord, StoredMonitorRule, new_environment_id,
    },
    local_storage_status::{FileIoEvidence, FileReadState, LocalFileStatus, inspect_json},
};

#[path = "monitor_commit.rs"]
mod monitor_commit;
#[path = "storage_setup.rs"]
mod storage_setup;
pub(crate) use storage_setup::{NativeStorageSetup, SelectionSource, WorkspaceSetupAction};

/// File compatibility constructors and SQL clients share this exact opened handle.
pub struct DesktopConfigStore {
    path: PathBuf,
    history_path: Option<PathBuf>,
    context: ChildServiceContext,
    from_environment: bool,
    engine_override: Result<Option<StorageEngine>, ConfigStoreFailure>,
    native: tokio::sync::OnceCell<Arc<DashboardStore>>,
    selection: tokio::sync::OnceCell<storage_setup::FrozenSelection>,
    io_gate: tokio::sync::Mutex<()>,
    protected_original: AtomicBool,
    closed: AtomicBool,
    document: parking_lot::RwLock<Option<DesktopDocument>>,
    io_evidence: Arc<parking_lot::RwLock<FileIoEvidence>>,
    configuration_changes: tokio::sync::watch::Sender<Option<NativeConfigurationChange>>,
}

/// Only configuration identity drives provider/view invalidation; journal traffic does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeConfigurationChange {
    pub revision: u64,
    pub environment_id: rocketmq_dashboard_common::EnvironmentId,
}

impl DesktopConfigStore {
    pub fn from_environment(context: ChildServiceContext) -> Result<Arc<Self>, ConfigStoreFailure> {
        let path = match std::env::var_os(CONFIG_PATH_ENV) {
            Some(path) if !path.is_empty() => PathBuf::from(path),
            _ => dirs::config_dir()
                .ok_or_else(|| ConfigStoreFailure::validation(ConfigValidation::UserDirectoryUnavailable))?
                .join("rocketmq-dashboard")
                .join("gpui")
                .join("config.json"),
        };
        Ok(Self::compose(path, context, true, None))
    }

    /// Keeps injected file paths deterministic without reading process-wide engine variables.
    #[cfg(test)]
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Self::compose(path, context, false, None)
    }

    /// Wraps an already opened authority, including independent SQL clients in native tests.
    #[cfg(test)]
    pub(crate) fn with_native_storage(
        path: PathBuf,
        context: ChildServiceContext,
        storage: Arc<DashboardStore>,
    ) -> Arc<Self> {
        Self::compose(path, context, false, Some(storage))
    }

    #[cfg(test)]
    pub(in crate::infrastructure) fn with_history_path(
        path: PathBuf,
        history_path: PathBuf,
        context: ChildServiceContext,
    ) -> Arc<Self> {
        let mut store = Self::fields(path, context, false, None);
        store.history_path = Some(history_path);
        Arc::new(store)
    }

    fn compose(
        path: PathBuf,
        context: ChildServiceContext,
        from_environment: bool,
        storage: Option<Arc<DashboardStore>>,
    ) -> Arc<Self> {
        Arc::new(Self::fields(path, context, from_environment, storage))
    }

    fn fields(
        path: PathBuf,
        context: ChildServiceContext,
        from_environment: bool,
        storage: Option<Arc<DashboardStore>>,
    ) -> Self {
        Self {
            path,
            context,
            from_environment,
            engine_override: if from_environment {
                storage_setup::engine_override_from_environment()
            } else {
                Ok(None)
            },
            history_path: None,
            native: tokio::sync::OnceCell::new_with(storage),
            selection: tokio::sync::OnceCell::new(),
            io_gate: tokio::sync::Mutex::new(()),
            protected_original: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            document: parking_lot::RwLock::new(None),
            io_evidence: Arc::new(parking_lot::RwLock::new(FileIoEvidence::default())),
            configuration_changes: tokio::sync::watch::channel(None).0,
        }
    }

    /// The legacy file/import location. A SQL authority never reads it implicitly.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn configuration_changes(&self) -> tokio::sync::watch::Receiver<Option<NativeConfigurationChange>> {
        self.configuration_changes.subscribe()
    }

    /// A later successful authoritative read must re-emit a peer change that could not be applied.
    pub(crate) fn retry_configuration_observation(&self) {
        self.configuration_changes.send_replace(None);
    }

    /// One process-owned authority for configuration, History, Monitor, Session and Audit.
    /// Driver/connect failures are explicit; an empty SQL database is not initialized here.
    pub(crate) async fn native_storage(&self) -> Result<Arc<DashboardStore>, ConfigStoreFailure> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ConfigStoreFailure::rejected(ConfigStoreRejection::Closed));
        }
        let storage = self
            .native
            .get_or_try_init(|| async {
                let options = self.storage_options().await?;
                let storage = match (options, self.history_path.as_ref()) {
                    (StorageOptions::File { config_path }, Some(history_path)) => {
                        DashboardStore::open_file_paths(config_path, history_path.clone(), self.context.clone()).await
                    }
                    (options, _) => DashboardStore::open(options, self.context.clone()).await,
                }
                .map_err(ConfigStoreFailure::from)?;
                if storage.engine() == StorageEngine::File {
                    let status = storage.status().await.map_err(ConfigStoreFailure::from)?;
                    if status.last_successful_write_epoch_ms.is_some() {
                        self.io_evidence.write().record_write(true);
                    }
                }
                Ok::<_, ConfigStoreFailure>(Arc::new(storage))
            })
            .await?;
        Ok(Arc::clone(storage))
    }

    async fn storage_options(&self) -> Result<StorageOptions, ConfigStoreFailure> {
        let engine = self.frozen_selection().await.engine.clone()?;
        if engine == StorageEngine::File {
            return Ok(StorageOptions::File {
                config_path: self.path.clone(),
            });
        }
        if !engine.is_compiled() {
            return Ok(StorageOptions::Database {
                engine,
                connection: DatabaseConnection::new(String::new()),
            });
        }
        // The shared boundary checks feature availability before parsing or connecting.
        let connection = std::env::var(DATABASE_URL_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ConfigStoreFailure::validation(ConfigValidation::DatabaseConnection))?;
        Ok(StorageOptions::Database {
            engine,
            connection: DatabaseConnection::new(connection),
        })
    }

    pub(crate) async fn close(&self) -> Result<(), ConfigStoreFailure> {
        if let Some(storage) = self.native.get() {
            storage.close().await?;
        }
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn owns_writer(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && self
                .native
                .get()
                .is_some_and(|store| store.engine() == StorageEngine::File)
    }

    pub(crate) async fn inspect_storage(&self) -> LocalFileStatus {
        let _guard = self.io_gate.lock().await;
        let file = self
            .native
            .get()
            .is_some_and(|store| store.engine() == StorageEngine::File);
        if file {
            let path = self.path.clone();
            let evidence = Arc::clone(&self.io_evidence);
            if self
                .context
                .storage_io()
                .spawn_io("gpui-config-status", move || {
                    inspect_json::<DesktopDocument>(&path, CONFIG_SCHEMA_VERSION, &evidence, |document| {
                        document.validate()?;
                        Ok(Some(document.settings.revision))
                    });
                })
                .await
                .is_err()
            {
                self.io_evidence.write().record_read(FileReadState::Unknown, None, None);
            }
        } else {
            self.io_evidence.write().record_read(FileReadState::Unknown, None, None);
        }
        LocalFileStatus {
            path: self.path.clone(),
            writer_owned: self.owns_writer(),
            evidence: *self.io_evidence.read(),
        }
    }

    async fn snapshot(&self) -> Result<DesktopSnapshot, ConfigStoreFailure> {
        let result = match self.native_storage().await {
            Ok(storage) => storage
                .load_desktop()
                .await
                .map_err(ConfigStoreFailure::from)
                .and_then(|snapshot| {
                    snapshot.ok_or_else(|| ConfigStoreFailure::rejected(ConfigStoreRejection::Uninitialized))
                }),
            Err(error) => Err(error),
        };
        match result {
            Ok(snapshot) => {
                self.protected_original.store(false, Ordering::Release);
                self.publish(snapshot.document.clone())?;
                Ok(snapshot)
            }
            Err(error) => {
                self.io_evidence.write().record_read(FileReadState::Failed, None, None);
                if error.protects_original() {
                    self.protected_original.store(true, Ordering::Release);
                }
                Err(error)
            }
        }
    }

    fn publish(&self, document: DesktopDocument) -> Result<DesktopConfig, ConfigStoreFailure> {
        let config = document.config()?;
        let change = NativeConfigurationChange {
            revision: config.revision,
            environment_id: document.active_environment_id.clone(),
        };
        if self
            .native
            .get()
            .is_some_and(|store| store.engine() == StorageEngine::File)
        {
            self.io_evidence.write().record_read(
                FileReadState::Readable,
                Some(document.schema_version),
                Some(config.revision),
            );
        }
        *self.document.write() = Some(document);
        let changed = self.configuration_changes.borrow().as_ref() != Some(&change);
        if changed {
            self.configuration_changes.send_replace(Some(change));
        }
        Ok(config)
    }

    async fn commit(
        &self,
        before: DesktopSnapshot,
        document: DesktopDocument,
    ) -> Result<DesktopDocument, ConfigStoreFailure> {
        let storage = self.native_storage().await?;
        let result = storage
            .compare_exchange_desktop(before.storage_revision, document)
            .await;
        if storage.engine() == StorageEngine::File {
            match &result {
                Ok(_) => self.io_evidence.write().record_write(true),
                Err(error) if error.io_operation() == Some(StorageIoOperation::Write) => {
                    self.io_evidence.write().record_write(false)
                }
                Err(_) => {}
            }
        }
        let document = result.map_err(ConfigStoreFailure::from)?.document;
        self.publish(document.clone())?;
        Ok(document)
    }

    pub async fn load(&self) -> Result<DesktopConfig, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        self.snapshot()
            .await?
            .document
            .config()
            .map_err(ConfigStoreFailure::from)
    }

    pub(crate) fn authentication_enabled(&self) -> Result<bool, ConfigStoreFailure> {
        self.document
            .read()
            .as_ref()
            .map(|document| document.settings.authentication_enabled)
            .ok_or_else(|| ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile))
    }

    pub(in crate::infrastructure) async fn audit_snapshot(&self) -> Result<AuditJournal, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        Ok(self.snapshot().await?.document.audit)
    }

    pub(in crate::infrastructure) async fn accept_audit(
        &self,
        operation: AuditAcceptedOperation,
    ) -> Result<(), ConfigStoreFailure> {
        self.accept_audit_batch(vec![operation]).await
    }

    pub(in crate::infrastructure) async fn accept_audit_batch(
        &self,
        operations: Vec<AuditAcceptedOperation>,
    ) -> Result<(), ConfigStoreFailure> {
        if operations.is_empty() {
            return Ok(());
        }
        let _guard = self.io_gate.lock().await;
        let snapshot = self.snapshot().await?;
        let mut next = snapshot.document.clone();
        for operation in operations {
            validate_context(&snapshot.document, &operation.context)?;
            next.audit.accept(operation)?;
        }
        self.commit(snapshot, next).await?;
        Ok(())
    }

    pub(in crate::infrastructure) async fn update_audit(
        &self,
        _name: &'static str,
        update: impl FnOnce(&mut AuditJournal) -> Result<(), ConfigStoreFailure> + Send + 'static,
    ) -> Result<(), ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let snapshot = self.snapshot().await?;
        let mut next = snapshot.document.clone();
        update(&mut next.audit)?;
        if next.audit != snapshot.document.audit {
            self.commit(snapshot, next).await?;
        }
        Ok(())
    }

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

    pub async fn save_next_with_context(
        &self,
        config: DesktopConfig,
        context: OperationContext,
    ) -> Result<DesktopConfig, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        if self.protected_original.load(Ordering::Acquire) {
            return Err(ConfigStoreFailure::rejected(ConfigStoreRejection::ProtectedOriginal));
        }
        let snapshot = self.snapshot().await?;
        if snapshot
            .document
            .local_operations
            .iter()
            .any(|record| record.context.request_id == context.request_id)
        {
            return snapshot.document.config().map_err(ConfigStoreFailure::from);
        }
        validate_context(&snapshot.document, &context)?;
        let mut config = config.normalize()?;
        if config.revision != snapshot.document.settings.revision {
            return Err(ConfigStoreFailure::stale_revision());
        }
        config.revision = config
            .revision
            .checked_add(1)
            .ok_or_else(|| ConfigStoreFailure::rejected(ConfigStoreRejection::RevisionExhausted))?;
        let mut next = snapshot.document.clone();
        next.apply_config(&config)?;
        finish_local_document(&mut next, context, LocalOperationKind::ConfigurationSaved)?;
        self.commit(snapshot, next)
            .await?
            .config()
            .map_err(ConfigStoreFailure::from)
    }

    pub fn environments(&self) -> Option<EnvironmentCatalog> {
        self.document.read().as_ref().map(DesktopDocument::catalog)
    }

    pub fn history_settings(&self) -> Option<FoundationFlags> {
        self.document
            .read()
            .as_ref()
            .map(|document| document.settings.foundations)
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

    pub(crate) async fn authoritative_environment_snapshot(
        &self,
    ) -> Result<EnvironmentConnectionSnapshot, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let document = self.snapshot().await?.document;
        Ok(EnvironmentConnectionSnapshot {
            environment_id: document.active_environment_id.clone(),
            connection: document.config()?.connection_snapshot(),
        })
    }

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

    pub async fn change_environment(
        &self,
        change: EnvironmentChange,
        context: OperationContext,
    ) -> Result<DesktopConfig, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let snapshot = self.snapshot().await?;
        if snapshot
            .document
            .local_operations
            .iter()
            .any(|record| record.context.request_id == context.request_id)
        {
            return snapshot.document.config().map_err(ConfigStoreFailure::from);
        }
        validate_context(&snapshot.document, &context)?;
        let mut next = snapshot.document.clone();
        match change {
            EnvironmentChange::Create { name } => {
                if name.trim().is_empty() {
                    return Err(ConfigStoreFailure::validation(EnvironmentValidation::InvalidProfile));
                }
                let id = new_environment_id()?;
                next.profiles.push(EnvironmentProfile::from_config(
                    id.clone(),
                    name.trim().into(),
                    &DesktopConfig::default(),
                ));
                next.active_environment_id = id;
            }
            EnvironmentChange::Select { id } => {
                if !next.profiles.iter().any(|profile| profile.id == id) {
                    return Err(ConfigStoreFailure::validation(EnvironmentValidation::MissingProfile));
                }
                next.active_environment_id = id;
            }
        }
        next.settings.revision = next
            .settings
            .revision
            .checked_add(1)
            .ok_or_else(|| ConfigStoreFailure::rejected(ConfigStoreRejection::RevisionExhausted))?;
        finish_local_document(&mut next, context, LocalOperationKind::ConfigurationSaved)?;
        self.commit(snapshot, next)
            .await?
            .config()
            .map_err(ConfigStoreFailure::from)
    }

    pub async fn monitor_rules(&self) -> Result<Vec<StoredMonitorRule>, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        Ok(self.snapshot().await?.document.monitors.rules)
    }

    #[cfg(test)]
    pub async fn save_monitor(
        &self,
        rule: Option<StoredMonitorRule>,
        id: String,
        context: OperationContext,
    ) -> Result<bool, ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let snapshot = self.snapshot().await?;
        if snapshot
            .document
            .local_operations
            .iter()
            .any(|record| record.context.request_id == context.request_id)
        {
            return Ok(false);
        }
        validate_context(&snapshot.document, &context)?;
        let mut next = snapshot.document.clone();
        let before = next.monitors.rules.len();
        next.monitors.rules.retain(|existing| {
            !(existing.environment_id.as_ref() == Some(&context.environment_id) && existing.rule.id == id)
        });
        let removed = before != next.monitors.rules.len();
        let kind = if let Some(rule) = rule {
            next.monitors.rules.push(rule);
            LocalOperationKind::MonitorSaved
        } else {
            LocalOperationKind::MonitorDeleted
        };
        finish_local_document(&mut next, context, kind)?;
        self.commit(snapshot, next).await?;
        Ok(removed)
    }
}

fn validate_context(document: &DesktopDocument, context: &OperationContext) -> Result<(), ConfigStoreFailure> {
    if context.configuration_revision != document.settings.revision
        || context.environment_id != document.active_environment_id
    {
        Err(ConfigStoreFailure::stale_revision())
    } else {
        Ok(())
    }
}

fn finish_local_document(
    document: &mut DesktopDocument,
    context: OperationContext,
    kind: LocalOperationKind,
) -> Result<(), ConfigStoreFailure> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(ConfigStoreFailure::validation)?;
    let committed_at_epoch_ms = u64::try_from(timestamp.as_millis()).map_err(ConfigStoreFailure::validation)?;
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
    document.validate().map_err(ConfigStoreFailure::from)
}
