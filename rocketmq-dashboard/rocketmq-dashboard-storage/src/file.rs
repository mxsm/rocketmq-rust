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

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, TryLockError},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use rocketmq_dashboard_common::{EnvironmentHistoryPoint, EnvironmentId, HistoryPoint, HistoryRetention};
use rocketmq_runtime::ChildServiceContext;

use crate::{
    atomic_file::write_json_atomically, document, history, session::SessionDocument, store::now_ms, DesktopDocument,
    DesktopSnapshot, HistoryAppendOutcome, HistoryDocument, HistoryLease, ImportSnapshot, SessionId, SessionPage,
    SessionQuery, SessionRecord, StorageEngine, StorageError, StorageErrorKind, StorageRevision, StorageStatus,
};

/// Never replaced or removed, so every native instance locks the same file object.
struct DirectoryWriter {
    _file: File,
}

impl DirectoryWriter {
    fn acquire(config: &Path) -> Result<Self, StorageError> {
        let directory = config.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(directory).map_err(|_| StorageError::new(StorageErrorKind::Io))?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join(".dashboard-writer.lock"))
            .map_err(|_| StorageError::new(StorageErrorKind::Io))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(StorageError::new(StorageErrorKind::WriterUnavailable)),
            Err(TryLockError::Error(_)) => Err(StorageError::new(StorageErrorKind::Io)),
        }
    }
}

struct FileState {
    config_path: PathBuf,
    revision: StorageRevision,
    writer: Option<DirectoryWriter>,
    leases: BTreeMap<EnvironmentId, HistoryLease>,
    last_write_epoch_ms: Option<u64>,
}

impl FileState {
    fn ensure_open(&self) -> Result<(), StorageError> {
        if self.writer.is_none() {
            Err(StorageError::new(StorageErrorKind::Closed))
        } else {
            Ok(())
        }
    }

    fn load(&self) -> Result<DesktopDocument, StorageError> {
        self.ensure_open()?;
        document::load_document(&self.config_path).map(|(document, _)| document)
    }

    fn wrote(&mut self) {
        self.last_write_epoch_ms = now_ms().ok().and_then(|value| value.try_into().ok());
    }

    fn session_path(&self) -> PathBuf {
        self.config_path.with_file_name("sessions.json")
    }

    fn session_update<R>(
        &mut self,
        update: impl FnOnce(&mut SessionDocument) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        self.ensure_open()?;
        let path = self.session_path();
        let mut sessions = read_sessions(&path)?;
        let before = sessions.clone();
        let result = update(&mut sessions)?;
        sessions.validate()?;
        if sessions != before {
            write_json_atomically(&path, &sessions)?;
            self.wrote();
        }
        Ok(result)
    }

    fn active_lease(&self, lease: &HistoryLease, now: i64) -> bool {
        self.leases
            .get(lease.environment_id())
            .is_some_and(|current| current.matches(lease, now))
    }
}

pub(crate) struct FileStore {
    context: ChildServiceContext,
    state: Arc<Mutex<FileState>>,
}

impl FileStore {
    pub(crate) async fn open(config_path: PathBuf, context: ChildServiceContext) -> Result<Self, StorageError> {
        let state = context
            .storage_io()
            .spawn_io("native-storage-open", move || {
                let writer = DirectoryWriter::acquire(&config_path)?;
                let (document, migrated) = document::load_document(&config_path)?;
                document.validate()?;
                if migrated {
                    write_json_atomically(&config_path, &document)?;
                }
                Ok::<_, StorageError>(FileState {
                    config_path,
                    revision: StorageRevision(0),
                    writer: Some(writer),
                    leases: BTreeMap::new(),
                    last_write_epoch_ms: if migrated {
                        now_ms().ok().and_then(|value| value.try_into().ok())
                    } else {
                        None
                    },
                })
            })
            .await
            .map_err(|_| StorageError::new(StorageErrorKind::Runtime))??;
        Ok(Self {
            context,
            state: Arc::new(Mutex::new(state)),
        })
    }

    async fn run<R: Send + 'static>(
        &self,
        name: &'static str,
        operation: impl FnOnce(&mut FileState) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        let state = Arc::clone(&self.state);
        self.context
            .storage_io()
            .spawn_io(name, move || {
                let mut state = state.lock().map_err(|_| StorageError::new(StorageErrorKind::Runtime))?;
                operation(&mut state)
            })
            .await
            .map_err(|_| StorageError::new(StorageErrorKind::Runtime))?
    }

    pub(crate) async fn load_desktop(&self) -> Result<Option<DesktopSnapshot>, StorageError> {
        self.run("native-desktop-load", |state| {
            Ok(Some(DesktopSnapshot {
                storage_revision: state.revision,
                document: state.load()?,
            }))
        })
        .await
    }

    pub(crate) async fn initialize(&self, _document: DesktopDocument) -> Result<DesktopSnapshot, StorageError> {
        Err(StorageError::new(StorageErrorKind::AlreadyInitialized))
    }

    pub(crate) async fn compare_exchange_desktop(
        &self,
        expected: StorageRevision,
        document: DesktopDocument,
    ) -> Result<DesktopSnapshot, StorageError> {
        self.run("native-desktop-commit", move |state| {
            state.ensure_open()?;
            if state.revision != expected {
                return Err(StorageError::conflict());
            }
            let next = expected.next()?;
            write_json_atomically(&state.config_path, &document)?;
            state.revision = next;
            state.wrote();
            Ok(DesktopSnapshot {
                storage_revision: next,
                document,
            })
        })
        .await
    }

    pub(crate) async fn import(&self, _snapshot: ImportSnapshot) -> Result<DesktopSnapshot, StorageError> {
        Err(StorageError::new(StorageErrorKind::AlreadyInitialized))
    }

    pub(crate) async fn read_import(
        config_path: PathBuf,
        context: ChildServiceContext,
    ) -> Result<ImportSnapshot, StorageError> {
        context
            .storage_io()
            .spawn_io("native-storage-read-import", move || {
                let _writer = DirectoryWriter::acquire(&config_path)?;
                if !config_path
                    .try_exists()
                    .map_err(|_| StorageError::new(StorageErrorKind::Io))?
                {
                    return Err(StorageError::new(StorageErrorKind::EmptySource));
                }
                let (document, _) = document::load_document(&config_path)?;
                let (history, _) = history::load_document(&config_path.with_file_name("history.json"))?;
                history.validate()?;
                let sessions = read_sessions(&config_path.with_file_name("sessions.json"))?;
                Ok(ImportSnapshot {
                    document,
                    history,
                    sessions,
                })
            })
            .await
            .map_err(|_| StorageError::new(StorageErrorKind::Runtime))?
    }

    pub(crate) async fn history(&self) -> Result<HistoryDocument, StorageError> {
        self.run("native-history-read", |state| {
            state.ensure_open()?;
            let path = state.config_path.with_file_name("history.json");
            let (history, migrated) = history::load_document(&path)?;
            history.validate()?;
            if migrated {
                write_json_atomically(&path, &history)?;
                state.wrote();
            }
            Ok(history)
        })
        .await
    }

    pub(crate) async fn acquire_history_lease(
        &self,
        environment_id: EnvironmentId,
        holder_id: String,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, StorageError> {
        self.run("native-history-acquire", move |state| {
            if !state
                .load()?
                .profiles
                .iter()
                .any(|profile| profile.id == environment_id)
            {
                return Err(StorageError::new(StorageErrorKind::Validation));
            }
            let now = now_ms()?;
            let previous = state.leases.get(&environment_id);
            if previous.is_some_and(|lease| lease.expires_at_epoch_ms > now) {
                return Ok(None);
            }
            let token = previous
                .map_or(0, |lease| lease.fencing_token)
                .checked_add(1)
                .ok_or_else(|| StorageError::new(StorageErrorKind::RevisionExhausted))?;
            let lease = HistoryLease {
                environment_id,
                holder_id,
                fencing_token: token,
                expires_at_epoch_ms: now
                    .checked_add(ttl_ms)
                    .ok_or_else(|| StorageError::new(StorageErrorKind::Validation))?,
            };
            state.leases.insert(lease.environment_id.clone(), lease.clone());
            Ok(Some(lease))
        })
        .await
    }

    pub(crate) async fn renew_history_lease(
        &self,
        lease: HistoryLease,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, StorageError> {
        self.run("native-history-renew", move |state| {
            state.ensure_open()?;
            let now = now_ms()?;
            if !state.active_lease(&lease, now) {
                return Ok(None);
            }
            let renewed = HistoryLease {
                expires_at_epoch_ms: now
                    .checked_add(ttl_ms)
                    .ok_or_else(|| StorageError::new(StorageErrorKind::Validation))?,
                ..lease
            };
            state.leases.insert(renewed.environment_id.clone(), renewed.clone());
            Ok(Some(renewed))
        })
        .await
    }

    pub(crate) async fn release_history_lease(&self, lease: HistoryLease) -> Result<bool, StorageError> {
        self.run("native-history-release", move |state| {
            state.ensure_open()?;
            if !state.active_lease(&lease, now_ms()?) {
                return Ok(false);
            }
            if let Some(current) = state.leases.get_mut(&lease.environment_id) {
                current.expires_at_epoch_ms = 0;
            }
            Ok(true)
        })
        .await
    }

    pub(crate) async fn append_history(
        &self,
        lease: HistoryLease,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, StorageError> {
        self.run("native-history-append", move |state| {
            state.ensure_open()?;
            if !state.active_lease(&lease, now_ms()?) {
                return Err(StorageError::new(StorageErrorKind::LeaseLost));
            }
            if sample.is_empty() {
                return Ok(HistoryAppendOutcome::NotRetained);
            }
            let path = state.config_path.with_file_name("history.json");
            let (mut history, _) = history::load_document(&path)?;
            history.validate()?;
            let before = history.clone();
            let outcome = history.append(
                sample
                    .into_iter()
                    .map(|point| EnvironmentHistoryPoint {
                        environment_id: Some(lease.environment_id.clone()),
                        point,
                    })
                    .collect(),
                retention,
            );
            if history != before {
                write_json_atomically(&path, &history)?;
                state.wrote();
            }
            Ok(outcome)
        })
        .await
    }

    pub(crate) async fn create_session(&self, session: SessionRecord, now: u64) -> Result<(), StorageError> {
        self.run("native-session-create", move |state| {
            state.session_update(|document| document.create(session, now))
        })
        .await
    }

    pub(crate) async fn access_session(&self, id: SessionId, now: u64) -> Result<Option<SessionRecord>, StorageError> {
        self.run("native-session-access", move |state| {
            state.session_update(|document| Ok(document.access(&id, now)))
        })
        .await
    }

    pub(crate) async fn sessions(&self, query: SessionQuery) -> Result<SessionPage, StorageError> {
        self.run("native-session-query", move |state| {
            state.ensure_open()?;
            read_sessions(&state.session_path())?.query(query)
        })
        .await
    }

    pub(crate) async fn revoke_session(&self, id: SessionId, now: u64) -> Result<bool, StorageError> {
        self.run("native-session-revoke", move |state| {
            state.session_update(|document| Ok(document.revoke(&id, now)))
        })
        .await
    }

    pub(crate) async fn revoke_all_sessions(&self, username: String, now: u64) -> Result<usize, StorageError> {
        self.run("native-session-revoke-all", move |state| {
            state.session_update(|document| document.revoke_all(&username, now))
        })
        .await
    }

    pub(crate) async fn status(&self) -> Result<StorageStatus, StorageError> {
        self.run("native-storage-status", |state| {
            let document = state.load()?;
            Ok(StorageStatus {
                engine: StorageEngine::File,
                schema_version: 1,
                document_schema_version: Some(document.schema_version),
                initialized: true,
                last_successful_write_epoch_ms: state.last_write_epoch_ms,
                pool_connections: None,
                pool_idle_connections: None,
            })
        })
        .await
    }

    pub(crate) async fn close(&self) -> Result<(), StorageError> {
        self.run("native-storage-close", |state| {
            state.writer.take();
            state.leases.clear();
            Ok(())
        })
        .await
    }
}

fn read_sessions(path: &Path) -> Result<SessionDocument, StorageError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(SessionDocument::empty()),
        Err(_) => return Err(StorageError::new(StorageErrorKind::Io)),
    };
    let sessions: SessionDocument =
        serde_json::from_slice(&bytes).map_err(|_| StorageError::new(StorageErrorKind::InvalidDocument))?;
    sessions.validate()?;
    Ok(sessions)
}
