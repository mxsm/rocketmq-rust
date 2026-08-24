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
use crate::config::StorageConfig;
use crate::model::AuditEvent;
use crate::persistence::StorageHealth;
use crate::persistence::StorageMode;
use crate::persistence::StorageStatus;
use crate::persistence::error::PersistenceError;
use chrono::Utc;
use fs4::fs_std::FileExt;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering as AtomicOrdering;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::oneshot;

#[path = "audit_file_store.rs"]
mod audit_file_store;
#[path = "history_file_store.rs"]
mod history_file_store;
#[path = "session_file_store.rs"]
mod session_file_store;

const FORMAT_VERSION: u32 = 1;
const MIN_AVAILABLE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
struct FileDirectoryLock {
    _file: File,
}

impl FileDirectoryLock {
    fn acquire(root: &Path) -> Result<Self, PersistenceError> {
        if root.is_file() {
            return Err(PersistenceError::UnsupportedLayout);
        }
        std::fs::create_dir_all(root).map_err(PersistenceError::Io)?;
        reject_legacy_layout(root)?;
        let lock_directory = root.join("locks");
        std::fs::create_dir_all(&lock_directory).map_err(PersistenceError::Io)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_directory.join("dashboard.lock"))
            .map_err(PersistenceError::Io)?;
        let locked = file.try_lock_exclusive().map_err(PersistenceError::Io)?;
        if !locked {
            return Err(PersistenceError::LockUnavailable);
        }
        recover_incomplete_snapshot_transactions(root)?;
        session_file_store::recover_session_audit_transactions(root)?;
        session_file_store::recover_session_touch_transactions(root)?;
        session_file_store::recover_session_cleanup_transactions(root)?;
        audit_file_store::recover_audit_rewrite_transactions(root)?;
        history_file_store::recover_history_file_operations(root)?;
        initialize_manifest(root)?;
        Ok(Self { _file: file })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileManifest {
    format_version: u32,
    backend: String,
    created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileSnapshot {
    pub format_version: u32,
    pub revision: u64,
    pub payload: Value,
}

/// A staged multi-collection publication. The marker remains durable until
/// commit or rollback is complete; startup uses its state to either finish a
/// committed cleanup or remove the staged snapshots.
#[derive(Debug, Clone)]
pub(crate) struct FileSnapshotTransactionWrite {
    pub collection: String,
    pub expected_revision: u64,
    pub payload: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileSnapshotTransaction {
    format_version: u32,
    writes: Vec<FileSnapshotTransactionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    audit: Option<FileSnapshotAuditAppend>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileSnapshotTransactionRecord {
    collection: String,
    revision: u64,
}

/// The only journal rollback data persisted with a snapshot aggregate. The
/// append payload itself is already durable in the journal; no checksum or
/// token-derived identifier is stored in the marker.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileSnapshotAuditAppend {
    relative_path: String,
    existed: bool,
    original_length: u64,
}

/// The result sent by an admitted storage-I/O task. A task-timeout is not a
/// cancellation signal for `spawn_io`, so callers retain this receipt until
/// the blocking closure acknowledges that it has stopped mutating the file
/// store.
enum FileMutationDispatch<T> {
    Completed(Result<T, PersistenceError>),
    TimedOut {
        error: RuntimeError,
        receipt: oneshot::Receiver<Result<T, PersistenceError>>,
    },
    Rejected(RuntimeError),
}

/// A successfully staged mutation. Both finalization and rollback stay with
/// the runtime-owned mutation task rather than the request future.
struct FileMutationOutcome<T> {
    value: T,
    /// Makes staged snapshots durable as a committed aggregate.
    finalize: Box<dyn FnOnce() -> Result<(), PersistenceError> + Send>,
    /// Best-effort cleanup after commit. A surviving committed marker is safe:
    /// restart preserves its snapshots and removes the marker later.
    cleanup: Box<dyn FnOnce() -> Result<(), PersistenceError> + Send>,
    /// Restores the prior aggregate while the marker still proves ownership.
    rollback: Box<dyn FnOnce() -> Result<(), PersistenceError> + Send>,
}

#[cfg(test)]
#[derive(Debug)]
struct TestMutationBlocker {
    started: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    panic_after_release: bool,
    rollback_failure_after_deletions: Option<usize>,
}

#[cfg(test)]
#[derive(Debug)]
struct TestCommittedCleanupBlocker {
    started: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    fail_after_release: bool,
}

#[derive(Debug, Default)]
struct FileMutationBlocker {
    #[cfg(test)]
    blocker: std::sync::Mutex<Option<Arc<TestMutationBlocker>>>,
    #[cfg(test)]
    committed_cleanup_blocker: std::sync::Mutex<Option<Arc<TestCommittedCleanupBlocker>>>,
}

impl FileMutationBlocker {
    fn wait_after_mutation(&self) {
        #[cfg(test)]
        {
            let blocker = self.blocker.lock().ok().and_then(|guard| guard.clone());
            if let Some(blocker) = blocker {
                let _ = blocker.started.send(());
                if let Ok(receiver) = blocker.release.lock() {
                    let _ = receiver.recv();
                }
                assert!(
                    !blocker.panic_after_release,
                    "injected post-dispatch blocking mutation failure"
                );
            }
        }
    }

    fn rollback_failure_after_deletions(&self) -> Option<usize> {
        #[cfg(test)]
        {
            self.blocker.lock().ok().and_then(|blocker| {
                blocker
                    .as_ref()
                    .and_then(|blocker| blocker.rollback_failure_after_deletions)
            })
        }
        #[cfg(not(test))]
        {
            None
        }
    }

    fn wait_after_committed_cleanup(&self) -> Result<(), PersistenceError> {
        #[cfg(test)]
        {
            let blocker = self
                .committed_cleanup_blocker
                .lock()
                .ok()
                .and_then(|guard| guard.clone());
            if let Some(blocker) = blocker {
                let _ = blocker.started.send(());
                if let Ok(receiver) = blocker.release.lock() {
                    let _ = receiver.recv();
                }
                if blocker.fail_after_release {
                    return Err(PersistenceError::Io(std::io::Error::other(
                        "injected committed cleanup failure",
                    )));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn install(&self, blocker: Arc<TestMutationBlocker>) {
        if let Ok(mut current) = self.blocker.lock() {
            *current = Some(blocker);
        }
    }

    #[cfg(test)]
    fn clear(&self) {
        if let Ok(mut current) = self.blocker.lock() {
            *current = None;
        }
    }

    #[cfg(test)]
    fn install_committed_cleanup(&self, blocker: Arc<TestCommittedCleanupBlocker>) {
        if let Ok(mut current) = self.committed_cleanup_blocker.lock() {
            *current = Some(blocker);
        }
    }

    #[cfg(test)]
    fn clear_committed_cleanup(&self) {
        if let Ok(mut current) = self.committed_cleanup_blocker.lock() {
            *current = None;
        }
    }
}

/// Single-node File backend. The lock remains open for the lifetime of this
/// instance, and every filesystem operation is delegated to storage I/O.
#[derive(Debug, Clone)]
pub struct FilePersistence {
    root: PathBuf,
    _lock: Arc<FileDirectoryLock>,
    service_context: ChildServiceContext,
    last_successful_write_at: Arc<AtomicI64>,
    unavailable: Arc<AtomicBool>,
    write_lock: Arc<RwLock<()>>,
    mutation_blocker: Arc<FileMutationBlocker>,
    #[cfg(test)]
    history_replace_failpoint: Arc<std::sync::atomic::AtomicU8>,
}

impl FilePersistence {
    pub async fn initialize(
        config: &StorageConfig,
        service_context: ChildServiceContext,
    ) -> Result<Self, PersistenceError> {
        let root = config.data_path.clone();
        let lock = service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-initialize", move || {
                FileDirectoryLock::acquire(&root)
            })
            .await
            .map_err(PersistenceError::Runtime)??;
        Ok(Self {
            root: config.data_path.clone(),
            _lock: Arc::new(lock),
            service_context,
            last_successful_write_at: Arc::new(AtomicI64::new(0)),
            unavailable: Arc::new(AtomicBool::new(false)),
            write_lock: Arc::new(RwLock::new(())),
            mutation_blocker: Arc::new(FileMutationBlocker::default()),
            #[cfg(test)]
            history_replace_failpoint: Arc::new(std::sync::atomic::AtomicU8::new(0)),
        })
    }

    #[cfg(test)]
    pub(crate) fn install_test_committed_cleanup_blocker(
        &self,
        started: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
        fail_after_release: bool,
    ) {
        self.mutation_blocker
            .install_committed_cleanup(Arc::new(TestCommittedCleanupBlocker {
                started,
                release: std::sync::Mutex::new(release),
                fail_after_release,
            }));
    }

    #[cfg(test)]
    pub(crate) fn clear_test_committed_cleanup_blocker(&self) {
        self.mutation_blocker.clear_committed_cleanup();
    }

    #[cfg(test)]
    pub(crate) fn set_history_replace_failpoint(&self, point: u8) {
        self.history_replace_failpoint.store(point, AtomicOrdering::SeqCst);
    }

    pub async fn write_snapshot(
        &self,
        collection: &str,
        revision: u64,
        payload: Value,
    ) -> Result<(), PersistenceError> {
        let write_guard = self.write_guard().await;
        self.write_snapshot_locked(write_guard, collection, revision, payload)
            .await
    }

    /// Acquires the repository-wide write gate. Environment and monitor
    /// repositories use the same gate so a cascade cannot race a monitor CAS.
    pub(crate) async fn write_guard(&self) -> OwnedRwLockWriteGuard<()> {
        self.write_lock.clone().write_owned().await
    }

    /// Acquires the repository-wide read gate. Environment and monitor reads
    /// use it so a staged multi-collection delete cannot expose one snapshot
    /// before its transaction commits.
    pub(crate) async fn read_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.write_lock.read().await
    }

    /// Writes an immutable snapshot while [`Self::write_guard`] is held.
    pub(crate) async fn write_snapshot_locked(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        collection: &str,
        revision: u64,
        payload: Value,
    ) -> Result<(), PersistenceError> {
        self.ensure_available()?;
        let root = self.root.clone();
        let collection = collection.to_string();
        let mutation_blocker = self.mutation_blocker.clone();
        self.dispatch_file_mutation(write_guard, "dashboard-file-storage-write-snapshot", move || {
            validate_collection(&collection)?;
            let prepared = prepare_snapshot_transaction(
                &root,
                vec![PreparedSnapshotWrite {
                    collection,
                    revision,
                    payload,
                }],
                None,
                None,
            )?;
            mutation_blocker.wait_after_mutation();
            Ok(FileMutationOutcome {
                value: (),
                finalize: Box::new({
                    let prepared = prepared.clone();
                    move || prepared.finalize()
                }),
                cleanup: Box::new({
                    let prepared = prepared.clone();
                    let cleanup_blocker = mutation_blocker.clone();
                    move || {
                        cleanup_blocker.wait_after_committed_cleanup()?;
                        prepared.cleanup()
                    }
                }),
                rollback: Box::new(move || prepared.rollback(mutation_blocker.rollback_failure_after_deletions())),
            })
        })
        .await?;
        self.record_write();
        Ok(())
    }

    pub async fn load_latest_snapshot(&self, collection: &str) -> Result<Option<FileSnapshot>, PersistenceError> {
        self.ensure_available()?;
        let root = self.root.clone();
        let collection = collection.to_string();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-load-snapshot", move || {
                validate_collection(&collection)?;
                load_latest_snapshot_file(&root.join(collection).join("snapshots"))
            })
            .await
            .map_err(PersistenceError::Runtime)?
    }

    /// Lists the immediate snapshot collections below a repository-owned
    /// parent.  Callers still load and validate each latest snapshot.
    pub(crate) async fn list_snapshot_collections(&self, parent: &str) -> Result<Vec<String>, PersistenceError> {
        self.ensure_available()?;
        validate_collection(parent)?;
        let root = self.root.clone();
        let parent = parent.to_string();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-list-collections", move || {
                let directory = root.join(&parent);
                if !directory.exists() {
                    return Ok(Vec::new());
                }
                let mut collections = Vec::new();
                for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
                    let entry = entry.map_err(PersistenceError::Io)?;
                    if !entry.file_type().map_err(PersistenceError::Io)?.is_dir() {
                        continue;
                    }
                    let name = entry.file_name().to_string_lossy().into_owned();
                    validate_segment(&name)?;
                    collections.push(format!("{parent}/{name}"));
                }
                collections.sort();
                Ok(collections)
            })
            .await
            .map_err(PersistenceError::Runtime)?
    }

    /// Atomically verifies the currently published revision and creates the
    /// next immutable snapshot while the File backend write lock is held.
    pub async fn compare_and_write_snapshot(
        &self,
        collection: &str,
        expected_revision: u64,
        payload: Value,
    ) -> Result<u64, PersistenceError> {
        let write_guard = self.write_guard().await;
        self.compare_and_write_snapshot_locked(write_guard, collection, expected_revision, payload)
            .await
    }

    /// Performs snapshot CAS while [`Self::write_guard`] is held. Keeping the
    /// revision read, validation, and publication inside one gate prevents a
    /// same-process File writer from observing a half-completed cascade.
    pub(crate) async fn compare_and_write_snapshot_locked(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        collection: &str,
        expected_revision: u64,
        payload: Value,
    ) -> Result<u64, PersistenceError> {
        self.ensure_available()?;
        let root = self.root.clone();
        let collection = collection.to_string();
        let mutation_blocker = self.mutation_blocker.clone();
        let revision = self
            .dispatch_file_mutation(write_guard, "dashboard-file-storage-cas-snapshot", move || {
                validate_collection(&collection)?;
                let directory = root.join(&collection).join("snapshots");
                let current = load_latest_snapshot_file(&directory)?;
                let current_revision = current.map_or(0, |snapshot| snapshot.revision);
                if current_revision != expected_revision {
                    return Err(PersistenceError::Conflict);
                }
                let revision = current_revision.checked_add(1).ok_or(PersistenceError::Conflict)?;
                let prepared = prepare_snapshot_transaction(
                    &root,
                    vec![PreparedSnapshotWrite {
                        collection,
                        revision,
                        payload,
                    }],
                    None,
                    None,
                )?;
                mutation_blocker.wait_after_mutation();
                Ok(FileMutationOutcome {
                    value: revision,
                    finalize: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.finalize()
                    }),
                    cleanup: Box::new({
                        let prepared = prepared.clone();
                        let cleanup_blocker = mutation_blocker.clone();
                        move || {
                            cleanup_blocker.wait_after_committed_cleanup()?;
                            prepared.cleanup()
                        }
                    }),
                    rollback: Box::new(move || prepared.rollback(mutation_blocker.rollback_failure_after_deletions())),
                })
            })
            .await?;
        self.record_write();
        Ok(revision)
    }

    /// Atomically stages snapshots for related collections while the
    /// repository write gate is held. The runtime-owned mutation resolver
    /// makes the durable commit or rollback decision after all snapshots have
    /// been staged.
    pub(crate) async fn publish_snapshot_transaction_locked(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        writes: Vec<FileSnapshotTransactionWrite>,
    ) -> Result<(), PersistenceError> {
        self.publish_snapshot_transaction_locked_with_failure(write_guard, writes, None, None)
            .await
    }

    /// Publishes related snapshots and one already-safe audit event behind the
    /// same prepared/committed decision marker. The caller must retain the
    /// returned request ownership through `dispatch_file_mutation`; a request
    /// cancellation can therefore only observe the old aggregate or the
    /// committed aggregate plus its journal entry.
    pub(crate) async fn publish_snapshot_transaction_with_audit_locked(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        writes: Vec<FileSnapshotTransactionWrite>,
        audit: AuditEvent,
    ) -> Result<(), PersistenceError> {
        self.ensure_available()?;
        if writes.is_empty() {
            return Err(PersistenceError::InvalidConfig(
                "snapshot audit transaction requires at least one write".to_string(),
            ));
        }
        let root = self.root.clone();
        let mutation_blocker = self.mutation_blocker.clone();
        self.dispatch_file_mutation(
            write_guard,
            "dashboard-file-storage-publish-snapshot-audit-transaction",
            move || {
                let encoded = serde_json::to_vec(&audit).map_err(PersistenceError::Serialization)?;
                let audit_append = prepare_snapshot_audit_append(&root, audit.created_at_ms)?;
                let prepared = prepare_transaction_writes_with_audit(&root, &writes, audit_append.clone())?;
                append_snapshot_audit(&root, &audit_append, &encoded)?;
                mutation_blocker.wait_after_mutation();
                Ok(FileMutationOutcome {
                    value: (),
                    finalize: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.finalize()
                    }),
                    cleanup: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.cleanup()
                    }),
                    rollback: Box::new(move || prepared.rollback(None)),
                })
            },
        )
        .await?;
        self.record_write();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn publish_snapshot_transaction_with_failure_after_locked(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        writes: Vec<FileSnapshotTransactionWrite>,
        fail_after_writes: usize,
    ) -> Result<(), PersistenceError> {
        self.publish_snapshot_transaction_locked_with_failure(write_guard, writes, Some(fail_after_writes), None)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn publish_snapshot_transaction_with_rollback_failure_locked(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        writes: Vec<FileSnapshotTransactionWrite>,
        fail_after_writes: usize,
    ) -> Result<(), PersistenceError> {
        self.publish_snapshot_transaction_locked_with_failure(write_guard, writes, Some(fail_after_writes), Some(0))
            .await
    }

    async fn publish_snapshot_transaction_locked_with_failure(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        writes: Vec<FileSnapshotTransactionWrite>,
        fail_after_writes: Option<usize>,
        fail_rollback_after_deletions: Option<usize>,
    ) -> Result<(), PersistenceError> {
        self.ensure_available()?;
        if writes.is_empty() {
            return Ok(());
        }
        let root = self.root.clone();
        let mutation_blocker = self.mutation_blocker.clone();
        self.dispatch_file_mutation(
            write_guard,
            "dashboard-file-storage-publish-snapshot-transaction",
            move || {
                let prepared = prepare_transaction_writes(&root, &writes, fail_after_writes)?;
                mutation_blocker.wait_after_mutation();
                Ok(FileMutationOutcome {
                    value: (),
                    finalize: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.finalize()
                    }),
                    cleanup: Box::new({
                        let prepared = prepared.clone();
                        let cleanup_blocker = mutation_blocker.clone();
                        move || {
                            cleanup_blocker.wait_after_committed_cleanup()?;
                            prepared.cleanup()
                        }
                    }),
                    rollback: Box::new(move || {
                        prepared.rollback(
                            fail_rollback_after_deletions
                                .or_else(|| mutation_blocker.rollback_failure_after_deletions()),
                        )
                    }),
                })
            },
        )
        .await?;
        self.record_write();
        Ok(())
    }

    pub async fn append_jsonl(&self, stream: &str, payload: Value) -> Result<(), PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let stream = stream.to_string();
        let mutation_blocker = self.mutation_blocker.clone();
        self.dispatch_file_mutation(write_guard, "dashboard-file-storage-append-jsonl", move || {
            validate_segment(&stream)?;
            let directory = root.join("history");
            std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
            let path = directory.join(format!("{stream}.jsonl"));
            let existed = path.exists();
            truncate_incomplete_tail(&path)?;
            let original_length = if existed {
                std::fs::metadata(&path).map_err(PersistenceError::Io)?.len()
            } else {
                0
            };
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(PersistenceError::Io)?;
            serde_json::to_writer(&mut file, &payload).map_err(PersistenceError::Serialization)?;
            file.write_all(b"\n").map_err(PersistenceError::Io)?;
            file.flush().map_err(PersistenceError::Io)?;
            file.sync_data().map_err(PersistenceError::Io)?;
            mutation_blocker.wait_after_mutation();
            Ok(FileMutationOutcome {
                value: (),
                finalize: Box::new(|| Ok(())),
                cleanup: Box::new(|| Ok(())),
                rollback: Box::new(move || rollback_jsonl_append(path, original_length, existed)),
            })
        })
        .await?;
        self.record_write();
        Ok(())
    }

    pub async fn read_jsonl(&self, stream: &str) -> Result<Vec<Value>, PersistenceError> {
        self.ensure_available()?;
        let root = self.root.clone();
        let stream = stream.to_string();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-read-jsonl", move || {
                validate_segment(&stream)?;
                read_jsonl_file(&root.join("history").join(format!("{stream}.jsonl")))
            })
            .await
            .map_err(PersistenceError::Runtime)?
    }

    pub async fn storage_health(&self) -> StorageHealth {
        if self.unavailable.load(AtomicOrdering::Acquire) {
            return StorageHealth {
                backend: crate::model::StorageBackend::File,
                mode: StorageMode::SingleNode,
                status: StorageStatus::Unavailable,
                schema_version: None,
                last_successful_write_at: non_zero(self.last_successful_write_at.load(AtomicOrdering::Acquire)),
                available_bytes: None,
                pool_size: None,
                idle_connections: None,
            };
        }
        let root = self.root.clone();
        let available_bytes = self
            .service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-health", move || validate_file_health(&root))
            .await
            .ok()
            .and_then(Result::ok);
        let status = match available_bytes {
            Some(bytes) if bytes >= MIN_AVAILABLE_BYTES => StorageStatus::Available,
            Some(_) => StorageStatus::Degraded,
            None => StorageStatus::Unavailable,
        };
        StorageHealth {
            backend: crate::model::StorageBackend::File,
            mode: StorageMode::SingleNode,
            status,
            schema_version: available_bytes.map(|_| i64::from(FORMAT_VERSION)),
            last_successful_write_at: non_zero(self.last_successful_write_at.load(AtomicOrdering::Acquire)),
            available_bytes,
            pool_size: None,
            idle_connections: None,
        }
    }

    fn record_write(&self) {
        self.last_successful_write_at
            .store(Utc::now().timestamp_millis(), AtomicOrdering::Release);
    }

    fn ensure_available(&self) -> Result<(), PersistenceError> {
        if self.unavailable.load(AtomicOrdering::Acquire) {
            Err(PersistenceError::ConnectionUnavailable)
        } else {
            Ok(())
        }
    }

    /// Hands a File mutation to the service task group. Once the task is
    /// accepted, its owned write gate, completion receipt, and rollback stay
    /// alive even if the HTTP request future is dropped.
    async fn dispatch_file_mutation<T, F>(
        &self,
        write_guard: OwnedRwLockWriteGuard<()>,
        name: &'static str,
        operation: F,
    ) -> Result<T, PersistenceError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<FileMutationOutcome<T>, PersistenceError> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let store = self.clone();
        self.service_context
            .spawn_service(name, async move {
                let _write_guard = write_guard;
                let result = store.execute_file_mutation(name, operation).await;
                let _ = sender.send(result);
            })
            .map_err(PersistenceError::Runtime)?;
        receiver.await.unwrap_or(Err(PersistenceError::ConnectionUnavailable))
    }

    async fn execute_file_mutation<T, F>(&self, name: &'static str, operation: F) -> Result<T, PersistenceError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<FileMutationOutcome<T>, PersistenceError> + Send + 'static,
    {
        match self.run_blocking_mutation(name, operation).await {
            FileMutationDispatch::Completed(Ok(outcome)) => self.finalize_file_mutation(outcome).await,
            FileMutationDispatch::Completed(Err(error)) => {
                let recovery = self.recover_incomplete_file_operations_owned().await;
                if recovery.is_err() {
                    self.unavailable.store(true, AtomicOrdering::Release);
                }
                recovery?;
                Err(error)
            }
            FileMutationDispatch::TimedOut { error, receipt } => {
                self.unavailable.store(true, AtomicOrdering::Release);
                let (acknowledged, recovered) = match Self::await_mutation_receipt(receipt).await {
                    Ok(outcome) => (
                        true,
                        self.run_recovery_operation_owned(
                            "dashboard-file-storage-rollback-owned-mutation",
                            outcome.rollback,
                        )
                        .await,
                    ),
                    // The closure failed after admission. A marker-backed
                    // transaction can still be recovered, but without an
                    // acknowledgement a single-snapshot result is unknown,
                    // so the instance remains poisoned either way.
                    Err(_) => (false, self.recover_incomplete_file_operations_owned().await),
                };
                if acknowledged && recovered.is_ok() {
                    self.unavailable.store(false, AtomicOrdering::Release);
                }
                Err(PersistenceError::Runtime(error))
            }
            FileMutationDispatch::Rejected(error) if is_pre_admission_rejection(&error) => {
                Err(PersistenceError::Runtime(error))
            }
            FileMutationDispatch::Rejected(error) => {
                // A join or lifecycle failure can occur after the blocking
                // closure has started. It is never safe to classify it as a
                // queue rejection or allow this instance to keep serving.
                self.unavailable.store(true, AtomicOrdering::Release);
                let _ = self.recover_incomplete_file_operations_owned().await;
                Err(PersistenceError::Runtime(error))
            }
        }
    }

    /// Turns a prepared marker into a committed aggregate before exposing its
    /// value. Cleanup is intentionally non-fatal: a committed marker tells a
    /// later opener to retain the snapshots and finish removing metadata.
    async fn finalize_file_mutation<T>(&self, outcome: FileMutationOutcome<T>) -> Result<T, PersistenceError>
    where
        T: Send + 'static,
    {
        let FileMutationOutcome {
            value,
            finalize,
            cleanup,
            rollback: _,
        } = outcome;
        match self
            .run_blocking_mutation("dashboard-file-storage-finalize-mutation", finalize)
            .await
        {
            FileMutationDispatch::Completed(Ok(())) => {
                self.cleanup_committed_mutation(cleanup).await;
                Ok(value)
            }
            FileMutationDispatch::TimedOut { receipt, .. } => match Self::await_mutation_receipt(receipt).await {
                // `spawn_io` timed out, but the owned finalizer reported that
                // its committed marker is durable. Its value is authoritative.
                Ok(()) => {
                    self.cleanup_committed_mutation(cleanup).await;
                    Ok(value)
                }
                Err(error) => self.fail_closed_after_finalize(error).await,
            },
            FileMutationDispatch::Completed(Err(error)) => self.fail_closed_after_finalize(error).await,
            FileMutationDispatch::Rejected(error) => {
                self.fail_closed_after_finalize(PersistenceError::Runtime(error)).await
            }
        }
    }

    async fn cleanup_committed_mutation(&self, cleanup: Box<dyn FnOnce() -> Result<(), PersistenceError> + Send>) {
        // This work only removes committed metadata; the committed marker is
        // already the durable decision point. Unlike rollback/recovery, a
        // storage-I/O timeout here must not poison a store that has just
        // successfully committed its aggregate. Retain the completion receipt
        // so the blocking closure cannot outlive the mutation owner, then
        // leave any failed marker cleanup for deterministic reopen recovery.
        match self
            .run_blocking_mutation("dashboard-file-storage-cleanup-committed-mutation", cleanup)
            .await
        {
            FileMutationDispatch::Completed(Ok(()))
            | FileMutationDispatch::Completed(Err(_))
            | FileMutationDispatch::Rejected(_) => {}
            FileMutationDispatch::TimedOut { receipt, .. } => {
                let _ = Self::await_mutation_receipt(receipt).await;
            }
        }
    }

    /// Finalizer failure is post-admission ambiguity. Reconcile any durable
    /// marker, then keep this live instance unavailable until reopen rather
    /// than serving a possibly unacknowledged aggregate.
    async fn fail_closed_after_finalize<T>(&self, error: PersistenceError) -> Result<T, PersistenceError> {
        self.unavailable.store(true, AtomicOrdering::Release);
        let _ = self.recover_incomplete_file_operations_owned().await;
        Err(error)
    }

    /// Runs a blocking closure with a completion receipt retained by the
    /// runtime-owned mutation task. The HTTP request never owns this receiver.
    async fn run_blocking_mutation<T, F>(&self, name: &'static str, operation: F) -> FileMutationDispatch<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, PersistenceError> + Send + 'static,
    {
        let (sender, receipt) = oneshot::channel();
        match self
            .service_context
            .storage_io()
            .spawn_io(name, move || {
                let result = operation();
                let _ = sender.send(result);
            })
            .await
        {
            Ok(()) => FileMutationDispatch::Completed(Self::await_mutation_receipt(receipt).await),
            Err(error @ RuntimeError::BlockingTaskTimeoutStillRunning { .. }) => {
                FileMutationDispatch::TimedOut { error, receipt }
            }
            Err(error) => FileMutationDispatch::Rejected(error),
        }
    }

    async fn await_mutation_receipt<T>(
        receipt: oneshot::Receiver<Result<T, PersistenceError>>,
    ) -> Result<T, PersistenceError> {
        receipt.await.unwrap_or(Err(PersistenceError::ConnectionUnavailable))
    }

    /// Runs rollback work inside the same mutation owner. A recovery timeout
    /// is also awaited before the owner can release the File write gate.
    async fn run_recovery_operation_owned<F>(&self, name: &'static str, operation: F) -> Result<(), PersistenceError>
    where
        F: FnOnce() -> Result<(), PersistenceError> + Send + 'static,
    {
        match self.run_blocking_mutation(name, operation).await {
            FileMutationDispatch::Completed(result) => result,
            FileMutationDispatch::TimedOut { receipt, .. } => {
                self.unavailable.store(true, AtomicOrdering::Release);
                Self::await_mutation_receipt(receipt).await
            }
            FileMutationDispatch::Rejected(error) => Err(PersistenceError::Runtime(error)),
        }
    }

    /// Replays all marker-owned file operations while the repository write
    /// gate is held. This includes history rewrites, so a failed append never
    /// leaves a live File instance serving a temporarily absent day segment.
    async fn recover_incomplete_file_operations_owned(&self) -> Result<(), PersistenceError> {
        let root = self.root.clone();
        self.run_recovery_operation_owned("dashboard-file-storage-recover-file-operations", move || {
            recover_incomplete_snapshot_transactions(&root)?;
            session_file_store::recover_session_audit_transactions(&root)?;
            session_file_store::recover_session_touch_transactions(&root)?;
            session_file_store::recover_session_cleanup_transactions(&root)?;
            audit_file_store::recover_audit_rewrite_transactions(&root)?;
            history_file_store::recover_history_file_operations(&root)
        })
        .await
    }
}

fn is_pre_admission_rejection(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::BlockingQueueTimeout { .. } | RuntimeError::BlockingQueueFull { .. }
    )
}

fn rollback_jsonl_append(path: PathBuf, original_length: u64, existed: bool) -> Result<(), PersistenceError> {
    if !path.exists() {
        return Ok(());
    }
    if !existed && original_length == 0 {
        return std::fs::remove_file(path).map_err(PersistenceError::Io);
    }
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    file.set_len(original_length).map_err(PersistenceError::Io)?;
    file.sync_data().map_err(PersistenceError::Io)
}

fn reject_legacy_layout(root: &Path) -> Result<(), PersistenceError> {
    let former_defaults = [
        PathBuf::from("data/dashboard-interim-config.json"),
        PathBuf::from("data/monitor/consumer-monitor-config.json"),
    ];
    let root_relative_defaults = root.parent().and_then(|parent| {
        (root.file_name().is_some_and(|name| name == "dashboard")
            && parent.file_name().is_some_and(|name| name == "data"))
        .then(|| {
            [
                parent.join("dashboard-interim-config.json"),
                parent.join("monitor/consumer-monitor-config.json"),
            ]
        })
    });
    if root.join("dashboard-config.json").exists()
        || root.join("consumer-monitor-config.json").exists()
        || former_defaults.iter().any(|path| path.exists())
        || root_relative_defaults
            .as_ref()
            .is_some_and(|paths| paths.iter().any(|path| path.exists()))
    {
        return Err(PersistenceError::UnsupportedLayout);
    }
    Ok(())
}

fn initialize_manifest(root: &Path) -> Result<(), PersistenceError> {
    let path = root.join("manifest.json");
    if !path.exists() {
        // A missing manifest can follow an interrupted publication. Scan the
        // durable snapshots before declaring this a fresh directory.
        let _latest_revisions = recover_snapshot_collections(root)?;
        return write_json_new_file(&path, &new_manifest());
    }
    let file = File::open(&path).map_err(PersistenceError::Io)?;
    match serde_json::from_reader::<_, FileManifest>(file) {
        Ok(manifest) => validate_manifest_layout(&manifest),
        Err(_) => {
            // Snapshot files are the durable source of truth. A torn manifest
            // is reconstructed from the highest valid revision in each
            // collection while the directory lock excludes other writers.
            let _latest_revisions = recover_snapshot_collections(root)?;
            write_json_replace(&path, &new_manifest())
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedSnapshotWrite {
    collection: String,
    revision: u64,
    payload: Value,
}

/// Durable transaction intent retained between staging and the owner decision.
/// Separate prepared/committed filenames avoid an in-place replacement window
/// on Windows: committed always wins during recovery.
#[derive(Debug, Clone)]
struct PreparedSnapshotTransaction {
    prepared_marker: PathBuf,
    committed_marker: PathBuf,
}

impl PreparedSnapshotTransaction {
    fn finalize(&self) -> Result<(), PersistenceError> {
        commit_prepared_snapshot_transaction(&self.prepared_marker, &self.committed_marker)
    }

    fn cleanup(&self) -> Result<(), PersistenceError> {
        cleanup_committed_snapshot_transaction(Some(&self.prepared_marker), &self.committed_marker)
    }

    fn rollback(&self, fail_after_deletions: Option<usize>) -> Result<(), PersistenceError> {
        rollback_prepared_snapshot_transaction(&self.prepared_marker, &self.committed_marker, fail_after_deletions)
    }
}

fn recover_incomplete_snapshot_transactions(root: &Path) -> Result<(), PersistenceError> {
    let directory = root.join("transactions");
    if !directory.exists() {
        return Ok(());
    }
    let mut prepared_markers = BTreeMap::new();
    let mut committed_markers = BTreeMap::new();
    for entry in std::fs::read_dir(&directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        if !entry.file_type().map_err(PersistenceError::Io)?.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return Err(PersistenceError::CorruptedData);
        };
        if name.ends_with(".session-audit.prepared.json")
            || name.ends_with(".session-audit.committed.json")
            || name.ends_with(".session-touch.prepared.json")
            || name.ends_with(".session-touch.committed.json")
            || name.ends_with(".session-touch-stage.json")
            || name.ends_with(".session-touch-backup.json")
            || name.ends_with(".session-cleanup.prepared.json")
            || name.ends_with(".session-cleanup.committed.json")
            || name.ends_with(".audit-rewrite.prepared.json")
            || name.ends_with(".audit-rewrite.committed.json")
        {
            // Session/audit markers share the transaction directory but have
            // their own recovery grammar and must never be interpreted as a
            // snapshot aggregate.
            continue;
        } else if let Some(transaction_id) = name.strip_suffix(".prepared.json") {
            prepared_markers.insert(transaction_id.to_string(), path);
        } else if let Some(transaction_id) = name.strip_suffix(".committed.json") {
            committed_markers.insert(transaction_id.to_string(), path);
        } else if path.extension().is_some_and(|extension| extension == "json") {
            // Markers written before the prepared/committed split were always
            // rollback intents, so remain conservative on upgrade.
            prepared_markers.insert(name.to_string(), path);
        }
    }
    for (transaction_id, committed_marker) in committed_markers {
        let _transaction = read_snapshot_transaction(&committed_marker)?;
        let prepared_marker = prepared_markers.remove(&transaction_id);
        cleanup_committed_snapshot_transaction(prepared_marker.as_deref(), &committed_marker)?;
    }
    for prepared_marker in prepared_markers.into_values() {
        let committed_marker = committed_marker_for_prepared(&prepared_marker)?;
        rollback_prepared_snapshot_transaction(&prepared_marker, &committed_marker, None)?;
    }
    Ok(())
}

fn committed_marker_for_prepared(prepared_marker: &Path) -> Result<PathBuf, PersistenceError> {
    let name = prepared_marker
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(PersistenceError::CorruptedData)?;
    if let Some(transaction_id) = name.strip_suffix(".prepared.json") {
        return Ok(prepared_marker.with_file_name(format!("{transaction_id}.committed.json")));
    }
    // Pre-split markers never had a companion committed marker. Keep a
    // non-matching sentinel path so recovery treats them as rollback intents.
    Ok(prepared_marker.with_extension("committed.json"))
}

fn prepare_transaction_writes(
    root: &Path,
    writes: &[FileSnapshotTransactionWrite],
    fail_after_writes: Option<usize>,
) -> Result<PreparedSnapshotTransaction, PersistenceError> {
    let mut prepared_writes = Vec::with_capacity(writes.len());
    for write in writes {
        validate_collection(&write.collection)?;
        let directory = root.join(&write.collection).join("snapshots");
        let current_revision = load_latest_snapshot_file(&directory)?.map_or(0, |snapshot| snapshot.revision);
        if current_revision != write.expected_revision {
            return Err(PersistenceError::Conflict);
        }
        prepared_writes.push(PreparedSnapshotWrite {
            collection: write.collection.clone(),
            revision: current_revision.checked_add(1).ok_or(PersistenceError::Conflict)?,
            payload: write.payload.clone(),
        });
    }
    prepare_snapshot_transaction(root, prepared_writes, fail_after_writes, None)
}

fn prepare_transaction_writes_with_audit(
    root: &Path,
    writes: &[FileSnapshotTransactionWrite],
    audit: FileSnapshotAuditAppend,
) -> Result<PreparedSnapshotTransaction, PersistenceError> {
    let mut prepared_writes = Vec::with_capacity(writes.len());
    for write in writes {
        validate_collection(&write.collection)?;
        let directory = root.join(&write.collection).join("snapshots");
        let current_revision = load_latest_snapshot_file(&directory)?.map_or(0, |snapshot| snapshot.revision);
        if current_revision != write.expected_revision {
            return Err(PersistenceError::Conflict);
        }
        prepared_writes.push(PreparedSnapshotWrite {
            collection: write.collection.clone(),
            revision: current_revision.checked_add(1).ok_or(PersistenceError::Conflict)?,
            payload: write.payload.clone(),
        });
    }
    prepare_snapshot_transaction(root, prepared_writes, None, Some(audit))
}

fn prepare_snapshot_transaction(
    root: &Path,
    writes: Vec<PreparedSnapshotWrite>,
    fail_after_writes: Option<usize>,
    audit: Option<FileSnapshotAuditAppend>,
) -> Result<PreparedSnapshotTransaction, PersistenceError> {
    if writes.is_empty() {
        return Err(PersistenceError::InvalidConfig(
            "snapshot transaction requires at least one write".to_string(),
        ));
    }
    let records = writes
        .iter()
        .map(|write| {
            validate_collection(&write.collection)?;
            let target = root
                .join(&write.collection)
                .join("snapshots")
                .join(format!("{:020}.json", write.revision));
            // An intent can only name snapshots owned by this transaction.
            // Otherwise recovery could delete an earlier committed snapshot
            // after a failed explicit duplicate write.
            if target.exists() {
                return Err(PersistenceError::Conflict);
            }
            Ok(FileSnapshotTransactionRecord {
                collection: write.collection.clone(),
                revision: write.revision,
            })
        })
        .collect::<Result<Vec<_>, PersistenceError>>()?;
    let transaction_directory = root.join("transactions");
    std::fs::create_dir_all(&transaction_directory).map_err(PersistenceError::Io)?;
    let transaction_id = format!(
        "{:020}-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        std::process::id()
    );
    let prepared_marker = transaction_directory.join(format!("{transaction_id}.prepared.json"));
    let committed_marker = transaction_directory.join(format!("{transaction_id}.committed.json"));
    write_json_new_file(
        &prepared_marker,
        &FileSnapshotTransaction {
            format_version: FORMAT_VERSION,
            writes: records,
            audit,
        },
    )?;
    for (index, write) in writes.into_iter().enumerate() {
        let target = root
            .join(&write.collection)
            .join("snapshots")
            .join(format!("{:020}.json", write.revision));
        write_json_new_file(
            &target,
            &FileSnapshot {
                format_version: FORMAT_VERSION,
                revision: write.revision,
                payload: write.payload,
            },
        )?;
        if fail_after_writes.is_some_and(|count| count == index + 1) {
            return Err(PersistenceError::Io(std::io::Error::other(
                "injected multi-collection publication failure",
            )));
        }
    }
    Ok(PreparedSnapshotTransaction {
        prepared_marker,
        committed_marker,
    })
}

fn prepare_snapshot_audit_append(root: &Path, created_at_ms: i64) -> Result<FileSnapshotAuditAppend, PersistenceError> {
    let day = chrono::DateTime::from_timestamp_millis(created_at_ms)
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string();
    let relative_path = format!("audit/{day}.jsonl");
    let path = root.join(&relative_path);
    std::fs::create_dir_all(root.join("audit")).map_err(PersistenceError::Io)?;
    truncate_incomplete_tail(&path)?;
    let existed = path.exists();
    let original_length = if existed {
        std::fs::metadata(&path).map_err(PersistenceError::Io)?.len()
    } else {
        0
    };
    Ok(FileSnapshotAuditAppend {
        relative_path,
        existed,
        original_length,
    })
}

fn append_snapshot_audit(
    root: &Path,
    append: &FileSnapshotAuditAppend,
    encoded: &[u8],
) -> Result<(), PersistenceError> {
    let path = audit_append_path(root, append)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    file.write_all(encoded).map_err(PersistenceError::Io)?;
    file.write_all(b"\n").map_err(PersistenceError::Io)?;
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_data().map_err(PersistenceError::Io)
}

fn audit_append_path(root: &Path, append: &FileSnapshotAuditAppend) -> Result<PathBuf, PersistenceError> {
    let path = Path::new(&append.relative_path);
    if path.components().count() != 2
        || path.parent() != Some(Path::new("audit"))
        || path.extension().is_none_or(|extension| extension != "jsonl")
    {
        return Err(PersistenceError::CorruptedData);
    }
    Ok(root.join(path))
}

fn commit_prepared_snapshot_transaction(
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    if committed_marker.exists() {
        let _transaction = read_snapshot_transaction(committed_marker)?;
        return Ok(());
    }
    let transaction = read_snapshot_transaction(prepared_marker)?;
    write_json_new_file(committed_marker, &transaction)
}

fn cleanup_committed_snapshot_transaction(
    prepared_marker: Option<&Path>,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    remove_file_if_exists(prepared_marker)?;
    remove_file_if_exists(Some(committed_marker))
}

fn rollback_prepared_snapshot_transaction(
    prepared_marker: &Path,
    committed_marker: &Path,
    fail_after_deletions: Option<usize>,
) -> Result<(), PersistenceError> {
    if committed_marker.exists() {
        return Err(PersistenceError::Conflict);
    }
    let transaction = read_snapshot_transaction(prepared_marker)?;
    let root = prepared_marker
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| PersistenceError::InvalidConfig("transaction marker has no storage root".to_string()))?;
    let mut rollback_failure = None;
    for (index, write) in transaction.writes.into_iter().rev().enumerate() {
        let snapshot = root
            .join(write.collection)
            .join("snapshots")
            .join(format!("{:020}.json", write.revision));
        if !snapshot.exists() {
            continue;
        }
        if fail_after_deletions.is_some_and(|count| count == index) {
            rollback_failure = Some(PersistenceError::Io(std::io::Error::other(
                "injected multi-collection rollback deletion failure",
            )));
            continue;
        }
        if let Err(error) = std::fs::remove_file(snapshot) {
            rollback_failure = Some(PersistenceError::Io(error));
        }
    }
    if let Some(error) = rollback_failure {
        return Err(error);
    }
    if let Some(audit) = transaction.audit {
        rollback_jsonl_append(audit_append_path(root, &audit)?, audit.original_length, audit.existed)?;
    }
    remove_file_if_exists(Some(prepared_marker))
}

fn read_snapshot_transaction(marker: &Path) -> Result<FileSnapshotTransaction, PersistenceError> {
    let transaction: FileSnapshotTransaction =
        serde_json::from_reader(File::open(marker).map_err(PersistenceError::Io)?)
            .map_err(|_| PersistenceError::CorruptedData)?;
    if transaction.format_version != FORMAT_VERSION || transaction.writes.is_empty() {
        return Err(PersistenceError::CorruptedData);
    }
    for write in &transaction.writes {
        validate_collection(&write.collection)?;
    }
    if let Some(audit) = &transaction.audit {
        let _ = audit_append_path(
            marker
                .parent()
                .and_then(Path::parent)
                .ok_or(PersistenceError::CorruptedData)?,
            audit,
        )?;
    }
    Ok(transaction)
}

fn remove_file_if_exists(path: Option<&Path>) -> Result<(), PersistenceError> {
    if let Some(path) = path.filter(|path| path.exists()) {
        std::fs::remove_file(path).map_err(PersistenceError::Io)?;
    }
    Ok(())
}

fn new_manifest() -> FileManifest {
    FileManifest {
        format_version: FORMAT_VERSION,
        backend: "file".to_string(),
        created_at_ms: Utc::now().timestamp_millis(),
    }
}

fn validate_file_health(root: &Path) -> Result<u64, PersistenceError> {
    reject_legacy_layout(root)?;
    validate_manifest(root)?;
    let lock_directory = root.join("locks");
    std::fs::read_dir(root).map_err(PersistenceError::Io)?;
    std::fs::metadata(lock_directory.join("dashboard.lock")).map_err(PersistenceError::Io)?;
    let available_bytes = fs4::available_space(root).map_err(PersistenceError::Io)?;
    validate_snapshot_files(root)?;

    let probe = lock_directory.join(format!(
        ".health-{}-{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .truncate(false)
        .open(&probe)
        .map_err(PersistenceError::Io)?;
    file.write_all(b"dashboard storage health probe\n")
        .map_err(PersistenceError::Io)?;
    file.sync_data().map_err(PersistenceError::Io)?;
    drop(file);
    std::fs::remove_file(probe).map_err(PersistenceError::Io)?;
    Ok(available_bytes)
}

fn validate_manifest(root: &Path) -> Result<(), PersistenceError> {
    let file = File::open(root.join("manifest.json")).map_err(PersistenceError::Io)?;
    let manifest: FileManifest = serde_json::from_reader(file).map_err(|_| PersistenceError::UnsupportedLayout)?;
    validate_manifest_layout(&manifest)
}

fn validate_manifest_layout(manifest: &FileManifest) -> Result<(), PersistenceError> {
    if manifest.format_version != FORMAT_VERSION || manifest.backend != "file" {
        return Err(PersistenceError::UnsupportedLayout);
    }
    Ok(())
}

fn recover_snapshot_collections(root: &Path) -> Result<BTreeMap<String, u64>, PersistenceError> {
    let mut latest_revisions = BTreeMap::new();
    for collection in std::fs::read_dir(root).map_err(PersistenceError::Io)? {
        let collection = collection.map_err(PersistenceError::Io)?;
        if !collection.file_type().map_err(PersistenceError::Io)?.is_dir() {
            continue;
        }
        let snapshots = collection.path().join("snapshots");
        if !snapshots.exists() {
            continue;
        }
        let mut latest_revision: Option<u64> = None;
        let mut contains_snapshot_file = false;
        for snapshot in std::fs::read_dir(&snapshots).map_err(PersistenceError::Io)? {
            let snapshot = snapshot.map_err(PersistenceError::Io)?;
            if snapshot.path().extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            contains_snapshot_file = true;
            let file = File::open(snapshot.path()).map_err(PersistenceError::Io)?;
            match serde_json::from_reader::<_, FileSnapshot>(file) {
                Ok(snapshot) if snapshot.format_version == FORMAT_VERSION => {
                    latest_revision =
                        Some(latest_revision.map_or(snapshot.revision, |latest| latest.max(snapshot.revision)));
                }
                Ok(_) => return Err(PersistenceError::UnsupportedLayout),
                Err(_) => continue,
            }
        }
        if contains_snapshot_file && latest_revision.is_none() {
            return Err(PersistenceError::CorruptedData);
        }
        if let Some(revision) = latest_revision {
            let collection = collection.file_name().to_string_lossy().into_owned();
            latest_revisions.insert(collection, revision);
        }
    }
    Ok(latest_revisions)
}

fn validate_snapshot_files(root: &Path) -> Result<(), PersistenceError> {
    recover_snapshot_collections(root).map(|_| ())
}

fn write_json_new_file<T: Serialize>(target: &Path, value: &T) -> Result<(), PersistenceError> {
    let parent = target
        .parent()
        .ok_or_else(|| PersistenceError::InvalidConfig("file target has no parent".to_string()))?;
    std::fs::create_dir_all(parent).map_err(PersistenceError::Io)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        target.file_name().and_then(|name| name.to_str()).unwrap_or("snapshot"),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(PersistenceError::Io)?;
    serde_json::to_writer(&mut file, value).map_err(PersistenceError::Serialization)?;
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)?;
    drop(file);
    // A hard link creates the target name only when it does not exist. Unlike
    // rename on Unix, it cannot replace a snapshot published by a concurrent
    // writer or a prior process.
    match std::fs::hard_link(&temporary, target) {
        Ok(()) => {
            let _ = std::fs::remove_file(temporary);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = std::fs::remove_file(temporary);
            Err(PersistenceError::Conflict)
        }
        Err(error) => {
            let _ = std::fs::remove_file(temporary);
            Err(PersistenceError::Io(error))
        }
    }
}

fn write_json_replace<T: Serialize>(target: &Path, value: &T) -> Result<(), PersistenceError> {
    let parent = target
        .parent()
        .ok_or_else(|| PersistenceError::InvalidConfig("file target has no parent".to_string()))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        target.file_name().and_then(|name| name.to_str()).unwrap_or("manifest"),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(PersistenceError::Io)?;
    serde_json::to_writer(&mut file, value).map_err(PersistenceError::Serialization)?;
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)?;
    drop(file);
    if target.exists() {
        std::fs::remove_file(target).map_err(PersistenceError::Io)?;
    }
    std::fs::rename(&temporary, target).map_err(PersistenceError::Io)
}

fn load_latest_snapshot_file(directory: &Path) -> Result<Option<FileSnapshot>, PersistenceError> {
    if !directory.exists() {
        return Ok(None);
    }
    let mut snapshots = Vec::new();
    for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        if entry.path().extension().is_some_and(|extension| extension == "json") {
            let file = File::open(entry.path()).map_err(PersistenceError::Io)?;
            if let Ok(snapshot) = serde_json::from_reader::<_, FileSnapshot>(file)
                && snapshot.format_version == FORMAT_VERSION
            {
                snapshots.push(snapshot);
            }
        }
    }
    snapshots.sort_by(|left, right| {
        if left.revision == right.revision {
            Ordering::Equal
        } else if left.revision < right.revision {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });
    Ok(snapshots.pop())
}

fn truncate_incomplete_tail(path: &Path) -> Result<(), PersistenceError> {
    if !path.exists() {
        return Ok(());
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    let length = file.metadata().map_err(PersistenceError::Io)?.len();
    if length == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::End(-1)).map_err(PersistenceError::Io)?;
    let mut final_byte = [0_u8; 1];
    file.read_exact(&mut final_byte).map_err(PersistenceError::Io)?;
    if final_byte != [b'\n'] {
        // Only the incomplete final record is recoverable. Locate the last
        // complete JSONL delimiter from the end in fixed-size blocks so a
        // large audit file is never loaded to recover one torn append.
        const BLOCK: usize = 8 * 1024;
        let mut end = length;
        let mut complete_length = 0_u64;
        while end > 0 {
            let start = end.saturating_sub(BLOCK as u64);
            let block_length = usize::try_from(end - start).map_err(|_| PersistenceError::CorruptedData)?;
            let mut block = vec![0_u8; block_length];
            file.seek(SeekFrom::Start(start)).map_err(PersistenceError::Io)?;
            file.read_exact(&mut block).map_err(PersistenceError::Io)?;
            if let Some(position) = block.iter().rposition(|byte| *byte == b'\n') {
                complete_length = start + position as u64 + 1;
                break;
            }
            end = start;
        }
        let length = complete_length;
        file.set_len(length).map_err(PersistenceError::Io)?;
        file.seek(SeekFrom::Start(length)).map_err(PersistenceError::Io)?;
        file.sync_data().map_err(PersistenceError::Io)?;
    }
    Ok(())
}

fn read_jsonl_file(path: &Path) -> Result<Vec<Value>, PersistenceError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut contents = String::new();
    File::open(path)
        .map_err(PersistenceError::Io)?
        .read_to_string(&mut contents)
        .map_err(PersistenceError::Io)?;
    contents
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .map(|line| serde_json::from_str(line.trim_end()).map_err(|_| PersistenceError::CorruptedData))
        .collect()
}

fn validate_segment(value: &str) -> Result<(), PersistenceError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(PersistenceError::InvalidConfig(
            "file collection names must contain only ASCII letters, digits, hyphens, or underscores".to_string(),
        ));
    }
    Ok(())
}

fn validate_collection(value: &str) -> Result<(), PersistenceError> {
    if value.split('/').any(|segment| validate_segment(segment).is_err()) {
        return Err(PersistenceError::InvalidConfig(
            "file collection names must contain safe path segments".to_string(),
        ));
    }
    Ok(())
}

fn non_zero(value: i64) -> Option<i64> {
    (value > 0).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlPoolConfig;
    use crate::model::AuditAction;
    use crate::model::AuditActor;
    use crate::model::AuditEvent;
    use crate::model::AuditOutcome;
    use crate::model::AuditResourceType;
    use crate::model::NewSession;
    use crate::model::SessionTokenHash;
    use crate::model::StorageBackend;
    use crate::persistence::StorageStatus;
    use crate::persistence::audit_repository::AuditQuery;
    use crate::persistence::session_repository::SessionQuery;
    use crate::service::readiness_status_from_storage;
    use rocketmq_runtime::BlockingPoolPolicy;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use serde_json::json;
    use std::time::Duration;

    fn file_config(root: PathBuf) -> StorageConfig {
        StorageConfig {
            backend: StorageBackend::File,
            data_path: root,
            database_url: None,
            pool: SqlPoolConfig::default(),
        }
    }

    fn audit_event(created_at_ms: i64) -> AuditEvent {
        AuditEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            request_id: uuid::Uuid::now_v7().to_string(),
            actor: AuditActor::admin("operator"),
            action: AuditAction::ConfigNameserverAdd,
            resource_type: AuditResourceType::Nameserver,
            resource_name: Some("127.0.0.1:9876".to_string()),
            environment_id: None,
            outcome: AuditOutcome::Succeeded,
            detail: None,
            created_at_ms,
        }
    }

    fn expired_session(token_hash: SessionTokenHash) -> NewSession {
        NewSession {
            session_id: uuid::Uuid::now_v7().to_string(),
            token_hash,
            username: "expired-session-owner".to_string(),
            created_at_ms: 1,
            expires_at_ms: 2,
        }
    }

    fn active_session(token_hash: SessionTokenHash, username: &str) -> NewSession {
        NewSession {
            session_id: uuid::Uuid::now_v7().to_string(),
            token_hash,
            username: username.to_string(),
            created_at_ms: 1,
            expires_at_ms: 10_000,
        }
    }

    fn timeout_runtime_config() -> RuntimeConfig {
        let mut config = RuntimeConfig::default();
        config.blocking_lane_policies.storage_io = BlockingPoolPolicy {
            name: "dashboard-file-timeout-test.storage-io".to_string(),
            max_concurrency: 1,
            max_queue_depth: 4,
            queue_timeout: Duration::from_secs(1),
            task_timeout: Duration::from_millis(250),
            warn_after: Duration::from_millis(1),
        };
        config
    }

    fn cancellation_runtime_config() -> RuntimeConfig {
        let mut config = timeout_runtime_config();
        // The request is aborted immediately after the blocking hook reports
        // admission, so this remains a pre-timeout cancellation regression.
        config.blocking_lane_policies.storage_io.task_timeout = Duration::from_millis(500);
        config
    }

    fn install_mutation_blocker(
        store: &FilePersistence,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::SyncSender<()>) {
        install_mutation_blocker_with_rollback_failure(store, None)
    }

    fn install_mutation_blocker_with_rollback_failure(
        store: &FilePersistence,
        rollback_failure_after_deletions: Option<usize>,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::SyncSender<()>) {
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        store.mutation_blocker.install(Arc::new(TestMutationBlocker {
            started: started_sender,
            release: std::sync::Mutex::new(release_receiver),
            panic_after_release: false,
            rollback_failure_after_deletions,
        }));
        (started_receiver, release_sender)
    }

    fn install_panicking_mutation_blocker(
        store: &FilePersistence,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::SyncSender<()>) {
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        store.mutation_blocker.install(Arc::new(TestMutationBlocker {
            started: started_sender,
            release: std::sync::Mutex::new(release_receiver),
            panic_after_release: true,
            rollback_failure_after_deletions: None,
        }));
        (started_receiver, release_sender)
    }

    fn install_committed_cleanup_blocker(
        store: &FilePersistence,
        fail_after_release: bool,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::SyncSender<()>) {
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        store.install_test_committed_cleanup_blocker(started_sender, release_receiver, fail_after_release);
        (started_receiver, release_sender)
    }

    async fn release_mutation_after_storage_timeout(
        started: std::sync::mpsc::Receiver<()>,
        release: std::sync::mpsc::SyncSender<()>,
    ) {
        tokio::task::spawn_blocking(move || started.recv())
            .await
            .expect("wait for mutation hook task")
            .expect("mutation hook must start");
        tokio::time::sleep(Duration::from_millis(500)).await;
        release.send(()).expect("release mutation hook");
    }

    async fn wait_for_mutation_start(started: std::sync::mpsc::Receiver<()>) {
        tokio::task::spawn_blocking(move || started.recv())
            .await
            .expect("wait for mutation hook task")
            .expect("mutation hook must start");
    }

    #[test]
    fn cancelled_cas_request_leaves_the_owned_mutation_to_complete_and_release_its_gate() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(cancellation_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("cancelled-cas-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed snapshot");
            let (started, release) = install_mutation_blocker(&store);
            let request_store = store.clone();
            let request = tokio::spawn(async move {
                request_store
                    .compare_and_write_snapshot("config", 1, json!({"value": "committed"}))
                    .await
            });
            wait_for_mutation_start(started).await;
            request.abort();
            assert!(request.await.expect_err("cancelled request").is_cancelled());
            release.send(()).expect("release mutation hook");
            store.mutation_blocker.clear();
            let gate = tokio::time::timeout(Duration::from_secs(1), store.write_guard())
                .await
                .expect("runtime-owned mutation must release its gate");
            drop(gate);
            assert_eq!(
                store
                    .load_latest_snapshot("config")
                    .await
                    .expect("read completed mutation")
                    .expect("completed snapshot")
                    .payload,
                json!({"value": "committed"})
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn cancelled_multi_collection_delete_request_keeps_its_owned_gate_and_receipt() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(cancellation_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("cancelled-delete-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("environments/cancelled-delete", 1, json!({"deleted": false}))
                .await
                .expect("seed environment");
            store
                .write_snapshot("monitors/cancelled-delete", 1, json!({"rules": ["before"]}))
                .await
                .expect("seed monitor rules");
            let (started, release) = install_mutation_blocker(&store);
            let request_store = store.clone();
            let request = tokio::spawn(async move {
                let write_guard = request_store.write_guard().await;
                request_store
                    .publish_snapshot_transaction_locked(
                        write_guard,
                        vec![
                            FileSnapshotTransactionWrite {
                                collection: "environments/cancelled-delete".to_string(),
                                expected_revision: 1,
                                payload: json!({"deleted": true}),
                            },
                            FileSnapshotTransactionWrite {
                                collection: "monitors/cancelled-delete".to_string(),
                                expected_revision: 1,
                                payload: json!({"rules": []}),
                            },
                        ],
                    )
                    .await
            });
            wait_for_mutation_start(started).await;
            request.abort();
            assert!(request.await.expect_err("cancelled request").is_cancelled());
            release.send(()).expect("release mutation hook");
            store.mutation_blocker.clear();
            let gate = tokio::time::timeout(Duration::from_secs(1), store.write_guard())
                .await
                .expect("runtime-owned transaction must release its gate");
            drop(gate);
            assert_eq!(
                store
                    .load_latest_snapshot("environments/cancelled-delete")
                    .await
                    .expect("read completed environment delete")
                    .expect("environment snapshot")
                    .payload,
                json!({"deleted": true})
            );
            assert_eq!(
                store
                    .load_latest_snapshot("monitors/cancelled-delete")
                    .await
                    .expect("read completed monitor delete")
                    .expect("monitor snapshot")
                    .payload,
                json!({"rules": []})
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn post_dispatch_join_failure_poisons_the_active_file_store() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("post-dispatch-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed snapshot");
            let (started, release) = install_panicking_mutation_blocker(&store);
            let (result, ()) = tokio::join!(
                store.compare_and_write_snapshot("config", 1, json!({"value": "unknown"})),
                async {
                    wait_for_mutation_start(started).await;
                    release.send(()).expect("release mutation hook");
                },
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(RuntimeError::BlockingJoin { .. }))
            ));
            store.mutation_blocker.clear();
            assert!(matches!(
                store.load_latest_snapshot("config").await,
                Err(PersistenceError::ConnectionUnavailable)
            ));
            assert_eq!(store.storage_health().await.status, StorageStatus::Unavailable);
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn cas_timeout_waits_for_rollback_before_exposing_or_reopening_file_state() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("cas-timeout-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed snapshot");
            let (started, release) = install_mutation_blocker(&store);
            let (result, ()) = tokio::join!(
                store.compare_and_write_snapshot("config", 1, json!({"value": "late"})),
                release_mutation_after_storage_timeout(started, release),
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(
                    RuntimeError::BlockingTaskTimeoutStillRunning { .. }
                ))
            ));
            store.mutation_blocker.clear();
            let snapshot = store
                .load_latest_snapshot("config")
                .await
                .expect("active store remains readable after rollback")
                .expect("original snapshot");
            assert_eq!(snapshot.revision, 1);
            assert_eq!(snapshot.payload, json!({"value": "before"}));
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root),
                owner.root_context().component("cas-timeout-file-store-reopen"),
            )
            .await
            .expect("reopen after CAS rollback");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("read reopened snapshot")
                    .expect("original snapshot")
                    .payload,
                json!({"value": "before"})
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn multi_collection_delete_timeout_rolls_back_before_active_or_reopened_reads() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("delete-timeout-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("environments/timeout-delete", 1, json!({"deleted": false}))
                .await
                .expect("seed environment");
            store
                .write_snapshot("monitors/timeout-delete", 1, json!({"rules": ["before"]}))
                .await
                .expect("seed monitor rules");
            let (started, release) = install_mutation_blocker(&store);
            let write_guard = store.write_guard().await;
            let (result, ()) = tokio::join!(
                store.publish_snapshot_transaction_locked(
                    write_guard,
                    vec![
                        FileSnapshotTransactionWrite {
                            collection: "environments/timeout-delete".to_string(),
                            expected_revision: 1,
                            payload: json!({"deleted": true}),
                        },
                        FileSnapshotTransactionWrite {
                            collection: "monitors/timeout-delete".to_string(),
                            expected_revision: 1,
                            payload: json!({"rules": []}),
                        },
                    ]
                ),
                release_mutation_after_storage_timeout(started, release),
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(
                    RuntimeError::BlockingTaskTimeoutStillRunning { .. }
                ))
            ));
            store.mutation_blocker.clear();
            assert_eq!(
                store
                    .load_latest_snapshot("environments/timeout-delete")
                    .await
                    .expect("active environment read")
                    .expect("environment snapshot")
                    .payload,
                json!({"deleted": false})
            );
            assert_eq!(
                store
                    .load_latest_snapshot("monitors/timeout-delete")
                    .await
                    .expect("active monitor read")
                    .expect("monitor snapshot")
                    .payload,
                json!({"rules": ["before"]})
            );
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root),
                owner.root_context().component("delete-timeout-file-store-reopen"),
            )
            .await
            .expect("reopen after delete rollback");
            assert_eq!(
                reopened
                    .load_latest_snapshot("environments/timeout-delete")
                    .await
                    .expect("read reopened environment")
                    .expect("environment snapshot")
                    .payload,
                json!({"deleted": false})
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn committed_cleanup_timeout_waits_for_completion_without_poisoning_the_active_store() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("cleanup-timeout-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed snapshot");
            let (started, release) = install_committed_cleanup_blocker(&store, false);
            let (result, ()) = tokio::join!(
                store.compare_and_write_snapshot("config", 1, json!({"value": "committed"})),
                release_mutation_after_storage_timeout(started, release),
            );
            assert_eq!(result.expect("committed CAS"), 2);
            store.clear_test_committed_cleanup_blocker();
            assert_eq!(store.storage_health().await.status, StorageStatus::Available);
            assert_eq!(
                store
                    .load_latest_snapshot("config")
                    .await
                    .expect("read committed snapshot")
                    .expect("committed snapshot")
                    .payload,
                json!({"value": "committed"})
            );
            store
                .write_snapshot("config", 3, json!({"value": "still-available"}))
                .await
                .expect("post-cleanup write must remain available");
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn committed_cleanup_failure_retains_marker_and_reopen_keeps_the_new_aggregate() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("cleanup-failure-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed snapshot");
            let (started, release) = install_committed_cleanup_blocker(&store, true);
            let (result, ()) = tokio::join!(
                store.compare_and_write_snapshot("config", 1, json!({"value": "committed"})),
                async {
                    wait_for_mutation_start(started).await;
                    release.send(()).expect("release cleanup hook");
                },
            );
            assert_eq!(result.expect("committed CAS"), 2);
            store.clear_test_committed_cleanup_blocker();
            assert_eq!(store.storage_health().await.status, StorageStatus::Available);
            assert_eq!(
                store
                    .load_latest_snapshot("config")
                    .await
                    .expect("active store remains readable after cleanup failure")
                    .expect("committed snapshot")
                    .payload,
                json!({"value": "committed"})
            );
            assert!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory")
                    .next()
                    .is_some(),
                "failed cleanup must leave a committed recovery marker"
            );
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("cleanup-failure-file-store-reopen"),
            )
            .await
            .expect("reopen committed aggregate");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("read committed aggregate")
                    .expect("committed aggregate")
                    .payload,
                json!({"value": "committed"})
            );
            assert!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory")
                    .next()
                    .is_none(),
                "reopen must clear committed metadata after preserving snapshots"
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn single_snapshot_timeout_with_failed_compensation_poisoning_recovers_on_reopen() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("single-compensation-failure-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed snapshot");
            let (started, release) = install_mutation_blocker_with_rollback_failure(&store, Some(0));
            let (result, ()) = tokio::join!(
                store.compare_and_write_snapshot("config", 1, json!({"value": "staged"})),
                release_mutation_after_storage_timeout(started, release),
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(
                    RuntimeError::BlockingTaskTimeoutStillRunning { .. }
                ))
            ));
            store.mutation_blocker.clear();
            assert!(matches!(
                store.load_latest_snapshot("config").await,
                Err(PersistenceError::ConnectionUnavailable)
            ));
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("single-compensation-failure-reopen"),
            )
            .await
            .expect("marker-backed recovery");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("load recovered snapshot")
                    .expect("original snapshot")
                    .payload,
                json!({"value": "before"})
            );
            assert!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory")
                    .next()
                    .is_none(),
                "successful reopen cleanup must consume the prepared marker"
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn multi_snapshot_timeout_with_failed_compensation_poisoning_recovers_on_reopen() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("multi-compensation-failure-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("environments/compensation", 1, json!({"deleted": false}))
                .await
                .expect("seed environment");
            store
                .write_snapshot("monitors/compensation", 1, json!({"rules": ["before"]}))
                .await
                .expect("seed monitors");
            let (started, release) = install_mutation_blocker_with_rollback_failure(&store, Some(0));
            let write_guard = store.write_guard().await;
            let (result, ()) = tokio::join!(
                store.publish_snapshot_transaction_locked(
                    write_guard,
                    vec![
                        FileSnapshotTransactionWrite {
                            collection: "environments/compensation".to_string(),
                            expected_revision: 1,
                            payload: json!({"deleted": true}),
                        },
                        FileSnapshotTransactionWrite {
                            collection: "monitors/compensation".to_string(),
                            expected_revision: 1,
                            payload: json!({"rules": []}),
                        },
                    ],
                ),
                release_mutation_after_storage_timeout(started, release),
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(
                    RuntimeError::BlockingTaskTimeoutStillRunning { .. }
                ))
            ));
            store.mutation_blocker.clear();
            assert!(matches!(
                store.load_latest_snapshot("environments/compensation").await,
                Err(PersistenceError::ConnectionUnavailable)
            ));
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("multi-compensation-failure-reopen"),
            )
            .await
            .expect("marker-backed recovery");
            assert_eq!(
                reopened
                    .load_latest_snapshot("environments/compensation")
                    .await
                    .expect("load recovered environment")
                    .expect("original environment")
                    .payload,
                json!({"deleted": false})
            );
            assert_eq!(
                reopened
                    .load_latest_snapshot("monitors/compensation")
                    .await
                    .expect("load recovered monitors")
                    .expect("original monitors")
                    .payload,
                json!({"rules": ["before"]})
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn prepared_single_and_multi_snapshot_intents_roll_back_after_interruption() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("prepared-interruption-file-store"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed config");
            store
                .write_snapshot("environments/interruption", 1, json!({"deleted": false}))
                .await
                .expect("seed environment");
            store
                .write_snapshot("monitors/interruption", 1, json!({"rules": ["before"]}))
                .await
                .expect("seed monitors");

            let _single = prepare_snapshot_transaction(
                &root,
                vec![PreparedSnapshotWrite {
                    collection: "config".to_string(),
                    revision: 2,
                    payload: json!({"value": "interrupted"}),
                }],
                None,
                None,
            )
            .expect("stage single intent");
            let _multi = prepare_snapshot_transaction(
                &root,
                vec![
                    PreparedSnapshotWrite {
                        collection: "environments/interruption".to_string(),
                        revision: 2,
                        payload: json!({"deleted": true}),
                    },
                    PreparedSnapshotWrite {
                        collection: "monitors/interruption".to_string(),
                        revision: 2,
                        payload: json!({"rules": []}),
                    },
                ],
                None,
                None,
            )
            .expect("stage multi intent");
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root),
                owner.root_context().component("prepared-interruption-reopen"),
            )
            .await
            .expect("recover interrupted intents");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("load config")
                    .expect("original config")
                    .payload,
                json!({"value": "before"})
            );
            assert_eq!(
                reopened
                    .load_latest_snapshot("environments/interruption")
                    .await
                    .expect("load environment")
                    .expect("original environment")
                    .payload,
                json!({"deleted": false})
            );
            assert_eq!(
                reopened
                    .load_latest_snapshot("monitors/interruption")
                    .await
                    .expect("load monitors")
                    .expect("original monitors")
                    .payload,
                json!({"rules": ["before"]})
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn snapshot_audit_transactions_commit_or_recover_with_one_durable_decision() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("snapshot-audit-normal"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed config");
            let normal_event = audit_event(1_000);
            let write_guard = store.write_guard().await;
            store
                .publish_snapshot_transaction_with_audit_locked(
                    write_guard,
                    vec![FileSnapshotTransactionWrite {
                        collection: "config".to_string(),
                        expected_revision: 1,
                        payload: json!({"value": "normal"}),
                    }],
                    normal_event.clone(),
                )
                .await
                .expect("commit snapshot and audit");
            assert_eq!(
                store
                    .load_latest_snapshot("config")
                    .await
                    .expect("load normal snapshot")
                    .expect("normal snapshot")
                    .payload,
                json!({"value": "normal"})
            );
            assert_eq!(
                store
                    .query_audit_events(AuditQuery {
                        start_ms: 0,
                        end_ms: 2_000,
                        actor: None,
                        action: None,
                        outcome: None,
                        environment_id: None,
                        cursor: None,
                        limit: 10,
                    })
                    .await
                    .expect("query normal audit")
                    .events,
                vec![normal_event.clone()]
            );
            drop(store);

            let staged = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("snapshot-audit-prepared"),
            )
            .await
            .expect("stage file store");
            let prepared_event = audit_event(2_000);
            let append =
                prepare_snapshot_audit_append(&root, prepared_event.created_at_ms).expect("prepare audit append");
            let prepared = prepare_transaction_writes_with_audit(
                &root,
                &[FileSnapshotTransactionWrite {
                    collection: "config".to_string(),
                    expected_revision: 2,
                    payload: json!({"value": "prepared"}),
                }],
                append.clone(),
            )
            .expect("stage prepared transaction");
            append_snapshot_audit(
                &root,
                &append,
                &serde_json::to_vec(&prepared_event).expect("encode audit"),
            )
            .expect("append staged audit");
            drop(prepared);
            drop(staged);

            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("snapshot-audit-prepared-reopen"),
            )
            .await
            .expect("recover prepared transaction");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("load recovered snapshot")
                    .expect("normal snapshot")
                    .payload,
                json!({"value": "normal"})
            );
            assert_eq!(
                reopened
                    .query_audit_events(AuditQuery {
                        start_ms: 0,
                        end_ms: 3_000,
                        actor: None,
                        action: None,
                        outcome: None,
                        environment_id: None,
                        cursor: None,
                        limit: 10,
                    })
                    .await
                    .expect("query recovered audit")
                    .events,
                vec![normal_event.clone()]
            );
            drop(reopened);

            let committed = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("snapshot-audit-committed"),
            )
            .await
            .expect("committed file store");
            let committed_event = audit_event(3_000);
            let append =
                prepare_snapshot_audit_append(&root, committed_event.created_at_ms).expect("prepare committed audit");
            let committed_transaction = prepare_transaction_writes_with_audit(
                &root,
                &[FileSnapshotTransactionWrite {
                    collection: "config".to_string(),
                    expected_revision: 2,
                    payload: json!({"value": "committed"}),
                }],
                append.clone(),
            )
            .expect("stage committed transaction");
            append_snapshot_audit(
                &root,
                &append,
                &serde_json::to_vec(&committed_event).expect("encode audit"),
            )
            .expect("append committed audit");
            committed_transaction.finalize().expect("write committed decision");
            drop(committed_transaction);
            drop(committed);

            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("snapshot-audit-committed-reopen"),
            )
            .await
            .expect("recover committed transaction");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("load committed snapshot")
                    .expect("committed snapshot")
                    .payload,
                json!({"value": "committed"})
            );
            let events = reopened
                .query_audit_events(AuditQuery {
                    start_ms: 0,
                    end_ms: 4_000,
                    actor: None,
                    action: None,
                    outcome: None,
                    environment_id: None,
                    cursor: None,
                    limit: 10,
                })
                .await
                .expect("query committed audit")
                .events;
            assert_eq!(events, vec![committed_event, normal_event]);
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn injected_prepared_snapshot_audit_failure_reopens_with_neither_side_committed() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("snapshot-audit-failure"),
            )
            .await
            .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "before"}))
                .await
                .expect("seed config");

            // The failure happens only after the transaction has staged the
            // replacement snapshot and appended the audit line. Recovery must
            // therefore use the prepared marker to remove both artifacts.
            let (started, release) = install_panicking_mutation_blocker(&store);
            let failed_event = audit_event(1_000);
            let (result, ()) = tokio::join!(
                async {
                    let write_guard = store.write_guard().await;
                    store
                        .publish_snapshot_transaction_with_audit_locked(
                            write_guard,
                            vec![FileSnapshotTransactionWrite {
                                collection: "config".to_string(),
                                expected_revision: 1,
                                payload: json!({"value": "after"}),
                            }],
                            failed_event,
                        )
                        .await
                },
                async {
                    wait_for_mutation_start(started).await;
                    release.send(()).expect("release injected failure");
                },
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(RuntimeError::BlockingJoin { .. }))
            ));
            store.mutation_blocker.clear();
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root),
                owner.root_context().component("snapshot-audit-failure-reopen"),
            )
            .await
            .expect("reopen prepared failure");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("load original config")
                    .expect("original snapshot")
                    .payload,
                json!({"value": "before"})
            );
            assert!(
                reopened
                    .query_audit_events(AuditQuery {
                        start_ms: 0,
                        end_ms: 2_000,
                        actor: None,
                        action: None,
                        outcome: None,
                        environment_id: None,
                        cursor: None,
                        limit: 10,
                    })
                    .await
                    .expect("query recovered journal")
                    .events
                    .is_empty()
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn cancelled_session_cleanup_request_keeps_the_owned_mutation_alive_until_commit() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(cancellation_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("cancelled-session-cleanup-file-store"),
            )
            .await
            .expect("file store");
            let token_hash = SessionTokenHash([41; 32]);
            store
                .create_session(expired_session(token_hash))
                .await
                .expect("seed expired session");
            let (started, release) = install_mutation_blocker(&store);
            let request_store = store.clone();
            let request = tokio::spawn(async move { request_store.delete_sessions_before(3, 10).await });
            wait_for_mutation_start(started).await;
            request.abort();
            assert!(request.await.expect_err("cancelled request").is_cancelled());
            release.send(()).expect("release cleanup mutation");
            store.mutation_blocker.clear();
            let gate = tokio::time::timeout(Duration::from_secs(1), store.write_guard())
                .await
                .expect("owned cleanup must release its write gate");
            drop(gate);
            assert!(
                store
                    .find_session(&token_hash)
                    .await
                    .expect("read committed cleanup")
                    .is_none()
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn timed_out_session_cleanup_rolls_back_before_active_or_reopened_reads() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("timeout-session-cleanup-file-store"),
            )
            .await
            .expect("file store");
            let token_hash = SessionTokenHash([42; 32]);
            store
                .create_session(expired_session(token_hash))
                .await
                .expect("seed expired session");
            let (started, release) = install_mutation_blocker(&store);
            let (result, ()) = tokio::join!(
                store.delete_sessions_before(3, 10),
                release_mutation_after_storage_timeout(started, release),
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(
                    RuntimeError::BlockingTaskTimeoutStillRunning { .. }
                ))
            ));
            store.mutation_blocker.clear();
            assert!(
                store
                    .find_session(&token_hash)
                    .await
                    .expect("read rollback result")
                    .is_some()
            );
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root),
                owner
                    .root_context()
                    .component("timeout-session-cleanup-file-store-reopen"),
            )
            .await
            .expect("reopen after cleanup rollback");
            assert!(
                reopened
                    .find_session(&token_hash)
                    .await
                    .expect("read reopened rollback result")
                    .is_some()
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn session_cleanup_serializes_a_concurrent_session_writer() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(cancellation_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("serialized-session-cleanup-file-store"),
            )
            .await
            .expect("file store");
            let expired_hash = SessionTokenHash([43; 32]);
            let writer_hash = SessionTokenHash([44; 32]);
            store
                .create_session(expired_session(expired_hash))
                .await
                .expect("seed expired session");
            let (started, release) = install_mutation_blocker(&store);
            let cleanup_store = store.clone();
            let cleanup = tokio::spawn(async move { cleanup_store.delete_sessions_before(3, 10).await });
            wait_for_mutation_start(started).await;

            let writer_store = store.clone();
            let mut writer = tokio::spawn(async move {
                writer_store
                    .create_session(NewSession {
                        session_id: uuid::Uuid::now_v7().to_string(),
                        token_hash: writer_hash,
                        username: "writer".to_string(),
                        created_at_ms: 3,
                        expires_at_ms: 10_000,
                    })
                    .await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut writer)
                    .await
                    .is_err(),
                "the writer must wait for the cleanup decision"
            );
            release.send(()).expect("release cleanup mutation");
            store.mutation_blocker.clear();
            assert_eq!(cleanup.await.expect("join cleanup").expect("commit cleanup"), 1);
            writer
                .await
                .expect("join writer")
                .expect("writer must finish after cleanup");
            assert!(
                store
                    .find_session(&writer_hash)
                    .await
                    .expect("read writer session")
                    .is_some()
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn session_touch_reopen_recovers_prepared_old_or_committed_new_decisions() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            for (name, token_hash, committed, expected_last_seen) in [
                ("prepared", SessionTokenHash([51; 32]), false, 1),
                ("committed", SessionTokenHash([52; 32]), true, 2),
            ] {
                let root = directory.path().join(name);
                let store = FilePersistence::initialize(
                    &file_config(root.clone()),
                    owner.root_context().component("touch-reopen-stage"),
                )
                .await
                .expect("stage file store");
                store
                    .create_session(active_session(token_hash, "touch-owner"))
                    .await
                    .expect("seed active session");
                session_file_store::stage_session_touch_for_reopen_test(&root, token_hash, 2, committed)
                    .expect("stage interrupted touch");
                drop(store);

                let reopened = FilePersistence::initialize(
                    &file_config(root.clone()),
                    owner.root_context().component("touch-reopen-recover"),
                )
                .await
                .expect("recover interrupted touch");
                let record = reopened
                    .find_session(&token_hash)
                    .await
                    .expect("read recovered session")
                    .expect("recovered session");
                assert_eq!(record.last_seen_at_ms, expected_last_seen, "{name} decision");
                assert_eq!(
                    std::fs::read_dir(root.join("transactions"))
                        .expect("transaction directory")
                        .count(),
                    0,
                    "{name} recovery must remove every marker and sidecar"
                );
            }
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn cancelled_session_touch_request_keeps_the_owned_mutation_until_commit() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(cancellation_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("cancelled-session-touch-file-store"),
            )
            .await
            .expect("file store");
            let token_hash = SessionTokenHash([53; 32]);
            store
                .create_session(active_session(token_hash, "touch-owner"))
                .await
                .expect("seed active session");
            let (started, release) = install_mutation_blocker(&store);
            let request_store = store.clone();
            let request = tokio::spawn(async move { request_store.touch_session(&token_hash, 2).await });
            wait_for_mutation_start(started).await;
            request.abort();
            assert!(request.await.expect_err("cancelled request").is_cancelled());
            release.send(()).expect("release touch mutation");
            store.mutation_blocker.clear();
            let gate = tokio::time::timeout(Duration::from_secs(1), store.write_guard())
                .await
                .expect("owned touch must release its write gate");
            drop(gate);
            assert_eq!(
                store
                    .find_session(&token_hash)
                    .await
                    .expect("read committed touch")
                    .expect("active session")
                    .last_seen_at_ms,
                2
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn timed_out_session_touch_rolls_back_before_active_or_reopened_reads() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(timeout_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("timeout-session-touch-file-store"),
            )
            .await
            .expect("file store");
            let token_hash = SessionTokenHash([54; 32]);
            store
                .create_session(active_session(token_hash, "touch-owner"))
                .await
                .expect("seed active session");
            let (started, release) = install_mutation_blocker(&store);
            let (result, ()) = tokio::join!(
                store.touch_session(&token_hash, 2),
                release_mutation_after_storage_timeout(started, release),
            );
            assert!(matches!(
                result,
                Err(PersistenceError::Runtime(
                    RuntimeError::BlockingTaskTimeoutStillRunning { .. }
                ))
            ));
            store.mutation_blocker.clear();
            assert_eq!(
                store
                    .find_session(&token_hash)
                    .await
                    .expect("read rollback result")
                    .expect("active session")
                    .last_seen_at_ms,
                1
            );
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root),
                owner
                    .root_context()
                    .component("timeout-session-touch-file-store-reopen"),
            )
            .await
            .expect("reopen after touch rollback");
            assert_eq!(
                reopened
                    .find_session(&token_hash)
                    .await
                    .expect("read reopened rollback result")
                    .expect("active session")
                    .last_seen_at_ms,
                1
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn failed_session_touch_cleanup_reopens_the_committed_record_and_removes_its_marker() {
        let directory = tempfile::tempdir().expect("temp dir");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("touch-cleanup-failure-file-store"),
            )
            .await
            .expect("file store");
            let token_hash = SessionTokenHash([55; 32]);
            store
                .create_session(active_session(token_hash, "touch-owner"))
                .await
                .expect("seed active session");
            let (started, release) = install_committed_cleanup_blocker(&store, true);
            let (result, ()) = tokio::join!(store.touch_session(&token_hash, 2), async {
                wait_for_mutation_start(started).await;
                release.send(()).expect("release cleanup hook");
            },);
            assert!(result.expect("committed touch"));
            store.clear_test_committed_cleanup_blocker();
            assert_eq!(store.storage_health().await.status, StorageStatus::Available);
            assert!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory")
                    .next()
                    .is_some(),
                "failed cleanup must retain a committed touch marker"
            );
            drop(store);

            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner
                    .root_context()
                    .component("touch-cleanup-failure-file-store-reopen"),
            )
            .await
            .expect("reopen committed touch");
            assert_eq!(
                reopened
                    .find_session(&token_hash)
                    .await
                    .expect("read committed touch")
                    .expect("active session")
                    .last_seen_at_ms,
                2
            );
            assert_eq!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory")
                    .count(),
                0
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn session_touch_serializes_cleanup_listing_and_login_cap_scan() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(cancellation_runtime_config()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(directory.path().join("dashboard")),
                owner.root_context().component("serialized-session-touch-file-store"),
            )
            .await
            .expect("file store");
            let touched_hash = SessionTokenHash([56; 32]);
            let expired_hash = SessionTokenHash([57; 32]);
            let login_hash = SessionTokenHash([58; 32]);
            store
                .create_session(active_session(touched_hash, "touch-owner"))
                .await
                .expect("seed touched session");
            store
                .create_session(expired_session(expired_hash))
                .await
                .expect("seed expired session");
            let (started, release) = install_mutation_blocker(&store);
            let touch_store = store.clone();
            let touch = tokio::spawn(async move { touch_store.touch_session(&touched_hash, 2).await });
            wait_for_mutation_start(started).await;

            let list_store = store.clone();
            let mut listing = tokio::spawn(async move {
                list_store
                    .list_sessions(SessionQuery {
                        username: None,
                        cursor: None,
                        limit: 50,
                    })
                    .await
            });
            let cleanup_store = store.clone();
            let cleanup = tokio::spawn(async move { cleanup_store.delete_sessions_before(3, 10).await });
            let login_store = store.clone();
            let login = tokio::spawn(async move {
                login_store
                    .create_session_with_audit_capped(active_session(login_hash, "login-owner"), audit_event(3), 32, 3)
                    .await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut listing)
                    .await
                    .is_err(),
                "listing must wait for the touch commit decision"
            );
            release.send(()).expect("release touch mutation");
            store.mutation_blocker.clear();
            assert!(touch.await.expect("join touch").expect("commit touch"));
            listing
                .await
                .expect("join listing")
                .expect("listing after touch decision");
            assert_eq!(cleanup.await.expect("join cleanup").expect("commit cleanup"), 1);
            login
                .await
                .expect("join login-cap scan")
                .expect("login-cap scan after touch decision");
            assert_eq!(
                store
                    .find_session(&touched_hash)
                    .await
                    .expect("read touched session")
                    .expect("touched session")
                    .last_seen_at_ms,
                2
            );
            assert!(
                store
                    .find_session(&expired_hash)
                    .await
                    .expect("read cleaned session")
                    .is_none()
            );
            assert!(
                store
                    .find_session(&login_hash)
                    .await
                    .expect("read capped-login session")
                    .is_some()
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn exclusive_lock_prevents_a_second_open() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let config = file_config(directory.path().join("dashboard"));
            let first = FilePersistence::initialize(&config, owner.root_context().component("first-file-store"))
                .await
                .expect("first file store");
            let second =
                FilePersistence::initialize(&config, owner.root_context().component("second-file-store")).await;
            assert!(matches!(second, Err(PersistenceError::LockUnavailable)));
            drop(first);
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn documented_interim_default_paths_fail_closed_without_importing_them() {
        let directory = tempfile::tempdir().expect("temp dir");
        let data = directory.path().join("data");
        std::fs::create_dir_all(data.join("monitor")).expect("create former monitor directory");
        std::fs::write(data.join("dashboard-interim-config.json"), b"{}").expect("seed former config path");
        std::fs::write(data.join("monitor/consumer-monitor-config.json"), b"{}").expect("seed former monitor path");

        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let result = FilePersistence::initialize(
                &file_config(data.join("dashboard")),
                owner.root_context().component("former-default-paths"),
            )
            .await;
            assert!(matches!(result, Err(PersistenceError::UnsupportedLayout)));
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn snapshots_recover_after_reinitialization_and_jsonl_discards_an_incomplete_tail() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let root = directory.path().join("dashboard");
            let store =
                FilePersistence::initialize(&file_config(root.clone()), owner.root_context().component("file-store"))
                    .await
                    .expect("file store");
            store
                .write_snapshot("config", 1, json!({"value": "first"}))
                .await
                .expect("first snapshot");
            store
                .write_snapshot("config", 2, json!({"value": "second"}))
                .await
                .expect("second snapshot");
            assert_eq!(
                store
                    .load_latest_snapshot("config")
                    .await
                    .expect("read second snapshot")
                    .expect("second snapshot")
                    .revision,
                2
            );
            let duplicate = store.write_snapshot("config", 2, json!({"value": "replacement"})).await;
            assert!(matches!(duplicate, Err(PersistenceError::Conflict)));
            drop(store);
            std::fs::remove_file(root.join("manifest.json")).expect("remove manifest");
            let recovered = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("recovered-file-store"),
            )
            .await
            .expect("recover file store");
            let snapshot = recovered
                .load_latest_snapshot("config")
                .await
                .expect("load snapshot")
                .expect("latest snapshot");
            assert_eq!(snapshot.revision, 2);
            assert_eq!(snapshot.payload, json!({"value": "second"}));

            recovered
                .append_jsonl("metrics", json!({"value": 1}))
                .await
                .expect("append first");
            std::fs::OpenOptions::new()
                .append(true)
                .open(root.join("history/metrics.jsonl"))
                .expect("open history")
                .write_all(b"{\"value\":")
                .expect("write partial record");
            let (second, third) = tokio::join!(
                recovered.append_jsonl("metrics", json!({"value": 2})),
                recovered.append_jsonl("metrics", json!({"value": 3})),
            );
            second.expect("append second");
            third.expect("append third");
            let values = recovered.read_jsonl("metrics").await.expect("read history");
            assert_eq!(values.len(), 3);
            assert_eq!(values.first(), Some(&json!({"value": 1})));
            assert!(values.contains(&json!({"value": 2})));
            assert!(values.contains(&json!({"value": 3})));
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn corrupt_manifest_rebuilds_from_the_latest_valid_snapshot_per_collection() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let root = directory.path().join("dashboard");
            let config = file_config(root.clone());
            let store = FilePersistence::initialize(&config, owner.root_context().component("manifest-seed"))
                .await
                .expect("file store");
            store
                .write_snapshot("configuration", 1, json!({"revision": 1}))
                .await
                .expect("first configuration snapshot");
            store
                .write_snapshot("configuration", 3, json!({"revision": 3}))
                .await
                .expect("latest configuration snapshot");
            store
                .write_snapshot("audit", 4, json!({"revision": 4}))
                .await
                .expect("first audit snapshot");
            store
                .write_snapshot("audit", 6, json!({"revision": 6}))
                .await
                .expect("latest audit snapshot");
            drop(store);

            std::fs::write(root.join("manifest.json"), b"not valid json").expect("corrupt manifest");
            std::fs::write(
                root.join("configuration/snapshots/00000000000000000002.json"),
                b"torn snapshot",
            )
            .expect("write torn snapshot");
            let recovered = FilePersistence::initialize(&config, owner.root_context().component("manifest-recovered"))
                .await
                .expect("recover corrupt manifest");
            let manifest: FileManifest =
                serde_json::from_slice(&std::fs::read(root.join("manifest.json")).expect("read rebuilt manifest"))
                    .expect("parse rebuilt manifest");
            assert_eq!(manifest.format_version, FORMAT_VERSION);
            assert_eq!(manifest.backend, "file");
            assert_eq!(
                recovered
                    .load_latest_snapshot("configuration")
                    .await
                    .expect("load configuration")
                    .expect("configuration snapshot")
                    .revision,
                3
            );
            assert_eq!(
                recovered
                    .load_latest_snapshot("audit")
                    .await
                    .expect("load audit")
                    .expect("audit snapshot")
                    .revision,
                6
            );
            let health = recovered.storage_health().await;
            assert_eq!(health.status, StorageStatus::Available);
            assert_eq!(readiness_status_from_storage(health).status, "UP");
            drop(recovered);

            std::fs::write(
                root.join("manifest.json"),
                br#"{"formatVersion":2,"backend":"file","createdAtMs":0}"#,
            )
            .expect("write incompatible manifest");
            let incompatible =
                FilePersistence::initialize(&config, owner.root_context().component("manifest-incompatible")).await;
            assert!(matches!(incompatible, Err(PersistenceError::UnsupportedLayout)));
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn missing_manifest_with_mixed_snapshots_recovers_and_stays_ready() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let root = directory.path().join("dashboard");
            let config = file_config(root.clone());
            let store = FilePersistence::initialize(&config, owner.root_context().component("missing-manifest-seed"))
                .await
                .expect("file store");
            store
                .write_snapshot("configuration", 2, json!({"revision": 2}))
                .await
                .expect("configuration snapshot");
            drop(store);

            std::fs::remove_file(root.join("manifest.json")).expect("remove manifest");
            std::fs::write(
                root.join("configuration/snapshots/00000000000000000003.json"),
                b"torn snapshot",
            )
            .expect("write torn snapshot");
            let recovered =
                FilePersistence::initialize(&config, owner.root_context().component("missing-manifest-recovered"))
                    .await
                    .expect("recover missing manifest");
            assert_eq!(
                recovered
                    .load_latest_snapshot("configuration")
                    .await
                    .expect("load configuration")
                    .expect("configuration snapshot")
                    .revision,
                2
            );
            let health = recovered.storage_health().await;
            assert_eq!(health.status, StorageStatus::Available);
            assert_eq!(readiness_status_from_storage(health).status, "UP");
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn corrupt_manifest_with_only_corrupt_snapshots_is_rejected_at_startup() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let root = directory.path().join("dashboard");
            let config = file_config(root.clone());
            let store = FilePersistence::initialize(&config, owner.root_context().component("corrupt-seed"))
                .await
                .expect("file store");
            drop(store);
            std::fs::remove_file(root.join("manifest.json")).expect("remove manifest");
            let snapshots = root.join("configuration/snapshots");
            std::fs::create_dir_all(&snapshots).expect("create snapshots");
            std::fs::write(snapshots.join("00000000000000000001.json"), b"not valid json")
                .expect("write corrupt snapshot");

            let result = FilePersistence::initialize(&config, owner.root_context().component("corrupt-recovery")).await;
            assert!(matches!(result, Err(PersistenceError::CorruptedData)));
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    fn docker_file_initializes_a_mounted_volume() {
        let volume_root = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH must be set by the storage test runner");
        let root = PathBuf::from(volume_root).join(format!(
            "run-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("docker-file-store"),
            )
            .await
            .expect("file storage initialization");
            store
                .write_snapshot("config", 1, json!({"source": "docker"}))
                .await
                .expect("write snapshot");
            drop(store);
            let reopened = FilePersistence::initialize(
                &file_config(root.clone()),
                owner.root_context().component("docker-file-store-reopen"),
            )
            .await
            .expect("reopen file storage");
            assert_eq!(
                reopened
                    .load_latest_snapshot("config")
                    .await
                    .expect("load snapshot")
                    .expect("snapshot")
                    .revision,
                1
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
