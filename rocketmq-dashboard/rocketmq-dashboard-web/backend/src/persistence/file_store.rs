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
use crate::persistence::StorageHealth;
use crate::persistence::StorageMode;
use crate::persistence::StorageStatus;
use crate::persistence::error::PersistenceError;
use chrono::Utc;
use fs4::fs_std::FileExt;
use rocketmq_runtime::ChildServiceContext;
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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering as AtomicOrdering;
use tokio::sync::Mutex;

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

/// Single-node File backend. The lock remains open for the lifetime of this
/// instance, and every filesystem operation is delegated to storage I/O.
#[derive(Debug, Clone)]
pub struct FilePersistence {
    root: PathBuf,
    _lock: Arc<FileDirectoryLock>,
    service_context: ChildServiceContext,
    last_successful_write_at: Arc<AtomicI64>,
    write_lock: Arc<Mutex<()>>,
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
            write_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn write_snapshot(
        &self,
        collection: &str,
        revision: u64,
        payload: Value,
    ) -> Result<(), PersistenceError> {
        let _write_guard = self.write_lock.lock().await;
        let root = self.root.clone();
        let collection = collection.to_string();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-write-snapshot", move || {
                validate_segment(&collection)?;
                let directory = root.join(&collection).join("snapshots");
                std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
                let snapshot = FileSnapshot {
                    format_version: FORMAT_VERSION,
                    revision,
                    payload,
                };
                let target = directory.join(format!("{revision:020}.json"));
                if target.exists() {
                    return Err(PersistenceError::Conflict);
                }
                write_json_new_file(&target, &snapshot)
            })
            .await
            .map_err(PersistenceError::Runtime)??;
        self.record_write();
        Ok(())
    }

    pub async fn load_latest_snapshot(&self, collection: &str) -> Result<Option<FileSnapshot>, PersistenceError> {
        let root = self.root.clone();
        let collection = collection.to_string();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-load-snapshot", move || {
                validate_segment(&collection)?;
                load_latest_snapshot_file(&root.join(collection).join("snapshots"))
            })
            .await
            .map_err(PersistenceError::Runtime)?
    }

    pub async fn append_jsonl(&self, stream: &str, payload: Value) -> Result<(), PersistenceError> {
        let _write_guard = self.write_lock.lock().await;
        let root = self.root.clone();
        let stream = stream.to_string();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-append-jsonl", move || {
                validate_segment(&stream)?;
                let directory = root.join("history");
                std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
                let path = directory.join(format!("{stream}.jsonl"));
                truncate_incomplete_tail(&path)?;
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(PersistenceError::Io)?;
                serde_json::to_writer(&mut file, &payload).map_err(PersistenceError::Serialization)?;
                file.write_all(b"\n").map_err(PersistenceError::Io)?;
                file.flush().map_err(PersistenceError::Io)?;
                file.sync_data().map_err(PersistenceError::Io)
            })
            .await
            .map_err(PersistenceError::Runtime)??;
        self.record_write();
        Ok(())
    }

    pub async fn read_jsonl(&self, stream: &str) -> Result<Vec<Value>, PersistenceError> {
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
}

fn reject_legacy_layout(root: &Path) -> Result<(), PersistenceError> {
    if root.join("dashboard-config.json").exists() || root.join("consumer-monitor-config.json").exists() {
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
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(PersistenceError::Io)?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        let length = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1);
        file.set_len(length as u64).map_err(PersistenceError::Io)?;
        file.seek(SeekFrom::Start(length as u64))
            .map_err(PersistenceError::Io)?;
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

fn non_zero(value: i64) -> Option<i64> {
    (value > 0).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlPoolConfig;
    use crate::model::StorageBackend;
    use crate::persistence::StorageStatus;
    use crate::service::readiness_status_from_storage;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use serde_json::json;

    fn file_config(root: PathBuf) -> StorageConfig {
        StorageConfig {
            backend: StorageBackend::File,
            data_path: root,
            database_url: None,
            pool: SqlPoolConfig::default(),
        }
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
