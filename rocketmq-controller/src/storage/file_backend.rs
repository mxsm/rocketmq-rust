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

//! Crash-safe file storage for the single-node development profile.
//!
//! A commit writes immutable, checksummed records first and then atomically
//! publishes one checksummed manifest. Readers only observe records referenced
//! by the latest complete manifest. Production deployments use RocksDB.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use rocketmq_common::utils::crc32_utils::crc32;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::debug;
use tracing::info;

use crate::error::ControllerError;
use crate::error::Result;
use crate::storage::StorageBackend;
use crate::storage::StorageStats;

const FILE_FORMAT_VERSION: u16 = 1;
const DATA_DIRECTORY: &str = "data";
const MANIFEST_DIRECTORY: &str = "manifests";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordPointer {
    generation: u64,
    file_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileRecord {
    format_version: u16,
    generation: u64,
    key: String,
    value: Option<Vec<u8>>,
    checksum: u32,
}

impl FileRecord {
    fn new(generation: u64, key: String, value: Option<Vec<u8>>) -> Result<Self> {
        let mut record = Self {
            format_version: FILE_FORMAT_VERSION,
            generation,
            key,
            value,
            checksum: 0,
        };
        record.checksum = record.calculate_checksum()?;
        Ok(record)
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != FILE_FORMAT_VERSION {
            return Err(integrity_error(format!(
                "unsupported file record format version {}",
                self.format_version
            )));
        }
        let actual = self.calculate_checksum()?;
        if actual != self.checksum {
            return Err(integrity_error(format!(
                "file record checksum mismatch: expected {}, calculated {}",
                self.checksum, actual
            )));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<u32> {
        let bytes = serde_json::to_vec(&(self.format_version, self.generation, &self.key, &self.value))
            .map_err(|error| ControllerError::serialization_source("serialize file record checksum payload", error))?;
        Ok(crc32(&bytes))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileManifest {
    format_version: u16,
    generation: u64,
    entries: BTreeMap<String, RecordPointer>,
    checksum: u32,
}

impl FileManifest {
    fn new(generation: u64, entries: BTreeMap<String, RecordPointer>) -> Result<Self> {
        let mut manifest = Self {
            format_version: FILE_FORMAT_VERSION,
            generation,
            entries,
            checksum: 0,
        };
        manifest.checksum = manifest.calculate_checksum()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != FILE_FORMAT_VERSION {
            return Err(integrity_error(format!(
                "unsupported file manifest format version {}",
                self.format_version
            )));
        }
        let actual = self.calculate_checksum()?;
        if actual != self.checksum {
            return Err(integrity_error(format!(
                "file manifest checksum mismatch: expected {}, calculated {}",
                self.checksum, actual
            )));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<u32> {
        let bytes = serde_json::to_vec(&(self.format_version, self.generation, &self.entries)).map_err(|error| {
            ControllerError::serialization_source("serialize file manifest checksum payload", error)
        })?;
        Ok(crc32(&bytes))
    }
}

#[derive(Debug, Clone)]
struct BackendState {
    generation: u64,
    index: BTreeMap<String, RecordPointer>,
}

enum Mutation {
    Put(String, Vec<u8>),
    Delete(String),
}

/// File storage for the explicitly enabled `dev-single` profile.
///
/// Each mutation batch has a single manifest publication point. Temporary
/// files are ignored and removed during startup, while records and manifests
/// are checksummed before use.
pub struct FileBackend {
    path: PathBuf,
    state: Arc<RwLock<BackendState>>,
    commit_lock: Mutex<()>,
}

impl FileBackend {
    /// Opens or initializes a file backend.
    pub async fn new(path: PathBuf) -> Result<Self> {
        info!(path = ?path, "Opening development file storage");
        let data_dir = path.join(DATA_DIRECTORY);
        let manifest_dir = path.join(MANIFEST_DIRECTORY);
        fs::create_dir_all(&data_dir)
            .await
            .map_err(|error| ControllerError::storage_source("create file storage data directory", error))?;
        fs::create_dir_all(&manifest_dir)
            .await
            .map_err(|error| ControllerError::storage_source("create file storage manifest directory", error))?;
        Self::remove_temporary_files(&data_dir).await?;
        Self::remove_temporary_files(&manifest_dir).await?;

        let manifest = match Self::load_latest_manifest(&manifest_dir).await? {
            Some(manifest) => manifest,
            None => {
                let manifest = FileManifest::new(0, BTreeMap::new())?;
                Self::write_manifest(&manifest_dir, &manifest).await?;
                manifest
            }
        };
        Self::validate_manifest_records(&data_dir, &manifest).await?;
        Self::remove_unreferenced_records(&data_dir, &manifest).await?;

        info!(
            generation = manifest.generation,
            keys = manifest.entries.len(),
            "Development file storage opened"
        );
        Ok(Self {
            path,
            state: Arc::new(RwLock::new(BackendState {
                generation: manifest.generation,
                index: manifest.entries,
            })),
            commit_lock: Mutex::new(()),
        })
    }

    /// Returns the storage root.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn data_dir(&self) -> PathBuf {
        self.path.join(DATA_DIRECTORY)
    }

    fn manifest_dir(&self) -> PathBuf {
        self.path.join(MANIFEST_DIRECTORY)
    }

    async fn remove_temporary_files(directory: &Path) -> Result<()> {
        let mut entries = fs::read_dir(directory)
            .await
            .map_err(|error| ControllerError::storage_source("scan file storage directory", error))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| ControllerError::storage_source("read file storage directory entry", error))?
        {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "tmp") {
                fs::remove_file(&path)
                    .await
                    .map_err(|error| ControllerError::storage_source("remove incomplete storage file", error))?;
            }
        }
        Ok(())
    }

    async fn load_latest_manifest(directory: &Path) -> Result<Option<FileManifest>> {
        let mut candidates = Vec::new();
        let mut entries = fs::read_dir(directory)
            .await
            .map_err(|error| ControllerError::storage_source("scan file storage manifests", error))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| ControllerError::storage_source("read file storage manifest entry", error))?
        {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "manifest") {
                let generation = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.parse::<u64>().ok())
                    .ok_or_else(|| integrity_error(format!("invalid manifest filename: {}", path.display())))?;
                candidates.push((generation, path));
            }
        }
        candidates.sort_unstable_by_key(|(generation, _)| *generation);
        let Some((filename_generation, path)) = candidates.pop() else {
            return Ok(None);
        };
        let bytes = fs::read(&path)
            .await
            .map_err(|error| ControllerError::storage_source("read file storage manifest", error))?;
        let manifest: FileManifest = serde_json::from_slice(&bytes)
            .map_err(|error| ControllerError::serialization_source("decode file storage manifest", error))?;
        manifest.validate()?;
        if manifest.generation != filename_generation {
            return Err(integrity_error(format!(
                "manifest generation {} does not match filename generation {}",
                manifest.generation, filename_generation
            )));
        }
        Ok(Some(manifest))
    }

    async fn validate_manifest_records(data_dir: &Path, manifest: &FileManifest) -> Result<()> {
        for (key, pointer) in &manifest.entries {
            let record = Self::read_record(data_dir, pointer).await?;
            if record.key != *key || record.value.is_none() || record.generation != pointer.generation {
                return Err(integrity_error(format!(
                    "manifest entry for key {key} does not match its record"
                )));
            }
        }
        Ok(())
    }

    async fn remove_unreferenced_records(data_dir: &Path, manifest: &FileManifest) -> Result<()> {
        let referenced = manifest
            .entries
            .values()
            .map(|pointer| pointer.file_name.as_str())
            .collect::<HashSet<_>>();
        let mut entries = fs::read_dir(data_dir)
            .await
            .map_err(|error| ControllerError::storage_source("scan file storage records", error))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| ControllerError::storage_source("read file storage record entry", error))?
        {
            let path = entry.path();
            let is_record = path.extension().is_some_and(|extension| extension == "record");
            let is_referenced = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| referenced.contains(name));
            if is_record && !is_referenced {
                fs::remove_file(&path)
                    .await
                    .map_err(|error| ControllerError::storage_source("remove uncommitted storage record", error))?;
            }
        }
        Ok(())
    }

    async fn read_record(data_dir: &Path, pointer: &RecordPointer) -> Result<FileRecord> {
        let path = data_dir.join(&pointer.file_name);
        let bytes = fs::read(&path)
            .await
            .map_err(|error| ControllerError::storage_source("read file storage record", error))?;
        let record: FileRecord = serde_json::from_slice(&bytes)
            .map_err(|error| ControllerError::serialization_source("decode file storage record", error))?;
        record.validate()?;
        Ok(record)
    }

    async fn write_durable_temp_then_rename(final_path: &Path, bytes: &[u8]) -> Result<()> {
        let temp_path = final_path.with_extension(format!(
            "{}.tmp",
            final_path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("file")
        ));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await
            .map_err(|error| ControllerError::storage_source("create temporary storage file", error))?;
        file.write_all(bytes)
            .await
            .map_err(|error| ControllerError::storage_source("write temporary storage file", error))?;
        file.sync_all()
            .await
            .map_err(|error| ControllerError::storage_source("sync temporary storage file", error))?;
        drop(file);
        fs::rename(&temp_path, final_path)
            .await
            .map_err(|error| ControllerError::storage_source("atomically publish storage file", error))
    }

    async fn write_manifest(directory: &Path, manifest: &FileManifest) -> Result<()> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|error| ControllerError::serialization_source("encode file storage manifest", error))?;
        let path = directory.join(format!("{:020}.manifest", manifest.generation));
        Self::write_durable_temp_then_rename(&path, &bytes).await
    }

    async fn commit(&self, mutations: Vec<Mutation>) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let _commit_guard = self.commit_lock.lock().await;
        let current = self.state.read().clone();
        let generation = current
            .generation
            .checked_add(1)
            .ok_or_else(|| integrity_error("file storage generation overflow"))?;
        let mut next_index = current.index;
        let data_dir = self.data_dir();

        for (ordinal, mutation) in mutations.into_iter().enumerate() {
            let (key, value) = match mutation {
                Mutation::Put(key, value) => (key, Some(value)),
                Mutation::Delete(key) => (key, None),
            };
            let record = FileRecord::new(generation, key.clone(), value)?;
            let file_name = format!("{generation:020}-{ordinal:08}.record");
            let bytes = serde_json::to_vec(&record)
                .map_err(|error| ControllerError::serialization_source("encode file storage record", error))?;
            Self::write_durable_temp_then_rename(&data_dir.join(&file_name), &bytes).await?;
            if record.value.is_some() {
                next_index.insert(key, RecordPointer { generation, file_name });
            } else {
                next_index.remove(&key);
            }
        }

        let manifest = FileManifest::new(generation, next_index.clone())?;
        Self::write_manifest(&self.manifest_dir(), &manifest).await?;
        *self.state.write() = BackendState {
            generation,
            index: next_index,
        };
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for FileBackend {
    async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        debug!(key, size = value.len(), "File storage put");
        self.commit(vec![Mutation::Put(key.to_string(), value.to_vec())]).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let pointer = match self.state.read().index.get(key) {
            Some(pointer) => pointer.clone(),
            None => return Ok(None),
        };
        let record = Self::read_record(&self.data_dir(), &pointer).await?;
        if record.key != key || record.generation != pointer.generation {
            return Err(integrity_error(format!("record identity mismatch for key {key}")));
        }
        record
            .value
            .ok_or_else(|| integrity_error(format!("manifest points to tombstone for key {key}")))
            .map(Some)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.commit(vec![Mutation::Delete(key.to_string())]).await
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .state
            .read()
            .index
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn batch_put(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        self.commit(
            items
                .into_iter()
                .map(|(key, value)| Mutation::Put(key, value))
                .collect(),
        )
        .await
    }

    async fn batch_delete(&self, keys: Vec<String>) -> Result<()> {
        self.commit(keys.into_iter().map(Mutation::Delete).collect()).await
    }

    async fn write_batch(&self, puts: Vec<(String, Vec<u8>)>, deletes: Vec<String>) -> Result<()> {
        let mut mutations = Vec::with_capacity(puts.len() + deletes.len());
        mutations.extend(deletes.into_iter().map(Mutation::Delete));
        mutations.extend(puts.into_iter().map(|(key, value)| Mutation::Put(key, value)));
        self.commit(mutations).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.state.read().index.contains_key(key))
    }

    async fn clear(&self) -> Result<()> {
        let keys = self.state.read().index.keys().cloned().collect::<Vec<_>>();
        self.commit(keys.into_iter().map(Mutation::Delete).collect()).await
    }

    async fn sync(&self) -> Result<()> {
        let _commit_guard = self.commit_lock.lock().await;
        let generation = self.state.read().generation;
        let path = self.manifest_dir().join(format!("{generation:020}.manifest"));
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .map_err(|error| ControllerError::storage_source("open current file storage manifest", error))?;
        file.sync_all()
            .await
            .map_err(|error| ControllerError::storage_source("sync current file storage manifest", error))
    }

    async fn stats(&self) -> Result<StorageStats> {
        let pointers = self.state.read().index.values().cloned().collect::<Vec<_>>();
        let mut total_size = 0_u64;
        for pointer in &pointers {
            total_size += fs::metadata(self.data_dir().join(&pointer.file_name))
                .await
                .map_err(|error| ControllerError::storage_source("read file storage record metadata", error))?
                .len();
        }
        Ok(StorageStats {
            key_count: pointers.len(),
            total_size,
            backend_info: format!("Development file storage at {:?}", self.path),
        })
    }
}

fn integrity_error(message: impl Into<String>) -> ControllerError {
    ControllerError::StorageError(format!("file storage integrity error: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn batch_is_visible_after_restart_as_one_manifest_generation() {
        let temp_dir = TempDir::new().expect("temp directory");
        let storage_path = temp_dir.path().join("file-storage");

        {
            let backend = FileBackend::new(storage_path.clone()).await.expect("open");
            backend
                .batch_put(vec![
                    ("alpha".to_string(), b"one".to_vec()),
                    ("beta".to_string(), b"two".to_vec()),
                ])
                .await
                .expect("batch put");
            assert_eq!(backend.state.read().generation, 1);
        }

        let backend = FileBackend::new(storage_path).await.expect("reopen");
        assert_eq!(backend.get("alpha").await.expect("get alpha"), Some(b"one".to_vec()));
        assert_eq!(backend.get("beta").await.expect("get beta"), Some(b"two".to_vec()));
        assert_eq!(backend.state.read().generation, 1);
    }

    #[tokio::test]
    async fn delete_and_clear_are_durable() {
        let temp_dir = TempDir::new().expect("temp directory");
        let storage_path = temp_dir.path().join("file-storage");
        let backend = FileBackend::new(storage_path.clone()).await.expect("open");
        backend.put("alpha", b"one").await.expect("put alpha");
        backend.put("beta", b"two").await.expect("put beta");
        backend.delete("alpha").await.expect("delete alpha");
        assert_eq!(backend.get("alpha").await.expect("get alpha"), None);
        backend.clear().await.expect("clear");
        drop(backend);

        let reopened = FileBackend::new(storage_path).await.expect("reopen");
        assert!(reopened.list_keys("").await.expect("list").is_empty());
    }

    #[tokio::test]
    async fn corrupted_record_is_rejected() {
        let temp_dir = TempDir::new().expect("temp directory");
        let storage_path = temp_dir.path().join("file-storage");
        let backend = FileBackend::new(storage_path).await.expect("open");
        backend.put("alpha", b"one").await.expect("put");
        let pointer = backend
            .state
            .read()
            .index
            .get("alpha")
            .cloned()
            .expect("record pointer");
        fs::write(backend.data_dir().join(pointer.file_name), b"corrupt")
            .await
            .expect("corrupt record");

        assert!(backend.get("alpha").await.is_err());
    }

    #[tokio::test]
    async fn incomplete_temporary_files_are_removed_on_restart() {
        let temp_dir = TempDir::new().expect("temp directory");
        let storage_path = temp_dir.path().join("file-storage");
        let backend = FileBackend::new(storage_path.clone()).await.expect("open");
        let incomplete = backend.data_dir().join("incomplete.record.tmp");
        fs::write(&incomplete, b"partial").await.expect("write temporary file");
        drop(backend);

        FileBackend::new(storage_path).await.expect("reopen");
        assert!(!incomplete.exists());
    }

    #[tokio::test]
    async fn uncommitted_record_is_removed_on_restart() {
        let temp_dir = TempDir::new().expect("temp directory");
        let storage_path = temp_dir.path().join("file-storage");
        let backend = FileBackend::new(storage_path.clone()).await.expect("open");
        let orphan = backend.data_dir().join("00000000000000000001-00000000.record");
        fs::write(&orphan, b"interrupted commit")
            .await
            .expect("write orphan record");
        drop(backend);

        FileBackend::new(storage_path).await.expect("reopen");
        assert!(!orphan.exists());
    }
}
