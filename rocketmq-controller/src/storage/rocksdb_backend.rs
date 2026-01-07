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

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use rocksdb::Options;
use rocksdb::WriteBatch;
use rocksdb::DB;
use tracing::debug;
use tracing::info;

use crate::error::ControllerError;
use crate::error::Result;
use crate::storage::StorageBackend;
use crate::storage::StorageStats;

/// RocksDB storage backend
///
/// Provides persistent storage using RocksDB, a high-performance
/// embedded database based on LevelDB.
pub struct RocksDBBackend {
    /// RocksDB instance
    db: Arc<DB>,

    /// Database path
    path: PathBuf,
}

impl RocksDBBackend {
    /// Create a new RocksDB backend
    pub async fn new(path: PathBuf) -> Result<Self> {
        info!("Opening RocksDB at {:?}", path);

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| ControllerError::StorageError(format!("Failed to create directory: {}", e)))?;
        }

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Performance tuning
        opts.set_max_open_files(1000);
        opts.set_use_fsync(false);
        opts.set_bytes_per_sync(1024 * 1024);
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_max_background_jobs(4);

        // Write buffer settings
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(2);

        // Open the database
        let db = DB::open(&opts, &path)
            .map_err(|e| ControllerError::StorageError(format!("Failed to open RocksDB: {}", e)))?;

        info!("RocksDB opened successfully");

        Ok(Self { db: Arc::new(db), path })
    }

    /// Get the database path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

#[async_trait]
impl StorageBackend for RocksDBBackend {
    async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        debug!("RocksDB put: key={}, size={}", key, value.len());

        let db = self.db.clone();
        let key = key.to_string();
        let value = value.to_vec();

        tokio::task::spawn_blocking(move || {
            db.put(key.as_bytes(), value)
                .map_err(|e| ControllerError::StorageError(format!("RocksDB put failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        debug!("RocksDB get: key={}", key);

        let db = self.db.clone();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            db.get(key.as_bytes())
                .map_err(|e| ControllerError::StorageError(format!("RocksDB get failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))?
    }

    async fn delete(&self, key: &str) -> Result<()> {
        debug!("RocksDB delete: key={}", key);

        let db = self.db.clone();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            db.delete(key.as_bytes())
                .map_err(|e| ControllerError::StorageError(format!("RocksDB delete failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        debug!("RocksDB list_keys: prefix={}", prefix);

        let db = self.db.clone();
        let prefix = prefix.to_string();

        tokio::task::spawn_blocking(move || {
            let mut keys = Vec::new();
            let iter = db.iterator(rocksdb::IteratorMode::Start);

            for item in iter {
                match item {
                    Ok((key_bytes, _)) => {
                        if let Ok(key_str) = String::from_utf8(key_bytes.to_vec()) {
                            if key_str.starts_with(&prefix) {
                                keys.push(key_str);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(ControllerError::StorageError(format!(
                            "RocksDB iteration failed: {}",
                            e
                        )));
                    }
                }
            }

            Ok(keys)
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))?
    }

    async fn batch_put(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        debug!("RocksDB batch_put: {} items", items.len());

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();

            for (key, value) in items {
                batch.put(key.as_bytes(), value);
            }

            db.write(batch)
                .map_err(|e| ControllerError::StorageError(format!("RocksDB batch write failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<String>) -> Result<()> {
        debug!("RocksDB batch_delete: {} keys", keys.len());

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();

            for key in keys {
                batch.delete(key.as_bytes());
            }

            db.write(batch)
                .map_err(|e| ControllerError::StorageError(format!("RocksDB batch delete failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        debug!("RocksDB exists: key={}", key);

        let db = self.db.clone();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            db.get(key.as_bytes())
                .map(|opt| opt.is_some())
                .map_err(|e| ControllerError::StorageError(format!("RocksDB exists check failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))?
    }

    async fn clear(&self) -> Result<()> {
        info!("RocksDB clear: removing all data");

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            let iter = db.iterator(rocksdb::IteratorMode::Start);

            for item in iter {
                match item {
                    Ok((key, _)) => {
                        batch.delete(&key);
                    }
                    Err(e) => {
                        return Err(ControllerError::StorageError(format!(
                            "RocksDB iteration failed: {}",
                            e
                        )));
                    }
                }
            }

            db.write(batch)
                .map_err(|e| ControllerError::StorageError(format!("RocksDB clear failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        debug!("RocksDB sync");

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            db.flush()
                .map_err(|e| ControllerError::StorageError(format!("RocksDB sync failed: {}", e)))
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn stats(&self) -> Result<StorageStats> {
        debug!("RocksDB stats");

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut key_count = 0;
            let mut total_size = 0u64;

            let iter = db.iterator(rocksdb::IteratorMode::Start);
            for item in iter {
                match item {
                    Ok((key, value)) => {
                        key_count += 1;
                        total_size += (key.len() + value.len()) as u64;
                    }
                    Err(e) => {
                        return Err(ControllerError::StorageError(format!(
                            "RocksDB iteration failed: {}",
                            e
                        )));
                    }
                }
            }

            // Get RocksDB property
            let backend_info = db
                .property_value("rocksdb.stats")
                .unwrap_or(None)
                .unwrap_or_else(|| "RocksDB".to_string());

            Ok(StorageStats {
                key_count,
                total_size,
                backend_info,
            })
        })
        .await
        .map_err(|e| ControllerError::StorageError(format!("Task join error: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_rocksdb_backend() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let backend = RocksDBBackend::new(db_path).await.unwrap();

        // Test put and get
        backend.put("test_key", b"test_value").await.unwrap();
        let value = backend.get("test_key").await.unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));

        // Test exists
        assert!(backend.exists("test_key").await.unwrap());
        assert!(!backend.exists("nonexistent").await.unwrap());

        // Test delete
        backend.delete("test_key").await.unwrap();
        assert!(!backend.exists("test_key").await.unwrap());

        // Test batch operations
        let items = vec![
            ("batch_1".to_string(), b"value1".to_vec()),
            ("batch_2".to_string(), b"value2".to_vec()),
        ];
        backend.batch_put(items).await.unwrap();

        assert!(backend.exists("batch_1").await.unwrap());
        assert!(backend.exists("batch_2").await.unwrap());

        // Test list_keys
        backend.put("prefix_1", b"value1").await.unwrap();
        backend.put("prefix_2", b"value2").await.unwrap();

        let keys = backend.list_keys("prefix_").await.unwrap();
        assert_eq!(keys.len(), 2);

        // Test stats
        let stats = backend.stats().await.unwrap();
        assert!(stats.key_count >= 4);

        // Test sync
        backend.sync().await.unwrap();
    }
}
