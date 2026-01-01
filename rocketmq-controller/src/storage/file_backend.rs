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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::error::ControllerError;
use crate::error::Result;
use crate::storage::StorageBackend;
use crate::storage::StorageStats;

/// File-based storage backend
///
/// Provides persistent storage using individual files for each key.
/// This is simpler than RocksDB but less performant for large datasets.
///
/// File structure:
/// ```text
///  data_dir/
///    ├── metadata.json (index of all keys)
///    └── data/
///         ├── <key_hash_1>.dat
///         ├── <key_hash_2>.dat
///         └── ...
/// ```
pub struct FileBackend {
    /// Data directory path
    path: PathBuf,

    /// In-memory index: key -> file path
    index: Arc<RwLock<HashMap<String, PathBuf>>>,
}

impl FileBackend {
    /// Create a new file-based backend
    pub async fn new(path: PathBuf) -> Result<Self> {
        info!("Opening file-based storage at {:?}", path);

        // Create directories
        fs::create_dir_all(&path)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to create directory: {}", e)))?;

        let data_dir = path.join("data");
        fs::create_dir_all(&data_dir)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to create data directory: {}", e)))?;

        let backend = Self {
            path,
            index: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load existing index
        backend.load_index().await?;

        info!("File-based storage opened successfully");

        Ok(backend)
    }

    /// Get the storage path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Load index from metadata file
    async fn load_index(&self) -> Result<()> {
        let metadata_path = self.path.join("metadata.json");

        if !metadata_path.exists() {
            info!("No existing index found, starting fresh");
            return Ok(());
        }

        let content = fs::read(&metadata_path)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to read metadata: {}", e)))?;

        let loaded_index: HashMap<String, PathBuf> = serde_json::from_slice(&content)
            .map_err(|e| ControllerError::SerializationError(format!("Failed to parse metadata: {}", e)))?;

        *self.index.write() = loaded_index;

        info!("Loaded index with {} keys", self.index.read().len());

        Ok(())
    }

    /// Save index to metadata file
    async fn save_index(&self) -> Result<()> {
        let metadata_path = self.path.join("metadata.json");

        let index = self.index.read().clone();
        let content = serde_json::to_vec_pretty(&index)
            .map_err(|e| ControllerError::SerializationError(format!("Failed to serialize metadata: {}", e)))?;

        fs::write(&metadata_path, content)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to write metadata: {}", e)))?;

        Ok(())
    }

    /// Get file path for a key
    fn get_file_path(&self, key: &str) -> PathBuf {
        // Use hash to avoid file system issues with special characters
        let hash = Self::hash_key(key);
        self.path.join("data").join(format!("{}.dat", hash))
    }

    /// Hash a key to generate a filename
    fn hash_key(key: &str) -> String {
        // Simple hash function - use the key itself if safe, otherwise hash it
        if key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            key.to_string()
        } else {
            // Use a simple hash for keys with special characters
            format!("{:x}", Self::simple_hash(key))
        }
    }

    /// Simple hash function
    fn simple_hash(s: &str) -> u64 {
        let mut hash = 0u64;
        for byte in s.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
        hash
    }
}

#[async_trait]
impl StorageBackend for FileBackend {
    async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        debug!("File put: key={}, size={}", key, value.len());

        let file_path = self.get_file_path(key);

        // Write data to file
        let mut file = fs::File::create(&file_path)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to create file: {}", e)))?;

        file.write_all(value)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to write file: {}", e)))?;

        file.sync_all()
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to sync file: {}", e)))?;

        // Update index
        self.index.write().insert(key.to_string(), file_path);

        // Save index periodically (every 10 operations)
        if self.index.read().len() % 10 == 0 {
            self.save_index().await?;
        }

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        debug!("File get: key={}", key);

        let file_path = match self.index.read().get(key) {
            Some(path) => path.clone(),
            None => return Ok(None),
        };

        // Check if file exists
        if !file_path.exists() {
            warn!("File not found for key: {}", key);
            self.index.write().remove(key);
            return Ok(None);
        }

        // Read file
        let mut file = fs::File::open(&file_path)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to open file: {}", e)))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .map_err(|e| ControllerError::StorageError(format!("Failed to read file: {}", e)))?;

        Ok(Some(buffer))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        debug!("File delete: key={}", key);

        let file_path = match self.index.write().remove(key) {
            Some(path) => path,
            None => return Ok(()), // Key doesn't exist
        };

        // Delete file
        if file_path.exists() {
            fs::remove_file(&file_path)
                .await
                .map_err(|e| ControllerError::StorageError(format!("Failed to delete file: {}", e)))?;
        }

        // Save index
        self.save_index().await?;

        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        debug!("File list_keys: prefix={}", prefix);

        let keys: Vec<String> = self
            .index
            .read()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        Ok(keys)
    }

    async fn batch_put(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        debug!("File batch_put: {} items", items.len());

        for (key, value) in items {
            self.put(&key, &value).await?;
        }

        // Save index after batch
        self.save_index().await?;

        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<String>) -> Result<()> {
        debug!("File batch_delete: {} keys", keys.len());

        for key in keys {
            self.delete(&key).await?;
        }

        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        debug!("File exists: key={}", key);
        Ok(self.index.read().contains_key(key))
    }

    async fn clear(&self) -> Result<()> {
        info!("File clear: removing all data");

        // Delete all files
        let data_dir = self.path.join("data");
        if data_dir.exists() {
            fs::remove_dir_all(&data_dir)
                .await
                .map_err(|e| ControllerError::StorageError(format!("Failed to clear data: {}", e)))?;

            fs::create_dir_all(&data_dir)
                .await
                .map_err(|e| ControllerError::StorageError(format!("Failed to recreate data directory: {}", e)))?;
        }

        // Clear index
        self.index.write().clear();
        self.save_index().await?;

        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        debug!("File sync");
        self.save_index().await
    }

    async fn stats(&self) -> Result<StorageStats> {
        debug!("File stats");

        // Clone the paths to avoid holding the lock across await
        let paths: Vec<PathBuf> = {
            let index = self.index.read();
            index.values().cloned().collect()
        };

        let key_count = paths.len();

        // Calculate total size
        let mut total_size = 0u64;
        for path in paths {
            if path.exists() {
                if let Ok(metadata) = fs::metadata(&path).await {
                    total_size += metadata.len();
                }
            }
        }

        Ok(StorageStats {
            key_count,
            total_size,
            backend_info: format!("File-based storage at {:?}", self.path),
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_file_backend() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("file_storage");

        let backend = FileBackend::new(storage_path).await.unwrap();

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

    #[tokio::test]
    async fn test_file_backend_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("persistent_storage");

        // First session: write data
        {
            let backend = FileBackend::new(storage_path.clone()).await.unwrap();
            backend.put("persist_key", b"persist_value").await.unwrap();
            backend.sync().await.unwrap();
        }

        // Second session: read data
        {
            let backend = FileBackend::new(storage_path).await.unwrap();
            let value = backend.get("persist_key").await.unwrap();
            assert_eq!(value, Some(b"persist_value".to_vec()));
        }
    }
}
