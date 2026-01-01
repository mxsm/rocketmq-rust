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

//! Storage backend abstraction
//!
//! This module provides a trait-based abstraction for different storage backends,
//! allowing the controller to use either RocksDB or file-based storage.

#[cfg(feature = "storage-rocksdb")]
pub mod rocksdb_backend;

#[cfg(feature = "storage-file")]
pub mod file_backend;

use std::path::PathBuf;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::Result;

/// Storage backend configuration
#[derive(Debug, Clone)]
pub enum StorageConfig {
    /// RocksDB storage
    #[cfg(feature = "storage-rocksdb")]
    RocksDB {
        /// Database path
        path: PathBuf,
    },

    /// File-based storage
    #[cfg(feature = "storage-file")]
    File {
        /// Data directory path
        path: PathBuf,
    },

    /// In-memory storage (for testing)
    Memory,
}

/// Storage backend trait
///
/// This trait abstracts over different storage implementations,
/// providing a unified interface for storing and retrieving data.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Put a key-value pair
    async fn put(&self, key: &str, value: &[u8]) -> Result<()>;

    /// Get a value by key
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Delete a key
    async fn delete(&self, key: &str) -> Result<()>;

    /// List all keys with a given prefix
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>>;

    /// Batch put multiple key-value pairs
    async fn batch_put(&self, items: Vec<(String, Vec<u8>)>) -> Result<()>;

    /// Batch delete multiple keys
    async fn batch_delete(&self, keys: Vec<String>) -> Result<()>;

    /// Check if a key exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Clear all data (use with caution!)
    async fn clear(&self) -> Result<()>;

    /// Sync data to disk
    async fn sync(&self) -> Result<()>;

    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;
}

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Number of keys
    pub key_count: usize,

    /// Total size in bytes
    pub total_size: u64,

    /// Backend-specific info
    pub backend_info: String,
}

/// Helper methods for storing/retrieving typed data
#[async_trait]
pub trait StorageBackendExt: StorageBackend {
    /// Put a serializable value
    async fn put_json<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> Result<()> {
        let data =
            serde_json::to_vec(value).map_err(|e| crate::error::ControllerError::SerializationError(e.to_string()))?;
        self.put(key, &data).await
    }

    /// Get and deserialize a value
    async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(data) => {
                let value = serde_json::from_slice(&data)
                    .map_err(|e| crate::error::ControllerError::SerializationError(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// List all values with a given prefix
    async fn list_json<T: DeserializeOwned + Send>(&self, prefix: &str) -> Result<Vec<T>> {
        let keys = self.list_keys(prefix).await?;
        let mut values = Vec::new();

        for key in keys {
            if let Some(data) = self.get(&key).await? {
                let value: T = serde_json::from_slice(&data)
                    .map_err(|e| crate::error::ControllerError::SerializationError(e.to_string()))?;
                values.push(value);
            }
        }

        Ok(values)
    }
}

// Blanket implementation for all StorageBackend implementors
impl<T: StorageBackend + ?Sized> StorageBackendExt for T {}

/// Create a storage backend based on configuration
pub async fn create_storage(config: StorageConfig) -> Result<Box<dyn StorageBackend>> {
    match config {
        #[cfg(feature = "storage-rocksdb")]
        StorageConfig::RocksDB { path } => {
            let backend = rocksdb_backend::RocksDBBackend::new(path).await?;
            Ok(Box::new(backend))
        }

        #[cfg(feature = "storage-file")]
        StorageConfig::File { path } => {
            let backend = file_backend::FileBackend::new(path).await?;
            Ok(Box::new(backend))
        }

        StorageConfig::Memory => {
            // For testing, use a simple in-memory implementation
            use std::collections::HashMap;
            use std::sync::Arc;

            use parking_lot::RwLock;

            #[derive(Clone)]
            struct MemoryBackend {
                data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
            }

            #[async_trait]
            impl StorageBackend for MemoryBackend {
                async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
                    self.data.write().insert(key.to_string(), value.to_vec());
                    Ok(())
                }

                async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
                    Ok(self.data.read().get(key).cloned())
                }

                async fn delete(&self, key: &str) -> Result<()> {
                    self.data.write().remove(key);
                    Ok(())
                }

                async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
                    Ok(self
                        .data
                        .read()
                        .keys()
                        .filter(|k| k.starts_with(prefix))
                        .cloned()
                        .collect())
                }

                async fn batch_put(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
                    let mut data = self.data.write();
                    for (key, value) in items {
                        data.insert(key, value);
                    }
                    Ok(())
                }

                async fn batch_delete(&self, keys: Vec<String>) -> Result<()> {
                    let mut data = self.data.write();
                    for key in keys {
                        data.remove(&key);
                    }
                    Ok(())
                }

                async fn exists(&self, key: &str) -> Result<bool> {
                    Ok(self.data.read().contains_key(key))
                }

                async fn clear(&self) -> Result<()> {
                    self.data.write().clear();
                    Ok(())
                }

                async fn sync(&self) -> Result<()> {
                    Ok(())
                }

                async fn stats(&self) -> Result<StorageStats> {
                    let data = self.data.read();
                    let total_size: u64 = data.values().map(|v| v.len() as u64).sum();
                    Ok(StorageStats {
                        key_count: data.len(),
                        total_size,
                        backend_info: "Memory".to_string(),
                    })
                }
            }

            Ok(Box::new(MemoryBackend {
                data: Arc::new(RwLock::new(HashMap::new())),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_backend() {
        let backend = create_storage(StorageConfig::Memory).await.unwrap();

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

        // Test list_keys
        backend.put("prefix_1", b"value1").await.unwrap();
        backend.put("prefix_2", b"value2").await.unwrap();
        backend.put("other_1", b"value3").await.unwrap();

        let keys = backend.list_keys("prefix_").await.unwrap();
        assert_eq!(keys.len(), 2);

        // Test stats
        let stats = backend.stats().await.unwrap();
        assert_eq!(stats.key_count, 3);
    }

    #[tokio::test]
    async fn test_json_operations() {
        use serde::Deserialize;
        use serde::Serialize;

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestData {
            id: u64,
            name: String,
        }

        let backend = create_storage(StorageConfig::Memory).await.unwrap();

        let data = TestData {
            id: 123,
            name: "test".to_string(),
        };

        // Test put_json and get_json
        backend.put_json("test_json", &data).await.unwrap();
        let retrieved: Option<TestData> = backend.get_json("test_json").await.unwrap();
        assert_eq!(retrieved, Some(data));
    }
}
