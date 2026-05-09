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

use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::dispatcher::TieredDispatchRequest;
use crate::file::TieredFlatFile;
use crate::file::TieredIndexEntry;
use crate::metadata::JsonMetadataStore;
use crate::metadata::TieredMetadataStore;
use crate::provider::TieredStoreProvider;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FlatFileKey {
    topic: String,
    queue_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndexKey {
    topic: String,
    key: String,
}

pub struct TieredFlatFileStore<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    metadata_store: Arc<JsonMetadataStore>,
    provider: P,
    files: DashMap<FlatFileKey, Arc<TieredFlatFile<P>>>,
    index: DashMap<IndexKey, Vec<TieredIndexEntry>>,
}

impl<P> TieredFlatFileStore<P>
where
    P: TieredStoreProvider,
{
    pub fn new(config: Arc<TieredStoreConfig>, metadata_store: Arc<JsonMetadataStore>, provider: P) -> Self {
        Self {
            config,
            metadata_store,
            provider,
            files: DashMap::new(),
            index: DashMap::new(),
        }
    }

    pub async fn load(&self) -> Result<(), RocketMQError> {
        let queues = self.metadata_store.list_queues().await?;
        for queue in queues {
            let flat_file = self.get_or_create(queue.topic, queue.queue_id)?;
            flat_file.recover().await?;
        }
        self.load_index().await?;
        Ok(())
    }

    pub fn get(&self, topic: &str, queue_id: i32) -> Option<Arc<TieredFlatFile<P>>> {
        let key = FlatFileKey {
            topic: topic.to_owned(),
            queue_id,
        };
        self.files.get(&key).map(|entry| entry.value().clone())
    }

    pub fn get_or_create(&self, topic: String, queue_id: i32) -> Result<Arc<TieredFlatFile<P>>, RocketMQError> {
        let key = FlatFileKey {
            topic: topic.clone(),
            queue_id,
        };
        let entry = self.files.entry(key).or_insert_with(|| {
            Arc::new(TieredFlatFile::new(
                topic,
                queue_id,
                self.config.clone(),
                self.metadata_store.clone(),
                self.provider.clone(),
            ))
        });
        Ok(entry.value().clone())
    }

    pub async fn cleanup_expired(&self, now_millis: i64) -> Result<(), RocketMQError> {
        let reserved_millis = self.config.file_reserved_time.as_millis() as i64;
        let expire_before_millis = now_millis.saturating_sub(reserved_millis);
        let flat_files = self.files.iter().map(|entry| entry.value().clone()).collect::<Vec<_>>();
        for flat_file in flat_files {
            flat_file.cleanup_expired(now_millis).await?;
        }
        self.remove_expired_index_entries(expire_before_millis);
        self.metadata_store
            .delete_index_entries_before(expire_before_millis)
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RocketMQError> {
        Ok(())
    }

    pub async fn destroy(&self) -> Result<(), RocketMQError> {
        self.files.clear();
        self.index.clear();
        Ok(())
    }

    pub async fn append_index(
        &self,
        request: &TieredDispatchRequest,
        tiered_commit_log_offset: u64,
    ) -> Result<(), RocketMQError> {
        if !self.config.message_index_enable || request.store_timestamp < 0 || request.message_size <= 0 {
            return Ok(());
        }

        let entries = build_index_entries(request, tiered_commit_log_offset);
        if entries.is_empty() {
            return Ok(());
        }

        for entry in entries {
            self.metadata_store.upsert_index_entry(entry.clone()).await?;
            self.put_index_entry(entry);
        }
        Ok(())
    }

    pub fn query_index(&self, topic: &str, key: &str, max_num: usize, begin: i64, end: i64) -> Vec<TieredIndexEntry> {
        if max_num == 0 || key.is_empty() || end < begin {
            return Vec::new();
        }

        let index_key = IndexKey {
            topic: topic.to_owned(),
            key: key.to_owned(),
        };
        let Some(entries) = self.index.get(&index_key) else {
            return Vec::new();
        };

        let mut result = entries
            .iter()
            .filter(|entry| entry.in_time_range(begin, end))
            .cloned()
            .collect::<Vec<_>>();
        result.sort_by_key(|entry| (entry.store_timestamp, entry.queue_id, entry.queue_offset));
        result.truncate(max_num);
        result
    }

    fn remove_expired_index_entries(&self, expire_before_millis: i64) {
        for mut entry in self.index.iter_mut() {
            entry.retain(|index_entry| index_entry.store_timestamp >= expire_before_millis);
        }
        let empty_keys = self
            .index
            .iter()
            .filter_map(|entry| entry.value().is_empty().then(|| entry.key().clone()))
            .collect::<Vec<_>>();
        for key in empty_keys {
            self.index.remove(&key);
        }
    }

    async fn load_index(&self) -> Result<(), RocketMQError> {
        self.index.clear();
        for entry in self.metadata_store.list_index_entries().await? {
            self.put_index_entry(entry);
        }
        Ok(())
    }

    fn put_index_entry(&self, entry: TieredIndexEntry) {
        let index_key = IndexKey {
            topic: entry.topic.clone(),
            key: entry.key.clone(),
        };
        let mut entries = self.index.entry(index_key).or_default();
        if !entries.contains(&entry) {
            entries.push(entry);
            entries.sort_by_key(|entry| (entry.store_timestamp, entry.queue_id, entry.queue_offset));
        }
    }
}

fn build_index_entries(request: &TieredDispatchRequest, tiered_commit_log_offset: u64) -> Vec<TieredIndexEntry> {
    collect_index_keys(request)
        .into_iter()
        .map(|key| TieredIndexEntry {
            topic: request.topic.clone(),
            key,
            queue_id: request.queue_id,
            queue_offset: request.queue_offset,
            commit_log_offset: tiered_commit_log_offset,
            message_size: request.message_size as usize,
            store_timestamp: request.store_timestamp,
        })
        .collect()
}

fn collect_index_keys(request: &TieredDispatchRequest) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(raw_keys) = &request.keys {
        for key in raw_keys.split_whitespace() {
            push_unique_key(&mut keys, key);
        }
    }
    if let Some(uniq_key) = &request.uniq_key {
        push_unique_key(&mut keys, uniq_key);
    }
    if let Some(offset_id) = &request.offset_id {
        push_unique_key(&mut keys, offset_id);
    }
    keys
}

fn push_unique_key(keys: &mut Vec<String>, key: &str) {
    if key.is_empty() || keys.iter().any(|existing| existing == key) {
        return;
    }
    keys.push(key.to_owned());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;

    fn test_request(timestamp: i64, keys: &str) -> TieredDispatchRequest {
        TieredDispatchRequest {
            topic: "TopicA".to_owned(),
            queue_id: 0,
            queue_offset: 1,
            commit_log_offset: 0,
            message_size: 4,
            tags_code: 0,
            store_timestamp: timestamp,
            keys: Some(keys.to_owned()),
            uniq_key: Some("uniqA".to_owned()),
            offset_id: None,
            sys_flag: 0,
            body: None,
        }
    }

    #[tokio::test]
    async fn index_query_deduplicates_keys_and_cleanup_removes_expired_entries() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            file_reserved_time: Duration::from_millis(10),
            backend_provider: "memory".to_owned(),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let store = TieredFlatFileStore::new(config, metadata_store, MemoryProvider::default());

        let old_request = test_request(100, "keyA keyA");
        let recent_request = test_request(195, "keyA");
        store.append_index(&old_request, 0).await?;
        store.append_index(&recent_request, 4).await?;

        assert_eq!(store.query_index("TopicA", "keyA", 10, 0, 500).len(), 2);
        assert_eq!(store.query_index("TopicA", "uniqA", 10, 0, 500).len(), 2);

        store.cleanup_expired(200).await?;

        let entries = store.query_index("TopicA", "keyA", 10, 0, 500);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].store_timestamp, 195);
        Ok(())
    }

    #[tokio::test]
    async fn load_restores_persisted_index_entries() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        metadata_store.load().await?;
        let provider = MemoryProvider::default();
        let store = TieredFlatFileStore::new(config.clone(), metadata_store.clone(), provider.clone());

        let request = test_request(100, "keyA");
        store.append_index(&request, 0).await?;
        assert_eq!(store.query_index("TopicA", "keyA", 10, 0, 500).len(), 1);

        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        reloaded_metadata_store.load().await?;
        let reloaded_store = TieredFlatFileStore::new(config, reloaded_metadata_store, provider);
        reloaded_store.load().await?;

        let entries = reloaded_store.query_index("TopicA", "keyA", 10, 0, 500);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].store_timestamp, 100);
        Ok(())
    }
}
