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
use crate::fetcher::read_ahead_cache::ReadAheadCache;
use crate::file::FileSegmentStatus;
use crate::file::FileSegmentType;
use crate::file::IndexFileSegment;
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

pub struct TieredFlatFileStore<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    metadata_store: Arc<JsonMetadataStore>,
    provider: P,
    read_ahead_cache: Arc<ReadAheadCache>,
    files: DashMap<FlatFileKey, Arc<TieredFlatFile<P>>>,
    index_file: IndexFileSegment<P>,
}

impl<P> TieredFlatFileStore<P>
where
    P: TieredStoreProvider,
{
    pub fn new(config: Arc<TieredStoreConfig>, metadata_store: Arc<JsonMetadataStore>, provider: P) -> Self {
        let read_ahead_cache = Arc::new(ReadAheadCache::new(
            config.read_ahead_cache_enable,
            config.read_ahead_cache_max_bytes,
            config.read_ahead_cache_expire,
        ));
        let index_file = IndexFileSegment::with_limits(
            IndexFileSegment::<P>::default_path().to_owned(),
            provider.clone(),
            config.index_file_max_hash_slot_num as usize,
            config.index_file_max_index_num as usize,
        );
        Self {
            config,
            metadata_store,
            provider,
            read_ahead_cache,
            files: DashMap::new(),
            index_file,
        }
    }

    pub async fn load(&self) -> Result<(), RocketMQError> {
        let keys = self.recoverable_flat_file_keys().await?;
        for key in keys {
            let flat_file = self.get_or_create(key.topic, key.queue_id)?;
            flat_file.recover().await?;
        }
        self.recover_index_file().await?;
        Ok(())
    }

    pub fn get(&self, topic: &str, queue_id: i32) -> Option<Arc<TieredFlatFile<P>>> {
        let key = FlatFileKey {
            topic: topic.to_owned(),
            queue_id,
        };
        self.files.get(&key).map(|entry| entry.value().clone())
    }

    pub fn flat_file_count(&self) -> usize {
        self.files.len()
    }

    pub async fn index_segment_count(&self) -> Result<usize, RocketMQError> {
        self.index_file.segment_count().await
    }

    pub fn get_or_create(&self, topic: String, queue_id: i32) -> Result<Arc<TieredFlatFile<P>>, RocketMQError> {
        let key = FlatFileKey {
            topic: topic.clone(),
            queue_id,
        };
        let entry = self.files.entry(key).or_insert_with(|| {
            Arc::new(TieredFlatFile::new_with_read_ahead_cache(
                topic,
                queue_id,
                self.config.clone(),
                self.metadata_store.clone(),
                self.provider.clone(),
                self.read_ahead_cache.clone(),
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
        self.metadata_store
            .delete_index_entries_before(expire_before_millis)
            .await?;
        self.compact_index_file(expire_before_millis).await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RocketMQError> {
        Ok(())
    }

    pub async fn destroy(&self) -> Result<(), RocketMQError> {
        self.files.clear();
        self.read_ahead_cache.clear();
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
            self.index_file.append_entry(&entry).await?;
            self.metadata_store.upsert_index_entry(entry.clone()).await?;
        }
        Ok(())
    }

    pub(crate) async fn query_index_entries(
        &self,
        topic: &str,
        key: &str,
        max_num: usize,
        begin: i64,
        end: i64,
    ) -> Result<Vec<TieredIndexEntry>, RocketMQError> {
        if max_num == 0 || topic.is_empty() || key.is_empty() || end < begin {
            return Ok(Vec::new());
        }

        if self.index_file.segment_count().await? == 0 {
            return Ok(Vec::new());
        }

        self.index_file.query_entries(topic, key, max_num, begin, end).await
    }

    async fn recover_index_file(&self) -> Result<(), RocketMQError> {
        if self.index_file.segment_count().await? > 0 {
            let entries = self.index_file.load_entries().await?;
            if !entries.is_empty() {
                return Ok(());
            }
        }

        let metadata_entries = self.metadata_store.list_index_entries().await?;
        if !metadata_entries.is_empty() {
            self.index_file.compact_entries(&metadata_entries).await?;
        }

        Ok(())
    }

    async fn compact_index_file(&self, retain_from_timestamp_millis: i64) -> Result<(), RocketMQError> {
        let mut entries = self.index_entries_for_compaction().await?;
        entries.retain(|entry| entry.store_timestamp >= retain_from_timestamp_millis);
        self.index_file.compact_entries(&entries).await
    }

    async fn index_entries_for_compaction(&self) -> Result<Vec<TieredIndexEntry>, RocketMQError> {
        let mut entries = if self.index_file.segment_count().await? > 0 {
            self.index_file.load_entries().await?
        } else {
            self.metadata_store.list_index_entries().await?
        };
        entries.sort_by_key(|entry| {
            (
                entry.topic.clone(),
                entry.key.clone(),
                entry.store_timestamp,
                entry.queue_id,
                entry.queue_offset,
            )
        });
        entries.dedup();
        Ok(entries)
    }

    async fn recoverable_flat_file_keys(&self) -> Result<Vec<FlatFileKey>, RocketMQError> {
        let mut keys = self
            .metadata_store
            .list_queues()
            .await?
            .into_iter()
            .map(|queue| FlatFileKey {
                topic: queue.topic,
                queue_id: queue.queue_id,
            })
            .collect::<Vec<_>>();

        for segment in self.metadata_store.list_all_file_segments().await? {
            if segment.status == FileSegmentStatus::Deleted || segment.segment_type == FileSegmentType::Index {
                continue;
            }
            let Some(key) = flat_file_key_from_segment_path(&segment.path) else {
                continue;
            };
            if !keys.contains(&key) {
                keys.push(key);
            }
        }

        keys.sort_by(|left, right| left.topic.cmp(&right.topic).then(left.queue_id.cmp(&right.queue_id)));
        keys.dedup();
        Ok(keys)
    }
}

fn flat_file_key_from_segment_path(path: &str) -> Option<FlatFileKey> {
    let mut parts = path.rsplitn(4, '/');
    let _base_offset = parts.next()?;
    let segment_name = parts.next()?;
    let queue_id = parts.next()?.parse::<i32>().ok()?;
    let topic = parts.next()?;
    if topic.is_empty() {
        return None;
    }
    match segment_name {
        "commitlog" | "consumequeue" => Some(FlatFileKey {
            topic: topic.to_owned(),
            queue_id,
        }),
        _ => None,
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

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::file::ConsumeQueueUnit;
    use crate::file::FileSegment;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;

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
            file_reserved_time: Duration::from_millis(10_000),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let store = TieredFlatFileStore::new(config, metadata_store, MemoryProvider::default());

        let old_request = test_request(100_000, "keyA keyA");
        let recent_request = test_request(195_000, "keyA");
        store.append_index(&old_request, 0).await?;
        store.append_index(&recent_request, 4).await?;

        assert_eq!(
            store.query_index_entries("TopicA", "keyA", 10, 0, 500_000).await?.len(),
            2
        );
        assert_eq!(
            store
                .query_index_entries("TopicA", "uniqA", 10, 0, 500_000)
                .await?
                .len(),
            2
        );

        store.cleanup_expired(200_000).await?;

        let entries = store.query_index_entries("TopicA", "keyA", 10, 0, 500_000).await?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].store_timestamp, 195_000);
        let compacted_entries = store.index_file.load_entries().await?;
        assert!(!compacted_entries.is_empty());
        assert!(compacted_entries.iter().all(|entry| entry.store_timestamp >= 190_000));
        Ok(())
    }

    #[tokio::test]
    async fn load_restores_persisted_index_entries() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        metadata_store.load().await?;
        let provider = MemoryProvider::default();
        let store = TieredFlatFileStore::new(config.clone(), metadata_store.clone(), provider.clone());

        let request = test_request(100, "keyA");
        store.append_index(&request, 0).await?;
        assert_eq!(store.query_index_entries("TopicA", "keyA", 10, 0, 500).await?.len(), 1);

        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        reloaded_metadata_store.load().await?;
        let reloaded_store = TieredFlatFileStore::new(config, reloaded_metadata_store, provider);
        reloaded_store.load().await?;

        let entries = reloaded_store.query_index_entries("TopicA", "keyA", 10, 0, 500).await?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].store_timestamp, 100);
        Ok(())
    }

    #[tokio::test]
    async fn load_restores_index_entries_from_provider_index_file() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().join("metadata-a"),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let provider = MemoryProvider::default();
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let store = TieredFlatFileStore::new(config.clone(), metadata_store, provider.clone());

        let request = test_request(100, "keyA");
        store.append_index(&request, 0).await?;

        let reloaded_config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().join("metadata-b"),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(reloaded_config.clone()));
        reloaded_metadata_store.load().await?;
        let reloaded_store = TieredFlatFileStore::new(reloaded_config, reloaded_metadata_store, provider);

        let entries = reloaded_store.query_index_entries("TopicA", "keyA", 10, 0, 500).await?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].store_timestamp, 100);
        Ok(())
    }

    #[tokio::test]
    async fn load_recovers_index_file_from_metadata_when_index_file_is_absent() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let entry = TieredIndexEntry {
            topic: "TopicA".to_owned(),
            key: "keyA".to_owned(),
            queue_id: 0,
            queue_offset: 1,
            commit_log_offset: 0,
            message_size: 4,
            store_timestamp: 100,
        };
        metadata_store.upsert_index_entry(entry.clone()).await?;

        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        reloaded_metadata_store.load().await?;
        let store = TieredFlatFileStore::new(config, reloaded_metadata_store, MemoryProvider::default());
        store.load().await?;
        assert_eq!(store.index_file.segment_count().await?, 1);

        let entries = store.query_index_entries("TopicA", "keyA", 10, 0, 500).await?;

        assert_eq!(entries, vec![entry]);
        Ok(())
    }

    #[tokio::test]
    async fn load_recovers_queue_from_segment_metadata_when_queue_metadata_is_missing() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 8,
            consume_queue_segment_size: crate::file::CONSUME_QUEUE_UNIT_SIZE as u64,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();

        let commit_log_segment = provider
            .create_segment(
                "TopicA/0/commitlog/00000000000000000000".to_owned(),
                FileSegmentType::CommitLog,
                0,
                config.commit_log_segment_size,
            )
            .await?;
        commit_log_segment.append(Bytes::from_static(b"body"), 100).await?;
        commit_log_segment.commit().await?;
        metadata_store
            .upsert_file_segment(commit_log_segment.metadata())
            .await?;

        let consume_queue_segment = provider
            .create_segment(
                "TopicA/0/consumequeue/00000000000000000000".to_owned(),
                FileSegmentType::ConsumeQueue,
                0,
                config.consume_queue_segment_size,
            )
            .await?;
        consume_queue_segment
            .append(
                ConsumeQueueUnit {
                    commit_log_offset: 0,
                    size: 4,
                    tags_code: 1,
                }
                .encode(),
                100,
            )
            .await?;
        consume_queue_segment.commit().await?;
        metadata_store
            .upsert_file_segment(consume_queue_segment.metadata())
            .await?;

        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        reloaded_metadata_store.load().await?;
        let store = TieredFlatFileStore::new(config, reloaded_metadata_store.clone(), provider);
        store.load().await?;

        let flat_file = store
            .get("TopicA", 0)
            .ok_or_else(|| RocketMQError::Internal("missing recovered flat file".to_owned()))?;
        assert_eq!(
            flat_file.read_message_by_queue_offset(0).await?,
            Some(Bytes::from_static(b"body"))
        );
        let queue_metadata = reloaded_metadata_store
            .get_queue("TopicA", 0)
            .await?
            .ok_or_else(|| RocketMQError::Internal("missing recovered queue metadata".to_owned()))?;
        assert_eq!(queue_metadata.min_offset, 0);
        assert_eq!(queue_metadata.max_offset, 1);
        Ok(())
    }

    #[test]
    fn parses_flat_file_key_from_segment_path() {
        assert_eq!(
            flat_file_key_from_segment_path("TopicA/3/commitlog/00000000000000000000"),
            Some(FlatFileKey {
                topic: "TopicA".to_owned(),
                queue_id: 3,
            })
        );
        assert_eq!(flat_file_key_from_segment_path("TopicA/3/index/000"), None);
        assert_eq!(flat_file_key_from_segment_path("bad-path"), None);
    }
}
