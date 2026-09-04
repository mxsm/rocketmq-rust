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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::RwLock;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tokio::fs;

use crate::config::TieredStoreConfig;
use crate::error;
use crate::file::TieredIndexEntry;
use crate::metadata::FileSegmentMetadata;
use crate::metadata::TopicMetadata;
use crate::metadata::TopicQueueMetadata;
use crate::provider::TieredProviderDescriptor;
use crate::provider::TieredProviderPersistence;

#[cfg(feature = "serde")]
const METADATA_FORMAT: &str = "rocketmq-tiered-metadata";
#[cfg(feature = "serde")]
const METADATA_VERSION: u32 = 1;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PersistedProviderContract {
    id: String,
    config_version: u32,
    format: Option<String>,
    version: Option<u32>,
}

impl From<TieredProviderDescriptor> for PersistedProviderContract {
    fn from(descriptor: TieredProviderDescriptor) -> Self {
        let (format, version) = match descriptor.persistence() {
            TieredProviderPersistence::Ephemeral => (None, None),
            TieredProviderPersistence::Stable { format, version } => (Some(format.to_owned()), Some(version)),
        };
        Self {
            id: descriptor.id().to_owned(),
            config_version: descriptor.config_version(),
            format,
            version,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait TieredMetadataStore: Send + Sync {
    async fn load(&self) -> Result<(), StoreError>;

    async fn persist(&self) -> Result<(), StoreError>;

    async fn destroy(&self) -> Result<(), StoreError>;

    async fn get_topic(&self, topic: &str) -> Result<Option<TopicMetadata>, StoreError>;

    async fn upsert_topic(&self, metadata: TopicMetadata) -> Result<(), StoreError>;

    async fn delete_topic(&self, topic: &str) -> Result<(), StoreError>;

    async fn get_queue(&self, topic: &str, queue_id: i32) -> Result<Option<TopicQueueMetadata>, StoreError>;

    async fn list_queues(&self) -> Result<Vec<TopicQueueMetadata>, StoreError>;

    async fn upsert_queue(&self, metadata: TopicQueueMetadata) -> Result<(), StoreError>;

    async fn delete_queue(&self, topic: &str, queue_id: i32) -> Result<(), StoreError>;

    async fn list_file_segments(&self, topic: &str, queue_id: i32) -> Result<Vec<FileSegmentMetadata>, StoreError>;

    async fn list_all_file_segments(&self) -> Result<Vec<FileSegmentMetadata>, StoreError>;

    async fn upsert_file_segment(&self, metadata: FileSegmentMetadata) -> Result<(), StoreError>;

    async fn mark_file_segment_deleted(&self, path: &str, base_offset: u64) -> Result<(), StoreError>;

    async fn list_index_entries(&self) -> Result<Vec<TieredIndexEntry>, StoreError>;

    async fn upsert_index_entry(&self, entry: TieredIndexEntry) -> Result<(), StoreError>;

    async fn delete_index_entries_before(&self, timestamp_millis: i64) -> Result<(), StoreError>;
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default, rename_all = "camelCase"))]
#[derive(Debug, Clone)]
struct MetadataState {
    #[cfg(feature = "serde")]
    format: String,
    #[cfg(feature = "serde")]
    version: u32,
    #[cfg(feature = "serde")]
    provider: Option<PersistedProviderContract>,
    topics: HashMap<String, TopicMetadata>,
    queues: HashMap<String, TopicQueueMetadata>,
    segments: HashMap<String, FileSegmentMetadata>,
    #[cfg_attr(feature = "serde", serde(default))]
    index: HashMap<String, Vec<TieredIndexEntry>>,
}

impl MetadataState {
    fn new(provider: Option<PersistedProviderContract>) -> Self {
        #[cfg(not(feature = "serde"))]
        let _ = provider;
        Self {
            #[cfg(feature = "serde")]
            format: METADATA_FORMAT.to_owned(),
            #[cfg(feature = "serde")]
            version: METADATA_VERSION,
            #[cfg(feature = "serde")]
            provider,
            topics: HashMap::new(),
            queues: HashMap::new(),
            segments: HashMap::new(),
            index: HashMap::new(),
        }
    }
}

impl Default for MetadataState {
    fn default() -> Self {
        Self::new(None)
    }
}

pub struct JsonMetadataStore {
    path: PathBuf,
    state: RwLock<MetadataState>,
    expected_provider: Option<PersistedProviderContract>,
    persist_lock: tokio::sync::Mutex<()>,
    successful_persists: AtomicU64,
}

impl JsonMetadataStore {
    pub fn new(config: Arc<TieredStoreConfig>) -> Self {
        Self::new_with_provider_descriptor(config, None)
    }

    pub fn new_with_provider_descriptor(
        config: Arc<TieredStoreConfig>,
        provider_descriptor: Option<TieredProviderDescriptor>,
    ) -> Self {
        let expected_provider = provider_descriptor.map(PersistedProviderContract::from);
        Self {
            path: config
                .store_path_root_dir
                .join("config")
                .join("tieredStoreMetadata.json"),
            state: RwLock::new(MetadataState::new(expected_provider.clone())),
            expected_provider,
            persist_lock: tokio::sync::Mutex::new(()),
            successful_persists: AtomicU64::new(0),
        }
    }

    /// Returns metadata snapshots this store instance successfully replaced on disk.
    pub fn successful_persist_count(&self) -> u64 {
        self.successful_persists.load(Ordering::Relaxed)
    }

    fn queue_key(topic: &str, queue_id: i32) -> String {
        format!("{topic}@{queue_id}")
    }

    fn segment_key(path: &str, base_offset: u64) -> String {
        format!("{path}@{base_offset:020}")
    }

    fn index_key(topic: &str, key: &str) -> String {
        format!("{topic}@{key}")
    }
}

impl TieredMetadataStore for JsonMetadataStore {
    async fn load(&self) -> Result<(), StoreError> {
        if !metadata_exists(fs::metadata(&self.path).await)? {
            return Ok(());
        }

        #[cfg(feature = "serde")]
        {
            let data = fs::read(&self.path)
                .await
                .map_err(|source| error::io_failed(StoreOperation::Load, source))?;
            let mut state = serde_json::from_slice::<MetadataState>(&data)
                .map_err(|source| error::state_corrupted_source(StoreOperation::Load, source))?;
            if state.format != METADATA_FORMAT || state.version != METADATA_VERSION {
                return Err(error::state_corrupted(StoreOperation::Load));
            }
            match (&state.provider, &self.expected_provider) {
                (Some(stored), Some(expected)) if stored != expected => {
                    return Err(error::state_corrupted(StoreOperation::Load));
                }
                (None, Some(expected)) => state.provider = Some(expected.clone()),
                _ => {}
            }
            *self.state.write() = state;
        }

        Ok(())
    }

    async fn persist(&self) -> Result<(), StoreError> {
        let _persist_guard = self.persist_lock.lock().await;
        let Some(parent) = self.path.parent() else {
            return Err(error::internal_failure(StoreOperation::AppendDerived));
        };
        fs::create_dir_all(parent)
            .await
            .map_err(|source| error::io_failed(StoreOperation::AppendDerived, source))?;

        #[cfg(feature = "serde")]
        {
            let snapshot = self.state.read().clone();
            let data = serde_json::to_vec_pretty(&snapshot)
                .map_err(|source| error::write_failed(StoreOperation::AppendDerived, source))?;
            let tmp_path = self.path.with_extension("json.tmp");
            fs::write(&tmp_path, data)
                .await
                .map_err(|source| error::io_failed(StoreOperation::AppendDerived, source))?;
            fs::OpenOptions::new()
                .write(true)
                .open(&tmp_path)
                .await
                .map_err(|source| error::io_failed(StoreOperation::AppendDerived, source))?
                .sync_all()
                .await
                .map_err(|source| error::io_failed(StoreOperation::AppendDerived, source))?;
            fs::rename(&tmp_path, &self.path)
                .await
                .map_err(|source| error::io_failed(StoreOperation::AppendDerived, source))?;
            self.successful_persists.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn destroy(&self) -> Result<(), StoreError> {
        *self.state.write() = MetadataState::new(self.expected_provider.clone());
        Ok(())
    }

    async fn get_topic(&self, topic: &str) -> Result<Option<TopicMetadata>, StoreError> {
        Ok(self.state.read().topics.get(topic).cloned())
    }

    async fn upsert_topic(&self, metadata: TopicMetadata) -> Result<(), StoreError> {
        {
            self.state.write().topics.insert(metadata.topic.clone(), metadata);
        }
        self.persist().await
    }

    async fn delete_topic(&self, topic: &str) -> Result<(), StoreError> {
        {
            self.state.write().topics.remove(topic);
        }
        self.persist().await
    }

    async fn get_queue(&self, topic: &str, queue_id: i32) -> Result<Option<TopicQueueMetadata>, StoreError> {
        Ok(self.state.read().queues.get(&Self::queue_key(topic, queue_id)).cloned())
    }

    async fn list_queues(&self) -> Result<Vec<TopicQueueMetadata>, StoreError> {
        Ok(self.state.read().queues.values().cloned().collect())
    }

    async fn upsert_queue(&self, metadata: TopicQueueMetadata) -> Result<(), StoreError> {
        {
            self.state
                .write()
                .queues
                .insert(Self::queue_key(&metadata.topic, metadata.queue_id), metadata);
        }
        self.persist().await
    }

    async fn delete_queue(&self, topic: &str, queue_id: i32) -> Result<(), StoreError> {
        {
            self.state.write().queues.remove(&Self::queue_key(topic, queue_id));
        }
        self.persist().await
    }

    async fn list_file_segments(&self, topic: &str, queue_id: i32) -> Result<Vec<FileSegmentMetadata>, StoreError> {
        let queue_prefix = format!("{topic}/{queue_id}/");
        let mut segments = self
            .state
            .read()
            .segments
            .values()
            .filter(|segment| segment.path.starts_with(&queue_prefix))
            .cloned()
            .collect::<Vec<_>>();
        segments.sort_by_key(|segment| (segment.segment_type, segment.base_offset));
        Ok(segments)
    }

    async fn list_all_file_segments(&self) -> Result<Vec<FileSegmentMetadata>, StoreError> {
        let mut segments = self.state.read().segments.values().cloned().collect::<Vec<_>>();
        segments.sort_by_key(|segment| (segment.path.clone(), segment.segment_type, segment.base_offset));
        Ok(segments)
    }

    async fn upsert_file_segment(&self, metadata: FileSegmentMetadata) -> Result<(), StoreError> {
        {
            self.state
                .write()
                .segments
                .insert(Self::segment_key(&metadata.path, metadata.base_offset), metadata);
        }
        self.persist().await
    }

    async fn mark_file_segment_deleted(&self, path: &str, base_offset: u64) -> Result<(), StoreError> {
        {
            if let Some(segment) = self
                .state
                .write()
                .segments
                .get_mut(&Self::segment_key(path, base_offset))
            {
                segment.status = crate::file::FileSegmentStatus::Deleted;
            }
        }
        self.persist().await
    }

    async fn list_index_entries(&self) -> Result<Vec<TieredIndexEntry>, StoreError> {
        Ok(self
            .state
            .read()
            .index
            .values()
            .flat_map(|entries| entries.iter().cloned())
            .collect())
    }

    async fn upsert_index_entry(&self, entry: TieredIndexEntry) -> Result<(), StoreError> {
        {
            let mut state = self.state.write();
            let entries = state
                .index
                .entry(Self::index_key(&entry.topic, &entry.key))
                .or_default();
            if !entries.contains(&entry) {
                entries.push(entry);
                entries.sort_by_key(|entry| (entry.store_timestamp, entry.queue_id, entry.queue_offset));
            }
        }
        self.persist().await
    }

    async fn delete_index_entries_before(&self, timestamp_millis: i64) -> Result<(), StoreError> {
        {
            let mut state = self.state.write();
            for entries in state.index.values_mut() {
                entries.retain(|entry| entry.store_timestamp >= timestamp_millis);
            }
            state.index.retain(|_, entries| !entries.is_empty());
        }
        self.persist().await
    }
}

fn metadata_exists(result: std::io::Result<std::fs::Metadata>) -> Result<bool, StoreError> {
    match result {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(error::io_failed(StoreOperation::Load, source)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_store_api::StoreError;

    use crate::config::TieredStoreConfig;
    #[cfg(feature = "serde")]
    use crate::file::FileSegmentType;
    #[cfg(feature = "serde")]
    use crate::metadata::FileSegmentMetadata;
    use crate::metadata::JsonMetadataStore;
    use crate::metadata::TieredMetadataStore;
    use crate::metadata::TopicMetadata;
    #[cfg(feature = "serde")]
    use crate::metadata::TopicQueueMetadata;
    #[cfg(feature = "serde")]
    use crate::TieredIndexEntry;

    fn assert_redacted(error: &StoreError, sentinel: &str) {
        assert!(!error.to_string().contains(sentinel));
        assert!(!format!("{error:?}").contains(sentinel));
    }

    #[test]
    fn load_preserves_non_not_found_io_source_without_rendering_path() {
        let sentinel = "sensitive-metadata-io-canary";
        let error = super::metadata_exists(Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, sentinel)))
            .expect_err("metadata stat should fail");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_IO_FAILED);
        assert_eq!(error.operation(), rocketmq_store_api::StoreOperation::Load);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
        assert_redacted(&error, sentinel);
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn load_preserves_json_source_without_rendering_payload() {
        let temp_dir = tempfile::tempdir().expect("temporary directory");
        let sentinel = "sensitive-metadata-json-canary";
        let config_dir = temp_dir.path().join("config");
        tokio::fs::create_dir_all(&config_dir).await.expect("config directory");
        tokio::fs::write(
            config_dir.join("tieredStoreMetadata.json"),
            format!("{{\"{sentinel}\":"),
        )
        .await
        .expect("corrupt metadata");
        let store = JsonMetadataStore::new(Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            ..TieredStoreConfig::default()
        }));

        let error = store.load().await.expect_err("invalid metadata JSON should fail");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_STATE_CORRUPTED);
        assert_eq!(error.operation(), rocketmq_store_api::StoreOperation::Load);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<serde_json::Error>())
            .is_some());
        assert_redacted(&error, sentinel);
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn persists_and_loads_metadata() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            ..TieredStoreConfig::default()
        });

        let metadata_store = JsonMetadataStore::new(config.clone());
        let topic_metadata = TopicMetadata {
            topic_id: 1,
            topic: "TopicA".to_owned(),
            reserve_time_millis: 72 * 60 * 60 * 1000,
            status: 0,
            update_timestamp: 100,
        };
        let queue_metadata = TopicQueueMetadata {
            topic: "TopicA".to_owned(),
            queue_id: 0,
            min_offset: 10,
            max_offset: 20,
            update_timestamp: 101,
        };

        metadata_store.upsert_topic(topic_metadata.clone()).await?;
        metadata_store.upsert_queue(queue_metadata.clone()).await?;
        let segment = FileSegmentMetadata::new(
            "TopicA/0/commitlog/00000000000000000000".to_owned(),
            FileSegmentType::CommitLog,
            0,
        );
        metadata_store.upsert_file_segment(segment.clone()).await?;
        let index_entry = TieredIndexEntry {
            topic: "TopicA".to_owned(),
            key: "keyA".to_owned(),
            queue_id: 0,
            queue_offset: 10,
            commit_log_offset: 0,
            message_size: 4,
            store_timestamp: 100,
        };
        metadata_store.upsert_index_entry(index_entry.clone()).await?;
        assert_eq!(metadata_store.successful_persist_count(), 4);

        let reloaded_store = JsonMetadataStore::new(config);
        reloaded_store.load().await?;
        assert_eq!(reloaded_store.successful_persist_count(), 0);

        assert_eq!(reloaded_store.get_topic("TopicA").await?, Some(topic_metadata));
        assert_eq!(reloaded_store.get_queue("TopicA", 0).await?, Some(queue_metadata));
        assert_eq!(reloaded_store.list_file_segments("TopicA", 0).await?.len(), 1);
        assert_eq!(reloaded_store.list_all_file_segments().await?, vec![segment]);
        assert_eq!(reloaded_store.list_index_entries().await?, vec![index_entry]);
        Ok(())
    }

    #[tokio::test]
    async fn failed_metadata_replace_does_not_advance_persist_counter() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let blocked_root = temp_dir.path().join("not-a-directory");
        tokio::fs::write(&blocked_root, b"blocked").await.map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let metadata_store = JsonMetadataStore::new(Arc::new(TieredStoreConfig {
            store_path_root_dir: blocked_root,
            ..TieredStoreConfig::default()
        }));

        assert!(metadata_store
            .upsert_topic(TopicMetadata {
                topic_id: 1,
                topic: "TopicA".to_owned(),
                reserve_time_millis: 10_000,
                status: 0,
                update_timestamp: 100,
            })
            .await
            .is_err());
        assert_eq!(metadata_store.successful_persist_count(), 0);
        Ok(())
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn supports_topic_queue_and_segment_delete_semantics() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            ..TieredStoreConfig::default()
        });
        let metadata_store = JsonMetadataStore::new(config.clone());
        metadata_store
            .upsert_topic(TopicMetadata {
                topic_id: 1,
                topic: "TopicA".to_owned(),
                reserve_time_millis: 10_000,
                status: 0,
                update_timestamp: 100,
            })
            .await?;
        metadata_store
            .upsert_queue(TopicQueueMetadata {
                topic: "TopicA".to_owned(),
                queue_id: 0,
                min_offset: 0,
                max_offset: 2,
                update_timestamp: 101,
            })
            .await?;
        let segment = FileSegmentMetadata::new(
            "TopicA/0/consumequeue/00000000000000000000".to_owned(),
            FileSegmentType::ConsumeQueue,
            0,
        );
        metadata_store.upsert_file_segment(segment.clone()).await?;

        metadata_store.delete_topic("TopicA").await?;
        metadata_store.delete_queue("TopicA", 0).await?;
        metadata_store
            .mark_file_segment_deleted(&segment.path, segment.base_offset)
            .await?;

        let reloaded_store = JsonMetadataStore::new(config);
        reloaded_store.load().await?;

        assert_eq!(reloaded_store.get_topic("TopicA").await?, None);
        assert_eq!(reloaded_store.get_queue("TopicA", 0).await?, None);
        let segments = reloaded_store.list_file_segments("TopicA", 0).await?;
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, crate::file::FileSegmentStatus::Deleted);
        Ok(())
    }
}
