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

use rocketmq_error::RocketMQError;

use crate::file::TieredFlatFileStore;
use crate::metadata::JsonMetadataStore;
use crate::metadata::TieredMetadataStore;
use crate::provider::TieredStoreProvider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieredRecoverResult {
    pub flat_file_count: usize,
    pub index_segment_count: usize,
}

pub struct CommitLogRecoverService<P>
where
    P: TieredStoreProvider,
{
    metadata_store: Arc<JsonMetadataStore>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
}

impl<P> CommitLogRecoverService<P>
where
    P: TieredStoreProvider,
{
    pub fn new(metadata_store: Arc<JsonMetadataStore>, flat_file_store: Arc<TieredFlatFileStore<P>>) -> Self {
        Self {
            metadata_store,
            flat_file_store,
        }
    }

    pub async fn recover(&self) -> Result<TieredRecoverResult, RocketMQError> {
        self.metadata_store.load().await?;
        self.flat_file_store.load().await?;
        Ok(TieredRecoverResult {
            flat_file_count: self.flat_file_store.flat_file_count(),
            index_segment_count: self.flat_file_store.index_segment_count().await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;

    use super::CommitLogRecoverService;
    use crate::config::TieredStoreConfig;
    use crate::file::ConsumeQueueUnit;
    use crate::file::FileSegment;
    use crate::file::FileSegmentType;
    use crate::file::TieredFlatFileStore;
    use crate::metadata::JsonMetadataStore;
    use crate::metadata::TieredMetadataStore;
    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;

    #[tokio::test]
    async fn recover_corrects_half_committed_segment_metadata_size() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 64,
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
        let mut commit_log_metadata = commit_log_segment.metadata();
        commit_log_metadata.size = 12;
        metadata_store.upsert_file_segment(commit_log_metadata.clone()).await?;

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
                    tags_code: 7,
                }
                .encode(),
                100,
            )
            .await?;
        consume_queue_segment.commit().await?;
        let mut consume_queue_metadata = consume_queue_segment.metadata();
        consume_queue_metadata.size = crate::file::CONSUME_QUEUE_UNIT_SIZE as u64 * 2;
        metadata_store
            .upsert_file_segment(consume_queue_metadata.clone())
            .await?;

        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config,
            reloaded_metadata_store.clone(),
            provider,
        ));
        let service = CommitLogRecoverService::new(reloaded_metadata_store.clone(), flat_file_store.clone());
        let result = service.recover().await?;

        assert_eq!(result.flat_file_count, 1);
        let flat_file = flat_file_store
            .get("TopicA", 0)
            .ok_or_else(|| RocketMQError::Internal("missing recovered flat file".to_owned()))?;
        assert_eq!(
            flat_file.read_message_by_queue_offset(0).await?,
            Some(Bytes::from_static(b"body"))
        );
        let segments = reloaded_metadata_store.list_file_segments("TopicA", 0).await?;
        assert!(segments.iter().any(|segment| {
            segment.segment_type == FileSegmentType::CommitLog
                && segment.path == commit_log_metadata.path
                && segment.size == 4
        }));
        assert!(segments.iter().any(|segment| {
            segment.segment_type == FileSegmentType::ConsumeQueue
                && segment.path == consume_queue_metadata.path
                && segment.size == crate::file::CONSUME_QUEUE_UNIT_SIZE as u64
        }));
        Ok(())
    }
}
