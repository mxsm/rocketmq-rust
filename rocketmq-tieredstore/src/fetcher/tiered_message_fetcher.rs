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

use bytes::Bytes;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TieredGetMessageStatus {
    Found,
    NoMatchedMessage,
    OffsetFoundNull,
    OffsetOverflowBadly,
    OffsetOverflowOne,
    OffsetTooSmall,
    #[default]
    NoMatchedLogicQueue,
}

#[derive(Debug, Default)]
pub struct TieredGetMessageResult {
    pub status: TieredGetMessageStatus,
    pub messages: Vec<Bytes>,
    pub min_offset: i64,
    pub max_offset: i64,
    pub next_begin_offset: i64,
}

#[derive(Debug, Default)]
pub struct TieredQueryResult<T> {
    pub values: Vec<T>,
}

#[allow(async_fn_in_trait)]
pub trait TieredMessageFetcher: Send + Sync {
    async fn get_message(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
        max_msg_nums: i32,
    ) -> Result<TieredGetMessageResult, RocketMQError>;

    async fn get_message_timestamp(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<i64, RocketMQError>;

    async fn get_offset_by_time(
        &self,
        topic: String,
        queue_id: i32,
        timestamp_millis: i64,
    ) -> Result<i64, RocketMQError>;

    async fn get_offset_by_time_with_boundary(
        &self,
        topic: String,
        queue_id: i32,
        timestamp_millis: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, RocketMQError>;

    async fn query_message(
        &self,
        topic: String,
        key: String,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<TieredQueryResult<Bytes>, RocketMQError>;
}

pub struct DefaultTieredMessageFetcher<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
}

impl<P> DefaultTieredMessageFetcher<P>
where
    P: TieredStoreProvider,
{
    pub fn new(config: Arc<TieredStoreConfig>, flat_file_store: Arc<TieredFlatFileStore<P>>) -> Self {
        Self {
            config,
            flat_file_store,
        }
    }
}

impl<P> TieredMessageFetcher for DefaultTieredMessageFetcher<P>
where
    P: TieredStoreProvider,
{
    async fn get_message(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
        max_msg_nums: i32,
    ) -> Result<TieredGetMessageResult, RocketMQError> {
        let Some(flat_file) = self.flat_file_store.get(&topic, queue_id) else {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::NoMatchedLogicQueue,
                ..TieredGetMessageResult::default()
            });
        };

        let min_offset = flat_file.consume_queue_min_offset();
        let max_offset = flat_file.consume_queue_commit_offset();
        if max_offset <= min_offset {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::NoMatchedMessage,
                min_offset,
                max_offset,
                next_begin_offset: max_offset,
                ..TieredGetMessageResult::default()
            });
        }
        if queue_offset < min_offset {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::OffsetTooSmall,
                min_offset,
                max_offset,
                next_begin_offset: min_offset,
                ..TieredGetMessageResult::default()
            });
        }
        if queue_offset == max_offset {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::OffsetOverflowOne,
                min_offset,
                max_offset,
                next_begin_offset: max_offset,
                ..TieredGetMessageResult::default()
            });
        }
        if queue_offset > max_offset {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::OffsetOverflowBadly,
                min_offset,
                max_offset,
                next_begin_offset: max_offset,
                ..TieredGetMessageResult::default()
            });
        }

        let max_msg_nums = (max_msg_nums.max(1) as usize).min(self.config.read_ahead_message_count.max(1));
        let mut messages = Vec::with_capacity(max_msg_nums.min(16));
        let mut next_begin_offset = queue_offset;
        for offset in queue_offset..max_offset {
            if messages.len() >= max_msg_nums {
                break;
            }
            match flat_file.read_message_by_queue_offset(offset).await? {
                Some(message) => {
                    messages.push(message);
                    next_begin_offset = offset.saturating_add(1);
                }
                None => {
                    return Ok(TieredGetMessageResult {
                        status: TieredGetMessageStatus::OffsetFoundNull,
                        messages,
                        min_offset,
                        max_offset,
                        next_begin_offset: offset,
                    });
                }
            }
        }

        let _ = &self.config;
        if messages.is_empty() {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::NoMatchedMessage,
                min_offset,
                max_offset,
                next_begin_offset,
                ..TieredGetMessageResult::default()
            });
        }
        Ok(TieredGetMessageResult {
            status: TieredGetMessageStatus::Found,
            messages,
            min_offset,
            max_offset,
            next_begin_offset,
        })
    }

    async fn get_message_timestamp(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<i64, RocketMQError> {
        let Some(flat_file) = self.flat_file_store.get(&topic, queue_id) else {
            return Ok(-1);
        };
        if queue_offset.saturating_add(1) == flat_file.consume_queue_commit_offset() {
            return Ok(flat_file.max_store_timestamp());
        }
        Ok(flat_file
            .read_message_store_timestamp(queue_offset)
            .await?
            .unwrap_or(-1))
    }

    async fn get_offset_by_time(
        &self,
        topic: String,
        queue_id: i32,
        timestamp_millis: i64,
    ) -> Result<i64, RocketMQError> {
        self.get_offset_by_time_with_boundary(topic, queue_id, timestamp_millis, BoundaryType::Lower)
            .await
    }

    async fn get_offset_by_time_with_boundary(
        &self,
        topic: String,
        queue_id: i32,
        timestamp_millis: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, RocketMQError> {
        let Some(flat_file) = self.flat_file_store.get(&topic, queue_id) else {
            return Ok(-1);
        };
        flat_file
            .queue_offset_by_time_with_boundary(timestamp_millis, boundary_type)
            .await
    }

    async fn query_message(
        &self,
        topic: String,
        key: String,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<TieredQueryResult<Bytes>, RocketMQError> {
        let max_num = max_num.max(0) as usize;
        let entries = self
            .flat_file_store
            .query_index_entries(&topic, &key, max_num, begin, end)
            .await?;
        if entries.is_empty() {
            return Ok(TieredQueryResult::default());
        }

        let mut values = Vec::with_capacity(entries.len());
        for entry in entries {
            if values.len() >= max_num {
                break;
            }
            let Some(flat_file) = self.flat_file_store.get(&entry.topic, entry.queue_id) else {
                continue;
            };
            if let Some(message) = flat_file
                .read_commit_log(entry.commit_log_offset, entry.message_size)
                .await?
            {
                values.push(message);
            }
        }
        Ok(TieredQueryResult { values })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use bytes::BytesMut;
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::config::TieredStoreConfig;
    use crate::dispatcher::TieredDispatchRequest;
    use crate::file::ConsumeQueueUnit;
    use crate::file::TieredFlatFileStore;
    use crate::metadata::JsonMetadataStore;
    use crate::metadata::TieredMetadataStore;
    use crate::provider::MemoryProvider;

    async fn build_fetcher() -> Result<DefaultTieredMessageFetcher<MemoryProvider>, RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(config.clone(), metadata_store, provider));
        let flat_file = flat_file_store.get_or_create("TopicA".to_owned(), 0)?;

        let first_offset = flat_file.append_commit_log(Bytes::from_static(b"msg-a"), 100).await?;
        flat_file
            .append_consume_queue(
                3,
                ConsumeQueueUnit {
                    commit_log_offset: first_offset as i64,
                    size: 5,
                    tags_code: 1,
                },
                100,
            )
            .await?;

        let second_offset = flat_file.append_commit_log(Bytes::from_static(b"msg-b"), 101).await?;
        flat_file
            .append_consume_queue(
                4,
                ConsumeQueueUnit {
                    commit_log_offset: second_offset as i64,
                    size: 5,
                    tags_code: 1,
                },
                101,
            )
            .await?;
        flat_file.commit().await?;

        Ok(DefaultTieredMessageFetcher::new(config, flat_file_store))
    }

    async fn build_timestamp_fetcher() -> Result<DefaultTieredMessageFetcher<MemoryProvider>, RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            index_file_max_hash_slot_num: 8,
            index_file_max_index_num: 16,
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(config.clone(), metadata_store, provider));
        let flat_file = flat_file_store.get_or_create("TopicA".to_owned(), 0)?;

        for (queue_offset, timestamp) in [(3, 100), (4, 200), (5, 300)] {
            let message = message_with_store_timestamp(timestamp);
            let commit_log_offset = flat_file.append_commit_log(message, timestamp).await?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: commit_log_offset as i64,
                        size: 64,
                        tags_code: 1,
                    },
                    timestamp,
                )
                .await?;
        }
        flat_file.commit().await?;

        Ok(DefaultTieredMessageFetcher::new(config, flat_file_store))
    }

    async fn build_query_fetcher() -> Result<DefaultTieredMessageFetcher<MemoryProvider>, RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(config.clone(), metadata_store, provider));
        let flat_file = flat_file_store.get_or_create("TopicA".to_owned(), 0)?;

        for (queue_offset, timestamp, body, keys, uniq_key) in [
            (3, 100, Bytes::from_static(b"first"), Some("keyA keyB"), Some("uniqA")),
            (4, 200, Bytes::from_static(b"second"), Some("keyA"), Some("uniqB")),
            (5, 300, Bytes::from_static(b"third"), Some("keyC"), None),
        ] {
            let commit_log_offset = flat_file.append_commit_log(body.clone(), timestamp).await?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: commit_log_offset as i64,
                        size: body.len() as i32,
                        tags_code: 1,
                    },
                    timestamp,
                )
                .await?;
            flat_file.commit().await?;
            flat_file_store
                .append_index(
                    &TieredDispatchRequest {
                        topic: "TopicA".to_owned(),
                        queue_id: 0,
                        queue_offset,
                        commit_log_offset: 0,
                        message_size: body.len() as i32,
                        tags_code: 1,
                        store_timestamp: timestamp,
                        keys: keys.map(str::to_owned),
                        uniq_key: uniq_key.map(str::to_owned),
                        offset_id: None,
                        sys_flag: 0,
                        body: Some(body),
                    },
                    commit_log_offset,
                )
                .await?;
        }

        Ok(DefaultTieredMessageFetcher::new(config, flat_file_store))
    }

    fn message_with_store_timestamp(timestamp: i64) -> Bytes {
        let mut bytes = BytesMut::zeroed(64);
        bytes[56..64].copy_from_slice(&timestamp.to_be_bytes());
        bytes.freeze()
    }

    #[tokio::test]
    async fn fetches_messages_by_queue_offset() -> Result<(), RocketMQError> {
        let fetcher = build_fetcher().await?;

        let result = fetcher.get_message("TopicA".to_owned(), 0, 3, 2).await?;

        assert_eq!(result.status, TieredGetMessageStatus::Found);
        assert_eq!(result.min_offset, 3);
        assert_eq!(result.max_offset, 5);
        assert_eq!(result.next_begin_offset, 5);
        assert_eq!(
            result.messages,
            vec![Bytes::from_static(b"msg-a"), Bytes::from_static(b"msg-b")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn returns_java_aligned_offset_boundaries() -> Result<(), RocketMQError> {
        let fetcher = build_fetcher().await?;

        let too_small = fetcher.get_message("TopicA".to_owned(), 0, 2, 1).await?;
        assert_eq!(too_small.status, TieredGetMessageStatus::OffsetTooSmall);
        assert_eq!(too_small.next_begin_offset, 3);

        let overflow_one = fetcher.get_message("TopicA".to_owned(), 0, 5, 1).await?;
        assert_eq!(overflow_one.status, TieredGetMessageStatus::OffsetOverflowOne);

        let overflow_badly = fetcher.get_message("TopicA".to_owned(), 0, 6, 1).await?;
        assert_eq!(overflow_badly.status, TieredGetMessageStatus::OffsetOverflowBadly);
        Ok(())
    }

    #[tokio::test]
    async fn missing_flat_file_returns_no_matched_logic_queue() -> Result<(), RocketMQError> {
        let fetcher = build_fetcher().await?;

        let result = fetcher.get_message("MissingTopic".to_owned(), 0, 0, 1).await?;

        assert_eq!(result.status, TieredGetMessageStatus::NoMatchedLogicQueue);
        Ok(())
    }

    #[tokio::test]
    async fn reads_message_store_timestamp_from_commit_log() -> Result<(), RocketMQError> {
        let fetcher = build_timestamp_fetcher().await?;

        assert_eq!(fetcher.get_message_timestamp("TopicA".to_owned(), 0, 4).await?, 200);
        assert_eq!(
            fetcher.get_message_timestamp("MissingTopic".to_owned(), 0, 4).await?,
            -1
        );
        Ok(())
    }

    #[tokio::test]
    async fn finds_lower_bound_offset_by_store_timestamp() -> Result<(), RocketMQError> {
        let fetcher = build_timestamp_fetcher().await?;

        assert_eq!(fetcher.get_offset_by_time("TopicA".to_owned(), 0, 50).await?, 3);
        assert_eq!(fetcher.get_offset_by_time("TopicA".to_owned(), 0, 100).await?, 3);
        assert_eq!(fetcher.get_offset_by_time("TopicA".to_owned(), 0, 150).await?, 4);
        assert_eq!(fetcher.get_offset_by_time("TopicA".to_owned(), 0, 300).await?, 5);
        assert_eq!(fetcher.get_offset_by_time("TopicA".to_owned(), 0, 301).await?, 6);
        assert_eq!(fetcher.get_offset_by_time("MissingTopic".to_owned(), 0, 100).await?, -1);
        Ok(())
    }

    #[tokio::test]
    async fn finds_upper_bound_offset_by_store_timestamp() -> Result<(), RocketMQError> {
        let fetcher = build_timestamp_fetcher().await?;

        assert_eq!(
            fetcher
                .get_offset_by_time_with_boundary("TopicA".to_owned(), 0, 50, BoundaryType::Upper)
                .await?,
            3
        );
        assert_eq!(
            fetcher
                .get_offset_by_time_with_boundary("TopicA".to_owned(), 0, 100, BoundaryType::Upper)
                .await?,
            3
        );
        assert_eq!(
            fetcher
                .get_offset_by_time_with_boundary("TopicA".to_owned(), 0, 150, BoundaryType::Upper)
                .await?,
            3
        );
        assert_eq!(
            fetcher
                .get_offset_by_time_with_boundary("TopicA".to_owned(), 0, 300, BoundaryType::Upper)
                .await?,
            5
        );
        assert_eq!(
            fetcher
                .get_offset_by_time_with_boundary("TopicA".to_owned(), 0, 301, BoundaryType::Upper)
                .await?,
            5
        );
        assert_eq!(
            fetcher
                .get_offset_by_time_with_boundary("MissingTopic".to_owned(), 0, 100, BoundaryType::Upper)
                .await?,
            -1
        );
        Ok(())
    }

    #[tokio::test]
    async fn queries_messages_by_key_and_time_range() -> Result<(), RocketMQError> {
        let fetcher = build_query_fetcher().await?;

        let result = fetcher
            .query_message("TopicA".to_owned(), "keyA".to_owned(), 10, 0, 250)
            .await?;

        assert_eq!(
            result.values,
            vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn queries_messages_by_uniq_key() -> Result<(), RocketMQError> {
        let fetcher = build_query_fetcher().await?;

        let result = fetcher
            .query_message("TopicA".to_owned(), "uniqB".to_owned(), 10, 0, 500)
            .await?;

        assert_eq!(result.values, vec![Bytes::from_static(b"second")]);
        Ok(())
    }

    #[tokio::test]
    async fn query_message_respects_max_num() -> Result<(), RocketMQError> {
        let fetcher = build_query_fetcher().await?;

        let result = fetcher
            .query_message("TopicA".to_owned(), "keyA".to_owned(), 1, 0, 500)
            .await?;

        assert_eq!(result.values, vec![Bytes::from_static(b"second")]);
        Ok(())
    }

    #[tokio::test]
    async fn query_message_recovers_index_and_segments_after_load() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            metadata_store.clone(),
            provider.clone(),
        ));
        let flat_file = flat_file_store.get_or_create("TopicA".to_owned(), 0)?;
        let message = Bytes::from_static(b"persisted");
        let commit_log_offset = flat_file.append_commit_log(message.clone(), 100).await?;
        flat_file
            .append_consume_queue(
                3,
                ConsumeQueueUnit {
                    commit_log_offset: commit_log_offset as i64,
                    size: message.len() as i32,
                    tags_code: 1,
                },
                100,
            )
            .await?;
        flat_file.commit().await?;
        flat_file_store
            .append_index(
                &TieredDispatchRequest {
                    topic: "TopicA".to_owned(),
                    queue_id: 0,
                    queue_offset: 3,
                    commit_log_offset: 0,
                    message_size: message.len() as i32,
                    tags_code: 1,
                    store_timestamp: 100,
                    keys: Some("keyA".to_owned()),
                    uniq_key: None,
                    offset_id: None,
                    sys_flag: 0,
                    body: Some(message.clone()),
                },
                commit_log_offset,
            )
            .await?;

        let reloaded_metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        reloaded_metadata_store.load().await?;
        let reloaded_flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            reloaded_metadata_store,
            provider,
        ));
        reloaded_flat_file_store.load().await?;
        let fetcher = DefaultTieredMessageFetcher::new(config, reloaded_flat_file_store);

        let result = fetcher
            .query_message("TopicA".to_owned(), "keyA".to_owned(), 1, 0, 500)
            .await?;

        assert_eq!(result.values, vec![message]);
        Ok(())
    }
}
