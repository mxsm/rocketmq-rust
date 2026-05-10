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
use tokio::time::interval;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;

pub struct CleanupService<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    shutdown: CancellationToken,
}

impl<P> CleanupService<P>
where
    P: TieredStoreProvider,
{
    pub fn new(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            flat_file_store,
            shutdown,
        }
    }

    pub async fn run(self) -> Result<(), RocketMQError> {
        let mut ticker = interval(self.config.delete_file_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    break;
                }
                _ = ticker.tick() => {
                    self.flat_file_store.cleanup_expired(current_time_millis()).await?;
                }
            }
        }
        Ok(())
    }
}

fn current_time_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;
    use tokio::time::sleep;

    use super::*;
    use crate::file::ConsumeQueueUnit;
    use crate::file::CONSUME_QUEUE_UNIT_SIZE;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;

    #[tokio::test]
    async fn cleanup_service_runs_periodically_and_stops_on_shutdown() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 8,
            consume_queue_segment_size: CONSUME_QUEUE_UNIT_SIZE as u64,
            file_reserved_time: Duration::from_millis(1),
            delete_file_interval: Duration::from_millis(10),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            metadata_store,
            provider.clone(),
        ));
        let flat_file = flat_file_store.get_or_create("CleanupTopic".to_owned(), 0)?;

        for (queue_offset, body, timestamp) in [
            (0, Bytes::from_static(b"old-a"), 10),
            (1, Bytes::from_static(b"old-b"), 11),
            (2, Bytes::from_static(b"new-c"), current_time_millis()),
        ] {
            let commit_log_offset = flat_file.append_commit_log(body.clone(), timestamp).await?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: commit_log_offset as i64,
                        size: body.len() as i32,
                        tags_code: 0,
                    },
                    timestamp,
                )
                .await?;
        }
        flat_file.commit().await?;

        let first_commit_log_path = "CleanupTopic/0/commitlog/00000000000000000000".to_owned();
        assert_eq!(provider.segment_size(first_commit_log_path.clone()).await?, 5);

        let shutdown = CancellationToken::new();
        let service = CleanupService::new(config, flat_file_store, shutdown.clone());
        let handle = tokio::spawn(service.run());

        for _ in 0..50 {
            if provider.segment_size(first_commit_log_path.clone()).await? == 0 {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        shutdown.cancel();
        handle.await.map_err(|err| RocketMQError::Internal(err.to_string()))??;

        assert_eq!(provider.segment_size(first_commit_log_path).await?, 0);
        Ok(())
    }
}
