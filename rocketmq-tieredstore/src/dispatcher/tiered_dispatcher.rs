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
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::dispatcher::TieredDispatchRequest;
use crate::file::ConsumeQueueUnit;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;

#[allow(async_fn_in_trait)]
pub trait TieredDispatcher: Send + Sync {
    async fn dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError>;

    async fn start(&self) -> Result<(), RocketMQError>;

    async fn shutdown(&self) -> Result<(), RocketMQError>;
}

pub struct DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    sender: mpsc::Sender<TieredDispatchRequest>,
    receiver: tokio::sync::Mutex<Option<mpsc::Receiver<TieredDispatchRequest>>>,
    permits: Arc<Semaphore>,
    shutdown: CancellationToken,
    handle: tokio::sync::Mutex<Option<JoinHandle<Result<(), RocketMQError>>>>,
}

impl<P> DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    pub fn new(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.max_pending_tasks);
        let permits = Arc::new(Semaphore::new((config.max_pending_tasks / 4).max(1)));
        Self {
            config,
            flat_file_store,
            sender,
            receiver: tokio::sync::Mutex::new(Some(receiver)),
            permits,
            shutdown,
            handle: tokio::sync::Mutex::new(None),
        }
    }

    pub fn try_dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        self.sender
            .try_send(request)
            .map_err(|err| RocketMQError::storage_write_failed("tiered_dispatch_queue", err.to_string()))
    }

    async fn run(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        mut receiver: mpsc::Receiver<TieredDispatchRequest>,
        permits: Arc<Semaphore>,
        shutdown: CancellationToken,
    ) -> Result<(), RocketMQError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    while let Ok(request) = receiver.try_recv() {
                        Self::dispatch_one(flat_file_store.clone(), permits.clone(), request).await?;
                    }
                    break;
                }
                maybe_request = receiver.recv() => {
                    let Some(request) = maybe_request else {
                        break;
                    };
                    Self::dispatch_one(flat_file_store.clone(), permits.clone(), request).await?;
                }
            }
        }
        Ok(())
    }

    async fn dispatch_one(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        permits: Arc<Semaphore>,
        request: TieredDispatchRequest,
    ) -> Result<(), RocketMQError> {
        let _permit = permits
            .acquire_owned()
            .await
            .map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let file = flat_file_store.get_or_create(request.topic.clone(), request.queue_id)?;
        let consume_queue_min_offset = file.consume_queue_min_offset();
        let consume_queue_commit_offset = file.consume_queue_commit_offset();
        if consume_queue_commit_offset > consume_queue_min_offset {
            if request.queue_offset < consume_queue_commit_offset {
                return Ok(());
            }
            if request.queue_offset > consume_queue_commit_offset {
                return Err(RocketMQError::illegal_argument(format!(
                    "tiered consume queue offset gap, expected {consume_queue_commit_offset}, got {}",
                    request.queue_offset
                )));
            }
        }

        let message = request.body.clone().unwrap_or_default();
        let tiered_offset = file.append_commit_log(message, request.store_timestamp).await?;
        file.append_consume_queue(
            request.queue_offset,
            ConsumeQueueUnit {
                commit_log_offset: tiered_offset as i64,
                size: request.message_size,
                tags_code: request.tags_code,
            },
            request.store_timestamp,
        )
        .await?;
        file.commit().await?;
        flat_file_store.append_index(&request, tiered_offset).await
    }
}

impl<P> TieredDispatcher for DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    async fn dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        self.sender
            .send(request)
            .await
            .map_err(|err| RocketMQError::storage_write_failed("tiered_dispatch_queue", err.to_string()))
    }

    async fn start(&self) -> Result<(), RocketMQError> {
        let mut receiver_guard = self.receiver.lock().await;
        let Some(receiver) = receiver_guard.take() else {
            return Ok(());
        };
        let handle = tokio::spawn(Self::run(
            self.flat_file_store.clone(),
            receiver,
            self.permits.clone(),
            self.shutdown.clone(),
        ));
        *self.handle.lock().await = Some(handle);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RocketMQError> {
        self.shutdown.cancel();
        if let Some(handle) = self.handle.lock().await.take() {
            handle.await.map_err(|err| RocketMQError::Internal(err.to_string()))??;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::config::TieredStoreConfig;
    use crate::file::TieredFlatFileStore;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;

    #[tokio::test]
    async fn dispatch_writes_commit_log_and_consume_queue_unit() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            max_pending_tasks: 4,
            ..TieredStoreConfig::default()
        });
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            Arc::new(JsonMetadataStore::new(config.clone())),
            MemoryProvider::default(),
        ));
        let dispatcher = DefaultTieredDispatcher::new(config, flat_file_store.clone(), CancellationToken::new());

        dispatcher.start().await?;
        dispatcher
            .dispatch(TieredDispatchRequest {
                topic: "TopicA".to_owned(),
                queue_id: 0,
                queue_offset: 3,
                commit_log_offset: 1024,
                message_size: 4,
                tags_code: 7,
                store_timestamp: 100,
                keys: None,
                uniq_key: None,
                offset_id: None,
                sys_flag: 0,
                body: Some(Bytes::from_static(b"body")),
            })
            .await?;
        dispatcher.shutdown().await?;

        let flat_file = flat_file_store
            .get("TopicA", 0)
            .ok_or_else(|| RocketMQError::Internal("missing dispatched flat file".to_owned()))?;
        let cq_unit = flat_file
            .read_consume_queue_unit(3)
            .await?
            .ok_or_else(|| RocketMQError::Internal("missing dispatched consume queue unit".to_owned()))?;

        assert_eq!(cq_unit.commit_log_offset, 0);
        assert_eq!(cq_unit.size, 4);
        assert_eq!(cq_unit.tags_code, 7);
        assert_eq!(
            flat_file.read_message_by_queue_offset(3).await?,
            Some(Bytes::from_static(b"body"))
        );
        Ok(())
    }
}
