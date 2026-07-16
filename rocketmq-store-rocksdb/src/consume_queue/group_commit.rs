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

use super::ConsumeQueueBatchWriteRequest;
use super::RocksDbConsumeQueueBatchWriter;
use crate::store::RocksDbStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbConsumeQueueGroupCommitConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
}

impl Default for RocksDbConsumeQueueGroupCommitConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100_000,
            batch_size: 256,
        }
    }
}

impl RocksDbConsumeQueueGroupCommitConfig {
    fn validate(self) -> Result<Self, RocketMQError> {
        if self.queue_capacity == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.consume_queue.group_commit.queue_capacity",
                value: self.queue_capacity.to_string(),
                reason: "queue capacity must be greater than zero".to_string(),
            });
        }
        if self.batch_size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.consume_queue.group_commit.batch_size",
                value: self.batch_size.to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        Ok(self)
    }
}

pub struct RocksDbConsumeQueueGroupCommitService {
    sender: mpsc::Sender<ConsumeQueueBatchWriteRequest>,
    task_group: rocketmq_runtime::TaskGroup,
    task_error: GroupCommitTaskErrorSlot,
}

type GroupCommitTaskErrorSlot = Arc<tokio::sync::Mutex<Option<RocketMQError>>>;

impl RocksDbConsumeQueueGroupCommitService {
    pub fn start(
        store: Arc<RocksDbStore>,
        config: RocksDbConsumeQueueGroupCommitConfig,
    ) -> Result<Self, RocketMQError> {
        let config = config.validate()?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let task_group = crate::runtime::task_group("rocksdb.consume_queue.group_commit")?;
        let task_error = Arc::new(tokio::sync::Mutex::new(None));
        let task_error_clone = task_error.clone();
        task_group
            .spawn_service("consume-queue-group-commit", async move {
                if let Err(error) = run_group_commit_loop(store, receiver, config.batch_size).await {
                    *task_error_clone.lock().await = Some(error);
                }
            })
            .map_err(|error| RocketMQError::storage_write_failed("rocksdb", error.to_string()))?;
        Ok(Self {
            sender,
            task_group,
            task_error,
        })
    }

    pub async fn submit(&self, request: ConsumeQueueBatchWriteRequest) -> Result<(), RocketMQError> {
        self.sender.send(request).await.map_err(|error| {
            RocketMQError::storage_write_failed("rocksdb", format!("group commit queue closed: {error}"))
        })
    }

    pub async fn shutdown(self) -> Result<(), RocketMQError> {
        let Self {
            sender,
            task_group,
            task_error,
        } = self;
        drop(sender);
        let report = task_group.shutdown(std::time::Duration::from_secs(5)).await;
        crate::runtime::shutdown_report_result("consume queue group commit shutdown", report)?;
        let task_error = task_error.lock().await.take();
        group_commit_worker_result(task_error)?;
        Ok(())
    }
}

fn group_commit_worker_result(error: Option<RocketMQError>) -> Result<(), RocketMQError> {
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn run_group_commit_loop(
    store: Arc<RocksDbStore>,
    mut receiver: mpsc::Receiver<ConsumeQueueBatchWriteRequest>,
    batch_size: usize,
) -> Result<(), RocketMQError> {
    while let Some(first_request) = receiver.recv().await {
        let mut requests = Vec::with_capacity(batch_size);
        requests.push(first_request);

        while requests.len() < batch_size {
            match receiver.try_recv() {
                Ok(request) => requests.push(request),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let request = ConsumeQueueBatchWriteRequest::merge(requests);
        if request.is_empty() {
            continue;
        }

        let store = Arc::clone(&store);
        crate::runtime::spawn_io("rocksdb.consume_queue.group_commit", move || {
            let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
            writer.write(&request)
        })
        .await??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;

    use super::*;

    #[test]
    fn group_commit_worker_result_preserves_original_error_kind() {
        let error = group_commit_worker_result(Some(RocketMQError::ConfigInvalidValue {
            key: "rocksdb.consume_queue.group_commit.batch_size",
            value: "0".to_string(),
            reason: "batch size must be greater than zero".to_string(),
        }))
        .expect_err("group commit worker error should be propagated");

        assert_eq!(error.kind(), ErrorKind::ConfigInvalidValue);
        assert!(error.to_string().contains("batch size must be greater than zero"));
    }
}
