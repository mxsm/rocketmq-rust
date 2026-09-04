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

use rocketmq_store_api::StoreError;
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
    fn validate(self) -> Result<Self, StoreError> {
        if self.queue_capacity == 0 {
            return Err(crate::error::request_invalid(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        if self.batch_size == 0 {
            return Err(crate::error::request_invalid(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        Ok(self)
    }
}

pub struct RocksDbConsumeQueueGroupCommitService {
    sender: mpsc::Sender<ConsumeQueueBatchWriteRequest>,
    task_group: rocketmq_runtime::TaskGroup,
    task_error: GroupCommitTaskErrorSlot,
}

type GroupCommitTaskErrorSlot = Arc<tokio::sync::Mutex<Option<StoreError>>>;

impl RocksDbConsumeQueueGroupCommitService {
    pub fn start(
        store: Arc<RocksDbStore>,
        config: RocksDbConsumeQueueGroupCommitConfig,
        runtime_scope: crate::runtime::RocksDbRuntimeScope,
    ) -> Result<Self, StoreError> {
        let config = config.validate()?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let task_group = crate::runtime::task_group(&runtime_scope, "rocksdb.consume_queue.group_commit");
        let task_error = Arc::new(tokio::sync::Mutex::new(None));
        let task_error_clone = task_error.clone();
        task_group
            .spawn_service("consume-queue-group-commit", async move {
                if let Err(error) = run_group_commit_loop(store, receiver, config.batch_size, runtime_scope).await {
                    *task_error_clone.lock().await = Some(error);
                }
            })
            .map_err(|source| crate::error::runtime_error(rocketmq_store_api::StoreOperation::Start, source))?;
        Ok(Self {
            sender,
            task_group,
            task_error,
        })
    }

    pub async fn submit(&self, request: ConsumeQueueBatchWriteRequest) -> Result<(), StoreError> {
        self.sender
            .send(request)
            .await
            .map_err(|_| crate::error::unavailable(rocketmq_store_api::StoreOperation::AppendDerived))
    }

    pub async fn shutdown(self) -> Result<(), StoreError> {
        let Self {
            sender,
            task_group,
            task_error,
        } = self;
        drop(sender);
        let report = task_group.shutdown(std::time::Duration::from_secs(5)).await;
        crate::runtime::shutdown_report_result(
            "consume queue group commit shutdown",
            rocketmq_store_api::StoreOperation::Shutdown,
            report,
        )?;
        let task_error = task_error.lock().await.take();
        group_commit_worker_result(task_error)?;
        Ok(())
    }
}

fn group_commit_worker_result(error: Option<StoreError>) -> Result<(), StoreError> {
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn run_group_commit_loop(
    store: Arc<RocksDbStore>,
    mut receiver: mpsc::Receiver<ConsumeQueueBatchWriteRequest>,
    batch_size: usize,
    runtime_scope: crate::runtime::RocksDbRuntimeScope,
) -> Result<(), StoreError> {
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
        crate::runtime::spawn_io(
            &runtime_scope,
            "rocksdb.consume_queue.group_commit",
            rocketmq_store_api::StoreOperation::AppendDerived,
            move || {
                let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
                writer.write(&request)
            },
        )
        .await??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_commit_worker_result_preserves_original_error_kind() {
        let error = group_commit_worker_result(Some(crate::error::request_invalid(
            rocketmq_store_api::StoreOperation::AppendDerived,
        )))
        .expect_err("group commit worker error should be propagated");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
        assert_eq!(error.operation(), rocketmq_store_api::StoreOperation::AppendDerived);
    }
}
