// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

pub(in crate::message_store::local_file_message_store) struct FlushConsumeQueueService {
    runtime_scope: StoreRuntimeScope,
    message_store_config: Arc<MessageStoreConfig>,
    consume_queue_store: ConsumeQueueStore,
    store_checkpoint: Arc<StoreCheckpoint>,
    worker_group: parking_lot::Mutex<Option<rocketmq_runtime::TaskGroup>>,
    shutdown_token: parking_lot::Mutex<CancellationToken>,
    wakeup: Arc<Notify>,
}

impl FlushConsumeQueueService {
    pub(in crate::message_store::local_file_message_store) fn new(
        runtime_scope: StoreRuntimeScope,
        message_store_config: Arc<MessageStoreConfig>,
        consume_queue_store: ConsumeQueueStore,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        Self {
            runtime_scope,
            message_store_config,
            consume_queue_store,
            store_checkpoint,
            worker_group: parking_lot::Mutex::new(None),
            shutdown_token: parking_lot::Mutex::new(CancellationToken::new()),
            wakeup: Arc::new(Notify::new()),
        }
    }

    pub(in crate::message_store::local_file_message_store) fn flush_once_blocking(
        consume_queue_store: &ConsumeQueueStore,
        store_checkpoint: &StoreCheckpoint,
        flush_least_pages: i32,
    ) {
        let consume_queue_table = consume_queue_store.get_consume_queue_table().lock().clone();
        for consume_queue_table in consume_queue_table.values() {
            for consume_queue in consume_queue_table.values() {
                let consume_queue = consume_queue.read();
                let _ = consume_queue_store.flush(consume_queue.as_ref(), flush_least_pages);
            }
        }

        if let Err(error) = store_checkpoint.flush() {
            error!("flush consume queue service failed to flush store checkpoint: {error}");
        }
    }

    pub(super) async fn flush_once(
        runtime_scope: StoreRuntimeScope,
        consume_queue_store: ConsumeQueueStore,
        store_checkpoint: Arc<StoreCheckpoint>,
        flush_least_pages: i32,
    ) {
        if let Err(error) = crate::runtime::spawn_io(&runtime_scope, "flush-consume-queue", move || {
            Self::flush_once_blocking(&consume_queue_store, &store_checkpoint, flush_least_pages);
        })
        .await
        {
            error!("flush consume queue service task failed: {error}");
        }
    }

    pub(in crate::message_store::local_file_message_store) fn start(&self) {
        let mut worker_group = self.worker_group.lock();
        if worker_group.is_some() {
            return;
        }

        let group = crate::runtime::task_group(&self.runtime_scope, "rocketmq-store.flush-consume-queue");

        let message_store_config = self.message_store_config.clone();
        let consume_queue_store = self.consume_queue_store.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let runtime_scope = self.runtime_scope.clone();
        let shutdown_token = CancellationToken::new();
        *self.shutdown_token.lock() = shutdown_token.clone();
        let wakeup = self.wakeup.clone();

        match group.spawn_service("flush-consume-queue", async move {
            let interval = message_store_config.flush_interval_consume_queue.max(1) as u64;
            let thorough_interval = message_store_config.flush_consume_queue_thorough_interval as u64;
            let default_least_pages = message_store_config.flush_consume_queue_least_pages as i32;
            let mut last_thorough_flush_timestamp = current_millis();

            loop {
                let now = current_millis();
                let flush_least_pages =
                    if thorough_interval == 0 || now >= last_thorough_flush_timestamp + thorough_interval {
                        last_thorough_flush_timestamp = now;
                        0
                    } else {
                        default_least_pages
                    };

                Self::flush_once(
                    runtime_scope.clone(),
                    consume_queue_store.clone(),
                    store_checkpoint.clone(),
                    flush_least_pages,
                )
                .await;

                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = wakeup.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(interval)) => {}
                }
            }

            Self::flush_once(runtime_scope, consume_queue_store, store_checkpoint, 0).await;
        }) {
            Ok(_) => {
                *worker_group = Some(group);
            }
            Err(error) => {
                error!("failed to start flush consume queue service task: {error}");
            }
        }
    }

    pub(in crate::message_store::local_file_message_store) async fn shutdown(&self) {
        self.shutdown_token.lock().cancel();
        self.wakeup.notify_waiters();

        let worker_group = self.worker_group.lock().take();
        if let Some(worker_group) = worker_group {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("FlushConsumeQueueService", report) {
                error!("flush consume queue service task failed during shutdown: {error}");
            }
        }
    }
}
