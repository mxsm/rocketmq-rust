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
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;

use crate::kv::compaction_store::CompactionStore;

pub struct CompactionService {
    compaction_store: Arc<CompactionStore>,
    schedule_interval: Duration,
    shutdown_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
    loaded: bool,
}

impl CompactionService {
    pub fn new(compaction_store: Arc<CompactionStore>, schedule_interval_ms: usize) -> Self {
        Self {
            compaction_store,
            schedule_interval: Duration::from_millis(schedule_interval_ms.max(1) as u64),
            shutdown_token: CancellationToken::new(),
            worker_handle: None,
            loaded: false,
        }
    }

    pub fn load(&mut self, exit_ok: bool) -> bool {
        self.loaded = true;
        info!("load compaction service, exit ok: {}", exit_ok);
        true
    }

    pub fn start(&mut self) {
        if self.worker_handle.is_some() {
            return;
        }

        self.shutdown_token = CancellationToken::new();
        let shutdown = self.shutdown_token.clone();
        let compaction_store = self.compaction_store.clone();
        let schedule_interval = self.schedule_interval;

        self.worker_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(schedule_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    _ = interval.tick() => {
                        let removed = compaction_store.compact_once();
                        if removed > 0 {
                            info!("compaction service removed {} obsolete messages", removed);
                        }
                    }
                }
            }
        }));
    }

    pub async fn shutdown_gracefully(&mut self) {
        self.shutdown_token.cancel();
        if let Some(worker_handle) = self.worker_handle.take() {
            if let Err(error) = worker_handle.await {
                error!("compaction service task failed during shutdown: {error}");
            }
        }
        self.loaded = false;
        info!("shutdown compaction service");
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded
    }

    pub fn has_worker_handle(&self) -> bool {
        self.worker_handle.is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use cheetah_string::CheetahString;

    use super::CompactionService;
    use crate::kv::compaction_store::CompactionStore;

    #[tokio::test]
    async fn start_compacts_and_shutdown_clears_worker_handle() {
        let compaction_store = Arc::new(CompactionStore::new());
        let topic = CheetahString::from_static_str("compaction-service-topic");
        let key = CheetahString::from_static_str("same-key");
        compaction_store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"old-message"));
        compaction_store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::from_static(b"latest-message"));

        let mut service = CompactionService::new(compaction_store.clone(), 1);
        assert!(service.load(true));
        service.start();
        assert!(service.has_worker_handle());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while compaction_store.message_count(&topic, 0) != 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "compaction service did not compact duplicate keys in time"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        service.shutdown_gracefully().await;
        assert!(!service.has_worker_handle());
        assert!(!service.is_loaded());
    }
}
