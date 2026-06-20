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

use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;

use crate::kv::compaction_store::CompactionStore;

pub struct CompactionService {
    compaction_store: Arc<CompactionStore>,
    schedule_interval: Duration,
    shutdown_token: CancellationToken,
    worker_group: Option<rocketmq_runtime::TaskGroup>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
    loaded: bool,
}

impl CompactionService {
    pub fn new(compaction_store: Arc<CompactionStore>, schedule_interval_ms: usize) -> Self {
        Self {
            compaction_store,
            schedule_interval: Duration::from_millis(schedule_interval_ms.max(1) as u64),
            shutdown_token: CancellationToken::new(),
            worker_group: None,
            scheduled_tasks: None,
            loaded: false,
        }
    }

    pub fn load(&mut self, exit_ok: bool) -> bool {
        self.loaded = true;
        info!("load compaction service, exit ok: {}", exit_ok);
        true
    }

    pub fn start(&mut self) {
        if self.worker_group.is_some() {
            return;
        }

        self.shutdown_token = CancellationToken::new();
        let compaction_store = self.compaction_store.clone();
        let schedule_interval = self.schedule_interval;
        let worker_group = match crate::runtime::task_group("rocketmq-store.kv.compaction") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                error!("failed to create compaction service task group: {error}");
                return;
            }
        };
        let scheduled_tasks = ScheduledTaskGroup::new(worker_group.child("scheduled"));
        let mut config = ScheduledTaskConfig::fixed_rate_no_overlap("store.kv.compaction", schedule_interval);
        config.initial_delay = schedule_interval;

        if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap(config, move || {
            let compaction_store = compaction_store.clone();
            async move {
                let removed = compaction_store.compact_once();
                if removed > 0 {
                    info!("compaction service removed {} obsolete messages", removed);
                }
            }
        }) {
            error!("failed to spawn compaction service task: {error}");
            return;
        }

        self.scheduled_tasks = Some(scheduled_tasks);
        self.worker_group = Some(worker_group);
    }

    pub async fn shutdown_gracefully(&mut self) {
        let _ = self.shutdown_with_report().await;
        info!("shutdown compaction service");
    }

    pub async fn shutdown_with_report(&mut self) -> Option<ShutdownReport> {
        self.shutdown_token.cancel();
        self.scheduled_tasks.take();
        let mut shutdown_report = None;
        if let Some(worker_group) = self.worker_group.take() {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("CompactionService", report.clone()) {
                error!("compaction service task failed during shutdown: {error}");
            }
            shutdown_report = Some(report);
        }
        self.loaded = false;
        shutdown_report
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded
    }

    pub fn has_worker_handle(&self) -> bool {
        self.worker_group.is_some()
    }

    pub fn task_count(&self) -> usize {
        let root_count = self
            .worker_group
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
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
