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

use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;

use crate::SessionConfig;

#[derive(Clone, Debug)]
pub struct GrpcHousekeepingRunReport {
    pub schedule_snapshot: Vec<ScheduledTaskSnapshot>,
    pub shutdown_report: ShutdownReport,
}

/// Lock-free deadline used to opportunistically reap session state on ingress.
#[derive(Clone)]
pub struct ReapSchedule {
    next_reap_at_ms: Arc<AtomicU64>,
}

impl ReapSchedule {
    pub fn new(interval: Duration) -> Self {
        Self {
            next_reap_at_ms: Arc::new(AtomicU64::new(
                current_epoch_millis().saturating_add(duration_millis(interval)),
            )),
        }
    }

    /// Returns `true` to the single caller that wins the current reap deadline.
    pub fn claim_if_due(&self, interval: Duration) -> bool {
        let now = current_epoch_millis();
        let next = self.next_reap_at_ms.load(Ordering::Relaxed);
        if now < next {
            return false;
        }
        self.next_reap_at_ms
            .compare_exchange(
                next,
                now.saturating_add(duration_millis(interval)),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    pub fn schedule_next(&self, interval: Duration) {
        self.next_reap_at_ms.store(
            current_epoch_millis().saturating_add(duration_millis(interval)),
            Ordering::Relaxed,
        );
    }
}

pub fn housekeeping_interval(config: &SessionConfig) -> Duration {
    let min_ttl = config.client_ttl().min(config.receipt_handle_ttl());
    Duration::from_millis(min_ttl.as_millis().saturating_div(2).clamp(1_000, 30_000) as u64)
}

pub async fn run_housekeeping_until<F, C, CFut>(
    interval: Duration,
    shutdown: F,
    task_group: TaskGroup,
    run_once: C,
) -> GrpcHousekeepingRunReport
where
    F: Future<Output = ()> + Send,
    C: Fn() -> CFut + Clone + Send + Sync + 'static,
    CFut: Future<Output = ()> + Send + 'static,
{
    let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
    let schedule_result = scheduled_tasks.schedule_fixed_rate_no_overlap(
        ScheduledTaskConfig::fixed_rate_no_overlap("proxy.grpc.housekeeping", interval),
        run_once,
    );
    if schedule_result.is_err() {
        // The report exposes the absent schedule; the facade may attach its
        // provider-specific logging or metrics without Core knowing about it.
    }

    shutdown.await;
    let schedule_snapshot = scheduled_tasks.snapshot();
    let shutdown_report = task_group.shutdown(Duration::from_secs(5)).await;
    GrpcHousekeepingRunReport {
        schedule_snapshot,
        shutdown_report,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64
}

fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn housekeeping_interval_is_bounded() {
        assert_eq!(
            housekeeping_interval(&SessionConfig {
                client_ttl_ms: 1,
                receipt_handle_ttl_ms: 1,
                ..SessionConfig::default()
            }),
            Duration::from_secs(1)
        );
        assert_eq!(
            housekeeping_interval(&SessionConfig {
                client_ttl_ms: 120_000,
                receipt_handle_ttl_ms: 120_000,
                ..SessionConfig::default()
            }),
            Duration::from_secs(30)
        );
    }
}
