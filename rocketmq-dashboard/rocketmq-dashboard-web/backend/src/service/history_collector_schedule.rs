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

use tokio::time::Duration;

/// Fixed ticker periods and small mutable state for the history collector.
///
/// Keeping this separate from I/O lets the collector prioritize cancellation
/// and lease renewal while retention drains bounded batches cooperatively.
#[derive(Debug, Clone, Copy)]
pub(super) struct HistoryCollectorSchedule {
    renewal_period: Duration,
    collection_period: Duration,
    retention_period: Duration,
}

impl HistoryCollectorSchedule {
    pub(super) fn new(collection_interval_secs: u64, lease_ttl_ms: i64) -> Self {
        let renewal_ms = u64::try_from((lease_ttl_ms / 3).max(1)).unwrap_or(u64::MAX);
        Self {
            renewal_period: Duration::from_millis(renewal_ms),
            collection_period: Duration::from_secs(collection_interval_secs),
            retention_period: Duration::from_secs(collection_interval_secs.saturating_mul(10).max(1)),
        }
    }

    pub(super) const fn renewal_period(self) -> Duration {
        self.renewal_period
    }

    pub(super) const fn collection_period(self) -> Duration {
        self.collection_period
    }

    pub(super) const fn retention_period(self) -> Duration {
        self.retention_period
    }
}

#[derive(Debug, Default)]
pub(super) struct HistoryCollectorState {
    leader: bool,
    retention_pending: bool,
}

impl HistoryCollectorState {
    pub(super) fn became_leader(&mut self) {
        self.leader = true;
    }

    pub(super) fn lost_lease(&mut self) {
        self.leader = false;
        self.retention_pending = false;
    }

    pub(super) const fn is_leader(&self) -> bool {
        self.leader
    }

    pub(super) const fn can_collect(&self) -> bool {
        self.leader
    }

    pub(super) fn retention_due(&mut self) {
        if self.leader {
            self.retention_pending = true;
        }
    }

    pub(super) const fn can_retain(&self) -> bool {
        self.leader && self.retention_pending
    }

    pub(super) fn completed_retention_batch(&mut self, has_more: bool) {
        self.retention_pending = self.leader && has_more;
    }

    /// Cancellation stops new work before moving the lease out for its
    /// conditional holder/token release. A standby that never acquired a
    /// lease therefore never attempts a release write.
    pub(super) fn cancel<T>(&mut self, lease: &mut Option<T>) -> Option<T> {
        self.lost_lease();
        lease.take()
    }
}

#[cfg(test)]
mod tests {
    use super::HistoryCollectorSchedule;
    use super::HistoryCollectorState;
    use std::future::poll_fn;
    use std::task::Poll;
    use tokio::time::Duration;
    use tokio::time::Instant;
    use tokio::time::interval_at;

    #[tokio::test(start_paused = true)]
    async fn one_second_collection_tick_is_independent_of_lease_renewal() {
        let schedule = HistoryCollectorSchedule::new(1, 30_000);
        assert_eq!(schedule.collection_period(), Duration::from_secs(1));
        assert_eq!(schedule.renewal_period(), Duration::from_secs(10));

        let start = Instant::now();
        let mut collection = interval_at(start + schedule.collection_period(), schedule.collection_period());
        let mut renewal = interval_at(start + schedule.renewal_period(), schedule.renewal_period());
        tokio::time::advance(Duration::from_secs(1)).await;
        collection.tick().await;
        let mut renewal_tick = std::pin::pin!(renewal.tick());
        let renewal_is_ready = poll_fn(|context| match renewal_tick.as_mut().poll(context) {
            Poll::Ready(_) => Poll::Ready(true),
            Poll::Pending => Poll::Ready(false),
        })
        .await;
        assert!(
            !renewal_is_ready,
            "renewal must not gate the one-second collection tick"
        );
    }

    #[test]
    fn retention_drains_one_bounded_batch_at_a_time_until_converged() {
        let mut state = HistoryCollectorState::default();
        state.became_leader();
        state.retention_due();
        assert!(state.can_retain());
        state.completed_retention_batch(true);
        assert!(state.can_retain(), "has_more schedules the next yielded pass");
        state.completed_retention_batch(false);
        assert!(!state.can_retain());
    }

    #[test]
    fn lease_loss_cancels_collection_and_pending_retention() {
        let mut state = HistoryCollectorState::default();
        state.became_leader();
        state.retention_due();
        state.lost_lease();
        assert!(!state.is_leader());
        assert!(!state.can_collect());
        assert!(!state.can_retain());
    }

    #[test]
    fn cancellation_releases_only_an_acquired_lease() {
        let mut leader = HistoryCollectorState::default();
        leader.became_leader();
        let mut acquired = Some("holder-token");
        assert_eq!(leader.cancel(&mut acquired), Some("holder-token"));
        assert!(acquired.is_none());
        assert!(!leader.can_collect());

        let mut standby = HistoryCollectorState::default();
        let mut no_lease = None::<()>;
        assert!(standby.cancel(&mut no_lease).is_none());
    }
}
