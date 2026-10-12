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

//! Request budgets per principal, cluster and operation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use rocketmq_observability::metrics::mcp::McpMetricsRecorder;
use rocketmq_observability::metrics::mcp::McpRateLimitDecision;

use crate::guard::GuardRejection;
use crate::infrastructure::metrics::ComponentMetrics;

/// The time in which a budget refills completely, and the most a budget holds.
const REFILL_PERIOD: Duration = Duration::from_secs(60);
/// How often the budgets that are full again are dropped.
const SWEEP_INTERVAL: Duration = REFILL_PERIOD;
/// Budgets tracked at once. A caller decides part of every key, so the table has a bound: when
/// it is full and nothing is idle, a key that is not tracked yet is refused.
const MAX_TRACKED_BUDGETS: usize = 65_536;

#[derive(Debug, Clone, Default)]
pub(crate) struct RateLimiter {
    state: Arc<Mutex<LimiterState>>,
    telemetry: ComponentMetrics,
}

#[derive(Debug, Default)]
struct LimiterState {
    budgets: HashMap<String, TokenBucket>,
    swept_at: Option<Instant>,
}

/// A budget of `per_minute` requests that refills evenly and holds at most one minute of them.
///
/// The budget is kept as time: a request costs the share of a minute that one request is worth,
/// and elapsed time is credited back up to a full minute. Integer time adds up exactly, where a
/// fractional token count would drift and refuse a request that is due. Two values are all a key
/// needs, however many requests it has made.
#[derive(Debug)]
struct TokenBucket {
    credit: Duration,
    refilled_at: Instant,
}

impl TokenBucket {
    fn full(now: Instant) -> Self {
        Self {
            credit: REFILL_PERIOD,
            refilled_at: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        self.credit = self.credit.saturating_add(elapsed).min(REFILL_PERIOD);
        self.refilled_at = now;
    }

    fn is_full(&self) -> bool {
        self.credit >= REFILL_PERIOD
    }

    /// Takes one request out of the budget, or returns how long until one is due.
    fn take(&mut self, per_minute: u32) -> Result<(), Duration> {
        let cost = REFILL_PERIOD / per_minute.max(1);
        match self.credit.checked_sub(cost) {
            Some(remaining) => {
                self.credit = remaining;
                Ok(())
            }
            None => Err(cost - self.credit),
        }
    }
}

impl RateLimiter {
    /// Creates a limiter that reports every decision through `metrics`.
    pub(crate) fn with_metrics(metrics: McpMetricsRecorder) -> Self {
        Self {
            state: Arc::default(),
            telemetry: ComponentMetrics::new(metrics),
        }
    }

    /// Charges one request to the budget of `principal_id` for `operation` on `cluster`.
    ///
    /// A budget holds `limit_per_minute` requests and refills evenly, so a caller that used it up
    /// gets one request back every `60 / limit_per_minute` seconds instead of all of them a
    /// minute later.
    ///
    /// # Errors
    ///
    /// Returns [`GuardRejection::RateLimited`] with the time after which one request is due.
    pub(crate) fn check(
        &self,
        principal_id: &str,
        cluster: Option<&str>,
        operation: &str,
        limit_per_minute: u32,
    ) -> Result<(), GuardRejection> {
        self.check_at(principal_id, cluster, operation, limit_per_minute, Instant::now())
    }

    fn check_at(
        &self,
        principal_id: &str,
        cluster: Option<&str>,
        operation: &str,
        limit_per_minute: u32,
        now: Instant,
    ) -> Result<(), GuardRejection> {
        let decision = self.decide(principal_id, cluster, operation, limit_per_minute, now);
        self.telemetry.record_rate_limit(match decision {
            Ok(()) => McpRateLimitDecision::Accepted,
            Err(_) => McpRateLimitDecision::Rejected,
        });
        decision.map_err(|retry_after| GuardRejection::RateLimited {
            retry_after_ms: retry_after_millis(retry_after),
        })
    }

    fn decide(
        &self,
        principal_id: &str,
        cluster: Option<&str>,
        operation: &str,
        limit_per_minute: u32,
        now: Instant,
    ) -> Result<(), Duration> {
        if limit_per_minute == 0 {
            return Err(REFILL_PERIOD);
        }
        // The operation and the cluster are names of this server and come first; the principal
        // is the caller's and comes last, so no principal can spell another key.
        let key = format!("{operation}\u{1f}{}\u{1f}{principal_id}", cluster.unwrap_or_default());
        // A panic while the table is locked leaves it usable: every entry is two plain values.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let sweep_is_due = state
            .swept_at
            .is_none_or(|swept_at| now.saturating_duration_since(swept_at) >= SWEEP_INTERVAL);
        let table_is_full = state.budgets.len() >= MAX_TRACKED_BUDGETS && !state.budgets.contains_key(&key);
        if sweep_is_due || table_is_full {
            state.forget_idle(now);
            if state.budgets.len() >= MAX_TRACKED_BUDGETS && !state.budgets.contains_key(&key) {
                return Err(REFILL_PERIOD);
            }
        }
        let budget = state.budgets.entry(key).or_insert_with(|| TokenBucket::full(now));
        budget.refill(now);
        budget.take(limit_per_minute)
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.state.lock().unwrap().budgets.len()
    }
}

impl LimiterState {
    /// Drops every budget that is full again: forgetting it changes no decision.
    fn forget_idle(&mut self, now: Instant) {
        self.budgets.retain(|_, budget| {
            budget.refill(now);
            !budget.is_full()
        });
        self.swept_at = Some(now);
    }
}

/// The wait a caller is told, in whole milliseconds and never zero: a caller that waits this
/// long finds the request due.
fn retry_after_millis(retry_after: Duration) -> u64 {
    let millis = retry_after.as_nanos().div_ceil(1_000_000);
    u64::try_from(millis).unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry_after(decision: Result<(), GuardRejection>) -> u64 {
        match decision {
            Err(GuardRejection::RateLimited { retry_after_ms }) => retry_after_ms,
            other => panic!("expected a rate limit, got {other:?}"),
        }
    }

    #[test]
    fn a_budget_refills_evenly_and_says_when_the_next_request_is_due() {
        let limiter = RateLimiter::default();
        let start = Instant::now();
        let check = |after: Duration| limiter.check_at("alice", Some("cluster-a"), "tool", 6, start + after);

        // The whole minute of requests is available at once.
        for _ in 0..6 {
            check(Duration::ZERO).unwrap();
        }
        // Then one request comes back every ten seconds, and the caller is told how long to wait.
        assert_eq!(retry_after(check(Duration::ZERO)), 10_000);
        assert_eq!(retry_after(check(Duration::from_secs(4))), 6_000);
        check(Duration::from_secs(10)).unwrap();
        assert_eq!(retry_after(check(Duration::from_secs(10))), 10_000);
        // It does not come back all at once a minute after the first request.
        check(Duration::from_secs(20)).unwrap();
        assert_eq!(retry_after(check(Duration::from_millis(20_001))), 9_999);

        // An idle minute refills it completely, and never beyond one minute of requests.
        for _ in 0..6 {
            check(Duration::from_secs(600)).unwrap();
        }
        assert_eq!(retry_after(check(Duration::from_secs(600))), 10_000);
    }

    #[test]
    fn budgets_are_separate_per_principal_cluster_and_operation() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        limiter.check_at("alice", Some("cluster-a"), "tool", 1, now).unwrap();
        assert!(limiter.check_at("alice", Some("cluster-a"), "tool", 1, now).is_err());

        limiter.check_at("bob", Some("cluster-a"), "tool", 1, now).unwrap();
        limiter.check_at("alice", Some("cluster-b"), "tool", 1, now).unwrap();
        limiter.check_at("alice", None, "tool", 1, now).unwrap();
        limiter.check_at("alice", Some("cluster-a"), "other", 1, now).unwrap();
        // A principal cannot spell the key of another one, with or without a cluster.
        limiter
            .check_at("alice\u{1f}", Some("cluster-a"), "tool", 1, now)
            .unwrap();
        limiter.check_at("cluster-a\u{1f}alice", None, "tool", 1, now).unwrap();

        // A limit of zero admits nothing and still names a wait.
        assert_eq!(retry_after(limiter.check_at("carol", None, "tool", 0, now)), 60_000);
    }

    #[test]
    fn budgets_that_are_full_again_are_forgotten() {
        let limiter = RateLimiter::default();
        let start = Instant::now();
        for principal in 0..100 {
            limiter
                .check_at(&format!("principal-{principal}"), None, "tool", 60, start)
                .unwrap();
        }
        assert_eq!(limiter.tracked(), 100);

        // Half a minute later one caller uses two thirds of its budget; the sweep is not due yet.
        let later = start + Duration::from_secs(30);
        for _ in 0..40 {
            limiter.check_at("principal-0", None, "tool", 60, later).unwrap();
        }
        assert_eq!(limiter.tracked(), 100);

        // After the sweep interval only the budgets that are not full again remain: the one of
        // the caller that asks now, and the one that half a minute could not refill.
        let swept = start + Duration::from_secs(61);
        limiter.check_at("principal-1", None, "tool", 60, swept).unwrap();
        assert_eq!(limiter.tracked(), 2);
        // What was kept is still charged: 20 requests were left, and 31 came back.
        for _ in 0..51 {
            limiter.check_at("principal-0", None, "tool", 60, swept).unwrap();
        }
        assert!(limiter.check_at("principal-0", None, "tool", 60, swept).is_err());

        // Forgetting a budget changes no decision: the caller starts from a full one.
        for _ in 0..60 {
            limiter.check_at("principal-2", None, "tool", 60, swept).unwrap();
        }
        assert!(limiter.check_at("principal-2", None, "tool", 60, swept).is_err());
    }

    #[test]
    fn a_full_table_refuses_only_callers_it_does_not_track() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        for principal in 0..MAX_TRACKED_BUDGETS {
            limiter
                .check_at(&format!("principal-{principal}"), None, "tool", 60, now)
                .unwrap();
        }
        assert_eq!(limiter.tracked(), MAX_TRACKED_BUDGETS);

        assert_eq!(retry_after(limiter.check_at("newcomer", None, "tool", 60, now)), 60_000);
        limiter.check_at("principal-7", None, "tool", 60, now).unwrap();
        assert_eq!(limiter.tracked(), MAX_TRACKED_BUDGETS);

        // Once the others are idle again there is room.
        let later = now + Duration::from_secs(61);
        limiter.check_at("newcomer", None, "tool", 60, later).unwrap();
        assert_eq!(limiter.tracked(), 1);
    }

    #[test]
    fn a_wait_is_rounded_up_to_whole_milliseconds() {
        assert_eq!(retry_after_millis(Duration::from_nanos(1)), 1);
        assert_eq!(retry_after_millis(Duration::from_micros(1_500)), 2);
        assert_eq!(retry_after_millis(Duration::from_secs(60)), 60_000);
        assert_eq!(retry_after_millis(Duration::ZERO), 1);
    }
}
