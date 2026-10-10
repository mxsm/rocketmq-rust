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

//! Admission control in front of the durable audit trail.
//!
//! Every admitted call writes an audit pair, dry runs included. Without a limit, one caller that
//! holds the write scope could loop dry runs and fill audit segments as fast as the disk allows.
//! A refused call is therefore refused before its `started` record: it is logged and counted,
//! and it costs no audit record.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use crate::config::LimitsConfig;
use crate::error::ControlError;

/// Principals whose budgets are tracked at once. A principal that has not called for a minute
/// has full budgets again and is forgotten when room is needed.
const MAX_TRACKED_PRINCIPALS: usize = 4_096;
const REFILL_PERIOD: Duration = Duration::from_secs(60);

pub(super) struct Admission {
    limits: LimitsConfig,
    state: Mutex<AdmissionState>,
}

#[derive(Default)]
struct AdmissionState {
    principals: BTreeMap<String, PrincipalBudgets>,
    in_flight: usize,
}

struct PrincipalBudgets {
    dry_run: TokenBucket,
    execute: TokenBucket,
}

/// A budget of `per_minute` calls that refills evenly and holds at most one minute of calls.
///
/// The budget is kept as time: a call costs the share of a minute that one call is worth, and
/// elapsed time is credited back up to a full minute. Integer time adds up exactly, where a
/// fractional token count would drift and refuse a call that is due.
struct TokenBucket {
    credit: Duration,
    refilled_at: tokio::time::Instant,
}

impl TokenBucket {
    fn full(now: tokio::time::Instant) -> Self {
        Self {
            credit: REFILL_PERIOD,
            refilled_at: now,
        }
    }

    fn refill(&mut self, now: tokio::time::Instant) {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        self.credit = self.credit.saturating_add(elapsed).min(REFILL_PERIOD);
        self.refilled_at = now;
    }

    fn is_full(&self) -> bool {
        self.credit >= REFILL_PERIOD
    }

    fn take(&mut self, per_minute: u32) -> bool {
        let cost = REFILL_PERIOD / per_minute.max(1);
        match self.credit.checked_sub(cost) {
            Some(remaining) => {
                self.credit = remaining;
                true
            }
            None => false,
        }
    }
}

impl Admission {
    pub(super) fn new(limits: LimitsConfig) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(AdmissionState::default()),
        })
    }

    /// Admits one call of `principal`, or refuses it with `rate_limited`.
    ///
    /// The returned permit holds one of the concurrent slots until it is dropped. Dry runs and
    /// execute calls draw on separate budgets, so planning cannot use up the budget for writes.
    pub(super) fn admit(self: &Arc<Self>, principal: &str, dry_run: bool) -> Result<AdmissionPermit, ControlError> {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // A saturated server refuses the call without charging the caller's budget.
        if state.in_flight >= self.limits.max_concurrent_calls {
            return Err(ControlError::rate_limited());
        }
        if !state.principals.contains_key(principal) && state.principals.len() >= MAX_TRACKED_PRINCIPALS {
            self.forget_idle_principals(&mut state, now);
            if state.principals.len() >= MAX_TRACKED_PRINCIPALS {
                return Err(ControlError::rate_limited());
            }
        }
        let budgets = state
            .principals
            .entry(principal.to_owned())
            .or_insert_with(|| PrincipalBudgets {
                dry_run: TokenBucket::full(now),
                execute: TokenBucket::full(now),
            });
        let (bucket, per_minute) = if dry_run {
            (&mut budgets.dry_run, self.limits.dry_runs_per_minute)
        } else {
            (&mut budgets.execute, self.limits.executes_per_minute)
        };
        bucket.refill(now);
        if !bucket.take(per_minute) {
            return Err(ControlError::rate_limited());
        }
        state.in_flight += 1;
        Ok(AdmissionPermit {
            admission: Arc::clone(self),
        })
    }

    /// Drops every principal whose budgets are full again: forgetting it changes nothing.
    fn forget_idle_principals(&self, state: &mut AdmissionState, now: tokio::time::Instant) {
        state.principals.retain(|_, budgets| {
            budgets.dry_run.refill(now);
            budgets.execute.refill(now);
            !(budgets.dry_run.is_full() && budgets.execute.is_full())
        });
    }
}

/// One concurrent slot, returned when the call that holds it ends.
pub(super) struct AdmissionPermit {
    admission: Arc<Admission>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.in_flight = state.in_flight.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ControlErrorCode;

    fn limits(dry_runs_per_minute: u32, executes_per_minute: u32, max_concurrent_calls: usize) -> LimitsConfig {
        LimitsConfig {
            dry_runs_per_minute,
            executes_per_minute,
            max_concurrent_calls,
        }
    }

    fn refused(result: Result<AdmissionPermit, ControlError>) -> bool {
        match result {
            Ok(_) => false,
            Err(error) => {
                assert_eq!(error.code(), ControlErrorCode::RateLimited);
                true
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn budgets_are_per_principal_per_mode_and_refill_evenly() {
        let admission = Admission::new(limits(6, 2, 64));
        for _ in 0..6 {
            assert!(!refused(admission.admit("alice", true)));
        }
        assert!(refused(admission.admit("alice", true)));
        // Planning does not use up the budget for writes, and one caller does not use up another's.
        for _ in 0..2 {
            assert!(!refused(admission.admit("alice", false)));
        }
        assert!(refused(admission.admit("alice", false)));
        assert!(!refused(admission.admit("bob", true)));

        // Six dry runs a minute refill at one every ten seconds.
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(refused(admission.admit("alice", true)));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!refused(admission.admit("alice", true)));
        assert!(refused(admission.admit("alice", true)));

        // A budget never holds more than one minute of calls.
        tokio::time::advance(Duration::from_secs(600)).await;
        for _ in 0..6 {
            assert!(!refused(admission.admit("alice", true)));
        }
        assert!(refused(admission.admit("alice", true)));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_slots_are_shared_and_returned_with_the_permit() {
        let admission = Admission::new(limits(60, 60, 2));
        let first = admission.admit("alice", true).unwrap();
        let second = admission.admit("bob", false).unwrap();
        assert!(refused(admission.admit("carol", true)));
        // A refusal for lack of a slot is not charged to the caller.
        drop(first);
        let third = admission.admit("carol", true).unwrap();
        assert!(refused(admission.admit("alice", true)));
        drop(second);
        drop(third);
        assert_eq!(admission.state.lock().unwrap().in_flight, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_principals_are_forgotten_when_room_is_needed() {
        let admission = Admission::new(limits(1, 1, 64));
        for index in 0..MAX_TRACKED_PRINCIPALS {
            assert!(!refused(admission.admit(&format!("principal-{index}"), true)));
        }
        // Every tracked principal has spent its budget, so none can be forgotten yet.
        assert!(refused(admission.admit("late", true)));
        tokio::time::advance(REFILL_PERIOD).await;
        assert!(!refused(admission.admit("late", true)));
        assert_eq!(admission.state.lock().unwrap().principals.len(), 1);
    }
}
