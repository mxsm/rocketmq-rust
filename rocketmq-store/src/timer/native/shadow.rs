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

//! What a native engine running in shadow found when it compared itself with the active engine.
//!
//! The shadow indexes the timers the active engine admits and takes each one out of its wheel
//! when it comes due, as the native engine would deliver it. It writes nothing. It matches the
//! timer with the active engine's delivery by the delivery token they share, and records how
//! far apart the two times are: a timer the active engine has not delivered a grace period
//! after the shadow found it due, and a delivery the shadow never found due, are differences.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use serde::Serialize;

/// Differences kept as samples, the newest ones.
const SAMPLE_LIMIT: usize = 64;

/// Upper bounds, in milliseconds after the shadow's deadline, of the lateness buckets the
/// active engine's deliveries fall into. The last bucket takes everything later.
const LATENESS_BOUNDS_MS: [i64; 5] = [0, 1_000, 5_000, 30_000, i64::MAX];
const LATENESS_NAMES: [&str; 5] = [
    "shadowOnTime",
    "shadowLateWithin1s",
    "shadowLateWithin5s",
    "shadowLateWithin30s",
    "shadowLateOver30s",
];

/// How a shadow and the active engine disagreed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum NativeShadowDifference {
    /// The active engine delivered the timer before the shadow found it due, by more than
    /// the precision.
    Early,
    /// The active engine had not delivered the timer when the grace period ran out.
    NotDelivered,
    /// The active engine delivered a timer the shadow did not find due.
    Unindexed,
}

/// One difference, without the message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeShadowSample {
    pub(crate) difference: NativeShadowDifference,
    pub(crate) token: String,
    /// When the shadow found the timer due, or -1 when it never did.
    pub(crate) due_ms: i64,
    /// When the active engine's delivery was stored, or -1 when it was not.
    pub(crate) delivered_ms: i64,
}

/// What a shadow does with a due timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeShadowVerdict {
    /// The active engine delivered it; the timer leaves the wheel.
    Matched,
    /// Not delivered yet, within the grace period; the timer waits.
    Waits,
    /// Not delivered within the grace period; the timer leaves the wheel.
    NotDelivered,
}

/// Counters and samples of a shadow, live since the store started.
pub(crate) struct NativeShadowReport {
    grace_ms: i64,
    precision_ms: i64,
    matched: AtomicU64,
    early: AtomicU64,
    lateness: [AtomicU64; LATENESS_BOUNDS_MS.len()],
    max_lateness_ms: AtomicI64,
    not_delivered: AtomicU64,
    unindexed: AtomicU64,
    untracked: AtomicU64,
    samples: Mutex<VecDeque<NativeShadowSample>>,
}

impl NativeShadowReport {
    pub(crate) fn new(grace_ms: i64, precision_ms: i64) -> Self {
        Self {
            grace_ms,
            precision_ms,
            matched: AtomicU64::new(0),
            early: AtomicU64::new(0),
            lateness: std::array::from_fn(|_| AtomicU64::new(0)),
            max_lateness_ms: AtomicI64::new(i64::MIN),
            not_delivered: AtomicU64::new(0),
            unindexed: AtomicU64::new(0),
            untracked: AtomicU64::new(0),
            samples: Mutex::new(VecDeque::new()),
        }
    }

    /// How long after a timer came due its delivery is waited for.
    pub(crate) fn grace_ms(&self) -> i64 {
        self.grace_ms
    }

    /// Decides on a due timer with delivery token `token`, found due at `due_ms`: the active
    /// engine stored its delivery at `delivered_ms`, or not yet at `now_ms`.
    pub(crate) fn judge(
        &self,
        token: &str,
        due_ms: i64,
        delivered_ms: Option<i64>,
        now_ms: i64,
    ) -> NativeShadowVerdict {
        match delivered_ms {
            Some(delivered_ms) => {
                self.record_match(token, due_ms, delivered_ms);
                NativeShadowVerdict::Matched
            }
            None if now_ms < due_ms.saturating_add(self.grace_ms) => NativeShadowVerdict::Waits,
            None => {
                self.not_delivered.fetch_add(1, Ordering::Relaxed);
                self.sample(NativeShadowDifference::NotDelivered, token, due_ms, -1);
                NativeShadowVerdict::NotDelivered
            }
        }
    }

    /// Counts a due timer without a delivery token, which nothing can match.
    pub(crate) fn record_untracked(&self) {
        self.untracked.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts deliveries of the active engine the shadow never found due.
    pub(crate) fn record_unindexed(&self, deliveries: &[(String, i64)], total: usize) {
        self.unindexed
            .fetch_add(u64::try_from(total).unwrap_or(u64::MAX), Ordering::Relaxed);
        for (token, delivered_ms) in deliveries {
            self.sample(NativeShadowDifference::Unindexed, token, -1, *delivered_ms);
        }
    }

    fn record_match(&self, token: &str, due_ms: i64, delivered_ms: i64) {
        self.matched.fetch_add(1, Ordering::Relaxed);
        let lateness_ms = delivered_ms.saturating_sub(due_ms);
        self.max_lateness_ms.fetch_max(lateness_ms, Ordering::Relaxed);
        if lateness_ms < -self.precision_ms {
            self.early.fetch_add(1, Ordering::Relaxed);
            self.sample(NativeShadowDifference::Early, token, due_ms, delivered_ms);
            return;
        }
        let bucket = LATENESS_BOUNDS_MS
            .iter()
            .position(|bound| lateness_ms <= *bound)
            .unwrap_or(LATENESS_BOUNDS_MS.len() - 1);
        self.lateness[bucket].fetch_add(1, Ordering::Relaxed);
    }

    fn sample(&self, difference: NativeShadowDifference, token: &str, due_ms: i64, delivered_ms: i64) {
        let mut samples = self.samples.lock();
        if samples.len() == SAMPLE_LIMIT {
            samples.pop_front();
        }
        samples.push_back(NativeShadowSample {
            difference,
            token: token.to_owned(),
            due_ms,
            delivered_ms,
        });
    }

    /// The counters, by runtime-info name.
    pub(crate) fn metrics(&self) -> BTreeMap<&'static str, i64> {
        let total = |counter: &AtomicU64| i64::try_from(counter.load(Ordering::Relaxed)).unwrap_or(i64::MAX);
        let mut metrics = BTreeMap::from([
            ("shadowMatched", total(&self.matched)),
            ("shadowEarly", total(&self.early)),
            ("shadowNotDelivered", total(&self.not_delivered)),
            ("shadowUnindexed", total(&self.unindexed)),
            ("shadowUntracked", total(&self.untracked)),
            ("shadowGraceMs", self.grace_ms),
            (
                "shadowMaxLatenessMs",
                match self.max_lateness_ms.load(Ordering::Relaxed) {
                    i64::MIN => -1,
                    lateness => lateness,
                },
            ),
        ]);
        for (name, counter) in LATENESS_NAMES.iter().zip(&self.lateness) {
            metrics.insert(name, total(counter));
        }
        metrics
    }

    /// The newest differences, oldest first.
    pub(crate) fn samples(&self) -> Vec<NativeShadowSample> {
        self.samples.lock().iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_delivery_within_the_precision_is_on_time_and_an_earlier_one_is_a_difference() {
        let report = NativeShadowReport::new(60_000, 1_000);

        assert_eq!(
            report.judge("F:1:a:0:0", 10_000, Some(10_400), 10_400),
            NativeShadowVerdict::Matched
        );
        assert_eq!(
            report.judge("F:1:b:0:0", 10_000, Some(9_500), 10_400),
            NativeShadowVerdict::Matched
        );
        assert_eq!(
            report.judge("F:1:c:0:0", 10_000, Some(8_000), 10_400),
            NativeShadowVerdict::Matched
        );
        assert_eq!(
            report.judge("F:1:d:0:0", 10_000, Some(16_000), 16_000),
            NativeShadowVerdict::Matched
        );

        let metrics = report.metrics();
        assert_eq!(metrics["shadowMatched"], 4);
        assert_eq!(metrics["shadowEarly"], 1);
        assert_eq!(metrics["shadowOnTime"], 1);
        assert_eq!(metrics["shadowLateWithin1s"], 1);
        assert_eq!(metrics["shadowLateWithin30s"], 1);
        assert_eq!(metrics["shadowMaxLatenessMs"], 6_000);
        assert_eq!(report.samples()[0].difference, NativeShadowDifference::Early);
    }

    #[test]
    fn a_timer_waits_for_its_delivery_until_the_grace_period_runs_out() {
        let report = NativeShadowReport::new(60_000, 1_000);

        assert_eq!(
            report.judge("F:1:a:0:0", 10_000, None, 69_999),
            NativeShadowVerdict::Waits
        );
        assert_eq!(
            report.judge("F:1:a:0:0", 10_000, None, 70_000),
            NativeShadowVerdict::NotDelivered
        );

        assert_eq!(report.metrics()["shadowNotDelivered"], 1);
        assert_eq!(
            report.samples(),
            [NativeShadowSample {
                difference: NativeShadowDifference::NotDelivered,
                token: "F:1:a:0:0".to_owned(),
                due_ms: 10_000,
                delivered_ms: -1,
            }]
        );
    }

    #[test]
    fn samples_keep_the_newest_differences() {
        let report = NativeShadowReport::new(0, 1_000);
        for index in 0..SAMPLE_LIMIT + 3 {
            report.judge(&format!("F:1:{index}:0:0"), 0, None, 1);
        }

        let samples = report.samples();
        assert_eq!(samples.len(), SAMPLE_LIMIT);
        assert_eq!(samples[0].token, "F:1:3:0:0");
        assert_eq!(report.metrics()["shadowNotDelivered"], (SAMPLE_LIMIT + 3) as i64);
    }
}
