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

//! How bad a backlog is.
//!
//! The size of a backlog alone says little: a thousand waiting messages are a fraction of a second
//! of traffic on a busy Topic and weeks of it on a quiet one. The consume delay tells the two
//! apart, because it is the backlog measured in time: the size divided by the rate at which the
//! messages arrived. A backlog is therefore graded by size and by delay.

use super::Thresholds;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_collector::LagFigures;

/// Multiple of a threshold from which a backlog is `high`.
const HIGH_MULTIPLE: u64 = 10;

/// Multiple of a threshold from which a backlog is `critical`.
const CRITICAL_MULTIPLE: u64 = 20;

/// The grades of a backlog above the lag threshold, least severe first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Grade {
    Medium,
    High,
    Critical,
}

impl Grade {
    /// Grades `value`, which is above `threshold`, by how many times it exceeds it.
    fn of(value: u64, threshold: u64) -> Self {
        if value >= threshold.saturating_mul(CRITICAL_MULTIPLE) {
            Self::Critical
        } else if value >= threshold.saturating_mul(HIGH_MULTIPLE) {
            Self::High
        } else {
            Self::Medium
        }
    }
}

impl From<Grade> for Severity {
    fn from(grade: Grade) -> Self {
        match grade {
            Grade::Medium => Self::Medium,
            Grade::High => Self::High,
            Grade::Critical => Self::Critical,
        }
    }
}

/// Grades the backlog of `lag`.
///
/// - No queue was observed: `unknown`.
/// - The backlog is at or below the lag threshold: `healthy`.
/// - The backlog is above it, but its consume delay is below the delay threshold: `low`, however
///   large it is. At the rate the messages arrive, it is less than the delay threshold of traffic.
///   This does not hold while `consumption_stopped`: a backlog that nothing consumes is not
///   harmless for being young.
/// - Otherwise the higher of two grades: the size against the lag threshold and the consume delay
///   against the delay threshold. Each is `medium`, `high` from ten times its threshold and
///   `critical` from twenty times. Without a measured delay, the size decides alone.
pub(crate) fn grade(lag: LagFigures<'_>, thresholds: &Thresholds, consumption_stopped: bool) -> Severity {
    if lag.queues.queue_count == 0 {
        return Severity::Unknown;
    }
    if lag.total_lag <= thresholds.lag {
        return Severity::Healthy;
    }
    let by_size = Grade::of(
        u64::try_from(lag.total_lag).unwrap_or_default(),
        u64::try_from(thresholds.lag).unwrap_or_default(),
    );
    match lag.delay.consume_delay_ms {
        Some(delay) if delay < thresholds.consume_delay_ms && !consumption_stopped => Severity::Low,
        Some(delay) if delay >= thresholds.consume_delay_ms => {
            by_size.max(Grade::of(delay, thresholds.consume_delay_ms)).into()
        }
        _ => by_size.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::diagnosis_collector::fixtures;
    use crate::service::diagnosis_collector::fixtures::queue;
    use crate::service::diagnosis_collector::fixtures::queue_behind;

    const THRESHOLDS: Thresholds = Thresholds {
        lag: 1_000,
        consume_delay_ms: 300_000,
        pop_inflight: 1_000,
        retry_activity_window_ms: 600_000,
    };

    /// The severity of a backlog of `lag` whose last consumed message was stored `seconds` ago.
    fn behind(lag: i64, seconds: i64) -> Severity {
        grade(
            fixtures::lag_summary(vec![queue_behind("broker-a", 0, lag, seconds)], 100.0).figures(),
            &THRESHOLDS,
            false,
        )
    }

    /// The severity of a backlog of `lag` on a queue that reports no consumed message.
    fn unmeasured(lag: i64) -> Severity {
        grade(
            fixtures::lag_summary(vec![queue("broker-a", 0, lag)], 100.0).figures(),
            &THRESHOLDS,
            false,
        )
    }

    #[test]
    fn a_backlog_within_the_lag_threshold_is_healthy_however_old() {
        assert_eq!(behind(1_000, 86_400), Severity::Healthy);
        assert_eq!(unmeasured(0), Severity::Healthy);
        assert_eq!(
            grade(fixtures::lag_summary(Vec::new(), 0.0).figures(), &THRESHOLDS, false),
            Severity::Unknown
        );
    }

    #[test]
    fn size_alone_grades_a_backlog_whose_delay_cannot_be_measured() {
        assert_eq!(unmeasured(1_001), Severity::Medium);
        assert_eq!(unmeasured(9_999), Severity::Medium);
        assert_eq!(unmeasured(10_000), Severity::High);
        assert_eq!(unmeasured(20_000), Severity::Critical);
    }

    #[test]
    fn a_backlog_younger_than_the_delay_threshold_is_low_whatever_its_size() {
        assert_eq!(behind(1_001, 1), Severity::Low);
        assert_eq!(behind(5_000_000, 299), Severity::Low);
    }

    #[test]
    fn the_higher_of_size_and_delay_grades_an_old_backlog() {
        // Five minutes behind: the size decides.
        assert_eq!(behind(1_001, 300), Severity::Medium);
        assert_eq!(behind(20_000, 300), Severity::Critical);
        // A small backlog that is far behind: the delay decides.
        assert_eq!(behind(1_001, 2_999), Severity::Medium);
        assert_eq!(behind(1_001, 3_000), Severity::High);
        assert_eq!(behind(1_001, 6_000), Severity::Critical);
    }

    #[test]
    fn a_young_backlog_is_not_low_while_nothing_consumes_it() {
        let lag = fixtures::lag_summary(vec![queue_behind("broker-a", 0, 15_000, 10)], 0.0);

        assert_eq!(grade(lag.figures(), &THRESHOLDS, false), Severity::Low);
        assert_eq!(grade(lag.figures(), &THRESHOLDS, true), Severity::High);
    }

    #[test]
    fn a_lag_threshold_of_zero_makes_any_backlog_critical() {
        let thresholds = Thresholds { lag: 0, ..THRESHOLDS };

        assert_eq!(
            grade(
                fixtures::lag_summary(vec![queue("broker-a", 0, 1)], 100.0).figures(),
                &thresholds,
                false
            ),
            Severity::Critical
        );
        assert_eq!(
            grade(
                fixtures::lag_summary(vec![queue("broker-a", 0, 0)], 100.0).figures(),
                &thresholds,
                false
            ),
            Severity::Healthy
        );
    }
}
