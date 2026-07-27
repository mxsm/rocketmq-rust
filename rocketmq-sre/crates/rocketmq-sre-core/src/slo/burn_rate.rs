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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_sre_contracts::BurnRateSeverity;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::SloDimension;

/// One fixed short/long burn-rate window policy.
#[derive(Clone, Debug, PartialEq)]
pub struct BurnWindowPolicy {
    pub id: String,
    pub short_window_seconds: u64,
    pub long_window_seconds: u64,
    pub threshold: f64,
    pub severity: BurnRateSeverity,
}

/// One SLI owned by exactly one deterministic health dimension.
#[derive(Clone, Debug, PartialEq)]
pub struct SliPolicy {
    pub id: String,
    pub display_name: String,
    pub dimension: SloDimension,
    pub objective: f64,
    pub weight: u8,
}

/// Complete deterministic SLO evaluation policy.
#[derive(Clone, Debug, PartialEq)]
pub struct SloPolicy {
    pub windows: Vec<BurnWindowPolicy>,
    pub slis: Vec<SliPolicy>,
    pub freshness_seconds: u64,
}

impl SloPolicy {
    /// Validates identifiers, windows, objectives and ownership before use.
    ///
    /// # Errors
    ///
    /// Returns a stable reason code when a policy is ambiguous or unsafe.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.windows.len() != 3 || self.slis.is_empty() || self.freshness_seconds == 0 {
            return Err("invalid_slo_policy_shape");
        }
        let mut window_ids = BTreeSet::new();
        for window in &self.windows {
            if !valid_identifier(&window.id)
                || !window_ids.insert(window.id.as_str())
                || window.short_window_seconds == 0
                || window.long_window_seconds <= window.short_window_seconds
                || !window.threshold.is_finite()
                || window.threshold <= 0.0
            {
                return Err("invalid_burn_window");
            }
        }
        let mut sli_ids = BTreeSet::new();
        for sli in &self.slis {
            if !valid_identifier(&sli.id)
                || sli.display_name.trim().is_empty()
                || !sli_ids.insert(sli.id.as_str())
                || !sli.objective.is_finite()
                || !(0.0..1.0).contains(&sli.objective)
                || sli.weight == 0
            {
                return Err("invalid_sli_policy");
            }
        }
        Ok(())
    }
}

/// Whether a Prometheus series represents the short or long side of a pair.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WindowRole {
    Short,
    Long,
}

/// One normalized, bounded Prometheus burn-rate sample.
#[derive(Clone, Debug, PartialEq)]
pub struct BurnRatePoint {
    pub sli_id: String,
    pub window_id: String,
    pub role: WindowRole,
    pub value: f64,
    pub observed_epoch_seconds: i64,
    pub evidence_id: EvidenceId,
    pub partial: bool,
}

/// Evaluation of one configured short/long window.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowEvaluation {
    pub window_id: String,
    pub short_window_seconds: u64,
    pub long_window_seconds: u64,
    pub short_burn_rate: Option<f64>,
    pub long_burn_rate: Option<f64>,
    pub threshold: f64,
    pub severity: BurnRateSeverity,
    pub triggered: bool,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub observed_epoch_seconds: Option<i64>,
    pub evidence_ids: Vec<EvidenceId>,
    pub reason_codes: Vec<String>,
}

/// Explainable evaluation of one SLI across all window pairs.
#[derive(Clone, Debug, PartialEq)]
pub struct SliEvaluation {
    pub id: String,
    pub display_name: String,
    pub dimension: SloDimension,
    pub objective: f64,
    pub weight: u8,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub windows: Vec<WindowEvaluation>,
    pub evidence_ids: Vec<EvidenceId>,
    pub reason_codes: Vec<String>,
}

/// Evaluates all configured SLIs with no model or network input.
///
/// The newest point wins for each `(sli, window, role)` key. A pair triggers
/// only when both short and long values cross the configured threshold.
///
/// # Errors
///
/// Returns a stable policy or sample reason code for invalid input.
pub fn evaluate_burn_rates(
    policy: &SloPolicy,
    points: &[BurnRatePoint],
    now_epoch_seconds: i64,
) -> Result<Vec<SliEvaluation>, &'static str> {
    policy.validate()?;
    let configured_slis = policy.slis.iter().map(|sli| sli.id.as_str()).collect::<BTreeSet<_>>();
    let configured_windows = policy
        .windows
        .iter()
        .map(|window| window.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut latest: BTreeMap<(&str, &str, WindowRole), &BurnRatePoint> = BTreeMap::new();
    for point in points {
        if !configured_slis.contains(point.sli_id.as_str()) || !configured_windows.contains(point.window_id.as_str()) {
            return Err("unknown_burn_rate_series");
        }
        if !point.value.is_finite() || point.value < 0.0 || point.observed_epoch_seconds > now_epoch_seconds + 60 {
            return Err("invalid_burn_rate_sample");
        }
        let key = (point.sli_id.as_str(), point.window_id.as_str(), point.role);
        match latest.get(&key) {
            Some(previous)
                if previous.observed_epoch_seconds > point.observed_epoch_seconds
                    || (previous.observed_epoch_seconds == point.observed_epoch_seconds
                        && previous.evidence_id <= point.evidence_id) => {}
            _ => {
                latest.insert(key, point);
            }
        }
    }

    Ok(policy
        .slis
        .iter()
        .map(|sli| {
            let windows = policy
                .windows
                .iter()
                .map(|window| evaluate_window(policy, sli, window, &latest, now_epoch_seconds))
                .collect::<Vec<_>>();
            let status = aggregate_status(windows.iter().map(|window| window.status));
            let data_quality = aggregate_quality(windows.iter().map(|window| window.data_quality));
            let evidence_ids = windows
                .iter()
                .flat_map(|window| window.evidence_ids.iter().copied())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let reason_codes = windows
                .iter()
                .flat_map(|window| window.reason_codes.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            SliEvaluation {
                id: sli.id.clone(),
                display_name: sli.display_name.clone(),
                dimension: sli.dimension,
                objective: sli.objective,
                weight: sli.weight,
                status,
                data_quality,
                windows,
                evidence_ids,
                reason_codes,
            }
        })
        .collect())
}

fn evaluate_window(
    policy: &SloPolicy,
    sli: &SliPolicy,
    window: &BurnWindowPolicy,
    latest: &BTreeMap<(&str, &str, WindowRole), &BurnRatePoint>,
    now_epoch_seconds: i64,
) -> WindowEvaluation {
    let short = latest.get(&(sli.id.as_str(), window.id.as_str(), WindowRole::Short));
    let long = latest.get(&(sli.id.as_str(), window.id.as_str(), WindowRole::Long));
    let mut reason_codes = Vec::new();
    let data_quality = match (short, long) {
        (Some(short), Some(long)) => {
            let oldest = short.observed_epoch_seconds.min(long.observed_epoch_seconds);
            let age = now_epoch_seconds.saturating_sub(oldest);
            if age > i64::try_from(policy.freshness_seconds).unwrap_or(i64::MAX) {
                reason_codes.push("slo_data_stale".to_owned());
                HealthDataQuality::Stale
            } else if short.partial || long.partial {
                reason_codes.push("slo_data_partial".to_owned());
                HealthDataQuality::Partial
            } else {
                HealthDataQuality::Complete
            }
        }
        _ => {
            reason_codes.push("slo_window_missing".to_owned());
            HealthDataQuality::Missing
        }
    };
    let triggered = matches!(data_quality, HealthDataQuality::Complete | HealthDataQuality::Partial)
        && short.is_some_and(|point| point.value >= window.threshold)
        && long.is_some_and(|point| point.value >= window.threshold);
    if triggered {
        reason_codes.push(format!("burn_rate_{}_triggered", window.id));
    }
    let status = match (triggered, window.severity, data_quality) {
        (_, _, HealthDataQuality::Missing | HealthDataQuality::Stale) => HealthStatus::Unknown,
        (true, BurnRateSeverity::Critical, _) => HealthStatus::Critical,
        (true, BurnRateSeverity::Warning, _) => HealthStatus::Degraded,
        (false, _, _) => HealthStatus::Healthy,
    };
    let evidence_ids = short
        .into_iter()
        .chain(long)
        .map(|point| point.evidence_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    WindowEvaluation {
        window_id: window.id.clone(),
        short_window_seconds: window.short_window_seconds,
        long_window_seconds: window.long_window_seconds,
        short_burn_rate: short.map(|point| point.value),
        long_burn_rate: long.map(|point| point.value),
        threshold: window.threshold,
        severity: window.severity,
        triggered,
        status,
        data_quality,
        observed_epoch_seconds: short
            .into_iter()
            .chain(long)
            .map(|point| point.observed_epoch_seconds)
            .min(),
        evidence_ids,
        reason_codes,
    }
}

pub(crate) fn aggregate_status(values: impl IntoIterator<Item = HealthStatus>) -> HealthStatus {
    values
        .into_iter()
        .max_by_key(|status| match status {
            HealthStatus::Healthy => 0,
            HealthStatus::Unknown => 1,
            HealthStatus::Degraded => 2,
            HealthStatus::Critical => 3,
        })
        .unwrap_or(HealthStatus::Unknown)
}

pub(crate) fn aggregate_quality(values: impl IntoIterator<Item = HealthDataQuality>) -> HealthDataQuality {
    values
        .into_iter()
        .max_by_key(|quality| match quality {
            HealthDataQuality::Complete => 0,
            HealthDataQuality::Partial => 1,
            HealthDataQuality::Stale => 2,
            HealthDataQuality::Missing => 3,
        })
        .unwrap_or(HealthDataQuality::Missing)
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> SloPolicy {
        SloPolicy {
            windows: vec![
                BurnWindowPolicy {
                    id: "fast".into(),
                    short_window_seconds: 300,
                    long_window_seconds: 3_600,
                    threshold: 14.4,
                    severity: BurnRateSeverity::Critical,
                },
                BurnWindowPolicy {
                    id: "medium".into(),
                    short_window_seconds: 1_800,
                    long_window_seconds: 21_600,
                    threshold: 6.0,
                    severity: BurnRateSeverity::Critical,
                },
                BurnWindowPolicy {
                    id: "slow".into(),
                    short_window_seconds: 21_600,
                    long_window_seconds: 259_200,
                    threshold: 1.0,
                    severity: BurnRateSeverity::Warning,
                },
            ],
            slis: vec![SliPolicy {
                id: "delivery_ratio".into(),
                display_name: "Delivery ratio".into(),
                dimension: SloDimension::Traffic,
                objective: 0.999,
                weight: 100,
            }],
            freshness_seconds: 120,
        }
    }

    fn point(window: &str, role: WindowRole, value: f64, at: i64) -> BurnRatePoint {
        BurnRatePoint {
            sli_id: "delivery_ratio".into(),
            window_id: window.into(),
            role,
            value,
            observed_epoch_seconds: at,
            evidence_id: EvidenceId::new(),
            partial: false,
        }
    }

    #[test]
    fn requires_both_sides_of_a_pair_to_trigger() {
        let now = 1_000;
        let points = [
            point("fast", WindowRole::Short, 20.0, now),
            point("fast", WindowRole::Long, 10.0, now),
        ];
        let result = evaluate_burn_rates(&policy(), &points, now).expect("evaluation");

        assert_eq!(result[0].windows[0].status, HealthStatus::Healthy);
        assert!(!result[0].windows[0].triggered);
    }

    #[test]
    fn reports_critical_partial_stale_and_missing_without_fabricating_zero() {
        let now = 1_000;
        let mut partial_short = point("fast", WindowRole::Short, 20.0, now);
        partial_short.partial = true;
        let points = [
            partial_short,
            point("fast", WindowRole::Long, 20.0, now),
            point("medium", WindowRole::Short, 20.0, now - 300),
            point("medium", WindowRole::Long, 20.0, now - 300),
        ];
        let result = evaluate_burn_rates(&policy(), &points, now).expect("evaluation");
        let windows = &result[0].windows;

        assert_eq!(windows[0].status, HealthStatus::Critical);
        assert_eq!(windows[0].data_quality, HealthDataQuality::Partial);
        assert_eq!(windows[1].status, HealthStatus::Unknown);
        assert_eq!(windows[1].data_quality, HealthDataQuality::Stale);
        assert_eq!(windows[2].short_burn_rate, None);
        assert_eq!(windows[2].long_burn_rate, None);
        assert_eq!(windows[2].data_quality, HealthDataQuality::Missing);
    }

    #[test]
    fn latest_point_wins_deterministically() {
        let now = 1_000;
        let old = point("fast", WindowRole::Short, 50.0, now - 10);
        let newest = point("fast", WindowRole::Short, 1.0, now);
        let points = [old, newest.clone(), point("fast", WindowRole::Long, 1.0, now)];
        let result = evaluate_burn_rates(&policy(), &points, now).expect("evaluation");

        assert_eq!(result[0].windows[0].short_burn_rate, Some(newest.value));
    }
}
