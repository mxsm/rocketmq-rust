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

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::AnomalyAssessment;
use rocketmq_sre_contracts::AnomalyBaseline;
use rocketmq_sre_contracts::BacklogEta;
use rocketmq_sre_contracts::BaselineId;
use rocketmq_sre_contracts::CapacityForecast;
use rocketmq_sre_contracts::ChangePoint;
use rocketmq_sre_contracts::ChangePointId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ForecastId;
use rocketmq_sre_contracts::ForecastPoint;
use rocketmq_sre_contracts::ForecastStatus;
use rocketmq_sre_contracts::ForecastWindow;
use rocketmq_sre_contracts::ResourceRef;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::prediction::backlog_eta::evaluate_backlog;
use rocketmq_sre_core::prediction::baseline::AnomalyEvaluation;
use rocketmq_sre_core::prediction::baseline::BaselineEvaluation;
use rocketmq_sre_core::prediction::baseline::BaselinePolicy;
use rocketmq_sre_core::prediction::baseline::assess_anomaly;
use rocketmq_sre_core::prediction::baseline::build_baseline;
use rocketmq_sre_core::prediction::capacity::evaluate_capacity;
use rocketmq_sre_core::prediction::change_point::detect_change_point;
use rocketmq_sre_core::prediction::trend::ObservedPoint;
use rocketmq_sre_core::prediction::trend::TrendEvaluation;
use rocketmq_sre_core::prediction::trend::evaluate_trend;
use serde_json::Value;

use super::policy::ForecastAggregation;
use super::policy::ForecastTarget;
use super::policy::ForecastTargetKind;
use super::policy::ForecastWindowPolicy;
use crate::ControlPlaneError;

const PROMETHEUS_SCHEMA: &str = "rocketmq.prometheus-evidence.v1";

pub(super) fn parse_prometheus_points(
    content: &Value,
    metric: &str,
    window: ForecastWindow,
    aggregation: ForecastAggregation,
    max_points: usize,
) -> Result<Vec<ObservedPoint>, ControlPlaneError> {
    let object = content.as_object().ok_or_else(invalid_evidence)?;
    let expected_kind = match window {
        ForecastWindow::SevenDays => "trend_7d",
        ForecastWindow::ThirtyDays => "trend_30d",
    };
    if object.get("schema_version").and_then(Value::as_str) != Some(PROMETHEUS_SCHEMA)
        || object.get("query_kind").and_then(Value::as_str) != Some(expected_kind)
        || object.get("metric").and_then(Value::as_str) != Some(metric)
    {
        return Err(invalid_evidence());
    }
    let series = object
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(invalid_evidence)?;
    if series.len() > max_points {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "Prometheus forecast series exceed the configured bound",
        ));
    }
    let mut grouped = BTreeMap::<i64, Aggregate>::new();
    let mut scanned = 0_usize;
    for raw_series in series {
        let samples = raw_series
            .get("samples")
            .and_then(Value::as_array)
            .ok_or_else(invalid_evidence)?;
        for sample in samples {
            scanned = scanned.saturating_add(1);
            if scanned > max_points {
                return Err(ControlPlaneError::validation(
                    "output_too_large",
                    "Prometheus forecast samples exceed the configured bound",
                ));
            }
            let at = sample
                .get("observed_at")
                .and_then(Value::as_str)
                .ok_or_else(invalid_evidence)?
                .parse::<DateTime<Utc>>()
                .map_err(|_| invalid_evidence())?;
            let value = sample
                .get("value")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite())
                .ok_or_else(invalid_evidence)?;
            grouped.entry(at.timestamp()).or_default().add(value);
        }
    }
    Ok(grouped
        .into_iter()
        .filter_map(|(at_seconds, aggregate)| {
            aggregate
                .finish(aggregation)
                .map(|value| ObservedPoint { at_seconds, value })
        })
        .collect())
}

#[derive(Clone, Copy, Debug, Default)]
struct Aggregate {
    sum: f64,
    count: u32,
    min: Option<f64>,
    max: Option<f64>,
}

impl Aggregate {
    fn add(&mut self, value: f64) {
        self.sum += value;
        self.count = self.count.saturating_add(1);
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
    }

    fn finish(self, aggregation: ForecastAggregation) -> Option<f64> {
        match aggregation {
            ForecastAggregation::Sum => (self.count > 0).then_some(self.sum),
            ForecastAggregation::Max => self.max,
            ForecastAggregation::Min => self.min,
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "contract projection keeps authenticated scope and algorithm metadata explicit"
)]
pub(super) fn capacity_forecast(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &ForecastTarget,
    window: &ForecastWindowPolicy,
    points: &[ObservedPoint],
    evidence_ids: Vec<EvidenceId>,
    algorithm_version: &str,
    now: DateTime<Utc>,
) -> Result<CapacityForecast, ControlPlaneError> {
    let threshold = target
        .threshold
        .ok_or_else(|| ControlPlaneError::configuration("capacity forecast target does not define a threshold"))?;
    let (evaluation, advisories) = match target.kind {
        ForecastTargetKind::Capacity => {
            let capacity = evaluate_capacity(points, window.trend, threshold, now.timestamp()).map_err(|reason| {
                ControlPlaneError::configuration(format!("capacity trend policy cannot be evaluated: {reason}"))
            })?;
            (capacity.trend, capacity.advisories)
        }
        ForecastTargetKind::Expiry => {
            let direction = target.threshold_direction.ok_or_else(|| {
                ControlPlaneError::configuration("expiry forecast target does not define a threshold direction")
            })?;
            let trend = evaluate_trend(points, window.trend, Some((threshold, direction)), now.timestamp()).map_err(
                |reason| ControlPlaneError::configuration(format!("expiry trend policy cannot be evaluated: {reason}")),
            )?;
            let advisory = if trend.exhaustion_at_seconds.is_some() {
                "rotate_or_review_before_expiry"
            } else if trend.status == ForecastStatus::Ready {
                "continue_expiry_observation"
            } else {
                "restore_expiry_signal_coverage"
            };
            (trend, vec![advisory.to_owned()])
        }
        ForecastTargetKind::Backlog => {
            return Err(ControlPlaneError::configuration(
                "backlog target cannot be projected as capacity",
            ));
        }
    };
    let (sample_start, sample_end) = sample_range(&evaluation, window, now)?;
    Ok(CapacityForecast {
        id: ForecastId::new(),
        tenant_id,
        cluster_id,
        resource: resource(target),
        metric: target.metric.clone(),
        window: window.window,
        trend: evaluation.trend,
        status: evaluation.status,
        quality: evaluation.quality,
        algorithm_version: algorithm_version.to_owned(),
        sample_start,
        sample_end,
        coverage_ratio: evaluation.coverage_ratio,
        slope_per_hour: evaluation.slope_per_hour,
        volatility: evaluation.normalized_volatility,
        threshold: evaluation.threshold,
        exhaustion_at: optional_datetime(evaluation.exhaustion_at_seconds)?,
        points: contract_points(&evaluation)?,
        backtest: evaluation.backtest,
        advisories,
        evidence_ids,
        execution_eligible: false,
        observed_at: now,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "contract projection keeps authenticated scope and algorithm metadata explicit"
)]
pub(super) fn backlog_forecast(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &ForecastTarget,
    window: &ForecastWindowPolicy,
    points: &[ObservedPoint],
    evidence_ids: Vec<EvidenceId>,
    algorithm_version: &str,
    now: DateTime<Utc>,
) -> Result<BacklogEta, ControlPlaneError> {
    let evaluation = evaluate_backlog(points, window.trend, now.timestamp()).map_err(|reason| {
        ControlPlaneError::configuration(format!("backlog trend policy cannot be evaluated: {reason}"))
    })?;
    let (sample_start, sample_end) = sample_range(&evaluation.trend, window, now)?;
    Ok(BacklogEta {
        id: ForecastId::new(),
        tenant_id,
        cluster_id,
        resource: resource(target),
        backlog_kind: target.category.clone(),
        window: window.window,
        trend: evaluation.trend.trend,
        status: evaluation.trend.status,
        quality: evaluation.trend.quality,
        current_value: evaluation.current_value,
        slope_per_hour: evaluation.trend.slope_per_hour,
        arrival_rate_per_second: evaluation.arrival_rate_per_second,
        drain_rate_per_second: evaluation.drain_rate_per_second,
        estimated_clear_at: optional_datetime(evaluation.estimated_clear_at_seconds)?,
        sample_start,
        sample_end,
        coverage_ratio: evaluation.trend.coverage_ratio,
        algorithm_version: algorithm_version.to_owned(),
        backtest: evaluation.trend.backtest,
        evidence_ids,
        execution_eligible: false,
        observed_at: now,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "baseline projection keeps authenticated scope and evidence metadata explicit"
)]
pub(super) fn baseline_artifacts(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &ForecastTarget,
    policy: BaselinePolicy,
    points: &[ObservedPoint],
    evidence_ids: &[EvidenceId],
    algorithm_version: &str,
    now: DateTime<Utc>,
) -> Result<(Option<AnomalyBaseline>, AnomalyAssessment), ControlPlaneError> {
    let reference = points.last().map_or(now.timestamp(), |point| point.at_seconds);
    let baseline = build_baseline(points, reference, policy).map_err(|reason| {
        ControlPlaneError::configuration(format!("seasonal baseline policy cannot be evaluated: {reason}"))
    })?;
    let current = points.last().map(|point| point.value);
    let anomaly = assess_anomaly(&baseline, current, policy);
    let assessment = anomaly_contract(tenant_id, cluster_id, target, policy, &anomaly, evidence_ids, now);
    let contract = baseline_contract(tenant_id, cluster_id, target, policy, &baseline, algorithm_version, now)?;
    Ok((contract, assessment))
}

#[allow(
    clippy::too_many_arguments,
    reason = "change-point projection keeps authenticated scope and evidence metadata explicit"
)]
pub(super) fn change_point_artifact(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &ForecastTarget,
    points: &[ObservedPoint],
    window_samples: usize,
    score_threshold: f64,
    evidence_ids: Vec<EvidenceId>,
    algorithm_version: &str,
) -> Result<Option<ChangePoint>, ControlPlaneError> {
    detect_change_point(points, window_samples, score_threshold)
        .map(|change| {
            Ok(ChangePoint {
                id: ChangePointId::new(),
                tenant_id,
                cluster_id,
                resource: resource(target),
                metric: target.metric.clone(),
                detected_at: required_datetime(change.detected_at_seconds)?,
                before_value: change.before_value,
                after_value: change.after_value,
                score: change.score,
                algorithm_version: algorithm_version.to_owned(),
                evidence_ids,
            })
        })
        .transpose()
}

fn baseline_contract(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &ForecastTarget,
    policy: BaselinePolicy,
    baseline: &BaselineEvaluation,
    algorithm_version: &str,
    now: DateTime<Utc>,
) -> Result<Option<AnomalyBaseline>, ControlPlaneError> {
    let (Some(median), Some(mad)) = (baseline.median, baseline.median_absolute_deviation) else {
        return Ok(None);
    };
    Ok(Some(AnomalyBaseline {
        id: BaselineId::new(),
        tenant_id,
        cluster_id,
        resource: resource(target),
        metric: target.metric.clone(),
        seasonality: policy.seasonality,
        period_seconds: policy.period_seconds,
        median,
        median_absolute_deviation: mad,
        sample_count: u32::try_from(baseline.sample_count).unwrap_or(u32::MAX),
        coverage_ratio: baseline.coverage_ratio,
        algorithm_version: algorithm_version.to_owned(),
        valid_from: now
            - Duration::seconds(
                i64::try_from(policy.history_window_seconds)
                    .map_err(|_| ControlPlaneError::configuration("baseline history window cannot be represented"))?,
            ),
        valid_until: now
            + Duration::seconds(
                i64::try_from(policy.period_seconds)
                    .map_err(|_| ControlPlaneError::configuration("baseline period cannot be represented"))?,
            ),
    }))
}

fn anomaly_contract(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    target: &ForecastTarget,
    policy: BaselinePolicy,
    anomaly: &AnomalyEvaluation,
    evidence_ids: &[EvidenceId],
    now: DateTime<Utc>,
) -> AnomalyAssessment {
    AnomalyAssessment {
        tenant_id,
        cluster_id,
        resource: resource(target),
        metric: target.metric.clone(),
        seasonality: policy.seasonality,
        status: anomaly.status,
        observed_value: anomaly.observed_value,
        baseline_median: anomaly.baseline_median,
        robust_z_score: anomaly.robust_z_score,
        empirical_quantile: anomaly.empirical_quantile,
        anomaly: anomaly.anomaly,
        evidence_ids: evidence_ids.to_vec(),
        observed_at: now,
    }
}

fn resource(target: &ForecastTarget) -> ResourceRef {
    ResourceRef {
        kind: target.resource_kind,
        key: format!("forecast/{}", target.id),
        display_name: Some(target.category.clone()),
    }
}

fn sample_range(
    evaluation: &TrendEvaluation,
    window: &ForecastWindowPolicy,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ControlPlaneError> {
    let start = evaluation
        .sample_start_seconds
        .map(required_datetime)
        .transpose()?
        .unwrap_or_else(|| now - Duration::seconds(i64::try_from(window.trend.window_seconds).unwrap_or(i64::MAX)));
    let end = evaluation
        .sample_end_seconds
        .map(required_datetime)
        .transpose()?
        .unwrap_or(now);
    Ok((start, end))
}

fn contract_points(evaluation: &TrendEvaluation) -> Result<Vec<ForecastPoint>, ControlPlaneError> {
    evaluation
        .observed_points
        .iter()
        .map(|point| {
            Ok(ForecastPoint {
                at: required_datetime(point.at_seconds)?,
                value: point.value,
                projected: false,
            })
        })
        .chain(evaluation.projected_points.iter().map(|point| {
            Ok(ForecastPoint {
                at: required_datetime(point.at_seconds)?,
                value: point.value,
                projected: true,
            })
        }))
        .collect()
}

fn required_datetime(seconds: i64) -> Result<DateTime<Utc>, ControlPlaneError> {
    DateTime::from_timestamp(seconds, 0).ok_or_else(|| {
        ControlPlaneError::validation("invalid_forecast", "forecast timestamp is outside the supported range")
    })
}

fn optional_datetime(seconds: Option<i64>) -> Result<Option<DateTime<Utc>>, ControlPlaneError> {
    seconds.map(required_datetime).transpose()
}

fn invalid_evidence() -> ControlPlaneError {
    ControlPlaneError::validation(
        "invalid_forecast_evidence",
        "Prometheus forecast evidence does not match the canonical schema",
    )
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::ResourceKind;
    use rocketmq_sre_core::prediction::trend::TrendPolicy;
    use serde_json::json;

    use super::*;

    #[test]
    fn prometheus_series_are_aggregated_by_timestamp_with_a_hard_bound() {
        let value = json!({
            "schema_version": PROMETHEUS_SCHEMA,
            "query_kind": "trend_7d",
            "metric": "rocketmq_consumer_lag",
            "series": [
                {"samples": [{"observed_at": "2026-01-01T00:00:00Z", "value": 4.0}]},
                {"samples": [{"observed_at": "2026-01-01T00:00:00Z", "value": 6.0}]}
            ]
        });
        let points = parse_prometheus_points(
            &value,
            "rocketmq_consumer_lag",
            ForecastWindow::SevenDays,
            ForecastAggregation::Sum,
            8,
        )
        .expect("points");
        assert_eq!(
            points,
            [ObservedPoint {
                at_seconds: 1_767_225_600,
                value: 10.0
            }]
        );
    }

    #[test]
    fn no_evidence_creates_explicit_insufficient_contract() {
        let target = ForecastTarget {
            id: "disk".into(),
            category: "broker_disk".into(),
            kind: ForecastTargetKind::Capacity,
            metric: "disk_used".into(),
            resource_kind: ResourceKind::Broker,
            aggregation: ForecastAggregation::Max,
            threshold: Some(0.85),
            threshold_direction: Some(rocketmq_sre_core::prediction::trend::ThresholdDirection::Upper),
        };
        let window = ForecastWindowPolicy {
            window: ForecastWindow::SevenDays,
            trend: TrendPolicy {
                window_seconds: 604_800,
                expected_interval_seconds: 3_600,
                min_samples: 120,
                min_coverage_ratio: 0.7,
                freshness_seconds: 7_200,
                max_normalized_volatility: 0.15,
                min_absolute_slope_per_hour: 0.0001,
                projection_horizon_hours: 168,
                projection_points: 8,
            },
        };
        let forecast = capacity_forecast(
            TenantId::new(),
            ClusterId::new(),
            &target,
            &window,
            &[],
            vec![],
            "test",
            Utc::now(),
        )
        .expect("forecast");
        assert_eq!(forecast.status, ForecastStatus::InsufficientData);
        assert!(!forecast.execution_eligible);
    }
}
