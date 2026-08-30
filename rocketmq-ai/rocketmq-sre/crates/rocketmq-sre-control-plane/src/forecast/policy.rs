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

use std::collections::BTreeSet;
use std::time::Duration;

use rocketmq_sre_contracts::ForecastWindow;
use rocketmq_sre_contracts::ResourceKind;
use rocketmq_sre_contracts::Seasonality;
use rocketmq_sre_core::prediction::baseline::BaselinePolicy;
use rocketmq_sre_core::prediction::trend::ThresholdDirection;
use rocketmq_sre_core::prediction::trend::TrendPolicy;
use serde::Deserialize;

use crate::ControlPlaneError;

const FORECAST_CONFIG: &str = include_str!("../../../../config/forecast/rocketmq-forecast.yaml");
const SCHEMA_VERSION: &str = "rocketmq-sre.forecast-config.v1";
const REQUIRED_CATEGORIES: &[&str] = &[
    "broker_disk",
    "pvc_disk",
    "commitlog",
    "consume_queue",
    "tiered_store",
    "rocksdb",
    "proxy_capacity",
    "broker_capacity",
    "controller_capacity",
    "tps",
    "connections",
    "consumer_lag",
    "retry_backlog",
    "dlq_backlog",
    "pop_backlog",
    "timer_backlog",
    "certificate_expiry",
    "secret_expiry",
    "jwks_expiry",
    "knowledge_expiry",
    "policy_expiry",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForecastTargetKind {
    Capacity,
    Backlog,
    Expiry,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForecastAggregation {
    Sum,
    Max,
    Min,
}

#[derive(Clone, Debug)]
pub(crate) struct ForecastWindowPolicy {
    pub(crate) window: ForecastWindow,
    pub(crate) trend: TrendPolicy,
}

#[derive(Clone, Debug)]
pub(crate) struct ForecastTarget {
    pub(crate) id: String,
    pub(crate) category: String,
    pub(crate) kind: ForecastTargetKind,
    pub(crate) metric: String,
    pub(crate) resource_kind: ResourceKind,
    pub(crate) aggregation: ForecastAggregation,
    pub(crate) threshold: Option<f64>,
    pub(crate) threshold_direction: Option<ThresholdDirection>,
}

/// Parsed and validated deterministic forecasting configuration.
#[derive(Clone, Debug)]
pub(crate) struct ForecastConfiguration {
    pub(crate) algorithm_version: String,
    pub(crate) worker_interval: Duration,
    pub(crate) query_timeout: Duration,
    pub(crate) max_evaluations_per_run: usize,
    pub(crate) max_response_points: usize,
    pub(crate) change_point_window_samples: usize,
    pub(crate) change_point_score_threshold: f64,
    pub(crate) windows: Vec<ForecastWindowPolicy>,
    pub(crate) baselines: Vec<BaselinePolicy>,
    pub(crate) targets: Vec<ForecastTarget>,
}

impl ForecastConfiguration {
    pub(crate) fn embedded() -> Result<Self, ControlPlaneError> {
        let document: ForecastConfigurationDocument = serde_yaml::from_str(FORECAST_CONFIG).map_err(|error| {
            ControlPlaneError::configuration(format!("embedded forecast configuration cannot be parsed: {error}"))
        })?;
        if document.schema_version != SCHEMA_VERSION
            || document.algorithm_version.trim().is_empty()
            || document.worker_interval_seconds == 0
            || document.query_timeout_seconds == 0
            || document.query_timeout_seconds >= document.worker_interval_seconds
            || !(1..=64).contains(&document.max_evaluations_per_run)
            || !(128..=8_192).contains(&document.max_response_points)
            || document.change_point_window_samples < 3
            || !document.change_point_score_threshold.is_finite()
            || document.change_point_score_threshold <= 0.0
            || !document.anomaly_robust_z_threshold.is_finite()
            || document.anomaly_robust_z_threshold <= 0.0
        {
            return Err(ControlPlaneError::configuration(
                "embedded forecast configuration has an unsupported shape",
            ));
        }
        let windows = document
            .windows
            .into_iter()
            .map(|window| {
                let trend = TrendPolicy {
                    window_seconds: window.window_seconds,
                    expected_interval_seconds: window.expected_interval_seconds,
                    min_samples: window.min_samples,
                    min_coverage_ratio: window.min_coverage_ratio,
                    freshness_seconds: window.freshness_seconds,
                    max_normalized_volatility: window.max_normalized_volatility,
                    min_absolute_slope_per_hour: window.min_absolute_slope_per_hour,
                    projection_horizon_hours: window.projection_horizon_hours,
                    projection_points: window.projection_points,
                };
                trend.validate().map_err(|reason| {
                    ControlPlaneError::configuration(format!("forecast trend policy is invalid: {reason}"))
                })?;
                Ok(ForecastWindowPolicy {
                    window: window.window,
                    trend,
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        if windows.len() != 2
            || windows.iter().map(|window| window.window).collect::<BTreeSet<_>>()
                != BTreeSet::from([ForecastWindow::SevenDays, ForecastWindow::ThirtyDays])
        {
            return Err(ControlPlaneError::configuration(
                "forecast configuration must contain exactly the 7d and 30d windows",
            ));
        }
        let baselines = document
            .baselines
            .into_iter()
            .map(|baseline| {
                let policy = BaselinePolicy {
                    seasonality: baseline.seasonality,
                    period_seconds: baseline.period_seconds,
                    bucket_width_seconds: baseline.bucket_width_seconds,
                    history_window_seconds: baseline.history_window_seconds,
                    min_samples: baseline.min_samples,
                    robust_z_threshold: document.anomaly_robust_z_threshold,
                    lower_quantile_threshold: 0.01,
                    upper_quantile_threshold: 0.99,
                };
                policy.validate().map_err(|reason| {
                    ControlPlaneError::configuration(format!("forecast baseline policy is invalid: {reason}"))
                })?;
                Ok(policy)
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        if baselines
            .iter()
            .map(|baseline| baseline.seasonality)
            .collect::<BTreeSet<_>>()
            != BTreeSet::from([Seasonality::Hourly, Seasonality::Daily, Seasonality::Weekly])
        {
            return Err(ControlPlaneError::configuration(
                "forecast configuration must contain hourly, daily and weekly baselines",
            ));
        }
        let mut target_ids = BTreeSet::new();
        let mut categories = BTreeSet::new();
        let targets = document
            .targets
            .into_iter()
            .map(|target| {
                validate_identifier("forecast target id", &target.id)?;
                validate_identifier("forecast category", &target.category)?;
                validate_metric(&target.metric)?;
                if !target_ids.insert(target.id.clone()) || !categories.insert(target.category.clone()) {
                    return Err(ControlPlaneError::configuration(
                        "forecast target ids and categories must be unique",
                    ));
                }
                match target.kind {
                    ForecastTargetKind::Capacity | ForecastTargetKind::Expiry
                        if target.threshold.is_none() || target.threshold_direction.is_none() =>
                    {
                        return Err(ControlPlaneError::configuration(
                            "capacity and expiry targets require a finite threshold and direction",
                        ));
                    }
                    ForecastTargetKind::Backlog
                        if target.threshold.is_some() || target.threshold_direction.is_some() =>
                    {
                        return Err(ControlPlaneError::configuration(
                            "backlog targets cannot define a capacity threshold",
                        ));
                    }
                    ForecastTargetKind::Capacity | ForecastTargetKind::Backlog | ForecastTargetKind::Expiry => {}
                }
                if target.threshold.is_some_and(|value| !value.is_finite()) {
                    return Err(ControlPlaneError::configuration(
                        "forecast target threshold must be finite",
                    ));
                }
                Ok(ForecastTarget {
                    id: target.id,
                    category: target.category,
                    kind: target.kind,
                    metric: target.metric,
                    resource_kind: target.resource_kind,
                    aggregation: target.aggregation,
                    threshold: target.threshold,
                    threshold_direction: target.threshold_direction.map(Into::into),
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        if categories != REQUIRED_CATEGORIES.iter().map(|value| (*value).to_owned()).collect() {
            return Err(ControlPlaneError::configuration(
                "forecast target catalog does not cover every required Phase 2 category",
            ));
        }
        Ok(Self {
            algorithm_version: document.algorithm_version,
            worker_interval: Duration::from_secs(document.worker_interval_seconds),
            query_timeout: Duration::from_secs(document.query_timeout_seconds),
            max_evaluations_per_run: document.max_evaluations_per_run,
            max_response_points: document.max_response_points,
            change_point_window_samples: document.change_point_window_samples,
            change_point_score_threshold: document.change_point_score_threshold,
            windows,
            baselines,
            targets,
        })
    }
}

#[derive(Deserialize)]
struct ForecastConfigurationDocument {
    schema_version: String,
    algorithm_version: String,
    worker_interval_seconds: u64,
    query_timeout_seconds: u64,
    max_evaluations_per_run: usize,
    max_response_points: usize,
    change_point_window_samples: usize,
    change_point_score_threshold: f64,
    anomaly_robust_z_threshold: f64,
    windows: Vec<ForecastWindowDocument>,
    baselines: Vec<BaselineDocument>,
    targets: Vec<ForecastTargetDocument>,
}

#[derive(Deserialize)]
struct ForecastWindowDocument {
    window: ForecastWindow,
    window_seconds: u64,
    expected_interval_seconds: u64,
    min_samples: usize,
    min_coverage_ratio: f64,
    freshness_seconds: u64,
    max_normalized_volatility: f64,
    min_absolute_slope_per_hour: f64,
    projection_horizon_hours: u64,
    projection_points: usize,
}

#[derive(Deserialize)]
struct BaselineDocument {
    seasonality: Seasonality,
    period_seconds: u64,
    bucket_width_seconds: u64,
    history_window_seconds: u64,
    min_samples: usize,
}

#[derive(Deserialize)]
struct ForecastTargetDocument {
    id: String,
    category: String,
    kind: ForecastTargetKind,
    metric: String,
    resource_kind: ResourceKind,
    aggregation: ForecastAggregation,
    threshold: Option<f64>,
    threshold_direction: Option<ThresholdDirectionDocument>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ThresholdDirectionDocument {
    Upper,
    Lower,
}

impl From<ThresholdDirectionDocument> for ThresholdDirection {
    fn from(value: ThresholdDirectionDocument) -> Self {
        match value {
            ThresholdDirectionDocument::Upper => Self::Upper,
            ThresholdDirectionDocument::Lower => Self::Lower,
        }
    }
}

fn validate_identifier(name: &str, value: &str) -> Result<(), ControlPlaneError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ControlPlaneError::configuration(format!("{name} is invalid")));
    }
    Ok(())
}

fn validate_metric(value: &str) -> Result<(), ControlPlaneError> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'.'))
    {
        return Err(ControlPlaneError::configuration("forecast metric name is invalid"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_covers_every_required_target_and_window() {
        let config = ForecastConfiguration::embedded().expect("forecast configuration");
        assert_eq!(config.targets.len(), REQUIRED_CATEGORIES.len());
        assert_eq!(config.windows.len(), 2);
        assert_eq!(config.baselines.len(), 3);
        assert_eq!(
            config
                .targets
                .iter()
                .map(|target| target.category.as_str())
                .collect::<BTreeSet<_>>(),
            REQUIRED_CATEGORIES.iter().copied().collect()
        );
    }

    #[test]
    fn configured_queries_are_fixed_metric_identifiers() {
        let config = ForecastConfiguration::embedded().expect("forecast configuration");
        assert!(
            config
                .targets
                .iter()
                .all(|target| !target.metric.contains(['{', '}', '(', ')', ' ']))
        );
    }
}
