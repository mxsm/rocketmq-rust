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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::BaselineId;
use crate::ChangePointId;
use crate::ClusterId;
use crate::EvidenceId;
use crate::ForecastId;
use crate::ResourceRef;
use crate::TenantId;

/// Historical window used to build a forecast.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastWindow {
    SevenDays,
    ThirtyDays,
}

/// Explainable direction of the fitted trend.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastTrend {
    Increasing,
    Decreasing,
    Stable,
    Unstable,
    Unknown,
}

/// Forecast availability and degradation state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastStatus {
    Ready,
    InsufficientData,
    Stale,
    UnstableTrend,
    Unsupported,
}

/// Explainable quality assessment for a forecast.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastQuality {
    Low,
    Medium,
    High,
}

/// One observed or projected point in a forecast series.
#[derive(Clone, Copy, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ForecastPoint {
    pub at: DateTime<Utc>,
    pub value: f64,
    pub projected: bool,
}

/// Deterministic holdout result used to monitor forecast accuracy.
#[derive(Clone, Copy, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ForecastBacktest {
    pub evaluated_points: u32,
    pub mean_absolute_error: Option<f64>,
    pub bias: Option<f64>,
    pub interval_coverage_ratio: Option<f64>,
}

/// Accuracy calculated from persisted outcomes of earlier projections.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ForecastAccuracy {
    pub metric: String,
    pub window: ForecastWindow,
    pub evaluated_points: u32,
    pub mean_absolute_error: Option<f64>,
    pub bias: Option<f64>,
    pub interval_coverage_ratio: Option<f64>,
    pub observed_at: DateTime<Utc>,
}

/// Capacity trend for one resource and metric.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CapacityForecast {
    pub id: ForecastId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub metric: String,
    pub window: ForecastWindow,
    pub trend: ForecastTrend,
    pub status: ForecastStatus,
    pub quality: ForecastQuality,
    pub algorithm_version: String,
    pub sample_start: DateTime<Utc>,
    pub sample_end: DateTime<Utc>,
    pub coverage_ratio: f64,
    pub slope_per_hour: Option<f64>,
    pub volatility: Option<f64>,
    pub threshold: Option<f64>,
    pub exhaustion_at: Option<DateTime<Utc>>,
    pub points: Vec<ForecastPoint>,
    pub backtest: ForecastBacktest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advisories: Vec<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub execution_eligible: bool,
    pub observed_at: DateTime<Utc>,
}

/// Estimated drain time for a lag, retry, DLQ, POP, or timer backlog.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct BacklogEta {
    pub id: ForecastId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub backlog_kind: String,
    pub window: ForecastWindow,
    pub trend: ForecastTrend,
    pub status: ForecastStatus,
    pub quality: ForecastQuality,
    pub current_value: f64,
    pub slope_per_hour: Option<f64>,
    pub arrival_rate_per_second: Option<f64>,
    pub drain_rate_per_second: Option<f64>,
    pub estimated_clear_at: Option<DateTime<Utc>>,
    pub sample_start: DateTime<Utc>,
    pub sample_end: DateTime<Utc>,
    pub coverage_ratio: f64,
    pub algorithm_version: String,
    pub backtest: ForecastBacktest,
    pub evidence_ids: Vec<EvidenceId>,
    pub execution_eligible: bool,
    pub observed_at: DateTime<Utc>,
}

/// Seasonal period represented by a deterministic baseline.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Seasonality {
    Hourly,
    Daily,
    Weekly,
}

/// Seasonal baseline used for deterministic anomaly detection.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct AnomalyBaseline {
    pub id: BaselineId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub metric: String,
    pub seasonality: Seasonality,
    pub period_seconds: u64,
    pub median: f64,
    pub median_absolute_deviation: f64,
    pub sample_count: u32,
    pub coverage_ratio: f64,
    pub algorithm_version: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}

/// Result of robust z-score and empirical-quantile anomaly evaluation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct AnomalyAssessment {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub metric: String,
    pub seasonality: Seasonality,
    pub status: ForecastStatus,
    pub observed_value: Option<f64>,
    pub baseline_median: Option<f64>,
    pub robust_z_score: Option<f64>,
    pub empirical_quantile: Option<f64>,
    pub anomaly: bool,
    pub evidence_ids: Vec<EvidenceId>,
    pub observed_at: DateTime<Utc>,
}

/// Non-causal change point emitted as an investigation hint.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ChangePoint {
    pub id: ChangePointId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub metric: String,
    pub detected_at: DateTime<Utc>,
    pub before_value: f64,
    pub after_value: f64,
    pub score: f64,
    pub algorithm_version: String,
    pub evidence_ids: Vec<EvidenceId>,
}

/// Bounded read-only projection returned by the cluster forecast API.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ClusterForecastReport {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub forecasts: Vec<CapacityForecast>,
    pub backlog_etas: Vec<BacklogEta>,
    pub baselines: Vec<AnomalyBaseline>,
    pub anomalies: Vec<AnomalyAssessment>,
    pub change_points: Vec<ChangePoint>,
    pub accuracy: Vec<ForecastAccuracy>,
    pub partial: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub execution_eligible: bool,
    pub observed_at: DateTime<Utc>,
}
