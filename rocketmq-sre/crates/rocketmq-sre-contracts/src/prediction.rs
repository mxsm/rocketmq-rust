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

/// Forecast availability and degradation state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
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

/// Capacity trend for one resource and metric.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CapacityForecast {
    pub id: ForecastId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub metric: String,
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
    pub evidence_ids: Vec<EvidenceId>,
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
    pub status: ForecastStatus,
    pub current_value: f64,
    pub arrival_rate_per_second: Option<f64>,
    pub drain_rate_per_second: Option<f64>,
    pub estimated_clear_at: Option<DateTime<Utc>>,
    pub coverage_ratio: f64,
    pub algorithm_version: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub observed_at: DateTime<Utc>,
}

/// Seasonal baseline used for deterministic anomaly detection.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct AnomalyBaseline {
    pub id: BaselineId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource: ResourceRef,
    pub metric: String,
    pub period_seconds: u64,
    pub median: f64,
    pub median_absolute_deviation: f64,
    pub sample_count: u32,
    pub coverage_ratio: f64,
    pub algorithm_version: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
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
