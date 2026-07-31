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

use crate::ClusterId;
use crate::EvidenceId;
use crate::HealthSnapshotId;
use crate::TenantId;

/// Fixed health dimensions used by the deterministic Phase 2 score.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SloDimension {
    Traffic,
    Consumer,
    Broker,
    Store,
    HaController,
    RoutingProxy,
    Security,
    Platform,
}

/// Severity assigned to a configured multi-window burn-rate trigger.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BurnRateSeverity {
    Warning,
    Critical,
}

/// Health result of an SLI, dimension, cluster, or fleet.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Critical,
    Unknown,
}

/// Explicit quality of the data used by a health evaluation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthDataQuality {
    Complete,
    Partial,
    Stale,
    Missing,
}

/// Operational context is carried independently so maintenance or drills do
/// not hide the underlying health severity.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthOperationalState {
    Normal,
    Maintenance,
    FaultDrill,
}

/// Evaluation of one configured short/long burn-rate window pair.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct BurnRateWindowResult {
    pub window_id: String,
    pub short_window_seconds: u64,
    pub long_window_seconds: u64,
    pub short_burn_rate: Option<f64>,
    pub long_burn_rate: Option<f64>,
    pub threshold: f64,
    pub severity: BurnRateSeverity,
    pub triggered: bool,
    pub data_quality: HealthDataQuality,
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reason_codes: Vec<String>,
}

/// Explainable multi-window result for one SLI.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct SliHealth {
    pub id: String,
    pub display_name: String,
    pub dimension: SloDimension,
    pub objective: f64,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub windows: Vec<BurnRateWindowResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reason_codes: Vec<String>,
}

/// Deterministic score and contributing SLIs for one health dimension.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct HealthDimensionScore {
    pub dimension: SloDimension,
    pub weight: u8,
    pub score: Option<u8>,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggered_sli_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reason_codes: Vec<String>,
}

/// Previous-to-current deterministic health transition.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct HealthRecentChange {
    pub previous_score: Option<u8>,
    pub current_score: Option<u8>,
    pub score_delta: Option<i16>,
    pub previous_status: HealthStatus,
    pub current_status: HealthStatus,
    pub occurred_at: DateTime<Utc>,
}

/// Incident counts incorporated into the cluster overview.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IncidentHealthSummary {
    pub active_incidents: u32,
    pub critical_incidents: u32,
    pub unassigned_incidents: u32,
    pub last_alert_at: Option<DateTime<Utc>>,
}

/// Immutable, explainable health evaluation for one cluster.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ClusterHealthReport {
    pub schema_version: String,
    pub id: HealthSnapshotId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub score: Option<u8>,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub operational_state: HealthOperationalState,
    pub dimensions: Vec<HealthDimensionScore>,
    pub slis: Vec<SliHealth>,
    pub incident_summary: IncidentHealthSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggered_sli_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_changes: Vec<HealthRecentChange>,
    pub algorithm_version: String,
    pub model_adjustment_supported: bool,
    pub execution_eligible: bool,
    pub observed_at: DateTime<Utc>,
}

/// Bounded cluster summary used in fleet aggregation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct FleetClusterHealth {
    pub cluster_id: ClusterId,
    pub external_cluster_key: String,
    pub region: String,
    pub score: Option<u8>,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub operational_state: HealthOperationalState,
    pub critical_incidents: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggered_sli_ids: Vec<String>,
    pub observed_at: DateTime<Utc>,
}

/// Tenant/region fleet view whose score and status are determined by the
/// worst cluster rather than an average.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct FleetHealthReport {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub region: Option<String>,
    pub score: Option<u8>,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub worst_cluster_id: Option<ClusterId>,
    pub cluster_count: u32,
    pub healthy_clusters: u32,
    pub degraded_clusters: u32,
    pub critical_clusters: Vec<ClusterId>,
    pub unknown_clusters: Vec<ClusterId>,
    pub maintenance_clusters: Vec<ClusterId>,
    pub fault_drill_clusters: Vec<ClusterId>,
    pub clusters: Vec<FleetClusterHealth>,
    pub aggregation: String,
    pub observed_at: DateTime<Utc>,
}
