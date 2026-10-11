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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    ConsumerLag,
    TopicRoute,
    BrokerSummary,
    ConsumerGroup,
    TopicConfig,
    TopicStats,
    ConsumerProgress,
    BrokerDiagnostics,
    BrokerConfig,
    HaStatus,
    BrokerInventory,
    MessageMetadata,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Healthy,
    Low,
    Medium,
    High,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    Present,
    Missing,
    Unavailable,
    Timeout,
    Unauthorized,
    Invalid,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EvidencePayload {
    ConsumerLag(Value),
    TopicRoute(Value),
    BrokerSummary(Value),
    ConsumerGroup(Value),
    TopicConfig(Value),
    TopicStats(Value),
    ConsumerProgress(Value),
    BrokerDiagnostics(Value),
    BrokerConfig(Value),
    HaStatus(Value),
    BrokerInventory(Value),
    MessageMetadata(Value),
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EvidenceItem {
    pub id: String,
    pub kind: EvidenceKind,
    pub source_tool: String,
    pub observed_at: String,
    pub freshness_ms: u64,
    pub status: EvidenceStatus,
    pub query_hash: String,
    pub payload: Option<EvidencePayload>,
    pub error_code: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EvidenceSnapshot {
    pub evidence_version: String,
    pub query_hash: String,
    pub cluster: String,
    pub target: Value,
    pub observed_at: String,
    pub items: Vec<EvidenceItem>,
}

/// A diagnosis and the evidence it was evaluated on. `evidence_snapshot` is the only place the
/// evidence appears; `evidence_refs`, `missing_evidence` and `root_causes[].evidence_refs` name its
/// items by `id`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DiagnosisReport {
    pub report_type: String,
    pub evidence_version: String,
    pub rules_version: String,
    pub cluster: String,
    pub target: Value,
    pub severity: Severity,
    pub confidence: f32,
    pub confidence_band: ConfidenceBand,
    pub policy_profile: String,
    pub partial: bool,
    pub missing_evidence: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub evidence_snapshot: EvidenceSnapshot,
    pub summary: String,
    pub impacts: Vec<ImpactItem>,
    pub root_causes: Vec<RootCauseCandidate>,
    pub recommendations: Vec<Recommendation>,
    pub risks: Vec<String>,
    pub follow_up_metrics: Vec<MetricWatchItem>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceBand {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ImpactItem {
    pub area: String,
    pub description: String,
    pub severity: Severity,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RootCauseCandidate {
    /// Stable identifier of the rule that drew this conclusion. The wording of `cause` and
    /// `reasoning` may change between rule versions; this identifier does not.
    pub rule_id: String,
    pub cause: String,
    pub confidence: f32,
    pub evidence_refs: Vec<String>,
    pub reasoning: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Recommendation {
    pub action: String,
    pub priority: String,
    pub rationale: String,
    pub risk: String,
    pub verification: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct MetricWatchItem {
    pub name: String,
    pub target: String,
    pub reason: String,
}
