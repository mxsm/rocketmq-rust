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
pub enum Severity {
    Healthy,
    Low,
    Medium,
    High,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum EvidenceStatus {
    Present,
    Missing,
    Error,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DiagnosisReport {
    pub report_type: String,
    pub cluster: String,
    pub target: Value,
    pub severity: Severity,
    pub confidence: f32,
    pub summary: String,
    pub impacts: Vec<ImpactItem>,
    pub evidences: Vec<Evidence>,
    pub root_causes: Vec<RootCauseCandidate>,
    pub recommendations: Vec<Recommendation>,
    pub risks: Vec<String>,
    pub follow_up_metrics: Vec<MetricWatchItem>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ImpactItem {
    pub area: String,
    pub description: String,
    pub severity: Severity,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Evidence {
    pub id: String,
    pub source_tool: String,
    pub status: EvidenceStatus,
    pub summary: String,
    pub data: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RootCauseCandidate {
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
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct MetricWatchItem {
    pub name: String,
    pub target: String,
    pub reason: String,
}
