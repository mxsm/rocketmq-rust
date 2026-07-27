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
use serde_json::Value;

use crate::ClusterId;
use crate::EvidenceId;
use crate::SimulationId;
use crate::TenantId;

/// Supported deterministic Phase 2 what-if families.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimulationKind {
    BrokerOffline,
    ProxyOffline,
    TrafficIncrease,
    BrokerScaleOut,
    ProxyScaleOut,
    TopicQueueExpand,
    VersionUpgrade,
    ConfigurationDiff,
}

/// Lifecycle of a read-only simulation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimulationStatus {
    Completed,
    InsufficientData,
    Unsupported,
}

/// Read-only what-if result. It never carries an executable action.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct WhatIfSimulation {
    pub id: SimulationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub kind: SimulationKind,
    pub status: SimulationStatus,
    pub input: Value,
    pub assumptions: Vec<String>,
    pub projected_utilization: Value,
    pub bottlenecks: Vec<String>,
    pub blast_radius: Vec<String>,
    pub missing_assumptions: Vec<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub algorithm_version: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}
