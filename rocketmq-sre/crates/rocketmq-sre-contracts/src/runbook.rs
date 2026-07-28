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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ActionRisk;
use crate::ChangeScheduleId;
use crate::ChangeWindowId;
use crate::ClusterId;
use crate::ExecutionAction;
use crate::RunbookId;
use crate::RunbookStepId;
use crate::TenantId;

/// Deterministic condition operator available to a runbook step.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunbookConditionOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    Exists,
}

/// Evidence or schedule fact required before a step may start.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunbookCondition {
    pub fact: String,
    pub operator: RunbookConditionOperator,
    pub expected: Option<Value>,
}

/// Explicit human gate inside a composite runbook.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualGate {
    pub gate_id: String,
    pub title: String,
    pub instructions: String,
    pub required_role: String,
    pub timeout_seconds: u64,
}

/// Closed body of one runbook step.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunbookStepBody {
    Action {
        action: ExecutionAction,
        descriptor_version: String,
        resource: String,
        parameters: Value,
    },
    ManualGate {
        gate: ManualGate,
    },
}

/// One dependency-aware runbook step.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunbookStep {
    pub id: RunbookStepId,
    pub sequence: u16,
    pub name: String,
    #[serde(default)]
    pub depends_on: BTreeSet<RunbookStepId>,
    pub parallel_group: Option<String>,
    pub condition: Option<RunbookCondition>,
    pub body: RunbookStepBody,
}

/// Trigger for a compensation edge.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompensationTrigger {
    VerificationFailed,
    StepFailed,
    CancelAfterIntent,
}

/// Directed compensation relationship inside one runbook.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompensationEdge {
    pub from_step: RunbookStepId,
    pub compensation_step: RunbookStepId,
    pub trigger: CompensationTrigger,
}

/// Versioned composite change definition.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunbookDefinition {
    pub schema_version: String,
    pub id: RunbookId,
    pub name: String,
    pub version: String,
    pub owner: String,
    pub description: String,
    pub risk: ActionRisk,
    pub max_parallelism: u16,
    pub steps: Vec<RunbookStep>,
    #[serde(default)]
    pub compensation_edges: Vec<CompensationEdge>,
    pub created_at: DateTime<Utc>,
}

impl RunbookDefinition {
    pub const SCHEMA_VERSION: &'static str = "rocketmq-sre.runbook-definition.v1";
}

/// Calendar window semantics.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeWindowKind {
    Maintenance,
    Freeze,
    Blackout,
}

/// One absolute maintenance, freeze, or blackout interval.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeWindow {
    pub schema_version: String,
    pub id: ChangeWindowId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub name: String,
    pub kind: ChangeWindowKind,
    pub timezone: String,
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    #[serde(default)]
    pub resource_keys: BTreeSet<String>,
    pub max_parallelism: u16,
    pub reason: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

impl ChangeWindow {
    pub const SCHEMA_VERSION: &'static str = "rocketmq-sre.change-window.v1";
}

/// Durable scheduler lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeScheduleStatus {
    Scheduled,
    Running,
    Paused,
    SafeStopping,
    Reconciling,
    Completed,
    Cancelled,
    Rejected,
}

/// Scheduled runbook execution.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeSchedule {
    pub schema_version: String,
    pub id: ChangeScheduleId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub runbook_id: RunbookId,
    pub runbook_version: String,
    pub scheduled_start: DateTime<Utc>,
    pub scheduled_end: DateTime<Utc>,
    #[serde(default)]
    pub resource_keys: BTreeSet<String>,
    pub status: ChangeScheduleStatus,
    pub intent_persisted: bool,
    pub next_step_sequence: u16,
    pub pause_requested_at: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ChangeSchedule {
    pub const SCHEMA_VERSION: &'static str = "rocketmq-sre.change-schedule.v1";
}

/// Stable conflict classification returned by the scheduler.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeConflictCode {
    OutsideMaintenanceWindow,
    FreezeWindow,
    BlackoutWindow,
    ResourceOverlap,
    ParallelismExceeded,
}

/// One blocking calendar or schedule conflict.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeConflict {
    pub code: ChangeConflictCode,
    pub message: String,
    pub resource_key: Option<String>,
    pub window_id: Option<ChangeWindowId>,
    pub conflicting_schedule_id: Option<ChangeScheduleId>,
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub blocking: bool,
}
