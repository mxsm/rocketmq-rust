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
use rocketmq_sre_contracts::ChangeConflict;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ChangeWindowKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::RunbookStepId;
use rocketmq_sre_contracts::RunbookStepPlanBinding;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateRunbookRequest {
    pub(super) cluster_id: ClusterId,
    pub(super) definition: RunbookDefinition,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct RunbookListQuery {
    pub(super) cluster_id: ClusterId,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct RunbookGetQuery {
    pub(super) cluster_id: ClusterId,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct RunbookPage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<RunbookDefinition>,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateChangeWindowRequest {
    pub(super) cluster_id: ClusterId,
    pub(super) name: String,
    pub(super) kind: ChangeWindowKind,
    pub(super) timezone: String,
    pub(super) starts_at: DateTime<Utc>,
    pub(super) ends_at: DateTime<Utc>,
    #[serde(default)]
    pub(super) resource_keys: BTreeSet<String>,
    pub(super) max_parallelism: u16,
    pub(super) reason: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct ChangeWindowListQuery {
    pub(super) cluster_id: ClusterId,
    pub(super) from: DateTime<Utc>,
    pub(super) to: DateTime<Utc>,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ChangeWindowPage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<ChangeWindow>,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateChangeScheduleRequest {
    pub(super) cluster_id: ClusterId,
    pub(super) runbook_id: RunbookId,
    pub(super) runbook_version: String,
    pub(super) scheduled_start: DateTime<Utc>,
    pub(super) scheduled_end: DateTime<Utc>,
    pub(super) plan_bindings: Vec<RunbookStepPlanBinding>,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct ChangeScheduleListQuery {
    pub(super) cluster_id: ClusterId,
    pub(super) status: Option<ChangeScheduleStatus>,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ChangeSchedulePage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<ChangeSchedule>,
    pub(super) partial: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ChangeSchedulePreview {
    pub(super) schema_version: &'static str,
    pub(super) schedule: ChangeSchedule,
    pub(super) conflicts: Vec<ChangeConflict>,
    pub(super) schedulable: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScheduleTransitionRequest {
    pub(super) reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManualGateDecisionRequest {
    pub(super) reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ManualGateDecision {
    Approved,
    Rejected,
}

impl ManualGateDecision {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ManualGateDecisionRecord {
    pub(super) id: Uuid,
    pub(super) schedule_id: rocketmq_sre_contracts::ChangeScheduleId,
    pub(super) step_id: RunbookStepId,
    pub(super) decision: ManualGateDecision,
    pub(super) actor_subject: String,
    pub(super) actor_role: String,
    pub(super) reason: String,
    pub(super) occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(super) struct ScheduleEvent {
    pub(super) id: Uuid,
    pub(super) schedule_id: rocketmq_sre_contracts::ChangeScheduleId,
    pub(super) correlation_id: CorrelationId,
    pub(super) from_status: Option<ChangeScheduleStatus>,
    pub(super) to_status: ChangeScheduleStatus,
    pub(super) reason_code: String,
    pub(super) actor_subject: String,
    pub(super) details: Value,
    pub(super) occurred_at: DateTime<Utc>,
}
