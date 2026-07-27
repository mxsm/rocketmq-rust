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

use crate::AuditEventId;
use crate::ClusterId;
use crate::CorrelationId;
use crate::TenantId;

/// Append-only audit event kind for the supervised change timeline.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventKind {
    PlanCreated,
    PlanSubmitted,
    PolicyEvaluated,
    CriticReviewed,
    Approved,
    Rejected,
    ExecutionSubmitted,
    StateChanged,
    StepIntentPersisted,
    StepResultPersisted,
    QuarantineCreated,
    QuarantineCleared,
    Cancelled,
}

/// Sanitized append-only audit event.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub event_kind: AuditEventKind,
    pub actor_subject: String,
    pub actor_role: String,
    pub resource_kind: String,
    pub resource_id: String,
    pub reason_code: String,
    pub details: Value,
    pub occurred_at: DateTime<Utc>,
}
