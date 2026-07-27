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

use crate::ActionPlanId;
use crate::ApprovalId;
use crate::ClusterId;
use crate::TenantId;

/// Human approval outcome.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approved,
    Rejected,
}

/// Append-only human decision bound to one exact plan hash.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRecord {
    pub id: ApprovalId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub requester_subject: String,
    pub approver_subject: String,
    pub approver_role: String,
    pub decision: ApprovalDecision,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Short-lived Control Plane grant accepted only by the Executor audience.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalGrant {
    pub issuer: String,
    pub audience: String,
    pub approval_id: ApprovalId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub precondition_hash: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub approver_subject: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}
