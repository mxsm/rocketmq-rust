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

use crate::ActionPlanId;
use crate::ApprovalDecision;
use crate::ClusterId;
use crate::IncidentId;
use crate::IntegrationDeliveryId;
use crate::IntegrationTargetId;
use crate::ReleaseId;
use crate::TenantId;

/// Wire family emitted by Phase 3 integration adapters.
pub const INTEGRATION_DELIVERY_SCHEMA_VERSION: &str = "rocketmq-sre.integration-delivery.v1";

/// Closed adapter families supported by the first integration SPI.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationAdapterKind {
    MockItsm,
    SignedWebhookItsm,
    ChatOpsWebhook,
    Pager,
    Email,
}

/// Events an external adapter may receive.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationEventKind {
    PlanSubmitted,
    ApprovalChanged,
    ReleaseStarted,
    ReleasePaused,
    ReleaseRollingBack,
    ReleaseCompleted,
    ManualTakeoverRequired,
}

/// Durable state of an idempotent integration delivery.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationDeliveryStatus {
    Pending,
    Delivering,
    Delivered,
    RetryScheduled,
    Failed,
}

/// Tenant- and cluster-scoped adapter configuration.
///
/// Credentials are represented only by a secret reference and are never part
/// of a delivery, model input, audit detail, or operator-facing view.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationTarget {
    pub id: IntegrationTargetId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub descriptor_id: String,
    pub descriptor_version: String,
    pub name: String,
    pub adapter_kind: IntegrationAdapterKind,
    pub endpoint: String,
    pub secret_reference: Option<String>,
    pub enabled: bool,
    pub inbound_approval: bool,
    pub outbound_events: BTreeSet<IntegrationEventKind>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One bounded outbox record. The idempotency key is unique per target.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationDelivery {
    pub schema_version: String,
    pub id: IntegrationDeliveryId,
    pub target_id: IntegrationTargetId,
    pub descriptor_id: String,
    pub descriptor_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub plan_id: Option<ActionPlanId>,
    pub release_id: Option<ReleaseId>,
    pub event_kind: IntegrationEventKind,
    pub idempotency_key: String,
    pub sanitized_summary: String,
    pub deep_link: String,
    pub status: IntegrationDeliveryStatus,
    pub attempt_count: u16,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error_code: Option<String>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Sanitized ITSM projection bound to the immutable plan hash.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItsmTicketLink {
    pub target_id: IntegrationTargetId,
    pub external_ticket_key: String,
    pub incident_id: IncidentId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub approval_status: String,
    pub sre_url: String,
    pub sanitized_summary: String,
    pub last_synced_at: DateTime<Utc>,
}

/// Provider-neutral inbound approval signal.
///
/// It is only an input to the normal Control Plane approval service. The
/// service must still verify scope, role, step-up, plan hash, expiry, and
/// separation of duties before recording an Approval.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalApprovalInput {
    pub schema_version: String,
    pub target_id: IntegrationTargetId,
    pub external_event_id: String,
    pub external_ticket_key: String,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub decision: ApprovalDecision,
    pub subject: String,
    pub roles: BTreeSet<String>,
    pub mfa_verified: bool,
    pub step_up_verified: bool,
    pub expires_at: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
}
