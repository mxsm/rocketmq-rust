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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ActionPlanId;
use crate::ApprovalDecision;
use crate::ClusterId;
use crate::EnterpriseIntegrationEventId;
use crate::IncidentId;
use crate::IntegrationDeliveryId;
use crate::IntegrationTargetId;
use crate::ReleaseId;
use crate::TenantId;

/// Wire family emitted by Phase 3 integration adapters.
pub const INTEGRATION_DELIVERY_SCHEMA_VERSION: &str = "rocketmq-sre.integration-delivery.v1";
/// Wire family for signed CMDB, GitOps, and CI/CD ingress.
pub const ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION: &str = "rocketmq-sre.enterprise-integration-event.v1";

/// Closed adapter families supported by the first integration SPI.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationAdapterKind {
    MockItsm,
    SignedWebhookItsm,
    ChatOpsWebhook,
    Pager,
    Email,
    MockCmdb,
    MockGitOps,
    SignedReleaseWebhook,
}

/// Data classification declared by an enterprise integration descriptor.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationDataClass {
    #[default]
    AggregatedMetadata,
    OperationalMetadata,
    RestrictedMetadata,
}

/// Bounded retry and health policy attached to one adapter descriptor.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationOperationalPolicy {
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
    #[serde(default)]
    pub data_class: IntegrationDataClass,
    pub rate_limit_per_minute: u32,
    pub timeout_seconds: u16,
    pub max_attempts: u16,
    pub health_check_interval_seconds: u32,
    pub secret_required: bool,
}

impl Default for IntegrationOperationalPolicy {
    fn default() -> Self {
        Self {
            required_scopes: BTreeSet::new(),
            data_class: IntegrationDataClass::AggregatedMetadata,
            rate_limit_per_minute: 60,
            timeout_seconds: 8,
            max_attempts: 5,
            health_check_interval_seconds: 300,
            secret_required: true,
        }
    }
}

/// Runtime health of one configured integration target.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationHealthStatus {
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
    Disabled,
}

/// Signed inbound event families accepted by representative enterprise adapters.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnterpriseIntegrationEventKind {
    CmdbSnapshot,
    GitOpsSnapshot,
    ReleaseStarted,
    ReleaseCanary,
    ReleasePromoted,
    ReleaseRolledBack,
}

/// Sanitized CMDB ownership and dependency projection.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CmdbSnapshot {
    pub cluster_id: ClusterId,
    pub owner: String,
    pub environment: String,
    #[serde(default)]
    pub service_dependencies: BTreeSet<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Sanitized GitOps desired-state projection without repository credentials.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitOpsSnapshot {
    pub cluster_id: ClusterId,
    pub repository_ref: String,
    pub commit_sha: String,
    pub desired_image_digest: Option<String>,
    pub configuration_digest: Option<String>,
    pub feature_digest: Option<String>,
    pub rollout_link: Option<String>,
}

/// Sanitized CI/CD release event that may only start read-only readiness work.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePipelineEvent {
    pub cluster_id: ClusterId,
    pub release_ref: String,
    pub change_id: String,
    pub artifact_digest: String,
    pub target_version: String,
}

/// Closed typed payload set; arbitrary JSON never reaches an adapter handler.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum EnterpriseIntegrationPayload {
    Cmdb(CmdbSnapshot),
    GitOps(GitOpsSnapshot),
    Release(ReleasePipelineEvent),
}

impl EnterpriseIntegrationPayload {
    /// Returns the cluster scope embedded in the typed payload.
    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        match self {
            Self::Cmdb(payload) => payload.cluster_id,
            Self::GitOps(payload) => payload.cluster_id,
            Self::Release(payload) => payload.cluster_id,
        }
    }
}

/// Immutable signed inbound event after replay and scope validation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnterpriseIntegrationEvent {
    pub schema_version: String,
    pub id: EnterpriseIntegrationEventId,
    pub target_id: IntegrationTargetId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub event_kind: EnterpriseIntegrationEventKind,
    pub external_event_id: String,
    pub source_version: String,
    pub payload_digest: String,
    pub payload: EnterpriseIntegrationPayload,
    pub signature_verified: bool,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

/// Latest bounded config/secret/endpoint health observation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationHealth {
    pub target_id: IntegrationTargetId,
    pub status: IntegrationHealthStatus,
    pub config_valid: bool,
    pub secret_available: bool,
    pub endpoint_valid: bool,
    pub last_delivery_at: Option<DateTime<Utc>>,
    pub last_error_code: Option<String>,
    pub observed_at: DateTime<Utc>,
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
