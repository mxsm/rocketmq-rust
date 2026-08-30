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

use crate::ClusterId;
use crate::GovernanceAdmissionId;
use crate::GovernanceArtifactId;
use crate::GovernanceEventId;
use crate::GovernanceVersionId;
use crate::TenantId;

pub const GOVERNANCE_SCHEMA_VERSION: &str = "rocketmq-sre.governance.v1";

/// Closed set of artifacts governed by the lifecycle center.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceObjectKind {
    DataPolicy,
    EvidencePolicy,
    Prompt,
    Knowledge,
    ModelProfile,
    ProviderProfile,
    DiagnosticPack,
    PolicyBundle,
    ActionDescriptor,
    Runbook,
    IntegrationAdapter,
}

/// Unified artifact lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceLifecycleState {
    Draft,
    Review,
    Active,
    Deprecated,
    Quarantined,
    Retired,
}

impl GovernanceLifecycleState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Retired)
    }

    #[must_use]
    pub const fn permits_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Draft, Self::Review | Self::Retired)
                | (
                    Self::Review,
                    Self::Draft | Self::Active | Self::Quarantined | Self::Retired
                )
                | (Self::Active, Self::Deprecated | Self::Quarantined)
                | (Self::Deprecated, Self::Review | Self::Quarantined | Self::Retired)
                | (Self::Quarantined, Self::Review | Self::Retired)
        )
    }
}

/// Stable logical artifact head.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceArtifact {
    pub id: GovernanceArtifactId,
    pub tenant_id: TenantId,
    pub kind: GovernanceObjectKind,
    pub logical_key: String,
    pub owner: String,
    pub reviewer: String,
    pub current_version_id: Option<GovernanceVersionId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Exact dependency on another governed artifact.
#[derive(Clone, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GovernanceDependency {
    pub kind: GovernanceObjectKind,
    pub logical_key: String,
    pub version: String,
}

/// Detached signature stored for a reviewed artifact version.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceSignature {
    pub algorithm: String,
    pub key_id: String,
    pub value: String,
}

/// Immutable content identity plus a human-controlled lifecycle.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceVersion {
    pub id: GovernanceVersionId,
    pub artifact_id: GovernanceArtifactId,
    pub tenant_id: TenantId,
    pub version: String,
    pub content_digest: String,
    pub signature: Option<GovernanceSignature>,
    pub state: GovernanceLifecycleState,
    pub applicable_components: BTreeSet<String>,
    pub applicable_version_range: String,
    pub dependencies: BTreeSet<GovernanceDependency>,
    pub review_due_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub replacement_version_id: Option<GovernanceVersionId>,
    pub rollback_version_id: Option<GovernanceVersionId>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Canonical fields covered by a governance signature.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceSignaturePayload {
    pub schema_version: String,
    pub artifact_id: GovernanceArtifactId,
    pub version_id: GovernanceVersionId,
    pub tenant_id: TenantId,
    pub version: String,
    pub content_digest: String,
    pub applicable_components: BTreeSet<String>,
    pub applicable_version_range: String,
    pub dependencies: BTreeSet<GovernanceDependency>,
    pub review_due_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl From<&GovernanceVersion> for GovernanceSignaturePayload {
    fn from(version: &GovernanceVersion) -> Self {
        Self {
            schema_version: GOVERNANCE_SCHEMA_VERSION.to_owned(),
            artifact_id: version.artifact_id,
            version_id: version.id,
            tenant_id: version.tenant_id,
            version: version.version.clone(),
            content_digest: version.content_digest.clone(),
            applicable_components: version.applicable_components.clone(),
            applicable_version_range: version.applicable_version_range.clone(),
            dependencies: version.dependencies.clone(),
            review_due_at: version.review_due_at,
            expires_at: version.expires_at,
        }
    }
}

/// Object type that currently depends on a governed version.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceImpactKind {
    Cluster,
    DiagnosticPack,
    ActionPlan,
    Action,
    Incident,
    ModelRoute,
    Integration,
}

/// Bounded reverse reference used by the impact view.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceImpact {
    pub version_id: GovernanceVersionId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub kind: GovernanceImpactKind,
    pub reference_id: String,
    pub label: String,
    pub observed_at: DateTime<Utc>,
}

/// Actor class recorded in lifecycle audit. Model actors are never authorized
/// to publish or retire versions.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceActorKind {
    Human,
    Service,
    Model,
}

/// Append-only governance transition event.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceEvent {
    pub id: GovernanceEventId,
    pub tenant_id: TenantId,
    pub artifact_id: GovernanceArtifactId,
    pub version_id: GovernanceVersionId,
    pub from_state: Option<GovernanceLifecycleState>,
    pub to_state: GovernanceLifecycleState,
    pub actor: String,
    pub actor_kind: GovernanceActorKind,
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
}

/// Requested access path for governance admission.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceAccessPath {
    ReadOnly,
    HighPrivilege,
}

/// Persisted fail-closed admission decision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GovernanceAdmission {
    pub id: GovernanceAdmissionId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub access_path: GovernanceAccessPath,
    pub required_version_ids: Vec<GovernanceVersionId>,
    pub allowed: bool,
    pub degraded: bool,
    pub reason_codes: Vec<String>,
    pub evaluated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_governance_version_is_terminal() {
        assert!(GovernanceLifecycleState::Retired.is_terminal());
        assert!(!GovernanceLifecycleState::Retired.permits_transition_to(GovernanceLifecycleState::Review));
    }

    #[test]
    fn active_version_requires_deprecation_or_quarantine_before_retirement() {
        assert!(!GovernanceLifecycleState::Active.permits_transition_to(GovernanceLifecycleState::Retired));
        assert!(GovernanceLifecycleState::Active.permits_transition_to(GovernanceLifecycleState::Deprecated));
        assert!(GovernanceLifecycleState::Active.permits_transition_to(GovernanceLifecycleState::Quarantined));
    }
}
