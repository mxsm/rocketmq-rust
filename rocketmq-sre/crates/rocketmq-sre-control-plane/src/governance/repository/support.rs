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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::GovernanceAccessPath;
use rocketmq_sre_contracts::GovernanceAdmission;
use rocketmq_sre_contracts::GovernanceAdmissionId;
use rocketmq_sre_contracts::GovernanceActorKind;
use rocketmq_sre_contracts::GovernanceArtifact;
use rocketmq_sre_contracts::GovernanceArtifactId;
use rocketmq_sre_contracts::GovernanceDependency;
use rocketmq_sre_contracts::GovernanceEvent;
use rocketmq_sre_contracts::GovernanceEventId;
use rocketmq_sre_contracts::GovernanceImpact;
use rocketmq_sre_contracts::GovernanceImpactKind;
use rocketmq_sre_contracts::GovernanceLifecycleState;
use rocketmq_sre_contracts::GovernanceObjectKind;
use rocketmq_sre_contracts::GovernanceSignature;
use rocketmq_sre_contracts::GovernanceVersion;
use rocketmq_sre_contracts::GovernanceVersionId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;

pub(super) fn artifact_from_row(row: &PgRow) -> Result<GovernanceArtifact, ControlPlaneError> {
    Ok(GovernanceArtifact {
        id: GovernanceArtifactId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        kind: object_kind(row.try_get("object_kind")?)?,
        logical_key: row.try_get("logical_key")?,
        owner: row.try_get("owner_name")?,
        reviewer: row.try_get("reviewer_name")?,
        current_version_id: row
            .try_get::<Option<Uuid>, _>("current_version_id")?
            .map(GovernanceVersionId::from_uuid),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn version_from_row(row: &PgRow) -> Result<GovernanceVersion, ControlPlaneError> {
    let algorithm = row.try_get::<Option<String>, _>("signature_algorithm")?;
    let key_id = row.try_get::<Option<String>, _>("signing_key_id")?;
    let value = row.try_get::<Option<String>, _>("signature_value")?;
    let signature = match (algorithm, key_id, value) {
        (Some(algorithm), Some(key_id), Some(value)) => Some(GovernanceSignature {
            algorithm,
            key_id,
            value,
        }),
        (None, None, None) => None,
        _ => {
            return Err(ControlPlaneError::validation(
                "invalid_persisted_governance_state",
                "governance signature metadata is incomplete",
            ));
        }
    };
    Ok(GovernanceVersion {
        id: GovernanceVersionId::from_uuid(row.try_get("id")?),
        artifact_id: GovernanceArtifactId::from_uuid(row.try_get("artifact_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        version: row.try_get("version_name")?,
        content_digest: row.try_get("content_digest")?,
        signature,
        state: lifecycle_state(row.try_get("lifecycle_state")?)?,
        applicable_components: serde_json::from_value(row.try_get("applicable_components")?).map_err(|_| {
            ControlPlaneError::validation(
                "invalid_persisted_governance_state",
                "applicable components are invalid",
            )
        })?,
        applicable_version_range: row.try_get("applicable_version_range")?,
        dependencies: serde_json::from_value(row.try_get("dependencies")?).map_err(|_| {
            ControlPlaneError::validation("invalid_persisted_governance_state", "governance dependencies are invalid")
        })?,
        review_due_at: row.try_get("review_due_at")?,
        expires_at: row.try_get("expires_at")?,
        replacement_version_id: row
            .try_get::<Option<Uuid>, _>("replacement_version_id")?
            .map(GovernanceVersionId::from_uuid),
        rollback_version_id: row
            .try_get::<Option<Uuid>, _>("rollback_version_id")?
            .map(GovernanceVersionId::from_uuid),
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn impact_from_row(row: &PgRow) -> Result<GovernanceImpact, ControlPlaneError> {
    Ok(GovernanceImpact {
        version_id: GovernanceVersionId::from_uuid(row.try_get("version_id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        kind: impact_kind(row.try_get("impact_kind")?)?,
        reference_id: row.try_get("reference_id")?,
        label: row.try_get("label")?,
        observed_at: row.try_get("observed_at")?,
    })
}

pub(super) fn event_from_row(row: &PgRow) -> Result<GovernanceEvent, ControlPlaneError> {
    Ok(GovernanceEvent {
        id: GovernanceEventId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        artifact_id: GovernanceArtifactId::from_uuid(row.try_get("artifact_id")?),
        version_id: GovernanceVersionId::from_uuid(row.try_get("version_id")?),
        from_state: row
            .try_get::<Option<String>, _>("from_state")?
            .as_deref()
            .map(lifecycle_state)
            .transpose()?,
        to_state: lifecycle_state(row.try_get("to_state")?)?,
        actor: row.try_get("actor_name")?,
        actor_kind: actor_kind(row.try_get("actor_kind")?)?,
        reason: row.try_get("reason")?,
        occurred_at: row.try_get("occurred_at")?,
    })
}

pub(super) fn admission_from_row(row: &PgRow) -> Result<GovernanceAdmission, ControlPlaneError> {
    Ok(GovernanceAdmission {
        id: GovernanceAdmissionId::from_uuid(row.try_get("id")?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        access_path: access_path(row.try_get("access_path")?)?,
        required_version_ids: row
            .try_get::<Vec<Uuid>, _>("required_version_ids")?
            .into_iter()
            .map(GovernanceVersionId::from_uuid)
            .collect(),
        allowed: row.try_get("allowed")?,
        degraded: row.try_get("degraded")?,
        reason_codes: row.try_get("reason_codes")?,
        evaluated_at: row.try_get("evaluated_at")?,
    })
}

pub(super) const fn object_kind_name(value: GovernanceObjectKind) -> &'static str {
    match value {
        GovernanceObjectKind::DataPolicy => "data_policy",
        GovernanceObjectKind::EvidencePolicy => "evidence_policy",
        GovernanceObjectKind::Prompt => "prompt",
        GovernanceObjectKind::Knowledge => "knowledge",
        GovernanceObjectKind::ModelProfile => "model_profile",
        GovernanceObjectKind::ProviderProfile => "provider_profile",
        GovernanceObjectKind::DiagnosticPack => "diagnostic_pack",
        GovernanceObjectKind::PolicyBundle => "policy_bundle",
        GovernanceObjectKind::ActionDescriptor => "action_descriptor",
        GovernanceObjectKind::Runbook => "runbook",
        GovernanceObjectKind::IntegrationAdapter => "integration_adapter",
    }
}

pub(super) const fn lifecycle_state_name(value: GovernanceLifecycleState) -> &'static str {
    match value {
        GovernanceLifecycleState::Draft => "draft",
        GovernanceLifecycleState::Review => "review",
        GovernanceLifecycleState::Active => "active",
        GovernanceLifecycleState::Deprecated => "deprecated",
        GovernanceLifecycleState::Quarantined => "quarantined",
        GovernanceLifecycleState::Retired => "retired",
    }
}

pub(super) const fn impact_kind_name(value: GovernanceImpactKind) -> &'static str {
    match value {
        GovernanceImpactKind::Cluster => "cluster",
        GovernanceImpactKind::DiagnosticPack => "diagnostic_pack",
        GovernanceImpactKind::ActionPlan => "action_plan",
        GovernanceImpactKind::Action => "action",
        GovernanceImpactKind::Incident => "incident",
        GovernanceImpactKind::ModelRoute => "model_route",
        GovernanceImpactKind::Integration => "integration",
    }
}

pub(super) const fn actor_kind_name(value: GovernanceActorKind) -> &'static str {
    match value {
        GovernanceActorKind::Human => "human",
        GovernanceActorKind::Service => "service",
        GovernanceActorKind::Model => "model",
    }
}

pub(super) const fn access_path_name(value: GovernanceAccessPath) -> &'static str {
    match value {
        GovernanceAccessPath::ReadOnly => "read_only",
        GovernanceAccessPath::HighPrivilege => "high_privilege",
    }
}

fn object_kind(value: &str) -> Result<GovernanceObjectKind, ControlPlaneError> {
    match value {
        "data_policy" => Ok(GovernanceObjectKind::DataPolicy),
        "evidence_policy" => Ok(GovernanceObjectKind::EvidencePolicy),
        "prompt" => Ok(GovernanceObjectKind::Prompt),
        "knowledge" => Ok(GovernanceObjectKind::Knowledge),
        "model_profile" => Ok(GovernanceObjectKind::ModelProfile),
        "provider_profile" => Ok(GovernanceObjectKind::ProviderProfile),
        "diagnostic_pack" => Ok(GovernanceObjectKind::DiagnosticPack),
        "policy_bundle" => Ok(GovernanceObjectKind::PolicyBundle),
        "action_descriptor" => Ok(GovernanceObjectKind::ActionDescriptor),
        "runbook" => Ok(GovernanceObjectKind::Runbook),
        "integration_adapter" => Ok(GovernanceObjectKind::IntegrationAdapter),
        _ => invalid("governance object kind"),
    }
}

fn lifecycle_state(value: &str) -> Result<GovernanceLifecycleState, ControlPlaneError> {
    match value {
        "draft" => Ok(GovernanceLifecycleState::Draft),
        "review" => Ok(GovernanceLifecycleState::Review),
        "active" => Ok(GovernanceLifecycleState::Active),
        "deprecated" => Ok(GovernanceLifecycleState::Deprecated),
        "quarantined" => Ok(GovernanceLifecycleState::Quarantined),
        "retired" => Ok(GovernanceLifecycleState::Retired),
        _ => invalid("governance lifecycle state"),
    }
}

fn impact_kind(value: &str) -> Result<GovernanceImpactKind, ControlPlaneError> {
    match value {
        "cluster" => Ok(GovernanceImpactKind::Cluster),
        "diagnostic_pack" => Ok(GovernanceImpactKind::DiagnosticPack),
        "action_plan" => Ok(GovernanceImpactKind::ActionPlan),
        "action" => Ok(GovernanceImpactKind::Action),
        "incident" => Ok(GovernanceImpactKind::Incident),
        "model_route" => Ok(GovernanceImpactKind::ModelRoute),
        "integration" => Ok(GovernanceImpactKind::Integration),
        _ => invalid("governance impact kind"),
    }
}

fn actor_kind(value: &str) -> Result<GovernanceActorKind, ControlPlaneError> {
    match value {
        "human" => Ok(GovernanceActorKind::Human),
        "service" => Ok(GovernanceActorKind::Service),
        "model" => Ok(GovernanceActorKind::Model),
        _ => invalid("governance actor kind"),
    }
}

fn access_path(value: &str) -> Result<GovernanceAccessPath, ControlPlaneError> {
    match value {
        "read_only" => Ok(GovernanceAccessPath::ReadOnly),
        "high_privilege" => Ok(GovernanceAccessPath::HighPrivilege),
        _ => invalid("governance access path"),
    }
}

fn invalid<T>(name: &str) -> Result<T, ControlPlaneError> {
    Err(ControlPlaneError::validation(
        "invalid_persisted_governance_state",
        format!("{name} is not recognized"),
    ))
}

pub(super) fn dependency_value(
    dependencies: &BTreeSet<GovernanceDependency>,
) -> Result<serde_json::Value, ControlPlaneError> {
    serde_json::to_value(dependencies)
        .map_err(|_| ControlPlaneError::validation("invalid_governance_version", "dependencies cannot be encoded"))
}
