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

use super::*;

pub(super) async fn insert_policy(
    transaction: &mut Transaction<'_, Postgres>,
    decision: &PolicyDecision,
) -> Result<(), ControlPlaneRequestFailure> {
    sqlx::query(
        "INSERT INTO policy_decisions (
            id, tenant_id, cluster_id, plan_id, plan_hash, policy_version,
            input_hash, effect, reason_codes, evaluated_by,
            decision_snapshot, evaluated_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10,
            $11, $12
         )",
    )
    .bind(decision.id.as_uuid())
    .bind(decision.tenant_id.as_uuid())
    .bind(decision.cluster_id.as_uuid())
    .bind(decision.plan_id.as_uuid())
    .bind(&decision.plan_hash)
    .bind(&decision.policy_version)
    .bind(&decision.input_hash)
    .bind(enum_name(&decision.effect)?)
    .bind(&decision.reason_codes)
    .bind(&decision.evaluated_by)
    .bind(json_value(decision)?)
    .bind(decision.evaluated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) async fn insert_audit(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AuditEvent,
) -> Result<(), ControlPlaneRequestFailure> {
    sqlx::query(
        "INSERT INTO audit_events (
            event_id, tenant_id, cluster_id, correlation_id, event_kind,
            actor_subject, actor_role, resource_kind, resource_id,
            reason_code, details, event_snapshot, occurred_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13
         )",
    )
    .bind(event.id.as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(event.correlation_id.as_uuid())
    .bind(enum_name(&event.event_kind)?)
    .bind(&event.actor_subject)
    .bind(&event.actor_role)
    .bind(&event.resource_kind)
    .bind(&event.resource_id)
    .bind(&event.reason_code)
    .bind(&event.details)
    .bind(json_value(event)?)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) fn quarantine_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ResourceQuarantine, ControlPlaneRequestFailure> {
    Ok(ResourceQuarantine {
        id: ResourceQuarantineId::from_uuid(row.try_get("id")?),
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        resource_key: row.try_get("resource_key")?,
        action_id: row.try_get("action_id")?,
        reason_code: row.try_get("reason_code")?,
        source_execution_id: row
            .try_get::<Option<Uuid>, _>("source_execution_id")?
            .map(ExecutionId::from_uuid),
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        cleared_by: row.try_get("cleared_by")?,
        clear_reason: row.try_get("clear_reason")?,
        clear_evidence_ids: row
            .try_get::<Vec<Uuid>, _>("clear_evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        cleared_at: row.try_get("cleared_at")?,
    })
}

pub(super) fn json_value(value: &impl Serialize) -> Result<Value, ControlPlaneRequestFailure> {
    serde_json::to_value(value)
        .map_err(|source| ControlPlaneRequestFailure::from(ControlPlaneError::configuration_source(source)))
}

pub(super) fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, ControlPlaneRequestFailure> {
    serde_json::from_value(value)
        .map_err(|source| ControlPlaneRequestFailure::from(ControlPlaneError::configuration_source(source)))
}

pub(super) fn enum_name(value: &impl Serialize) -> Result<String, ControlPlaneRequestFailure> {
    serde_json::to_value(value)
        .map_err(|source| ControlPlaneRequestFailure::from(ControlPlaneError::configuration_source(source)))?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| ControlPlaneRequestFailure::configuration("enum did not encode as a string"))
}

pub(super) fn risk_name(risk: ActionRisk) -> Result<&'static str, ControlPlaneRequestFailure> {
    match risk {
        ActionRisk::R1 => Ok("r1"),
        ActionRisk::R2 => Ok("r2"),
        ActionRisk::Read | ActionRisk::Plan | ActionRisk::R3 => Err(ControlPlaneRequestFailure::validation(
            "action_not_executable",
            "only R1 and R2 plans may be persisted",
        )),
    }
}

pub(super) fn parse_risk(value: &str) -> Result<ActionRisk, ControlPlaneRequestFailure> {
    match value {
        "r1" => Ok(ActionRisk::R1),
        "r2" => Ok(ActionRisk::R2),
        _ => Err(ControlPlaneRequestFailure::configuration(
            "stored plan risk is unsupported",
        )),
    }
}

pub(super) const fn plan_status_name(status: PlanStatus) -> &'static str {
    match status {
        PlanStatus::Draft => "draft",
        PlanStatus::NeedsCritic => "needs_critic",
        PlanStatus::ReadyForApproval => "ready_for_approval",
        PlanStatus::InReview => "in_review",
        PlanStatus::Approved => "approved",
        PlanStatus::Rejected => "rejected",
        PlanStatus::Expired => "expired",
        PlanStatus::Superseded => "superseded",
    }
}

pub(super) fn parse_plan_status(value: &str) -> Result<PlanStatus, ControlPlaneRequestFailure> {
    match value {
        "draft" => Ok(PlanStatus::Draft),
        "needs_critic" => Ok(PlanStatus::NeedsCritic),
        "ready_for_approval" => Ok(PlanStatus::ReadyForApproval),
        "in_review" => Ok(PlanStatus::InReview),
        "approved" => Ok(PlanStatus::Approved),
        "rejected" => Ok(PlanStatus::Rejected),
        "expired" => Ok(PlanStatus::Expired),
        "superseded" => Ok(PlanStatus::Superseded),
        _ => Err(ControlPlaneRequestFailure::configuration(
            "stored plan status is unsupported",
        )),
    }
}

pub(super) fn parse_execution_state(value: &str) -> Result<ExecutionState, ControlPlaneRequestFailure> {
    match value {
        "pending" => Ok(ExecutionState::Pending),
        "prechecking" => Ok(ExecutionState::Prechecking),
        "intent_persisted" => Ok(ExecutionState::IntentPersisted),
        "applying" => Ok(ExecutionState::Applying),
        "unknown" => Ok(ExecutionState::Unknown),
        "reconciling" => Ok(ExecutionState::Reconciling),
        "verifying" => Ok(ExecutionState::Verifying),
        "compensating" => Ok(ExecutionState::Compensating),
        "succeeded" => Ok(ExecutionState::Succeeded),
        "rolled_back" => Ok(ExecutionState::RolledBack),
        "escalated" => Ok(ExecutionState::Escalated),
        _ => Err(ControlPlaneRequestFailure::configuration(
            "stored execution state is unsupported",
        )),
    }
}

pub(super) const fn approval_decision_name(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::Approved => "approved",
        ApprovalDecision::Rejected => "rejected",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_enum_decoders_fail_closed() {
        assert_eq!(parse_risk("r1").expect("risk"), ActionRisk::R1);
        assert_eq!(
            parse_execution_state("reconciling").expect("state"),
            ExecutionState::Reconciling
        );
        assert!(parse_risk("r3").is_err());
        assert!(parse_plan_status("automatic").is_err());
        assert!(parse_execution_state("retrying_write").is_err());
    }

    #[test]
    fn new_quarantine_audit_kind_has_a_stable_wire_name() {
        assert_eq!(
            enum_name(&rocketmq_sre_contracts::AuditEventKind::QuarantineClearRequested).expect("enum"),
            "quarantine_clear_requested"
        );
    }
}
