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

pub(super) fn validate_candidate_steps(steps: &[CandidatePlanStep]) -> Result<(), ControlPlaneError> {
    if steps.is_empty() || steps.len() > MAX_PLAN_STEPS {
        return Err(ControlPlaneError::validation(
            "invalid_plan",
            "plan must contain between one and sixteen steps",
        ));
    }
    for step in steps {
        if step.action_id.trim().is_empty()
            || step.descriptor_version.trim().is_empty()
            || step.resource.trim().is_empty()
            || step.resource.chars().count() > 512
            || step.resource.chars().any(char::is_control)
            || step.evidence_ids.is_empty()
            || step.evidence_ids.len() > MAX_STEP_EVIDENCE
        {
            return Err(ControlPlaneError::validation(
                "invalid_plan_step",
                "every step requires bounded action, version, resource, parameters, and Evidence IDs",
            ));
        }
    }
    Ok(())
}

pub(super) fn single_manual_action<'a>(
    resolved: &'a [CatalogResolution<'a>],
) -> Result<Option<&'a ManualAction>, ControlPlaneError> {
    let manual = resolved
        .iter()
        .filter_map(|entry| match entry {
            CatalogResolution::ManualOnly(manual) => Some(*manual),
            CatalogResolution::Supervised(_, _) => None,
        })
        .collect::<Vec<_>>();
    if manual.is_empty() {
        return Ok(None);
    }
    if resolved.len() != 1 {
        return Err(ControlPlaneError::validation(
            "mixed_manual_and_execution_plan",
            "R3 manual-only action must be emitted as a standalone runbook",
        ));
    }
    Ok(manual.first().copied())
}

pub(super) fn manual_runbook(
    auth: &AuthContext,
    request: &CreatePlanRequest,
    action_id: &str,
    title: &str,
    reason_code: &str,
    instruction: &str,
) -> ManualRunbookDraft {
    ManualRunbookDraft {
        schema_version: "rocketmq-sre.manual-runbook.v1".to_owned(),
        tenant_id: auth.tenant_id,
        cluster_id: request.cluster_id,
        incident_id: request.incident_id,
        diagnosis_revision: request.diagnosis_revision_id,
        title: title.to_owned(),
        reason_code: reason_code.to_owned(),
        action_id: action_id.to_owned(),
        instructions: vec![
            instruction.to_owned(),
            "Follow the existing human change process; AI SRE execution is disabled.".to_owned(),
        ],
        execution_supported: false,
    }
}

pub(super) fn evidence_hash(evidence: &BTreeMap<EvidenceId, EvidenceSnapshot>) -> Result<String, ControlPlaneError> {
    let bindings = evidence.values().map(evidence_binding).collect::<Vec<_>>();
    canonical_sha256(&bindings)
        .map_err(|error| ControlPlaneError::validation("invalid_evidence_hash", error.to_string()))
}

pub(super) fn step_precondition_hash(
    ids: &[EvidenceId],
    evidence: &BTreeMap<EvidenceId, EvidenceSnapshot>,
) -> Result<String, ControlPlaneError> {
    let mut bindings = ids
        .iter()
        .map(|id| {
            evidence.get(id).map(evidence_binding).ok_or_else(|| {
                ControlPlaneError::validation("invalid_evidence_binding", "plan step references unknown Evidence")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    bindings.sort_by_key(|binding| binding.evidence_id);
    canonical_precondition_hash(&bindings)
        .map_err(|error| ControlPlaneError::validation("invalid_precondition_hash", error.to_string()))
}

pub(super) fn evidence_binding(snapshot: &EvidenceSnapshot) -> EvidenceBinding {
    EvidenceBinding {
        evidence_id: snapshot.evidence_id,
        content_hash: snapshot.content_hash.clone(),
        resource: snapshot.resource.clone(),
        observed_at: snapshot.observed_at,
    }
}

pub(super) fn aggregate_risk(risks: &[ActionRisk]) -> Result<ActionRisk, ControlPlaneError> {
    if risks.contains(&ActionRisk::R2) {
        Ok(ActionRisk::R2)
    } else if risks.iter().all(|risk| *risk == ActionRisk::R1) && !risks.is_empty() {
        Ok(ActionRisk::R1)
    } else {
        Err(ControlPlaneError::validation(
            "unsupported_execution_risk",
            "supervised plan contains a non-executable risk",
        ))
    }
}

pub(super) fn validated_plan_expiry(
    now: DateTime<Utc>,
    requested: Option<DateTime<Utc>>,
    max_ttl_seconds: u64,
) -> Result<DateTime<Utc>, ControlPlaneError> {
    let maximum = now + duration_seconds(max_ttl_seconds)?;
    let expires_at = requested.unwrap_or(maximum);
    if expires_at <= now || expires_at > maximum {
        return Err(ControlPlaneError::validation(
            "invalid_plan_window",
            "plan expiry must be in the future and within the policy TTL",
        ));
    }
    Ok(expires_at)
}

pub(super) fn duration_seconds(seconds: u64) -> Result<Duration, ControlPlaneError> {
    i64::try_from(seconds)
        .map(Duration::seconds)
        .map_err(|_| ControlPlaneError::validation("invalid_time_window", "time window exceeds i64 seconds"))
}

pub(super) fn ensure_live_ready(facts: PolicyFacts) -> Result<(), ControlPlaneError> {
    if !facts.evidence_current {
        return Err(ControlPlaneError::conflict_code(
            "evidence_missing_or_stale",
            "plan Evidence is no longer current and complete",
        ));
    }
    if facts.resource_quarantined {
        return Err(ControlPlaneError::conflict_code(
            RESOURCE_QUARANTINED,
            "target resource is quarantined",
        ));
    }
    if facts.resource_busy {
        return Err(ControlPlaneError::conflict_code(
            "resource_change_in_progress",
            "target resource already has an active change",
        ));
    }
    if !facts.maintenance_window_open {
        return Err(ControlPlaneError::conflict_code(
            "maintenance_window_closed",
            "current time is outside the configured maintenance window",
        ));
    }
    if !facts.rollback_available {
        return Err(ControlPlaneError::conflict_code(
            "rollback_unavailable",
            "plan has no descriptor-defined compensation path",
        ));
    }
    Ok(())
}

pub(super) fn validate_reason(reason: &str) -> Result<(), ControlPlaneError> {
    let length = reason.trim().chars().count();
    if !(1..=2_048).contains(&length) || reason.chars().any(char::is_control) {
        return Err(ControlPlaneError::validation(
            "invalid_reason",
            "reason must contain between one and 2048 printable characters",
        ));
    }
    Ok(())
}

pub(super) fn validate_idempotency_key(value: &str) -> Result<(), ControlPlaneError> {
    let length = value.chars().count();
    if !(16..=200).contains(&length)
        || value
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | ':' | '.')))
    {
        return Err(ControlPlaneError::validation(
            "invalid_idempotency_key",
            "idempotency key must contain 16 to 200 allowlisted ASCII characters",
        ));
    }
    Ok(())
}

pub(super) fn require_cluster(
    auth: &AuthContext,
    cluster_id: rocketmq_sre_contracts::ClusterId,
) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

pub(super) fn plan_creation_audits(
    auth: &AuthContext,
    plan: &ActionPlan,
    decision: &rocketmq_sre_contracts::PolicyDecision,
    correlation_id: CorrelationId,
    now: DateTime<Utc>,
) -> Vec<AuditEvent> {
    let mut audits = vec![
        audit_event(
            auth,
            plan.cluster_id,
            correlation_id,
            AuditEventKind::PlanCreated,
            "operator",
            "action_plan",
            plan.id.to_string(),
            "PlanCreatedFromConfirmedDiagnosis",
            json!({"plan_hash": plan.plan_hash, "version": plan.version}),
            now,
        ),
        audit_event(
            auth,
            plan.cluster_id,
            correlation_id,
            AuditEventKind::PolicyEvaluated,
            "operator",
            "action_plan",
            plan.id.to_string(),
            "DeterministicPolicyEvaluated",
            json!({
                "effect": decision.effect,
                "reason_codes": decision.reason_codes,
                "policy_version": decision.policy_version,
            }),
            now,
        ),
    ];
    let (kind, reason) = if decision.effect == PolicyEffect::RequireApproval {
        (AuditEventKind::PlanSubmitted, "PlanSubmittedForHumanReview")
    } else {
        (AuditEventKind::Rejected, "PlanRejectedByDeterministicPolicy")
    };
    audits.push(audit_event(
        auth,
        plan.cluster_id,
        correlation_id,
        kind,
        "operator",
        "action_plan",
        plan.id.to_string(),
        reason,
        json!({"status": plan.status, "plan_hash": plan.plan_hash}),
        now,
    ));
    audits
}

pub(super) fn approval_audits(
    auth: &AuthContext,
    plan: &ActionPlan,
    decision: ApprovalDecision,
    correlation_id: CorrelationId,
    now: DateTime<Utc>,
) -> Vec<AuditEvent> {
    let (kind, reason, status) = match decision {
        ApprovalDecision::Approved => (AuditEventKind::Approved, "HumanApprovalGranted", PlanStatus::Approved),
        ApprovalDecision::Rejected => (AuditEventKind::Rejected, "HumanApprovalRejected", PlanStatus::Rejected),
    };
    vec![
        audit_event(
            auth,
            plan.cluster_id,
            correlation_id,
            kind,
            "approver",
            "action_plan",
            plan.id.to_string(),
            reason,
            json!({"plan_hash": plan.plan_hash}),
            now,
        ),
        audit_event(
            auth,
            plan.cluster_id,
            correlation_id,
            AuditEventKind::StateChanged,
            "approver",
            "action_plan",
            plan.id.to_string(),
            "PlanReviewStateChanged",
            json!({"to": status, "plan_hash": plan.plan_hash}),
            now,
        ),
    ]
}

#[allow(
    clippy::too_many_arguments,
    reason = "audit fields are intentionally explicit to prevent implicit scope or actor defaults"
)]
pub(super) fn audit_event(
    auth: &AuthContext,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    correlation_id: CorrelationId,
    kind: AuditEventKind,
    actor_role: &str,
    resource_kind: &str,
    resource_id: String,
    reason_code: &str,
    details: serde_json::Value,
    occurred_at: DateTime<Utc>,
) -> AuditEvent {
    AuditEvent {
        id: AuditEventId::new(),
        tenant_id: auth.tenant_id,
        cluster_id,
        correlation_id,
        event_kind: kind,
        actor_subject: auth.subject.clone(),
        actor_role: actor_role.to_owned(),
        resource_kind: resource_kind.to_owned(),
        resource_id,
        reason_code: reason_code.to_owned(),
        details,
        occurred_at,
    }
}

pub(super) fn execution_projection_keys(plan: &ActionPlan) -> (String, String) {
    if let [step] = plan.steps.as_slice() {
        (step.resource.clone(), step.action.id().to_owned())
    } else {
        (format!("plan/{}", plan.id), "composite_plan".to_owned())
    }
}

pub(super) const fn audit_event_name(kind: AuditEventKind) -> &'static str {
    match kind {
        AuditEventKind::PlanCreated => "plan_created",
        AuditEventKind::PlanSubmitted => "plan_submitted",
        AuditEventKind::PolicyEvaluated => "policy_evaluated",
        AuditEventKind::CriticReviewed => "critic_reviewed",
        AuditEventKind::Approved => "approved",
        AuditEventKind::Rejected => "rejected",
        AuditEventKind::ExecutionSubmitted => "execution_submitted",
        AuditEventKind::StateChanged => "state_changed",
        AuditEventKind::StepIntentPersisted => "step_intent_persisted",
        AuditEventKind::StepResultPersisted => "step_result_persisted",
        AuditEventKind::VerificationCaptured => "verification_captured",
        AuditEventKind::VerificationCompleted => "verification_completed",
        AuditEventKind::RollbackStarted => "rollback_started",
        AuditEventKind::ManualTakeoverRequired => "manual_takeover_required",
        AuditEventKind::QuarantineCreated => "quarantine_created",
        AuditEventKind::QuarantineClearRequested => "quarantine_clear_requested",
        AuditEventKind::QuarantineCleared => "quarantine_cleared",
        AuditEventKind::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_bounds_and_idempotency_keys_fail_closed() {
        assert!(validate_candidate_steps(&[]).is_err());
        assert!(validate_idempotency_key("short").is_err());
        assert!(validate_idempotency_key("valid-idempotency-key-01").is_ok());
        assert!(validate_reason("operator confirmed current evidence").is_ok());
        assert!(validate_reason("\n").is_err());
    }

    #[test]
    fn wave_two_plans_cannot_use_expired_or_overlong_windows() {
        let now = Utc::now();
        assert!(validated_plan_expiry(now, Some(now), 3600).is_err());
        assert!(validated_plan_expiry(now, Some(now - Duration::seconds(1)), 3600).is_err());
        assert!(validated_plan_expiry(now, Some(now + Duration::seconds(3601)), 3600).is_err());
        assert_eq!(
            validated_plan_expiry(now, Some(now + Duration::seconds(3600)), 3600).expect("maximum window"),
            now + Duration::seconds(3600)
        );
    }

    #[test]
    fn aggregate_risk_never_accepts_r3() {
        assert_eq!(aggregate_risk(&[ActionRisk::R1]).expect("R1"), ActionRisk::R1);
        assert_eq!(
            aggregate_risk(&[ActionRisk::R1, ActionRisk::R2]).expect("R2"),
            ActionRisk::R2
        );
        assert!(aggregate_risk(&[ActionRisk::R3]).is_err());
    }

    #[test]
    fn audit_event_names_cover_new_quarantine_request() {
        assert_eq!(
            audit_event_name(AuditEventKind::QuarantineClearRequested),
            "quarantine_clear_requested"
        );
    }
}
