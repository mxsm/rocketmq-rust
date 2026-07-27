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

impl SupervisedExecutionService {
    pub(crate) async fn approve(
        &self,
        auth: &AuthContext,
        id: ActionPlanId,
        request: &ApprovalDecisionRequest,
        correlation_id: CorrelationId,
    ) -> Result<ApprovalDecisionResponse, ControlPlaneError> {
        self.decide(auth, id, request, ApprovalDecision::Approved, correlation_id)
            .await
    }

    pub(crate) async fn reject(
        &self,
        auth: &AuthContext,
        id: ActionPlanId,
        request: &ApprovalDecisionRequest,
        correlation_id: CorrelationId,
    ) -> Result<ApprovalDecisionResponse, ControlPlaneError> {
        self.decide(auth, id, request, ApprovalDecision::Rejected, correlation_id)
            .await
    }

    async fn decide(
        &self,
        auth: &AuthContext,
        id: ActionPlanId,
        request: &ApprovalDecisionRequest,
        decision: ApprovalDecision,
        correlation_id: CorrelationId,
    ) -> Result<ApprovalDecisionResponse, ControlPlaneError> {
        self.policy.require_approver(auth)?;
        validate_reason(&request.reason)?;
        let projection = self.repository.supervised_plan(auth, id).await?;
        let persisted_risk = projection.risk;
        let plan = projection.plan;
        if auth.subject == plan.created_by {
            return Err(ControlPlaneError::forbidden(
                "self_approval_forbidden",
                "a plan creator cannot approve or reject the same plan",
            ));
        }
        if request.plan_hash != plan.plan_hash {
            return Err(ControlPlaneError::conflict_code(
                "plan_hash_mismatch",
                "approval does not bind the current immutable plan hash",
            ));
        }
        let now = self.now();
        if plan.expires_at <= now {
            return Err(ControlPlaneError::conflict_code(
                "plan_expired",
                "plan expired before the approval decision",
            ));
        }
        let approved_precondition_hash = if decision == ApprovalDecision::Approved {
            let live = self.live_plan_state(auth, &plan, now).await?;
            if aggregate_risk(&live.risks)? != persisted_risk {
                return Err(ControlPlaneError::conflict_code(
                    "plan_risk_mismatch",
                    "persisted plan risk no longer matches its action descriptors",
                ));
            }
            if request.precondition_hash != live.precondition_hash || plan.evidence_hash != live.evidence_hash {
                return Err(ControlPlaneError::conflict_code(
                    "precondition_changed",
                    "live evidence no longer matches the reviewed plan",
                ));
            }
            ensure_live_ready(live.facts)?;
            if persisted_risk == ActionRisk::R2 && self.repository.valid_critic_review(auth, &plan).await?.is_none() {
                return Err(ControlPlaneError::conflict_code(
                    "critic_required",
                    "R2 plan requires a valid heterogeneous Critic review before approval",
                ));
            }
            if plan.status != PlanStatus::ReadyForApproval {
                return Err(ControlPlaneError::conflict_code(
                    "plan_state_changed",
                    "plan is not ready for approval",
                ));
            }
            self.ensure_current_policy(auth, &plan).await?;
            Some(live.precondition_hash)
        } else {
            None
        };
        let validity = request
            .validity_seconds
            .unwrap_or(self.policy.approval_ttl_seconds())
            .min(self.policy.approval_ttl_seconds());
        if validity == 0 {
            return Err(ControlPlaneError::validation(
                "invalid_approval_window",
                "approval validity must be greater than zero",
            ));
        }
        let expires_at = (now + duration_seconds(validity)?).min(plan.expires_at);
        let approval = ApprovalRecord {
            id: ApprovalId::new(),
            plan_id: plan.id,
            plan_hash: plan.plan_hash.clone(),
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            requester_subject: plan.created_by.clone(),
            approver_subject: auth.subject.clone(),
            approver_role: "approver".to_owned(),
            decision,
            reason: request.reason.trim().to_owned(),
            decided_at: now,
            expires_at,
        };
        let (grant, next_status, expected_statuses) = match (decision, approved_precondition_hash) {
            (ApprovalDecision::Approved, Some(precondition_hash)) => {
                let mut grant = ApprovalGrant {
                    issuer: CONTROL_PLANE_ISSUER.to_owned(),
                    audience: self.policy.executor_audience().to_owned(),
                    approval_id: approval.id,
                    plan_id: plan.id,
                    plan_hash: plan.plan_hash.clone(),
                    precondition_hash,
                    tenant_id: plan.tenant_id,
                    cluster_id: plan.cluster_id,
                    approver_subject: auth.subject.clone(),
                    issued_at: now,
                    expires_at,
                    nonce: Uuid::new_v4().to_string(),
                    signature: String::new(),
                };
                self.signer.sign_approval(&mut grant)?;
                (Some(grant), PlanStatus::Approved, vec![PlanStatus::ReadyForApproval])
            }
            (ApprovalDecision::Rejected, None) => (
                None,
                PlanStatus::Rejected,
                vec![
                    PlanStatus::NeedsCritic,
                    PlanStatus::ReadyForApproval,
                    PlanStatus::InReview,
                ],
            ),
            _ => {
                return Err(ControlPlaneError::configuration(
                    "approval precondition was inconsistent with the requested decision",
                ));
            }
        };
        let audits = approval_audits(auth, &plan, decision, correlation_id, now);
        let updated = self
            .repository
            .persist_approval_decision(
                &plan,
                &approval,
                grant.as_ref(),
                &expected_statuses,
                next_status,
                &audits,
            )
            .await?;
        self.publish_audits(&audits);
        Ok(ApprovalDecisionResponse {
            plan: updated,
            approval,
            grant,
        })
    }

    pub(crate) async fn submit_execution(
        &self,
        auth: &AuthContext,
        request: &SubmitExecutionRequest,
        correlation_id: CorrelationId,
    ) -> Result<ExecutionSubmissionView, ControlPlaneError> {
        self.policy.require_operator(auth)?;
        validate_idempotency_key(&request.idempotency_key)?;
        if let Some(existing) = self
            .repository
            .supervised_execution_by_idempotency(auth, &request.idempotency_key)
            .await?
        {
            if existing.request.plan.id != request.plan_id
                || existing.request.plan.plan_hash != request.plan_hash
                || existing.request.requested_by != auth.subject
            {
                return Err(ControlPlaneError::conflict_code(
                    "idempotency_conflict",
                    "idempotency key is bound to a different execution",
                ));
            }
            return Ok(ExecutionSubmissionView {
                execution: existing.request,
                state: existing.state,
                submitted_at: existing.submitted_at,
            });
        }
        let projection = self.repository.supervised_plan(auth, request.plan_id).await?;
        let persisted_risk = projection.risk;
        let plan = projection.plan;
        if plan.status != PlanStatus::Approved {
            return Err(ControlPlaneError::conflict_code(
                "approval_required",
                "plan must have a current human approval before execution",
            ));
        }
        if request.plan_hash != plan.plan_hash {
            return Err(ControlPlaneError::conflict_code(
                "plan_hash_mismatch",
                "execution request does not bind the approved plan hash",
            ));
        }
        let now = self.now();
        let live = self.live_plan_state(auth, &plan, now).await?;
        if aggregate_risk(&live.risks)? != persisted_risk {
            return Err(ControlPlaneError::conflict_code(
                "plan_risk_mismatch",
                "persisted plan risk no longer matches its action descriptors",
            ));
        }
        ensure_live_ready(live.facts)?;
        if request.precondition_hash != live.precondition_hash || plan.evidence_hash != live.evidence_hash {
            return Err(ControlPlaneError::conflict_code(
                "precondition_changed",
                "live state changed after approval",
            ));
        }
        self.ensure_current_policy(auth, &plan).await?;
        let grant = self
            .repository
            .current_approval_grant(auth, &plan, now)
            .await?
            .ok_or_else(|| {
                ControlPlaneError::conflict_code("approval_expired", "no current service-issued approval grant exists")
            })?;
        self.signer.verify_approval(&grant)?;
        if grant.precondition_hash != live.precondition_hash
            || grant.audience != self.policy.executor_audience()
            || grant.expires_at <= now
        {
            return Err(ControlPlaneError::conflict_code(
                "approval_invalidated",
                "approval no longer binds the current plan and live state",
            ));
        }
        if !live.execution_supported {
            return Err(ControlPlaneError::conflict_code(
                "action_not_ready",
                "one or more typed action handlers are not enabled",
            ));
        }
        let expires_at = (now + Duration::minutes(5)).min(grant.expires_at).min(plan.expires_at);
        let id = ExecutionId::new();
        let mut execution = ExecutionRequest {
            schema_version: ExecutionRequest::SCHEMA_VERSION.to_owned(),
            id,
            tenant_id: plan.tenant_id,
            cluster_id: plan.cluster_id,
            correlation_id,
            plan: plan.clone(),
            approvals: vec![grant],
            requested_by: auth.subject.clone(),
            idempotency_key: request.idempotency_key.clone(),
            issuer: CONTROL_PLANE_ISSUER.to_owned(),
            audience: self.policy.executor_audience().to_owned(),
            issued_at: now,
            expires_at,
            nonce: Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        self.signer.sign_execution(&mut execution)?;
        self.signer.verify_execution(&execution)?;
        execution
            .validate_at(now, self.policy.executor_audience())
            .map_err(|error| ControlPlaneError::validation("invalid_execution_request", error.to_string()))?;
        let (resource_key, action_id) = execution_projection_keys(&plan);
        let audit = audit_event(
            auth,
            plan.cluster_id,
            correlation_id,
            AuditEventKind::ExecutionSubmitted,
            "operator",
            "execution",
            id.to_string(),
            "HumanApprovedExecutionSubmitted",
            json!({"plan_id": plan.id, "plan_hash": plan.plan_hash, "state": "pending"}),
            now,
        );
        let stored = self
            .repository
            .persist_execution_submission(
                &NewExecutionProjection {
                    id,
                    tenant_id: plan.tenant_id,
                    cluster_id: plan.cluster_id,
                    correlation_id,
                    resource_key,
                    action_id,
                    request: execution,
                },
                &audit,
            )
            .await?;
        self.publish_audits(std::slice::from_ref(&audit));
        Ok(ExecutionSubmissionView {
            execution: stored.request,
            state: stored.state,
            submitted_at: stored.submitted_at,
        })
    }

    pub(crate) async fn execution(
        &self,
        auth: &AuthContext,
        id: ExecutionId,
    ) -> Result<ExecutionSubmissionView, ControlPlaneError> {
        let stored = self.repository.supervised_execution(auth, id).await?;
        Ok(ExecutionSubmissionView {
            execution: stored.request,
            state: stored.state,
            submitted_at: stored.submitted_at,
        })
    }

    pub(crate) async fn audit(
        &self,
        auth: &AuthContext,
        correlation_id: CorrelationId,
    ) -> Result<AuditPage, ControlPlaneError> {
        let mut items = self
            .repository
            .audit_timeline(auth, correlation_id, MAX_AUDIT_EVENTS)
            .await?;
        let partial = items.len() >= usize::try_from(MAX_AUDIT_EVENTS).unwrap_or(usize::MAX);
        items.truncate(usize::try_from(MAX_AUDIT_EVENTS - 1).unwrap_or(500));
        Ok(AuditPage {
            schema_version: "rocketmq-sre.audit-page.v1",
            correlation_id,
            items,
            partial,
        })
    }

    pub(crate) async fn quarantines(
        &self,
        auth: &AuthContext,
        query: &QuarantineListQuery,
    ) -> Result<QuarantinePage, ControlPlaneError> {
        require_cluster(auth, query.cluster_id)?;
        let limit = query.limit.unwrap_or(50);
        if !(1..=MAX_QUARANTINES).contains(&limit) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "quarantine limit must be between 1 and 200",
            ));
        }
        let mut items = self
            .repository
            .resource_quarantines(
                auth,
                query.cluster_id,
                query.include_cleared.unwrap_or(false),
                i64::from(limit + 1),
            )
            .await?;
        let partial = items.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        items.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(QuarantinePage {
            schema_version: "rocketmq-sre.resource-quarantine-page.v1",
            items,
            partial,
        })
    }

    pub(crate) async fn clear_quarantine(
        &self,
        auth: &AuthContext,
        id: ResourceQuarantineId,
        request: &ClearQuarantineRequest,
        correlation_id: CorrelationId,
    ) -> Result<ResourceQuarantine, ControlPlaneError> {
        self.policy.require_approver(auth)?;
        validate_reason(&request.reason)?;
        if request.evidence_ids.is_empty() || request.evidence_ids.len() > 16 {
            return Err(ControlPlaneError::validation(
                "verification_evidence_required",
                "quarantine clear requires between one and sixteen Evidence IDs",
            ));
        }
        let quarantine = self.repository.quarantine(auth, id).await?;
        if !quarantine.is_active() {
            return Err(ControlPlaneError::conflict_code(
                "quarantine_state_changed",
                "resource quarantine is already cleared",
            ));
        }
        let now = self.now();
        for evidence_id in &request.evidence_ids {
            let evidence = self.repository.evidence(auth, *evidence_id).await?;
            if evidence.cluster_id != quarantine.cluster_id || !self.evidence_is_current(&evidence, now)? {
                return Err(ControlPlaneError::conflict_code(
                    "verification_evidence_invalid",
                    "quarantine clear evidence is stale or outside the target cluster",
                ));
            }
        }
        let requested = audit_event(
            auth,
            quarantine.cluster_id,
            correlation_id,
            AuditEventKind::QuarantineClearRequested,
            "approver",
            "resource_quarantine",
            quarantine.id.to_string(),
            "HumanVerificationSubmitted",
            json!({"evidence_ids": request.evidence_ids, "resource_key": quarantine.resource_key}),
            now,
        );
        let cleared = audit_event(
            auth,
            quarantine.cluster_id,
            correlation_id,
            AuditEventKind::QuarantineCleared,
            "approver",
            "resource_quarantine",
            quarantine.id.to_string(),
            "ResourceQuarantineCleared",
            json!({"evidence_ids": request.evidence_ids, "resource_key": quarantine.resource_key}),
            now,
        );
        let audits = [requested, cleared];
        let updated = self
            .repository
            .clear_resource_quarantine(
                &quarantine,
                &auth.subject,
                request.reason.trim(),
                &request.evidence_ids,
                now,
                &audits,
            )
            .await?;
        self.publish_audits(&audits);
        Ok(updated)
    }
}
