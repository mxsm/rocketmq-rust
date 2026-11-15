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

use rocketmq_sre_contracts::ACTION_READINESS_SCHEMA_VERSION;
use rocketmq_sre_contracts::ActionExecutionReadiness;
use rocketmq_sre_contracts::ActionReadinessBinding;
use rocketmq_sre_contracts::ActionReadinessDiagnosis;
use rocketmq_sre_contracts::ActionReadinessEvidenceBinding;
use rocketmq_sre_contracts::ActionReadinessFact;
use rocketmq_sre_contracts::ActionReadinessOperation;
use rocketmq_sre_contracts::ActionReadinessOperations;
use rocketmq_sre_contracts::ActionReadinessReason as Reason;
use rocketmq_sre_contracts::ActionReadinessRequest;
use rocketmq_sre_contracts::ActionReadinessSource as Source;
use rocketmq_sre_contracts::ActionReadinessStages;
use rocketmq_sre_contracts::ActionReadinessState as State;
use rocketmq_sre_contracts::ActionReadinessView;

use super::preparation::PreparationCandidate;
use super::preparation::diagnostic_bindings;
use super::preparation::digest;
use super::preparation::persistence_time;
use super::preparation::preparation_failure;
use super::*;

impl SupervisedExecutionService {
    /// Reads stored facts only. This does not call the Executor, renew a proof,
    /// create a Plan, or grant approval/dispatch authority.
    pub(crate) async fn action_readiness(
        &self,
        auth: &AuthContext,
        incident_id: rocketmq_sre_contracts::IncidentId,
        request: &ActionReadinessRequest,
    ) -> Result<ActionReadinessView, ControlPlaneRequestFailure> {
        require_cluster(auth, request.cluster_id)?;
        validate_request(request)?;
        let context = self
            .repository
            .diagnosis_plan_context(auth, request.cluster_id, incident_id, request.diagnosis_revision_id)
            .await?;
        let now = self.now();
        let observed_at = persistence_time(now)?;
        let mut valid_until = observed_at + Duration::seconds(30);
        let mut plan = None;
        let mut selected_proof = request.precondition_evidence_id;
        let mut proof_actor = auth.subject.as_str();
        if let (Some(id), Some(step_id)) = (request.plan_id, request.plan_step_id) {
            let stored = self.repository.supervised_plan(auth, id).await?.plan;
            stored
                .verify_plan_hash()
                .map_err(|_| preparation_failure("plan_binding_mismatch"))?;
            let step = stored
                .steps
                .iter()
                .find(|step| step.id == step_id)
                .ok_or_else(|| preparation_failure("plan_binding_mismatch"))?;
            if stored.incident_id != incident_id
                || stored.cluster_id != request.cluster_id
                || stored.diagnosis_revision != request.diagnosis_revision_id
                || step.action.id() != request.action_id
                || step.descriptor_version != request.descriptor_version
                || step.resource != request.resource
                || step.parameters != request.parameters
                || selected_proof.is_some_and(|id| !step.evidence_ids.contains(&id))
            {
                return Err(preparation_failure("plan_binding_mismatch"));
            }
            if selected_proof.is_none() {
                let mut candidates = Vec::new();
                for id in &step.evidence_ids {
                    let snapshot = self.repository.evidence(auth, *id).await?;
                    if snapshot.source == "execution-agent" && snapshot.resource == step.resource {
                        candidates.push(*id);
                    }
                }
                if candidates.len() == 1 {
                    selected_proof = candidates.first().copied();
                }
            }
            valid_until = valid_until.min(stored.expires_at);
            plan = Some(stored);
        }
        if let Some(plan) = &plan {
            proof_actor = &plan.created_by;
        }
        let is_current = context.latest_diagnosis_revision_id == request.diagnosis_revision_id;
        let is_confirmed = context.status == "confirmed" && context.execution_eligible;
        let is_complete = !context.partial && !context.evidence_ids.is_empty();
        let active = context.incident_status == "monitoring";
        let mut diagnosis_reasons = Vec::new();
        if !is_current {
            diagnosis_reasons.push(Reason::DiagnosisNotCurrent);
        }
        if !active {
            diagnosis_reasons.push(Reason::IncidentInactive);
        }
        if !is_confirmed {
            diagnosis_reasons.push(Reason::DiagnosisNotConfirmed);
        }
        if !is_complete {
            diagnosis_reasons.push(Reason::DiagnosisIncomplete);
        }
        if !context.primary_model_lineage_valid {
            diagnosis_reasons.push(Reason::PrimaryModelLineageInvalid);
        }
        let diagnosis_ready = diagnosis_reasons.is_empty();
        let operator = self.policy.require_operator(auth).is_ok();
        let candidate = PreparationCandidate {
            context: &context,
            action_id: &request.action_id,
            descriptor_version: &request.descriptor_version,
            resource: &request.resource,
            parameters: &request.parameters,
            actor: proof_actor,
        };
        let resolution = self.catalog.resolve(&request.action_id);
        let mut descriptor = fact(State::Blocked, Reason::ActionUnknown, Source::DescriptorCatalog);
        let mut parameters = None;
        let mut parameters_digest = None;
        let mut descriptor_valid = false;
        let mut risk = Vec::new();
        let mut execution_supported = false;
        let mut rollback_declared = false;
        if let Ok(resolution) = &resolution {
            match resolution {
                CatalogResolution::ManualOnly(_) => {
                    descriptor = fact(State::NotApplicable, Reason::ManualOnly, Source::DescriptorCatalog)
                }
                CatalogResolution::Supervised(_, action) => {
                    risk.push(action.risk);
                    execution_supported = action.execution_supported;
                    rollback_declared = action.compensation.mode != CompensationMode::NotAvailable;
                    if action.version != request.descriptor_version {
                        descriptor = fact(
                            State::Blocked,
                            Reason::DescriptorVersionMismatch,
                            Source::DescriptorCatalog,
                        );
                    } else if request.resource.is_empty() {
                        descriptor = fact(State::Blocked, Reason::ResourceRequired, Source::DescriptorCatalog);
                    } else {
                        match validate_parameters(action, &request.parameters) {
                            Ok(()) => {
                                descriptor = fact(
                                    State::Satisfied,
                                    Reason::ObservationAvailable,
                                    Source::DescriptorCatalog,
                                );
                                descriptor_valid = true;
                                parameters = Some(request.parameters.clone());
                                parameters_digest = Some(digest(&request.parameters)?);
                            }
                            Err(ControlPlaneRequestFailure::Rejected(error))
                                if error.code() == "forbidden_action_field" =>
                            {
                                return Err(error.into());
                            }
                            Err(ControlPlaneRequestFailure::Operational(error)) => return Err(error.into()),
                            Err(_) => {
                                descriptor = fact(State::Blocked, Reason::ParametersInvalid, Source::DescriptorCatalog)
                            }
                        }
                    }
                }
            }
        }
        let target_scope_digest = if descriptor_valid {
            self.target_scope_digest(&candidate)?
        } else {
            None
        };
        let mut evidence = fact(State::Unknown, Reason::EvidenceMissing, Source::CurrentDiagnosis);
        let mut diagnostic = BTreeMap::new();
        let mut diagnostic_reasons = vec![Reason::EvidenceMissing];
        match self.diagnostic_evidence(auth, &context).await {
            Ok(snapshots) => {
                diagnostic_reasons.clear();
                for snapshot in snapshots.values() {
                    match self.evidence_readiness_deadline(snapshot, now) {
                        Ok(deadline) => valid_until = valid_until.min(deadline),
                        Err(reason) => {
                            if !diagnostic_reasons.contains(&reason) {
                                diagnostic_reasons.push(reason);
                            }
                        }
                    }
                }
                if snapshots.is_empty() {
                    diagnostic_reasons.push(Reason::EvidenceMissing);
                }
                evidence = if diagnostic_reasons.is_empty() {
                    fact(
                        State::Unknown,
                        Reason::PreparationBindingMissing,
                        Source::StoredPreparation,
                    )
                } else {
                    unavailable_evidence(diagnostic_reasons.clone(), Source::CurrentDiagnosis)
                };
                diagnostic = snapshots;
            }
            Err(error)
                if error.failure() == crate::ControlPlaneFailure::Validation
                    && error.code() == "source_unavailable" =>
            {
                // This local repository read rejects malformed stored metadata
                // and content hashes before it can return a snapshot.
                diagnostic_reasons = vec![Reason::EvidenceInvalid];
                evidence = unavailable_evidence(diagnostic_reasons.clone(), Source::CurrentDiagnosis);
            }
            Err(ControlPlaneRequestFailure::Operational(error)) => {
                return Err(ControlPlaneRequestFailure::Operational(error));
            }
            Err(_) => {}
        }
        let mut handler = fact(State::Unknown, Reason::HandlerNotObserved, Source::NotObserved);
        let mut enabled = if matches!(self.executor, ExecutorSubmissionClient::Disabled) {
            fact(
                State::Blocked,
                Reason::ExecutorNotConfigured,
                Source::ExecutorConfiguration,
            )
        } else {
            fact(
                State::Unknown,
                Reason::RuntimeEnablementNotObserved,
                Source::NotObserved,
            )
        };
        let mut target = if target_scope_digest.is_none() {
            fact(
                State::Blocked,
                Reason::TargetScopeUnverified,
                Source::TargetScopeConfiguration,
            )
        } else {
            fact(State::Unknown, Reason::TargetSupportNotObserved, Source::NotObserved)
        };
        let mut precondition_evidence = None;
        let mut precondition_hash = None;
        let mut preparation_valid = false;
        if let Some(id) = selected_proof {
            // An arbitrary ID may not be used to read another Incident's record.
            if !self
                .repository
                .evidence_linked_to_incident(auth, incident_id, id)
                .await?
            {
                return Err(ControlPlaneRequestFailure::not_found());
            }
            let snapshot = self.repository.evidence(auth, id).await?;
            if snapshot.cluster_id != request.cluster_id {
                return Err(ControlPlaneRequestFailure::not_found());
            }
            precondition_evidence = Some(ActionReadinessEvidenceBinding {
                evidence_id: id,
                content_hash: snapshot.content_hash.clone(),
            });
            match self.validate_preparation(&snapshot, &candidate, &diagnostic, now) {
                Ok(valid) => {
                    valid_until = valid_until.min(valid.valid_until);
                    precondition_hash = Some(valid.proof.agent_result.precondition_hash.clone());
                    let observed = valid.proof.agent_result.observed_at;
                    handler = observed_fact(
                        State::Satisfied,
                        Reason::HandlerReadObserved,
                        id,
                        observed,
                        valid.valid_until,
                    );
                    if !matches!(self.executor, ExecutorSubmissionClient::Disabled) {
                        enabled = observed_fact(
                            State::Satisfied,
                            Reason::HandlerReadObserved,
                            id,
                            observed,
                            valid.valid_until,
                        );
                    }
                    if valid.proof.agent_result.ready && valid.proof.agent_result.reason_codes.is_empty() {
                        preparation_valid = true;
                        evidence = observed_fact(
                            State::Satisfied,
                            Reason::ObservationAvailable,
                            id,
                            observed,
                            valid.valid_until,
                        );
                    } else {
                        target = observed_fact(
                            State::Blocked,
                            Reason::TargetPreconditionNotReady,
                            id,
                            observed,
                            valid.valid_until,
                        );
                    }
                }
                Err(ControlPlaneRequestFailure::Operational(error)) => {
                    return Err(ControlPlaneRequestFailure::Operational(error));
                }
                Err(error) => {
                    // The admission endpoint intentionally has one safe failure code.
                    // The read projection retains the actual missing/invalid/age facts.
                    let validity_reason = self
                        .evidence_readiness_deadline(&snapshot, now)
                        .err()
                        .or_else(|| diagnostic_reasons.first().copied());
                    let reason = preparation_reason(&error, validity_reason);
                    let mut reasons = diagnostic_reasons.clone();
                    if !reasons.contains(&reason) {
                        reasons.push(reason);
                    }
                    evidence = unavailable_evidence(reasons, Source::StoredPreparation);
                }
            }
        }
        let mut quarantined = false;
        let mut busy = false;
        if let Ok(CatalogResolution::Supervised(action, _)) = resolution {
            quarantined = self
                .repository
                .resource_is_quarantined(auth, request.cluster_id, &request.resource, action)
                .await?;
            busy = self
                .repository
                .resource_has_active_change(auth, request.cluster_id, &request.resource)
                .await?;
        }
        let policy_reasons = self.policy.denial_reasons(
            auth,
            auth.tenant_id,
            request.cluster_id,
            &risk,
            PolicyFacts {
                diagnosis_confirmed: diagnosis_ready,
                diagnosis_execution_eligible: context.execution_eligible,
                evidence_current: preparation_valid,
                resource_quarantined: quarantined,
                resource_busy: busy,
                maintenance_window_open: self.policy.maintenance_window_open(now),
                rollback_available: rollback_declared,
            },
        );
        let policy = if policy_reasons.is_empty() {
            fact(State::Satisfied, Reason::ObservationAvailable, Source::PolicyEvaluation)
        } else {
            let mut value = fact(State::Blocked, Reason::PolicyDenied, Source::PolicyEvaluation);
            if quarantined {
                value.reason_codes.push(Reason::ResourceQuarantined);
            }
            if busy {
                value.reason_codes.push(Reason::ResourceBusy);
            }
            if !self.policy.maintenance_window_open(now) {
                value.reason_codes.push(Reason::MaintenanceWindowClosed);
            }
            value
        };
        let mut approval = fact(State::NotApplicable, Reason::PlanNotCreated, Source::NotObserved);
        if let Some(plan) = &plan {
            approval = fact(State::Blocked, Reason::ApprovalRequired, Source::StoredApproval);
            if plan.expires_at <= now {
                approval = fact(State::Blocked, Reason::PlanExpired, Source::StoredApproval);
            } else if let Some(grant) = self.repository.current_approval_grant(auth, plan, now).await?
                && self.signer.verify_approval(&grant).is_ok()
                && grant.plan_id == plan.id
                && grant.plan_hash == plan.plan_hash
                && grant.tenant_id == plan.tenant_id
                && grant.cluster_id == plan.cluster_id
                && grant.precondition_hash
                    == plan
                        .compute_precondition_hash()
                        .map_err(|_| preparation_failure("plan_binding_mismatch"))?
                && grant.audience == self.policy.executor_audience()
                && grant.expires_at > now
            {
                valid_until = valid_until.min(grant.expires_at);
                approval = fact(State::Satisfied, Reason::ObservationAvailable, Source::StoredApproval);
                approval.observed_at = Some(grant.issued_at);
                approval.valid_until = Some(grant.expires_at);
            }
        }
        let mut prepare_reasons = diagnosis_reasons.clone();
        if !operator {
            prepare_reasons.push(Reason::OperatorRequired);
        }
        if !descriptor_valid {
            prepare_reasons.extend(&descriptor.reason_codes);
        }
        if !execution_supported {
            prepare_reasons.push(Reason::PlanOnly);
        }
        if matches!(self.executor, ExecutorSubmissionClient::Disabled) {
            prepare_reasons.push(Reason::ExecutorNotConfigured);
        }
        if target_scope_digest.is_none() {
            prepare_reasons.push(Reason::TargetScopeUnverified);
        }
        prepare_reasons.extend(&diagnostic_reasons);
        if quarantined {
            prepare_reasons.push(Reason::ResourceQuarantined);
        }
        if busy {
            prepare_reasons.push(Reason::ResourceBusy);
        }
        if !self.policy.maintenance_window_open(now) {
            prepare_reasons.push(Reason::MaintenanceWindowClosed);
        }
        let mut create_reasons = prepare_reasons.clone();
        if !preparation_valid {
            create_reasons.extend(&evidence.reason_codes);
        }
        if policy.state != State::Satisfied {
            create_reasons.extend(&policy.reason_codes);
        }
        if plan.is_some() {
            prepare_reasons.push(Reason::PlanImmutable);
            create_reasons.push(Reason::PlanImmutable);
        }
        let mut confirm_reasons = Vec::new();
        if !operator {
            confirm_reasons.push(Reason::OperatorRequired);
        }
        if !is_current {
            confirm_reasons.push(Reason::DiagnosisNotCurrent);
        }
        if !active {
            confirm_reasons.push(Reason::IncidentInactive);
        }
        if !is_complete {
            confirm_reasons.push(Reason::DiagnosisIncomplete);
        }
        if !context.primary_model_lineage_valid {
            confirm_reasons.push(Reason::PrimaryModelLineageInvalid);
        }
        if is_confirmed {
            confirm_reasons.push(Reason::DiagnosisAlreadyConfirmed);
        }
        let mut binding = ActionReadinessBinding {
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            incident_id,
            diagnosis_revision_id: request.diagnosis_revision_id,
            latest_diagnosis_revision_id: Some(context.latest_diagnosis_revision_id),
            action_id: request.action_id.clone(),
            descriptor_version: request.descriptor_version.clone(),
            resource: request.resource.clone(),
            parameters,
            parameters_digest,
            target_scope_digest,
            diagnostic_evidence: diagnostic_bindings(&diagnostic),
            precondition_evidence,
            precondition_hash,
            policy_version: self.policy.version().to_owned(),
            actor_subject: auth.subject.clone(),
            plan_id: request.plan_id,
            plan_step_id: request.plan_step_id,
            plan_hash: plan.as_ref().map(|plan| plan.plan_hash.clone()),
            binding_digest: String::new(),
        };
        binding.binding_digest = digest(&binding)?;
        let stages = ActionReadinessStages {
            descriptor,
            handler,
            enabled,
            target,
            evidence,
            policy,
            approval,
            qualification: fact(
                State::Unknown,
                Reason::ActionTargetQualificationUnavailable,
                Source::QualificationUnavailable,
            ),
            compensation: fact(State::Unknown, Reason::CompensationNotObserved, Source::NotObserved),
        };
        let execution_readiness = if [
            &stages.descriptor,
            &stages.enabled,
            &stages.target,
            &stages.evidence,
            &stages.policy,
            &stages.approval,
        ]
        .iter()
        .any(|fact| fact.state == State::Blocked)
        {
            ActionExecutionReadiness::Blocked
        } else {
            ActionExecutionReadiness::Unknown
        };
        Ok(ActionReadinessView {
            schema_version: ACTION_READINESS_SCHEMA_VERSION.to_owned(),
            binding,
            diagnosis: ActionReadinessDiagnosis {
                is_current,
                is_confirmed,
                is_complete,
                primary_model_lineage_valid: context.primary_model_lineage_valid,
                reason_codes: diagnosis_reasons,
            },
            stages,
            execution_readiness,
            operations: ActionReadinessOperations {
                confirm_diagnosis: operation(confirm_reasons),
                prepare_precondition: operation(prepare_reasons),
                create_plan: operation(create_reasons),
            },
            observed_at,
            valid_until: (valid_until >= now).then_some(valid_until),
        })
    }

    pub(super) async fn require_dispatch_readiness(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
    ) -> Result<(), ControlPlaneRequestFailure> {
        for step in &plan.steps {
            let readiness = self
                .action_readiness(
                    auth,
                    plan.incident_id,
                    &ActionReadinessRequest {
                        cluster_id: plan.cluster_id,
                        diagnosis_revision_id: plan.diagnosis_revision,
                        action_id: step.action.id().to_owned(),
                        descriptor_version: step.descriptor_version.clone(),
                        resource: step.resource.clone(),
                        parameters: step.parameters.clone(),
                        precondition_evidence_id: None,
                        plan_id: Some(plan.id),
                        plan_step_id: Some(step.id),
                    },
                )
                .await?;
            if readiness.execution_readiness != ActionExecutionReadiness::Satisfied {
                return Err(ControlPlaneRequestFailure::conflict_code(
                    "action_readiness_blocked",
                    "fresh action, target, qualification, and compensation proofs are required before first dispatch",
                ));
            }
        }
        Ok(())
    }
}

fn validate_request(request: &ActionReadinessRequest) -> Result<(), ControlPlaneRequestFailure> {
    if request.plan_id.is_some() != request.plan_step_id.is_some()
        || request.action_id.len() > 160
        || request.descriptor_version.len() > 64
        || request.resource.len() > 512
        || [&request.action_id, &request.descriptor_version, &request.resource]
            .iter()
            .any(|value| value.trim() != value.as_str() || value.chars().any(char::is_control))
        || serde_json::to_vec(&request.parameters).map_or(true, |bytes| bytes.len() > 32 * 1024)
    {
        return Err(ControlPlaneRequestFailure::validation(
            "invalid_action_readiness",
            "action candidate or Plan selectors are invalid",
        ));
    }
    Ok(())
}

fn fact(state: State, reason: Reason, source: Source) -> ActionReadinessFact {
    ActionReadinessFact {
        state,
        reason_codes: vec![reason],
        source,
        evidence_ids: Vec::new(),
        observed_at: None,
        valid_until: None,
    }
}

fn observed_fact(
    state: State,
    reason: Reason,
    id: EvidenceId,
    at: DateTime<Utc>,
    until: DateTime<Utc>,
) -> ActionReadinessFact {
    let mut value = fact(state, reason, Source::StoredPreparation);
    value.evidence_ids.push(id);
    value.observed_at = Some(at);
    value.valid_until = Some(until);
    value
}

fn unavailable_evidence(reasons: Vec<Reason>, source: Source) -> ActionReadinessFact {
    let state = if reasons.iter().all(|reason| *reason == Reason::EvidenceMissing) {
        State::Unknown
    } else {
        State::Blocked
    };
    let mut value = fact(state, Reason::EvidenceMissing, source);
    value.reason_codes = reasons;
    value
}

fn operation(mut reasons: Vec<Reason>) -> ActionReadinessOperation {
    let mut unique = Vec::new();
    for reason in reasons.drain(..) {
        if !unique.contains(&reason) {
            unique.push(reason);
        }
    }
    ActionReadinessOperation {
        allowed: unique.is_empty(),
        reason_codes: unique,
    }
}

fn preparation_reason(error: &ControlPlaneRequestFailure, validity_reason: Option<Reason>) -> Reason {
    match error {
        ControlPlaneRequestFailure::Rejected(error) => match error.code() {
            "preparation_binding_missing" => Reason::PreparationBindingMissing,
            "diagnosis_evidence_missing" => validity_reason.unwrap_or(Reason::EvidenceMissing),
            "preparation_origin_invalid" => Reason::PreparationOriginInvalid,
            "target_scope_unverified" => Reason::TargetScopeUnverified,
            "target_scope_changed" => Reason::TargetScopeChanged,
            "action_evidence_stale_or_invalid" => validity_reason.unwrap_or(Reason::EvidenceInvalid),
            "preparation_expired" => Reason::PreparationExpired,
            _ => Reason::PreparationBindingMismatch,
        },
        ControlPlaneRequestFailure::Operational(_) => Reason::SourceUnavailable,
    }
}
