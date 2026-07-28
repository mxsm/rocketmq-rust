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
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanDraft;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ApprovalDecision;
use rocketmq_sre_contracts::ApprovalGrant;
use rocketmq_sre_contracts::ApprovalId;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ManualRunbookDraft;
use rocketmq_sre_contracts::PlanStatus;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::PolicyEffect;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::ResourceQuarantineId;
use rocketmq_sre_contracts::canonical_precondition_hash;
use rocketmq_sre_contracts::canonical_sha256;
use serde_json::json;
use uuid::Uuid;

use super::catalog::ActionCatalog;
use super::catalog::CatalogResolution;
use super::catalog::ManualAction;
use super::catalog::validate_parameters;
use super::executor_client::ExecutorSubmissionClient;
use super::model::ActionPlanView;
use super::model::ApprovalDecisionRequest;
use super::model::ApprovalDecisionResponse;
use super::model::AuditPage;
use super::model::CandidatePlanStep;
use super::model::ClearQuarantineRequest;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::EvidenceBinding;
use super::model::ExecutionSubmissionView;
use super::model::NewExecutionProjection;
use super::model::QuarantineListQuery;
use super::model::QuarantinePage;
use super::model::SubmitExecutionRequest;
use super::policy::PolicyEvaluator;
use super::policy::PolicyFacts;
use super::policy::RESOURCE_QUARANTINED;
use super::policy::RULES_ONLY_NOT_EXECUTABLE;
use super::signing::GrantSigner;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::models::ModelGatewayService;
use crate::workflow::WorkflowService;
use crate::workflow::WorkflowStreamEvent;

mod critic;
mod support;
mod workflow;

use critic::critic_gate_state;
use support::*;

const MAX_PLAN_STEPS: usize = 16;
const MAX_STEP_EVIDENCE: usize = 32;
const MAX_AUDIT_EVENTS: i64 = 501;
const MAX_QUARANTINES: u32 = 200;
const CONTROL_PLANE_ISSUER: &str = "rocketmq-sre-control-plane";
type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct SupervisedExecutionService {
    repository: PostgresRepository,
    catalog: Arc<ActionCatalog>,
    policy: Arc<PolicyEvaluator>,
    signer: GrantSigner,
    model_gateway: ModelGatewayService,
    workflow: WorkflowService,
    executor: ExecutorSubmissionClient,
    clock: Clock,
}

struct LivePlanState {
    evidence_hash: String,
    precondition_hash: String,
    risks: Vec<ActionRisk>,
    facts: PolicyFacts,
    execution_supported: bool,
}

impl SupervisedExecutionService {
    pub(crate) fn new_with_executor(
        repository: PostgresRepository,
        workflow: WorkflowService,
        signing_key: impl AsRef<[u8]>,
        model_gateway: ModelGatewayService,
        executor: ExecutorSubmissionClient,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_with_clock_inner(
            repository,
            workflow,
            signing_key,
            model_gateway,
            executor,
            Arc::new(Utc::now),
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_clock(
        repository: PostgresRepository,
        workflow: WorkflowService,
        signing_key: impl AsRef<[u8]>,
        clock: Clock,
    ) -> Result<Self, ControlPlaneError> {
        let model_gateway = ModelGatewayService::disabled(repository.clone());
        Self::new_with_clock_inner(
            repository,
            workflow,
            signing_key,
            model_gateway,
            ExecutorSubmissionClient::disabled(),
            clock,
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_clock_and_model(
        repository: PostgresRepository,
        workflow: WorkflowService,
        signing_key: impl AsRef<[u8]>,
        model_gateway: ModelGatewayService,
        clock: Clock,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_with_clock_inner(
            repository,
            workflow,
            signing_key,
            model_gateway,
            ExecutorSubmissionClient::disabled(),
            clock,
        )
    }

    fn new_with_clock_inner(
        repository: PostgresRepository,
        workflow: WorkflowService,
        signing_key: impl AsRef<[u8]>,
        model_gateway: ModelGatewayService,
        executor: ExecutorSubmissionClient,
        clock: Clock,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository,
            catalog: Arc::new(ActionCatalog::embedded()?),
            policy: Arc::new(PolicyEvaluator::embedded()?),
            signer: GrantSigner::new(signing_key)?,
            model_gateway,
            workflow,
            executor,
            clock,
        })
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    pub(crate) async fn create_plan(
        &self,
        auth: &AuthContext,
        request: &CreatePlanRequest,
        correlation_id: CorrelationId,
    ) -> Result<CreatePlanResponse, ControlPlaneError> {
        self.policy.require_operator(auth)?;
        require_cluster(auth, request.cluster_id)?;
        validate_candidate_steps(&request.steps)?;
        let context = self
            .repository
            .diagnosis_plan_context(
                auth,
                request.cluster_id,
                request.incident_id,
                request.diagnosis_revision_id,
            )
            .await?;
        if context.tenant_id != auth.tenant_id
            || context.cluster_id != request.cluster_id
            || context.incident_id != request.incident_id
            || context.diagnosis_revision_id != request.diagnosis_revision_id
        {
            return Err(ControlPlaneError::forbidden(
                "tenant_mismatch",
                "diagnosis does not match the authenticated plan scope",
            ));
        }

        let resolved = request
            .steps
            .iter()
            .map(|step| self.catalog.resolve(&step.action_id))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(manual) = single_manual_action(&resolved)? {
            return Ok(CreatePlanResponse::ManualRunbook {
                runbook: manual_runbook(
                    auth,
                    request,
                    manual.id.as_str(),
                    manual.title.as_str(),
                    "R3ManualOnly",
                    manual.description.as_str(),
                ),
            });
        }
        if !context.execution_eligible {
            let action_id = request.steps[0].action_id.as_str();
            return Ok(CreatePlanResponse::ManualRunbook {
                runbook: manual_runbook(
                    auth,
                    request,
                    action_id,
                    "Manual operator runbook",
                    RULES_ONLY_NOT_EXECUTABLE,
                    "The diagnosis is rules-only and cannot authorize an executable plan.",
                ),
            });
        }
        if context.status != "confirmed" {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_not_confirmed",
                "only a confirmed diagnosis can create an action plan",
            ));
        }
        let primary_model_invocation_id = context.primary_model_invocation_id.ok_or_else(|| {
            ControlPlaneError::conflict_code(
                RULES_ONLY_NOT_EXECUTABLE,
                "execution-eligible diagnosis has no primary model invocation",
            )
        })?;
        if context.partial {
            return Err(ControlPlaneError::conflict_code(
                "diagnosis_partial",
                "a partial diagnosis cannot create an action plan",
            ));
        }

        let now = self.now();
        let expires_at = validated_plan_expiry(now, request.expires_at, self.policy.plan_ttl_seconds())?;
        let version = self
            .repository
            .next_action_plan_version(auth, request.incident_id)
            .await?;
        let evidence = self
            .load_candidate_evidence(auth, &context.evidence_ids, &request.steps)
            .await?;
        let evidence_hash = evidence_hash(&evidence)?;
        let mut risks = Vec::with_capacity(request.steps.len());
        let mut steps = Vec::with_capacity(request.steps.len());
        for (index, candidate) in request.steps.iter().enumerate() {
            let CatalogResolution::Supervised(action, descriptor) = self.catalog.resolve(&candidate.action_id)? else {
                return Err(ControlPlaneError::validation(
                    "mixed_manual_and_execution_plan",
                    "manual-only and executable actions cannot share one plan",
                ));
            };
            if descriptor.version != candidate.descriptor_version {
                return Err(ControlPlaneError::validation(
                    "action_version_mismatch",
                    "candidate descriptor version is not the active exact version",
                ));
            }
            validate_parameters(descriptor, &candidate.parameters)?;
            risks.push(descriptor.risk);
            steps.push(PlanStep {
                id: PlanStepId::new(),
                sequence: u16::try_from(index + 1)
                    .map_err(|_| ControlPlaneError::validation("invalid_plan", "plan step sequence exceeds u16"))?,
                action,
                descriptor_version: descriptor.version.clone(),
                resource: candidate.resource.clone(),
                parameters: candidate.parameters.clone(),
                evidence_ids: candidate.evidence_ids.clone(),
                precondition_hash: step_precondition_hash(&candidate.evidence_ids, &evidence)?,
                max_impact: descriptor.max_impact,
                verification: descriptor.verification.clone(),
                compensation: descriptor.compensation.clone(),
            });
        }
        let risk = aggregate_risk(&risks)?;
        let draft = ActionPlanDraft {
            id: ActionPlanId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            incident_id: request.incident_id,
            diagnosis_revision: request.diagnosis_revision_id,
            primary_model_invocation_id,
            diagnosis_execution_eligible: true,
            version,
            created_by: auth.subject.clone(),
            created_at: now,
            expires_at,
            evidence_hash,
            steps,
        };
        let mut plan = ActionPlan::seal(draft)
            .map_err(|error| ControlPlaneError::validation("invalid_plan", error.to_string()))?;
        let live = self.live_plan_state(auth, &plan, now).await?;
        let decision = self.policy.evaluate(auth, &plan, &risks, live.facts, now)?;
        plan = if decision.effect == PolicyEffect::RequireApproval {
            plan.submit_for_review(now, risk == ActionRisk::R2)
                .map_err(|error| ControlPlaneError::conflict_code("plan_state_changed", error.to_string()))?
        } else {
            plan.status = PlanStatus::Rejected;
            plan
        };
        let audits = plan_creation_audits(auth, &plan, &decision, correlation_id, now);
        self.repository
            .persist_plan_bundle(&plan, risk, &decision, &audits)
            .await?;
        self.publish_audits(&audits);
        Ok(CreatePlanResponse::ActionPlan {
            plan: Box::new(plan),
            risk,
            policy_decision: decision,
        })
    }

    pub(crate) async fn plan(&self, auth: &AuthContext, id: ActionPlanId) -> Result<ActionPlanView, ControlPlaneError> {
        let projection = self.repository.supervised_plan(auth, id).await?;
        let latest_critic_review = self.repository.latest_critic_review(auth, &projection.plan).await?;
        let critic_state = critic_gate_state(projection.risk, latest_critic_review.as_ref());
        let latest_policy_decision = self.repository.latest_policy_decision(auth, &projection.plan).await?;
        let latest_approval = self.repository.latest_approval(auth, &projection.plan).await?;
        Ok(ActionPlanView {
            plan: projection.plan,
            risk: projection.risk,
            critic_state,
            latest_critic_review,
            latest_policy_decision,
            latest_approval,
        })
    }

    async fn ensure_current_policy(&self, auth: &AuthContext, plan: &ActionPlan) -> Result<(), ControlPlaneError> {
        let decision = self
            .repository
            .latest_policy_decision(auth, plan)
            .await?
            .ok_or_else(|| {
                ControlPlaneError::conflict_code("policy_missing", "plan has no persisted policy decision")
            })?;
        if decision.plan_hash != plan.plan_hash
            || decision.policy_version != self.policy.version()
            || decision.effect != PolicyEffect::RequireApproval
        {
            return Err(ControlPlaneError::conflict_code(
                "policy_decision_invalidated",
                "current policy does not permit the plan to proceed to human approval",
            ));
        }
        Ok(())
    }

    async fn load_candidate_evidence(
        &self,
        auth: &AuthContext,
        diagnosis_evidence: &[EvidenceId],
        steps: &[CandidatePlanStep],
    ) -> Result<BTreeMap<EvidenceId, EvidenceSnapshot>, ControlPlaneError> {
        let allowed = diagnosis_evidence.iter().copied().collect::<BTreeSet<_>>();
        let requested = steps
            .iter()
            .flat_map(|step| step.evidence_ids.iter().copied())
            .collect::<BTreeSet<_>>();
        if requested.is_empty() || !requested.is_subset(&allowed) {
            return Err(ControlPlaneError::validation(
                "invalid_evidence_binding",
                "every plan Evidence ID must belong to the confirmed diagnosis",
            ));
        }
        let mut evidence = BTreeMap::new();
        for id in requested {
            let snapshot = self.repository.evidence(auth, id).await?;
            snapshot.verify_content_hash().map_err(|_| {
                ControlPlaneError::conflict_code(
                    "invalid_content_hash",
                    "stored Evidence content hash verification failed",
                )
            })?;
            evidence.insert(id, snapshot);
        }
        Ok(evidence)
    }

    async fn live_plan_state(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
        now: DateTime<Utc>,
    ) -> Result<LivePlanState, ControlPlaneError> {
        plan.verify_plan_hash()
            .map_err(|error| ControlPlaneError::conflict_code("plan_hash_mismatch", error.to_string()))?;
        let candidates = plan
            .steps
            .iter()
            .map(|step| CandidatePlanStep {
                action_id: step.action.id().to_owned(),
                descriptor_version: step.descriptor_version.clone(),
                resource: step.resource.clone(),
                parameters: step.parameters.clone(),
                evidence_ids: step.evidence_ids.clone(),
            })
            .collect::<Vec<_>>();
        let evidence_ids = candidates
            .iter()
            .flat_map(|step| step.evidence_ids.iter().copied())
            .collect::<Vec<_>>();
        let mut evidence = BTreeMap::new();
        for id in evidence_ids {
            if let std::collections::btree_map::Entry::Vacant(entry) = evidence.entry(id) {
                let bound = self.repository.evidence(auth, id).await?;
                let snapshot = self
                    .repository
                    .latest_cluster_source_evidence(auth, bound.cluster_id, &bound.source, &bound.resource)
                    .await?
                    .ok_or_else(|| {
                        ControlPlaneError::conflict_code(
                            "source_unavailable",
                            "no current Evidence exists for the plan resource",
                        )
                    })?;
                snapshot.verify_content_hash().map_err(|_| {
                    ControlPlaneError::conflict_code(
                        "invalid_content_hash",
                        "stored Evidence content hash verification failed",
                    )
                })?;
                entry.insert(snapshot);
            }
        }
        let mut risks = Vec::with_capacity(plan.steps.len());
        let mut resource_quarantined = false;
        let mut resource_busy = false;
        let mut rollback_available = true;
        let mut execution_supported = true;
        for step in &plan.steps {
            let descriptor = self.catalog.descriptor(step.action)?;
            if descriptor.version != step.descriptor_version
                || descriptor.max_impact != step.max_impact
                || descriptor.verification != step.verification
                || descriptor.compensation != step.compensation
            {
                return Err(ControlPlaneError::conflict_code(
                    "action_descriptor_changed",
                    "plan no longer matches the exact action descriptor",
                ));
            }
            validate_parameters(descriptor, &step.parameters)?;
            risks.push(descriptor.risk);
            rollback_available &= descriptor.compensation.mode != CompensationMode::NotAvailable;
            execution_supported &= descriptor.execution_supported;
            resource_quarantined |= self
                .repository
                .resource_is_quarantined(auth, plan.cluster_id, &step.resource, step.action)
                .await?;
            resource_busy |= self
                .repository
                .resource_has_active_change(auth, plan.cluster_id, &step.resource)
                .await?;
            if step.precondition_hash != step_precondition_hash(&step.evidence_ids, &evidence)? {
                return Err(ControlPlaneError::conflict_code(
                    "precondition_changed",
                    "step precondition no longer matches its Evidence set",
                ));
            }
        }
        let evidence_current = !evidence.is_empty()
            && evidence
                .values()
                .map(|snapshot| self.evidence_is_current(snapshot, now))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .all(std::convert::identity);
        let precondition_hash = plan
            .compute_precondition_hash()
            .map_err(|error| ControlPlaneError::validation("invalid_precondition_hash", error.to_string()))?;
        Ok(LivePlanState {
            evidence_hash: evidence_hash(&evidence)?,
            precondition_hash,
            risks,
            facts: PolicyFacts {
                diagnosis_confirmed: true,
                diagnosis_execution_eligible: plan.diagnosis_execution_eligible,
                evidence_current,
                resource_quarantined,
                resource_busy,
                maintenance_window_open: self.policy.maintenance_window_open(now),
                rollback_available,
            },
            execution_supported,
        })
    }

    fn evidence_is_current(&self, snapshot: &EvidenceSnapshot, now: DateTime<Utc>) -> Result<bool, ControlPlaneError> {
        if snapshot.observed_at > now
            || snapshot.partial
            || snapshot.coverage != CoverageStatus::Available
            || snapshot.freshness_seconds == 0
        {
            return Ok(false);
        }
        let allowed_seconds = snapshot.freshness_seconds.min(self.policy.max_evidence_age_seconds());
        let allowed = duration_seconds(allowed_seconds)?;
        Ok(now <= snapshot.observed_at + allowed)
    }

    fn publish_audits(&self, audits: &[AuditEvent]) {
        for audit in audits {
            self.workflow.publish_external(WorkflowStreamEvent {
                tenant_id: audit.tenant_id,
                cluster_id: audit.cluster_id,
                aggregate_type: "supervised_change",
                aggregate_id: audit.resource_id.clone(),
                event_type: audit_event_name(audit.event_kind),
                payload: json!({
                    "event_id": audit.id,
                    "resource_kind": audit.resource_kind,
                    "reason_code": audit.reason_code,
                }),
                correlation_id: audit.correlation_id,
                occurred_at: audit.occurred_at,
            });
        }
    }
}
