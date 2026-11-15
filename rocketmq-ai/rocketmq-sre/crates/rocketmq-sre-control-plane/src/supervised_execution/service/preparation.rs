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

use rocketmq_sre_contracts::ACTION_PREPARATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::ActionPreparationBinding;
use rocketmq_sre_contracts::ActionPreparationEvidence;
use rocketmq_sre_contracts::ActionReadinessEvidenceBinding;
use rocketmq_sre_contracts::ActionReadinessRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::action_evidence_valid_until;

use super::*;
use crate::supervised_execution::model::DiagnosisPlanContext;
use crate::supervised_execution::repository::ActionAdmission;
use crate::supervised_execution::repository::require_current_diagnosis;
use crate::supervised_execution::target_scope::TargetScopeCandidate;

pub(super) struct PreparationCandidate<'a> {
    pub(super) context: &'a DiagnosisPlanContext,
    pub(super) action_id: &'a str,
    pub(super) descriptor_version: &'a str,
    pub(super) resource: &'a str,
    pub(super) parameters: &'a serde_json::Value,
    pub(super) actor: &'a str,
}

pub(super) struct ValidPreparation {
    pub(super) proof: ActionPreparationEvidence,
    pub(super) valid_until: DateTime<Utc>,
}

impl SupervisedExecutionService {
    pub(super) fn target_scope_digest(
        &self,
        candidate: &PreparationCandidate<'_>,
    ) -> Result<Option<String>, ControlPlaneRequestFailure> {
        self.target_scope.matched_digest(TargetScopeCandidate {
            tenant_id: candidate.context.tenant_id,
            cluster_id: candidate.context.cluster_id,
            action_id: candidate.action_id,
            descriptor_version: candidate.descriptor_version,
            incident_resource: candidate.context.incident_resource.as_deref(),
            target: candidate.resource,
            parameters: candidate.parameters,
        })
    }

    pub(super) async fn diagnostic_evidence(
        &self,
        auth: &AuthContext,
        context: &DiagnosisPlanContext,
    ) -> Result<BTreeMap<EvidenceId, EvidenceSnapshot>, ControlPlaneRequestFailure> {
        let ids = context.evidence_ids.iter().copied().collect::<BTreeSet<_>>();
        if ids.is_empty() || ids.len() > MAX_STEP_EVIDENCE || ids.len() != context.evidence_ids.len() {
            return Err(preparation_failure("diagnosis_evidence_missing"));
        }
        let mut evidence = BTreeMap::new();
        for id in ids {
            let snapshot = self.repository.evidence(auth, id).await?;
            if snapshot.cluster_id != context.cluster_id
                || !self
                    .repository
                    .evidence_linked_to_incident(auth, context.incident_id, id)
                    .await?
            {
                return Err(preparation_failure("preparation_binding_mismatch"));
            }
            evidence.insert(id, snapshot);
        }
        Ok(evidence)
    }

    pub(super) fn validate_preparation(
        &self,
        snapshot: &EvidenceSnapshot,
        candidate: &PreparationCandidate<'_>,
        diagnostic: &BTreeMap<EvidenceId, EvidenceSnapshot>,
        now: DateTime<Utc>,
    ) -> Result<ValidPreparation, ControlPlaneRequestFailure> {
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return Err(preparation_failure("preparation_binding_missing"));
        };
        let proof: ActionPreparationEvidence =
            serde_json::from_value(content.clone()).map_err(|_| preparation_failure("preparation_binding_missing"))?;
        self.signer
            .verify_preparation(&proof)
            .map_err(|_| preparation_failure("preparation_origin_invalid"))?;
        let binding = &proof.candidate_binding;
        let valid_until = self.evidence_deadline(snapshot, now)?;
        let scope_digest = self
            .target_scope_digest(candidate)?
            .ok_or_else(|| preparation_failure("target_scope_unverified"))?;
        if binding.target_scope_digest != scope_digest {
            return Err(preparation_failure("target_scope_changed"));
        }
        if proof.evidence_id != snapshot.evidence_id
            || snapshot.tenant_id != candidate.context.tenant_id
            || snapshot.cluster_id != candidate.context.cluster_id
            || snapshot.source != "execution-agent"
            || snapshot.exposure != EvidenceExposure::ExecutionAgentApi
            || snapshot.resource != candidate.resource
            || snapshot.freshness_seconds != 0
            || snapshot.schema != current_evidence_schema()
            || binding.tenant_id != snapshot.tenant_id
            || binding.cluster_id != snapshot.cluster_id
            || binding.incident_id != candidate.context.incident_id
            || binding.diagnosis_revision_id != candidate.context.diagnosis_revision_id
            || binding.action_id != candidate.action_id
            || binding.descriptor_version != candidate.descriptor_version
            || binding.resource != candidate.resource
            || binding.actor_subject != candidate.actor
            || binding.parameters_digest != digest(candidate.parameters)?
            || binding.policy_version != self.policy.version()
            || binding.prepared_at > now
            || binding.valid_until < binding.prepared_at
            || binding.valid_until < now
            || binding.prepared_at != persistence_time(binding.prepared_at)?
            || binding.valid_until != persistence_time(binding.valid_until)?
            || proof.agent_result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
            || proof.agent_result.action.id() != candidate.action_id
            || proof.agent_result.target != candidate.resource
            || !rocketmq_sre_contracts::is_sha256_digest(&proof.agent_result.precondition_hash)
            || proof.agent_result.observed_at > now
            || persistence_time(proof.agent_result.observed_at)? > binding.prepared_at
            || snapshot.observed_at != persistence_time(proof.agent_result.observed_at)?
            || snapshot.time_range.start != snapshot.observed_at
            || snapshot.time_range.end != snapshot.observed_at
        {
            return Err(preparation_failure("preparation_binding_mismatch"));
        }
        let available_bindings = diagnostic_bindings(diagnostic);
        if binding.diagnostic_evidence != available_bindings {
            let expected_ids = candidate.context.evidence_ids.iter().copied().collect::<BTreeSet<_>>();
            let bound_ids = binding
                .diagnostic_evidence
                .iter()
                .map(|value| value.evidence_id)
                .collect::<BTreeSet<_>>();
            // An unavailable diagnostic record supplies no current proof. After
            // independent provenance/candidate checks, its absence is not an
            // observed hash or binding mismatch. Normal admission loads the
            // complete pool first; this also explains incomplete readonly views.
            if diagnostic.len() < expected_ids.len()
                && expected_ids.len() == candidate.context.evidence_ids.len()
                && bound_ids == expected_ids
                && bound_ids.len() == binding.diagnostic_evidence.len()
                && available_bindings
                    .iter()
                    .all(|value| binding.diagnostic_evidence.contains(value))
            {
                return Err(preparation_failure("diagnosis_evidence_missing"));
            }
            return Err(preparation_failure("preparation_binding_mismatch"));
        }
        // Neither a new Evidence ID nor a later collection can renew the original
        // Agent observation or any diagnosis evidence sealed into the preparation.
        let source_deadline = proof
            .agent_result
            .observed_at
            .checked_add_signed(duration_seconds(EXECUTION_PRECONDITION_VALIDITY_SECONDS)?)
            .ok_or_else(|| preparation_failure("preparation_expired"))?;
        if binding.valid_until > persistence_time(source_deadline)? {
            return Err(preparation_failure("preparation_binding_mismatch"));
        }
        let mut deadline = valid_until.min(binding.valid_until).min(source_deadline);
        for evidence in diagnostic.values() {
            deadline = deadline.min(self.evidence_deadline(evidence, now)?);
        }
        Ok(ValidPreparation {
            proof,
            valid_until: deadline,
        })
    }

    pub(super) fn evidence_deadline(
        &self,
        snapshot: &EvidenceSnapshot,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
        self.evidence_readiness_deadline(snapshot, now)
            .map_err(|_| preparation_failure("action_evidence_stale_or_invalid"))
    }

    pub(super) fn evidence_readiness_deadline(
        &self,
        snapshot: &EvidenceSnapshot,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, rocketmq_sre_contracts::ActionReadinessReason> {
        let maximum = if snapshot.source == "execution-agent" {
            EXECUTION_PRECONDITION_VALIDITY_SECONDS.min(self.policy.max_evidence_age_seconds())
        } else {
            self.policy.max_evidence_age_seconds()
        };
        action_evidence_valid_until(snapshot, now, maximum)
    }

    pub(super) fn action_admission(
        &self,
        context: &DiagnosisPlanContext,
        evidence: &BTreeMap<EvidenceId, EvidenceSnapshot>,
        limit: DateTime<Utc>,
    ) -> Result<ActionAdmission, ControlPlaneRequestFailure> {
        require_current_diagnosis(context)?;
        let now = self.now();
        let mut valid_until = limit;
        for snapshot in evidence.values() {
            valid_until = valid_until.min(self.evidence_deadline(snapshot, now)?);
        }
        Ok(ActionAdmission {
            tenant_id: context.tenant_id,
            cluster_id: context.cluster_id,
            incident_id: context.incident_id,
            diagnosis_revision_id: context.diagnosis_revision_id,
            valid_until,
            expires_before: None,
            evidence: evidence
                .iter()
                .map(|(id, snapshot)| (*id, snapshot.content_hash.clone()))
                .collect(),
            clock: self.clock.clone(),
            policy: self.policy.clone(),
        })
    }

    pub(in crate::supervised_execution) async fn admission_for_plan(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
    ) -> Result<ActionAdmission, ControlPlaneRequestFailure> {
        let context = self
            .repository
            .diagnosis_plan_context(auth, plan.cluster_id, plan.incident_id, plan.diagnosis_revision)
            .await?;
        require_current_diagnosis(&context)?;
        let diagnostic = self.diagnostic_evidence(auth, &context).await?;
        let mut evidence = diagnostic.clone();
        let mut deadline = plan.expires_at;
        for step in &plan.steps {
            let candidate = PreparationCandidate {
                context: &context,
                action_id: step.action.id(),
                descriptor_version: &step.descriptor_version,
                resource: &step.resource,
                parameters: &step.parameters,
                actor: &plan.created_by,
            };
            let mut prepared = 0;
            for id in &step.evidence_ids {
                let snapshot = self.repository.evidence(auth, *id).await?;
                if snapshot.cluster_id != plan.cluster_id
                    || !self
                        .repository
                        .evidence_linked_to_incident(auth, plan.incident_id, *id)
                        .await?
                {
                    return Err(preparation_failure("preparation_binding_mismatch"));
                }
                if snapshot.source == "execution-agent" {
                    let valid = self.validate_preparation(&snapshot, &candidate, &diagnostic, self.now())?;
                    if !valid.proof.agent_result.ready
                        || !valid.proof.agent_result.reason_codes.is_empty()
                        || step.precondition_hash != valid.proof.agent_result.precondition_hash
                    {
                        return Err(preparation_failure("execution_precondition_not_ready"));
                    }
                    deadline = deadline.min(valid.valid_until);
                    prepared += 1;
                } else if !diagnostic.contains_key(id) {
                    return Err(preparation_failure("preparation_binding_mismatch"));
                }
                evidence.insert(*id, snapshot);
            }
            if prepared != 1 {
                return Err(preparation_failure("preparation_binding_missing"));
            }
        }
        let mut admission = self.action_admission(&context, &evidence, deadline)?;
        admission.expires_before = Some(plan.expires_at);
        Ok(admission)
    }

    pub(crate) async fn prepare_execution_precondition(
        &self,
        auth: &AuthContext,
        incident_id: rocketmq_sre_contracts::IncidentId,
        request: &PrepareExecutionPreconditionRequest,
        correlation_id: CorrelationId,
    ) -> Result<ExecutionPreconditionEvidenceView, ControlPlaneRequestFailure> {
        self.policy.require_operator(auth)?;
        require_cluster(auth, request.cluster_id)?;
        let context = self
            .repository
            .diagnosis_plan_context(auth, request.cluster_id, incident_id, request.diagnosis_revision_id)
            .await?;
        require_current_diagnosis(&context)?;
        let CatalogResolution::Supervised(action, descriptor) = self.catalog.resolve(&request.action_id)? else {
            return Err(preparation_failure("action_not_executable"));
        };
        if descriptor.version != request.descriptor_version || !descriptor.execution_supported {
            return Err(preparation_failure("action_version_mismatch"));
        }
        validate_parameters(descriptor, &request.parameters)?;
        let candidate = PreparationCandidate {
            context: &context,
            action_id: &request.action_id,
            descriptor_version: &request.descriptor_version,
            resource: &request.resource,
            parameters: &request.parameters,
            actor: &auth.subject,
        };
        let target_scope_digest = self
            .target_scope_digest(&candidate)?
            .ok_or_else(|| preparation_failure("target_scope_unverified"))?;
        let diagnostic = self.diagnostic_evidence(auth, &context).await?;
        self.action_admission(&context, &diagnostic, self.now() + Duration::seconds(30))?;
        let result: AgentReadResult = self
            .executor
            .read_precondition(&AgentReadRequest {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                tenant_id: auth.tenant_id,
                cluster_id: request.cluster_id,
                execution_id: ExecutionId::new(),
                plan_step_id: PlanStepId::new(),
                action,
                descriptor_version: request.descriptor_version.clone(),
                target: request.resource.clone(),
                parameters: request.parameters.clone(),
            })
            .await?;
        let now = self.now();
        if result.observed_at > now {
            return Err(preparation_failure("action_evidence_stale_or_invalid"));
        }
        let observed_at = persistence_time(result.observed_at)?;
        let source_deadline = result
            .observed_at
            .checked_add_signed(duration_seconds(EXECUTION_PRECONDITION_VALIDITY_SECONDS)?)
            .ok_or_else(|| preparation_failure("preparation_expired"))?;
        let admission = self.action_admission(&context, &diagnostic, persistence_time(source_deadline)?)?;
        let evidence_id = EvidenceId::new();
        let precondition_hash = result.precondition_hash.clone();
        let mut proof = ActionPreparationEvidence {
            schema_version: ACTION_PREPARATION_SCHEMA_VERSION.to_owned(),
            evidence_id,
            candidate_binding: ActionPreparationBinding {
                tenant_id: auth.tenant_id,
                cluster_id: request.cluster_id,
                incident_id,
                diagnosis_revision_id: request.diagnosis_revision_id,
                action_id: request.action_id.clone(),
                descriptor_version: request.descriptor_version.clone(),
                resource: request.resource.clone(),
                parameters_digest: digest(&request.parameters)?,
                target_scope_digest,
                diagnostic_evidence: diagnostic_bindings(&diagnostic),
                policy_version: self.policy.version().to_owned(),
                actor_subject: auth.subject.clone(),
                prepared_at: persistence_time(now)?,
                valid_until: persistence_time(admission.valid_until)?,
            },
            agent_result: result,
            origin_signature: String::new(),
        };
        self.signer.seal_preparation(&mut proof)?;
        let content =
            serde_json::to_value(&proof).map_err(|_| preparation_failure("invalid_execution_precondition"))?;
        let content_digest = digest(&content)?;
        let mut snapshot = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id,
                tenant_id: auth.tenant_id,
                cluster_id: request.cluster_id,
                source: "execution-agent".to_owned(),
                resource: request.resource.clone(),
                time_range: TimeRange::new(observed_at, observed_at)
                    .map_err(|_| preparation_failure("invalid_execution_precondition"))?,
            },
            current_evidence_schema(),
            observed_at,
            EvidenceContent::Inline(content),
        )
        .map_err(|_| preparation_failure("invalid_execution_precondition"))?;
        snapshot.evidence_id = evidence_id;
        snapshot.sensitivity = Sensitivity::Internal;
        snapshot.exposure = EvidenceExposure::ExecutionAgentApi;
        let mut transaction = self.repository.pool.begin().await?;
        admission.lock_and_check(&mut transaction).await?;
        let evidence = self
            .repository
            .persist_evidence_in(
                auth,
                &snapshot,
                None,
                Some(incident_id),
                &content_digest,
                &mut transaction,
            )
            .await?;
        transaction.commit().await?;
        let readiness = self
            .action_readiness(
                auth,
                incident_id,
                &ActionReadinessRequest {
                    cluster_id: request.cluster_id,
                    diagnosis_revision_id: request.diagnosis_revision_id,
                    action_id: request.action_id.clone(),
                    descriptor_version: request.descriptor_version.clone(),
                    resource: request.resource.clone(),
                    parameters: request.parameters.clone(),
                    precondition_evidence_id: Some(evidence_id),
                    plan_id: None,
                    plan_step_id: None,
                },
            )
            .await?;
        Ok(ExecutionPreconditionEvidenceView {
            schema_version: EXECUTION_PRECONDITION_EVIDENCE_SCHEMA,
            incident_id,
            diagnosis_revision_id: request.diagnosis_revision_id,
            evidence,
            precondition_hash,
            readiness: Some(readiness),
        })
    }
}

pub(super) fn diagnostic_bindings(
    evidence: &BTreeMap<EvidenceId, EvidenceSnapshot>,
) -> Vec<ActionReadinessEvidenceBinding> {
    evidence
        .iter()
        .map(|(id, snapshot)| ActionReadinessEvidenceBinding {
            evidence_id: *id,
            content_hash: snapshot.content_hash.clone(),
        })
        .collect()
}

pub(super) fn digest(value: &impl serde::Serialize) -> Result<String, ControlPlaneRequestFailure> {
    canonical_sha256(value)
        .map_err(|source| ControlPlaneRequestFailure::from(ControlPlaneError::configuration_source(source)))
}

pub(super) fn persistence_time(value: DateTime<Utc>) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
    value
        .with_nanosecond((value.nanosecond() / 1_000) * 1_000)
        .ok_or_else(|| preparation_failure("invalid_execution_precondition"))
}

pub(super) fn preparation_failure(code: &'static str) -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::conflict_code(
        code,
        "the action preparation is missing, stale, or does not bind the current candidate",
    )
}
