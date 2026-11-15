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

use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EXECUTION_VERIFICATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionSliBinding;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::VerificationProofOrigin;
use rocketmq_sre_contracts::canonical_sha256;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_contracts::is_sha256_digest;
use serde_json::json;

use crate::ExecutionAgentClient;
use crate::ExecutionSliClient;
use crate::ExecutorRequestFailure;
use crate::VerificationCaptureRequest;
use crate::VerificationFuture;
use crate::VerificationObservation;
use crate::VerificationPhase;
use crate::VerificationSource;
use crate::VerificationSourceFacts;

/// Production verification source that keeps target resource reads and
/// technical health evaluation on independent read-only boundaries.
#[derive(Clone)]
pub struct ProductionVerificationSource {
    agent: Arc<dyn ExecutionAgentClient>,
    sli_client: Arc<dyn ExecutionSliClient>,
}

impl ProductionVerificationSource {
    #[must_use]
    pub fn new(agent: Arc<dyn ExecutionAgentClient>, sli_client: Arc<dyn ExecutionSliClient>) -> Self {
        Self { agent, sli_client }
    }
}

impl VerificationSource for ProductionVerificationSource {
    fn observe<'a>(&'a self, request: &'a VerificationCaptureRequest) -> VerificationFuture<'a> {
        Box::pin(async move {
            let agent_query = AgentReadRequest {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                execution_id: request.execution_id,
                plan_step_id: request.plan_step_id,
                action: request.action,
                descriptor_version: request.descriptor_version.clone(),
                target: request.target.clone(),
                parameters: request.parameters.clone(),
            };
            let sli_query = ExecutionSliQuery {
                schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                correlation_id: request.correlation_id,
                conditions: request.technical_slis.clone(),
                binding: Some(sli_binding(request)?),
            };
            let (resource, technical) =
                tokio::try_join!(self.agent.precheck(&agent_query), self.sli_client.observe(&sli_query))?;
            assemble_observation(request, resource, technical)
        })
    }
}

fn assemble_observation(
    request: &VerificationCaptureRequest,
    resource: AgentReadResult,
    technical: ExecutionSliObservation,
) -> Result<VerificationObservation, ExecutorRequestFailure> {
    validate_resource(request, &resource)?;
    validate_technical(request, &technical)?;
    // Collection completion cannot stand in for either data source's clock.
    let observed_at = chrono::Utc::now();
    let requested_resource_conditions = request.resource_conditions.iter().collect::<BTreeSet<_>>();
    let resource_conditions = resource
        .resource_conditions
        .into_iter()
        .filter(|(condition, _)| requested_resource_conditions.contains(condition))
        .collect();
    let technical_slis = technical.conditions;
    let expected_binding = sli_binding(request)?;
    let technical_source = technical.source_facts.as_ref();
    let technical_at = technical_source.and_then(|facts| facts.source_observed_at);
    let technical_scope_bound = technical_source
        .is_some_and(|facts| facts.exact_target_conditions && facts.binding.as_ref() == Some(&expected_binding));
    let source_facts = VerificationSourceFacts {
        provenance: VerificationProofOrigin::ProductionRead,
        resource_observed_at: Some(resource.observed_at),
        technical_sli_observed_at: technical_at,
        technical_scope_bound,
        // AgentReadResult does not contain an independently observed physical
        // software version. An operator profile must never fill that gap.
        target_version_observed: None,
    };
    let data_at = technical_at.map(|at| at.min(resource.observed_at));
    let age_nanos = data_at.and_then(|at| observed_at.signed_duration_since(at).num_nanoseconds());
    let clock_valid = age_nanos.is_some_and(|age| age >= 0)
        && resource.observed_at <= observed_at
        && technical_at.is_some_and(|at| at <= observed_at);
    let partial = !technical.complete || !technical_scope_bound || !clock_valid;
    let content = EvidenceContent::Inline(json!({
        "schema_version": "rocketmq-sre.production-verification.v2",
        "action": request.action,
        "phase": phase_name(request.phase),
        "resource_observed_at": resource.observed_at,
        "technical_sli_observed_at": technical_at,
        "technical_sli_evaluated_at": technical.observed_at,
        "resource_precondition_hash": resource.precondition_hash,
        "resource_conditions": resource_conditions,
        "technical_slis": technical_slis,
        "source_evidence_ids": technical.evidence_ids,
        "source_facts": source_facts,
    }));
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: request.correlation_id,
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        source: "execution-verification".to_owned(),
        resource: request.target.clone(),
        // This is a capture-time point. Actual source clocks above are the
        // only clocks the qualified window algorithm may advance with.
        time_range: TimeRange::new(observed_at, observed_at)
            .map_err(|_| ExecutorRequestFailure::VerificationRejected)?,
    };
    let mut evidence = EvidenceSnapshot::capture(query, current_evidence_schema(), observed_at, content)
        .map_err(|_| ExecutorRequestFailure::VerificationRejected)?;
    evidence.sensitivity = Sensitivity::Internal;
    if let Some(age) = age_nanos.filter(|age| *age >= 0) {
        evidence.freshness_seconds = (age as u64).div_ceil(1_000_000_000);
    }
    evidence.partial = partial;
    evidence.coverage = if partial {
        evidence.warnings.push("technical_sli_evidence_incomplete".to_owned());
        CoverageStatus::Partial
    } else {
        CoverageStatus::Available
    };
    Ok(VerificationObservation {
        evidence,
        resource_conditions,
        technical_slis,
        source_facts: Some(source_facts),
    })
}

fn validate_resource(
    request: &VerificationCaptureRequest,
    resource: &AgentReadResult,
) -> Result<(), ExecutorRequestFailure> {
    if resource.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
        || resource.action != request.action
        || resource.target != request.target
        || !is_sha256_digest(&resource.precondition_hash)
        || !required_surface(&request.resource_conditions, &resource.resource_conditions)
    {
        return Err(ExecutorRequestFailure::VerificationRejected);
    }
    Ok(())
}

fn validate_technical(
    request: &VerificationCaptureRequest,
    technical: &ExecutionSliObservation,
) -> Result<(), ExecutorRequestFailure> {
    let expected_binding = sli_binding(request)?;
    if technical.schema_version != EXECUTION_VERIFICATION_SCHEMA_VERSION
        || technical.tenant_id != request.tenant_id
        || technical.cluster_id != request.cluster_id
        || technical.correlation_id != request.correlation_id
        || !exact_surface(&request.technical_slis, &technical.conditions)
        || technical.source_facts.as_ref().is_some_and(|facts| {
            facts
                .binding
                .as_ref()
                .is_some_and(|binding| binding != &expected_binding)
        })
    {
        return Err(ExecutorRequestFailure::VerificationRejected);
    }
    Ok(())
}

fn sli_binding(request: &VerificationCaptureRequest) -> Result<ExecutionSliBinding, ExecutorRequestFailure> {
    Ok(ExecutionSliBinding {
        execution_id: request.execution_id,
        plan_step_id: request.plan_step_id,
        action: request.action,
        descriptor_version: request.descriptor_version.clone(),
        target: request.target.clone(),
        parameters_digest: canonical_sha256(&request.parameters).map_err(|_| ExecutorRequestFailure::InvalidRequest)?,
    })
}

fn exact_surface(expected: &[String], actual: &BTreeMap<String, bool>) -> bool {
    expected.len() == actual.len() && expected.iter().all(|condition| actual.contains_key(condition))
}

fn required_surface(expected: &[String], actual: &BTreeMap<String, bool>) -> bool {
    expected.iter().all(|condition| actual.contains_key(condition))
}

const fn phase_name(phase: VerificationPhase) -> &'static str {
    match phase {
        VerificationPhase::Pre => "pre",
        VerificationPhase::During => "during",
        VerificationPhase::Post => "post",
        VerificationPhase::RollbackPost => "rollback_post",
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::ExecutionAction;
    use rocketmq_sre_contracts::ExecutionId;
    use rocketmq_sre_contracts::ExecutionStepId;
    use rocketmq_sre_contracts::PlanStepId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn independent_resource_and_sli_results_form_canonical_evidence() {
        let request = request();
        let now = Utc::now();
        let observation = assemble_observation(&request, resource(&request, now), technical(&request, now, true))
            .expect("production observation");

        assert_eq!(observation.resource_conditions.get("patch_visible"), Some(&true));
        assert_eq!(observation.technical_slis.get("broker_error_ratio"), Some(&true));
        assert!(!observation.evidence.partial);
        assert_eq!(observation.evidence.coverage, CoverageStatus::Available);
        observation
            .evidence
            .verify_content_hash()
            .expect("canonical evidence hash");
        let serialized = serde_json::to_string(&observation.evidence).expect("serialize evidence");
        assert!(!serialized.contains("super-secret-parameter"));
    }

    #[test]
    fn incomplete_or_scope_drifted_sli_results_fail_closed() {
        let request = request();
        let now = Utc::now();
        let incomplete = assemble_observation(&request, resource(&request, now), technical(&request, now, false))
            .expect("bounded incomplete observation");
        assert!(incomplete.evidence.partial);
        assert_eq!(incomplete.evidence.coverage, CoverageStatus::Partial);

        let mut drifted = technical(&request, now, true);
        drifted.correlation_id = CorrelationId::new();
        assert!(assemble_observation(&request, resource(&request, now), drifted).is_err());
    }

    #[test]
    fn missing_or_extra_condition_surfaces_are_rejected() {
        let request = request();
        let now = Utc::now();
        let mut missing = resource(&request, now);
        missing.resource_conditions.clear();
        assert!(assemble_observation(&request, missing, technical(&request, now, true)).is_err());

        let mut extra = technical(&request, now, true);
        extra.conditions.insert("unapproved_sli".to_owned(), true);
        assert!(assemble_observation(&request, resource(&request, now), extra).is_err());
    }

    #[test]
    fn extra_resource_safety_conditions_are_bounded_to_the_descriptor_surface() {
        let request = request();
        let now = Utc::now();
        let mut resource = resource(&request, now);
        resource
            .resource_conditions
            .insert("independent_safety_precheck".to_owned(), true);

        let observation =
            assemble_observation(&request, resource, technical(&request, now, true)).expect("bounded observation");

        assert_eq!(
            observation.resource_conditions,
            [("patch_visible".to_owned(), true)].into_iter().collect()
        );
    }

    #[test]
    fn fresh_resource_and_evaluation_clocks_do_not_renew_old_or_missing_sli_data() {
        let request = request();
        let now = Utc::now();
        let mut old = technical(&request, now, true);
        old.source_facts.as_mut().unwrap().source_observed_at = Some(now - TimeDelta::seconds(250));
        let observation = assemble_observation(&request, resource(&request, now), old).unwrap();
        assert!(observation.evidence.observed_at >= now);
        assert!(observation.evidence.freshness_seconds >= 250);
        assert_eq!(
            observation.source_facts.as_ref().unwrap().technical_sli_observed_at,
            Some(now - TimeDelta::seconds(250))
        );
        assert!(
            observation
                .source_facts
                .as_ref()
                .unwrap()
                .target_version_observed
                .is_none()
        );

        let mut missing = technical(&request, now, true);
        missing.source_facts = None;
        let observation = assemble_observation(&request, resource(&request, now), missing).unwrap();
        assert!(observation.evidence.partial);
        assert!(observation.source_facts.unwrap().technical_sli_observed_at.is_none());

        let future = assemble_observation(
            &request,
            resource(&request, now + TimeDelta::seconds(1)),
            technical(&request, now, true),
        )
        .unwrap();
        assert!(future.evidence.partial);
    }

    fn request() -> VerificationCaptureRequest {
        VerificationCaptureRequest {
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            correlation_id: CorrelationId::new(),
            execution_id: ExecutionId::new(),
            step_id: ExecutionStepId::new(),
            plan_step_id: PlanStepId::new(),
            action: ExecutionAction::BrokerConfigPatchAllowlisted,
            descriptor_version: "1.0.0".to_owned(),
            target: "broker/broker-a".to_owned(),
            parameters: json!({"access_token": "super-secret-parameter"}),
            phase: VerificationPhase::Post,
            resource_conditions: vec!["patch_visible".to_owned()],
            technical_slis: vec!["broker_error_ratio".to_owned()],
            verification: rocketmq_sre_contracts::VerificationSpec {
                resource_conditions: vec!["patch_visible".to_owned()],
                technical_slis: vec!["broker_error_ratio".to_owned()],
                stable_window_seconds: 30,
                max_wait_seconds: 60,
            },
        }
    }

    fn resource(request: &VerificationCaptureRequest, observed_at: chrono::DateTime<Utc>) -> AgentReadResult {
        AgentReadResult {
            schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            action: request.action,
            target: request.target.clone(),
            precondition_hash: format!("sha256:{}", "1".repeat(64)),
            ready: false,
            reason_codes: vec!["precondition_changed_after_apply".to_owned()],
            resource_conditions: [("patch_visible".to_owned(), true)].into_iter().collect(),
            observed_at,
        }
    }

    fn technical(
        request: &VerificationCaptureRequest,
        observed_at: chrono::DateTime<Utc>,
        complete: bool,
    ) -> ExecutionSliObservation {
        ExecutionSliObservation {
            schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            correlation_id: request.correlation_id,
            conditions: [("broker_error_ratio".to_owned(), true)].into_iter().collect(),
            complete,
            evidence_ids: Vec::new(),
            observed_at,
            source_facts: Some(rocketmq_sre_contracts::ExecutionSliSourceFacts {
                binding: Some(sli_binding(request).expect("fixture binding")),
                source_observed_at: Some(observed_at),
                collected_at: observed_at,
                exact_target_conditions: true,
                reason_codes: Vec::new(),
            }),
        }
    }
}
