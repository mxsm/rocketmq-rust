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
use std::sync::Arc;

use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EXECUTION_VERIFICATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_contracts::is_sha256_digest;
use serde_json::json;

use crate::ExecutionAgentClient;
use crate::ExecutionSliClient;
use crate::ExecutorError;
use crate::VerificationCaptureRequest;
use crate::VerificationFuture;
use crate::VerificationObservation;
use crate::VerificationPhase;
use crate::VerificationSource;

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
) -> Result<VerificationObservation, ExecutorError> {
    validate_resource(request, &resource)?;
    validate_technical(request, &technical)?;
    let observed_at = resource.observed_at.max(technical.observed_at);
    let started_at = resource.observed_at.min(technical.observed_at);
    let resource_conditions = resource.resource_conditions;
    let technical_slis = technical.conditions;
    let partial = !technical.complete;
    let content = EvidenceContent::Inline(json!({
        "schema_version": "rocketmq-sre.production-verification.v1",
        "action": request.action,
        "phase": phase_name(request.phase),
        "resource_observed_at": resource.observed_at,
        "technical_sli_observed_at": technical.observed_at,
        "resource_precondition_hash": resource.precondition_hash,
        "resource_conditions": resource_conditions,
        "technical_slis": technical_slis,
        "source_evidence_ids": technical.evidence_ids,
    }));
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: request.correlation_id,
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        source: "execution-verification".to_owned(),
        resource: request.target.clone(),
        time_range: TimeRange::new(started_at, observed_at).map_err(|_| ExecutorError::VerificationRejected)?,
    };
    let mut evidence = EvidenceSnapshot::capture(query, current_evidence_schema(), observed_at, content)
        .map_err(|_| ExecutorError::VerificationRejected)?;
    evidence.sensitivity = Sensitivity::Internal;
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
    })
}

fn validate_resource(request: &VerificationCaptureRequest, resource: &AgentReadResult) -> Result<(), ExecutorError> {
    if resource.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
        || resource.action != request.action
        || resource.target != request.target
        || !is_sha256_digest(&resource.precondition_hash)
        || !exact_surface(&request.resource_conditions, &resource.resource_conditions)
    {
        return Err(ExecutorError::VerificationRejected);
    }
    Ok(())
}

fn validate_technical(
    request: &VerificationCaptureRequest,
    technical: &ExecutionSliObservation,
) -> Result<(), ExecutorError> {
    if technical.schema_version != EXECUTION_VERIFICATION_SCHEMA_VERSION
        || technical.tenant_id != request.tenant_id
        || technical.cluster_id != request.cluster_id
        || technical.correlation_id != request.correlation_id
        || !exact_surface(&request.technical_slis, &technical.conditions)
    {
        return Err(ExecutorError::VerificationRejected);
    }
    Ok(())
}

fn exact_surface(expected: &[String], actual: &BTreeMap<String, bool>) -> bool {
    expected.len() == actual.len() && expected.iter().all(|condition| actual.contains_key(condition))
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
        let observation = assemble_observation(
            &request,
            resource(&request, now),
            technical(&request, now + TimeDelta::seconds(1), true),
        )
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
        }
    }
}
