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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionStepId;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerificationOutcome;
use rocketmq_sre_contracts::VerificationResult;
use rocketmq_sre_contracts::VerificationSpec;

use crate::ExecutorError;

const MAX_CONDITIONS: usize = 64;
const DEFAULT_MAX_OBSERVATIONS: usize = 10_000;

/// Evidence position in a supervised action timeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationPhase {
    Pre,
    During,
    Post,
    RollbackPost,
}

/// Bounded request sent to a read-only verification source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationCaptureRequest {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub plan_step_id: PlanStepId,
    pub action: ExecutionAction,
    pub target: String,
    pub phase: VerificationPhase,
    pub resource_conditions: Vec<String>,
    pub technical_slis: Vec<String>,
}

/// One immutable observation with resource and technical-SLI outcomes.
#[derive(Clone, Debug, PartialEq)]
pub struct VerificationObservation {
    pub evidence: EvidenceSnapshot,
    pub resource_conditions: BTreeMap<String, bool>,
    pub technical_slis: BTreeMap<String, bool>,
}

/// Completed verification plus every post-change snapshot captured while
/// proving the descriptor-defined stability window.
#[derive(Clone, Debug, PartialEq)]
pub struct VerificationRun {
    pub result: VerificationResult,
    pub post_evidence: Vec<EvidenceSnapshot>,
}

pub type VerificationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<VerificationObservation, ExecutorError>> + Send + 'a>>;

/// Read-only resource/SLI source. It cannot mutate the target.
pub trait VerificationSource: Send + Sync {
    fn observe<'a>(&'a self, request: &'a VerificationCaptureRequest) -> VerificationFuture<'a>;
}

/// Deterministic descriptor-driven verifier.
#[derive(Clone)]
pub struct ExecutionVerifier {
    source: Arc<dyn VerificationSource>,
    poll_interval: Duration,
    max_observations: usize,
}

impl ExecutionVerifier {
    #[must_use]
    pub fn new(source: Arc<dyn VerificationSource>, poll_interval: Duration) -> Self {
        Self {
            source,
            poll_interval,
            max_observations: DEFAULT_MAX_OBSERVATIONS,
        }
    }

    #[cfg(test)]
    fn with_max_observations(mut self, max_observations: usize) -> Self {
        self.max_observations = max_observations;
        self
    }

    /// Captures and validates one pre/during observation.
    ///
    /// # Errors
    ///
    /// Rejects empty verification templates, scope drift, malformed Evidence,
    /// unbounded condition maps, and source failures.
    pub async fn capture(
        &self,
        request: &VerificationCaptureRequest,
    ) -> Result<VerificationObservation, ExecutorError> {
        validate_request(request)?;
        let observation = self.source.observe(request).await?;
        validate_observation(request, &observation)?;
        Ok(observation)
    }

    /// Polls post-change observations until every descriptor condition remains
    /// satisfied for the full stability window or the maximum wait expires.
    ///
    /// # Errors
    ///
    /// Rejects invalid descriptor windows, malformed Evidence, scope drift,
    /// source failures, and observation streams that cannot advance safely.
    pub async fn verify_post(
        &self,
        request: &VerificationCaptureRequest,
        spec: &VerificationSpec,
        started_at: DateTime<Utc>,
        pre_evidence_ids: Vec<rocketmq_sre_contracts::EvidenceId>,
        during_evidence_ids: Vec<rocketmq_sre_contracts::EvidenceId>,
    ) -> Result<VerificationRun, ExecutorError> {
        if request.phase != VerificationPhase::Post
            || spec.resource_conditions != request.resource_conditions
            || spec.technical_slis != request.technical_slis
            || spec.max_wait_seconds == 0
            || spec.stable_window_seconds > spec.max_wait_seconds
        {
            return Err(ExecutorError::InvalidRequest);
        }
        validate_request(request)?;
        let max_wait = i64::try_from(spec.max_wait_seconds).map_err(|_| ExecutorError::InvalidRequest)?;
        let stable_window = i64::try_from(spec.stable_window_seconds).map_err(|_| ExecutorError::InvalidRequest)?;
        let deadline = started_at
            .checked_add_signed(TimeDelta::seconds(max_wait))
            .ok_or(ExecutorError::InvalidRequest)?;
        let mut stable_since = None;
        let mut post_evidence = Vec::new();
        let mut satisfied_conditions = Vec::new();
        let mut failed_conditions = Vec::new();
        let mut inconclusive = false;
        let mut completed_at = started_at;

        for observation_index in 0..self.max_observations {
            let observation = self.source.observe(request).await?;
            validate_observation(request, &observation)?;
            completed_at = observation.evidence.observed_at;
            let evaluation = evaluate_conditions(request, &observation);
            satisfied_conditions = evaluation.satisfied;
            failed_conditions = evaluation.failed;
            inconclusive = evaluation.inconclusive;
            post_evidence.push(observation.evidence);

            if failed_conditions.is_empty() && !inconclusive {
                let since = stable_since.get_or_insert(completed_at);
                if completed_at.signed_duration_since(*since) >= TimeDelta::seconds(stable_window) {
                    return Ok(VerificationRun {
                        result: VerificationResult {
                            step_id: request.step_id,
                            outcome: VerificationOutcome::Succeeded,
                            started_at,
                            completed_at,
                            pre_evidence_ids,
                            during_evidence_ids,
                            post_evidence_ids: post_evidence.iter().map(|item| item.evidence_id).collect(),
                            satisfied_conditions,
                            failed_conditions: Vec::new(),
                            stable_window_seconds: spec.stable_window_seconds,
                        },
                        post_evidence,
                    });
                }
            } else {
                stable_since = None;
            }

            if completed_at >= deadline || observation_index + 1 == self.max_observations {
                break;
            }
            tokio::time::sleep(self.poll_interval).await;
        }

        Ok(VerificationRun {
            result: VerificationResult {
                step_id: request.step_id,
                outcome: if inconclusive {
                    VerificationOutcome::Inconclusive
                } else {
                    VerificationOutcome::Failed
                },
                started_at,
                completed_at,
                pre_evidence_ids,
                during_evidence_ids,
                post_evidence_ids: post_evidence.iter().map(|item| item.evidence_id).collect(),
                satisfied_conditions,
                failed_conditions,
                stable_window_seconds: spec.stable_window_seconds,
            },
            post_evidence,
        })
    }
}

struct ConditionEvaluation {
    satisfied: Vec<String>,
    failed: Vec<String>,
    inconclusive: bool,
}

fn validate_request(request: &VerificationCaptureRequest) -> Result<(), ExecutorError> {
    if request.target.trim().is_empty()
        || request.resource_conditions.is_empty()
        || request.technical_slis.is_empty()
        || request.resource_conditions.len() + request.technical_slis.len() > MAX_CONDITIONS
        || request
            .resource_conditions
            .iter()
            .chain(&request.technical_slis)
            .any(|condition| condition.trim().is_empty() || condition.len() > 128)
    {
        return Err(ExecutorError::InvalidRequest);
    }
    Ok(())
}

fn validate_observation(
    request: &VerificationCaptureRequest,
    observation: &VerificationObservation,
) -> Result<(), ExecutorError> {
    let evidence = &observation.evidence;
    evidence
        .verify_content_hash()
        .map_err(|_| ExecutorError::AgentRejected)?;
    if evidence.tenant_id != request.tenant_id
        || evidence.cluster_id != request.cluster_id
        || evidence.correlation_id != request.correlation_id
        || evidence.resource != request.target
        || observation.resource_conditions.len() > MAX_CONDITIONS
        || observation.technical_slis.len() > MAX_CONDITIONS
    {
        return Err(ExecutorError::AgentRejected);
    }
    Ok(())
}

fn evaluate_conditions(
    request: &VerificationCaptureRequest,
    observation: &VerificationObservation,
) -> ConditionEvaluation {
    let mut satisfied = Vec::new();
    let mut failed = Vec::new();
    let mut inconclusive = observation.evidence.partial || observation.evidence.coverage != CoverageStatus::Available;
    for (prefix, expected, actual) in [
        (
            "resource",
            request.resource_conditions.as_slice(),
            &observation.resource_conditions,
        ),
        ("sli", request.technical_slis.as_slice(), &observation.technical_slis),
    ] {
        for condition in expected {
            let name = format!("{prefix}:{condition}");
            match actual.get(condition) {
                Some(true) => satisfied.push(name),
                Some(false) => failed.push(name),
                None => {
                    failed.push(name);
                    inconclusive = true;
                }
            }
        }
    }
    ConditionEvaluation {
        satisfied,
        failed,
        inconclusive,
    }
}

#[cfg(test)]
#[path = "verifier_tests.rs"]
mod tests;
