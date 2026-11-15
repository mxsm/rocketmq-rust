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
use chrono::Utc;
use rocketmq_runtime::task_group::TaskGroup;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionStepId;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerificationProofOrigin;
use rocketmq_sre_contracts::VerificationResult;
use rocketmq_sre_contracts::VerificationSpec;
use serde_json::Value;

use crate::ExecutorRequestFailure;

mod clock;
mod qualification;
mod window;
use clock::SystemVerificationClock;
pub use clock::VerificationClock;

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
    pub descriptor_version: String,
    pub target: String,
    pub parameters: Value,
    pub phase: VerificationPhase,
    pub resource_conditions: Vec<String>,
    pub technical_slis: Vec<String>,
    pub verification: VerificationSpec,
}

/// Actual data clocks and scope returned by the selected readonly boundary.
/// A declared target version or a report evaluation time cannot populate a
/// missing physical version/data observation.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationSourceFacts {
    pub provenance: VerificationProofOrigin,
    pub resource_observed_at: Option<DateTime<Utc>>,
    pub technical_sli_observed_at: Option<DateTime<Utc>>,
    pub technical_scope_bound: bool,
    pub target_version_observed: Option<String>,
}

/// One immutable observation with resource and technical-SLI outcomes.
#[derive(Clone, Debug, PartialEq)]
pub struct VerificationObservation {
    pub evidence: EvidenceSnapshot,
    pub resource_conditions: BTreeMap<String, bool>,
    pub technical_slis: BTreeMap<String, bool>,
    pub source_facts: Option<VerificationSourceFacts>,
}

/// Completed verification plus every post-change snapshot captured while
/// proving the descriptor-defined stability window.
#[derive(Clone, Debug, PartialEq)]
pub struct VerificationRun {
    pub result: VerificationResult,
    pub post_evidence: Vec<EvidenceSnapshot>,
}

pub type VerificationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<VerificationObservation, ExecutorRequestFailure>> + Send + 'a>>;

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
    profiles: crate::VerificationProfiles,
    clock: Arc<dyn VerificationClock>,
    owner: Option<TaskGroup>,
}

impl ExecutionVerifier {
    #[must_use]
    pub fn new(source: Arc<dyn VerificationSource>, poll_interval: Duration) -> Self {
        Self {
            source,
            poll_interval,
            max_observations: DEFAULT_MAX_OBSERVATIONS,
            profiles: crate::VerificationProfiles::default(),
            clock: Arc::new(SystemVerificationClock::new()),
            owner: None,
        }
    }

    /// Installs bounded operator observation configuration, not dispatch authority.
    #[must_use]
    pub fn with_profiles(mut self, profiles: crate::VerificationProfiles) -> Self {
        self.profiles = profiles;
        self
    }

    /// Uses the existing process owner to cancel readonly observation waits.
    #[must_use]
    pub fn with_owner(mut self, owner: TaskGroup) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Injects deterministic clocks for isolated callers/tests. Production
    /// entrypoints retain the real default and expose no clock override.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn VerificationClock>) -> Self {
        self.clock = clock;
        self
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
    ) -> Result<VerificationObservation, ExecutorRequestFailure> {
        self.capture_qualified(request).await
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
    ) -> Result<VerificationRun, ExecutorRequestFailure> {
        self.verify_window(request, spec, started_at, pre_evidence_ids, during_evidence_ids)
            .await
    }

    pub(crate) fn now(&self) -> DateTime<Utc> {
        self.clock.utc_now()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.cancellation_token().is_cancelled())
    }
}

fn validate_request(request: &VerificationCaptureRequest) -> Result<(), ExecutorRequestFailure> {
    let resource_conditions = request
        .resource_conditions
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let technical_slis = request.technical_slis.iter().collect::<std::collections::BTreeSet<_>>();
    if request.descriptor_version.trim().is_empty()
        || request.target.trim().is_empty()
        || request.resource_conditions.is_empty()
        || request.technical_slis.is_empty()
        || request.verification.resource_conditions != request.resource_conditions
        || request.verification.technical_slis != request.technical_slis
        || request.verification.stable_window_seconds == 0
        || request.verification.stable_window_seconds > request.verification.max_wait_seconds
        || request.verification.max_wait_seconds > 86_400
        || request.resource_conditions.len() + request.technical_slis.len() > MAX_CONDITIONS
        || resource_conditions.len() != request.resource_conditions.len()
        || technical_slis.len() != request.technical_slis.len()
        || request
            .resource_conditions
            .iter()
            .chain(&request.technical_slis)
            .any(|condition| condition.trim().is_empty() || condition.len() > 128)
    {
        return Err(ExecutorRequestFailure::InvalidRequest);
    }
    Ok(())
}

#[cfg(test)]
#[path = "verifier_tests.rs"]
mod tests;
