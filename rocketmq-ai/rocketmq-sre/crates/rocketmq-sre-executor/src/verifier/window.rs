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
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionObservationReason as Reason;
use rocketmq_sre_contracts::VERIFICATION_WINDOW_PROOF_SCHEMA_VERSION;
use rocketmq_sre_contracts::VerificationOutcome;
use rocketmq_sre_contracts::VerificationProofOrigin;
use rocketmq_sre_contracts::VerificationResult;
use rocketmq_sre_contracts::VerificationSpec;
use rocketmq_sre_contracts::VerificationWindowPolicy;
use rocketmq_sre_contracts::VerificationWindowProof;
use rocketmq_sre_contracts::VerificationWindowSample;
use rocketmq_sre_contracts::canonical_sha256;

use super::ExecutionVerifier;
use super::VerificationCaptureRequest;
use super::VerificationObservation;
use super::VerificationPhase;
use super::VerificationRun;
use super::qualification;
use crate::ExecutorRequestFailure;

#[derive(Default)]
struct Progress {
    evidence: Vec<EvidenceSnapshot>,
    hashes: BTreeMap<EvidenceId, String>,
    samples: Vec<VerificationWindowSample>,
    satisfied: Vec<String>,
    failed: Vec<String>,
    missing: Vec<String>,
    reasons: Vec<Reason>,
    provenance: Option<VerificationProofOrigin>,
    version: Option<String>,
    stable_start: Option<VerificationWindowSample>,
    segment_start: Option<VerificationWindowSample>,
    last_advancing: Option<VerificationWindowSample>,
    segment_failure: bool,
    last_qualified: bool,
    observed_stable_seconds: u64,
}

impl ExecutionVerifier {
    pub(super) async fn capture_qualified(
        &self,
        request: &VerificationCaptureRequest,
    ) -> Result<VerificationObservation, ExecutorRequestFailure> {
        super::validate_request(request)?;
        let policy = self
            .profile(request)?
            .ok_or(ExecutorRequestFailure::ObservationUnknown)?;
        let timeout = Duration::from_secs(request.verification.max_wait_seconds.min(30));
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(ExecutorRequestFailure::InvalidRequest)?;
        let observation = self
            .read_until(request, deadline)
            .await
            .map_err(|_| ExecutorRequestFailure::ObservationUnknown)?;
        let frame = qualification::evaluate(request, &observation, &policy, self.clock.utc_now(), 0)
            .map_err(|_| ExecutorRequestFailure::VerificationRejected)?;
        if !frame.qualified {
            return Err(ExecutorRequestFailure::ObservationUnknown);
        }
        // Before a change its desired effect conditions are normally false.
        // Capture requires qualified observation, not an already-applied effect.
        Ok(observation)
    }

    pub(super) async fn verify_window(
        &self,
        request: &VerificationCaptureRequest,
        spec: &VerificationSpec,
        started_at: DateTime<Utc>,
        pre_evidence_ids: Vec<EvidenceId>,
        during_evidence_ids: Vec<EvidenceId>,
    ) -> Result<VerificationRun, ExecutorRequestFailure> {
        super::validate_request(request)?;
        if !matches!(request.phase, VerificationPhase::Post | VerificationPhase::RollbackPost)
            || spec != &request.verification
            || self.clock.utc_now() < started_at
        {
            return Err(ExecutorRequestFailure::InvalidRequest);
        }
        let policy = self.profile(request)?;
        let max_wait = Duration::from_secs(spec.max_wait_seconds);
        let hard_deadline = tokio::time::Instant::now()
            .checked_add(max_wait)
            .ok_or(ExecutorRequestFailure::InvalidRequest)?;
        let origin = self.clock.monotonic_elapsed();
        let mut progress = Progress::default();
        if policy.is_none() {
            progress.reason(Reason::PolicyNotConfigured);
            return self.finish(
                request,
                started_at,
                policy,
                progress,
                VerificationOutcome::Inconclusive,
                pre_evidence_ids,
                during_evidence_ids,
            );
        }
        let selected = policy.as_ref().ok_or(ExecutorRequestFailure::ObservationUnknown)?;
        for index in 0..self.max_observations {
            let elapsed = self.elapsed(origin)?;
            if elapsed >= max_wait || tokio::time::Instant::now() >= hard_deadline {
                break;
            }
            let observation = match self.read_until(request, hard_deadline).await {
                Ok(observation) => observation,
                Err(reason) => {
                    progress.reason(reason);
                    progress.last_qualified = false;
                    break;
                }
            };
            let elapsed = self.elapsed(origin)?;
            let as_of = self.clock.utc_now();
            if elapsed > max_wait || tokio::time::Instant::now() > hard_deadline {
                progress.reason(Reason::ReadDeadlineExceeded);
                progress.last_qualified = false;
                break;
            }
            if as_of < started_at {
                return Err(ExecutorRequestFailure::ObservationUnknown);
            }
            let elapsed_nanos =
                u64::try_from(elapsed.as_nanos()).map_err(|_| ExecutorRequestFailure::ObservationUnknown)?;
            let mut frame = match qualification::evaluate(request, &observation, selected, as_of, elapsed_nanos) {
                Ok(frame) => frame,
                Err(reason) => {
                    progress.reason(reason);
                    progress.last_qualified = false;
                    break;
                }
            };
            if let Some(facts) = &observation.source_facts {
                progress.provenance = Some(facts.provenance);
                progress.version.clone_from(&facts.target_version_observed);
            }
            let snapshot = &observation.evidence;
            let new_identity = match progress.hashes.get(&snapshot.evidence_id) {
                Some(hash) if hash != &snapshot.content_hash => {
                    progress.reason(Reason::EvidenceHashInvalid);
                    progress.last_qualified = false;
                    break;
                }
                Some(_) => false,
                None => true,
            };
            if new_identity {
                progress
                    .hashes
                    .insert(snapshot.evidence_id, snapshot.content_hash.clone());
                progress.evidence.push(snapshot.clone());
            }
            if snapshot.observed_at < started_at
                || frame.sample.as_ref().is_some_and(|sample| {
                    sample.resource_observed_at < started_at || sample.technical_sli_observed_at < started_at
                })
            {
                frame.qualified = false;
                frame.sample = None;
                frame.reasons.push(Reason::SourceBeforeWindow);
                frame.satisfied.clear();
                frame.failed.clear();
                frame.missing = request
                    .resource_conditions
                    .iter()
                    .map(|condition| format!("resource:{condition}"))
                    .chain(
                        request
                            .technical_slis
                            .iter()
                            .map(|condition| format!("sli:{condition}")),
                    )
                    .collect();
            }
            progress.satisfied = frame.satisfied;
            progress.failed = frame.failed;
            progress.missing = frame.missing;
            for reason in frame.reasons {
                progress.reason(reason);
            }
            progress.last_qualified = frame.qualified;
            let mut succeeded = false;
            if let Some(mut sample) = frame.sample {
                sample.complete &= frame.qualified;
                if frame.qualified {
                    succeeded = progress.observe(&sample, frame.all_conditions, selected);
                } else {
                    progress.break_window();
                }
                if new_identity {
                    progress.samples.push(sample);
                }
            } else {
                progress.break_window();
            }
            // A source timestamp cannot buy an early decision or extend the
            // independent actual run budget, even if a callback returns late.
            if succeeded && elapsed <= max_wait && tokio::time::Instant::now() <= hard_deadline {
                progress.reason(Reason::PolicyWindowSatisfied);
                return self.finish(
                    request,
                    started_at,
                    policy,
                    progress,
                    VerificationOutcome::Succeeded,
                    pre_evidence_ids,
                    during_evidence_ids,
                );
            }
            if index + 1 == self.max_observations {
                progress.reason(Reason::ObservationBudgetExhausted);
                progress.last_qualified = false;
                break;
            }
            let remaining = max_wait.saturating_sub(self.elapsed(origin)?);
            if remaining.is_zero() {
                break;
            }
            let pause = self.poll_interval.min(remaining);
            tokio::select! {
                biased;
                () = wait_cancelled(self.owner.as_ref()) => {
                    progress.reason(Reason::ObservationCancelled);
                    progress.last_qualified = false;
                    break;
                }
                () = tokio::time::sleep_until((tokio::time::Instant::now() + pause).min(hard_deadline)) => {}
            }
        }
        let as_of = self.clock.utc_now();
        if progress.last_qualified && !progress.is_current(selected, as_of) {
            progress.reason(Reason::SourceStale);
            progress.last_qualified = false;
        }
        let outcome = if progress.last_qualified && (!progress.failed.is_empty() || progress.segment_failure) {
            progress.reason(Reason::KnownConditionFailed);
            VerificationOutcome::Failed
        } else {
            progress.reason(Reason::StableWindowIncomplete);
            VerificationOutcome::Inconclusive
        };
        self.finish(
            request,
            started_at,
            policy,
            progress,
            outcome,
            pre_evidence_ids,
            during_evidence_ids,
        )
    }

    fn profile(
        &self,
        request: &VerificationCaptureRequest,
    ) -> Result<Option<VerificationWindowPolicy>, ExecutorRequestFailure> {
        self.profiles
            .resolve(
                request.tenant_id,
                request.cluster_id,
                request.action,
                &request.descriptor_version,
                &request.target,
                &request.verification,
            )
            .map_err(Into::into)
    }

    fn elapsed(&self, origin: Duration) -> Result<Duration, ExecutorRequestFailure> {
        self.clock
            .monotonic_elapsed()
            .checked_sub(origin)
            .ok_or(ExecutorRequestFailure::ObservationUnknown)
    }

    async fn read_until(
        &self,
        request: &VerificationCaptureRequest,
        deadline: tokio::time::Instant,
    ) -> Result<VerificationObservation, Reason> {
        tokio::select! {
            biased;
            () = wait_cancelled(self.owner.as_ref()) => Err(Reason::ObservationCancelled),
            response = tokio::time::timeout_at(deadline, self.source.observe(request)) => match response {
                Ok(Ok(observation)) => Ok(observation),
                Ok(Err(_)) => Err(Reason::SourceMissing),
                Err(_) => Err(Reason::ReadDeadlineExceeded),
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "result keeps the immutable before/during Evidence binding explicit"
    )]
    fn finish(
        &self,
        request: &VerificationCaptureRequest,
        started_at: DateTime<Utc>,
        policy: Option<VerificationWindowPolicy>,
        progress: Progress,
        outcome: VerificationOutcome,
        pre_evidence_ids: Vec<EvidenceId>,
        during_evidence_ids: Vec<EvidenceId>,
    ) -> Result<VerificationRun, ExecutorRequestFailure> {
        let completed_at = self.clock.utc_now();
        if completed_at < started_at {
            return Err(ExecutorRequestFailure::ObservationUnknown);
        }
        let proof = VerificationWindowProof {
            schema_version: VERIFICATION_WINDOW_PROOF_SCHEMA_VERSION.to_owned(),
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            execution_id: request.execution_id,
            step_id: request.step_id,
            plan_step_id: request.plan_step_id,
            action: request.action,
            descriptor_version: request.descriptor_version.clone(),
            target: request.target.clone(),
            parameters_digest: canonical_sha256(&request.parameters)
                .map_err(|_| ExecutorRequestFailure::InvalidRequest)?,
            policy,
            provenance: progress.provenance.unwrap_or(VerificationProofOrigin::RecordedOnly),
            target_version_observed: progress.version,
            started_at,
            completed_at,
            observed_stable_seconds: progress.observed_stable_seconds,
            samples: progress.samples,
            missing_conditions: progress.missing,
            reason_codes: progress.reasons,
            proof_hash: String::new(),
        }
        .seal()
        .map_err(|_| ExecutorRequestFailure::VerificationRejected)?;
        Ok(VerificationRun {
            result: VerificationResult {
                step_id: request.step_id,
                outcome,
                started_at,
                completed_at,
                pre_evidence_ids,
                during_evidence_ids,
                post_evidence_ids: progress.evidence.iter().map(|evidence| evidence.evidence_id).collect(),
                satisfied_conditions: progress.satisfied,
                failed_conditions: progress.failed,
                stable_window_seconds: request.verification.stable_window_seconds,
                window_proof: Some(proof),
            },
            post_evidence: progress.evidence,
        })
    }
}

impl Progress {
    fn reason(&mut self, reason: Reason) {
        if !self.reasons.contains(&reason) {
            self.reasons.push(reason);
        }
    }

    fn break_window(&mut self) {
        self.stable_start = None;
        self.segment_start = None;
        self.segment_failure = false;
        self.observed_stable_seconds = 0;
    }

    fn observe(
        &mut self,
        sample: &VerificationWindowSample,
        all_conditions: bool,
        policy: &VerificationWindowPolicy,
    ) -> bool {
        if let Some(previous) = self.last_advancing.clone() {
            if sample.resource_observed_at < previous.resource_observed_at
                || sample.technical_sli_observed_at < previous.technical_sli_observed_at
                || sample.elapsed_nanos < previous.elapsed_nanos
            {
                self.reason(Reason::SourceOutOfOrder);
                self.last_qualified = false;
                self.break_window();
                return false;
            }
            let gap = i128::from(policy.max_sample_gap_seconds) * 1_000_000_000;
            if i128::from(sample.elapsed_nanos - previous.elapsed_nanos) > gap
                || qualification::nanos(sample.resource_observed_at)
                    - qualification::nanos(previous.resource_observed_at)
                    > gap
                || qualification::nanos(sample.technical_sli_observed_at)
                    - qualification::nanos(previous.technical_sli_observed_at)
                    > gap
            {
                self.reason(Reason::ObservationGap);
                self.break_window();
                self.last_qualified = false;
            }
            if sample.resource_observed_at == previous.resource_observed_at
                || sample.technical_sli_observed_at == previous.technical_sli_observed_at
            {
                self.reason(Reason::SourceRepeated);
                if sample.resource_conditions_satisfied != previous.resource_conditions_satisfied
                    || sample.technical_slis_satisfied != previous.technical_slis_satisfied
                {
                    self.last_qualified = false;
                    self.break_window();
                }
                return false;
            }
        }
        self.last_advancing = Some(sample.clone());
        self.segment_start.get_or_insert_with(|| sample.clone());
        if !all_conditions {
            self.segment_failure = true;
            self.stable_start = None;
            self.observed_stable_seconds = 0;
            return false;
        }
        let first = self.stable_start.get_or_insert_with(|| sample.clone());
        let actual_elapsed = sample.elapsed_nanos.saturating_sub(first.elapsed_nanos);
        let resource_span =
            qualification::nanos(sample.resource_observed_at) - qualification::nanos(first.resource_observed_at);
        let sli_span = qualification::nanos(sample.technical_sli_observed_at)
            - qualification::nanos(first.technical_sli_observed_at);
        let observed_span = i128::from(actual_elapsed).min(resource_span).min(sli_span).max(0);
        self.observed_stable_seconds = u64::try_from(observed_span / 1_000_000_000).unwrap_or(0);
        observed_span >= i128::from(policy.stable_window_seconds) * 1_000_000_000
    }

    fn is_current(&self, policy: &VerificationWindowPolicy, as_of: DateTime<Utc>) -> bool {
        self.samples.last().is_some_and(|sample| {
            let elapsed = qualification::nanos(as_of) - qualification::nanos(sample.collected_at);
            elapsed >= 0
                && i128::from(sample.source_age_seconds) * 1_000_000_000 + elapsed
                    <= i128::from(policy.max_source_age_seconds) * 1_000_000_000
        })
    }
}

async fn wait_cancelled(owner: Option<&rocketmq_runtime::task_group::TaskGroup>) {
    match owner {
        Some(owner) => owner.cancellation_token().cancelled_owned().await,
        None => std::future::pending().await,
    }
}
