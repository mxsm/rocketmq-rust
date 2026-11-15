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
use chrono::TimeZone;
use rocketmq_sre_contracts::{
    EvidenceContent, EvidenceExposure, EvidenceQuery, ExecutionObservationReason as Reason, QueryId, TimeRange,
    VerificationOutcome, current_evidence_schema,
};
use serde_json::json;
use std::{collections::VecDeque, sync::Mutex};

struct TestClock(Mutex<(DateTime<Utc>, Duration)>);
impl TestClock {
    fn new() -> Self {
        Self(Mutex::new((timestamp(0), Duration::ZERO)))
    }
    fn set(&self, wall: u32, elapsed: u32) {
        *self.0.lock().unwrap() = (timestamp(wall), Duration::from_secs(u64::from(elapsed)));
    }
}
impl VerificationClock for TestClock {
    fn utc_now(&self) -> DateTime<Utc> {
        self.0.lock().unwrap().0
    }
    fn monotonic_elapsed(&self) -> Duration {
        self.0.lock().unwrap().1
    }
}

struct Frame {
    observation: VerificationObservation,
    wall: u32,
    elapsed: u32,
}
struct ScriptedSource {
    frames: Mutex<VecDeque<Frame>>,
    clock: Arc<TestClock>,
}
impl VerificationSource for ScriptedSource {
    fn observe<'a>(&'a self, _request: &'a VerificationCaptureRequest) -> VerificationFuture<'a> {
        Box::pin(async move {
            let frame = self
                .frames
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(ExecutorRequestFailure::ObservationUnknown)?;
            self.clock.set(frame.wall, frame.elapsed);
            Ok(frame.observation)
        })
    }
}

fn request(phase: VerificationPhase) -> VerificationCaptureRequest {
    let verification = VerificationSpec {
        resource_conditions: vec!["replacement_ready".to_owned()],
        technical_slis: vec!["proxy_error_ratio".to_owned()],
        stable_window_seconds: 30,
        max_wait_seconds: 60,
    };
    VerificationCaptureRequest {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        correlation_id: CorrelationId::new(),
        execution_id: ExecutionId::new(),
        step_id: ExecutionStepId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::ProxyRestartOne,
        descriptor_version: "1.0.0".to_owned(),
        target: "pod/proxy-0".to_owned(),
        parameters: json!({"namespace":"rocketmq-system"}),
        phase,
        resource_conditions: verification.resource_conditions.clone(),
        technical_slis: verification.technical_slis.clone(),
        verification,
    }
}

fn profiles(request: &VerificationCaptureRequest) -> crate::VerificationProfiles {
    crate::VerificationProfiles::from_json(
        &serde_json::to_vec(&json!({
        "schema_version":"rocketmq-sre.execution-verification-profiles.v1","rules":[{
            "tenant_id":request.tenant_id,"cluster_id":request.cluster_id,"action":request.action,
            "descriptor_version":request.descriptor_version,"target":request.target,"declared_target_version":"test-1",
            "policy_revision":"test-1","max_source_age_seconds":10,"max_sample_gap_seconds":60
        }]}))
        .unwrap(),
    )
    .unwrap()
}

fn frame(
    request: &VerificationCaptureRequest,
    point: u32,
    wall: u32,
    elapsed: u32,
    resource: bool,
    sli: bool,
) -> Frame {
    let resource_conditions = [("replacement_ready".to_owned(), resource)].into_iter().collect();
    let technical_slis = [("proxy_error_ratio".to_owned(), sli)].into_iter().collect();
    let facts = VerificationSourceFacts {
        provenance: VerificationProofOrigin::SyntheticFixture,
        resource_observed_at: Some(timestamp(point)),
        technical_sli_observed_at: Some(timestamp(point)),
        technical_scope_bound: true,
        target_version_observed: Some("test-1".to_owned()),
    };
    let mut evidence = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: request.correlation_id,
            tenant_id: request.tenant_id,
            cluster_id: request.cluster_id,
            source: "execution-verifier-fixture".to_owned(),
            resource: request.target.clone(),
            time_range: TimeRange::new(timestamp(wall), timestamp(wall)).unwrap(),
        },
        current_evidence_schema(),
        timestamp(wall),
        EvidenceContent::Inline(json!({
            "resource_conditions":resource_conditions,"technical_slis":technical_slis,"source_facts":facts
        })),
    )
    .unwrap();
    evidence.exposure = EvidenceExposure::Synthetic;
    Frame {
        observation: VerificationObservation {
            evidence,
            resource_conditions,
            technical_slis,
            source_facts: Some(facts),
        },
        wall,
        elapsed,
    }
}

async fn run(request: &VerificationCaptureRequest, frames: Vec<Frame>) -> VerificationRun {
    let clock = Arc::new(TestClock::new());
    let source = Arc::new(ScriptedSource {
        frames: Mutex::new(frames.into()),
        clock: clock.clone(),
    });
    ExecutionVerifier::new(source, Duration::ZERO)
        .with_clock(clock)
        .with_profiles(profiles(request))
        .with_max_observations(16)
        .verify_post(request, &request.verification, timestamp(0), Vec::new(), Vec::new())
        .await
        .unwrap()
}

#[tokio::test]
async fn stable_post_and_rollback_require_actual_elapsed_and_both_sources() {
    for phase in [VerificationPhase::Post, VerificationPhase::RollbackPost] {
        let request = request(phase);
        let run = run(
            &request,
            vec![
                frame(&request, 0, 0, 0, true, true),
                frame(&request, 30, 30, 30, true, true),
            ],
        )
        .await;
        assert_eq!(run.result.outcome, VerificationOutcome::Succeeded);
        assert_eq!(
            run.result.satisfied_conditions,
            ["resource:replacement_ready", "sli:proxy_error_ratio"]
        );
        let proof = run.result.window_proof.unwrap();
        proof.verify().unwrap();
        assert_eq!(proof.observed_stable_seconds, 30);
        assert_eq!(proof.samples.len(), 2);
    }
}

#[tokio::test]
async fn known_false_at_real_budget_end_is_failed() {
    let request = request(VerificationPhase::Post);
    let run = run(
        &request,
        vec![
            frame(&request, 0, 0, 0, true, false),
            frame(&request, 60, 60, 60, true, false),
        ],
    )
    .await;
    assert_eq!(run.result.outcome, VerificationOutcome::Failed);
    assert_eq!(run.result.failed_conditions, ["sli:proxy_error_ratio"]);
}

#[tokio::test]
async fn future_jump_old_data_repetition_and_clock_regression_cannot_succeed() {
    let request = request(VerificationPhase::Post);
    for frames in [
        vec![
            frame(&request, 0, 0, 0, true, true),
            frame(&request, 60, 0, 0, true, true),
        ],
        vec![
            frame(&request, 0, 0, 0, true, true),
            frame(&request, 0, 60, 60, true, true),
        ],
        vec![
            frame(&request, 0, 0, 0, true, true),
            frame(&request, 0, 5, 5, true, true),
        ],
        vec![
            frame(&request, 10, 10, 10, true, true),
            frame(&request, 5, 20, 20, true, true),
        ],
    ] {
        let run = run(&request, frames).await;
        assert_eq!(run.result.outcome, VerificationOutcome::Inconclusive);
        assert!(
            !run.result
                .window_proof
                .unwrap()
                .reason_codes
                .contains(&Reason::PolicyWindowSatisfied)
        );
    }
}

#[tokio::test]
async fn missing_version_scope_or_sli_is_unknown_not_known_false() {
    let request = request(VerificationPhase::Post);
    for case in 0..4 {
        let mut sample = frame(&request, 0, 0, 0, true, false);
        match case {
            0 => {
                sample
                    .observation
                    .source_facts
                    .as_mut()
                    .unwrap()
                    .target_version_observed = None
            }
            1 => sample.observation.source_facts.as_mut().unwrap().technical_scope_bound = false,
            2 => sample.observation.evidence.partial = true,
            _ => {
                sample.observation.technical_slis.clear();
            }
        }
        sample.observation.evidence.content =
            EvidenceContent::Inline(json!({"resource_conditions":sample.observation.resource_conditions,
            "technical_slis":sample.observation.technical_slis,"source_facts":sample.observation.source_facts}));
        sample.observation.evidence.content_hash = sample.observation.evidence.compute_content_hash().unwrap();
        let run = run(&request, vec![sample]).await;
        assert_eq!(run.result.outcome, VerificationOutcome::Inconclusive);
        assert!(run.result.failed_conditions.is_empty());
    }
}

#[tokio::test]
async fn no_policy_makes_no_source_call_and_preserves_recorded_only_proof() {
    let request = request(VerificationPhase::Post);
    let source = Arc::new(ScriptedSource {
        frames: Mutex::new(VecDeque::new()),
        clock: Arc::new(TestClock::new()),
    });
    let verifier = ExecutionVerifier::new(source, Duration::ZERO).with_clock(Arc::new(TestClock::new()));
    let run = verifier
        .verify_post(&request, &request.verification, timestamp(0), Vec::new(), Vec::new())
        .await
        .unwrap();
    assert_eq!(run.result.outcome, VerificationOutcome::Inconclusive);
    assert_eq!(
        run.result.window_proof.unwrap().reason_codes,
        [Reason::PolicyNotConfigured]
    );
}

#[tokio::test]
async fn scope_and_hash_tampering_are_not_legacy_or_qualified() {
    let request = request(VerificationPhase::Post);
    for case in 0..2 {
        let mut sample = frame(&request, 0, 0, 0, true, true);
        if case == 0 {
            sample.observation.evidence.cluster_id = ClusterId::new();
        } else {
            sample.observation.evidence.content_hash = "0".repeat(64);
        }
        let run = run(&request, vec![sample]).await;
        assert_eq!(run.result.outcome, VerificationOutcome::Inconclusive);
        assert!(run.post_evidence.is_empty());
        assert!(run.result.window_proof.is_some());
    }
}

fn timestamp(seconds: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 28, 1, 0, 0).single().unwrap() + chrono::TimeDelta::seconds(i64::from(seconds))
}

#[tokio::test]
async fn interrupted_true_window_does_not_skip_the_configured_stability_duration() {
    let request = request(VerificationPhase::Post);
    let run = run(
        &request,
        vec![
            frame(&request, 0, 0, 0, true, true),
            frame(&request, 10, 10, 10, true, false),
            frame(&request, 31, 31, 31, true, true),
            frame(&request, 60, 60, 60, true, true),
        ],
    )
    .await;
    assert_eq!(run.result.outcome, VerificationOutcome::Failed);
    let proof = run.result.window_proof.unwrap();
    assert_eq!(proof.observed_stable_seconds, 29);
    assert!(!proof.reason_codes.contains(&Reason::PolicyWindowSatisfied));
}

struct PendingSource;
impl VerificationSource for PendingSource {
    fn observe<'a>(&'a self, _request: &'a VerificationCaptureRequest) -> VerificationFuture<'a> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn frozen_source_clock_cannot_extend_the_actual_read_budget() {
    let mut request = request(VerificationPhase::Post);
    request.verification.stable_window_seconds = 1;
    request.verification.max_wait_seconds = 2;
    let clock = Arc::new(TestClock::new());
    let verifier = ExecutionVerifier::new(Arc::new(PendingSource), Duration::ZERO)
        .with_profiles(profiles(&request))
        .with_clock(clock);
    let start = tokio::time::Instant::now();
    let run = verifier
        .verify_post(&request, &request.verification, timestamp(0), Vec::new(), Vec::new())
        .await
        .unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    assert_eq!(run.result.outcome, VerificationOutcome::Inconclusive);
    assert!(
        run.result
            .window_proof
            .unwrap()
            .reason_codes
            .contains(&Reason::ReadDeadlineExceeded)
    );
}
