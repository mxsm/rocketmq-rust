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

use std::collections::VecDeque;
use std::sync::Mutex;

use chrono::TimeZone;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use serde_json::json;

use super::*;

struct ScriptedSource {
    observations: Mutex<VecDeque<VerificationObservation>>,
}

impl VerificationSource for ScriptedSource {
    fn observe<'a>(&'a self, _request: &'a VerificationCaptureRequest) -> VerificationFuture<'a> {
        Box::pin(async move {
            self.observations
                .lock()
                .expect("scripted source lock")
                .pop_front()
                .ok_or(ExecutorError::AgentUnavailable)
        })
    }
}

#[tokio::test]
async fn stable_post_window_requires_resource_and_technical_sli() {
    let started_at = timestamp(0);
    let request = request(VerificationPhase::Post);
    let source = Arc::new(ScriptedSource {
        observations: Mutex::new(
            [
                observation(&request, timestamp(0), true, true),
                observation(&request, timestamp(30), true, true),
            ]
            .into(),
        ),
    });
    let verifier = ExecutionVerifier::new(source, Duration::ZERO).with_max_observations(4);
    let spec = VerificationSpec {
        resource_conditions: request.resource_conditions.clone(),
        technical_slis: request.technical_slis.clone(),
        stable_window_seconds: 30,
        max_wait_seconds: 60,
    };

    let run = verifier
        .verify_post(&request, &spec, started_at, Vec::new(), Vec::new())
        .await
        .expect("stable verification");

    assert_eq!(run.result.outcome, VerificationOutcome::Succeeded);
    assert_eq!(run.post_evidence.len(), 2);
    assert_eq!(
        run.result.satisfied_conditions,
        ["resource:replacement_ready", "sli:proxy_error_ratio"]
    );
}

#[tokio::test]
async fn explicit_failure_at_deadline_is_failed() {
    let started_at = timestamp(0);
    let request = request(VerificationPhase::Post);
    let source = Arc::new(ScriptedSource {
        observations: Mutex::new(
            [
                observation(&request, timestamp(0), true, false),
                observation(&request, timestamp(60), true, false),
            ]
            .into(),
        ),
    });
    let verifier = ExecutionVerifier::new(source, Duration::ZERO).with_max_observations(4);
    let spec = VerificationSpec {
        resource_conditions: request.resource_conditions.clone(),
        technical_slis: request.technical_slis.clone(),
        stable_window_seconds: 30,
        max_wait_seconds: 60,
    };

    let run = verifier
        .verify_post(&request, &spec, started_at, Vec::new(), Vec::new())
        .await
        .expect("bounded failed verification");

    assert_eq!(run.result.outcome, VerificationOutcome::Failed);
    assert_eq!(run.result.failed_conditions, ["sli:proxy_error_ratio"]);
}

#[tokio::test]
async fn missing_or_partial_signal_is_inconclusive() {
    let started_at = timestamp(0);
    let request = request(VerificationPhase::Post);
    let mut incomplete = observation(&request, timestamp(60), true, true);
    incomplete.technical_slis.clear();
    incomplete.evidence.partial = true;
    let source = Arc::new(ScriptedSource {
        observations: Mutex::new([incomplete].into()),
    });
    let verifier = ExecutionVerifier::new(source, Duration::ZERO).with_max_observations(1);
    let spec = VerificationSpec {
        resource_conditions: request.resource_conditions.clone(),
        technical_slis: request.technical_slis.clone(),
        stable_window_seconds: 30,
        max_wait_seconds: 60,
    };

    let run = verifier
        .verify_post(&request, &spec, started_at, Vec::new(), Vec::new())
        .await
        .expect("bounded inconclusive verification");

    assert_eq!(run.result.outcome, VerificationOutcome::Inconclusive);
    assert_eq!(run.result.failed_conditions, ["sli:proxy_error_ratio"]);
}

fn request(phase: VerificationPhase) -> VerificationCaptureRequest {
    VerificationCaptureRequest {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        correlation_id: CorrelationId::new(),
        execution_id: ExecutionId::new(),
        step_id: ExecutionStepId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::ProxyRestartOne,
        target: "pod/proxy-0".to_owned(),
        phase,
        resource_conditions: vec!["replacement_ready".to_owned()],
        technical_slis: vec!["proxy_error_ratio".to_owned()],
    }
}

fn observation(
    request: &VerificationCaptureRequest,
    observed_at: DateTime<Utc>,
    resource_ok: bool,
    sli_ok: bool,
) -> VerificationObservation {
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: request.correlation_id,
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        source: "execution-verifier-fixture".to_owned(),
        resource: request.target.clone(),
        time_range: TimeRange::new(observed_at, observed_at).expect("time range"),
    };
    let mut evidence = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed_at,
        EvidenceContent::Inline(json!({
            "resource_conditions": {"replacement_ready": resource_ok},
            "technical_slis": {"proxy_error_ratio": sli_ok}
        })),
    )
    .expect("evidence");
    evidence.exposure = EvidenceExposure::Synthetic;
    evidence.sensitivity = Sensitivity::Internal;
    VerificationObservation {
        evidence,
        resource_conditions: [("replacement_ready".to_owned(), resource_ok)].into_iter().collect(),
        technical_slis: [("proxy_error_ratio".to_owned(), sli_ok)].into_iter().collect(),
    }
}

fn timestamp(seconds: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 28, 1, 0, 0).single().expect("timestamp") + TimeDelta::seconds(i64::from(seconds))
}
