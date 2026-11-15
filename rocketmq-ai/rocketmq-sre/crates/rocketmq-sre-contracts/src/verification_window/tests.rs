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

use chrono::TimeDelta;
use chrono::TimeZone;
use serde_json::json;

use super::*;
use crate::VerificationResult;

#[test]
fn old_verification_result_roundtrip_preserves_canonical_content() {
    let old = json!({
        "step_id": ExecutionStepId::new(), "outcome": "succeeded",
        "started_at": "2026-11-15T10:00:00.123456789Z",
        "completed_at": "2026-11-15T10:00:30.123456789Z",
        "pre_evidence_ids": [], "during_evidence_ids": [], "post_evidence_ids": [],
        "satisfied_conditions": ["resource:logger_level_applied"], "failed_conditions": [],
        "stable_window_seconds": 30
    });
    let decoded: VerificationResult = serde_json::from_value(old.clone()).unwrap();
    assert!(decoded.window_proof.is_none());
    let encoded = serde_json::to_value(&decoded).unwrap();
    assert_eq!(old, encoded);
    assert_eq!(canonical_sha256(&old).unwrap(), canonical_sha256(&encoded).unwrap());

    let mut malformed = old;
    malformed["window_proof"] = json!({"proof_hash": "not-a-proof"});
    assert!(serde_json::from_value::<VerificationResult>(malformed).is_err());
}

#[test]
fn present_proof_is_bound_to_content_and_keeps_nanosecond_times() {
    let proof = proof().seal().unwrap();
    proof.verify().unwrap();
    let restored: VerificationWindowProof = serde_json::from_value(serde_json::to_value(&proof).unwrap()).unwrap();
    assert_eq!(restored, proof);
    assert_eq!(restored.started_at.timestamp_subsec_nanos(), 123_456_789);

    let mut different_target = proof.clone();
    different_target.target = "broker/another-broker".to_owned();
    assert!(different_target.verify().is_err());
    let mut different_sample = proof.clone();
    different_sample.samples[0].technical_slis_satisfied = false;
    assert!(different_sample.verify().is_err());
    let mut changed_policy = proof;
    changed_policy.policy.as_mut().unwrap().max_source_age_seconds += 1;
    assert!(changed_policy.verify().is_err());
}

#[test]
fn future_sample_and_nonmonotonic_elapsed_cannot_be_sealed_as_valid_proof() {
    let mut future = proof();
    future.samples[0].technical_sli_observed_at += TimeDelta::nanoseconds(1);
    assert!(future.seal().is_err());

    let mut backwards = proof();
    backwards.samples[0].elapsed_nanos = 31_000_000_000;
    assert!(backwards.seal().is_err());

    let mut old_origin = proof();
    old_origin.provenance = VerificationProofOrigin::RecordedOnly;
    assert!(old_origin.seal().is_err());
}

fn proof() -> VerificationWindowProof {
    let start = Utc.timestamp_opt(1_763_200_800, 123_456_789).single().unwrap();
    let end = start + TimeDelta::seconds(30);
    VerificationWindowProof {
        schema_version: VERIFICATION_WINDOW_PROOF_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        execution_id: ExecutionId::new(),
        step_id: ExecutionStepId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        descriptor_version: "1.0.0".to_owned(),
        target: "broker/fixture-target".to_owned(),
        parameters_digest: format!("sha256:{}", "a".repeat(64)),
        policy: Some(VerificationWindowPolicy {
            policy_digest: format!("sha256:{}", "b".repeat(64)),
            policy_revision: "1".to_owned(),
            declared_target_version: "SyntheticFixture-1".to_owned(),
            stable_window_seconds: 30,
            max_wait_seconds: 60,
            max_source_age_seconds: 30,
            max_sample_gap_seconds: 30,
        }),
        provenance: VerificationProofOrigin::SyntheticFixture,
        target_version_observed: Some("SyntheticFixture-1".to_owned()),
        started_at: start,
        completed_at: end,
        observed_stable_seconds: 30,
        samples: [start, end]
            .into_iter()
            .enumerate()
            .map(|(index, at)| VerificationWindowSample {
                evidence_id: EvidenceId::new(),
                content_hash: format!("sha256:{}", "c".repeat(64)),
                resource_observed_at: at,
                technical_sli_observed_at: at,
                collected_at: at,
                source_age_seconds: 0,
                elapsed_nanos: index as u64 * 30_000_000_000,
                complete: true,
                resource_conditions_satisfied: true,
                technical_slis_satisfied: true,
                target_version_matched: true,
            })
            .collect(),
        missing_conditions: Vec::new(),
        reason_codes: vec![ExecutionObservationReason::PolicyWindowSatisfied],
        proof_hash: String::new(),
    }
}
