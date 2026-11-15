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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::ExecutionObservationReason as Reason;
use rocketmq_sre_contracts::VerificationProofOrigin;
use rocketmq_sre_contracts::VerificationWindowPolicy;
use rocketmq_sre_contracts::VerificationWindowSample;
use rocketmq_sre_contracts::current_evidence_schema;

use super::VerificationCaptureRequest;
use super::VerificationObservation;

pub(super) struct Frame {
    pub sample: Option<VerificationWindowSample>,
    pub qualified: bool,
    pub all_conditions: bool,
    pub satisfied: Vec<String>,
    pub failed: Vec<String>,
    pub missing: Vec<String>,
    pub reasons: Vec<Reason>,
}

pub(super) fn evaluate(
    request: &VerificationCaptureRequest,
    observation: &VerificationObservation,
    policy: &VerificationWindowPolicy,
    as_of: DateTime<Utc>,
    elapsed_nanos: u64,
) -> Result<Frame, Reason> {
    let evidence = &observation.evidence;
    if observation.resource_conditions.len() > super::MAX_CONDITIONS
        || observation.technical_slis.len() > super::MAX_CONDITIONS
    {
        return Err(Reason::EvidenceScopeMismatch);
    }
    evidence
        .verify_content_hash()
        .map_err(|_| Reason::EvidenceHashInvalid)?;
    if evidence.schema != current_evidence_schema()
        || evidence.tenant_id != request.tenant_id
        || evidence.cluster_id != request.cluster_id
        || evidence.correlation_id != request.correlation_id
        || evidence.resource != request.target
    {
        return Err(Reason::EvidenceScopeMismatch);
    }
    let EvidenceContent::Inline(content) = &evidence.content else {
        return Err(Reason::RetainedProofUnavailable);
    };
    if content.get("resource_conditions") != Some(&serde_json::json!(observation.resource_conditions))
        || content.get("technical_slis") != Some(&serde_json::json!(observation.technical_slis))
    {
        return Err(Reason::EvidenceHashInvalid);
    }
    match (&observation.source_facts, content.get("source_facts")) {
        (Some(facts), Some(encoded)) if encoded == &serde_json::json!(facts) => {}
        (None, None | Some(serde_json::Value::Null)) => {}
        _ => return Err(Reason::EvidenceHashInvalid),
    }
    let mut frame = Frame {
        sample: None,
        qualified: true,
        all_conditions: true,
        satisfied: Vec::new(),
        failed: Vec::new(),
        missing: Vec::new(),
        reasons: Vec::new(),
    };
    let Some(facts) = &observation.source_facts else {
        frame.reasons.push(Reason::SourceMissing);
        frame.qualified = false;
        evaluate_conditions(request, observation, &mut frame);
        return Ok(frame);
    };
    let origin_matches = match facts.provenance {
        VerificationProofOrigin::SyntheticFixture => evidence.exposure == EvidenceExposure::Synthetic,
        VerificationProofOrigin::ProductionRead => {
            evidence.source == "execution-verification" && evidence.exposure != EvidenceExposure::Synthetic
        }
        VerificationProofOrigin::RecordedOnly => false,
    };
    if !origin_matches {
        return Err(Reason::EvidenceScopeMismatch);
    }
    if evidence.partial || evidence.coverage != CoverageStatus::Available {
        frame.reasons.push(Reason::SourcePartial);
        frame.qualified = false;
    }
    if !facts.technical_scope_bound {
        frame.reasons.push(Reason::ExactSliSourceMissing);
        frame.qualified = false;
    }
    let version_matches = facts.target_version_observed.as_deref() == Some(policy.declared_target_version.as_str());
    if !version_matches {
        frame.reasons.push(Reason::TargetVersionNotObserved);
        frame.qualified = false;
    }
    match (facts.resource_observed_at, facts.technical_sli_observed_at) {
        (Some(resource), Some(technical)) => {
            let oldest = resource.min(technical);
            if resource > evidence.observed_at || technical > evidence.observed_at || evidence.observed_at > as_of {
                frame.reasons.push(Reason::SourceFuture);
                frame.qualified = false;
            } else {
                let measured_age = nanos(evidence.observed_at) - nanos(oldest);
                let measured_age_seconds =
                    u64::try_from((measured_age + 999_999_999) / 1_000_000_000).map_err(|_| Reason::SourceStale)?;
                let captured_age = evidence.freshness_seconds.max(measured_age_seconds);
                let elapsed = nanos(as_of) - nanos(evidence.observed_at);
                let total_age = i128::from(captured_age) * 1_000_000_000 + elapsed;
                if total_age > i128::from(policy.max_source_age_seconds) * 1_000_000_000 {
                    frame.reasons.push(Reason::SourceStale);
                    frame.qualified = false;
                }
                frame.sample = Some(VerificationWindowSample {
                    evidence_id: evidence.evidence_id,
                    content_hash: evidence.content_hash.clone(),
                    resource_observed_at: resource,
                    technical_sli_observed_at: technical,
                    collected_at: evidence.observed_at,
                    source_age_seconds: captured_age,
                    elapsed_nanos,
                    complete: frame.qualified,
                    resource_conditions_satisfied: request
                        .resource_conditions
                        .iter()
                        .all(|condition| observation.resource_conditions.get(condition) == Some(&true)),
                    technical_slis_satisfied: request
                        .technical_slis
                        .iter()
                        .all(|condition| observation.technical_slis.get(condition) == Some(&true)),
                    target_version_matched: version_matches,
                });
            }
        }
        _ => {
            frame.reasons.push(Reason::SourceMissing);
            frame.qualified = false;
        }
    }
    evaluate_conditions(request, observation, &mut frame);
    if let Some(sample) = &mut frame.sample {
        sample.complete = frame.qualified;
    }
    Ok(frame)
}

fn evaluate_conditions(request: &VerificationCaptureRequest, observation: &VerificationObservation, frame: &mut Frame) {
    for (prefix, expected, actual) in [
        (
            "resource",
            &request.resource_conditions,
            &observation.resource_conditions,
        ),
        ("sli", &request.technical_slis, &observation.technical_slis),
    ] {
        for condition in expected {
            let code = format!("{prefix}:{condition}");
            match (frame.qualified, actual.get(condition)) {
                (true, Some(true)) => frame.satisfied.push(code),
                (true, Some(false)) => {
                    frame.failed.push(code);
                    frame.all_conditions = false;
                }
                _ => {
                    frame.missing.push(code);
                    frame.all_conditions = false;
                }
            }
        }
    }
    if !frame.missing.is_empty() {
        frame.qualified = false;
        if frame.reasons.is_empty() {
            frame.reasons.push(Reason::SourceMissing);
        }
        frame.satisfied.clear();
        frame.failed.clear();
    }
}

pub(super) fn nanos(value: DateTime<Utc>) -> i128 {
    i128::from(value.timestamp()) * 1_000_000_000 + i128::from(value.timestamp_subsec_nanos())
}
