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

use crate::supervised_execution::repository::{ObservationJournal, VerificationRecord};
use chrono::{DateTime, Utc};
use rocketmq_sre_contracts::{
    CoverageStatus, EvidenceContent, EvidenceExposure, EvidenceSnapshot, ExecutionObservationReason as Reason,
    PlanStep, VerificationOutcome, VerificationProofOrigin, VerificationResult, VerificationSpec,
    VerificationWindowProof, canonical_sha256,
};

pub(super) fn validate(
    journal: &ObservationJournal,
    row: &VerificationRecord,
    step: &PlanStep,
    result: &VerificationResult,
    declaration: Option<&VerificationSpec>,
    proof: &VerificationWindowProof,
) -> Result<(), Reason> {
    proof.verify().map_err(|_| Reason::WindowProofInvalid)?;
    let allowed = step
        .verification
        .resource_conditions
        .iter()
        .map(|name| format!("resource:{name}"))
        .chain(
            step.verification
                .technical_slis
                .iter()
                .map(|name| format!("sli:{name}")),
        )
        .collect::<std::collections::BTreeSet<_>>();
    for conditions in [&result.satisfied_conditions, &result.failed_conditions] {
        if conditions.len() > 64
            || conditions.iter().collect::<std::collections::BTreeSet<_>>().len() != conditions.len()
            || conditions.iter().any(|name| !allowed.contains(name))
        {
            return Err(Reason::WindowProofInvalid);
        }
    }
    if result.outcome == VerificationOutcome::Succeeded
        && (!result.failed_conditions.is_empty()
            || result
                .satisfied_conditions
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                != allowed)
    {
        return Err(Reason::WindowProofInvalid);
    }
    let request = &journal.execution.request;
    if proof.tenant_id != request.tenant_id
        || proof.cluster_id != request.cluster_id
        || proof.execution_id != request.id
        || proof.step_id != row.step_id
        || proof.plan_step_id != step.id
        || proof.action != step.action
        || proof.descriptor_version != step.descriptor_version
        || proof.target != step.resource
        || proof.parameters_digest != canonical_sha256(&step.parameters).map_err(|_| Reason::WindowProofInvalid)?
        || proof.started_at != result.started_at
        || proof.completed_at != result.completed_at
    {
        return Err(Reason::WindowProofInvalid);
    }
    let Some(policy) = &proof.policy else {
        return if result.outcome == VerificationOutcome::Inconclusive
            && proof.samples.is_empty()
            && proof.observed_stable_seconds == 0
        {
            Ok(())
        } else {
            Err(Reason::WindowProofInvalid)
        };
    };
    if declaration != Some(&step.verification)
        || policy.stable_window_seconds != step.verification.stable_window_seconds
        || policy.max_wait_seconds != step.verification.max_wait_seconds
        || proof.observed_stable_seconds > policy.max_wait_seconds
        || (result.outcome == VerificationOutcome::Succeeded
            && (proof.provenance == VerificationProofOrigin::RecordedOnly
                || proof.target_version_observed.as_ref() != Some(&policy.declared_target_version)
                || !proof.missing_conditions.is_empty()))
    {
        return Err(Reason::WindowProofInvalid);
    }
    let mut first = None;
    let mut previous: Option<&rocketmq_sre_contracts::VerificationWindowSample> = None;
    let mut stable = 0_i128;
    let mut known_failure = false;
    for sample in &proof.samples {
        let record = journal
            .evidence
            .iter()
            .find(|record| {
                record.step_id == row.step_id
                    && record.attempt == row.attempt
                    && record.evidence_id == sample.evidence_id.as_uuid()
                    && record.phase == if row.compensation { "rollback_post" } else { "post" }
            })
            .ok_or(Reason::RetainedProofUnavailable)?;
        let snapshot: EvidenceSnapshot =
            serde_json::from_value(record.snapshot.clone().ok_or(Reason::RetainedProofUnavailable)?)
                .map_err(|_| Reason::WindowProofInvalid)?;
        snapshot.verify_content_hash().map_err(|_| Reason::WindowProofInvalid)?;
        if !result.post_evidence_ids.contains(&sample.evidence_id)
            || snapshot.evidence_id != sample.evidence_id
            || snapshot.content_hash != sample.content_hash
            || snapshot.tenant_id != request.tenant_id
            || snapshot.cluster_id != request.cluster_id
            || snapshot.correlation_id != request.correlation_id
            || snapshot.resource != step.resource
            || snapshot.observed_at != sample.collected_at
            || nanos(sample.collected_at) < nanos(proof.started_at)
        {
            return Err(Reason::WindowProofInvalid);
        }
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return Err(Reason::RetainedProofUnavailable);
        };
        let facts = content.get("source_facts").ok_or(Reason::WindowProofInvalid)?;
        let origin = serde_json::from_value::<VerificationProofOrigin>(
            facts.get("provenance").cloned().ok_or(Reason::WindowProofInvalid)?,
        )
        .map_err(|_| Reason::WindowProofInvalid)?;
        let clock = |name: &str| {
            facts
                .get(name)
                .and_then(serde_json::Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
        };
        if origin != proof.provenance
            || clock("resource_observed_at") != Some(sample.resource_observed_at)
            || clock("technical_sli_observed_at") != Some(sample.technical_sli_observed_at)
            || (origin == VerificationProofOrigin::SyntheticFixture && snapshot.exposure != EvidenceExposure::Synthetic)
            || (origin == VerificationProofOrigin::ProductionRead
                && (snapshot.exposure == EvidenceExposure::Synthetic || snapshot.source != "execution-verification"))
        {
            return Err(Reason::WindowProofInvalid);
        }
        let oldest = sample.resource_observed_at.min(sample.technical_sli_observed_at);
        let raw_age = nanos(sample.collected_at) - nanos(oldest);
        if raw_age < 0
            || u64::try_from((raw_age + 999_999_999) / 1_000_000_000)
                .ok()
                .is_none_or(|age| sample.source_age_seconds != age.max(snapshot.freshness_seconds))
        {
            return Err(Reason::WindowProofInvalid);
        }
        let resource = step
            .verification
            .resource_conditions
            .iter()
            .map(|name| {
                content
                    .get("resource_conditions")
                    .and_then(|map| map.get(name))
                    .and_then(serde_json::Value::as_bool)
            })
            .collect::<Option<Vec<_>>>();
        let technical = step
            .verification
            .technical_slis
            .iter()
            .map(|name| {
                content
                    .get("technical_slis")
                    .and_then(|map| map.get(name))
                    .and_then(serde_json::Value::as_bool)
            })
            .collect::<Option<Vec<_>>>();
        let full = resource.is_some() && technical.is_some();
        if full
            && (resource
                .as_ref()
                .is_some_and(|values| values.iter().all(|value| *value))
                != sample.resource_conditions_satisfied
                || technical
                    .as_ref()
                    .is_some_and(|values| values.iter().all(|value| *value))
                    != sample.technical_slis_satisfied)
        {
            return Err(Reason::WindowProofInvalid);
        }
        let data_valid = sample.complete
            && full
            && !snapshot.partial
            && snapshot.coverage == CoverageStatus::Available
            && facts.get("technical_scope_bound").and_then(serde_json::Value::as_bool) == Some(true)
            && facts.get("target_version_observed").and_then(serde_json::Value::as_str)
                == Some(policy.declared_target_version.as_str())
            && sample.target_version_matched
            && sample.source_age_seconds <= policy.max_source_age_seconds
            && oldest >= proof.started_at
            && sample.elapsed_nanos <= policy.max_wait_seconds * 1_000_000_000;
        known_failure |= data_valid && (!sample.resource_conditions_satisfied || !sample.technical_slis_satisfied);
        if let Some(old) = previous {
            if sample.resource_observed_at < old.resource_observed_at
                || sample.technical_sli_observed_at < old.technical_sli_observed_at
            {
                first = None;
                stable = 0;
                continue;
            }
            if sample.resource_observed_at == old.resource_observed_at
                || sample.technical_sli_observed_at == old.technical_sli_observed_at
            {
                if !data_valid
                    || sample.resource_conditions_satisfied != old.resource_conditions_satisfied
                    || sample.technical_slis_satisfied != old.technical_slis_satisfied
                {
                    first = None;
                    stable = 0;
                }
                continue;
            }
        }
        let ordered = previous.is_none_or(|old: &rocketmq_sre_contracts::VerificationWindowSample| {
            sample.resource_observed_at > old.resource_observed_at
                && sample.technical_sli_observed_at > old.technical_sli_observed_at
                && sample.elapsed_nanos > old.elapsed_nanos
                && sample.elapsed_nanos - old.elapsed_nanos <= policy.max_sample_gap_seconds * 1_000_000_000
                && nanos(sample.resource_observed_at) - nanos(old.resource_observed_at)
                    <= i128::from(policy.max_sample_gap_seconds) * 1_000_000_000
                && nanos(sample.technical_sli_observed_at) - nanos(old.technical_sli_observed_at)
                    <= i128::from(policy.max_sample_gap_seconds) * 1_000_000_000
        });
        if !data_valid || !ordered || !sample.resource_conditions_satisfied || !sample.technical_slis_satisfied {
            first = None;
            stable = 0;
        }
        if data_valid && sample.resource_conditions_satisfied && sample.technical_slis_satisfied {
            let first = first.get_or_insert(sample);
            stable = i128::from(sample.elapsed_nanos - first.elapsed_nanos)
                .min(nanos(sample.resource_observed_at) - nanos(first.resource_observed_at))
                .min(nanos(sample.technical_sli_observed_at) - nanos(first.technical_sli_observed_at));
        }
        previous = Some(sample);
    }
    if result.outcome == VerificationOutcome::Succeeded
        && (stable < i128::from(policy.stable_window_seconds) * 1_000_000_000
            || proof.observed_stable_seconds
                != u64::try_from(stable / 1_000_000_000).map_err(|_| Reason::WindowProofInvalid)?)
    {
        return Err(Reason::WindowProofInvalid);
    }
    if result.outcome == VerificationOutcome::Failed && !known_failure {
        return Err(Reason::WindowProofInvalid);
    }
    Ok(())
}

fn nanos(value: DateTime<Utc>) -> i128 {
    i128::from(value.timestamp()) * 1_000_000_000 + i128::from(value.timestamp_subsec_nanos())
}
