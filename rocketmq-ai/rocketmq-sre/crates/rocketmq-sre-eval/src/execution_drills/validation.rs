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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ExecutionAction;

use super::MAX_CASES;
use super::model::*;

const WIRED: [ExecutionAction; 9] = [
    ExecutionAction::ObservabilityLoggerLevelTtl,
    ExecutionAction::BrokerConfigPatchAllowlisted,
    ExecutionAction::TopicConfigPatchAllowlisted,
    ExecutionAction::SubscriptionGroupPatchAllowlisted,
    ExecutionAction::ProxyScaleOutOne,
    ExecutionAction::ProxyRestartOne,
    ExecutionAction::ProxyRolloutImageCanary,
    ExecutionAction::TelemetryCollectorRestartOne,
    ExecutionAction::SecurityCredentialRotateOverlap,
];

/// List every action/class even when no record exists. Counts describe input
/// claims, not independent verification, a complete scenario matrix or authority.
pub(super) fn coverage(cases: &[Case]) -> Vec<CoverageSummary> {
    WIRED
        .into_iter()
        .flat_map(|action| {
            [
                EvidenceClass::DeterministicContract,
                EvidenceClass::ProtocolFault,
                EvidenceClass::IsolatedDriver,
                EvidenceClass::NormalAdmission,
            ]
            .into_iter()
            .map(move |evidence_class| {
                let count = |outcome| {
                    cases
                        .iter()
                        .filter(|case| {
                            case.action == action && case.evidence_class == evidence_class && case.outcome == outcome
                        })
                        .count()
                };
                CoverageSummary {
                    action,
                    descriptor_version: "1.0.0",
                    evidence_class,
                    passed_case_claims: count(Outcome::Passed),
                    failed_case_claims: count(Outcome::Failed),
                    blocked_case_claims: count(Outcome::Blocked),
                    not_run_case_claims: count(Outcome::NotRun),
                }
            })
        })
        .collect()
}

pub(super) fn validate(report: &Report) -> Result<(), Rejection> {
    if !literal(&report.run_id, 64) || report.cases.is_empty() || report.cases.len() > MAX_CASES {
        return Err(Rejection::InvalidRecord);
    }
    let mut seen = BTreeSet::new();
    for case in &report.cases {
        validate_case(case)?;
        let version = case.observed_software_version.as_ref().map(|value| &value.value);
        let key = (
            case.action,
            &case.descriptor_version,
            case.tenant_id,
            case.cluster_id,
            &case.target_id,
            &case.target_artifact_identity,
            version,
            case.scenario,
            case.evidence_class,
            case.execution_path,
        );
        if !seen.insert(key) {
            return Err(Rejection::DuplicateCase);
        }
    }
    Ok(())
}

fn validate_case(case: &Case) -> Result<(), Rejection> {
    if !WIRED.contains(&case.action)
        || case.descriptor_version != "1.0.0"
        || case.tenant_id.as_uuid().is_nil()
        || case.cluster_id.as_uuid().is_nil()
        || !literal(&case.target_id, 64)
        || case.finished_at < case.started_at
        || case.artifact_references.len() > 32
        || case.limitations.len() > 16
        || case.artifact_references.iter().any(|reference| !literal(reference, 96))
        || unique(&case.artifact_references).is_err()
        || unique(&case.limitations).is_err()
    {
        return Err(Rejection::InvalidRecord);
    }
    let expected_path = match case.evidence_class {
        EvidenceClass::DeterministicContract => ExecutionPath::DeterministicHarness,
        EvidenceClass::ProtocolFault => ExecutionPath::InjectedExecutorAgent,
        EvidenceClass::IsolatedDriver => ExecutionPath::DirectHandlerAdapter,
        EvidenceClass::NormalAdmission => ExecutionPath::NormalControlPlane,
    };
    if case.execution_path != expected_path {
        return Err(Rejection::InvalidRecord);
    }
    if let Some(artifact) = &case.target_artifact_identity
        && (!literal(&artifact.opaque_id, 64)
            || (case.evidence_class == EvidenceClass::IsolatedDriver
                && artifact.kind == ArtifactKind::DeterministicFixture))
    {
        return Err(Rejection::InvalidRecord);
    }
    match &case.observed_software_version {
        Some(version)
            if case.evidence_class != EvidenceClass::IsolatedDriver
                || !version_label(&version.value)
                || !case.artifact_references.contains(&version.observation_reference)
                || case.limitations.contains(&Limitation::SoftwareVersionUnknown)
                || case.limitations.contains(&Limitation::GenericKubernetesMechanicsOnly)
                || (version.source == VersionSource::ControlledBinarySelfReport
                    && case
                        .target_artifact_identity
                        .as_ref()
                        .is_none_or(|artifact| artifact.kind != ArtifactKind::ControlledChildBinary))
                || (version.source == VersionSource::DeployedBinarySelfReport
                    && case
                        .target_artifact_identity
                        .as_ref()
                        .is_none_or(|artifact| artifact.kind != ArtifactKind::DeployedImage)) =>
        {
            return Err(Rejection::InvalidRecord);
        }
        None if !case.limitations.contains(&Limitation::SoftwareVersionUnknown) => {
            return Err(Rejection::InvalidRecord);
        }
        Some(_) | None => {}
    }
    if !case.limitations.contains(&Limitation::PhysicalAttestationUnavailable)
        || !case
            .limitations
            .contains(&Limitation::IndependentBusinessObservationUnavailable)
        || (case.evidence_class == EvidenceClass::ProtocolFault
            && !case.limitations.contains(&Limitation::InjectedAuthority))
    {
        return Err(Rejection::InvalidRecord);
    }
    let counters = &case.counters;
    let total = [
        counters.accepted_identities,
        counters.forward_effects,
        counters.compensation_effects,
        counters.stale_effects,
        counters.new_idempotency_keys,
        counters.quarantines,
    ]
    .into_iter()
    .flatten()
    .try_fold(
        0u64,
        |total, value| {
            if value > 128 { None } else { total.checked_add(value) }
        },
    );
    if total.is_none() {
        return Err(Rejection::LimitExceeded);
    }
    if let Some(identity) = &case.accepted_identity {
        if identity.execution_id.as_uuid().is_nil()
            || identity.correlation_id.as_uuid().is_nil()
            || identity.plan_id.as_uuid().is_nil()
            || identity.plan_step_id.as_uuid().is_nil()
            || !digest(&identity.plan_hash)
            || !digest(&identity.original_selector_digest)
            || identity.lease_epoch.as_ref().is_some_and(|epoch| {
                epoch
                    .parse::<u64>()
                    .ok()
                    .is_none_or(|value| value == 0 || value > i64::MAX as u64 || value.to_string() != *epoch)
            })
        {
            return Err(Rejection::InvalidRecord);
        }
    } else if case.counters.accepted_identities.is_some_and(|count| count > 0)
        || case.original_identity_preserved == Some(true)
    {
        return Err(Rejection::InvalidRecord);
    }
    if case.outcome != Outcome::Passed {
        return Ok(());
    }
    if case.artifact_references.is_empty()
        || counters.stale_effects != Some(0)
        || counters.new_idempotency_keys != Some(0)
        || (case.evidence_class == EvidenceClass::IsolatedDriver
            && (case.cleanup_confirmed != Some(true) || case.target_artifact_identity.is_none()))
    {
        return Err(Rejection::InvalidRecord);
    }
    if [
        counters.accepted_identities,
        counters.forward_effects,
        counters.compensation_effects,
        counters.quarantines,
    ]
    .into_iter()
    .any(|value| !matches!(value, Some(0 | 1)))
        || (case.observed_state == ObservedState::Quarantined) != (counters.quarantines == Some(1))
    {
        return Err(Rejection::InvalidRecord);
    }
    validate_passed_scenario(case)
}

fn validate_passed_scenario(case: &Case) -> Result<(), Rejection> {
    let rejection_guard = match case.scenario {
        Scenario::ExpiredApproval => Some(Guard::ApprovalExpired),
        Scenario::ExpiredFence => Some(Guard::FenceExpired),
        Scenario::FrozenAdmission => Some(Guard::ScopeFrozen),
        Scenario::NormalUnqualifiedDenied => Some(Guard::QualificationUnknown),
        Scenario::GuardedPositiveControl
        | Scenario::AcceptedDuplicate
        | Scenario::TargetDrift
        | Scenario::ResponseLoss
        | Scenario::TargetDisconnected
        | Scenario::VerificationUnknown
        | Scenario::VerificationFailed
        | Scenario::CompensationFailed
        | Scenario::OwnerCancelled => None,
    };
    if let Some(guard) = rejection_guard
        && (case.guard != Some(guard)
            || case.guard_reached != Some(true)
            || case.counters.forward_effects != Some(0)
            || case.counters.compensation_effects != Some(0)
            || case.observed_state != ObservedState::Rejected)
    {
        return Err(Rejection::InvalidRecord);
    }
    match case.scenario {
        Scenario::AcceptedDuplicate => {
            if case.evidence_class != EvidenceClass::ProtocolFault
                || case.original_identity_preserved != Some(true)
                || case.counters.accepted_identities != Some(1)
                || case.counters.forward_effects != Some(1)
                || case.counters.compensation_effects != Some(0)
                || case.observed_state != ObservedState::RecordedEffect
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::TargetDrift => {
            if !matches!(case.guard, Some(Guard::SelectorChanged | Guard::GenerationChanged))
                || case.guard_reached != Some(true)
                || case.counters.forward_effects != Some(0)
                || case.counters.compensation_effects != Some(0)
                || case.observed_state != ObservedState::Rejected
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::VerificationUnknown => {
            if case.counters.compensation_effects != Some(0)
                || !matches!(
                    case.observed_state,
                    ObservedState::UnknownEffect | ObservedState::Quarantined
                )
                || !original_accepted(case)
                || !matches!(
                    case.guard,
                    Some(Guard::SourceUnknown | Guard::SourceStale | Guard::TargetVersionUnknown)
                )
                || case.guard_reached != Some(true)
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::CompensationFailed => {
            if case.observed_state != ObservedState::Quarantined
                || case.counters.quarantines != Some(1)
                || case.counters.forward_effects != Some(1)
                || case.counters.compensation_effects != Some(1)
                || !original_accepted(case)
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::ResponseLoss | Scenario::TargetDisconnected | Scenario::OwnerCancelled => {
            if !original_accepted(case)
                || case.counters.compensation_effects != Some(0)
                || (case.observed_state == ObservedState::RecordedEffect && case.counters.forward_effects != Some(1))
                || !matches!(
                    case.observed_state,
                    ObservedState::UnknownEffect | ObservedState::Quarantined | ObservedState::RecordedEffect
                )
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::NormalUnqualifiedDenied => {
            if case.evidence_class != EvidenceClass::NormalAdmission || case.counters.accepted_identities != Some(0) {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::GuardedPositiveControl => {
            let path_bound = match case.execution_path {
                ExecutionPath::InjectedExecutorAgent | ExecutionPath::DeterministicHarness => original_accepted(case),
                ExecutionPath::DirectHandlerAdapter => case.counters.accepted_identities == Some(0),
                ExecutionPath::NormalControlPlane => false,
            };
            if !path_bound
                || case.counters.forward_effects != Some(1)
                || case.counters.compensation_effects != Some(0)
                || case.observed_state != ObservedState::RecordedEffect
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::VerificationFailed => {
            if !original_accepted(case)
                || case.guard != Some(Guard::QualifiedConditionFailed)
                || case.guard_reached != Some(true)
                || case.counters.forward_effects != Some(1)
                || !matches!(
                    case.observed_state,
                    ObservedState::Restored | ObservedState::Quarantined
                )
                || (case.observed_state == ObservedState::Restored && case.counters.compensation_effects != Some(1))
            {
                return Err(Rejection::InvalidRecord);
            }
        }
        Scenario::ExpiredApproval | Scenario::ExpiredFence | Scenario::FrozenAdmission => {}
    }
    Ok(())
}

fn original_accepted(case: &Case) -> bool {
    case.original_identity_preserved == Some(true)
        && case.accepted_identity.is_some()
        && case.counters.accepted_identities == Some(1)
}

fn unique<T: Ord>(values: &[T]) -> Result<(), Rejection> {
    if values.iter().collect::<BTreeSet<_>>().len() == values.len() {
        Ok(())
    } else {
        Err(Rejection::InvalidRecord)
    }
}

fn literal(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn version_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
}

fn digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}
