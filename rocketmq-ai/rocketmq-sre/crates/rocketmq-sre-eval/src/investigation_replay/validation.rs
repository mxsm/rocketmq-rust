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

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use rocketmq_sre_contracts::*;
use rocketmq_sre_core::diagnostics::{DiagnosticPack, DiagnosticPackRegistry, PackVersion, full_registry};
use serde::Serialize;

use super::*;

// Same complete-response bound as ConnectorChannel, not its capability hint.
pub(super) const COLLECTION_BYTES: u64 = 512 * 1024;
pub(super) const LOCAL_BYTES: u64 = 65_536;
const MAX_CASE_BYTES: u64 = 16 * 1024 * 1024;

pub(super) fn text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && value.trim() == value && !value.chars().any(char::is_control)
}

pub(super) fn validate_case(case: &InvestigationReplayCase) -> Result<(), InvestigationReplayRejection> {
    use InvestigationReplayRejection as Error;
    if case.schema_version != INVESTIGATION_REPLAY_SCHEMA_VERSION
        || !text(&case.case_id, 128)
        || case.packs.is_empty()
        || case.packs.len() > 16
        || case.bindings.len() > 256
        || case.reads.len() > 256
        || !case.packs.contains(&case.baseline_pack)
        || case.as_of < case.scope.time_range.end
    {
        return Err(Error::InvalidCase);
    }
    json_len(case, MAX_CASE_BYTES)?;
    case.scope.validate()?;
    let provenance = match &case.provenance {
        ReplayProvenance::SyntheticFixture { fixture_ref } => fixture_ref,
        ReplayProvenance::RedactedCapture { source_ref } => source_ref,
    };
    if !text(provenance, 512) {
        return Err(Error::InvalidCase);
    }
    let registry = full_registry().map_err(|_| Error::UnknownPack)?;
    let packs: BTreeSet<_> = case.packs.iter().collect();
    if packs.len() != case.packs.len() {
        return Err(Error::AmbiguousFixture);
    }
    for pack in &case.packs {
        exact_pack(&registry, pack)?;
    }
    let mut requirements = BTreeSet::new();
    for binding in &case.bindings {
        if !case.packs.contains(&binding.requirement.pack) {
            return Err(Error::UnknownPack);
        }
        let pack = exact_pack(&registry, &binding.requirement.pack)?;
        let requirement = pack
            .required_evidence()
            .iter()
            .chain(pack.optional_evidence())
            .find(|requirement| requirement.key == binding.requirement.requirement_id)
            .ok_or(Error::InvalidCase)?;
        if requirement.source != binding.source
            || !binding.resource.starts_with(requirement.resource_prefix)
            || !text(&binding.resource, 512)
        {
            return Err(Error::ScopeMismatch);
        }
        if !requirements.insert(&binding.requirement) {
            return Err(Error::AmbiguousFixture);
        }
    }
    let mut identities = BTreeSet::new();
    let mut evidence = BTreeMap::new();
    for read in &case.reads {
        if !identities.insert((&read.source, &read.resource)) || read.outcomes.is_empty() || read.outcomes.len() > 3 {
            return Err(Error::AmbiguousFixture);
        }
        if !case
            .bindings
            .iter()
            .any(|binding| binding.source == read.source && binding.resource == read.resource)
        {
            return Err(Error::ScopeMismatch);
        }
        for outcome in &read.outcomes {
            if let ReplayReadOutcome::SavedEvidence { snapshot } = outcome {
                if snapshot.tenant_id != case.scope.tenant_id
                    || snapshot.cluster_id != case.scope.cluster_id
                    || snapshot.time_range != case.scope.time_range
                    || snapshot.source != read.source
                    || snapshot.resource != read.resource
                    || snapshot.evidence_id.as_uuid().is_nil()
                    || snapshot.query_id.as_uuid().is_nil()
                    || snapshot.correlation_id.as_uuid().is_nil()
                {
                    return Err(Error::ScopeMismatch);
                }
                if !matches!(snapshot.content, EvidenceContent::Inline(_)) || snapshot.verify_content_hash().is_err() {
                    return Err(Error::InvalidEvidence);
                }
                json_len(&response_frame(outcome, 12), COLLECTION_BYTES)?;
                let digest = canonical_sha256(snapshot).map_err(|_| Error::InvalidEvidence)?;
                if evidence
                    .insert(snapshot.evidence_id, digest.clone())
                    .is_some_and(|old| old != digest)
                {
                    return Err(Error::AmbiguousFixture);
                }
            }
        }
    }
    // Run creation validates the same independent read/model/retry/deadline caps.
    let context = context(case, &case.packs, &[])?;
    InvestigationRun::create(&context, &create_request(case, &case.packs), None, case.as_of)?;
    Ok(())
}

/// Fixed wire-shaped accounting only. No session is registered and no RPC is
/// sent; immutable saved query/correlation IDs remain intact in the Evidence.
pub(super) fn response_frame(outcome: &ReplayReadOutcome, sequence: u64) -> ConnectorResponseEnvelope {
    let (evidence, error_code, retryable) = match outcome {
        ReplayReadOutcome::SavedEvidence { snapshot } => (Some(snapshot.as_ref().clone()), None, false),
        ReplayReadOutcome::RetryableUnavailable {} => (None, Some("source_unavailable".to_owned()), true),
        ReplayReadOutcome::Rejected {} => (None, Some("source_rejected".to_owned()), false),
        ReplayReadOutcome::OutcomeUnknown {} => (None, None, false),
    };
    let correlation_id = evidence
        .as_ref()
        .map_or_else(CorrelationId::new, |snapshot| snapshot.correlation_id);
    ConnectorResponseEnvelope {
        schema: SchemaVersion::new("rocketmq-sre.connector-channel", 1, 0),
        session_id: ConnectorSessionId::new(),
        correlation_id,
        sequence,
        evidence,
        error_code,
        retryable,
    }
}

pub(super) fn exact_pack<'a>(
    registry: &'a DiagnosticPackRegistry,
    pack: &InvestigationPackRef,
) -> Result<&'a dyn DiagnosticPack, InvestigationReplayRejection> {
    let version = pack
        .pack_version
        .split('.')
        .map(str::parse::<u16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| InvestigationReplayRejection::UnknownPack)?;
    let [major, minor, patch] = version.as_slice() else {
        return Err(InvestigationReplayRejection::UnknownPack);
    };
    let (base, _) = pack
        .pack_id
        .rsplit_once(".v")
        .ok_or(InvestigationReplayRejection::UnknownPack)?;
    registry
        .get(base, PackVersion::new(*major, *minor, *patch))
        .filter(|value| value.qualified_id() == pack.pack_id && value.version().to_string() == pack.pack_version)
        .ok_or(InvestigationReplayRejection::UnknownPack)
}

pub(super) fn create_request(
    case: &InvestigationReplayCase,
    packs: &[InvestigationPackRef],
) -> CreateInvestigationRunRequest {
    CreateInvestigationRunRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: InvestigationRunId::new(),
        scope: case.scope.clone(),
        packs: packs.to_vec(),
        budget: case.budget.clone(),
        deadline: case.deadline,
        idempotency_key: "fixed-evidence-replay".to_owned(),
    }
}

pub(super) fn context(
    case: &InvestigationReplayCase,
    packs: &[InvestigationPackRef],
    evidence: &[EvidenceSnapshot],
) -> Result<InvestigationRunContext, InvestigationReplayRejection> {
    let registry = full_registry().map_err(|_| InvestigationReplayRejection::UnknownPack)?;
    let mut allowed_requirements = vec![];
    for reference in packs {
        let pack = exact_pack(&registry, reference)?;
        allowed_requirements.extend(pack.required_evidence().iter().chain(pack.optional_evidence()).map(
            |requirement| InvestigationRequirementRef {
                pack: reference.clone(),
                requirement_id: requirement.key.to_owned(),
            },
        ));
    }
    Ok(InvestigationRunContext {
        actor_subject: "fixed-evidence-replay".to_owned(),
        permission: InvestigationRunPermission::Control,
        authorized_scope: case.scope.clone(),
        allowed_packs: packs.to_vec(),
        allowed_requirements,
        visible_evidence: evidence
            .iter()
            .map(|snapshot| InvestigationEvidenceContext {
                evidence_id: snapshot.evidence_id,
                scope: case.scope.clone(),
            })
            .collect(),
    })
}

pub(super) fn json_len(value: &impl Serialize, maximum: u64) -> Result<u64, InvestigationReplayRejection> {
    struct Counter {
        used: u64,
        maximum: u64,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let next = self
                .used
                .checked_add(bytes.len() as u64)
                .filter(|next| *next <= self.maximum)
                .ok_or_else(|| io::Error::other("bounded replay output"))?;
            self.used = next;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Counter { used: 0, maximum };
    serde_json::to_writer(&mut writer, value).map_err(|_| InvestigationReplayRejection::InputTooLarge)?;
    Ok(writer.used)
}
