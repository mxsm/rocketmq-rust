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

use super::codec::crc32;
use super::codec::decode_acknowledgement_file;
use super::codec::decode_acknowledgement_slot;
use super::codec::decode_commit_seal;
use super::codec::decode_next_frame;
use super::codec::encode_acknowledgement_slot;
use super::codec::encode_commit_seal;
use super::codec::encode_ledger_frame;
use super::codec::validate_acknowledged_frame;
use super::codec::AcknowledgementSlot;
use super::codec::AcknowledgementSlotState;
use super::codec::CommitSeal;
use super::codec::DecodeOutcome;
use super::codec::LedgerRecord;
use super::codec::OpenReason;
use super::sidecar::decode_snapshot;
use super::sidecar::encode_enabled_marker_slot;
use super::sidecar::EnabledMarkerFile;
use super::sidecar::EnabledMarkerSlot;
use super::sidecar::LifecycleSnapshot;
use super::sidecar::SnapshotMode;
use super::sidecar::StoreMeta;
use super::state::LedgerStateMachine;
use super::state::NeedsReconciliation;
use super::state::StateViolation;
use super::state::WriterRecoveryFrontier;

mod acknowledgement;
pub(in crate::mapped_file::retirement) mod discovery;
mod error;
mod model;
mod parsing;
mod semantic;
mod validation;

pub use discovery::{
    inspect_managed_lifecycle_read_only, inspect_managed_lifecycle_read_only_with_limits,
    inspect_managed_lifecycle_under_exclusive_lock, LockedManagedLifecycleInspection, ManagedLifecycleReadLimits,
    ManagedLifecycleReadOutcome, ManagedLifecycleRecoveryReason, ManagedLifecycleSession,
};
pub(crate) use error::ReplayViolation;
use model::*;

/// Explicit work bounds for pure replay over caller-owned sidecar bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplayLimits {
    max_generations: usize,
    max_sealed_units: usize,
}

impl Default for ReplayLimits {
    fn default() -> Self {
        Self {
            max_generations: 8,
            max_sealed_units: 1_000_000,
        }
    }
}

/// Immutable bytes for one snapshot/log generation pair.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GenerationBytes<'a> {
    generation: u64,
    snapshot: &'a [u8],
    log: &'a [u8],
}

/// Complete pure input for bounded replay. Filesystem discovery and reads happen outside M3.2.
#[derive(Debug)]
pub(crate) struct ReplayInput<'a> {
    store_meta: &'a StoreMeta,
    marker: &'a EnabledMarkerFile,
    acknowledgement_slots: [&'a [u8]; 2],
    generations: Vec<GenerationBytes<'a>>,
    limits: ReplayLimits,
}

/// Pure recovery classification. No variant executes writes or grants publication authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryDecision {
    NeedsReconciliation(NeedsReconciliation),
    AcknowledgeSelectedAnchor(AcknowledgeSelectedAnchorDecision),
    CompleteSeal(CompleteSealDecision),
    CompleteMarkerWitness(CompleteMarkerWitnessDecision),
    TailRepair(TailRepairDecision),
    ResumeGeneration(ResumeGenerationDecision),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcknowledgeSelectedAnchorDecision {
    source_generation: u64,
    generation: u64,
    frame_sequence: u64,
    frame_end_offset: u64,
    expected_acknowledgement_slot: [u8; super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
    expected_seal: [u8; super::codec::COMMIT_SEAL_LENGTH],
}

impl AcknowledgeSelectedAnchorDecision {
    pub(crate) const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn frame_sequence(&self) -> u64 {
        self.frame_sequence
    }

    pub(crate) const fn frame_end_offset(&self) -> u64 {
        self.frame_end_offset
    }

    pub(crate) const fn expected_acknowledgement_slot(&self) -> &[u8; super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH] {
        &self.expected_acknowledgement_slot
    }

    pub(crate) const fn expected_seal(&self) -> &[u8; super::codec::COMMIT_SEAL_LENGTH] {
        &self.expected_seal
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompleteSealDecision {
    generation: u64,
    frame_sequence: u64,
    frame_end_offset: u64,
    expected_seal: [u8; super::codec::COMMIT_SEAL_LENGTH],
    available_seal_bytes: usize,
}

impl CompleteSealDecision {
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn frame_sequence(&self) -> u64 {
        self.frame_sequence
    }

    pub(crate) const fn frame_end_offset(&self) -> u64 {
        self.frame_end_offset
    }

    pub(crate) const fn expected_seal(&self) -> &[u8; super::codec::COMMIT_SEAL_LENGTH] {
        &self.expected_seal
    }

    pub(crate) const fn available_seal_bytes(&self) -> usize {
        self.available_seal_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompleteMarkerWitnessDecision {
    generation: u64,
    anchor_sequence: u64,
    expected_frame: Vec<u8>,
    available_frame_bytes: usize,
    expected_acknowledgement_slot: [u8; super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
    expected_seal: [u8; super::codec::COMMIT_SEAL_LENGTH],
}

impl CompleteMarkerWitnessDecision {
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn anchor_sequence(&self) -> u64 {
        self.anchor_sequence
    }

    pub(crate) fn expected_frame(&self) -> &[u8] {
        &self.expected_frame
    }

    pub(crate) const fn available_frame_bytes(&self) -> usize {
        self.available_frame_bytes
    }

    pub(crate) const fn expected_acknowledgement_slot(&self) -> &[u8; super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH] {
        &self.expected_acknowledgement_slot
    }

    pub(crate) const fn expected_seal(&self) -> &[u8; super::codec::COMMIT_SEAL_LENGTH] {
        &self.expected_seal
    }
}

/// Classifies recovery without performing any filesystem or namespace action.
#[allow(
    clippy::too_many_lines,
    reason = "the recovery decision order mirrors the frozen crash-classification table"
)]
pub(crate) fn replay(input: ReplayInput<'_>) -> Result<RecoveryDecision, ReplayViolation> {
    if input.generations.len() > input.limits.max_generations {
        return Err(ReplayViolation::LimitExceeded {
            limit: "generations",
            actual: input.generations.len(),
            maximum: input.limits.max_generations,
        });
    }
    let marker = input.marker.selected_slot().map_err(ReplayViolation::Marker)?;
    let mut evidence = Vec::new();
    let mut parsed = BTreeMap::new();
    for generation in input.generations {
        if parsed.contains_key(&generation.generation) {
            return Err(ReplayViolation::DuplicateGeneration {
                generation: generation.generation,
            });
        }
        let decoded = parsing::parse_generation(generation, input.store_meta, input.limits, &mut evidence)?;
        parsed.insert(generation.generation, decoded);
    }
    validation::validate_all_sealed_units(&parsed, &evidence)?;
    validation::validate_generation_set(marker.log_generation, &parsed)?;
    validation::validate_retained_generation_links(&parsed, &evidence)?;
    validation::validate_marker_history(input.marker, input.store_meta, &parsed, &evidence)?;
    validation::validate_gc_backlog_markers(
        input.marker,
        input.store_meta,
        marker.log_generation,
        &parsed,
        &evidence,
    )?;
    let mut semantic_states = semantic::replay_generations(&parsed, &evidence)?;
    let selected = parsed
        .get(&marker.log_generation)
        .ok_or(ReplayViolation::MissingSelectedGeneration {
            generation: marker.log_generation,
        })?;
    validation::validate_marker_binding(marker, input.store_meta, selected)?;

    let resolved = resolve_acknowledgement(input.acknowledgement_slots, &evidence)?;
    let authoritative = &resolved.authoritative;
    if authoritative.store_uuid != input.store_meta.store_uuid
        || authoritative.bootstrap_id != input.store_meta.bootstrap_id
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    if authoritative.log_generation != marker.log_generation {
        if authoritative.log_generation.checked_add(1) == Some(marker.log_generation) {
            return acknowledge_selected_anchor(
                input.store_meta,
                input.marker,
                marker,
                &parsed,
                &evidence,
                &resolved,
                &mut semantic_states,
            );
        }
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    if authoritative.frame_sequence < marker.anchor_sequence {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    let selected_evidence = evidence
        .get(selected.evidence_range.clone())
        .ok_or(ReplayViolation::BrokenSealChain {
            generation: selected.bytes.generation,
        })?;
    let complete = selected_evidence
        .iter()
        .find(|unit| &unit.slot == authoritative && unit.encoded_slot == resolved.encoded_authoritative);
    let committed_sequence = if complete.is_some() {
        authoritative.frame_sequence
    } else {
        authoritative
            .frame_sequence
            .checked_sub(1)
            .ok_or(ReplayViolation::AuthoritativeFrameMissing)?
    };
    let predecessor = marker
        .log_generation
        .checked_sub(1)
        .and_then(|generation| parsed.get(&generation));
    validation::validate_selected_administration(
        selected,
        predecessor,
        &evidence,
        marker,
        input.store_meta,
        committed_sequence,
    )?;
    let Some(authoritative_unit) = complete else {
        let pending = semantic_states
            .get_mut(&selected.bytes.generation)
            .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
        if pending.sealed_through != committed_sequence {
            return Err(ReplayViolation::AuthoritativeFrameMissing);
        }
        let frame = selected
            .tail
            .as_ref()
            .and_then(|tail| tail.complete_frame.as_ref())
            .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
        if pending.applied_through == committed_sequence {
            pending
                .state
                .apply(frame.sequence, frame.record.clone())
                .map_err(ReplayViolation::State)?;
            pending.applied_through = frame.sequence;
        } else if pending.applied_through != frame.sequence {
            return Err(ReplayViolation::AuthoritativeFrameMissing);
        }
        if frame.sequence == marker.anchor_sequence {
            let witness_sequence = marker
                .anchor_sequence
                .checked_add(1)
                .ok_or(ReplayViolation::State(StateViolation::SequenceOverflow))?;
            pending
                .state
                .apply(witness_sequence, Some(validation::expected_marker_record(marker)?))
                .map_err(ReplayViolation::State)?;
            require_recovery_headroom(authoritative.frame_sequence, authoritative.acknowledgement_epoch, 2)?;
        } else if marker.anchor_sequence.checked_add(1) == Some(frame.sequence) {
            require_recovery_headroom(authoritative.frame_sequence, authoritative.acknowledgement_epoch, 1)?;
        }
        let prior = selected_evidence
            .last()
            .or_else(|| predecessor.and_then(|generation| evidence.get(generation.evidence_range.clone())?.last()));
        return classify_partial_seal(selected, marker, &resolved, prior);
    };
    if authoritative_unit.sealed_log_length
        != authoritative
            .sealed_log_length()
            .map_err(|source| ReplayViolation::InvalidAcknowledgementSlot {
                slot_index: authoritative.slot_index,
                source,
            })?
    {
        return Err(ReplayViolation::AuthoritativeFrameMissing);
    }
    validate_activation_binding(authoritative, marker)?;
    let selected_semantic = semantic_states
        .remove(&selected.bytes.generation)
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    if selected_semantic.sealed_through != authoritative.frame_sequence
        || selected_semantic.applied_through != authoritative.frame_sequence
    {
        return Err(ReplayViolation::AuthoritativeFrameMissing);
    }
    let mut selected_state = selected_semantic.state;

    let higher_generation = marker.log_generation.checked_add(1);
    let higher = higher_generation.and_then(|generation| parsed.get(&generation));
    if let Some(candidate) = higher {
        validation::validate_higher_candidate(candidate, selected, &evidence)?;
        let _anchor = candidate
            .tail
            .as_ref()
            .and_then(|tail| tail.complete_frame.as_ref())
            .ok_or(ReplayViolation::AmbiguousGenerationSet)?;
    }
    if let Some(LedgerRecord::GenerationPrepared {
        store_uuid,
        source_generation,
        target_generation,
        target_snapshot_generation,
        open_reason,
    }) = authoritative_unit.record.as_ref()
    {
        if *store_uuid != input.store_meta.store_uuid
            || *source_generation != selected.bytes.generation
            || Some(*target_generation) != higher_generation
            || *target_snapshot_generation != *target_generation
            || *open_reason != OpenReason::Compaction
            || selected.tail.is_some()
            || higher.is_some_and(|candidate| candidate.bytes.generation != *target_generation)
        {
            return Err(ReplayViolation::GenerationBindingMismatch);
        }
        require_recovery_headroom(authoritative.frame_sequence, authoritative.acknowledgement_epoch, 3)?;
        return Ok(RecoveryDecision::ResumeGeneration(ResumeGenerationDecision {
            source_generation: *source_generation,
            target_generation: *target_generation,
        }));
    }
    if let Some(candidate) = higher {
        let Some(LogTail {
            complete_frame:
                Some(TrailingFrame {
                    record: Some(LedgerRecord::LogOpened { open_reason, .. }),
                    ..
                }),
            ..
        }) = candidate.tail.as_ref()
        else {
            return Err(ReplayViolation::AmbiguousGenerationSet);
        };
        if *open_reason != OpenReason::TailRepair {
            return Err(ReplayViolation::AmbiguousGenerationSet);
        }
        require_recovery_headroom(authoritative.frame_sequence, authoritative.acknowledgement_epoch, 3)?;
        return Ok(RecoveryDecision::ResumeGeneration(ResumeGenerationDecision {
            source_generation: selected.bytes.generation,
            target_generation: candidate.bytes.generation,
        }));
    }

    if authoritative.frame_sequence == marker.anchor_sequence {
        let witness_sequence = marker
            .anchor_sequence
            .checked_add(1)
            .ok_or(ReplayViolation::State(StateViolation::SequenceOverflow))?;
        selected_state
            .apply(witness_sequence, Some(validation::expected_marker_record(marker)?))
            .map_err(ReplayViolation::State)?;
        require_recovery_headroom(authoritative.frame_sequence, authoritative.acknowledgement_epoch, 2)?;
        return complete_marker_witness(
            input.store_meta,
            selected,
            marker,
            authoritative,
            authoritative_unit.sealed_log_length,
        );
    }
    if let Some(tail) = &selected.tail {
        if tail.offset != authoritative_unit.sealed_log_length {
            return Err(ReplayViolation::AuthoritativeFrameMissing);
        }
        let suffix_length = validate_unacknowledged_suffix_length(tail.bytes.len())?;
        return Ok(RecoveryDecision::TailRepair(TailRepairDecision {
            generation: selected.bytes.generation,
            acknowledged_prefix_length: authoritative_unit.sealed_log_length,
            suffix_length,
            suffix_crc32: crc32(&tail.bytes),
        }));
    }

    let recovered = selected_state
        .finish(
            authoritative.frame_sequence,
            authoritative.acknowledgement_epoch,
            marker.marker_epoch,
        )
        .map_err(ReplayViolation::State)?;
    let next_sequence = authoritative
        .frame_sequence
        .checked_add(1)
        .ok_or(ReplayViolation::State(StateViolation::SequenceOverflow))?;
    let next_acknowledgement_epoch = authoritative
        .acknowledgement_epoch
        .checked_add(1)
        .ok_or(ReplayViolation::BrokenAcknowledgementChain)?;
    let writer_frontier = WriterRecoveryFrontier::from_validated_replay(
        input.store_meta.store_uuid,
        input.store_meta.bootstrap_id,
        selected.bytes.generation,
        next_sequence,
        next_acknowledgement_epoch,
        authoritative_unit.sealed_log_length,
        marker.marker_epoch,
    );
    Ok(RecoveryDecision::NeedsReconciliation(NeedsReconciliation::new(
        recovered,
        writer_frontier,
    )))
}

fn resolve_acknowledgement(
    raw_slots: [&[u8]; 2],
    seal_evidence: &[SealEvidence],
) -> Result<ResolvedAcknowledgement, ReplayViolation> {
    acknowledgement::resolve_acknowledgement(raw_slots, seal_evidence)
}

fn validate_unacknowledged_suffix_length(length: usize) -> Result<u32, ReplayViolation> {
    acknowledgement::validate_unacknowledged_suffix_length(length)
}

fn require_recovery_headroom(
    sequence: u64,
    acknowledgement_epoch: u64,
    remaining_steps: u64,
) -> Result<(), ReplayViolation> {
    sequence
        .checked_add(remaining_steps)
        .ok_or(ReplayViolation::State(StateViolation::SequenceOverflow))?;
    acknowledgement_epoch
        .checked_add(remaining_steps)
        .ok_or(ReplayViolation::BrokenAcknowledgementChain)?;
    Ok(())
}

fn acknowledge_selected_anchor(
    meta: &StoreMeta,
    marker_file: &EnabledMarkerFile,
    marker: &EnabledMarkerSlot,
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
    resolved: &ResolvedAcknowledgement,
    semantic_states: &mut BTreeMap<u64, semantic::SemanticGeneration>,
) -> Result<RecoveryDecision, ReplayViolation> {
    let source = parsed
        .get(&resolved.authoritative.log_generation)
        .ok_or(ReplayViolation::AmbiguousGenerationSet)?;
    let selected = parsed
        .get(&marker.log_generation)
        .ok_or(ReplayViolation::MissingSelectedGeneration {
            generation: marker.log_generation,
        })?;
    validation::validate_higher_candidate(selected, source, evidence)?;
    let source_units = evidence
        .get(source.evidence_range.clone())
        .ok_or(ReplayViolation::BrokenSealChain {
            generation: source.bytes.generation,
        })?;
    let terminal = source_units
        .iter()
        .find(|unit| unit.slot == resolved.authoritative && unit.encoded_slot == resolved.encoded_authoritative)
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let source_marker = marker_file
        .slots
        .iter()
        .flatten()
        .find(|candidate| candidate.log_generation == source.bytes.generation)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let source_predecessor = source
        .bytes
        .generation
        .checked_sub(1)
        .and_then(|generation| parsed.get(&generation));
    validation::validate_selected_administration(
        source,
        source_predecessor,
        evidence,
        source_marker,
        meta,
        resolved.authoritative.frame_sequence,
    )?;
    let anchor = selected
        .tail
        .as_ref()
        .and_then(|tail| tail.complete_frame.as_ref())
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let Some(LedgerRecord::LogOpened { open_reason, .. }) = anchor.record.as_ref() else {
        return Err(ReplayViolation::GenerationBindingMismatch);
    };
    match (open_reason, terminal.record.as_ref()) {
        (
            OpenReason::Compaction,
            Some(LedgerRecord::GenerationPrepared {
                store_uuid,
                source_generation,
                target_generation,
                target_snapshot_generation,
                open_reason: OpenReason::Compaction,
                ..
            }),
        ) if *store_uuid == meta.store_uuid
            && *source_generation == source.bytes.generation
            && *target_generation == selected.bytes.generation
            && *target_snapshot_generation == selected.snapshot.generation
            && source.tail.is_none() => {}
        (OpenReason::TailRepair, record)
            if source.tail.is_some() && !matches!(record, Some(LedgerRecord::GenerationPrepared { .. })) => {}
        _ => return Err(ReplayViolation::GenerationBindingMismatch),
    }
    let target = semantic_states
        .get_mut(&selected.bytes.generation)
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    if target.sealed_through != selected.snapshot.base_sequence || target.applied_through != anchor.sequence {
        return Err(ReplayViolation::AuthoritativeFrameMissing);
    }
    let witness_sequence = anchor
        .sequence
        .checked_add(1)
        .ok_or(ReplayViolation::State(StateViolation::SequenceOverflow))?;
    target
        .state
        .apply(witness_sequence, Some(validation::expected_marker_record(marker)?))
        .map_err(ReplayViolation::State)?;
    require_recovery_headroom(
        resolved.authoritative.frame_sequence,
        resolved.authoritative.acknowledgement_epoch,
        3,
    )?;
    if marker.anchor_sequence != anchor.sequence || marker.anchor_frame_crc32 != crc32(&anchor.encoded_frame) {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    let acknowledgement_epoch = resolved
        .authoritative
        .acknowledgement_epoch
        .checked_add(1)
        .ok_or(ReplayViolation::BrokenAcknowledgementChain)?;
    let slot = AcknowledgementSlot {
        slot_index: ((acknowledgement_epoch - 1) & 1) as u8,
        activated: true,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        acknowledgement_epoch,
        marker_epoch: marker.marker_epoch,
        log_generation: selected.bytes.generation,
        frame_sequence: anchor.sequence,
        frame_end_offset: anchor.frame_end_offset,
        frame_crc32: crc32(&anchor.encoded_frame),
    };
    let expected_acknowledgement_slot =
        encode_acknowledgement_slot(&slot).map_err(|source| ReplayViolation::InvalidAcknowledgementSlot {
            slot_index: slot.slot_index,
            source,
        })?;
    let seal = CommitSeal::from_acknowledgement_slot(&slot, &expected_acknowledgement_slot).map_err(|source| {
        ReplayViolation::InvalidAcknowledgementSlot {
            slot_index: slot.slot_index,
            source,
        }
    })?;
    let expected_seal = encode_commit_seal(&seal).map_err(|source| ReplayViolation::InvalidLog {
        generation: selected.bytes.generation,
        offset: anchor.frame_end_offset,
        source,
    })?;
    Ok(RecoveryDecision::AcknowledgeSelectedAnchor(
        AcknowledgeSelectedAnchorDecision {
            source_generation: source.bytes.generation,
            generation: selected.bytes.generation,
            frame_sequence: anchor.sequence,
            frame_end_offset: anchor.frame_end_offset,
            expected_acknowledgement_slot,
            expected_seal,
        },
    ))
}

fn classify_partial_seal(
    selected: &ParsedGeneration<'_>,
    marker: &EnabledMarkerSlot,
    resolved: &ResolvedAcknowledgement,
    prior: Option<&SealEvidence>,
) -> Result<RecoveryDecision, ReplayViolation> {
    let tail = selected
        .tail
        .as_ref()
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let frame = tail
        .complete_frame
        .as_ref()
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let slot = &resolved.authoritative;
    if let Some(prior) = prior {
        if prior.slot.acknowledgement_epoch.checked_add(1) != Some(slot.acknowledgement_epoch)
            || prior.slot.frame_sequence.checked_add(1) != Some(slot.frame_sequence)
        {
            return Err(ReplayViolation::BrokenAcknowledgementChain);
        }
    }
    if frame.sequence != slot.frame_sequence
        || frame.frame_end_offset != slot.frame_end_offset
        || crc32(&frame.encoded_frame) != slot.frame_crc32
    {
        return Err(ReplayViolation::AuthoritativeFrameMissing);
    }
    if marker.anchor_sequence.checked_add(1) == Some(frame.sequence) {
        let expected_frame = validation::expected_marker_frame(marker, frame.sequence)?;
        if frame.encoded_frame != expected_frame {
            return Err(ReplayViolation::GenerationBindingMismatch);
        }
    }
    validate_activation_binding(slot, marker)?;
    let seal = CommitSeal::from_acknowledgement_slot(slot, &resolved.encoded_authoritative).map_err(|source| {
        ReplayViolation::InvalidAcknowledgementSlot {
            slot_index: slot.slot_index,
            source,
        }
    })?;
    let expected_seal = encode_commit_seal(&seal).map_err(|source| ReplayViolation::InvalidLog {
        generation: slot.log_generation,
        offset: slot.frame_end_offset,
        source,
    })?;
    if frame.following_bytes.len() >= super::codec::COMMIT_SEAL_LENGTH
        || !expected_seal.starts_with(&frame.following_bytes)
    {
        return Err(ReplayViolation::PartialSealMismatch);
    }
    Ok(RecoveryDecision::CompleteSeal(CompleteSealDecision {
        generation: slot.log_generation,
        frame_sequence: slot.frame_sequence,
        frame_end_offset: slot.frame_end_offset,
        expected_seal,
        available_seal_bytes: frame.following_bytes.len(),
    }))
}

fn complete_marker_witness(
    meta: &StoreMeta,
    selected: &ParsedGeneration<'_>,
    marker: &EnabledMarkerSlot,
    authoritative: &AcknowledgementSlot,
    acknowledged_prefix_length: u64,
) -> Result<RecoveryDecision, ReplayViolation> {
    let witness_sequence = marker
        .anchor_sequence
        .checked_add(1)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let expected_frame = validation::expected_marker_frame(marker, witness_sequence)?;
    let available = match &selected.tail {
        None => 0,
        Some(tail) if tail.offset == acknowledged_prefix_length && expected_frame.starts_with(&tail.bytes) => {
            tail.bytes.len()
        }
        Some(_) => return Err(ReplayViolation::GenerationBindingMismatch),
    };
    let frame_end_offset = acknowledged_prefix_length
        .checked_add(u64::try_from(expected_frame.len()).map_err(|_| ReplayViolation::GenerationBindingMismatch)?)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let acknowledgement_epoch = authoritative
        .acknowledgement_epoch
        .checked_add(1)
        .ok_or(ReplayViolation::BrokenAcknowledgementChain)?;
    let slot = AcknowledgementSlot {
        slot_index: ((acknowledgement_epoch - 1) & 1) as u8,
        activated: true,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        acknowledgement_epoch,
        marker_epoch: marker.marker_epoch,
        log_generation: selected.bytes.generation,
        frame_sequence: witness_sequence,
        frame_end_offset,
        frame_crc32: crc32(&expected_frame),
    };
    let expected_acknowledgement_slot =
        encode_acknowledgement_slot(&slot).map_err(|source| ReplayViolation::InvalidAcknowledgementSlot {
            slot_index: slot.slot_index,
            source,
        })?;
    let seal = CommitSeal::from_acknowledgement_slot(&slot, &expected_acknowledgement_slot).map_err(|source| {
        ReplayViolation::InvalidAcknowledgementSlot {
            slot_index: slot.slot_index,
            source,
        }
    })?;
    let expected_seal = encode_commit_seal(&seal).map_err(|source| ReplayViolation::InvalidLog {
        generation: selected.bytes.generation,
        offset: frame_end_offset,
        source,
    })?;
    Ok(RecoveryDecision::CompleteMarkerWitness(CompleteMarkerWitnessDecision {
        generation: selected.bytes.generation,
        anchor_sequence: marker.anchor_sequence,
        expected_frame,
        available_frame_bytes: available,
        expected_acknowledgement_slot,
        expected_seal,
    }))
}

fn validate_activation_binding(slot: &AcknowledgementSlot, marker: &EnabledMarkerSlot) -> Result<(), ReplayViolation> {
    let valid = if slot.frame_sequence == marker.anchor_sequence && marker.log_generation == 0 {
        !slot.activated && slot.marker_epoch == 0
    } else {
        slot.activated && slot.marker_epoch == marker.marker_epoch
    };
    if !valid {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
