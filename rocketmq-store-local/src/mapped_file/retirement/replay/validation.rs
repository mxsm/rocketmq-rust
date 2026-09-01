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

pub(super) fn validate_generation_set(
    selected_generation: u64,
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
) -> Result<(), ReplayViolation> {
    if !parsed.contains_key(&selected_generation) {
        return Err(ReplayViolation::MissingSelectedGeneration {
            generation: selected_generation,
        });
    }
    let predecessor = selected_generation.saturating_sub(1);
    let maximum = selected_generation.saturating_add(1);
    if parsed.keys().any(|generation| *generation > maximum)
        || (selected_generation > 0 && !parsed.contains_key(&predecessor))
    {
        return Err(ReplayViolation::AmbiguousGenerationSet);
    }
    Ok(())
}

pub(super) fn validate_all_snapshot_states(
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
) -> Result<(), ReplayViolation> {
    for generation in parsed.values() {
        LedgerStateMachine::from_snapshot(&generation.snapshot).map_err(ReplayViolation::State)?;
    }
    Ok(())
}

pub(super) fn validate_marker_binding(
    marker: &EnabledMarkerSlot,
    meta: &StoreMeta,
    selected: &ParsedGeneration<'_>,
) -> Result<(), ReplayViolation> {
    if marker.log_generation.checked_add(1) != Some(marker.marker_epoch)
        || marker.store_uuid != meta.store_uuid
        || marker.bootstrap_id != meta.bootstrap_id
        || marker.snapshot_generation != marker.log_generation
        || marker.log_generation != selected.bytes.generation
        || selected.snapshot.store_uuid != meta.store_uuid
        || selected.snapshot.generation != marker.snapshot_generation
        || selected.snapshot.log_generation != marker.log_generation
        || marker.snapshot_file_length != selected.bytes.snapshot.len() as u64
        || marker.snapshot_file_crc32 != crc32(selected.bytes.snapshot)
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    Ok(())
}

pub(super) fn validate_marker_history(
    marker_file: &EnabledMarkerFile,
    meta: &StoreMeta,
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    let authoritative_marker = marker_file.selected_slot().map_err(ReplayViolation::Marker)?;
    for marker in marker_file.slots.iter().flatten() {
        let generation = parsed
            .get(&marker.log_generation)
            .ok_or(ReplayViolation::GenerationBindingMismatch)?;
        validate_marker_binding(marker, meta, generation)?;
        let units = evidence
            .get(generation.evidence_range.clone())
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: generation.bytes.generation,
            })?;
        if units
            .iter()
            .filter(|unit| unit.slot.activated)
            .any(|unit| unit.slot.marker_epoch != marker.marker_epoch)
        {
            return Err(ReplayViolation::GenerationBindingMismatch);
        }
        if generation.bytes.generation == 0 {
            validate_bootstrap_prefix(generation, units, marker, meta, marker.anchor_sequence)?;
            continue;
        }
        validate_generation_marker_anchor(
            generation,
            parsed,
            units,
            marker,
            evidence,
            marker.marker_epoch == authoritative_marker.marker_epoch,
        )?;
    }
    Ok(())
}

pub(super) fn validate_gc_backlog_markers(
    marker_file: &EnabledMarkerFile,
    meta: &StoreMeta,
    selected_generation: u64,
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    for (&generation_number, generation) in parsed.range(..selected_generation) {
        if marker_file
            .slots
            .iter()
            .flatten()
            .any(|marker| marker.log_generation == generation_number)
        {
            continue;
        }
        let units = evidence
            .get(generation.evidence_range.clone())
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: generation_number,
            })?;
        let (anchor_index, anchor) = if generation_number == 0 {
            (1, units.get(1))
        } else {
            (0, units.first())
        };
        let anchor = anchor.ok_or(ReplayViolation::GenerationBindingMismatch)?;
        let marker_epoch = generation_number
            .checked_add(1)
            .ok_or(ReplayViolation::GenerationBindingMismatch)?;
        let marker = EnabledMarkerSlot {
            slot_index: ((marker_epoch - 1) & 1) as u8,
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            marker_epoch,
            snapshot_generation: generation.snapshot.generation,
            log_generation: generation_number,
            anchor_sequence: anchor.slot.frame_sequence,
            snapshot_file_length: generation.bytes.snapshot.len() as u64,
            snapshot_file_crc32: crc32(generation.bytes.snapshot),
            anchor_frame_crc32: crc32(&anchor.encoded_frame),
        };
        validate_marker_binding(&marker, meta, generation)?;
        if generation_number == 0 {
            validate_bootstrap_prefix(
                generation,
                units,
                &marker,
                meta,
                marker
                    .anchor_sequence
                    .checked_add(1)
                    .ok_or(ReplayViolation::GenerationBindingMismatch)?,
            )?;
        } else {
            validate_generation_marker_anchor(generation, parsed, units, &marker, evidence, false)?;
        }
        let witness_sequence = marker
            .anchor_sequence
            .checked_add(1)
            .ok_or(ReplayViolation::GenerationBindingMismatch)?;
        let witness = units
            .get(anchor_index + 1)
            .ok_or(ReplayViolation::GenerationBindingMismatch)?;
        if witness.generation != generation_number
            || witness.slot.frame_sequence != witness_sequence
            || witness.encoded_frame != expected_marker_frame(&marker, witness_sequence)?
            || witness.record.is_none()
        {
            return Err(ReplayViolation::GenerationBindingMismatch);
        }
    }
    Ok(())
}

fn validate_generation_marker_anchor(
    generation: &ParsedGeneration<'_>,
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    units: &[SealEvidence],
    marker: &EnabledMarkerSlot,
    evidence: &[SealEvidence],
    is_authoritative_marker: bool,
) -> Result<(), ReplayViolation> {
    let synthetic;
    let anchor = if let Some(anchor) = units.first() {
        anchor
    } else if is_authoritative_marker {
        let frame = generation
            .tail
            .as_ref()
            .and_then(|tail| tail.complete_frame.as_ref())
            .ok_or(ReplayViolation::GenerationBindingMismatch)?;
        synthetic = synthetic_anchor(generation, frame);
        &synthetic
    } else {
        return Err(ReplayViolation::GenerationBindingMismatch);
    };
    let Some(LedgerRecord::LogOpened {
        store_uuid,
        generation: opened_generation,
        snapshot_generation,
        predecessor_log_generation,
        predecessor_terminal_acknowledged_sequence,
        snapshot_base_sequence,
        snapshot_file_length,
        snapshot_file_crc32,
        unacknowledged_suffix_length,
        unacknowledged_suffix_crc32,
        open_reason,
        ..
    }) = anchor.record.as_ref()
    else {
        return Err(ReplayViolation::GenerationBindingMismatch);
    };
    let predecessor_generation = generation
        .bytes
        .generation
        .checked_sub(1)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let expected_sequence = generation
        .snapshot
        .base_sequence
        .checked_add(1)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let expected_mode = match open_reason {
        OpenReason::Compaction => SnapshotMode::OrdinaryCompaction,
        OpenReason::TailRepair => SnapshotMode::TailRepair,
    };
    let suffix_length =
        usize::try_from(*unacknowledged_suffix_length).map_err(|_| ReplayViolation::GenerationBindingMismatch)?;
    let suffix_shape_is_valid = match open_reason {
        OpenReason::Compaction => *unacknowledged_suffix_length == 0 && *unacknowledged_suffix_crc32 == 0,
        OpenReason::TailRepair => validate_unacknowledged_suffix_length(suffix_length).is_ok(),
    };
    if anchor.frame_start_offset != 0
        || anchor.slot.frame_sequence != expected_sequence
        || marker.anchor_sequence != anchor.slot.frame_sequence
        || marker.anchor_frame_crc32 != anchor.slot.frame_crc32
        || *store_uuid != generation.snapshot.store_uuid
        || *opened_generation != generation.bytes.generation
        || *snapshot_generation != generation.snapshot.generation
        || *predecessor_log_generation != predecessor_generation
        || *predecessor_terminal_acknowledged_sequence != generation.snapshot.base_sequence
        || generation.snapshot.predecessor_log_generation != predecessor_generation
        || *snapshot_base_sequence != generation.snapshot.base_sequence
        || *snapshot_file_length != generation.bytes.snapshot.len() as u64
        || *snapshot_file_crc32 != crc32(generation.bytes.snapshot)
        || generation.snapshot.mode != expected_mode
        || !suffix_shape_is_valid
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    if let Some(predecessor) = parsed.get(&predecessor_generation) {
        validate_log_opened(generation, predecessor, anchor, evidence)?;
    }
    Ok(())
}

pub(super) fn validate_all_sealed_units(
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    for generation in parsed.values() {
        let units = evidence
            .get(generation.evidence_range.clone())
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: generation.bytes.generation,
            })?;
        if units.is_empty() {
            continue;
        }
        if generation.bytes.generation == 0 {
            validate_bootstrap_activation(units)?;
        } else {
            validate_generation_activation(generation, units)?;
        }
    }
    Ok(())
}

pub(super) fn validate_retained_generation_links(
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    for (&generation, current) in parsed.range(1..) {
        let Some(predecessor_generation) = generation.checked_sub(1) else {
            return Err(ReplayViolation::AmbiguousGenerationSet);
        };
        let Some(predecessor) = parsed.get(&predecessor_generation) else {
            continue;
        };
        let units = evidence
            .get(current.evidence_range.clone())
            .ok_or(ReplayViolation::BrokenSealChain { generation })?;
        let synthetic;
        let anchor = if let Some(anchor) = units.first() {
            anchor
        } else if let Some(frame) = current.tail.as_ref().and_then(|tail| tail.complete_frame.as_ref()) {
            synthetic = synthetic_anchor(current, frame);
            &synthetic
        } else {
            continue;
        };
        validate_log_opened(current, predecessor, anchor, evidence)?;
    }
    Ok(())
}

pub(super) fn validate_selected_administration(
    selected: &ParsedGeneration<'_>,
    predecessor: Option<&ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
    marker: &EnabledMarkerSlot,
    meta: &StoreMeta,
    authoritative_sequence: u64,
) -> Result<(), ReplayViolation> {
    let units = evidence
        .get(selected.evidence_range.clone())
        .ok_or(ReplayViolation::BrokenSealChain {
            generation: selected.bytes.generation,
        })?;
    if selected.bytes.generation == 0 {
        validate_bootstrap_prefix(selected, units, marker, meta, authoritative_sequence)
    } else {
        let predecessor = predecessor.ok_or(ReplayViolation::AmbiguousGenerationSet)?;
        let synthetic;
        let anchor = if let Some(anchor) = units.first() {
            anchor
        } else {
            let frame = selected
                .tail
                .as_ref()
                .and_then(|tail| tail.complete_frame.as_ref())
                .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
            synthetic = synthetic_anchor(selected, frame);
            &synthetic
        };
        validate_log_opened(selected, predecessor, anchor, evidence)?;
        if marker.anchor_sequence != anchor.slot.frame_sequence || marker.anchor_frame_crc32 != anchor.slot.frame_crc32
        {
            return Err(ReplayViolation::GenerationBindingMismatch);
        }
        validate_marker_witness_if_present(units, marker, authoritative_sequence)
    }
}

pub(super) fn expected_marker_record(marker: &EnabledMarkerSlot) -> Result<LedgerRecord, ReplayViolation> {
    let encoded_marker = encode_enabled_marker_slot(marker).map_err(ReplayViolation::Marker)?;
    let stored_crc = u32::from_le_bytes(
        encoded_marker[100..104]
            .try_into()
            .map_err(|_| ReplayViolation::GenerationBindingMismatch)?,
    );
    Ok(LedgerRecord::MarkerCommitted {
        store_uuid: marker.store_uuid,
        marker_epoch: marker.marker_epoch,
        snapshot_generation: marker.snapshot_generation,
        log_generation: marker.log_generation,
        anchor_sequence: marker.anchor_sequence,
        slot_index: marker.slot_index,
        slot_crc32: stored_crc,
    })
}

pub(super) fn expected_marker_frame(marker: &EnabledMarkerSlot, sequence: u64) -> Result<Vec<u8>, ReplayViolation> {
    let record = expected_marker_record(marker)?;
    encode_ledger_frame(&record, sequence, marker.log_generation).map_err(|source| ReplayViolation::InvalidLog {
        generation: marker.log_generation,
        offset: 0,
        source,
    })
}

pub(super) fn validate_higher_candidate(
    candidate: &ParsedGeneration<'_>,
    source: &ParsedGeneration<'_>,
    evidence: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    if !candidate.evidence_range.is_empty() {
        return Err(ReplayViolation::AmbiguousGenerationSet);
    }
    let Some(tail) = &candidate.tail else {
        return Err(ReplayViolation::AmbiguousGenerationSet);
    };
    let Some(frame) = &tail.complete_frame else {
        return Err(ReplayViolation::AmbiguousGenerationSet);
    };
    if tail.offset != 0 || !frame.following_bytes.is_empty() || tail.bytes != frame.encoded_frame {
        return Err(ReplayViolation::AmbiguousGenerationSet);
    }
    let synthetic = synthetic_anchor(candidate, frame);
    validate_log_opened(candidate, source, &synthetic, evidence)
}

fn synthetic_anchor(generation: &ParsedGeneration<'_>, frame: &TrailingFrame) -> SealEvidence {
    SealEvidence {
        slot: AcknowledgementSlot {
            slot_index: 0,
            activated: true,
            store_uuid: generation.snapshot.store_uuid,
            bootstrap_id: [1; 16],
            acknowledgement_epoch: 1,
            marker_epoch: 1,
            log_generation: generation.bytes.generation,
            frame_sequence: frame.sequence,
            frame_end_offset: frame.frame_end_offset,
            frame_crc32: crc32(&frame.encoded_frame),
        },
        encoded_slot: [0; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
        generation: generation.bytes.generation,
        sealed_log_length: frame.frame_end_offset + super::super::codec::COMMIT_SEAL_LENGTH as u64,
        frame_start_offset: 0,
        encoded_frame: frame.encoded_frame.clone(),
        record: frame.record.clone(),
    }
}

fn validate_bootstrap_prefix(
    selected: &ParsedGeneration<'_>,
    units: &[SealEvidence],
    marker: &EnabledMarkerSlot,
    meta: &StoreMeta,
    authoritative_sequence: u64,
) -> Result<(), ReplayViolation> {
    if selected.snapshot.mode != SnapshotMode::BootstrapInventory
        || selected.snapshot.base_sequence != 1
        || marker.anchor_sequence != 2
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    let initialized = units.first().ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let installed = units.get(1).ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let expected_initialized = LedgerRecord::StoreInitialized {
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        creation_time_ns: meta.creation_time_ns,
    };
    let expected_installed = LedgerRecord::BootstrapInstalled {
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        snapshot_generation: 0,
        snapshot_base_sequence: 1,
        snapshot_file_length: selected.bytes.snapshot.len() as u64,
        snapshot_file_crc32: crc32(selected.bytes.snapshot),
        inventory_count: selected.snapshot.entries.len() as u64,
        create_high_water: selected.snapshot.create_high_water,
        ticket_high_water: selected.snapshot.ticket_high_water,
    };
    if initialized.slot.frame_sequence != 1
        || initialized.record.as_ref() != Some(&expected_initialized)
        || installed.slot.frame_sequence != 2
        || installed.record.as_ref() != Some(&expected_installed)
        || marker.anchor_frame_crc32 != installed.slot.frame_crc32
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    validate_marker_witness_if_present(units, marker, authoritative_sequence)
}

fn validate_bootstrap_activation(units: &[SealEvidence]) -> Result<(), ReplayViolation> {
    let marker_epoch = units
        .iter()
        .find(|unit| unit.slot.frame_sequence >= 3)
        .map(|unit| unit.slot.marker_epoch);
    for unit in units {
        if unit.slot.frame_sequence <= 2 {
            if unit.slot.activated || unit.slot.marker_epoch != 0 {
                return Err(ReplayViolation::BrokenSealChain { generation: 0 });
            }
        } else if !unit.slot.activated || Some(unit.slot.marker_epoch) != marker_epoch {
            return Err(ReplayViolation::BrokenSealChain { generation: 0 });
        }
    }
    if let Some(epoch) = marker_epoch {
        if epoch != 1 {
            return Err(ReplayViolation::BrokenSealChain { generation: 0 });
        }
        let witness = units
            .iter()
            .find(|unit| unit.slot.frame_sequence == 3)
            .ok_or(ReplayViolation::BrokenSealChain { generation: 0 })?;
        if !matches!(
            witness.record.as_ref(),
            Some(LedgerRecord::MarkerCommitted {
                marker_epoch,
                snapshot_generation: 0,
                log_generation: 0,
                anchor_sequence: 2,
                ..
            }) if *marker_epoch == epoch
        ) {
            return Err(ReplayViolation::BrokenSealChain { generation: 0 });
        }
    }
    Ok(())
}

fn validate_generation_activation(
    generation: &ParsedGeneration<'_>,
    units: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    let marker_epoch = units[0].slot.marker_epoch;
    if generation.bytes.generation.checked_add(1) != Some(marker_epoch)
        || units
            .iter()
            .any(|unit| !unit.slot.activated || unit.slot.marker_epoch != marker_epoch)
    {
        return Err(ReplayViolation::BrokenSealChain {
            generation: generation.bytes.generation,
        });
    }
    if !matches!(units[0].record.as_ref(), Some(LedgerRecord::LogOpened { .. })) {
        return Err(ReplayViolation::BrokenSealChain {
            generation: generation.bytes.generation,
        });
    }
    if let Some(witness) = units.get(1) {
        let anchor = units[0].slot.frame_sequence;
        if !matches!(
            witness.record.as_ref(),
            Some(LedgerRecord::MarkerCommitted {
                marker_epoch: witness_epoch,
                snapshot_generation,
                log_generation,
                anchor_sequence,
                ..
            }) if *witness_epoch == marker_epoch
                && *snapshot_generation == generation.bytes.generation
                && *log_generation == generation.bytes.generation
                && *anchor_sequence == anchor
        ) {
            return Err(ReplayViolation::BrokenSealChain {
                generation: generation.bytes.generation,
            });
        }
    }
    Ok(())
}

fn validate_marker_witness_if_present(
    units: &[SealEvidence],
    marker: &EnabledMarkerSlot,
    authoritative_sequence: u64,
) -> Result<(), ReplayViolation> {
    if authoritative_sequence <= marker.anchor_sequence {
        return Ok(());
    }
    let witness_sequence = marker
        .anchor_sequence
        .checked_add(1)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let witness = units
        .iter()
        .find(|unit| unit.slot.frame_sequence == witness_sequence)
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let expected = expected_marker_frame(marker, witness_sequence)?;
    if witness.encoded_frame != expected || witness.record.is_none() {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    Ok(())
}

fn validate_log_opened(
    selected: &ParsedGeneration<'_>,
    predecessor: &ParsedGeneration<'_>,
    anchor: &SealEvidence,
    evidence: &[SealEvidence],
) -> Result<(), ReplayViolation> {
    let predecessor_units =
        evidence
            .get(predecessor.evidence_range.clone())
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: predecessor.bytes.generation,
            })?;
    let terminal = predecessor_units
        .last()
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    let Some(LedgerRecord::LogOpened {
        store_uuid,
        generation,
        snapshot_generation,
        predecessor_log_generation,
        predecessor_terminal_acknowledged_sequence,
        snapshot_base_sequence,
        snapshot_file_length,
        snapshot_file_crc32,
        predecessor_prefix_crc32,
        validated_prefix_length,
        unacknowledged_suffix_length,
        unacknowledged_suffix_crc32,
        open_reason,
        predecessor_acknowledgement_epoch,
        ..
    }) = anchor.record.as_ref()
    else {
        return Err(ReplayViolation::GenerationBindingMismatch);
    };
    let prefix_length =
        usize::try_from(*validated_prefix_length).map_err(|_| ReplayViolation::GenerationBindingMismatch)?;
    let suffix_length =
        usize::try_from(*unacknowledged_suffix_length).map_err(|_| ReplayViolation::GenerationBindingMismatch)?;
    let prefix = predecessor
        .bytes
        .log
        .get(..prefix_length)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let suffix_end = prefix_length
        .checked_add(suffix_length)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let suffix = predecessor
        .bytes
        .log
        .get(prefix_length..suffix_end)
        .ok_or(ReplayViolation::GenerationBindingMismatch)?;
    let expected_mode = match open_reason {
        OpenReason::Compaction => SnapshotMode::OrdinaryCompaction,
        OpenReason::TailRepair => SnapshotMode::TailRepair,
    };
    let suffix_shape_is_valid = match open_reason {
        OpenReason::Compaction => *unacknowledged_suffix_length == 0 && *unacknowledged_suffix_crc32 == 0,
        OpenReason::TailRepair => validate_unacknowledged_suffix_length(suffix_length).is_ok(),
    };
    if *store_uuid != selected.snapshot.store_uuid
        || predecessor.bytes.generation.checked_add(1) != Some(*generation)
        || *generation != selected.bytes.generation
        || *snapshot_generation != selected.snapshot.generation
        || *predecessor_log_generation != predecessor.bytes.generation
        || selected.snapshot.predecessor_log_generation != predecessor.bytes.generation
        || *predecessor_terminal_acknowledged_sequence != terminal.slot.frame_sequence
        || *snapshot_base_sequence != selected.snapshot.base_sequence
        || *snapshot_base_sequence != terminal.slot.frame_sequence
        || *snapshot_file_length != selected.bytes.snapshot.len() as u64
        || *snapshot_file_crc32 != crc32(selected.bytes.snapshot)
        || *validated_prefix_length != terminal.sealed_log_length
        || *predecessor_prefix_crc32 != crc32(prefix)
        || *predecessor_acknowledgement_epoch != terminal.slot.acknowledgement_epoch
        || *unacknowledged_suffix_crc32 != crc32(suffix)
        || suffix_end != predecessor.bytes.log.len()
        || selected.snapshot.mode != expected_mode
        || !suffix_shape_is_valid
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    Ok(())
}
