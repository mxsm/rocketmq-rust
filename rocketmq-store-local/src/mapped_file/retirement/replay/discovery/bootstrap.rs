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

use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::decode_acknowledgement_file;
use crate::mapped_file::retirement::codec::encode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::encode_commit_seal;
use crate::mapped_file::retirement::codec::encode_ledger_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlot;
use crate::mapped_file::retirement::codec::CommitSeal;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_FILE_LENGTH;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use crate::mapped_file::retirement::codec::COMMIT_SEAL_LENGTH;
use crate::mapped_file::retirement::sidecar::decode_snapshot;
use crate::mapped_file::retirement::sidecar::encode_snapshot;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::sidecar::StoreMeta;

use super::super::parsing;
use super::super::GenerationBytes;
use super::super::ReplayLimits;
use super::super::SealEvidence;
use super::corruption;
use super::map_codec_error;
use super::map_replay_error;
use super::map_sidecar_error;
use super::ManagedLifecycleReadFailure;
use super::ManagedLifecycleReadLimits;
use super::OwnedGeneration;

struct PlannedBootstrapUnit {
    frame_end: usize,
    sealed_end: usize,
    slot_index: usize,
    encoded_slot: [u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    bytes: Vec<u8>,
}

fn planned_bootstrap_unit(
    meta: &StoreMeta,
    record: &LedgerRecord,
    sequence: u64,
    acknowledgement_epoch: u64,
    unit_start: usize,
) -> Result<PlannedBootstrapUnit, ManagedLifecycleReadFailure> {
    let frame = encode_ledger_frame(record, sequence, 0).map_err(map_codec_error)?;
    let frame_end = unit_start
        .checked_add(frame.len())
        .ok_or_else(|| corruption("bootstrap frame offset overflow"))?;
    let frame_end_offset = u64::try_from(frame_end).map_err(|_| corruption("bootstrap frame offset overflow"))?;
    let slot_index = usize::try_from(
        acknowledgement_epoch
            .checked_sub(1)
            .ok_or_else(|| corruption("zero bootstrap acknowledgement epoch"))?
            & 1,
    )
    .map_err(|_| corruption("bootstrap acknowledgement slot overflow"))?;
    let slot_index_u8 = u8::try_from(slot_index).map_err(|_| corruption("bootstrap slot index overflow"))?;
    let slot = AcknowledgementSlot {
        slot_index: slot_index_u8,
        activated: false,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        acknowledgement_epoch,
        marker_epoch: 0,
        log_generation: 0,
        frame_sequence: sequence,
        frame_end_offset,
        frame_crc32: crc32(&frame),
    };
    let encoded_slot = encode_acknowledgement_slot(&slot).map_err(map_codec_error)?;
    let seal = CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot).map_err(map_codec_error)?;
    let encoded_seal = encode_commit_seal(&seal).map_err(map_codec_error)?;
    let sealed_end = frame_end
        .checked_add(COMMIT_SEAL_LENGTH)
        .ok_or_else(|| corruption("bootstrap seal offset overflow"))?;
    let mut bytes = frame;
    bytes.extend_from_slice(&encoded_seal);
    Ok(PlannedBootstrapUnit {
        frame_end,
        sealed_end,
        slot_index,
        encoded_slot,
        bytes,
    })
}

fn bootstrap_acknowledgement_matches(
    raw: &[u8],
    expected: &[u8; ACKNOWLEDGEMENT_FILE_LENGTH],
    reconstructable: [bool; 2],
    seal_evidence: &[SealEvidence],
) -> bool {
    if raw.len() != ACKNOWLEDGEMENT_FILE_LENGTH {
        return false;
    }
    for (physical_index, can_reconstruct) in reconstructable.into_iter().enumerate() {
        let start = physical_index * ACKNOWLEDGEMENT_SLOT_LENGTH;
        let end = start + ACKNOWLEDGEMENT_SLOT_LENGTH;
        let raw_slot = &raw[start..end];
        let expected_slot = &expected[start..end];
        if expected_slot.iter().all(|byte| *byte == 0) {
            if !raw_slot.iter().all(|byte| *byte == 0) {
                return false;
            }
        } else if raw_slot != expected_slot && (raw_slot.iter().all(|byte| *byte == 0) || !can_reconstruct) {
            return false;
        }
    }
    let Some(expected_authoritative) = decode_acknowledgement_file(expected)
        .ok()
        .and_then(|file| file.authoritative().cloned())
    else {
        return raw.iter().all(|byte| *byte == 0);
    };
    super::super::acknowledgement::resolve_acknowledgement(
        [&raw[..ACKNOWLEDGEMENT_SLOT_LENGTH], &raw[ACKNOWLEDGEMENT_SLOT_LENGTH..]],
        seal_evidence,
    )
    .is_ok_and(|resolved| resolved.authoritative == expected_authoritative)
}

fn validate_bootstrap_acknowledgement(
    raw: &[u8],
    log_length: usize,
    initialized: &PlannedBootstrapUnit,
    installed: Option<&PlannedBootstrapUnit>,
    seal_evidence: &[SealEvidence],
) -> Result<(), ManagedLifecycleReadFailure> {
    let zero = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    let mut after_initialized = zero;
    let initialized_start = initialized.slot_index * ACKNOWLEDGEMENT_SLOT_LENGTH;
    after_initialized[initialized_start..initialized_start + ACKNOWLEDGEMENT_SLOT_LENGTH]
        .copy_from_slice(&initialized.encoded_slot);
    let mut after_installed = after_initialized;
    if let Some(installed) = installed {
        let installed_start = installed.slot_index * ACKNOWLEDGEMENT_SLOT_LENGTH;
        after_installed[installed_start..installed_start + ACKNOWLEDGEMENT_SLOT_LENGTH]
            .copy_from_slice(&installed.encoded_slot);
    }
    let reconstructable = [
        log_length >= initialized.sealed_end,
        installed.is_some_and(|unit| log_length >= unit.sealed_end),
    ];
    let mut candidates = [&zero, &zero];
    let candidate_count = if log_length < initialized.frame_end {
        1
    } else if log_length == initialized.frame_end {
        candidates[1] = &after_initialized;
        2
    } else if log_length < initialized.sealed_end || installed.is_none() {
        candidates[0] = &after_initialized;
        1
    } else if let Some(installed) = installed {
        if log_length < installed.frame_end {
            candidates[0] = &after_initialized;
            1
        } else if log_length == installed.frame_end {
            candidates[0] = &after_initialized;
            candidates[1] = &after_installed;
            2
        } else {
            candidates[0] = &after_installed;
            1
        }
    } else {
        return Err(corruption("bootstrap log extends beyond StoreInitialized"));
    };
    if candidates[..candidate_count]
        .iter()
        .any(|expected| bootstrap_acknowledgement_matches(raw, expected, reconstructable, seal_evidence))
    {
        Ok(())
    } else {
        Err(corruption(
            "preactivation acknowledgement slots do not match the exact bootstrap frontier",
        ))
    }
}

pub(super) fn validate_marker_absent_bootstrap(
    meta: &StoreMeta,
    acknowledgement: Option<&[u8]>,
    generations: &[OwnedGeneration],
    bootstrap_log: Option<&[u8]>,
    limits: ManagedLifecycleReadLimits,
) -> Result<(), ManagedLifecycleReadFailure> {
    if generations.len() > 1 || (!generations.is_empty() && bootstrap_log.is_some()) {
        return Err(corruption(
            "preactivation bootstrap has multiple generation-0 representations",
        ));
    }
    let generation = generations.first();
    if generation.is_some_and(|generation| generation.generation != 0) {
        return Err(corruption("preactivation bootstrap contains a nonzero generation"));
    }
    let log = generation.map_or(bootstrap_log, |generation| Some(generation.log.as_slice()));
    let Some(acknowledgement) = acknowledgement else {
        if log.is_some() || generation.is_some() {
            return Err(corruption("generation-0 artifacts exist before ACKNOWLEDGED.v1"));
        }
        return Ok(());
    };

    let initialized_record = LedgerRecord::StoreInitialized {
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        creation_time_ns: meta.creation_time_ns,
    };
    let initialized = planned_bootstrap_unit(meta, &initialized_record, 1, 1, 0)?;
    let snapshot = generation
        .map(|generation| decode_snapshot(&generation.snapshot).map_err(map_sidecar_error))
        .transpose()?;
    if let Some(snapshot) = &snapshot {
        if snapshot.mode != SnapshotMode::BootstrapInventory
            || snapshot.store_uuid != meta.store_uuid
            || snapshot.generation != 0
            || snapshot.log_generation != 0
            || snapshot.predecessor_log_generation != u64::MAX
            || snapshot.base_sequence != 1
        {
            return Err(corruption("generation-0 snapshot is not the exact bootstrap inventory"));
        }
    }
    let installed = snapshot
        .as_ref()
        .map(|snapshot| {
            let snapshot_bytes = generation
                .map(|generation| generation.snapshot.as_slice())
                .ok_or_else(|| corruption("bootstrap snapshot has no generation file"))?;
            let inventory_count = u64::try_from(snapshot.entries.len())
                .map_err(|_| corruption("bootstrap inventory count does not fit u64"))?;
            let snapshot_file_length = u64::try_from(snapshot_bytes.len())
                .map_err(|_| corruption("bootstrap snapshot length does not fit u64"))?;
            let record = LedgerRecord::BootstrapInstalled {
                store_uuid: meta.store_uuid,
                bootstrap_id: meta.bootstrap_id,
                snapshot_generation: 0,
                snapshot_base_sequence: 1,
                snapshot_file_length,
                snapshot_file_crc32: crc32(snapshot_bytes),
                inventory_count,
                create_high_water: snapshot.create_high_water,
                ticket_high_water: snapshot.ticket_high_water,
            };
            planned_bootstrap_unit(meta, &record, 2, 2, initialized.sealed_end)
        })
        .transpose()?;

    let actual_log = log.unwrap_or_default();
    let mut expected_log = initialized.bytes.clone();
    if let Some(installed) = &installed {
        if actual_log.len() < initialized.sealed_end {
            return Err(corruption(
                "bootstrap snapshot exists before StoreInitialized is acknowledged and sealed",
            ));
        }
        expected_log.extend_from_slice(&installed.bytes);
    }
    if !expected_log.starts_with(actual_log) {
        return Err(corruption(
            "generation-0 log is not an exact prefix of the deterministic preactivation bootstrap",
        ));
    }

    let synthetic_snapshot;
    let snapshot_bytes = if let Some(generation) = generation {
        generation.snapshot.as_slice()
    } else {
        synthetic_snapshot = encode_snapshot(&LifecycleSnapshot {
            mode: SnapshotMode::BootstrapInventory,
            store_uuid: meta.store_uuid,
            generation: 0,
            log_generation: 0,
            predecessor_log_generation: u64::MAX,
            base_sequence: 1,
            create_high_water: 0,
            ticket_high_water: 0,
            entries: Vec::new(),
        })
        .map_err(map_sidecar_error)?;
        synthetic_snapshot.as_slice()
    };
    let mut seal_evidence = Vec::new();
    parsing::parse_generation(
        GenerationBytes {
            generation: 0,
            snapshot: snapshot_bytes,
            log: actual_log,
        },
        meta,
        ReplayLimits {
            max_generations: limits.max_generations,
            max_sealed_units: limits.max_sealed_units,
        },
        &mut seal_evidence,
    )
    .map_err(map_replay_error)?;
    validate_bootstrap_acknowledgement(
        acknowledgement,
        actual_log.len(),
        &initialized,
        installed.as_ref(),
        &seal_evidence,
    )
}
