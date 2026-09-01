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

use super::super::super::codec::crc32;
use super::super::super::codec::encode_ledger_frame;
use super::super::super::codec::LedgerRecord;
use super::super::super::codec::OpenReason;
use super::super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH;
use super::super::super::sidecar::decode_snapshot;
use super::super::super::sidecar::decode_store_meta;
use super::super::super::sidecar::encode_snapshot;
use super::super::super::sidecar::encode_store_meta;
use super::super::super::sidecar::LifecycleSnapshot;
use super::super::super::sidecar::SnapshotMode;
use super::super::super::sidecar::StoreMeta;
use super::super::proof::BootstrapInventoryEvidence;
use super::super::proof::CanonicalStoreMetaEvidence;
use super::super::proof::GenerationSwitchCommonEvidence;
use super::super::proof::GenerationSwitchFoundationEvidence;
use super::super::types::BootstrapPlanViolation;
use super::super::types::PlannedSnapshot;

pub(super) fn validate_canonical_store_meta(
    evidence: &CanonicalStoreMetaEvidence,
) -> Result<&StoreMeta, BootstrapPlanViolation> {
    let decoded = decode_store_meta(&evidence.canonical_bytes)?;
    let stored_crc32 = u32::from_le_bytes([
        evidence.canonical_bytes[60],
        evidence.canonical_bytes[61],
        evidence.canonical_bytes[62],
        evidence.canonical_bytes[63],
    ]);
    if decoded != evidence.meta
        || encode_store_meta(&decoded)? != evidence.canonical_bytes
        || stored_crc32 != evidence.stored_crc32
    {
        return Err(BootstrapPlanViolation::FoundationStoreMetaMismatch);
    }
    Ok(&evidence.meta)
}

pub(super) fn validate_initial_inventory(
    inventory: &BootstrapInventoryEvidence,
    meta: &StoreMeta,
) -> Result<(), BootstrapPlanViolation> {
    if inventory.store_uuid != meta.store_uuid || inventory.snapshot.store_uuid != meta.store_uuid {
        return Err(BootstrapPlanViolation::FoundationIdentityMismatch);
    }
    validate_canonical_snapshot(
        &inventory.snapshot,
        &inventory.canonical_snapshot,
        inventory.snapshot_crc32,
    )?;
    if inventory.snapshot.mode != SnapshotMode::BootstrapInventory
        || inventory.snapshot.generation != 0
        || inventory.snapshot.log_generation != 0
        || inventory.snapshot.predecessor_log_generation != u64::MAX
        || inventory.snapshot.base_sequence != 1
    {
        return Err(BootstrapPlanViolation::InvalidSnapshot {
            reason: "initial bootstrap requires the generation-0 inventory at base sequence 1",
        });
    }
    let inventory_count =
        u64::try_from(inventory.snapshot.entries.len()).map_err(|_| BootstrapPlanViolation::ArithmeticOverflow {
            field: "bootstrap inventory count",
        })?;
    if inventory.inventory_count != inventory_count
        || inventory.create_high_water != inventory.snapshot.create_high_water
        || inventory.ticket_high_water != inventory.snapshot.ticket_high_water
    {
        return Err(BootstrapPlanViolation::InvalidSnapshot {
            reason: "inventory proof metadata differs from the canonical snapshot",
        });
    }
    Ok(())
}

pub(super) fn validate_generation_foundation(
    foundation: &GenerationSwitchFoundationEvidence,
) -> Result<(), BootstrapPlanViolation> {
    let common = foundation.common();
    let meta = validate_canonical_store_meta(&common.store_meta)?;
    validate_canonical_snapshot(&common.snapshot, &common.canonical_snapshot, common.snapshot_crc32)?;
    if common.snapshot.store_uuid != meta.store_uuid {
        return Err(BootstrapPlanViolation::FoundationIdentityMismatch);
    }
    if common.snapshot.mode == SnapshotMode::BootstrapInventory || common.snapshot.generation == 0 {
        return Err(BootstrapPlanViolation::InvalidSnapshot {
            reason: "generation switch requires a non-bootstrap snapshot generation",
        });
    }
    if common.predecessor_generation != common.snapshot.predecessor_log_generation {
        return Err(invalid_switch("predecessor generation differs from the snapshot"));
    }
    if common.predecessor_terminal_sequence != common.snapshot.base_sequence {
        return Err(invalid_switch(
            "predecessor terminal sequence differs from snapshot base",
        ));
    }
    if common.predecessor_acknowledgement_epoch != common.predecessor_terminal_sequence {
        return Err(invalid_switch("predecessor acknowledgement epoch is not continuous"));
    }
    if common.predecessor_sealed_prefix_length == 0 {
        return Err(invalid_switch("validated predecessor prefix length is zero"));
    }
    let expected_marker_epoch = common
        .snapshot
        .generation
        .checked_add(1)
        .ok_or(BootstrapPlanViolation::ArithmeticOverflow { field: "marker epoch" })?;
    if common.marker_epoch != expected_marker_epoch {
        return Err(BootstrapPlanViolation::MarkerEpochMismatch {
            generation: common.snapshot.generation,
            expected: expected_marker_epoch,
            actual: common.marker_epoch,
        });
    }

    let (open_reason, suffix_length, suffix_crc32) = match foundation {
        GenerationSwitchFoundationEvidence::Compaction(evidence) => {
            if common.snapshot.mode != SnapshotMode::OrdinaryCompaction {
                return Err(invalid_switch("compaction proof has a non-compaction snapshot"));
            }
            let expected = LedgerRecord::GenerationPrepared {
                store_uuid: meta.store_uuid,
                source_generation: common.predecessor_generation,
                target_generation: common.snapshot.generation,
                target_snapshot_generation: common.snapshot.generation,
                open_reason: OpenReason::Compaction,
            };
            if evidence.prepared.record != expected
                || evidence.prepared.sequence != common.predecessor_terminal_sequence
                || evidence.prepared.acknowledgement_epoch != common.predecessor_acknowledgement_epoch
                || evidence.prepared.sealed_log_length != common.predecessor_sealed_prefix_length
            {
                return Err(invalid_switch("GenerationPrepared proof is not authoritative"));
            }
            (OpenReason::Compaction, 0, 0)
        }
        GenerationSwitchFoundationEvidence::TailRepair(evidence) => {
            if common.snapshot.mode != SnapshotMode::TailRepair {
                return Err(invalid_switch("tail-repair proof has a non-tail-repair snapshot"));
            }
            if evidence.tail.suffix_offset != common.predecessor_sealed_prefix_length {
                return Err(invalid_switch("tail suffix does not begin at the sealed prefix"));
            }
            if evidence.tail.suffix.is_empty()
                || evidence.tail.suffix.len() >= MAX_SEALED_RECORD_UNIT_LENGTH
                || crc32(&evidence.tail.suffix) != evidence.tail.suffix_crc32
            {
                return Err(invalid_switch("tail evidence length or checksum is invalid"));
            }
            let suffix_length = u32::try_from(evidence.tail.suffix.len())
                .map_err(|_| invalid_switch("tail suffix length exceeds u32"))?;
            (OpenReason::TailRepair, suffix_length, evidence.tail.suffix_crc32)
        }
    };

    validate_published_log_opened(common, meta, open_reason, suffix_length, suffix_crc32)
}

fn validate_published_log_opened(
    common: &GenerationSwitchCommonEvidence,
    meta: &StoreMeta,
    open_reason: OpenReason,
    suffix_length: u32,
    suffix_crc32: u32,
) -> Result<(), BootstrapPlanViolation> {
    let snapshot_file_length =
        u64::try_from(common.canonical_snapshot.len()).map_err(|_| BootstrapPlanViolation::ArithmeticOverflow {
            field: "snapshot file length",
        })?;
    let expected_log_opened = LedgerRecord::LogOpened {
        store_uuid: meta.store_uuid,
        generation: common.snapshot.generation,
        snapshot_generation: common.snapshot.generation,
        predecessor_log_generation: common.predecessor_generation,
        predecessor_terminal_acknowledged_sequence: common.predecessor_terminal_sequence,
        snapshot_base_sequence: common.snapshot.base_sequence,
        snapshot_file_length,
        snapshot_file_crc32: common.snapshot_crc32,
        predecessor_prefix_crc32: common.predecessor_prefix_crc32,
        validated_prefix_length: common.predecessor_sealed_prefix_length,
        unacknowledged_suffix_length: suffix_length,
        unacknowledged_suffix_crc32: suffix_crc32,
        open_reason,
        predecessor_acknowledgement_epoch: common.predecessor_acknowledgement_epoch,
    };
    let anchor_sequence =
        common
            .predecessor_terminal_sequence
            .checked_add(1)
            .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                field: "LogOpened sequence",
            })?;
    let expected_frame = encode_ledger_frame(&expected_log_opened, anchor_sequence, common.snapshot.generation)?;
    if common.log_opened_record != expected_log_opened
        || common.canonical_log_opened_frame != expected_frame
        || common.log_opened_frame_crc32 != crc32(&common.canonical_log_opened_frame)
    {
        return Err(invalid_switch("published LogOpened proof is not exact and canonical"));
    }
    Ok(())
}

fn validate_canonical_snapshot(
    snapshot: &LifecycleSnapshot,
    canonical: &[u8],
    expected_crc32: u32,
) -> Result<(), BootstrapPlanViolation> {
    let decoded = decode_snapshot(canonical)?;
    if decoded != *snapshot || encode_snapshot(&decoded)? != canonical || crc32(canonical) != expected_crc32 {
        return Err(BootstrapPlanViolation::InvalidSnapshot {
            reason: "snapshot proof is not exact and canonical",
        });
    }
    Ok(())
}

pub(super) const fn invalid_switch(reason: &'static str) -> BootstrapPlanViolation {
    BootstrapPlanViolation::InvalidGenerationSwitch { reason }
}

pub(super) fn planned_inventory_snapshot(
    inventory: BootstrapInventoryEvidence,
) -> Result<PlannedSnapshot, BootstrapPlanViolation> {
    Ok(PlannedSnapshot {
        encoded: inventory.canonical_snapshot,
        file_crc32: inventory.snapshot_crc32,
        inventory_count: inventory.inventory_count,
        create_high_water: inventory.create_high_water,
        ticket_high_water: inventory.ticket_high_water,
    })
}

pub(super) fn planned_generation_snapshot(
    common: &GenerationSwitchCommonEvidence,
) -> Result<PlannedSnapshot, BootstrapPlanViolation> {
    Ok(PlannedSnapshot {
        encoded: common.canonical_snapshot.clone(),
        file_crc32: common.snapshot_crc32,
        inventory_count: u64::try_from(common.snapshot.entries.len()).map_err(|_| {
            BootstrapPlanViolation::ArithmeticOverflow {
                field: "snapshot inventory count",
            }
        })?,
        create_high_water: common.snapshot.create_high_water,
        ticket_high_water: common.snapshot.ticket_high_water,
    })
}
