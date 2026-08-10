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

use super::refresh_payload_crc;
use super::samples::*;
use super::Fixture;
use super::FixtureValidation;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::encode_acknowledgement_file;
use crate::mapped_file::retirement::codec::encode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::encode_commit_seal;
use crate::mapped_file::retirement::codec::encode_ledger_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlot;
use crate::mapped_file::retirement::codec::AcknowledgementSlotState;
use crate::mapped_file::retirement::codec::CommitSeal;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::OpenReason;
use crate::mapped_file::retirement::codec::MAX_SEALED_RECORD_UNIT_LENGTH;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::encode_snapshot;
use crate::mapped_file::retirement::sidecar::EnabledMarkerFile;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::SnapshotMode;

pub(super) fn add_frontier_cases(fixtures: &mut Vec<Fixture>) {
    add_compaction_frontiers(fixtures);
    add_tail_repair_frontiers(fixtures);
    add_orphan_generation_pair(fixtures);
    add_acknowledged_suffix_loss(fixtures);
    add_overlong_tail_repair(fixtures);
}

fn add_compaction_frontiers(fixtures: &mut Vec<Fixture>) {
    const SOURCE_GENERATION: u64 = 2;
    const TARGET_GENERATION: u64 = 3;
    const BASE_SEQUENCE: u64 = 20;
    const PREPARED_ACK_EPOCH: u64 = 9;
    const TARGET_MARKER_EPOCH: u64 = 4;

    let prepared = LedgerRecord::GenerationPrepared {
        store_uuid: sample_store_uuid(),
        source_generation: SOURCE_GENERATION,
        target_generation: TARGET_GENERATION,
        target_snapshot_generation: TARGET_GENERATION,
        open_reason: OpenReason::Compaction,
    };
    let prepared_unit = acknowledged_unit(
        &prepared,
        BASE_SEQUENCE,
        SOURCE_GENERATION,
        PREPARED_ACK_EPOCH,
        TARGET_MARKER_EPOCH - 1,
        0,
    );
    fixtures.push(Fixture::new(
        "compaction.source-prepared.acknowledged-unit.bundle.bin",
        acknowledged_log_bundle(&[], &prepared_unit),
        FixtureValidation::AcknowledgedLogBundle {
            first_sequence: BASE_SEQUENCE,
            final_sequence: BASE_SEQUENCE,
            generation: SOURCE_GENERATION,
        },
    ));

    let snapshot = generation_snapshot(
        SnapshotMode::OrdinaryCompaction,
        TARGET_GENERATION,
        SOURCE_GENERATION,
        BASE_SEQUENCE,
    );
    let snapshot_bytes = encode_snapshot(&snapshot).expect("compaction target snapshot encodes");
    fixtures.push(Fixture::new(
        "compaction.target-snapshot.bin",
        snapshot_bytes.clone(),
        FixtureValidation::Snapshot,
    ));
    let opened = log_opened_record(
        &snapshot_bytes,
        TARGET_GENERATION,
        SOURCE_GENERATION,
        BASE_SEQUENCE,
        PREPARED_ACK_EPOCH,
        OpenReason::Compaction,
        &[],
    );
    let opened_unit = acknowledged_unit(
        &opened,
        BASE_SEQUENCE + 1,
        TARGET_GENERATION,
        PREPARED_ACK_EPOCH + 1,
        TARGET_MARKER_EPOCH,
        0,
    );
    fixtures.push(Fixture::new(
        "compaction.target-log-opened.unsealed.bin",
        opened_unit.frame.clone(),
        FixtureValidation::LedgerFrame {
            sequence: BASE_SEQUENCE + 1,
            generation: TARGET_GENERATION,
        },
    ));
    fixtures.push(Fixture::new(
        "compaction.target-log-opened.acknowledged-unit.bundle.bin",
        acknowledged_log_bundle(&[], &opened_unit),
        FixtureValidation::AcknowledgedLogBundle {
            first_sequence: BASE_SEQUENCE + 1,
            final_sequence: BASE_SEQUENCE + 1,
            generation: TARGET_GENERATION,
        },
    ));

    let marker = switched_marker(
        &snapshot_bytes,
        &opened_unit.frame,
        SOURCE_GENERATION,
        TARGET_GENERATION,
        BASE_SEQUENCE,
        TARGET_MARKER_EPOCH,
    );
    fixtures.push(Fixture::new(
        "compaction.marker-switched.bin",
        encode_enabled_marker_file(&marker).expect("compaction marker encodes"),
        FixtureValidation::MarkerFile,
    ));
    add_marker_committed_frontiers(
        fixtures,
        "compaction",
        &marker,
        &opened_unit,
        TARGET_GENERATION,
        BASE_SEQUENCE,
        PREPARED_ACK_EPOCH + 2,
        TARGET_MARKER_EPOCH,
    );
}

fn add_tail_repair_frontiers(fixtures: &mut Vec<Fixture>) {
    const SOURCE_GENERATION: u64 = 1;
    const TARGET_GENERATION: u64 = 2;
    const BASE_SEQUENCE: u64 = 20;
    const PREDECESSOR_ACK_EPOCH: u64 = 20;
    const TARGET_MARKER_EPOCH: u64 = 3;
    const SUFFIX: &[u8] = b"unacknowledged-frame-suffix";

    let snapshot = generation_snapshot(
        SnapshotMode::TailRepair,
        TARGET_GENERATION,
        SOURCE_GENERATION,
        BASE_SEQUENCE,
    );
    let snapshot_bytes = encode_snapshot(&snapshot).expect("tail-repair snapshot encodes");
    fixtures.push(Fixture::new(
        "tail-repair.target-snapshot.bin",
        snapshot_bytes.clone(),
        FixtureValidation::Snapshot,
    ));
    fixtures.push(Fixture::new(
        "tail-repair.quarantine-suffix.bin",
        SUFFIX,
        FixtureValidation::TailEvidence {
            length: SUFFIX.len(),
            crc32: crc32(SUFFIX),
        },
    ));
    let opened = log_opened_record(
        &snapshot_bytes,
        TARGET_GENERATION,
        SOURCE_GENERATION,
        BASE_SEQUENCE,
        PREDECESSOR_ACK_EPOCH,
        OpenReason::TailRepair,
        SUFFIX,
    );
    let opened_unit = acknowledged_unit(
        &opened,
        BASE_SEQUENCE + 1,
        TARGET_GENERATION,
        PREDECESSOR_ACK_EPOCH + 1,
        TARGET_MARKER_EPOCH,
        0,
    );
    fixtures.push(Fixture::new(
        "tail-repair.target-log-opened.unsealed.bin",
        opened_unit.frame.clone(),
        FixtureValidation::LedgerFrame {
            sequence: BASE_SEQUENCE + 1,
            generation: TARGET_GENERATION,
        },
    ));
    fixtures.push(Fixture::new(
        "tail-repair.target-log-opened.acknowledged-unit.bundle.bin",
        acknowledged_log_bundle(&[], &opened_unit),
        FixtureValidation::AcknowledgedLogBundle {
            first_sequence: BASE_SEQUENCE + 1,
            final_sequence: BASE_SEQUENCE + 1,
            generation: TARGET_GENERATION,
        },
    ));

    let marker = switched_marker(
        &snapshot_bytes,
        &opened_unit.frame,
        SOURCE_GENERATION,
        TARGET_GENERATION,
        BASE_SEQUENCE,
        TARGET_MARKER_EPOCH,
    );
    fixtures.push(Fixture::new(
        "tail-repair.marker-switched.bin",
        encode_enabled_marker_file(&marker).expect("tail-repair marker encodes"),
        FixtureValidation::MarkerFile,
    ));
    add_marker_committed_frontiers(
        fixtures,
        "tail-repair",
        &marker,
        &opened_unit,
        TARGET_GENERATION,
        BASE_SEQUENCE,
        PREDECESSOR_ACK_EPOCH + 2,
        TARGET_MARKER_EPOCH,
    );
}

fn add_marker_committed_frontiers(
    fixtures: &mut Vec<Fixture>,
    prefix: &str,
    marker: &EnabledMarkerFile,
    opened_unit: &AcknowledgedUnit,
    generation: u64,
    base_sequence: u64,
    acknowledgement_epoch: u64,
    marker_epoch: u64,
) {
    let selected = marker.selected_slot().expect("switched marker selects its target");
    let encoded_marker_slot = encode_enabled_marker_slot(selected).expect("selected marker slot encodes");
    let marker_committed = LedgerRecord::MarkerCommitted {
        store_uuid: sample_store_uuid(),
        marker_epoch,
        snapshot_generation: generation,
        log_generation: generation,
        anchor_sequence: base_sequence + 1,
        slot_index: selected.slot_index,
        slot_crc32: u32::from_le_bytes(encoded_marker_slot[100..104].try_into().expect("marker slot CRC")),
    };
    let opened_prefix = opened_unit.log_bytes();
    let committed_unit = acknowledged_unit(
        &marker_committed,
        base_sequence + 2,
        generation,
        acknowledgement_epoch,
        marker_epoch,
        opened_prefix.len() as u64,
    );

    let mut unsealed = opened_prefix.clone();
    unsealed.extend_from_slice(&committed_unit.frame);
    fixtures.push(Fixture::new(
        format!("{prefix}.marker-committed.unsealed.bin"),
        unsealed,
        FixtureValidation::SealedUnitsWithUnsealedFinal {
            first_sequence: base_sequence + 1,
            generation,
            sealed_units: 1,
        },
    ));
    fixtures.push(Fixture::new(
        format!("{prefix}.marker-committed.acknowledged-unit.bundle.bin"),
        acknowledged_log_bundle(&opened_prefix, &committed_unit),
        FixtureValidation::AcknowledgedLogBundle {
            first_sequence: base_sequence + 1,
            final_sequence: base_sequence + 2,
            generation,
        },
    ));
}

fn add_orphan_generation_pair(fixtures: &mut Vec<Fixture>) {
    const SOURCE_GENERATION: u64 = 3;
    const TARGET_GENERATION: u64 = 4;
    const BASE_SEQUENCE: u64 = 30;

    let snapshot = generation_snapshot(
        SnapshotMode::OrdinaryCompaction,
        TARGET_GENERATION,
        SOURCE_GENERATION,
        BASE_SEQUENCE,
    );
    let snapshot_bytes = encode_snapshot(&snapshot).expect("orphan snapshot encodes");
    let opened = log_opened_record(
        &snapshot_bytes,
        TARGET_GENERATION,
        SOURCE_GENERATION,
        BASE_SEQUENCE,
        30,
        OpenReason::Compaction,
        &[],
    );
    let log =
        encode_ledger_frame(&opened, BASE_SEQUENCE + 1, TARGET_GENERATION).expect("orphan unsealed LogOpened encodes");
    fixtures.push(Fixture::new(
        "generation.orphan-higher.snapshot.bin",
        snapshot_bytes,
        FixtureValidation::Snapshot,
    ));
    fixtures.push(Fixture::new(
        "generation.orphan-higher.log.bin",
        log,
        FixtureValidation::LedgerFrame {
            sequence: BASE_SEQUENCE + 1,
            generation: TARGET_GENERATION,
        },
    ));
}

fn add_acknowledged_suffix_loss(fixtures: &mut Vec<Fixture>) {
    const GENERATION: u64 = 3;
    const FIRST_SEQUENCE: u64 = 100;

    let first_record = completed_record(99, 66);
    let first = acknowledged_unit(&first_record, FIRST_SEQUENCE, GENERATION, 41, 4, 0);
    let retained_log = first.log_bytes();
    let missing_frame = encode_ledger_frame(&completed_record(99, 67), FIRST_SEQUENCE + 1, GENERATION)
        .expect("missing acknowledged frame encodes");
    let missing_slot = AcknowledgementSlot {
        slot_index: 1,
        activated: true,
        store_uuid: sample_store_uuid(),
        bootstrap_id: bootstrap_id(),
        acknowledgement_epoch: 42,
        marker_epoch: 4,
        log_generation: GENERATION,
        frame_sequence: FIRST_SEQUENCE + 1,
        frame_end_offset: retained_log.len() as u64 + missing_frame.len() as u64,
        frame_crc32: crc32(&missing_frame),
    };
    let history = encode_acknowledgement_file(&[
        AcknowledgementSlotState::Populated(first.slot.clone()),
        AcknowledgementSlotState::Populated(missing_slot),
    ])
    .expect("suffix-loss acknowledgement history encodes");
    let mut bundle = Vec::with_capacity(4 + retained_log.len() + history.len());
    bundle.extend_from_slice(&(retained_log.len() as u32).to_le_bytes());
    bundle.extend_from_slice(&retained_log);
    bundle.extend_from_slice(&history);
    fixtures.push(Fixture::new(
        "invalid.acknowledged-complete-suffix-loss.bundle.bin",
        bundle,
        FixtureValidation::AcknowledgedSuffixLossBundle {
            first_sequence: FIRST_SEQUENCE,
            generation: GENERATION,
        },
    ));
}

fn add_overlong_tail_repair(fixtures: &mut Vec<Fixture>) {
    let snapshot = generation_snapshot(SnapshotMode::TailRepair, 2, 1, 20);
    let snapshot_bytes = encode_snapshot(&snapshot).expect("tail-repair snapshot encodes");
    let record = log_opened_record(&snapshot_bytes, 2, 1, 20, 20, OpenReason::TailRepair, b"tail");
    let mut frame = encode_ledger_frame(&record, 21, 2).expect("valid tail-repair frame encodes");
    let suffix_length_offset = 40 + 80;
    frame[suffix_length_offset..suffix_length_offset + 4]
        .copy_from_slice(&(MAX_SEALED_RECORD_UNIT_LENGTH as u32).to_le_bytes());
    refresh_payload_crc(&mut frame);
    fixtures.push(Fixture::new(
        "invalid.log-opened-overlong-suffix.bin",
        frame,
        FixtureValidation::InvalidTypedLedgerFrame {
            sequence: 21,
            generation: 2,
        },
    ));
}

fn generation_snapshot(
    mode: SnapshotMode,
    generation: u64,
    predecessor_log_generation: u64,
    base_sequence: u64,
) -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode,
        store_uuid: sample_store_uuid(),
        generation,
        log_generation: generation,
        predecessor_log_generation,
        base_sequence,
        create_high_water: 7,
        ticket_high_water: 42,
        entries: Vec::new(),
    }
}

fn log_opened_record(
    snapshot_bytes: &[u8],
    generation: u64,
    predecessor_generation: u64,
    base_sequence: u64,
    predecessor_acknowledgement_epoch: u64,
    open_reason: OpenReason,
    unacknowledged_suffix: &[u8],
) -> LedgerRecord {
    LedgerRecord::LogOpened {
        store_uuid: sample_store_uuid(),
        generation,
        snapshot_generation: generation,
        predecessor_log_generation: predecessor_generation,
        predecessor_terminal_acknowledged_sequence: base_sequence,
        snapshot_base_sequence: base_sequence,
        snapshot_file_length: snapshot_bytes.len() as u64,
        snapshot_file_crc32: crc32(snapshot_bytes),
        predecessor_prefix_crc32: crc32(b"acknowledged-predecessor-prefix"),
        validated_prefix_length: 1_024,
        unacknowledged_suffix_length: unacknowledged_suffix.len() as u32,
        unacknowledged_suffix_crc32: crc32(unacknowledged_suffix),
        open_reason,
        predecessor_acknowledgement_epoch,
    }
}

fn switched_marker(
    snapshot_bytes: &[u8],
    opened_frame: &[u8],
    source_generation: u64,
    target_generation: u64,
    base_sequence: u64,
    marker_epoch: u64,
) -> EnabledMarkerFile {
    let target_index = ((marker_epoch - 1) & 1) as u8;
    let source_index = 1 - target_index;
    let source = marker_slot(
        source_index,
        marker_epoch - 1,
        source_generation,
        base_sequence,
        108,
        0x1111_1111,
        0x2222_2222,
    );
    let target = marker_slot(
        target_index,
        marker_epoch,
        target_generation,
        base_sequence + 1,
        snapshot_bytes.len() as u64,
        crc32(snapshot_bytes),
        crc32(opened_frame),
    );
    if target_index == 0 {
        EnabledMarkerFile {
            slots: [Some(target), Some(source)],
        }
    } else {
        EnabledMarkerFile {
            slots: [Some(source), Some(target)],
        }
    }
}

fn completed_record(namespace_absent_sequence: u64, completion_time_ns: u64) -> LedgerRecord {
    LedgerRecord::Completed {
        ticket_id: ticket(),
        incarnation: incarnation(),
        completion_time_ns,
        namespace_absent_sequence,
    }
}

struct AcknowledgedUnit {
    frame: Vec<u8>,
    slot: AcknowledgementSlot,
    encoded_slot: [u8; 104],
    encoded_seal: [u8; 72],
}

impl AcknowledgedUnit {
    fn log_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.frame.len() + self.encoded_seal.len());
        bytes.extend_from_slice(&self.frame);
        bytes.extend_from_slice(&self.encoded_seal);
        bytes
    }
}

fn acknowledged_unit(
    record: &LedgerRecord,
    sequence: u64,
    generation: u64,
    acknowledgement_epoch: u64,
    marker_epoch: u64,
    frame_offset: u64,
) -> AcknowledgedUnit {
    let frame = encode_ledger_frame(record, sequence, generation).expect("frontier frame encodes");
    let slot = AcknowledgementSlot {
        slot_index: ((acknowledgement_epoch - 1) & 1) as u8,
        activated: true,
        store_uuid: sample_store_uuid(),
        bootstrap_id: bootstrap_id(),
        acknowledgement_epoch,
        marker_epoch,
        log_generation: generation,
        frame_sequence: sequence,
        frame_end_offset: frame_offset + frame.len() as u64,
        frame_crc32: crc32(&frame),
    };
    let encoded_slot = encode_acknowledgement_slot(&slot).expect("frontier acknowledgement slot encodes");
    let seal =
        CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot).expect("frontier seal derives from the exact slot");
    let encoded_seal = encode_commit_seal(&seal).expect("frontier seal encodes");
    AcknowledgedUnit {
        frame,
        slot,
        encoded_slot,
        encoded_seal,
    }
}

fn acknowledged_log_bundle(prefix: &[u8], final_unit: &AcknowledgedUnit) -> Vec<u8> {
    let mut bundle = Vec::with_capacity(
        8 + prefix.len() + final_unit.frame.len() + final_unit.encoded_seal.len() + final_unit.encoded_slot.len(),
    );
    bundle.extend_from_slice(&(prefix.len() as u32).to_le_bytes());
    bundle.extend_from_slice(&(final_unit.frame.len() as u32).to_le_bytes());
    bundle.extend_from_slice(prefix);
    bundle.extend_from_slice(&final_unit.frame);
    bundle.extend_from_slice(&final_unit.encoded_seal);
    bundle.extend_from_slice(&final_unit.encoded_slot);
    bundle
}
