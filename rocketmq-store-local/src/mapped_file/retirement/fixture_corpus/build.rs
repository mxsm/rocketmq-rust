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
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::encode_snapshot;
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::EnabledMarkerFile;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotMode;

mod edges;
mod frontiers;
mod samples;

use edges::add_edge_cases;
use frontiers::add_frontier_cases;
use samples::*;

pub(super) fn corpus() -> Vec<Fixture> {
    let mut fixtures = Vec::new();
    add_fixed_sidecars(&mut fixtures);
    add_ledger_records(&mut fixtures);
    add_acknowledged_unit(&mut fixtures);
    add_retirement_chains(&mut fixtures);
    add_corruption_cases(&mut fixtures);
    add_edge_cases(&mut fixtures);
    add_frontier_cases(&mut fixtures);
    fixtures.sort_by(|left, right| left.name.cmp(&right.name));
    fixtures
}

fn add_fixed_sidecars(fixtures: &mut Vec<Fixture>) {
    let meta = sample_store_meta();
    fixtures.push(Fixture::new(
        "store.meta.bin",
        encode_store_meta(&meta).expect("sample store metadata encodes"),
        FixtureValidation::StoreMeta,
    ));

    let first = marker_slot(0, 1, 0, 2, 108, 0x1122_3344, 0x5566_7788);
    let second = marker_slot(1, 2, 1, 10, 200, 0x99aa_bbcc, 0xddee_ff00);
    fixtures.push(Fixture::new(
        "enabled.slot0.bin",
        encode_enabled_marker_slot(&first).expect("slot zero encodes"),
        FixtureValidation::MarkerSlot { physical_slot: 0 },
    ));
    fixtures.push(Fixture::new(
        "enabled.slot1.bin",
        encode_enabled_marker_slot(&second).expect("slot one encodes"),
        FixtureValidation::MarkerSlot { physical_slot: 1 },
    ));
    fixtures.push(Fixture::new(
        "enabled.slot0-only.bin",
        encode_enabled_marker_file(&EnabledMarkerFile {
            slots: [Some(first.clone()), None],
        })
        .expect("initial marker file encodes"),
        FixtureValidation::MarkerFile,
    ));
    let marker_file = encode_enabled_marker_file(&EnabledMarkerFile {
        slots: [Some(first), Some(second)],
    })
    .expect("two-slot marker file encodes");
    fixtures.push(Fixture::new(
        "enabled.both-slots.bin",
        marker_file,
        FixtureValidation::MarkerFile,
    ));

    for (name, snapshot) in [
        (
            "snapshot.bootstrap.bin",
            snapshot(SnapshotMode::BootstrapInventory, Vec::new()),
        ),
        (
            "snapshot.compacted.bin",
            snapshot(
                SnapshotMode::OrdinaryCompaction,
                vec![
                    SnapshotEntry::Incarnation(sample_incarnation_entry()),
                    SnapshotEntry::RetirementTicket(sample_retirement_entry(RetirementStage::NamespaceAbsent)),
                    SnapshotEntry::Quarantine(sample_quarantine_entry()),
                ],
            ),
        ),
        (
            "snapshot.tail-repair.bin",
            snapshot(
                SnapshotMode::TailRepair,
                vec![SnapshotEntry::Incarnation(sample_incarnation_entry())],
            ),
        ),
        (
            "snapshot.completed-retained.bin",
            snapshot(
                SnapshotMode::OrdinaryCompaction,
                vec![
                    SnapshotEntry::Incarnation(sample_incarnation_entry()),
                    SnapshotEntry::RetirementTicket(sample_retirement_entry(RetirementStage::CompletedRetained)),
                ],
            ),
        ),
        (
            "snapshot.completed-omitted-after-clean-start.bin",
            snapshot(SnapshotMode::OrdinaryCompaction, Vec::new()),
        ),
    ] {
        fixtures.push(Fixture::new(
            name,
            encode_snapshot(&snapshot).expect("sample snapshot encodes"),
            FixtureValidation::Snapshot,
        ));
    }
}

fn add_ledger_records(fixtures: &mut Vec<Fixture>) {
    for (name, record, sequence, generation) in sample_records() {
        fixtures.push(Fixture::new(
            format!("record.{name}.bin"),
            encode_ledger_frame(&record, sequence, generation).expect("sample record encodes"),
            FixtureValidation::LedgerFrame { sequence, generation },
        ));
    }

    let maximum_path = maximum_path();
    let record = LedgerRecord::Quarantined {
        entity_kind: QuarantineEntityKind::Canonical,
        reason: QuarantineReason::MalformedName,
        sequence_at_observation: 99,
        physical_key: None,
        content_fingerprint: None,
        source_path: maximum_path,
        destination_path: None,
    };
    fixtures.push(Fixture::new(
        "record.maximum-path.bin",
        encode_ledger_frame(&record, 100, 3).expect("maximum path record encodes"),
        FixtureValidation::LedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
}

fn add_acknowledged_unit(fixtures: &mut Vec<Fixture>) {
    let record = LedgerRecord::Completed {
        ticket_id: ticket(),
        incarnation: incarnation(),
        completion_time_ns: 0x0102_0304_0506_0708,
        namespace_absent_sequence: 9,
    };
    let frame = encode_ledger_frame(&record, 100, 2).expect("worked completed frame encodes");
    let slot0 = AcknowledgementSlot {
        slot_index: 0,
        activated: true,
        store_uuid: sample_store_uuid(),
        bootstrap_id: bootstrap_id(),
        acknowledgement_epoch: 77,
        marker_epoch: 5,
        log_generation: 2,
        frame_sequence: 100,
        frame_end_offset: frame.len() as u64,
        frame_crc32: crc32(&frame),
    };
    let encoded_slot0 = encode_acknowledgement_slot(&slot0).expect("worked acknowledgement slot encodes");
    let seal = CommitSeal::from_acknowledgement_slot(&slot0, &encoded_slot0)
        .expect("worked commit seal derives from the exact slot");
    let encoded_seal = encode_commit_seal(&seal).expect("worked commit seal encodes");
    let slot1 = AcknowledgementSlot {
        slot_index: 1,
        acknowledgement_epoch: 78,
        frame_sequence: 101,
        frame_end_offset: 272,
        frame_crc32: 0x1020_3040,
        ..slot0.clone()
    };
    let states = [
        AcknowledgementSlotState::Populated(slot0),
        AcknowledgementSlotState::Populated(slot1),
    ];

    fixtures.push(Fixture::new(
        "acknowledgement.slot0.bin",
        encoded_slot0,
        FixtureValidation::AcknowledgementSlot,
    ));
    fixtures.push(Fixture::new(
        "acknowledgement.both-slots.bin",
        encode_acknowledgement_file(&states).expect("two-slot acknowledgement encodes"),
        FixtureValidation::AcknowledgementFile,
    ));
    fixtures.push(Fixture::new(
        "completed.frame.bin",
        frame.clone(),
        FixtureValidation::LedgerFrame {
            sequence: 100,
            generation: 2,
        },
    ));
    fixtures.push(Fixture::new(
        "completed.seal.bin",
        encoded_seal,
        FixtureValidation::CommitSeal,
    ));
    let mut unit = frame;
    unit.extend_from_slice(&encoded_seal);
    fixtures.push(Fixture::new(
        "completed.acknowledged-unit.bin",
        unit,
        FixtureValidation::LedgerFrame {
            sequence: 100,
            generation: 2,
        },
    ));
}

fn add_retirement_chains(fixtures: &mut Vec<Fixture>) {
    let records = record_map();
    let direct = ["retirement-intent", "logical-removed", "namespace-absent", "completed"];
    let tombstone = [
        "retirement-intent",
        "logical-removed",
        "tombstoned",
        "namespace-absent",
        "completed",
    ];
    for (name, chain) in [
        ("chain.direct-unlink.bin", direct.as_slice()),
        ("chain.tombstone.bin", tombstone.as_slice()),
    ] {
        let mut bytes = Vec::new();
        for (index, record_name) in chain.iter().enumerate() {
            let record = records
                .iter()
                .find(|(candidate, _)| candidate == record_name)
                .map(|(_, record)| record)
                .expect("chain record exists");
            bytes.extend_from_slice(&encode_ledger_frame(record, 100 + index as u64, 3).expect("chain record encodes"));
        }
        fixtures.push(Fixture::new(
            name,
            bytes,
            FixtureValidation::LedgerFrameStream {
                first_sequence: 100,
                generation: 3,
            },
        ));
    }
}

fn add_corruption_cases(fixtures: &mut Vec<Fixture>) {
    let completed = encode_ledger_frame(
        &LedgerRecord::Completed {
            ticket_id: ticket(),
            incarnation: incarnation(),
            completion_time_ns: 66,
            namespace_absent_sequence: 99,
        },
        100,
        3,
    )
    .expect("completed frame encodes");
    let mut bad_header_crc = completed.clone();
    bad_header_crc[36] ^= 0x80;
    fixtures.push(Fixture::new(
        "invalid.bad-header-crc.bin",
        bad_header_crc,
        FixtureValidation::InvalidLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
    let mut bad_payload_crc = completed.clone();
    let last = bad_payload_crc.len() - 1;
    bad_payload_crc[last] ^= 0x80;
    fixtures.push(Fixture::new(
        "invalid.bad-payload-crc.bin",
        bad_payload_crc,
        FixtureValidation::InvalidLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));

    let mut unknown_noncritical = completed.clone();
    unknown_noncritical[8..10].copy_from_slice(&0x7777_u16.to_le_bytes());
    unknown_noncritical[12..14].copy_from_slice(&0_u16.to_le_bytes());
    refresh_header_crc(&mut unknown_noncritical);
    fixtures.push(Fixture::new(
        "record.unknown-noncritical.bin",
        unknown_noncritical,
        FixtureValidation::LedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
    let mut unknown_critical = completed.clone();
    unknown_critical[8..10].copy_from_slice(&0x7777_u16.to_le_bytes());
    refresh_header_crc(&mut unknown_critical);
    fixtures.push(Fixture::new(
        "invalid.unknown-critical.bin",
        unknown_critical,
        FixtureValidation::InvalidLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));

    let intent = record_map()
        .into_iter()
        .find(|(name, _)| *name == "retirement-intent")
        .map(|(_, record)| record)
        .expect("retirement intent sample exists");
    let mut invalid_enum = encode_ledger_frame(&intent, 100, 3).expect("intent frame encodes");
    invalid_enum[72..74].copy_from_slice(&u16::MAX.to_le_bytes());
    refresh_payload_crc(&mut invalid_enum);
    fixtures.push(Fixture::new(
        "invalid.retirement-reason.bin",
        invalid_enum,
        FixtureValidation::InvalidTypedLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
    let mut invalid_utf8 = encode_ledger_frame(&intent, 100, 3).expect("intent frame encodes");
    invalid_utf8[150] = 0xff;
    refresh_payload_crc(&mut invalid_utf8);
    fixtures.push(Fixture::new(
        "invalid.retirement-path-utf8.bin",
        invalid_utf8,
        FixtureValidation::InvalidTypedLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
    let mut damaged_intent = encode_ledger_frame(&intent, 100, 3).expect("intent frame encodes");
    let last = damaged_intent.len() - 1;
    damaged_intent[last] ^= 1;
    fixtures.push(Fixture::new(
        "invalid.damaged-retirement-intent.bin",
        damaged_intent,
        FixtureValidation::InvalidLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));

    let mut truncations = Vec::new();
    for length in 0..completed.len() {
        truncations.extend_from_slice(&(length as u16).to_le_bytes());
        truncations.extend_from_slice(&completed[..length]);
    }
    fixtures.push(Fixture::new(
        "invalid.completed-all-truncations.bin",
        truncations,
        FixtureValidation::TruncatedLedgerFrames {
            sequence: 100,
            generation: 3,
        },
    ));

    let mut marker = encode_enabled_marker_file(&EnabledMarkerFile {
        slots: [Some(marker_slot(0, 1, 0, 2, 108, 1, 2)), None],
    })
    .expect("marker encodes");
    marker[103] ^= 1;
    fixtures.push(Fixture::new(
        "invalid.marker-torn-slot.bin",
        marker,
        FixtureValidation::InvalidMarker,
    ));

    let frame_crc = crc32(&completed);
    let slot = AcknowledgementSlot {
        slot_index: 0,
        activated: true,
        store_uuid: sample_store_uuid(),
        bootstrap_id: bootstrap_id(),
        acknowledgement_epoch: 77,
        marker_epoch: 5,
        log_generation: 3,
        frame_sequence: 100,
        frame_end_offset: completed.len() as u64,
        frame_crc32: frame_crc,
    };
    let encoded_slot = encode_acknowledgement_slot(&slot).expect("ack slot encodes");
    let mut torn_ack = encoded_slot;
    torn_ack[103] ^= 1;
    fixtures.push(Fixture::new(
        "invalid.acknowledgement-torn-nonzero.bin",
        torn_ack,
        FixtureValidation::InvalidAcknowledgement,
    ));
    let seal = CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot).expect("seal derives");
    let mut bad_seal = encode_commit_seal(&seal).expect("seal encodes");
    bad_seal[71] ^= 1;
    fixtures.push(Fixture::new(
        "invalid.seal-crc.bin",
        bad_seal,
        FixtureValidation::InvalidCommitSeal,
    ));

    let mut bad_snapshot = encode_snapshot(&snapshot(SnapshotMode::OrdinaryCompaction, Vec::new()))
        .expect("empty compacted snapshot encodes");
    let last = bad_snapshot.len() - 1;
    bad_snapshot[last] ^= 1;
    fixtures.push(Fixture::new(
        "invalid.snapshot-body-crc.bin",
        bad_snapshot,
        FixtureValidation::InvalidSnapshot,
    ));
}

fn refresh_header_crc(frame: &mut [u8]) {
    let checksum = crc32(&frame[..36]);
    frame[36..40].copy_from_slice(&checksum.to_le_bytes());
}

fn refresh_payload_crc(frame: &mut [u8]) {
    let header_length = usize::from(u16::from_le_bytes([frame[14], frame[15]]));
    let payload_length = u32::from_le_bytes(frame[16..20].try_into().expect("payload length")) as usize;
    let end = header_length + payload_length;
    let checksum = crc32(&frame[header_length..end]);
    frame[end..end + 4].copy_from_slice(&checksum.to_le_bytes());
}
