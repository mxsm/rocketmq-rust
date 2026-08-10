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

use super::samples::*;
use super::Fixture;
use super::FixtureValidation;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::encode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::encode_commit_seal;
use crate::mapped_file::retirement::codec::encode_ledger_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlot;
use crate::mapped_file::retirement::codec::CommitSeal;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::MAX_PAYLOAD_LENGTH;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::encode_snapshot;
use crate::mapped_file::retirement::sidecar::EnabledMarkerFile;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::sidecar::MAX_SNAPSHOT_BODY_LENGTH;
use crate::mapped_file::retirement::sidecar::MAX_SNAPSHOT_ENTRY_COUNT;

pub(super) fn add_edge_cases(fixtures: &mut Vec<Fixture>) {
    let frame = completed_frame(100, 3);
    let slot0 = acknowledgement_slot(0, 77, 100, 3, &frame);
    let encoded_slot0 = encode_acknowledgement_slot(&slot0).expect("slot zero encodes");
    let seal = CommitSeal::from_acknowledgement_slot(&slot0, &encoded_slot0).expect("seal derives from slot zero");
    let encoded_seal = encode_commit_seal(&seal).expect("commit seal encodes");

    add_acknowledgement_edges(fixtures, &frame, &slot0, &encoded_slot0, &encoded_seal);
    add_stream_edges(fixtures, &frame, &encoded_seal);
    add_bounded_decode_edges(fixtures, &frame);
    add_marker_edges(fixtures);
}

fn add_acknowledgement_edges(
    fixtures: &mut Vec<Fixture>,
    frame: &[u8],
    slot0: &AcknowledgementSlot,
    encoded_slot0: &[u8; 104],
    encoded_seal: &[u8; 72],
) {
    let initial_slot0 = encode_acknowledgement_slot(&AcknowledgementSlot {
        acknowledgement_epoch: 1,
        ..slot0.clone()
    })
    .expect("initial slot zero encodes");
    let initial_slot1 = encode_acknowledgement_slot(&AcknowledgementSlot {
        slot_index: 1,
        acknowledgement_epoch: 2,
        frame_sequence: 101,
        ..slot0.clone()
    })
    .expect("initial slot one encodes");
    let mut slot1_file = initial_slot0.to_vec();
    slot1_file.extend_from_slice(&initial_slot1);
    fixtures.push(Fixture::new(
        "acknowledgement.slot1.bin",
        slot1_file,
        FixtureValidation::AcknowledgementFile,
    ));
    fixtures.push(Fixture::new(
        "acknowledgement.all-zero.bin",
        vec![0_u8; 208],
        FixtureValidation::AcknowledgementFileWithoutAuthoritative,
    ));

    let nonconsecutive_slot1 = AcknowledgementSlot {
        acknowledgement_epoch: 80,
        frame_sequence: 103,
        ..slot0.clone()
    };
    let mut nonconsecutive = encoded_slot0.to_vec();
    nonconsecutive.extend_from_slice(
        &encode_acknowledgement_slot(&AcknowledgementSlot {
            slot_index: 1,
            ..nonconsecutive_slot1
        })
        .expect("individually valid nonconsecutive slot encodes"),
    );
    fixtures.push(Fixture::new(
        "invalid.acknowledgement-nonconsecutive.bin",
        nonconsecutive,
        FixtureValidation::InvalidAcknowledgementFile,
    ));

    let mut torn_slot = *encoded_slot0;
    torn_slot[103] ^= 1;
    let mut reconstruction = bundle_prefix(frame);
    reconstruction.extend_from_slice(&torn_slot);
    reconstruction.extend_from_slice(encoded_slot0);
    reconstruction.extend_from_slice(encoded_seal);
    fixtures.push(Fixture::new(
        "recovery.ack-seal-reconstruction.bundle.bin",
        reconstruction,
        FixtureValidation::AcknowledgementReconstructionBundle {
            sequence: 100,
            generation: 3,
        },
    ));

    let mut mismatched_seal = *encoded_seal;
    let wrong_slot_crc = u32::from_le_bytes(mismatched_seal[60..64].try_into().expect("slot CRC field")) ^ 1;
    mismatched_seal[60..64].copy_from_slice(&wrong_slot_crc.to_le_bytes());
    let seal_crc = crc32(&mismatched_seal[..68]);
    mismatched_seal[68..72].copy_from_slice(&seal_crc.to_le_bytes());
    let mut mismatch_bundle = bundle_prefix(frame);
    mismatch_bundle.extend_from_slice(encoded_slot0);
    mismatch_bundle.extend_from_slice(&mismatched_seal);
    fixtures.push(Fixture::new(
        "invalid.seal-slot-crc-mismatch.bundle.bin",
        mismatch_bundle,
        FixtureValidation::InvalidAcknowledgedUnitBinding {
            sequence: 100,
            generation: 3,
        },
    ));
}

fn add_stream_edges(fixtures: &mut Vec<Fixture>, frame: &[u8], encoded_seal: &[u8; 72]) {
    let mut unit = frame.to_vec();
    unit.extend_from_slice(encoded_seal);
    let mut truncations = Vec::new();
    for length in 0..=unit.len() {
        truncations.extend_from_slice(&(length as u16).to_le_bytes());
        truncations.extend_from_slice(&unit[..length]);
    }
    fixtures.push(Fixture::new(
        "recovery.acknowledged-unit-all-truncations.bin",
        truncations,
        FixtureValidation::TruncatedSealedUnit {
            sequence: 100,
            generation: 3,
            frame_length: frame.len(),
        },
    ));
    fixtures.push(Fixture::new(
        "recovery.valid-ack-missing-seal.log.bin",
        frame,
        FixtureValidation::LedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
    let mut partial_seal = frame.to_vec();
    partial_seal.extend_from_slice(&encoded_seal[..36]);
    fixtures.push(Fixture::new(
        "recovery.valid-ack-partial-seal.log.bin",
        partial_seal,
        FixtureValidation::LedgerFrameThenPartialSeal {
            sequence: 100,
            generation: 3,
        },
    ));

    let frame101 = completed_frame(101, 3);
    let frame102 = completed_frame(102, 3);
    let mut gap = frame.to_vec();
    gap.extend_from_slice(&frame102);
    fixtures.push(Fixture::new(
        "invalid.sequence-gap.log.bin",
        gap,
        FixtureValidation::InvalidLedgerFrameStream {
            first_sequence: 100,
            generation: 3,
        },
    ));
    let mut duplicate = frame.to_vec();
    duplicate.extend_from_slice(frame);
    fixtures.push(Fixture::new(
        "invalid.sequence-duplicate.log.bin",
        duplicate,
        FixtureValidation::InvalidLedgerFrameStream {
            first_sequence: 100,
            generation: 3,
        },
    ));
    let mut damaged = frame.to_vec();
    let mut bad_second = frame101;
    let final_byte = bad_second.len() - 1;
    bad_second[final_byte] ^= 1;
    damaged.extend_from_slice(&bad_second);
    damaged.extend_from_slice(&frame102);
    fixtures.push(Fixture::new(
        "invalid.mid-log-damage.log.bin",
        damaged,
        FixtureValidation::InvalidLedgerFrameStream {
            first_sequence: 100,
            generation: 3,
        },
    ));

    let mut overflow = completed_frame(u64::MAX, 3);
    overflow.extend_from_slice(frame);
    fixtures.push(Fixture::new(
        "invalid.sequence-overflow.log.bin",
        overflow,
        FixtureValidation::SequenceOverflowStream { generation: 3 },
    ));
}

fn add_bounded_decode_edges(fixtures: &mut Vec<Fixture>, frame: &[u8]) {
    let mut oversized_header = frame[..16].to_vec();
    oversized_header[14..16].copy_from_slice(&41_u16.to_le_bytes());
    fixtures.push(Fixture::new(
        "invalid.oversized-header.bin",
        oversized_header,
        FixtureValidation::InvalidLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));
    let mut oversized_payload = frame[..20].to_vec();
    oversized_payload[16..20].copy_from_slice(&((MAX_PAYLOAD_LENGTH + 1) as u32).to_le_bytes());
    fixtures.push(Fixture::new(
        "invalid.oversized-payload.bin",
        oversized_payload,
        FixtureValidation::InvalidLedgerFrame {
            sequence: 100,
            generation: 3,
        },
    ));

    let empty_snapshot = encode_snapshot(&snapshot(SnapshotMode::OrdinaryCompaction, Vec::new()))
        .expect("empty compacted snapshot encodes");
    let mut oversized_body = empty_snapshot.clone();
    oversized_body[12..20].copy_from_slice(&(MAX_SNAPSHOT_BODY_LENGTH as u64 + 109).to_le_bytes());
    oversized_body[88..96].copy_from_slice(&(MAX_SNAPSHOT_BODY_LENGTH as u64 + 1).to_le_bytes());
    refresh_snapshot_header_crc(&mut oversized_body);
    fixtures.push(Fixture::new(
        "invalid.snapshot-oversized-body.bin",
        oversized_body,
        FixtureValidation::InvalidSnapshot,
    ));
    let mut too_many_entries = empty_snapshot.clone();
    too_many_entries[84..88].copy_from_slice(&(MAX_SNAPSHOT_ENTRY_COUNT + 1).to_le_bytes());
    refresh_snapshot_header_crc(&mut too_many_entries);
    fixtures.push(Fixture::new(
        "invalid.snapshot-too-many-entries.bin",
        too_many_entries,
        FixtureValidation::InvalidSnapshot,
    ));
    let mut reserved = empty_snapshot;
    reserved[96..100].copy_from_slice(&1_u32.to_le_bytes());
    refresh_snapshot_header_crc(&mut reserved);
    fixtures.push(Fixture::new(
        "invalid.snapshot-reserved.bin",
        reserved,
        FixtureValidation::InvalidSnapshot,
    ));
}

fn add_marker_edges(fixtures: &mut Vec<Fixture>) {
    let marker = EnabledMarkerFile {
        slots: [
            Some(marker_slot(0, 1, 0, 2, 108, 0x1111_1111, 0x2222_2222)),
            Some(marker_slot(1, 2, 1, 200, 108, 0x3333_3333, 0x4444_4444)),
        ],
    };
    fixtures.push(Fixture::new(
        "enabled.newer-slot-missing-pair.bin",
        encode_enabled_marker_file(&marker).expect("newer marker slot encodes"),
        FixtureValidation::MarkerFile,
    ));
}

fn completed_frame(sequence: u64, generation: u64) -> Vec<u8> {
    encode_ledger_frame(
        &LedgerRecord::Completed {
            ticket_id: ticket(),
            incarnation: incarnation(),
            completion_time_ns: 66,
            namespace_absent_sequence: 99,
        },
        sequence,
        generation,
    )
    .expect("completed edge frame encodes")
}

fn acknowledgement_slot(
    slot_index: u8,
    acknowledgement_epoch: u64,
    frame_sequence: u64,
    log_generation: u64,
    frame: &[u8],
) -> AcknowledgementSlot {
    AcknowledgementSlot {
        slot_index,
        activated: true,
        store_uuid: sample_store_uuid(),
        bootstrap_id: bootstrap_id(),
        acknowledgement_epoch,
        marker_epoch: 5,
        log_generation,
        frame_sequence,
        frame_end_offset: frame.len() as u64,
        frame_crc32: crc32(frame),
    }
}

fn bundle_prefix(frame: &[u8]) -> Vec<u8> {
    let mut bundle = Vec::with_capacity(4 + frame.len() + 208);
    bundle.extend_from_slice(&(frame.len() as u32).to_le_bytes());
    bundle.extend_from_slice(frame);
    bundle
}

fn refresh_snapshot_header_crc(snapshot: &mut [u8]) {
    let checksum = crc32(&snapshot[..100]);
    snapshot[100..104].copy_from_slice(&checksum.to_le_bytes());
}
