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
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::ContentFingerprint;
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;

const STORE_META_GOLDEN: [u8; 64] = hex_literal(
    "524d534d010000004000000000000000000102030405060708090a0b0c0d0e0f\
     0807060504030201101112131415161718191a1b1c1d1e1f000000004e4a603c",
);

const MARKER_SLOT_ZERO_GOLDEN: [u8; 104] = hex_literal(
    "524d454e010000006800000101000000000102030405060708090a0b0c0d0e0f\
     101112131415161718191a1b1c1d1e1f01000000000000000000000000000000\
     000000000000000002000000000000006c000000000000004433221188776655\
     000000003ed03b12",
);

const MARKER_SLOT_ONE_GOLDEN: [u8; 104] = hex_literal(
    "524d454e010000006800010101000000000102030405060708090a0b0c0d0e0f\
     101112131415161718191a1b1c1d1e1f02000000000000000100000000000000\
     01000000000000000a00000000000000c800000000000000ccbbaa9900ffeedd\
     000000004d6d9a0d",
);

const MARKER_SLOT_ZERO_NEWER_GOLDEN: [u8; 104] = hex_literal(
    "524d454e010000006800000101000000000102030405060708090a0b0c0d0e0f\
     101112131415161718191a1b1c1d1e1f03000000000000000200000000000000\
     020000000000000014000000000000002c010000000000000403020108070605\
     000000007620bbec",
);

const ORDINARY_EMPTY_SNAPSHOT_GOLDEN: [u8; 108] = hex_literal(
    "524d534e01000000680000006c00000000000000000102030405060708090a0b\
     0c0d0e0f01000000000000000100000000000000000000000000000064000000\
     0000000000000000000000000000000000000000000000000000000000000000\
     000000006a4d3c2800000000",
);

const BOOTSTRAP_EMPTY_SNAPSHOT_GOLDEN: [u8; 108] = hex_literal(
    "524d534e01000000680001006c00000000000000000102030405060708090a0b\
     0c0d0e0f00000000000000000000000000000000ffffffffffffffff01000000\
     0000000000000000000000000000000000000000000000000000000000000000\
     000000008142330000000000",
);

const TAIL_REPAIR_EMPTY_SNAPSHOT_GOLDEN: [u8; 108] = hex_literal(
    "524d534e01000000680002006c00000000000000000102030405060708090a0b\
     0c0d0e0f02000000000000000200000000000000010000000000000064000000\
     0000000000000000000000000000000000000000000000000000000000000000\
     000000008e8d636400000000",
);

const FULL_SNAPSHOT_GOLDEN: [u8; 773] = hex_literal(
    "524d534e01000000680000000503000000000000000102030405060708090a0b\
     0c0d0e0f01000000000000000100000000000000000000000000000064000000\
     0000000007000000000000002a00000000000000030000009902000000000000\
     00000000f6cfe99f01000100d9000000000102030405060708090a0b0c0d0e0f\
     0700000000000000030100000000000000000000000400000000000020212223\
     2425262728292a2b2c2d2e2f0100000000000000080706050403020118171615\
     1413121100000000000000001e00636f6d6d69746c6f672f3030303030303030\
     3030303030303030303030305b00636f6d6d69746c6f672f2e6372656174652e\
     69303030303030303030303030303030372e7330303030303030303030303030\
     303030303030302e6e3230323132323233323432353236323732383239326132\
     6232633264326532666a8d0d1a02000100150100002a00000000000000000102\
     030405060708090a0b0c0d0e0f07000000000000000507010063000000000000\
     0003000000000000000000000000000000000400000000000040414243444546\
     4748494a4b4c4d4e4f0100000000000000080706050403020118171615141312\
     1100000000000000001e00636f6d6d69746c6f672f3030303030303030303030\
     3030303030303030307f00636f6d6d69746c6f672f2e64656c6574652e7430\
     3030303030303030303030303032612e69303030303030303030303030303030\
     372e7330303030303030303030303030303030303030302e6d30303030303030\
     3030303030303030332e6e343034313432343334343435343634373438343934\
     61346234633464346534661814c2340300010087000000030107005800000000\
     00000002000000000000002827262524232221303132333435363738393a3b3c\
     3d3e3fd204000000000000ddccbbaa000000001e002e726f636b65746d712d6c\
     6966656379636c652f6f727068616e2e746d7029002e726f636b65746d712d6c\
     6966656379636c652f71756172616e74696e652f6f727068616e2e62696ea8ae\
     1baf7d6349db",
);

const fn hex_literal<const N: usize>(input: &str) -> [u8; N] {
    let bytes = input.as_bytes();
    let mut output = [0_u8; N];
    let mut source = 0;
    let mut target = 0;
    while source < bytes.len() {
        let high = bytes[source];
        if high == b' ' || high == b'\n' || high == b'\r' || high == b'\t' || high == b'\\' {
            source += 1;
            continue;
        }
        let low = bytes[source + 1];
        output[target] = (hex_nibble(high) << 4) | hex_nibble(low);
        source += 2;
        target += 1;
    }
    assert!(target == N);
    output
}

const fn hex_nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        b'A'..=b'F' => value - b'A' + 10,
        _ => panic!("invalid hexadecimal fixture"),
    }
}

fn sample_store_uuid() -> StoreUuid {
    StoreUuid::new(std::array::from_fn(|index| index as u8)).expect("golden UUID is non-zero")
}

fn sample_path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("sample path is canonical")
}

fn canonical_path() -> StoreRelativePath {
    sample_path("commitlog/00000000000000000000")
}

fn create_path() -> StoreRelativePath {
    sample_path("commitlog/.create.i0000000000000007.s00000000000000000000.n202122232425262728292a2b2c2d2e2f")
}

fn tombstone_path() -> StoreRelativePath {
    sample_path(
        "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n404142434445464748494a4b4c4d4e4f",
    )
}

fn sample_incarnation_entry() -> IncarnationSnapshotEntry {
    IncarnationSnapshotEntry {
        incarnation: FileIncarnationId::new(sample_store_uuid(), 7).expect("sample incarnation is non-zero"),
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: 1_024,
        create_nonce: std::array::from_fn(|index| 0x20 + index as u8),
        physical_key: Some(PhysicalFileKey::unix(0x0102_0304_0506_0708, 0x1112_1314_1516_1718)),
        canonical_path: canonical_path(),
        create_file_path: create_path(),
    }
}

fn sample_retirement_entry() -> RetirementTicketSnapshotEntry {
    RetirementTicketSnapshotEntry {
        ticket_id: TicketId::new(42).expect("sample ticket is non-zero"),
        incarnation: FileIncarnationId::new(sample_store_uuid(), 7).expect("sample incarnation is non-zero"),
        stage: RetirementStage::CompletedRetained,
        superseded_path_observed: true,
        quarantined: true,
        reason: RetirementReason::TtlExpired,
        stage_sequence: 99,
        mapping_generation: 3,
        segment_offset: 0,
        expected_file_length: 1_024,
        retirement_nonce: std::array::from_fn(|index| 0x40 + index as u8),
        target_key: PhysicalFileKey::unix(0x0102_0304_0506_0708, 0x1112_1314_1516_1718),
        canonical_path: canonical_path(),
        tombstone_path: Some(tombstone_path()),
    }
}

fn sample_quarantine_entry() -> QuarantineSnapshotEntry {
    QuarantineSnapshotEntry {
        entity_kind: QuarantineEntityKind::Sidecar,
        reason: QuarantineReason::UnknownOwner,
        sequence_at_observation: 88,
        physical_key: Some(PhysicalFileKey::windows(
            0x2122_2324_2526_2728,
            std::array::from_fn(|index| 0x30 + index as u8),
        )),
        content_fingerprint: Some(ContentFingerprint {
            length: 1_234,
            crc32: 0xaabb_ccdd,
        }),
        source_path: sample_path(".rocketmq-lifecycle/orphan.tmp"),
        destination_path: Some(sample_path(".rocketmq-lifecycle/quarantine/orphan.bin")),
    }
}

fn empty_snapshot(mode: SnapshotMode) -> LifecycleSnapshot {
    let (generation, predecessor_log_generation, base_sequence) = match mode {
        SnapshotMode::OrdinaryCompaction => (1, 0, 100),
        SnapshotMode::BootstrapInventory => (0, u64::MAX, 1),
        SnapshotMode::TailRepair => (2, 1, 100),
    };
    LifecycleSnapshot {
        mode,
        store_uuid: sample_store_uuid(),
        generation,
        log_generation: generation,
        predecessor_log_generation,
        base_sequence,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    }
}

fn full_snapshot(entries: Vec<SnapshotEntry>) -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: sample_store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 100,
        create_high_water: 7,
        ticket_high_water: 42,
        entries,
    }
}

fn sample_bootstrap_id() -> [u8; 16] {
    std::array::from_fn(|index| 0x10 + index as u8)
}

fn sample_store_meta() -> StoreMeta {
    StoreMeta {
        store_uuid: sample_store_uuid(),
        creation_time_ns: 0x0102_0304_0506_0708,
        bootstrap_id: sample_bootstrap_id(),
    }
}

fn marker_slot_zero() -> EnabledMarkerSlot {
    EnabledMarkerSlot {
        slot_index: 0,
        store_uuid: sample_store_uuid(),
        bootstrap_id: sample_bootstrap_id(),
        marker_epoch: 1,
        snapshot_generation: 0,
        log_generation: 0,
        anchor_sequence: 2,
        snapshot_file_length: 108,
        snapshot_file_crc32: 0x1122_3344,
        anchor_frame_crc32: 0x5566_7788,
    }
}

fn marker_slot_one() -> EnabledMarkerSlot {
    EnabledMarkerSlot {
        slot_index: 1,
        store_uuid: sample_store_uuid(),
        bootstrap_id: sample_bootstrap_id(),
        marker_epoch: 2,
        snapshot_generation: 1,
        log_generation: 1,
        anchor_sequence: 10,
        snapshot_file_length: 200,
        snapshot_file_crc32: 0x99aa_bbcc,
        anchor_frame_crc32: 0xddee_ff00,
    }
}

fn marker_slot_zero_newer() -> EnabledMarkerSlot {
    EnabledMarkerSlot {
        slot_index: 0,
        store_uuid: sample_store_uuid(),
        bootstrap_id: sample_bootstrap_id(),
        marker_epoch: 3,
        snapshot_generation: 2,
        log_generation: 2,
        anchor_sequence: 20,
        snapshot_file_length: 300,
        snapshot_file_crc32: 0x0102_0304,
        anchor_frame_crc32: 0x0506_0708,
    }
}

#[test]
fn store_meta_matches_frozen_golden_and_round_trips() {
    let meta = sample_store_meta();
    let encoded = encode_store_meta(&meta).expect("valid store metadata encodes");
    assert_eq!(encoded, STORE_META_GOLDEN);
    assert_eq!(decode_store_meta(&encoded), Ok(meta));
}

#[test]
fn marker_slots_and_both_physical_file_choices_match_frozen_goldens() {
    let slot_zero = marker_slot_zero();
    let slot_one = marker_slot_one();
    assert_eq!(encode_enabled_marker_slot(&slot_zero), Ok(MARKER_SLOT_ZERO_GOLDEN));
    assert_eq!(encode_enabled_marker_slot(&slot_one), Ok(MARKER_SLOT_ONE_GOLDEN));
    assert_eq!(
        decode_enabled_marker_slot(&MARKER_SLOT_ZERO_GOLDEN, 0),
        Ok(slot_zero.clone())
    );
    assert_eq!(
        decode_enabled_marker_slot(&MARKER_SLOT_ONE_GOLDEN, 1),
        Ok(slot_one.clone())
    );

    let slot_zero_only = EnabledMarkerFile {
        slots: [Some(slot_zero.clone()), None],
    };
    let encoded_zero = encode_enabled_marker_file(&slot_zero_only).expect("slot-zero file encodes");
    assert_eq!(&encoded_zero[..104], &MARKER_SLOT_ZERO_GOLDEN);
    assert_eq!(&encoded_zero[104..], &[0; 104]);
    assert_eq!(decode_enabled_marker_file(&encoded_zero), Ok(slot_zero_only));

    let both_slots = EnabledMarkerFile {
        slots: [Some(slot_zero), Some(slot_one.clone())],
    };
    let encoded_both = encode_enabled_marker_file(&both_slots).expect("two-slot file encodes");
    assert_eq!(&encoded_both[..104], &MARKER_SLOT_ZERO_GOLDEN);
    assert_eq!(&encoded_both[104..], &MARKER_SLOT_ONE_GOLDEN);
    assert_eq!(decode_enabled_marker_file(&encoded_both), Ok(both_slots));

    let newer_zero = marker_slot_zero_newer();
    assert_eq!(
        encode_enabled_marker_slot(&newer_zero),
        Ok(MARKER_SLOT_ZERO_NEWER_GOLDEN)
    );
    let selected_zero = EnabledMarkerFile {
        slots: [Some(newer_zero.clone()), Some(slot_one)],
    };
    assert_eq!(selected_zero.selected_slot(), Ok(&newer_zero));
}

#[test]
fn marker_selection_requires_consecutive_epochs_and_matching_identity() {
    let older = marker_slot_zero();
    let newer = marker_slot_one();
    let file = EnabledMarkerFile {
        slots: [Some(older.clone()), Some(newer.clone())],
    };
    assert_eq!(file.selected_slot(), Ok(&newer));

    let mut nonconsecutive = newer.clone();
    nonconsecutive.marker_epoch = 4;
    nonconsecutive.snapshot_generation = 3;
    nonconsecutive.log_generation = 3;
    let file = EnabledMarkerFile {
        slots: [Some(older.clone()), Some(nonconsecutive)],
    };
    assert!(matches!(
        file.selected_slot(),
        Err(SidecarError::NonConsecutiveMarkerEpochs { first: 1, second: 4 })
    ));

    let mut wrong_identity = newer;
    wrong_identity.bootstrap_id = [0x55; 16];
    let file = EnabledMarkerFile {
        slots: [Some(older), Some(wrong_identity)],
    };
    assert!(matches!(
        file.selected_slot(),
        Err(SidecarError::MarkerIdentityMismatch)
    ));

    let mut skipped_generation = marker_slot_one();
    skipped_generation.snapshot_generation = 2;
    skipped_generation.log_generation = 2;
    let file = EnabledMarkerFile {
        slots: [Some(marker_slot_zero()), Some(skipped_generation)],
    };
    assert!(matches!(
        file.selected_slot(),
        Err(SidecarError::InvalidMarkerSlotHistory)
    ));

    let mut repeated_anchor = marker_slot_one();
    repeated_anchor.anchor_sequence = 2;
    let file = EnabledMarkerFile {
        slots: [Some(marker_slot_zero()), Some(repeated_anchor)],
    };
    assert!(matches!(
        file.selected_slot(),
        Err(SidecarError::NonIncreasingMarkerAnchorSequence { older: 2, newer: 2 })
    ));
}

#[test]
fn marker_slots_bind_epoch_to_generation_and_bootstrap_anchor() {
    let mut impossible_generation = marker_slot_zero();
    impossible_generation.snapshot_generation = 7;
    impossible_generation.log_generation = 7;
    assert_eq!(
        encode_enabled_marker_slot(&impossible_generation),
        Err(SidecarError::InvalidMarkerSlotHistory)
    );

    let mut impossible_anchor = marker_slot_zero();
    impossible_anchor.anchor_sequence = 3;
    assert_eq!(
        encode_enabled_marker_slot(&impossible_anchor),
        Err(SidecarError::InvalidMarkerSlotHistory)
    );

    let mut encoded_generation = MARKER_SLOT_ZERO_GOLDEN;
    encoded_generation[56..64].copy_from_slice(&7_u64.to_le_bytes());
    encoded_generation[64..72].copy_from_slice(&7_u64.to_le_bytes());
    let checksum = crc32(&encoded_generation[..100]);
    encoded_generation[100..104].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(
        decode_enabled_marker_slot(&encoded_generation, 0),
        Err(SidecarError::InvalidMarkerSlotHistory)
    );

    let mut encoded_anchor = MARKER_SLOT_ZERO_GOLDEN;
    encoded_anchor[72..80].copy_from_slice(&3_u64.to_le_bytes());
    let checksum = crc32(&encoded_anchor[..100]);
    encoded_anchor[100..104].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(
        decode_enabled_marker_slot(&encoded_anchor, 0),
        Err(SidecarError::InvalidMarkerSlotHistory)
    );
}

#[test]
fn fixed_sidecars_reject_wrong_lengths_magic_versions_flags_reserved_and_crc() {
    assert!(matches!(
        decode_store_meta(&STORE_META_GOLDEN[..63]),
        Err(SidecarError::InvalidLength {
            structure: "store.meta",
            expected: 64,
            actual: 63
        })
    ));

    for (offset, expected) in [
        (0, "magic"),
        (4, "version"),
        (12, "flags"),
        (56, "reserved"),
        (60, "crc"),
    ] {
        let mut damaged = STORE_META_GOLDEN;
        damaged[offset] ^= 1;
        let error = decode_store_meta(&damaged).expect_err("damaged store metadata must fail");
        assert_eq!(error.category(), expected, "offset={offset}");
    }

    assert!(matches!(
        decode_enabled_marker_file(&[0; 207]),
        Err(SidecarError::InvalidLength {
            structure: "ENABLED.v1",
            expected: 208,
            actual: 207,
        })
    ));

    for (offset, expected) in [
        (0, "magic"),
        (4, "version"),
        (8, "length"),
        (10, "slot_index"),
        (11, "flags"),
        (12, "features"),
        (96, "reserved"),
        (100, "crc"),
    ] {
        let mut damaged = MARKER_SLOT_ZERO_GOLDEN;
        damaged[offset] ^= 1;
        let error = decode_enabled_marker_slot(&damaged, 0).expect_err("damaged marker slot must fail");
        assert_eq!(error.category(), expected, "offset={offset}");
    }
}

#[test]
fn fixed_sidecars_reject_zero_ids_and_invalid_marker_relationships() {
    let mut meta = sample_store_meta();
    meta.bootstrap_id = [0; 16];
    assert!(matches!(
        encode_store_meta(&meta),
        Err(SidecarError::ZeroOpaqueIdentifier { field: "bootstrap_id" })
    ));

    let mut slot = marker_slot_zero();
    slot.marker_epoch = 0;
    assert!(matches!(
        encode_enabled_marker_slot(&slot),
        Err(SidecarError::ZeroMarkerEpoch)
    ));
    slot.marker_epoch = 1;
    slot.anchor_sequence = 0;
    assert!(matches!(
        encode_enabled_marker_slot(&slot),
        Err(SidecarError::ZeroSnapshotAnchorSequence)
    ));
    slot.anchor_sequence = 2;
    slot.log_generation = 1;
    assert!(matches!(
        encode_enabled_marker_slot(&slot),
        Err(SidecarError::MarkerGenerationMismatch { snapshot: 0, log: 1 })
    ));

    let empty = EnabledMarkerFile { slots: [None, None] };
    assert!(matches!(empty.selected_slot(), Err(SidecarError::NoValidMarkerSlot)));
    assert!(matches!(
        encode_enabled_marker_file(&empty),
        Err(SidecarError::NoValidMarkerSlot)
    ));

    let impossible_single_slot = EnabledMarkerFile {
        slots: [None, Some(marker_slot_one())],
    };
    assert!(matches!(
        impossible_single_slot.selected_slot(),
        Err(SidecarError::InvalidMarkerSlotHistory)
    ));
}

#[test]
fn empty_snapshot_modes_match_frozen_header_body_goldens() {
    for (mode, golden) in [
        (SnapshotMode::OrdinaryCompaction, &ORDINARY_EMPTY_SNAPSHOT_GOLDEN),
        (SnapshotMode::BootstrapInventory, &BOOTSTRAP_EMPTY_SNAPSHOT_GOLDEN),
        (SnapshotMode::TailRepair, &TAIL_REPAIR_EMPTY_SNAPSHOT_GOLDEN),
    ] {
        let snapshot = empty_snapshot(mode);
        let encoded = encode_snapshot(&snapshot).expect("valid empty snapshot encodes");
        assert_eq!(encoded.as_slice(), golden, "mode={mode:?}");
        assert_eq!(decode_snapshot(&encoded), Ok(snapshot));
    }
}

#[test]
fn all_snapshot_payload_kinds_match_frozen_golden_and_encode_canonically() {
    let incarnation = SnapshotEntry::Incarnation(sample_incarnation_entry());
    let retirement = SnapshotEntry::RetirementTicket(sample_retirement_entry());
    let quarantine = SnapshotEntry::Quarantine(sample_quarantine_entry());
    let unordered = full_snapshot(vec![quarantine.clone(), retirement.clone(), incarnation.clone()]);

    let encoded = encode_snapshot(&unordered).expect("valid snapshot encodes");
    assert_eq!(encoded, FULL_SNAPSHOT_GOLDEN);

    let canonical = full_snapshot(vec![incarnation, retirement, quarantine]);
    let decoded = decode_snapshot(&encoded).expect("golden snapshot decodes");
    assert_eq!(decoded, canonical);
    let SnapshotEntry::RetirementTicket(retained) = &decoded.entries[1] else {
        panic!("second canonical entry must be the retirement ticket");
    };
    assert_eq!(retained.stage, RetirementStage::CompletedRetained);
    assert_eq!(retained.tombstone_path, Some(tombstone_path()));
}

#[test]
fn snapshot_decoder_rejects_noncanonical_order_and_duplicate_keys() {
    let incarnation_range = 104..333;
    let retirement_range = 333..622;
    let quarantine_range = 622..769;

    let mut reordered_body = Vec::new();
    reordered_body.extend_from_slice(&FULL_SNAPSHOT_GOLDEN[retirement_range.clone()]);
    reordered_body.extend_from_slice(&FULL_SNAPSHOT_GOLDEN[incarnation_range.clone()]);
    reordered_body.extend_from_slice(&FULL_SNAPSHOT_GOLDEN[quarantine_range]);
    let reordered = replace_snapshot_body(&FULL_SNAPSHOT_GOLDEN, &reordered_body, 3);
    assert!(matches!(
        decode_snapshot(&reordered),
        Err(SidecarError::NonCanonicalSnapshotOrder)
    ));

    let mut duplicate_body = Vec::new();
    duplicate_body.extend_from_slice(&FULL_SNAPSHOT_GOLDEN[incarnation_range.clone()]);
    duplicate_body.extend_from_slice(&FULL_SNAPSHOT_GOLDEN[incarnation_range]);
    let duplicate = replace_snapshot_body(&FULL_SNAPSHOT_GOLDEN, &duplicate_body, 2);
    assert!(matches!(
        decode_snapshot(&duplicate),
        Err(SidecarError::DuplicateSnapshotEntry { kind: 1 })
    ));

    let duplicate_model = full_snapshot(vec![
        SnapshotEntry::Incarnation(sample_incarnation_entry()),
        SnapshotEntry::Incarnation(sample_incarnation_entry()),
    ]);
    assert!(matches!(
        encode_snapshot(&duplicate_model),
        Err(SidecarError::DuplicateSnapshotEntry { kind: 1 })
    ));
}

#[test]
fn snapshot_decoder_checks_global_bounds_before_body_or_entry_allocation() {
    let mut oversized_body = BOOTSTRAP_EMPTY_SNAPSHOT_GOLDEN.to_vec();
    write_test_u64(&mut oversized_body, 12, MAX_SNAPSHOT_BODY_LENGTH as u64 + 109);
    write_test_u64(&mut oversized_body, 88, MAX_SNAPSHOT_BODY_LENGTH as u64 + 1);
    rewrite_snapshot_header_crc(&mut oversized_body);
    assert!(matches!(
        decode_snapshot(&oversized_body),
        Err(SidecarError::SnapshotBodyTooLarge { length, maximum })
            if length == MAX_SNAPSHOT_BODY_LENGTH as u64 + 1
                && maximum == MAX_SNAPSHOT_BODY_LENGTH as u64
    ));

    let mut too_many_entries = BOOTSTRAP_EMPTY_SNAPSHOT_GOLDEN.to_vec();
    write_test_u32(&mut too_many_entries, 84, MAX_SNAPSHOT_ENTRY_COUNT + 1);
    rewrite_snapshot_header_crc(&mut too_many_entries);
    assert!(matches!(
        decode_snapshot(&too_many_entries),
        Err(SidecarError::SnapshotEntryCountTooLarge { count, maximum })
            if count == u64::from(MAX_SNAPSHOT_ENTRY_COUNT + 1)
                && maximum == u64::from(MAX_SNAPSHOT_ENTRY_COUNT)
    ));

    let oversized_entry_header = [
        1, 0, 1, 0, 1, 0x40, 0, 0, // kind 1, version 1, payload length 16,385
        0, 0, 0, 0, // enough framing bytes to pass the count/body plausibility bound
    ];
    let oversized_entry = replace_snapshot_body(&BOOTSTRAP_EMPTY_SNAPSHOT_GOLDEN, &oversized_entry_header, 1);
    let error = decode_snapshot(&oversized_entry).expect_err("oversized entry payload must fail");
    assert!(
        matches!(
            error,
            SidecarError::SnapshotEntryPayloadTooLarge {
                kind: 1,
                length: 16_385,
                maximum: MAX_SNAPSHOT_ENTRY_PAYLOAD_LENGTH,
            }
        ),
        "{error:?}"
    );
}

#[test]
fn snapshot_rejects_header_body_entry_and_payload_corruption() {
    for (offset, expected_category) in [
        (0, "magic"),
        (4, "version"),
        (8, "length"),
        (11, "flags"),
        (96, "reserved"),
        (100, "crc"),
        (104, "body_crc"),
    ] {
        let mut damaged = BOOTSTRAP_EMPTY_SNAPSHOT_GOLDEN.to_vec();
        damaged[offset] ^= 1;
        let error = decode_snapshot(&damaged).expect_err("damaged snapshot must fail");
        assert_eq!(error.category(), expected_category, "offset={offset}");
    }

    let mut unknown_kind = FULL_SNAPSHOT_GOLDEN.to_vec();
    unknown_kind[104..106].copy_from_slice(&4_u16.to_le_bytes());
    rewrite_snapshot_body_crc(&mut unknown_kind);
    assert!(matches!(
        decode_snapshot(&unknown_kind),
        Err(SidecarError::InvalidSnapshotEntryKind { kind: 4 })
    ));

    let mut bad_version = FULL_SNAPSHOT_GOLDEN.to_vec();
    bad_version[106..108].copy_from_slice(&2_u16.to_le_bytes());
    rewrite_snapshot_body_crc(&mut bad_version);
    assert!(matches!(
        decode_snapshot(&bad_version),
        Err(SidecarError::UnsupportedSnapshotEntryVersion { kind: 1, version: 2 })
    ));

    let mut bad_entry_crc = FULL_SNAPSHOT_GOLDEN.to_vec();
    bad_entry_crc[329] ^= 1;
    rewrite_snapshot_body_crc(&mut bad_entry_crc);
    assert!(matches!(
        decode_snapshot(&bad_entry_crc),
        Err(SidecarError::SnapshotEntryChecksumMismatch { kind: 1, .. })
    ));

    let mut bad_phase = FULL_SNAPSHOT_GOLDEN.to_vec();
    bad_phase[136] = 4;
    rewrite_snapshot_entry_crc(&mut bad_phase, 104);
    rewrite_snapshot_body_crc(&mut bad_phase);
    assert!(matches!(
        decode_snapshot(&bad_phase),
        Err(SidecarError::InvalidEnumValue {
            field: "incarnation_phase",
            value: 4
        })
    ));
}

#[test]
fn snapshot_enforces_generation_high_water_stage_and_phase_relationships() {
    let mut snapshot = full_snapshot(vec![SnapshotEntry::Incarnation(sample_incarnation_entry())]);
    snapshot.log_generation = 2;
    assert!(matches!(
        encode_snapshot(&snapshot),
        Err(SidecarError::SnapshotGenerationMismatch { snapshot: 1, log: 2 })
    ));

    snapshot.log_generation = 1;
    snapshot.create_high_water = 6;
    assert!(matches!(
        encode_snapshot(&snapshot),
        Err(SidecarError::HighWaterBelowRepresented {
            field: "create_high_water",
            high_water: 6,
            represented: 7,
        })
    ));

    let mut retirement = sample_retirement_entry();
    retirement.stage_sequence = 101;
    let snapshot = full_snapshot(vec![SnapshotEntry::RetirementTicket(retirement)]);
    assert!(matches!(
        encode_snapshot(&snapshot),
        Err(SidecarError::StageSequenceOutOfRange {
            sequence: 101,
            base_sequence: 100
        })
    ));

    let mut allocated = sample_incarnation_entry();
    allocated.phase = IncarnationPhase::Allocated;
    assert!(matches!(
        encode_snapshot(&full_snapshot(vec![SnapshotEntry::Incarnation(allocated)])),
        Err(SidecarError::IncarnationPhaseKeyMismatch)
    ));
}

fn replace_snapshot_body(original: &[u8], body: &[u8], entry_count: u32) -> Vec<u8> {
    let mut output = original[..104].to_vec();
    write_test_u64(&mut output, 12, (108 + body.len()) as u64);
    write_test_u32(&mut output, 84, entry_count);
    write_test_u64(&mut output, 88, body.len() as u64);
    rewrite_snapshot_header_crc(&mut output);
    output.extend_from_slice(body);
    output.extend_from_slice(&crc32(body).to_le_bytes());
    output
}

fn rewrite_snapshot_header_crc(snapshot: &mut [u8]) {
    let checksum = crc32(&snapshot[..100]);
    snapshot[100..104].copy_from_slice(&checksum.to_le_bytes());
}

fn rewrite_snapshot_body_crc(snapshot: &mut [u8]) {
    let body_length = u64::from_le_bytes(snapshot[88..96].try_into().expect("body length field")) as usize;
    let body_end = 104 + body_length;
    let checksum = crc32(&snapshot[104..body_end]);
    snapshot[body_end..body_end + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn rewrite_snapshot_entry_crc(snapshot: &mut [u8], entry_offset: usize) {
    let payload_length = u32::from_le_bytes(
        snapshot[entry_offset + 4..entry_offset + 8]
            .try_into()
            .expect("payload length"),
    ) as usize;
    let crc_offset = entry_offset + 8 + payload_length;
    let checksum = crc32(&snapshot[entry_offset..crc_offset]);
    snapshot[crc_offset..crc_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn write_test_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_test_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
