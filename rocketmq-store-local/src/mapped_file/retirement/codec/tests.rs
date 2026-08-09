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

use super::super::identity::FileIncarnationId;
use super::super::identity::PhysicalFileKey;
use super::super::identity::StoreRelativePath;
use super::super::identity::StoreUuid;
use super::super::identity::TicketId;
use super::*;

mod acknowledgement;
mod frame;
mod record;

const COMPLETED_FRAME: [u8; 100] = [
    0x52, 0x4d, 0x4c, 0x43, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00, 0x01, 0x00, 0x01, 0x00, 0x28, 0x00, 0x38, 0x00, 0x00,
    0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xd8,
    0xd0, 0x0f, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05,
    0x04, 0x03, 0x02, 0x01, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x19, 0xfc, 0xea, 0x0a,
];

#[test]
fn crc32_matches_iso_hdlc_check_vector() {
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
}

#[test]
fn completed_golden_frame_decodes_byte_exactly() {
    let outcome = decode_next_frame(&COMPLETED_FRAME, 100, 2).expect("golden frame must decode");
    let DecodeOutcome::Frame(frame) = outcome else {
        panic!("complete input must produce a frame");
    };

    assert_eq!(frame.record_type(), RecordType::Completed);
    assert_eq!(frame.sequence(), 100);
    assert_eq!(frame.log_generation(), 2);
    assert_eq!(frame.payload(), &COMPLETED_FRAME[40..96]);
    assert_eq!(frame.encoded_len(), COMPLETED_FRAME.len());
}

#[test]
fn completed_golden_frame_encodes_byte_exactly() {
    let encoded = encode_frame(RecordType::Completed, 100, 2, &COMPLETED_FRAME[40..96])
        .expect("bounded known record must encode");
    assert_eq!(encoded, COMPLETED_FRAME);
}

#[test]
fn typed_completed_record_matches_complete_golden_frame() {
    let uuid_bytes = std::array::from_fn(|index| index as u8);
    let store_uuid = StoreUuid::new(uuid_bytes).expect("golden UUID is nonzero");
    let record = LedgerRecord::Completed {
        ticket_id: TicketId::new(42).expect("golden ticket is nonzero"),
        incarnation: FileIncarnationId::new(store_uuid, 7).expect("golden incarnation is nonzero"),
        completion_time_ns: 0x0102_0304_0506_0708,
        namespace_absent_sequence: 9,
    };

    assert_eq!(
        encode_ledger_frame(&record, 100, 2).expect("golden typed record must encode"),
        COMPLETED_FRAME
    );
    let DecodeOutcome::Frame(frame) = decode_next_frame(&COMPLETED_FRAME, 100, 2).expect("golden frame must decode")
    else {
        panic!("complete input must produce a frame");
    };
    assert_eq!(frame.decode_record(), Ok(Some(record)));
}

#[test]
fn every_assigned_v1_record_round_trips_with_exact_payload_length() {
    for (record, sequence, generation, expected_payload_length) in sample_records() {
        let encoded = encode_ledger_frame(&record, sequence, generation).expect("sample record must encode");
        assert_eq!(
            u32::from_le_bytes(encoded[16..20].try_into().expect("payload length field")) as usize,
            expected_payload_length,
            "record={:?}",
            record.record_type()
        );

        let DecodeOutcome::Frame(frame) =
            decode_next_frame(&encoded, sequence, generation).expect("encoded frame must decode")
        else {
            panic!("complete input must produce a frame");
        };
        assert_eq!(frame.record_type(), record.record_type());
        assert_eq!(frame.decode_record(), Ok(Some(record)));
    }
}

#[test]
fn optional_quarantine_and_namespace_fields_round_trip_when_absent() {
    let incarnation = sample_incarnation();
    let records = [
        LedgerRecord::NamespaceAbsent {
            ticket_id: sample_ticket(),
            incarnation,
            replacement_observed: false,
            observation_time_ns: 44,
            target_key: PhysicalFileKey::unix(0, 0),
            canonical_path: sample_path("commitlog/00000000000000000000"),
            tombstone_path: None,
        },
        LedgerRecord::Quarantined {
            entity_kind: QuarantineEntityKind::Sidecar,
            reason: QuarantineReason::UnknownOwner,
            sequence_at_observation: 8,
            physical_key: None,
            content_fingerprint: None,
            source_path: sample_path(".rocketmq-lifecycle/orphan.tmp"),
            destination_path: None,
        },
    ];

    for (index, record) in records.into_iter().enumerate() {
        let sequence = 200 + index as u64;
        let encoded = encode_ledger_frame(&record, sequence, 3).expect("optional record must encode");
        let DecodeOutcome::Frame(frame) = decode_next_frame(&encoded, sequence, 3).expect("optional frame must decode")
        else {
            panic!("complete input must produce a frame");
        };
        assert_eq!(frame.decode_record(), Ok(Some(record)));
    }
}

#[test]
fn payload_decoder_rejects_invalid_enum_reserved_path_key_and_proof_bytes() {
    let mut retirement = encoded_sample(RecordType::RetirementIntent);
    mutate_payload(&mut retirement, 32, &[10, 0]);
    assert!(matches!(
        decode_typed(&retirement, 100, 3),
        Err(CodecError::InvalidEnumValue {
            field: "retirement_reason",
            value: 10,
        })
    ));

    let mut store_initialized = encoded_sample(RecordType::StoreInitialized);
    mutate_payload(&mut store_initialized, 56, &2_u64.to_le_bytes());
    assert!(matches!(
        decode_typed(&store_initialized, 1, 0),
        Err(CodecError::NonZeroReserved {
            field: "feature_bitmap",
            value: 2,
        })
    ));

    let mut allocate = encoded_sample(RecordType::AllocateIncarnation);
    mutate_payload(&mut allocate, 58, b"\\");
    assert!(matches!(
        decode_typed(&allocate, 100, 3),
        Err(CodecError::InvalidIdentity {
            field: "canonical_path",
            ..
        })
    ));

    let mut bind = encoded_sample(RecordType::BindIncarnation);
    mutate_payload(&mut bind, 32, &[9]);
    assert_eq!(
        decode_typed(&bind, 100, 3),
        Err(CodecError::InvalidPhysicalFileKeyKind { kind: 9 })
    );

    let mut completed = COMPLETED_FRAME.to_vec();
    mutate_payload(&mut completed, 48, &0x0000_000b_u32.to_le_bytes());
    assert_eq!(
        decode_typed(&completed, 100, 2),
        Err(CodecError::InvalidProofFlags {
            field: "completed",
            flags: 0x0000_000b,
        })
    );
}

#[test]
fn payload_decoder_rejects_truncation_trailing_bytes_and_invalid_utf8() {
    let allocate = encoded_sample(RecordType::AllocateIncarnation);
    let payload = payload_of(&allocate);
    for length in [0, 1, 15, 16, 23, 24, 55, 56, payload.len() - 1] {
        let truncated = encode_frame(RecordType::AllocateIncarnation, 100, 3, &payload[..length])
            .expect("bounded truncated payload still has a valid envelope");
        assert!(decode_typed(&truncated, 100, 3).is_err(), "length={length}");
    }

    let mut trailing_payload = payload.to_vec();
    trailing_payload.push(0);
    let trailing = encode_frame(RecordType::AllocateIncarnation, 100, 3, &trailing_payload)
        .expect("bounded payload still has a valid envelope");
    assert!(matches!(
        decode_typed(&trailing, 100, 3),
        Err(CodecError::TrailingPayloadBytes { remaining: 1, .. })
    ));

    let mut invalid_utf8 = allocate;
    mutate_payload(&mut invalid_utf8, 58, &[0xff]);
    assert_eq!(
        decode_typed(&invalid_utf8, 100, 3),
        Err(CodecError::InvalidUtf8Path {
            field: "canonical_path",
        })
    );
}

#[test]
fn payload_codec_enforces_cross_field_and_envelope_invariants() {
    let uuid = sample_store_uuid();
    let invalid_prepared = LedgerRecord::GenerationPrepared {
        store_uuid: uuid,
        source_generation: 2,
        target_generation: 4,
        target_snapshot_generation: 4,
        open_reason: OpenReason::Compaction,
    };
    assert!(matches!(
        encode_ledger_frame(&invalid_prepared, 10, 2),
        Err(CodecError::InvalidGenerationRelationship { .. })
    ));

    let invalid_abort = LedgerRecord::GenerationAborted {
        store_uuid: uuid,
        source_generation: 2,
        target_generation: 3,
        prepared_sequence: 8,
        abort_reason: GenerationAbortReason::Validation,
    };
    assert!(matches!(
        encode_ledger_frame(&invalid_abort, 10, 2),
        Err(CodecError::InvalidEnvelopeRelationship { .. })
    ));

    let invalid_tail = LedgerRecord::LogOpened {
        store_uuid: uuid,
        generation: 3,
        snapshot_generation: 3,
        predecessor_log_generation: 2,
        predecessor_terminal_acknowledged_sequence: 9,
        snapshot_base_sequence: 9,
        snapshot_file_length: 108,
        snapshot_file_crc32: 1,
        predecessor_prefix_crc32: 2,
        validated_prefix_length: 100,
        unacknowledged_suffix_length: 1,
        unacknowledged_suffix_crc32: 3,
        open_reason: OpenReason::Compaction,
        predecessor_acknowledgement_epoch: 7,
    };
    assert_eq!(
        encode_ledger_frame(&invalid_tail, 10, 3),
        Err(CodecError::InvalidTailRepairFields)
    );
}

#[test]
fn tail_repair_suffix_length_is_strictly_bounded_by_one_sealed_record_unit() {
    let uuid = sample_store_uuid();
    let tail_repair = |unacknowledged_suffix_length| LedgerRecord::LogOpened {
        store_uuid: uuid,
        generation: 3,
        snapshot_generation: 3,
        predecessor_log_generation: 2,
        predecessor_terminal_acknowledged_sequence: 9,
        snapshot_base_sequence: 9,
        snapshot_file_length: 108,
        snapshot_file_crc32: 1,
        predecessor_prefix_crc32: 2,
        validated_prefix_length: 100,
        unacknowledged_suffix_length,
        unacknowledged_suffix_crc32: 3,
        open_reason: OpenReason::TailRepair,
        predecessor_acknowledgement_epoch: 7,
    };

    assert!(encode_ledger_frame(&tail_repair((MAX_SEALED_RECORD_UNIT_LENGTH - 1) as u32), 10, 3).is_ok());
    let mut invalid_decoded = encode_ledger_frame(&tail_repair((MAX_SEALED_RECORD_UNIT_LENGTH - 1) as u32), 10, 3)
        .expect("maximum valid tail-repair suffix encodes");
    for invalid_length in [0, MAX_SEALED_RECORD_UNIT_LENGTH as u32, u32::MAX] {
        assert_eq!(
            encode_ledger_frame(&tail_repair(invalid_length), 10, 3),
            Err(CodecError::InvalidTailRepairFields)
        );
        mutate_payload(&mut invalid_decoded, 80, &invalid_length.to_le_bytes());
        assert_eq!(
            decode_typed(&invalid_decoded, 10, 3),
            Err(CodecError::InvalidTailRepairFields)
        );
    }
}

#[test]
fn decoder_distinguishes_eof_from_valid_trailing_partial() {
    assert_eq!(decode_next_frame(&[], 100, 2), Ok(DecodeOutcome::EndOfInput));

    for length in [1, 4, 8, 16, 39, 40, 41, 95, 99] {
        assert!(matches!(
            decode_next_frame(&COMPLETED_FRAME[..length], 100, 2),
            Ok(DecodeOutcome::TrailingPartial(_))
        ));
    }
}

#[test]
fn malformed_partial_prefix_is_corruption_not_tail_partial() {
    let mut bad_magic = COMPLETED_FRAME[..8].to_vec();
    bad_magic[0] ^= 0xff;
    assert!(matches!(
        decode_next_frame(&bad_magic, 100, 2),
        Err(CodecError::InvalidMagic { .. })
    ));

    let mut bad_version = COMPLETED_FRAME[..8].to_vec();
    bad_version[4] = 2;
    assert!(matches!(
        decode_next_frame(&bad_version, 100, 2),
        Err(CodecError::UnsupportedFormatVersion { major: 2, minor: 0 })
    ));
}

#[test]
fn impossible_sequence_generation_and_crc_prefixes_are_not_tail_partials() {
    for (field, offset, available) in [
        ("sequence", 20, 21),
        ("log_generation", 28, 29),
        ("header_crc32", 36, 37),
        ("payload_crc32", 96, 97),
    ] {
        let mut impossible = COMPLETED_FRAME[..available].to_vec();
        impossible[offset] ^= 1;
        assert_eq!(
            decode_next_frame(&impossible, 100, 2),
            Err(CodecError::InvalidFieldPrefix { field, offset }),
            "field={field}"
        );
    }
}

#[test]
fn decoder_rejects_header_payload_and_ordering_corruption() {
    let mut bad_header_crc = COMPLETED_FRAME;
    bad_header_crc[36] ^= 1;
    assert!(matches!(
        decode_next_frame(&bad_header_crc, 100, 2),
        Err(CodecError::HeaderCrcMismatch { .. })
    ));

    let mut bad_payload_crc = COMPLETED_FRAME;
    bad_payload_crc[96] ^= 1;
    assert!(matches!(
        decode_next_frame(&bad_payload_crc, 100, 2),
        Err(CodecError::PayloadCrcMismatch { .. })
    ));

    assert_eq!(
        decode_next_frame(&COMPLETED_FRAME, 99, 2),
        Err(CodecError::SequenceMismatch {
            expected: 99,
            actual: 100,
        })
    );
    assert_eq!(
        decode_next_frame(&COMPLETED_FRAME, 100, 3),
        Err(CodecError::LogGenerationMismatch { expected: 3, actual: 2 })
    );
}

#[test]
fn decoder_skips_only_valid_unknown_noncritical_records() {
    let mut noncritical = COMPLETED_FRAME;
    noncritical[8..10].copy_from_slice(&0x7777_u16.to_le_bytes());
    noncritical[12..14].copy_from_slice(&0_u16.to_le_bytes());
    rewrite_header_crc(&mut noncritical);

    let DecodeOutcome::Frame(frame) =
        decode_next_frame(&noncritical, 100, 2).expect("unknown noncritical record is skippable")
    else {
        panic!("complete input must produce a frame");
    };
    assert_eq!(frame.record_type(), RecordType::Unknown(0x7777));

    noncritical[12..14].copy_from_slice(&1_u16.to_le_bytes());
    rewrite_header_crc(&mut noncritical);
    assert_eq!(
        decode_next_frame(&noncritical, 100, 2),
        Err(CodecError::UnknownCriticalRecordType { record_type: 0x7777 })
    );
}

#[test]
fn codec_enforces_frame_limits_and_reserved_values_before_allocation() {
    assert_eq!(
        encode_frame(RecordType::Completed, 0, 2, &[]),
        Err(CodecError::ZeroSequence)
    );
    assert_eq!(
        encode_frame(RecordType::Completed, 1, 0, &[0; MAX_PAYLOAD_LENGTH + 1]),
        Err(CodecError::PayloadTooLarge {
            length: MAX_PAYLOAD_LENGTH + 1,
            maximum: MAX_PAYLOAD_LENGTH,
        })
    );

    let mut oversized = COMPLETED_FRAME;
    oversized[16..20].copy_from_slice(&((MAX_PAYLOAD_LENGTH + 1) as u32).to_le_bytes());
    rewrite_header_crc(&mut oversized);
    assert_eq!(
        decode_next_frame(&oversized, 100, 2),
        Err(CodecError::PayloadTooLarge {
            length: MAX_PAYLOAD_LENGTH + 1,
            maximum: MAX_PAYLOAD_LENGTH,
        })
    );

    let mut short_header = COMPLETED_FRAME;
    short_header[14..16].copy_from_slice(&39_u16.to_le_bytes());
    assert_eq!(
        decode_next_frame(&short_header, 100, 2),
        Err(CodecError::InvalidHeaderLength {
            length: 39,
            minimum: MIN_HEADER_LENGTH,
            maximum: MIN_HEADER_LENGTH,
        })
    );
}

fn rewrite_header_crc(frame: &mut [u8]) {
    let header_length = u16::from_le_bytes([frame[14], frame[15]]) as usize;
    let mut covered = Vec::from(&frame[..36]);
    covered.extend_from_slice(&frame[40..header_length]);
    frame[36..40].copy_from_slice(&crc32(&covered).to_le_bytes());
}

fn sample_store_uuid() -> StoreUuid {
    StoreUuid::new([1; 16]).expect("sample store UUID is nonzero")
}

fn sample_incarnation() -> FileIncarnationId {
    FileIncarnationId::new(sample_store_uuid(), 7).expect("sample incarnation is nonzero")
}

fn sample_ticket() -> TicketId {
    TicketId::new(42).expect("sample ticket is nonzero")
}

fn sample_path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("sample path is canonical")
}

fn sample_nonce(value: u8) -> [u8; 16] {
    [value; 16]
}

fn sample_records() -> Vec<(LedgerRecord, u64, u64, usize)> {
    let store_uuid = sample_store_uuid();
    let incarnation = sample_incarnation();
    let ticket_id = sample_ticket();
    let canonical_path = sample_path("commitlog/00000000000000000000");
    let create_file_path =
        sample_path("commitlog/.create.i0000000000000007.s00000000000000000000.n11111111111111111111111111111111");
    let tombstone_path = sample_path(
        "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n22222222222222222222222222222222",
    );
    let unix_key = PhysicalFileKey::unix(0, 99);
    let windows_key = PhysicalFileKey::windows(7, [9; 16]);
    vec![
        (
            LedgerRecord::StoreInitialized {
                store_uuid,
                bootstrap_id: sample_nonce(1),
                creation_time_ns: 11,
            },
            1,
            0,
            64,
        ),
        (
            LedgerRecord::BootstrapInstalled {
                store_uuid,
                bootstrap_id: sample_nonce(1),
                snapshot_generation: 0,
                snapshot_base_sequence: 1,
                snapshot_file_length: 108,
                snapshot_file_crc32: 0x1234_5678,
                inventory_count: 3,
                create_high_water: 7,
                ticket_high_water: 42,
            },
            2,
            0,
            88,
        ),
        (
            LedgerRecord::LogOpened {
                store_uuid,
                generation: 3,
                snapshot_generation: 3,
                predecessor_log_generation: 2,
                predecessor_terminal_acknowledged_sequence: 20,
                snapshot_base_sequence: 20,
                snapshot_file_length: 108,
                snapshot_file_crc32: 0x1111_1111,
                predecessor_prefix_crc32: 0x2222_2222,
                validated_prefix_length: 1_000,
                unacknowledged_suffix_length: 0,
                unacknowledged_suffix_crc32: 0,
                open_reason: OpenReason::Compaction,
                predecessor_acknowledgement_epoch: 9,
            },
            21,
            3,
            104,
        ),
        (
            LedgerRecord::GenerationPrepared {
                store_uuid,
                source_generation: 2,
                target_generation: 3,
                target_snapshot_generation: 3,
                open_reason: OpenReason::Compaction,
            },
            10,
            2,
            56,
        ),
        (
            LedgerRecord::GenerationAborted {
                store_uuid,
                source_generation: 2,
                target_generation: 3,
                prepared_sequence: 10,
                abort_reason: GenerationAbortReason::Io,
            },
            11,
            2,
            48,
        ),
        (
            LedgerRecord::MarkerCommitted {
                store_uuid,
                marker_epoch: 1,
                snapshot_generation: 0,
                log_generation: 0,
                anchor_sequence: 2,
                slot_index: 0,
                slot_crc32: 0x3344_5566,
            },
            3,
            0,
            56,
        ),
        (
            LedgerRecord::AllocateIncarnation {
                incarnation,
                segment_offset: 0,
                expected_length: 1_024,
                create_nonce: sample_nonce(1),
                canonical_path: canonical_path.clone(),
                create_file_path: create_file_path.clone(),
            },
            100,
            3,
            56 + 2 + canonical_path.as_bytes().len() + 2 + create_file_path.as_bytes().len(),
        ),
        (
            LedgerRecord::BindIncarnation {
                incarnation,
                expected_length: 1_024,
                physical_key: unix_key,
                canonical_path: canonical_path.clone(),
                create_file_path: create_file_path.clone(),
            },
            100,
            3,
            64 + 2 + canonical_path.as_bytes().len() + 2 + create_file_path.as_bytes().len(),
        ),
        (
            LedgerRecord::PublishIncarnation {
                incarnation,
                expected_length: 1_024,
                physical_key: windows_key,
                canonical_path: canonical_path.clone(),
                create_file_path: create_file_path.clone(),
            },
            100,
            3,
            64 + 2 + canonical_path.as_bytes().len() + 2 + create_file_path.as_bytes().len(),
        ),
        (
            LedgerRecord::RetirementIntent {
                ticket_id,
                incarnation,
                reason: RetirementReason::TtlExpired,
                mapping_generation: 3,
                segment_offset: 0,
                expected_length: 1_024,
                retirement_nonce: sample_nonce(2),
                target_key: unix_key,
                canonical_path: canonical_path.clone(),
            },
            100,
            3,
            108 + 2 + canonical_path.as_bytes().len(),
        ),
        (
            LedgerRecord::LogicalRemoved {
                ticket_id,
                incarnation,
                target_key: unix_key,
                canonical_path: canonical_path.clone(),
            },
            100,
            3,
            64 + 2 + canonical_path.as_bytes().len(),
        ),
        (
            LedgerRecord::Tombstoned {
                ticket_id,
                incarnation,
                target_key: unix_key,
                retirement_nonce: sample_nonce(2),
                canonical_path: canonical_path.clone(),
                tombstone_path: tombstone_path.clone(),
            },
            100,
            3,
            80 + 2 + canonical_path.as_bytes().len() + 2 + tombstone_path.as_bytes().len(),
        ),
        (
            LedgerRecord::NamespaceAbsent {
                ticket_id,
                incarnation,
                replacement_observed: true,
                observation_time_ns: 55,
                target_key: unix_key,
                canonical_path: canonical_path.clone(),
                tombstone_path: Some(tombstone_path.clone()),
            },
            100,
            3,
            76 + 2 + canonical_path.as_bytes().len() + 2 + tombstone_path.as_bytes().len(),
        ),
        (
            LedgerRecord::Completed {
                ticket_id,
                incarnation,
                completion_time_ns: 66,
                namespace_absent_sequence: 99,
            },
            100,
            3,
            56,
        ),
        (
            LedgerRecord::SupersededPath {
                ticket_id,
                incarnation,
                expected_target_key: unix_key,
                observed_replacement_key: windows_key,
                canonical_path: canonical_path.clone(),
            },
            100,
            3,
            96 + 2 + canonical_path.as_bytes().len(),
        ),
        (
            LedgerRecord::Quarantined {
                entity_kind: QuarantineEntityKind::Tombstone,
                reason: QuarantineReason::KeyMismatch,
                sequence_at_observation: 77,
                physical_key: Some(windows_key),
                content_fingerprint: Some(ContentFingerprint {
                    length: 1_024,
                    crc32: 0x1234_5678,
                }),
                source_path: tombstone_path.clone(),
                destination_path: Some(sample_path(".rocketmq-lifecycle/quarantine/tombstone.bin")),
            },
            100,
            3,
            60 + 2 + tombstone_path.as_bytes().len() + 2 + ".rocketmq-lifecycle/quarantine/tombstone.bin".len(),
        ),
    ]
}

fn encoded_sample(record_type: RecordType) -> Vec<u8> {
    let (record, sequence, generation, _) = sample_records()
        .into_iter()
        .find(|(record, _, _, _)| record.record_type() == record_type)
        .expect("sample exists for every assigned record type");
    encode_ledger_frame(&record, sequence, generation).expect("sample record encodes")
}

fn payload_of(frame: &[u8]) -> &[u8] {
    let header_length = usize::from(u16::from_le_bytes([frame[14], frame[15]]));
    let payload_length = u32::from_le_bytes(frame[16..20].try_into().expect("payload length field")) as usize;
    &frame[header_length..header_length + payload_length]
}

fn mutate_payload(frame: &mut [u8], payload_offset: usize, replacement: &[u8]) {
    let header_length = usize::from(u16::from_le_bytes([frame[14], frame[15]]));
    let payload_length = u32::from_le_bytes(frame[16..20].try_into().expect("payload length field")) as usize;
    let start = header_length + payload_offset;
    frame[start..start + replacement.len()].copy_from_slice(replacement);
    let payload_crc = crc32(&frame[header_length..header_length + payload_length]);
    frame[header_length + payload_length..header_length + payload_length + 4]
        .copy_from_slice(&payload_crc.to_le_bytes());
}

fn decode_typed(frame: &[u8], sequence: u64, generation: u64) -> Result<LedgerRecord, CodecError> {
    let DecodeOutcome::Frame(frame) = decode_next_frame(frame, sequence, generation)? else {
        return Err(CodecError::InvalidEnvelopeRelationship {
            detail: "test expected a complete frame",
        });
    };
    frame.decode_record()?.ok_or(CodecError::InvalidEnvelopeRelationship {
        detail: "test expected a known record",
    })
}
