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

use super::super::*;
use super::encoded_sample;
use super::mutate_payload;
use super::COMPLETED_FRAME;

#[test]
fn unknown_noncritical_extended_header_uses_its_actual_payload_offset() {
    let payload = [9, 8, 7];
    let encoded = encode_unknown_noncritical(&[0xa5; 8], &payload, 100, 2);
    let DecodeOutcome::Frame(frame) = decode_next_frame(&encoded, 100, 2).expect("bounded extended header decodes")
    else {
        panic!("complete extended frame must decode");
    };
    assert_eq!(frame.record_type(), RecordType::Unknown(0x7777));
    assert_eq!(frame.payload(), payload);
    assert_eq!(frame.encoded_len(), 48 + payload.len() + 4);

    let maximum = encode_unknown_noncritical(
        &[0x5a; MAX_HEADER_LENGTH - MIN_HEADER_LENGTH],
        &[7; MAX_PAYLOAD_LENGTH],
        9,
        4,
    );
    assert_eq!(maximum.len(), MAX_FRAME_LENGTH);
    let DecodeOutcome::Frame(maximum_frame) = decode_next_frame(&maximum, 9, 4).expect("maximum bounded frame decodes")
    else {
        panic!("maximum frame must be complete");
    };
    assert_eq!(maximum_frame.payload().len(), MAX_PAYLOAD_LENGTH);
    assert_eq!(maximum_frame.encoded_len(), MAX_FRAME_LENGTH);
}

#[test]
fn known_headers_are_exactly_40_and_unknown_headers_are_at_most_256() {
    let mut known_with_extension = encode_unknown_noncritical(&[0; 8], &[1], 100, 2);
    known_with_extension[8..10].copy_from_slice(&RecordType::Completed.wire_value().to_le_bytes());
    known_with_extension[12..14].copy_from_slice(&CRITICAL_FLAG.to_le_bytes());
    rewrite_header_crc(&mut known_with_extension);
    assert_eq!(
        decode_next_frame(&known_with_extension, 100, 2),
        Err(CodecError::InvalidHeaderLength {
            length: 48,
            minimum: MIN_HEADER_LENGTH,
            maximum: MIN_HEADER_LENGTH,
        })
    );

    let overlong = encode_unknown_noncritical(&[0; MAX_HEADER_LENGTH - MIN_HEADER_LENGTH + 1], &[], 100, 2);
    assert_eq!(
        decode_next_frame(&overlong, 100, 2),
        Err(CodecError::InvalidHeaderLength {
            length: MAX_HEADER_LENGTH + 1,
            minimum: MIN_HEADER_LENGTH,
            maximum: MAX_HEADER_LENGTH,
        })
    );
}

#[test]
fn every_golden_cut_is_partial_but_impossible_partial_fields_are_corruption() {
    for length in 1..COMPLETED_FRAME.len() {
        let outcome = decode_next_frame(&COMPLETED_FRAME[..length], 100, 2);
        assert!(
            matches!(
                outcome,
                Ok(DecodeOutcome::TrailingPartial(TrailingPartial { available, .. }))
                    if available == length
            ),
            "length={length}, outcome={outcome:?}"
        );
    }

    let mut bad_record_version = COMPLETED_FRAME[..11].to_vec();
    bad_record_version[10] = 2;
    assert_eq!(
        decode_next_frame(&bad_record_version, 100, 2),
        Err(CodecError::InvalidHeaderPrefix { offset: 10 })
    );

    let mut bad_flags = COMPLETED_FRAME[..13].to_vec();
    bad_flags[12] = 0;
    assert_eq!(
        decode_next_frame(&bad_flags, 100, 2),
        Err(CodecError::InvalidHeaderPrefix { offset: 12 })
    );

    let mut bad_header_length = COMPLETED_FRAME[..15].to_vec();
    bad_header_length[14] = 39;
    assert_eq!(
        decode_next_frame(&bad_header_length, 100, 2),
        Err(CodecError::InvalidHeaderPrefix { offset: 14 })
    );

    let mut oversized_partial_payload = COMPLETED_FRAME[..18].to_vec();
    oversized_partial_payload[16..18].copy_from_slice(&16_385_u16.to_le_bytes());
    assert_eq!(
        decode_next_frame(&oversized_partial_payload, 100, 2),
        Err(CodecError::PayloadTooLarge {
            length: MAX_PAYLOAD_LENGTH + 1,
            maximum: MAX_PAYLOAD_LENGTH,
        })
    );
}

#[test]
fn known_declared_payload_lengths_are_rejected_before_payload_arrives() {
    let mut completed = COMPLETED_FRAME[..MIN_HEADER_LENGTH].to_vec();
    completed[16..20].copy_from_slice(&57_u32.to_le_bytes());
    rewrite_header_crc(&mut completed);
    assert_eq!(
        decode_next_frame(&completed, 100, 2),
        Err(CodecError::InvalidPayloadLength {
            record_type: RecordType::Completed.wire_value(),
            expected: 56,
            actual: 57,
        })
    );

    for invalid_length in [61_u32, 8_253] {
        let mut allocate = encoded_sample(RecordType::AllocateIncarnation);
        allocate.truncate(MIN_HEADER_LENGTH);
        allocate[16..20].copy_from_slice(&invalid_length.to_le_bytes());
        rewrite_header_crc(&mut allocate);
        assert_eq!(
            decode_next_frame(&allocate, 100, 3),
            Err(CodecError::InvalidVariablePayloadLength {
                record_type: RecordType::AllocateIncarnation.wire_value(),
                minimum: 62,
                maximum: 8_252,
                actual: invalid_length as usize,
            })
        );
    }
}

#[test]
fn impossible_known_payload_prefix_fields_are_corruption_not_partial() {
    let mut invalid_enum = encoded_sample(RecordType::RetirementIntent);
    mutate_payload(&mut invalid_enum, 32, &10_u16.to_le_bytes());
    invalid_enum.truncate(MIN_HEADER_LENGTH + 34);
    assert_eq!(
        decode_next_frame(&invalid_enum, 100, 3),
        Err(CodecError::InvalidEnumValue {
            field: "retirement_reason",
            value: 10,
        })
    );

    let mut nonzero_reserved = encoded_sample(RecordType::RetirementIntent);
    mutate_payload(&mut nonzero_reserved, 34, &1_u16.to_le_bytes());
    nonzero_reserved.truncate(MIN_HEADER_LENGTH + 36);
    assert_eq!(
        decode_next_frame(&nonzero_reserved, 100, 3),
        Err(CodecError::NonZeroReserved {
            field: "retirement_intent_flags",
            value: 1,
        })
    );

    let mut zero_ticket = COMPLETED_FRAME.to_vec();
    mutate_payload(&mut zero_ticket, 0, &[0; 8]);
    zero_ticket.truncate(MIN_HEADER_LENGTH + 8);
    assert!(matches!(
        decode_next_frame(&zero_ticket, 100, 2),
        Err(CodecError::InvalidIdentity { field: "ticket_id", .. })
    ));

    let mut impossible_path_length = encoded_sample(RecordType::AllocateIncarnation);
    mutate_payload(&mut impossible_path_length, 56, &4_097_u16.to_le_bytes());
    impossible_path_length.truncate(MIN_HEADER_LENGTH + 58);
    assert_eq!(
        decode_next_frame(&impossible_path_length, 100, 3),
        Err(CodecError::PayloadTooLarge {
            length: 4_097,
            maximum: 4_096,
        })
    );
}

#[test]
fn partial_admin_records_reject_impossible_envelope_bindings() {
    let cases: [(RecordType, u64, u64, &str); 4] = [
        (
            RecordType::StoreInitialized,
            2,
            0,
            "StoreInitialized must be sequence 1 in generation 0",
        ),
        (
            RecordType::StoreInitialized,
            1,
            1,
            "StoreInitialized must be sequence 1 in generation 0",
        ),
        (
            RecordType::BootstrapInstalled,
            3,
            0,
            "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
        ),
        (
            RecordType::BootstrapInstalled,
            2,
            1,
            "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
        ),
    ];

    for (record_type, sequence, generation, detail) in cases {
        let mut partial = encoded_sample(record_type);
        partial[20..28].copy_from_slice(&sequence.to_le_bytes());
        partial[28..36].copy_from_slice(&generation.to_le_bytes());
        rewrite_header_crc(&mut partial);
        partial.truncate(MIN_HEADER_LENGTH);

        assert_eq!(
            decode_next_frame(&partial, sequence, generation),
            Err(CodecError::InvalidEnvelopeRelationship { detail }),
            "record_type={record_type:?}",
        );
    }

    let detail = "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2";
    for available_payload in 33..=40 {
        let mut wrong_snapshot_generation = encoded_sample(RecordType::BootstrapInstalled);
        mutate_payload(&mut wrong_snapshot_generation, 32, &1_u64.to_le_bytes());
        wrong_snapshot_generation.truncate(MIN_HEADER_LENGTH + available_payload);
        let result = decode_next_frame(&wrong_snapshot_generation, 2, 0);
        assert!(
            result.is_err(),
            "available_payload={available_payload}, result={result:?}"
        );
    }

    for available_payload in 41..=48 {
        let mut wrong_snapshot_base = encoded_sample(RecordType::BootstrapInstalled);
        mutate_payload(&mut wrong_snapshot_base, 40, &2_u64.to_le_bytes());
        wrong_snapshot_base.truncate(MIN_HEADER_LENGTH + available_payload);
        let result = decode_next_frame(&wrong_snapshot_base, 2, 0);
        assert!(
            result.is_err(),
            "available_payload={available_payload}, result={result:?}"
        );
    }

    let mut complete_prefix = encoded_sample(RecordType::BootstrapInstalled);
    mutate_payload(&mut complete_prefix, 40, &2_u64.to_le_bytes());
    complete_prefix.truncate(MIN_HEADER_LENGTH + 48);
    assert_eq!(
        decode_next_frame(&complete_prefix, 2, 0),
        Err(CodecError::InvalidEnvelopeRelationship { detail }),
    );
}

#[test]
fn partial_admin_headers_reject_impossible_envelopes_as_fields_become_available() {
    let cases = [
        (
            RecordType::StoreInitialized,
            1_u64,
            "StoreInitialized must be sequence 1 in generation 0",
        ),
        (
            RecordType::BootstrapInstalled,
            2_u64,
            "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
        ),
    ];

    for (record_type, required_sequence, detail) in cases {
        for cut in 10..MIN_HEADER_LENGTH {
            let invalid_sequence = required_sequence + 1;
            let mut partial = encoded_sample(record_type);
            partial.truncate(cut);

            assert_eq!(
                decode_next_frame(&partial, invalid_sequence, 0),
                Err(CodecError::InvalidEnvelopeRelationship { detail }),
                "record_type={record_type:?}, cut={cut}, field=sequence",
            );

            let mut partial = encoded_sample(record_type);
            partial.truncate(cut);

            assert_eq!(
                decode_next_frame(&partial, required_sequence, 1),
                Err(CodecError::InvalidEnvelopeRelationship { detail }),
                "record_type={record_type:?}, cut={cut}, field=log_generation",
            );
        }

        for cut in 21..=28 {
            let available_sequence_bytes = (cut - 20).min(8);
            let invalid_sequence = required_sequence ^ (1_u64 << ((available_sequence_bytes - 1) * 8));
            let mut partial = encoded_sample(record_type);
            partial[20..28].copy_from_slice(&invalid_sequence.to_le_bytes());
            partial.truncate(cut);

            assert!(
                matches!(
                    decode_next_frame(&partial, required_sequence, 0),
                    Err(CodecError::InvalidFieldPrefix { field: "sequence", .. })
                        | Err(CodecError::SequenceMismatch { .. })
                ),
                "record_type={record_type:?}, cut={cut}, field=encoded_sequence",
            );
        }

        for cut in 29..=36 {
            let available_generation_bytes = (cut - 28).min(8);
            let invalid_generation = 1_u64 << ((available_generation_bytes - 1) * 8);
            let mut partial = encoded_sample(record_type);
            partial[28..36].copy_from_slice(&invalid_generation.to_le_bytes());
            partial.truncate(cut);

            assert!(
                matches!(
                    decode_next_frame(&partial, required_sequence, 0),
                    Err(CodecError::InvalidFieldPrefix {
                        field: "log_generation",
                        ..
                    }) | Err(CodecError::LogGenerationMismatch { .. })
                ),
                "record_type={record_type:?}, cut={cut}, field=encoded_log_generation",
            );
        }
    }
}

#[test]
fn unknown_noncritical_partial_payload_remains_generically_extensible() {
    let unknown = encode_unknown_noncritical(&[], &[0xff, 0xff, 0xff], 100, 2);

    for cut in 10..MIN_HEADER_LENGTH {
        assert!(
            matches!(
                decode_next_frame(&unknown[..cut], 100, 2),
                Ok(DecodeOutcome::TrailingPartial(_))
            ),
            "cut={cut}",
        );
    }

    assert!(matches!(
        decode_next_frame(&unknown[..MIN_HEADER_LENGTH + 1], 100, 2),
        Ok(DecodeOutcome::TrailingPartial(TrailingPartial {
            available: 41,
            required: 47,
        }))
    ));
}

#[test]
fn record_type_version_flags_and_sequence_fail_closed() {
    let mut zero_type = COMPLETED_FRAME;
    zero_type[8..10].copy_from_slice(&0_u16.to_le_bytes());
    assert_eq!(
        decode_next_frame(&zero_type, 100, 2),
        Err(CodecError::InvalidRecordTypeZero)
    );

    let mut unsupported_known_version = COMPLETED_FRAME;
    unsupported_known_version[10..12].copy_from_slice(&2_u16.to_le_bytes());
    assert_eq!(
        decode_next_frame(&unsupported_known_version, 100, 2),
        Err(CodecError::UnsupportedRecordVersion {
            record_type: RecordType::Completed.wire_value(),
            version: 2,
        })
    );

    let mut invalid_unknown_flags = COMPLETED_FRAME;
    invalid_unknown_flags[8..10].copy_from_slice(&0x7777_u16.to_le_bytes());
    invalid_unknown_flags[12..14].copy_from_slice(&2_u16.to_le_bytes());
    assert_eq!(
        decode_next_frame(&invalid_unknown_flags, 100, 2),
        Err(CodecError::InvalidRecordFlags {
            record_type: 0x7777,
            flags: 2,
        })
    );

    assert_eq!(
        decode_next_frame(&COMPLETED_FRAME, 99, 2),
        Err(CodecError::SequenceMismatch {
            expected: 99,
            actual: 100,
        })
    );
    assert_eq!(
        decode_next_frame(&COMPLETED_FRAME, 101, 2),
        Err(CodecError::SequenceMismatch {
            expected: 101,
            actual: 100,
        })
    );
    assert_eq!(
        decode_next_frame(&COMPLETED_FRAME, 0, 2),
        Err(CodecError::ZeroExpectedSequence)
    );

    let maximum_sequence = encode_frame(
        RecordType::Completed,
        u64::MAX,
        2,
        &COMPLETED_FRAME[MIN_HEADER_LENGTH..COMPLETED_FRAME.len() - 4],
    )
    .expect("the final sequence itself remains encodable");
    let DecodeOutcome::Frame(frame) =
        decode_next_frame(&maximum_sequence, u64::MAX, 2).expect("maximum sequence decodes")
    else {
        panic!("maximum sequence frame must be complete");
    };
    assert_eq!(frame.next_sequence(), Err(CodecError::SequenceOverflow));
}

fn encode_unknown_noncritical(header_extension: &[u8], payload: &[u8], sequence: u64, log_generation: u64) -> Vec<u8> {
    let header_length = MIN_HEADER_LENGTH + header_extension.len();
    let payload_length = u32::try_from(payload.len()).expect("test payload length fits u32");
    let mut encoded = Vec::with_capacity(header_length + payload.len() + 4);
    encoded.extend_from_slice(&FRAME_MAGIC);
    encoded.extend_from_slice(&FORMAT_MAJOR.to_le_bytes());
    encoded.extend_from_slice(&FORMAT_MINOR.to_le_bytes());
    encoded.extend_from_slice(&0x7777_u16.to_le_bytes());
    encoded.extend_from_slice(&RECORD_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0_u16.to_le_bytes());
    encoded.extend_from_slice(
        &u16::try_from(header_length)
            .expect("test header length fits u16")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&payload_length.to_le_bytes());
    encoded.extend_from_slice(&sequence.to_le_bytes());
    encoded.extend_from_slice(&log_generation.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(header_extension);
    rewrite_header_crc(&mut encoded);
    encoded.extend_from_slice(payload);
    encoded.extend_from_slice(&crc32(payload).to_le_bytes());
    encoded
}

fn rewrite_header_crc(frame: &mut [u8]) {
    let header_length = usize::from(u16::from_le_bytes([frame[14], frame[15]]));
    let mut covered = Vec::from(&frame[..36]);
    covered.extend_from_slice(&frame[40..header_length]);
    frame[36..40].copy_from_slice(&crc32(&covered).to_le_bytes());
}
