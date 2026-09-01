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
use super::sample_incarnation;
use super::sample_nonce;
use super::sample_path;
use super::sample_records;
use super::sample_store_uuid;

#[test]
fn log_opened_uses_frozen_offsets_and_predecessor_acknowledgement_epoch() {
    let record = log_opened(OpenReason::Compaction, 0, 0, 9);
    let encoded = encode_ledger_frame(&record, 21, 3).expect("valid compacted generation anchor encodes");
    let payload = &encoded[MIN_HEADER_LENGTH..encoded.len() - 4];
    assert_eq!(payload.len(), 104);
    assert_eq!(read_u64(payload, 40), Some(20));
    assert_eq!(read_u64(payload, 48), Some(20));
    assert_eq!(read_u32(payload, 80), Some(0));
    assert_eq!(read_u32(payload, 84), Some(0));
    assert_eq!(payload[88], OpenReason::Compaction.wire_value());
    assert_eq!(read_u64(payload, 89), Some(9));
    assert_eq!(&payload[97..104], &[0; 7]);
    assert_eq!(decode_typed(&encoded, 21, 3), Ok(record));

    let tail_repair = log_opened(OpenReason::TailRepair, 17, 0x1234_5678, 10);
    assert_eq!(round_trip(tail_repair.clone(), 21, 3), tail_repair);
}

#[test]
fn log_opened_rejects_zero_or_exhausted_predecessor_acknowledgement_epoch() {
    assert_eq!(
        encode_ledger_frame(&log_opened(OpenReason::Compaction, 0, 0, 0), 21, 3),
        Err(CodecViolation::ZeroAcknowledgementEpoch)
    );
    assert_eq!(
        encode_ledger_frame(&log_opened(OpenReason::Compaction, 0, 0, u64::MAX), 21, 3,),
        Err(CodecViolation::AcknowledgementEpochOverflow)
    );
}

#[test]
fn bootstrap_and_marker_envelopes_are_exact_and_marker_slots_alternate() {
    let bootstrap = LedgerRecord::BootstrapInstalled {
        store_uuid: sample_store_uuid(),
        bootstrap_id: sample_nonce(1),
        snapshot_generation: 0,
        snapshot_base_sequence: 1,
        snapshot_file_length: 108,
        snapshot_file_crc32: 7,
        inventory_count: 0,
        create_high_water: 0,
        ticket_high_water: 0,
    };
    assert!(encode_ledger_frame(&bootstrap, 2, 0).is_ok());
    assert!(matches!(
        encode_ledger_frame(&bootstrap, 3, 0),
        Err(CodecViolation::InvalidEnvelopeRelationship { .. })
    ));

    let wrong_marker_slot = LedgerRecord::MarkerCommitted {
        store_uuid: sample_store_uuid(),
        marker_epoch: 2,
        snapshot_generation: 1,
        log_generation: 1,
        anchor_sequence: 20,
        slot_index: 0,
        slot_crc32: 7,
    };
    assert_eq!(
        encode_ledger_frame(&wrong_marker_slot, 21, 1),
        Err(CodecViolation::MarkerSlotParityMismatch {
            marker_epoch: 2,
            expected_slot_index: 1,
            actual_slot_index: 0,
        })
    );
}

#[test]
fn all_assigned_enum_values_round_trip_or_fail_when_reserved() {
    let retirement_reasons = [
        RetirementReason::TtlExpired,
        RetirementReason::OffsetTruncate,
        RetirementReason::Reset,
        RetirementReason::DeleteLast,
        RetirementReason::StoreDestroy,
        RetirementReason::AllocationOrphan,
        RetirementReason::TopicRetirement,
        RetirementReason::DerivedFileRetirement,
        RetirementReason::AuditedOperatorRequest,
    ];
    for reason in retirement_reasons {
        let mut record = sample_record(RecordType::RetirementIntent);
        let LedgerRecord::RetirementIntent {
            reason: record_reason, ..
        } = &mut record
        else {
            unreachable!("record type lookup is exact");
        };
        *record_reason = reason;
        assert_eq!(round_trip(record.clone(), 100, 3), record);
    }

    let abort_reasons = [
        GenerationAbortReason::Io,
        GenerationAbortReason::Space,
        GenerationAbortReason::OperatorCancellation,
        GenerationAbortReason::Validation,
    ];
    for reason in abort_reasons {
        let record = LedgerRecord::GenerationAborted {
            store_uuid: sample_store_uuid(),
            source_generation: 2,
            target_generation: 3,
            prepared_sequence: 99,
            abort_reason: reason,
        };
        assert_eq!(round_trip(record.clone(), 100, 2), record);
    }

    let entity_kinds = [
        QuarantineEntityKind::Create,
        QuarantineEntityKind::Tombstone,
        QuarantineEntityKind::Sidecar,
        QuarantineEntityKind::Canonical,
    ];
    for entity_kind in entity_kinds {
        let mut record = sample_record(RecordType::Quarantined);
        let LedgerRecord::Quarantined {
            entity_kind: record_kind,
            ..
        } = &mut record
        else {
            unreachable!("record type lookup is exact");
        };
        *record_kind = entity_kind;
        assert_eq!(round_trip(record.clone(), 100, 3), record);
    }

    let quarantine_reasons = [
        QuarantineReason::UnknownOwner,
        QuarantineReason::KeyMismatch,
        QuarantineReason::MalformedName,
        QuarantineReason::RestoreRebindRequired,
    ];
    for reason in quarantine_reasons {
        let mut record = sample_record(RecordType::Quarantined);
        let LedgerRecord::Quarantined {
            reason: record_reason, ..
        } = &mut record
        else {
            unreachable!("record type lookup is exact");
        };
        *record_reason = reason;
        assert_eq!(round_trip(record.clone(), 100, 3), record);
    }
}

#[test]
fn variable_records_accept_the_exact_4096_byte_path_boundary() {
    let maximum_path = std::iter::repeat_n("a".repeat(240), 17).collect::<Vec<_>>().join("/");
    assert_eq!(maximum_path.len(), 4096);
    let maximum_path = sample_path(&maximum_path);
    let record = LedgerRecord::AllocateIncarnation {
        incarnation: sample_incarnation(),
        segment_offset: 0,
        expected_length: 1,
        create_nonce: sample_nonce(1),
        canonical_path: maximum_path.clone(),
        create_file_path: maximum_path,
    };
    let encoded = encode_ledger_frame(&record, 100, 3).expect("maximum paths fit the record bound");
    assert_eq!(read_u32(&encoded, 16), Some(8_252));
    assert_eq!(decode_typed(&encoded, 100, 3), Ok(record));
}

#[test]
fn mapped_file_records_reject_zero_expected_length_and_mapping_generation() {
    for (mut record, sequence, generation, _) in sample_records() {
        let expected_length = match &mut record {
            LedgerRecord::AllocateIncarnation { expected_length, .. }
            | LedgerRecord::BindIncarnation { expected_length, .. }
            | LedgerRecord::PublishIncarnation { expected_length, .. }
            | LedgerRecord::RetirementIntent { expected_length, .. } => expected_length,
            _ => continue,
        };
        *expected_length = 0;
        assert_eq!(
            encode_ledger_frame(&record, sequence, generation),
            Err(CodecViolation::ZeroExpectedFileLength)
        );
    }

    let mut intent = sample_record(RecordType::RetirementIntent);
    let LedgerRecord::RetirementIntent { mapping_generation, .. } = &mut intent else {
        unreachable!("record type lookup is exact");
    };
    *mapping_generation = 0;
    assert_eq!(
        encode_ledger_frame(&intent, 100, 3),
        Err(CodecViolation::ZeroMappingGeneration)
    );
}

fn log_opened(
    open_reason: OpenReason,
    unacknowledged_suffix_length: u32,
    unacknowledged_suffix_crc32: u32,
    predecessor_acknowledgement_epoch: u64,
) -> LedgerRecord {
    LedgerRecord::LogOpened {
        store_uuid: sample_store_uuid(),
        generation: 3,
        snapshot_generation: 3,
        predecessor_log_generation: 2,
        predecessor_terminal_acknowledged_sequence: 20,
        snapshot_base_sequence: 20,
        snapshot_file_length: 108,
        snapshot_file_crc32: 0x1111_1111,
        predecessor_prefix_crc32: 0x2222_2222,
        validated_prefix_length: 1_000,
        unacknowledged_suffix_length,
        unacknowledged_suffix_crc32,
        open_reason,
        predecessor_acknowledgement_epoch,
    }
}

fn sample_record(record_type: RecordType) -> LedgerRecord {
    sample_records()
        .into_iter()
        .find_map(|(record, _, _, _)| (record.record_type() == record_type).then_some(record))
        .expect("every assigned record type has a sample")
}

fn round_trip(record: LedgerRecord, sequence: u64, generation: u64) -> LedgerRecord {
    let encoded = encode_ledger_frame(&record, sequence, generation).expect("record encodes");
    decode_typed(&encoded, sequence, generation).expect("record decodes")
}

fn decode_typed(frame: &[u8], sequence: u64, generation: u64) -> Result<LedgerRecord, CodecViolation> {
    let DecodeOutcome::Frame(frame) = decode_next_frame(frame, sequence, generation)? else {
        return Err(CodecViolation::InvalidEnvelopeRelationship {
            detail: "test expected a complete frame",
        });
    };
    frame
        .decode_record()?
        .ok_or(CodecViolation::InvalidEnvelopeRelationship {
            detail: "test expected a known record",
        })
}
