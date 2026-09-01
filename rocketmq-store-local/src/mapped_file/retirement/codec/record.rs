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

use super::payload::push_incarnation;
use super::payload::push_optional_path;
use super::payload::push_optional_physical_key;
use super::payload::push_path;
use super::payload::push_physical_key;
use super::payload::push_store_uuid;
use super::payload::push_ticket;
use super::payload::push_u16;
use super::payload::push_u32;
use super::payload::push_u64;
use super::payload::validate_opaque_id;
use super::payload::PayloadDecoder;
use super::schema::validate_known_payload_length;
use super::semantics::validate_envelope_relationships;
use super::CodecViolation;
use super::ContentFingerprint;
use super::GenerationAbortReason;
use super::LedgerRecord;
use super::OpenReason;
use super::QuarantineEntityKind;
use super::QuarantineReason;
use super::RecordType;
use super::RetirementReason;
use super::MAX_PAYLOAD_LENGTH;

pub(super) fn encode_record_payload(record: &LedgerRecord, sequence: u64) -> Result<Vec<u8>, CodecViolation> {
    let mut output = Vec::new();
    match record {
        LedgerRecord::StoreInitialized {
            store_uuid,
            bootstrap_id,
            creation_time_ns,
        } => {
            validate_opaque_id("bootstrap_id", bootstrap_id)?;
            push_store_uuid(&mut output, *store_uuid);
            output.extend_from_slice(bootstrap_id);
            push_u64(&mut output, *creation_time_ns);
            push_u64(&mut output, 0);
            push_u64(&mut output, 0);
            push_u64(&mut output, 1);
        }
        LedgerRecord::BootstrapInstalled {
            store_uuid,
            bootstrap_id,
            snapshot_generation,
            snapshot_base_sequence,
            snapshot_file_length,
            snapshot_file_crc32,
            inventory_count,
            create_high_water,
            ticket_high_water,
        } => {
            validate_opaque_id("bootstrap_id", bootstrap_id)?;
            push_store_uuid(&mut output, *store_uuid);
            output.extend_from_slice(bootstrap_id);
            push_u64(&mut output, *snapshot_generation);
            push_u64(&mut output, *snapshot_base_sequence);
            push_u64(&mut output, *snapshot_file_length);
            push_u32(&mut output, *snapshot_file_crc32);
            push_u32(&mut output, 0);
            push_u64(&mut output, *inventory_count);
            push_u64(&mut output, *create_high_water);
            push_u64(&mut output, *ticket_high_water);
        }
        LedgerRecord::GenerationPrepared {
            store_uuid,
            source_generation,
            target_generation,
            target_snapshot_generation,
            open_reason,
        } => {
            push_store_uuid(&mut output, *store_uuid);
            push_u64(&mut output, *source_generation);
            push_u64(&mut output, *target_generation);
            push_u64(&mut output, *target_snapshot_generation);
            push_u64(&mut output, sequence);
            output.push(open_reason.wire_value());
            output.extend_from_slice(&[0; 7]);
        }
        LedgerRecord::GenerationAborted {
            store_uuid,
            source_generation,
            target_generation,
            prepared_sequence,
            abort_reason,
        } => {
            push_store_uuid(&mut output, *store_uuid);
            push_u64(&mut output, *source_generation);
            push_u64(&mut output, *target_generation);
            push_u64(&mut output, *prepared_sequence);
            push_u32(&mut output, abort_reason.wire_value());
            push_u32(&mut output, 0);
        }
        LedgerRecord::MarkerCommitted {
            store_uuid,
            marker_epoch,
            snapshot_generation,
            log_generation,
            anchor_sequence,
            slot_index,
            slot_crc32,
        } => {
            push_store_uuid(&mut output, *store_uuid);
            push_u64(&mut output, *marker_epoch);
            push_u64(&mut output, *snapshot_generation);
            push_u64(&mut output, *log_generation);
            push_u64(&mut output, *anchor_sequence);
            output.push(*slot_index);
            output.extend_from_slice(&[0; 3]);
            push_u32(&mut output, *slot_crc32);
        }
        LedgerRecord::LogOpened {
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
        } => {
            push_store_uuid(&mut output, *store_uuid);
            push_u64(&mut output, *generation);
            push_u64(&mut output, *snapshot_generation);
            push_u64(&mut output, *predecessor_log_generation);
            push_u64(&mut output, *predecessor_terminal_acknowledged_sequence);
            push_u64(&mut output, *snapshot_base_sequence);
            push_u64(&mut output, *snapshot_file_length);
            push_u32(&mut output, *snapshot_file_crc32);
            push_u32(&mut output, *predecessor_prefix_crc32);
            push_u64(&mut output, *validated_prefix_length);
            push_u32(&mut output, *unacknowledged_suffix_length);
            push_u32(&mut output, *unacknowledged_suffix_crc32);
            output.push(open_reason.wire_value());
            push_u64(&mut output, *predecessor_acknowledgement_epoch);
            output.extend_from_slice(&[0; 7]);
        }
        LedgerRecord::AllocateIncarnation {
            incarnation,
            segment_offset,
            expected_length,
            create_nonce,
            canonical_path,
            create_file_path,
        } => {
            validate_expected_length(*expected_length)?;
            validate_opaque_id("create_nonce", create_nonce)?;
            push_incarnation(&mut output, *incarnation);
            push_u64(&mut output, *segment_offset);
            push_u64(&mut output, *expected_length);
            output.extend_from_slice(create_nonce);
            push_path(&mut output, canonical_path)?;
            push_path(&mut output, create_file_path)?;
        }
        LedgerRecord::BindIncarnation {
            incarnation,
            expected_length,
            physical_key,
            canonical_path,
            create_file_path,
        }
        | LedgerRecord::PublishIncarnation {
            incarnation,
            expected_length,
            physical_key,
            canonical_path,
            create_file_path,
        } => {
            validate_expected_length(*expected_length)?;
            push_incarnation(&mut output, *incarnation);
            push_u64(&mut output, *expected_length);
            push_physical_key(&mut output, *physical_key);
            push_path(&mut output, canonical_path)?;
            push_path(&mut output, create_file_path)?;
        }
        LedgerRecord::RetirementIntent {
            ticket_id,
            incarnation,
            reason,
            mapping_generation,
            segment_offset,
            expected_length,
            retirement_nonce,
            target_key,
            canonical_path,
        } => {
            if *mapping_generation == 0 {
                return Err(CodecViolation::ZeroMappingGeneration);
            }
            validate_expected_length(*expected_length)?;
            validate_opaque_id("retirement_nonce", retirement_nonce)?;
            push_ticket(&mut output, *ticket_id);
            push_incarnation(&mut output, *incarnation);
            push_u16(&mut output, reason.wire_value());
            push_u16(&mut output, 0);
            push_u64(&mut output, *mapping_generation);
            push_u64(&mut output, *segment_offset);
            push_u64(&mut output, *expected_length);
            output.extend_from_slice(retirement_nonce);
            push_physical_key(&mut output, *target_key);
            push_path(&mut output, canonical_path)?;
        }
        LedgerRecord::LogicalRemoved {
            ticket_id,
            incarnation,
            target_key,
            canonical_path,
        } => {
            push_ticket(&mut output, *ticket_id);
            push_incarnation(&mut output, *incarnation);
            push_physical_key(&mut output, *target_key);
            push_path(&mut output, canonical_path)?;
        }
        LedgerRecord::Tombstoned {
            ticket_id,
            incarnation,
            target_key,
            retirement_nonce,
            canonical_path,
            tombstone_path,
        } => {
            validate_opaque_id("retirement_nonce", retirement_nonce)?;
            push_ticket(&mut output, *ticket_id);
            push_incarnation(&mut output, *incarnation);
            push_physical_key(&mut output, *target_key);
            output.extend_from_slice(retirement_nonce);
            push_path(&mut output, canonical_path)?;
            push_path(&mut output, tombstone_path)?;
        }
        LedgerRecord::NamespaceAbsent {
            ticket_id,
            incarnation,
            replacement_observed,
            observation_time_ns,
            target_key,
            canonical_path,
            tombstone_path,
        } => {
            push_ticket(&mut output, *ticket_id);
            push_incarnation(&mut output, *incarnation);
            let proof_flags = 0x0003 | if *replacement_observed { 0x0004 } else { 0 };
            push_u16(&mut output, proof_flags);
            push_u16(&mut output, 0);
            push_u64(&mut output, *observation_time_ns);
            push_physical_key(&mut output, *target_key);
            push_path(&mut output, canonical_path)?;
            push_optional_path(&mut output, tombstone_path.as_ref())?;
        }
        LedgerRecord::Completed {
            ticket_id,
            incarnation,
            completion_time_ns,
            namespace_absent_sequence,
        } => {
            push_ticket(&mut output, *ticket_id);
            push_incarnation(&mut output, *incarnation);
            push_u64(&mut output, *completion_time_ns);
            push_u64(&mut output, *namespace_absent_sequence);
            push_u32(&mut output, 0x0000_0003);
            push_u32(&mut output, 0);
        }
        LedgerRecord::SupersededPath {
            ticket_id,
            incarnation,
            expected_target_key,
            observed_replacement_key,
            canonical_path,
        } => {
            push_ticket(&mut output, *ticket_id);
            push_incarnation(&mut output, *incarnation);
            push_physical_key(&mut output, *expected_target_key);
            push_physical_key(&mut output, *observed_replacement_key);
            push_path(&mut output, canonical_path)?;
        }
        LedgerRecord::Quarantined {
            entity_kind,
            reason,
            sequence_at_observation,
            physical_key,
            content_fingerprint,
            source_path,
            destination_path,
        } => {
            output.push(entity_kind.wire_value());
            output.push(reason.wire_value());
            let mut flags = 0_u16;
            if physical_key.is_some() {
                flags |= 0x0001;
            }
            if content_fingerprint.is_some() {
                flags |= 0x0002;
            }
            if destination_path.is_some() {
                flags |= 0x0004;
            }
            push_u16(&mut output, flags);
            push_u64(&mut output, *sequence_at_observation);
            push_optional_physical_key(&mut output, *physical_key);
            if let Some(fingerprint) = content_fingerprint {
                push_u64(&mut output, fingerprint.length);
                push_u32(&mut output, fingerprint.crc32);
            } else {
                push_u64(&mut output, 0);
                push_u32(&mut output, 0);
            }
            push_u32(&mut output, 0);
            push_path(&mut output, source_path)?;
            push_optional_path(&mut output, destination_path.as_ref())?;
        }
    }
    if output.len() > MAX_PAYLOAD_LENGTH {
        return Err(CodecViolation::PayloadTooLarge {
            length: output.len(),
            maximum: MAX_PAYLOAD_LENGTH,
        });
    }
    Ok(output)
}

pub(super) fn decode_record_payload(
    record_type: RecordType,
    sequence: u64,
    log_generation: u64,
    payload: &[u8],
) -> Result<LedgerRecord, CodecViolation> {
    validate_known_payload_length(record_type, payload.len())?;
    let mut decoder = PayloadDecoder::new(record_type, payload);
    let record = match record_type {
        RecordType::StoreInitialized => {
            let store_uuid = decoder.take_store_uuid("store_uuid")?;
            let bootstrap_id = decoder.take_opaque_id("bootstrap_id")?;
            let creation_time_ns = decoder.take_u64()?;
            decoder.require_u64("initial_snapshot_generation", 0)?;
            decoder.require_u64("initial_log_generation", 0)?;
            decoder.require_u64("feature_bitmap", 1)?;
            LedgerRecord::StoreInitialized {
                store_uuid,
                bootstrap_id,
                creation_time_ns,
            }
        }
        RecordType::BootstrapInstalled => {
            let store_uuid = decoder.take_store_uuid("store_uuid")?;
            let bootstrap_id = decoder.take_opaque_id("bootstrap_id")?;
            let snapshot_generation = decoder.take_u64()?;
            let snapshot_base_sequence = decoder.take_u64()?;
            let snapshot_file_length = decoder.take_u64()?;
            let snapshot_file_crc32 = decoder.take_u32()?;
            decoder.require_u32("bootstrap_reserved", 0)?;
            let inventory_count = decoder.take_u64()?;
            let create_high_water = decoder.take_u64()?;
            let ticket_high_water = decoder.take_u64()?;
            LedgerRecord::BootstrapInstalled {
                store_uuid,
                bootstrap_id,
                snapshot_generation,
                snapshot_base_sequence,
                snapshot_file_length,
                snapshot_file_crc32,
                inventory_count,
                create_high_water,
                ticket_high_water,
            }
        }
        RecordType::LogOpened => {
            let store_uuid = decoder.take_store_uuid("store_uuid")?;
            let generation = decoder.take_u64()?;
            let snapshot_generation = decoder.take_u64()?;
            let predecessor_log_generation = decoder.take_u64()?;
            let predecessor_terminal_acknowledged_sequence = decoder.take_u64()?;
            let snapshot_base_sequence = decoder.take_u64()?;
            let snapshot_file_length = decoder.take_u64()?;
            let snapshot_file_crc32 = decoder.take_u32()?;
            let predecessor_prefix_crc32 = decoder.take_u32()?;
            let validated_prefix_length = decoder.take_u64()?;
            let unacknowledged_suffix_length = decoder.take_u32()?;
            let unacknowledged_suffix_crc32 = decoder.take_u32()?;
            let open_reason = OpenReason::from_wire(decoder.take_u8()?)?;
            let predecessor_acknowledgement_epoch = decoder.take_u64()?;
            decoder.require_zero_bytes("log_opened_reserved", 7)?;
            LedgerRecord::LogOpened {
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
            }
        }
        RecordType::GenerationPrepared => {
            let store_uuid = decoder.take_store_uuid("store_uuid")?;
            let source_generation = decoder.take_u64()?;
            let target_generation = decoder.take_u64()?;
            let target_snapshot_generation = decoder.take_u64()?;
            let repeated_sequence = decoder.take_u64()?;
            if repeated_sequence != sequence {
                return Err(CodecViolation::InvalidEnvelopeRelationship {
                    detail: "GenerationPrepared repeated sequence differs from its frame",
                });
            }
            let open_reason = OpenReason::from_wire(decoder.take_u8()?)?;
            decoder.require_zero_bytes("generation_prepared_reserved", 7)?;
            LedgerRecord::GenerationPrepared {
                store_uuid,
                source_generation,
                target_generation,
                target_snapshot_generation,
                open_reason,
            }
        }
        RecordType::GenerationAborted => {
            let store_uuid = decoder.take_store_uuid("store_uuid")?;
            let source_generation = decoder.take_u64()?;
            let target_generation = decoder.take_u64()?;
            let prepared_sequence = decoder.take_u64()?;
            let abort_reason = GenerationAbortReason::from_wire(decoder.take_u32()?)?;
            decoder.require_u32("generation_aborted_reserved", 0)?;
            LedgerRecord::GenerationAborted {
                store_uuid,
                source_generation,
                target_generation,
                prepared_sequence,
                abort_reason,
            }
        }
        RecordType::MarkerCommitted => {
            let store_uuid = decoder.take_store_uuid("store_uuid")?;
            let marker_epoch = decoder.take_u64()?;
            let snapshot_generation = decoder.take_u64()?;
            let selected_log_generation = decoder.take_u64()?;
            let anchor_sequence = decoder.take_u64()?;
            let slot_index = decoder.take_u8()?;
            decoder.require_zero_bytes("marker_committed_reserved", 3)?;
            let slot_crc32 = decoder.take_u32()?;
            LedgerRecord::MarkerCommitted {
                store_uuid,
                marker_epoch,
                snapshot_generation,
                log_generation: selected_log_generation,
                anchor_sequence,
                slot_index,
                slot_crc32,
            }
        }
        RecordType::AllocateIncarnation => {
            let incarnation = decoder.take_incarnation()?;
            let segment_offset = decoder.take_u64()?;
            let expected_length = decoder.take_u64()?;
            validate_expected_length(expected_length)?;
            let create_nonce = decoder.take_opaque_id("create_nonce")?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            let create_file_path = decoder.take_required_path("create_file_path")?;
            LedgerRecord::AllocateIncarnation {
                incarnation,
                segment_offset,
                expected_length,
                create_nonce,
                canonical_path,
                create_file_path,
            }
        }
        RecordType::BindIncarnation | RecordType::PublishIncarnation => {
            let incarnation = decoder.take_incarnation()?;
            let expected_length = decoder.take_u64()?;
            validate_expected_length(expected_length)?;
            let physical_key = decoder.take_physical_key()?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            let create_file_path = decoder.take_required_path("create_file_path")?;
            if record_type == RecordType::BindIncarnation {
                LedgerRecord::BindIncarnation {
                    incarnation,
                    expected_length,
                    physical_key,
                    canonical_path,
                    create_file_path,
                }
            } else {
                LedgerRecord::PublishIncarnation {
                    incarnation,
                    expected_length,
                    physical_key,
                    canonical_path,
                    create_file_path,
                }
            }
        }
        RecordType::RetirementIntent => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let reason = RetirementReason::from_wire(decoder.take_u16()?)?;
            decoder.require_u16("retirement_intent_flags", 0)?;
            let mapping_generation = decoder.take_u64()?;
            if mapping_generation == 0 {
                return Err(CodecViolation::ZeroMappingGeneration);
            }
            let segment_offset = decoder.take_u64()?;
            let expected_length = decoder.take_u64()?;
            validate_expected_length(expected_length)?;
            let retirement_nonce = decoder.take_opaque_id("retirement_nonce")?;
            let target_key = decoder.take_physical_key()?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            LedgerRecord::RetirementIntent {
                ticket_id,
                incarnation,
                reason,
                mapping_generation,
                segment_offset,
                expected_length,
                retirement_nonce,
                target_key,
                canonical_path,
            }
        }
        RecordType::LogicalRemoved => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let target_key = decoder.take_physical_key()?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            LedgerRecord::LogicalRemoved {
                ticket_id,
                incarnation,
                target_key,
                canonical_path,
            }
        }
        RecordType::Tombstoned => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let target_key = decoder.take_physical_key()?;
            let retirement_nonce = decoder.take_opaque_id("retirement_nonce")?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            let tombstone_path = decoder.take_required_path("tombstone_path")?;
            LedgerRecord::Tombstoned {
                ticket_id,
                incarnation,
                target_key,
                retirement_nonce,
                canonical_path,
                tombstone_path,
            }
        }
        RecordType::NamespaceAbsent => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let proof_flags = decoder.take_u16()?;
            if proof_flags & 0x0003 != 0x0003 || proof_flags & !0x0007 != 0 {
                return Err(CodecViolation::InvalidProofFlags {
                    field: "namespace_absent",
                    flags: u32::from(proof_flags),
                });
            }
            decoder.require_u16("namespace_absent_reserved", 0)?;
            let observation_time_ns = decoder.take_u64()?;
            let target_key = decoder.take_physical_key()?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            let tombstone_path = decoder.take_optional_path("tombstone_path")?;
            LedgerRecord::NamespaceAbsent {
                ticket_id,
                incarnation,
                replacement_observed: proof_flags & 0x0004 != 0,
                observation_time_ns,
                target_key,
                canonical_path,
                tombstone_path,
            }
        }
        RecordType::Completed => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let completion_time_ns = decoder.take_u64()?;
            let namespace_absent_sequence = decoder.take_u64()?;
            let proof_flags = decoder.take_u32()?;
            if proof_flags != 0x0000_0003 {
                return Err(CodecViolation::InvalidProofFlags {
                    field: "completed",
                    flags: proof_flags,
                });
            }
            decoder.require_u32("completed_reserved", 0)?;
            LedgerRecord::Completed {
                ticket_id,
                incarnation,
                completion_time_ns,
                namespace_absent_sequence,
            }
        }
        RecordType::SupersededPath => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let expected_target_key = decoder.take_physical_key()?;
            let observed_replacement_key = decoder.take_physical_key()?;
            let canonical_path = decoder.take_required_path("canonical_path")?;
            LedgerRecord::SupersededPath {
                ticket_id,
                incarnation,
                expected_target_key,
                observed_replacement_key,
                canonical_path,
            }
        }
        RecordType::Quarantined => {
            let entity_kind = QuarantineEntityKind::from_wire(decoder.take_u8()?)?;
            let reason = QuarantineReason::from_wire(decoder.take_u8()?)?;
            let flags = decoder.take_u16()?;
            if flags & !0x0007 != 0 {
                return Err(CodecViolation::InvalidQuarantineFlags { flags });
            }
            let sequence_at_observation = decoder.take_u64()?;
            let physical_key = decoder.take_optional_physical_key(flags & 0x0001 != 0)?;
            let content_length = decoder.take_u64()?;
            let content_crc32 = decoder.take_u32()?;
            let content_fingerprint = if flags & 0x0002 != 0 {
                Some(ContentFingerprint {
                    length: content_length,
                    crc32: content_crc32,
                })
            } else {
                if content_length != 0 {
                    return Err(CodecViolation::NonZeroReserved {
                        field: "absent_content_length",
                        value: content_length,
                    });
                }
                if content_crc32 != 0 {
                    return Err(CodecViolation::NonZeroReserved {
                        field: "absent_content_crc32",
                        value: u64::from(content_crc32),
                    });
                }
                None
            };
            decoder.require_u32("quarantine_reserved", 0)?;
            let source_path = decoder.take_required_path("source_path")?;
            let destination_path = decoder.take_optional_path("destination_path")?;
            if destination_path.is_some() != (flags & 0x0004 != 0) {
                return Err(CodecViolation::OptionalPathFlagMismatch {
                    field: "destination_path",
                });
            }
            LedgerRecord::Quarantined {
                entity_kind,
                reason,
                sequence_at_observation,
                physical_key,
                content_fingerprint,
                source_path,
                destination_path,
            }
        }
        RecordType::Unknown(_) => {
            return Err(CodecViolation::InvalidEnvelopeRelationship {
                detail: "unknown noncritical payload must be skipped without typed decoding",
            });
        }
    };
    decoder.finish()?;
    validate_envelope_relationships(&record, sequence, log_generation)?;
    Ok(record)
}

fn validate_expected_length(expected_length: u64) -> Result<(), CodecViolation> {
    if expected_length == 0 {
        return Err(CodecViolation::ZeroExpectedFileLength);
    }
    Ok(())
}
