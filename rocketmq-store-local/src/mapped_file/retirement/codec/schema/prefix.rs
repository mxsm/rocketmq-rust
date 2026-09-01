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

use super::super::super::identity::FileIncarnationId;
use super::super::super::identity::IdentityViolation;
use super::super::super::identity::StoreRelativePath;
use super::super::super::identity::StoreUuid;
use super::super::super::identity::TicketId;
use super::super::payload::decode_physical_key;
use super::super::CodecViolation;
use super::super::GenerationAbortReason;
use super::super::OpenReason;
use super::super::QuarantineEntityKind;
use super::super::QuarantineReason;
use super::super::RecordType;
use super::super::RetirementReason;
use super::super::MAX_SEALED_RECORD_UNIT_LENGTH;

pub(in crate::mapped_file::retirement::codec) fn validate_known_payload_prefix(
    record_type: RecordType,
    sequence: u64,
    log_generation: u64,
    declared_length: usize,
    available: &[u8],
) -> Result<(), CodecViolation> {
    if !record_type.is_known() {
        return Ok(());
    }
    super::validate_known_payload_length(record_type, declared_length)?;
    super::super::semantics::validate_initial_record_envelope(record_type, sequence, log_generation)?;
    let mut decoder = PrefixDecoder::new(record_type, declared_length, available);

    macro_rules! complete {
        ($expression:expr) => {
            match $expression? {
                Some(value) => value,
                None => return Ok(()),
            }
        };
    }

    match record_type {
        RecordType::StoreInitialized => {
            complete!(decoder.store_uuid("store_uuid"));
            complete!(decoder.opaque_id("bootstrap_id"));
            complete!(decoder.skip(8));
            complete!(decoder.require_u64("initial_snapshot_generation", 0));
            complete!(decoder.require_u64("initial_log_generation", 0));
            complete!(decoder.require_u64("feature_bitmap", 1));
        }
        RecordType::BootstrapInstalled => {
            complete!(decoder.store_uuid("store_uuid"));
            complete!(decoder.opaque_id("bootstrap_id"));
            decoder.validate_u64_prefix("snapshot_generation", &[0])?;
            let snapshot_generation = complete!(decoder.u64());
            if snapshot_generation != 0 {
                return Err(CodecViolation::InvalidEnvelopeRelationship {
                    detail: "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
                });
            }
            decoder.validate_u64_prefix("snapshot_base_sequence", &[1])?;
            let snapshot_base_sequence = complete!(decoder.u64());
            if snapshot_base_sequence != 1 {
                return Err(CodecViolation::InvalidEnvelopeRelationship {
                    detail: "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
                });
            }
            complete!(decoder.skip(8 + 4));
            complete!(decoder.require_u32("bootstrap_reserved", 0));
            complete!(decoder.skip(8 + 8 + 8));
        }
        RecordType::LogOpened => {
            complete!(decoder.store_uuid("store_uuid"));
            let generation = complete!(decoder.u64());
            let snapshot_generation = complete!(decoder.u64());
            let predecessor_generation = complete!(decoder.u64());
            let predecessor_sequence = complete!(decoder.u64());
            let snapshot_base_sequence = complete!(decoder.u64());
            complete!(decoder.skip(8 + 4 + 4 + 8));
            let suffix_length = complete!(decoder.u32());
            let suffix_crc32 = complete!(decoder.u32());
            let open_reason = OpenReason::from_wire(complete!(decoder.u8()))?;
            let predecessor_acknowledgement_epoch = complete!(decoder.u64());
            complete!(decoder.zero_bytes("log_opened_reserved", 7));
            validate_log_opened_prefix(
                sequence,
                log_generation,
                generation,
                snapshot_generation,
                predecessor_generation,
                predecessor_sequence,
                snapshot_base_sequence,
                suffix_length,
                suffix_crc32,
                open_reason,
                predecessor_acknowledgement_epoch,
            )?;
        }
        RecordType::GenerationPrepared => {
            complete!(decoder.store_uuid("store_uuid"));
            let source_generation = complete!(decoder.u64());
            let target_generation = complete!(decoder.u64());
            let target_snapshot_generation = complete!(decoder.u64());
            let repeated_sequence = complete!(decoder.u64());
            if repeated_sequence != sequence {
                return Err(CodecViolation::InvalidEnvelopeRelationship {
                    detail: "GenerationPrepared repeated sequence differs from its frame",
                });
            }
            let open_reason = OpenReason::from_wire(complete!(decoder.u8()))?;
            complete!(decoder.zero_bytes("generation_prepared_reserved", 7));
            let expected_target =
                source_generation
                    .checked_add(1)
                    .ok_or(CodecViolation::InvalidGenerationRelationship {
                        detail: "GenerationPrepared source generation cannot advance",
                    })?;
            if source_generation != log_generation
                || target_generation != expected_target
                || target_snapshot_generation != target_generation
                || open_reason != OpenReason::Compaction
            {
                return Err(CodecViolation::InvalidGenerationRelationship {
                    detail: "GenerationPrepared fields do not bind source + 1",
                });
            }
        }
        RecordType::GenerationAborted => {
            complete!(decoder.store_uuid("store_uuid"));
            let source_generation = complete!(decoder.u64());
            let target_generation = complete!(decoder.u64());
            let prepared_sequence = complete!(decoder.u64());
            decoder.validate_u32_prefix("generation_abort_reason", &[1, 2, 3, 4])?;
            GenerationAbortReason::from_wire(complete!(decoder.u32()))?;
            complete!(decoder.require_u32("generation_aborted_reserved", 0));
            if source_generation.checked_add(1) != Some(target_generation)
                || source_generation != log_generation
                || prepared_sequence.checked_add(1) != Some(sequence)
            {
                return Err(CodecViolation::InvalidGenerationRelationship {
                    detail: "GenerationAborted fields do not bind the immediately preceding preparation",
                });
            }
        }
        RecordType::MarkerCommitted => {
            complete!(decoder.store_uuid("store_uuid"));
            let marker_epoch = complete!(decoder.u64());
            let snapshot_generation = complete!(decoder.u64());
            let selected_log_generation = complete!(decoder.u64());
            let anchor_sequence = complete!(decoder.u64());
            let slot_index = complete!(decoder.u8());
            complete!(decoder.zero_bytes("marker_committed_reserved", 3));
            complete!(decoder.skip(4));
            validate_marker_prefix(
                sequence,
                log_generation,
                marker_epoch,
                snapshot_generation,
                selected_log_generation,
                anchor_sequence,
                slot_index,
            )?;
        }
        RecordType::AllocateIncarnation => {
            complete!(decoder.incarnation());
            complete!(decoder.skip(8));
            complete!(decoder.expected_length());
            complete!(decoder.opaque_id("create_nonce"));
            complete!(decoder.path("canonical_path", false, 3));
            complete!(decoder.path("create_file_path", false, 0));
        }
        RecordType::BindIncarnation | RecordType::PublishIncarnation => {
            complete!(decoder.incarnation());
            complete!(decoder.expected_length());
            complete!(decoder.physical_key(true));
            complete!(decoder.path("canonical_path", false, 3));
            complete!(decoder.path("create_file_path", false, 0));
        }
        RecordType::RetirementIntent => {
            complete!(decoder.ticket());
            complete!(decoder.incarnation());
            decoder.validate_u16_prefix("retirement_reason", &[1, 2, 3, 4, 5, 6, 7, 8, 9])?;
            RetirementReason::from_wire(complete!(decoder.u16()))?;
            complete!(decoder.require_u16("retirement_intent_flags", 0));
            let mapping_generation = complete!(decoder.u64());
            if mapping_generation == 0 {
                return Err(CodecViolation::ZeroMappingGeneration);
            }
            complete!(decoder.skip(8));
            complete!(decoder.expected_length());
            complete!(decoder.opaque_id("retirement_nonce"));
            complete!(decoder.physical_key(true));
            complete!(decoder.path("canonical_path", false, 0));
        }
        RecordType::LogicalRemoved => {
            complete!(decoder.ticket());
            complete!(decoder.incarnation());
            complete!(decoder.physical_key(true));
            complete!(decoder.path("canonical_path", false, 0));
        }
        RecordType::Tombstoned => {
            complete!(decoder.ticket());
            complete!(decoder.incarnation());
            complete!(decoder.physical_key(true));
            complete!(decoder.opaque_id("retirement_nonce"));
            complete!(decoder.path("canonical_path", false, 3));
            complete!(decoder.path("tombstone_path", false, 0));
        }
        RecordType::NamespaceAbsent => {
            complete!(decoder.ticket());
            complete!(decoder.incarnation());
            decoder.validate_u16_prefix("namespace_absent_flags", &[0x0003, 0x0007])?;
            let proof_flags = complete!(decoder.u16());
            if proof_flags & 0x0003 != 0x0003 || proof_flags & !0x0007 != 0 {
                return Err(CodecViolation::InvalidProofFlags {
                    field: "namespace_absent",
                    flags: u32::from(proof_flags),
                });
            }
            complete!(decoder.require_u16("namespace_absent_reserved", 0));
            complete!(decoder.skip(8));
            complete!(decoder.physical_key(true));
            complete!(decoder.path("canonical_path", false, 2));
            complete!(decoder.path("tombstone_path", true, 0));
        }
        RecordType::Completed => {
            complete!(decoder.ticket());
            complete!(decoder.incarnation());
            complete!(decoder.skip(8));
            let namespace_absent_sequence = complete!(decoder.u64());
            if namespace_absent_sequence == 0 || namespace_absent_sequence >= sequence {
                return Err(CodecViolation::InvalidNamespaceAbsentSequence);
            }
            decoder.validate_u32_prefix("completed_proof_flags", &[0x0000_0003])?;
            let proof_flags = complete!(decoder.u32());
            if proof_flags != 0x0000_0003 {
                return Err(CodecViolation::InvalidProofFlags {
                    field: "completed",
                    flags: proof_flags,
                });
            }
            complete!(decoder.require_u32("completed_reserved", 0));
        }
        RecordType::SupersededPath => {
            complete!(decoder.ticket());
            complete!(decoder.incarnation());
            complete!(decoder.physical_key(true));
            complete!(decoder.physical_key(true));
            complete!(decoder.path("canonical_path", false, 0));
        }
        RecordType::Quarantined => {
            QuarantineEntityKind::from_wire(complete!(decoder.u8()))?;
            QuarantineReason::from_wire(complete!(decoder.u8()))?;
            decoder.validate_u16_prefix("quarantine_flags", &[0, 1, 2, 3, 4, 5, 6, 7])?;
            let flags = complete!(decoder.u16());
            if flags & !0x0007 != 0 {
                return Err(CodecViolation::InvalidQuarantineFlags { flags });
            }
            complete!(decoder.skip(8));
            complete!(decoder.physical_key(flags & 0x0001 != 0));
            let content_length = complete!(decoder.u64());
            let content_crc32 = complete!(decoder.u32());
            if flags & 0x0002 == 0 {
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
            }
            complete!(decoder.require_u32("quarantine_reserved", 0));
            complete!(decoder.path("source_path", false, 2));
            let destination_present = complete!(decoder.path("destination_path", true, 0));
            if destination_present != (flags & 0x0004 != 0) {
                return Err(CodecViolation::OptionalPathFlagMismatch {
                    field: "destination_path",
                });
            }
        }
        RecordType::Unknown(_) => return Ok(()),
    }
    decoder.finish()
}

#[allow(
    clippy::too_many_arguments,
    reason = "the arguments are the fixed LogOpened wire fields"
)]
fn validate_log_opened_prefix(
    sequence: u64,
    log_generation: u64,
    generation: u64,
    snapshot_generation: u64,
    predecessor_generation: u64,
    predecessor_sequence: u64,
    snapshot_base_sequence: u64,
    suffix_length: u32,
    suffix_crc32: u32,
    open_reason: OpenReason,
    predecessor_acknowledgement_epoch: u64,
) -> Result<(), CodecViolation> {
    if predecessor_generation.checked_add(1) != Some(generation)
        || generation != snapshot_generation
        || generation != log_generation
    {
        return Err(CodecViolation::InvalidGenerationRelationship {
            detail: "LogOpened generation fields do not bind predecessor + 1",
        });
    }
    if snapshot_base_sequence.checked_add(1) != Some(sequence)
        || predecessor_sequence == 0
        || predecessor_sequence != snapshot_base_sequence
    {
        return Err(CodecViolation::InvalidEnvelopeRelationship {
            detail: "LogOpened sequence fields do not bind the snapshot base",
        });
    }
    if predecessor_acknowledgement_epoch == 0 {
        return Err(CodecViolation::ZeroAcknowledgementEpoch);
    }
    predecessor_acknowledgement_epoch
        .checked_add(1)
        .ok_or(CodecViolation::AcknowledgementEpochOverflow)?;
    let suffix_valid = match open_reason {
        OpenReason::Compaction => suffix_length == 0 && suffix_crc32 == 0,
        OpenReason::TailRepair => suffix_length != 0 && u64::from(suffix_length) < MAX_SEALED_RECORD_UNIT_LENGTH as u64,
    };
    if !suffix_valid {
        return Err(CodecViolation::InvalidTailRepairFields);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the arguments are the fixed MarkerCommitted wire fields"
)]
fn validate_marker_prefix(
    sequence: u64,
    log_generation: u64,
    marker_epoch: u64,
    snapshot_generation: u64,
    selected_log_generation: u64,
    anchor_sequence: u64,
    slot_index: u8,
) -> Result<(), CodecViolation> {
    if marker_epoch == 0 {
        return Err(CodecViolation::ZeroMarkerEpoch);
    }
    if slot_index > 1 {
        return Err(CodecViolation::InvalidMarkerSlotIndex { slot_index });
    }
    let expected_slot_index = ((marker_epoch - 1) & 1) as u8;
    if slot_index != expected_slot_index {
        return Err(CodecViolation::MarkerSlotParityMismatch {
            marker_epoch,
            expected_slot_index,
            actual_slot_index: slot_index,
        });
    }
    if snapshot_generation != log_generation || selected_log_generation != log_generation {
        return Err(CodecViolation::InvalidGenerationRelationship {
            detail: "MarkerCommitted generations differ from the containing log",
        });
    }
    if anchor_sequence.checked_add(1) != Some(sequence) {
        return Err(CodecViolation::InvalidEnvelopeRelationship {
            detail: "MarkerCommitted must immediately follow its anchor",
        });
    }
    Ok(())
}

struct PrefixDecoder<'a> {
    record_type: RecordType,
    declared_length: usize,
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PrefixDecoder<'a> {
    const fn new(record_type: RecordType, declared_length: usize, bytes: &'a [u8]) -> Self {
        Self {
            record_type,
            declared_length,
            bytes,
            offset: 0,
        }
    }

    fn take<const N: usize>(&mut self) -> Result<Option<[u8; N]>, CodecViolation> {
        let end = self.offset.checked_add(N).ok_or(CodecViolation::FrameLengthOverflow)?;
        self.require_declared_capacity(N, end)?;
        let Some(bytes) = self.bytes.get(self.offset..end) else {
            return Ok(None);
        };
        self.offset = end;
        bytes
            .try_into()
            .map(Some)
            .map_err(|_| CodecViolation::FrameLengthOverflow)
    }

    fn skip(&mut self, length: usize) -> Result<Option<()>, CodecViolation> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        self.require_declared_capacity(length, end)?;
        if self.bytes.get(self.offset..end).is_none() {
            return Ok(None);
        }
        self.offset = end;
        Ok(Some(()))
    }

    fn u8(&mut self) -> Result<Option<u8>, CodecViolation> {
        Ok(self.take::<1>()?.map(|bytes| bytes[0]))
    }

    fn u16(&mut self) -> Result<Option<u16>, CodecViolation> {
        Ok(self.take()?.map(u16::from_le_bytes))
    }

    fn u32(&mut self) -> Result<Option<u32>, CodecViolation> {
        Ok(self.take()?.map(u32::from_le_bytes))
    }

    fn u64(&mut self) -> Result<Option<u64>, CodecViolation> {
        Ok(self.take()?.map(u64::from_le_bytes))
    }

    fn validate_u16_prefix(&self, field: &'static str, allowed: &[u16]) -> Result<(), CodecViolation> {
        self.validate_allowed_prefix(field, allowed.iter().map(|value| value.to_le_bytes()))
    }

    fn validate_u32_prefix(&self, field: &'static str, allowed: &[u32]) -> Result<(), CodecViolation> {
        self.validate_allowed_prefix(field, allowed.iter().map(|value| value.to_le_bytes()))
    }

    fn validate_u64_prefix(&self, field: &'static str, allowed: &[u64]) -> Result<(), CodecViolation> {
        self.validate_allowed_prefix(field, allowed.iter().map(|value| value.to_le_bytes()))
    }

    fn validate_allowed_prefix<const N: usize>(
        &self,
        field: &'static str,
        mut allowed: impl Iterator<Item = [u8; N]>,
    ) -> Result<(), CodecViolation> {
        let available = self.bytes.len().saturating_sub(self.offset).min(N);
        if available == 0 || available == N {
            return Ok(());
        }
        let prefix = self
            .bytes
            .get(self.offset..self.offset + available)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        if allowed.any(|value| value.starts_with(prefix)) {
            return Ok(());
        }
        Err(CodecViolation::InvalidFieldPrefix {
            field,
            offset: self.offset + available - 1,
        })
    }

    fn store_uuid(&mut self, field: &'static str) -> Result<Option<()>, CodecViolation> {
        let Some(bytes) = self.take()? else {
            return Ok(None);
        };
        StoreUuid::new(bytes)
            .map(|_| Some(()))
            .map_err(|source| CodecViolation::InvalidIdentity { field, source })
    }

    fn incarnation(&mut self) -> Result<Option<()>, CodecViolation> {
        let Some(store_uuid_bytes) = self.take()? else {
            return Ok(None);
        };
        let store_uuid = StoreUuid::new(store_uuid_bytes).map_err(|source| CodecViolation::InvalidIdentity {
            field: "incarnation_store_uuid",
            source,
        })?;
        let Some(create_sequence) = self.u64()? else {
            return Ok(None);
        };
        FileIncarnationId::new(store_uuid, create_sequence)
            .map(|_| Some(()))
            .map_err(|source| CodecViolation::InvalidIdentity {
                field: "file_incarnation",
                source,
            })
    }

    fn ticket(&mut self) -> Result<Option<()>, CodecViolation> {
        let Some(value) = self.u64()? else {
            return Ok(None);
        };
        TicketId::new(value)
            .map(|_| Some(()))
            .map_err(|source| CodecViolation::InvalidIdentity {
                field: "ticket_id",
                source,
            })
    }

    fn opaque_id(&mut self, field: &'static str) -> Result<Option<()>, CodecViolation> {
        let Some(value) = self.take::<16>()? else {
            return Ok(None);
        };
        if value == [0; 16] {
            return Err(CodecViolation::ZeroOpaqueIdentifier { field });
        }
        Ok(Some(()))
    }

    fn expected_length(&mut self) -> Result<Option<()>, CodecViolation> {
        let Some(value) = self.u64()? else {
            return Ok(None);
        };
        if value == 0 {
            return Err(CodecViolation::ZeroExpectedFileLength);
        }
        Ok(Some(()))
    }

    fn require_u16(&mut self, field: &'static str, expected: u16) -> Result<Option<()>, CodecViolation> {
        self.require_integer_bytes(field, &expected.to_le_bytes())
    }

    fn require_u32(&mut self, field: &'static str, expected: u32) -> Result<Option<()>, CodecViolation> {
        self.require_integer_bytes(field, &expected.to_le_bytes())
    }

    fn require_u64(&mut self, field: &'static str, expected: u64) -> Result<Option<()>, CodecViolation> {
        self.require_integer_bytes(field, &expected.to_le_bytes())
    }

    fn require_integer_bytes(&mut self, field: &'static str, expected: &[u8]) -> Result<Option<()>, CodecViolation> {
        let end = self
            .offset
            .checked_add(expected.len())
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        self.require_declared_capacity(expected.len(), end)?;
        let available = self.bytes.len().saturating_sub(self.offset).min(expected.len());
        let prefix = self
            .bytes
            .get(self.offset..self.offset + available)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        if !expected.starts_with(prefix) {
            let value = prefix
                .iter()
                .enumerate()
                .fold(0_u64, |value, (index, byte)| value | (u64::from(*byte) << (index * 8)));
            return Err(CodecViolation::NonZeroReserved { field, value });
        }
        if available < expected.len() {
            return Ok(None);
        }
        self.offset += expected.len();
        Ok(Some(()))
    }

    fn zero_bytes(&mut self, field: &'static str, length: usize) -> Result<Option<()>, CodecViolation> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        self.require_declared_capacity(length, end)?;
        let available = self.bytes.len().saturating_sub(self.offset).min(length);
        let bytes = self
            .bytes
            .get(self.offset..self.offset + available)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        if let Some(value) = bytes.iter().copied().find(|value| *value != 0) {
            return Err(CodecViolation::NonZeroReserved {
                field,
                value: u64::from(value),
            });
        }
        if available < length {
            return Ok(None);
        }
        self.offset += length;
        Ok(Some(()))
    }

    fn physical_key(&mut self, present: bool) -> Result<Option<()>, CodecViolation> {
        let end = self.offset.checked_add(32).ok_or(CodecViolation::FrameLengthOverflow)?;
        self.require_declared_capacity(32, end)?;
        let available = self.bytes.len().saturating_sub(self.offset).min(32);
        let prefix = self
            .bytes
            .get(self.offset..self.offset + available)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        if !present {
            if prefix.iter().any(|byte| *byte != 0) {
                return Err(CodecViolation::InvalidAbsentPhysicalFileKey);
            }
        } else if let Some(kind) = prefix.first().copied() {
            if !matches!(kind, 1 | 2) {
                return Err(CodecViolation::InvalidPhysicalFileKeyKind { kind });
            }
            if prefix
                .get(1..prefix.len().min(8))
                .is_some_and(|bytes| bytes.iter().any(|byte| *byte != 0))
            {
                return Err(CodecViolation::NonZeroPhysicalFileKeyReserved);
            }
            if kind == 1
                && prefix
                    .get(24.min(prefix.len())..)
                    .is_some_and(|bytes| bytes.iter().any(|byte| *byte != 0))
            {
                return Err(CodecViolation::NonZeroPhysicalFileKeyReserved);
            }
        }
        if available < 32 {
            return Ok(None);
        }
        if present {
            let bytes = prefix.try_into().map_err(|_| CodecViolation::FrameLengthOverflow)?;
            decode_physical_key(bytes)?;
        }
        self.offset += 32;
        Ok(Some(()))
    }

    fn path(
        &mut self,
        field: &'static str,
        optional: bool,
        minimum_trailing: usize,
    ) -> Result<Option<bool>, CodecViolation> {
        let Some(length) = self.u16()? else {
            return Ok(None);
        };
        let length = usize::from(length);
        if length == 0 {
            if optional {
                return Ok(Some(false));
            }
            return Err(CodecViolation::InvalidIdentity {
                field,
                source: IdentityViolation::EmptyStoreRelativePath,
            });
        }
        if length > StoreRelativePath::MAX_BYTES {
            return Err(CodecViolation::PayloadTooLarge {
                length,
                maximum: StoreRelativePath::MAX_BYTES,
            });
        }
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        let required_end = end
            .checked_add(minimum_trailing)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        self.require_declared_capacity(required_end.saturating_sub(self.offset), required_end)?;
        let available = self.bytes.len().saturating_sub(self.offset).min(length);
        let bytes = self
            .bytes
            .get(self.offset..self.offset + available)
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        validate_path_prefix(field, bytes, available == length)?;
        if available < length {
            return Ok(None);
        }
        self.offset += length;
        Ok(Some(true))
    }

    fn require_declared_capacity(&self, needed: usize, end: usize) -> Result<(), CodecViolation> {
        if end > self.declared_length {
            return Err(CodecViolation::UnexpectedPayloadEnd {
                record_type: self.record_type.wire_value(),
                offset: self.offset,
                needed,
                remaining: self.declared_length.saturating_sub(self.offset),
            });
        }
        Ok(())
    }

    fn finish(self) -> Result<(), CodecViolation> {
        if self.offset != self.declared_length {
            return Err(CodecViolation::TrailingPayloadBytes {
                record_type: self.record_type.wire_value(),
                offset: self.offset,
                remaining: self.declared_length.saturating_sub(self.offset),
            });
        }
        if self.offset != self.bytes.len() {
            return Err(CodecViolation::TrailingPayloadBytes {
                record_type: self.record_type.wire_value(),
                offset: self.offset,
                remaining: self.bytes.len() - self.offset,
            });
        }
        Ok(())
    }
}

fn validate_path_prefix(field: &'static str, bytes: &[u8], complete: bool) -> Result<(), CodecViolation> {
    if bytes.first() == Some(&b'/') {
        return path_identity_error(field, IdentityViolation::AbsoluteStoreRelativePath);
    }
    if bytes.contains(&b'\\') {
        return path_identity_error(field, IdentityViolation::StoreRelativePathContainsBackslash);
    }
    if bytes.contains(&0) {
        return path_identity_error(field, IdentityViolation::StoreRelativePathContainsNul);
    }
    if bytes.contains(&b':') {
        return path_identity_error(field, IdentityViolation::StoreRelativePathContainsColon);
    }
    if bytes.iter().any(|byte| byte.is_ascii_control()) {
        return path_identity_error(field, IdentityViolation::StoreRelativePathContainsAsciiControl);
    }

    let valid_length = match std::str::from_utf8(bytes) {
        Ok(_) => bytes.len(),
        Err(source) if !complete && source.error_len().is_none() => source.valid_up_to(),
        Err(_) => return Err(CodecViolation::InvalidUtf8Path { field }),
    };
    let valid = bytes.get(..valid_length).ok_or(CodecViolation::FrameLengthOverflow)?;
    let mut component_start = 0;
    for (index, byte) in valid.iter().enumerate() {
        if *byte == b'/' {
            validate_complete_component(field, &valid[component_start..index])?;
            component_start = index + 1;
        }
    }
    let current_length = bytes.len().saturating_sub(component_start);
    if current_length > StoreRelativePath::MAX_COMPONENT_BYTES {
        return path_identity_error(
            field,
            IdentityViolation::StoreRelativePathComponentTooLong {
                length: current_length,
                maximum: StoreRelativePath::MAX_COMPONENT_BYTES,
            },
        );
    }
    if complete {
        validate_complete_component(field, &valid[component_start..])?;
    }
    Ok(())
}

fn validate_complete_component(field: &'static str, bytes: &[u8]) -> Result<(), CodecViolation> {
    let component = std::str::from_utf8(bytes).map_err(|_| CodecViolation::InvalidUtf8Path { field })?;
    match component {
        "" => return path_identity_error(field, IdentityViolation::EmptyStoreRelativePathSegment),
        "." => return path_identity_error(field, IdentityViolation::CurrentStoreRelativePathSegment),
        ".." => return path_identity_error(field, IdentityViolation::ParentStoreRelativePathSegment),
        _ => {}
    }
    if bytes.len() > StoreRelativePath::MAX_COMPONENT_BYTES {
        return path_identity_error(
            field,
            IdentityViolation::StoreRelativePathComponentTooLong {
                length: bytes.len(),
                maximum: StoreRelativePath::MAX_COMPONENT_BYTES,
            },
        );
    }
    if component.ends_with('.') || component.ends_with(' ') {
        return path_identity_error(field, IdentityViolation::StoreRelativePathComponentHasWindowsTrimSuffix);
    }
    let device_stem = component.split('.').next().unwrap_or(component);
    if is_windows_reserved_device_name(device_stem) {
        return path_identity_error(field, IdentityViolation::WindowsReservedStoreRelativePathComponent);
    }
    Ok(())
}

fn path_identity_error(field: &'static str, source: IdentityViolation) -> Result<(), CodecViolation> {
    Err(CodecViolation::InvalidIdentity { field, source })
}

fn is_windows_reserved_device_name(component: &str) -> bool {
    if component.eq_ignore_ascii_case("CON")
        || component.eq_ignore_ascii_case("PRN")
        || component.eq_ignore_ascii_case("AUX")
        || component.eq_ignore_ascii_case("NUL")
    {
        return true;
    }
    let bytes = component.as_bytes();
    bytes.len() == 4
        && matches!(bytes[3], b'1'..=b'9')
        && (bytes[..3].eq_ignore_ascii_case(b"COM") || bytes[..3].eq_ignore_ascii_case(b"LPT"))
}
