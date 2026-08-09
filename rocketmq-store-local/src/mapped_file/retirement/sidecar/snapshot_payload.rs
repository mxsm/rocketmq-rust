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

use super::super::codec::ContentFingerprint;
use super::super::codec::QuarantineEntityKind;
use super::super::codec::QuarantineReason;
use super::super::codec::RetirementReason;
use super::super::identity::FileIncarnationId;
use super::super::identity::PhysicalFileKey;
use super::super::identity::StoreRelativePath;
use super::super::identity::StoreUuid;
use super::super::identity::TicketId;
use super::types::IncarnationPhase;
use super::types::IncarnationSnapshotEntry;
use super::types::QuarantineSnapshotEntry;
use super::types::RetirementStage;
use super::types::RetirementTicketSnapshotEntry;
use super::types::SnapshotEntry;
use super::SidecarError;

pub(super) const INCARNATION_KIND: u16 = 1;
pub(super) const RETIREMENT_TICKET_KIND: u16 = 2;
pub(super) const QUARANTINE_KIND: u16 = 3;
pub(super) const INCARNATION_PAYLOAD_MAX: usize = 8_288;
pub(super) const RETIREMENT_TICKET_PAYLOAD_MAX: usize = 8_312;
pub(super) const QUARANTINE_PAYLOAD_MAX: usize = 8_256;

pub(super) const fn entry_kind(entry: &SnapshotEntry) -> u16 {
    match entry {
        SnapshotEntry::Incarnation(_) => INCARNATION_KIND,
        SnapshotEntry::RetirementTicket(_) => RETIREMENT_TICKET_KIND,
        SnapshotEntry::Quarantine(_) => QUARANTINE_KIND,
    }
}

pub(super) const fn kind_payload_max(kind: u16) -> Option<usize> {
    match kind {
        INCARNATION_KIND => Some(INCARNATION_PAYLOAD_MAX),
        RETIREMENT_TICKET_KIND => Some(RETIREMENT_TICKET_PAYLOAD_MAX),
        QUARANTINE_KIND => Some(QUARANTINE_PAYLOAD_MAX),
        _ => None,
    }
}

pub(super) fn encode_payload(
    entry: &SnapshotEntry,
    store_uuid: StoreUuid,
    base_sequence: u64,
) -> Result<Vec<u8>, SidecarError> {
    match entry {
        SnapshotEntry::Incarnation(entry) => {
            validate_incarnation(entry, store_uuid)?;
            let mut output = Vec::new();
            push_incarnation(&mut output, entry.incarnation);
            output.push(phase_wire(entry.phase));
            output.push(u8::from(entry.physical_key.is_some()));
            push_u16(&mut output, 0);
            push_u64(&mut output, entry.segment_offset);
            push_u64(&mut output, entry.expected_file_length);
            output.extend_from_slice(&entry.create_nonce);
            match entry.physical_key {
                Some(key) => push_physical_key(&mut output, key),
                None => output.extend_from_slice(&[0; 32]),
            }
            push_path(&mut output, &entry.canonical_path);
            push_path(&mut output, &entry.create_file_path);
            Ok(output)
        }
        SnapshotEntry::RetirementTicket(entry) => {
            validate_retirement(entry, store_uuid, base_sequence)?;
            let mut output = Vec::new();
            push_u64(&mut output, entry.ticket_id.get());
            push_incarnation(&mut output, entry.incarnation);
            output.push(stage_wire(entry.stage));
            let flags = u8::from(entry.tombstone_path.is_some())
                | (u8::from(entry.superseded_path_observed) << 1)
                | (u8::from(entry.quarantined) << 2);
            output.push(flags);
            push_u16(&mut output, retirement_reason_wire(entry.reason));
            push_u64(&mut output, entry.stage_sequence);
            push_u64(&mut output, entry.mapping_generation);
            push_u64(&mut output, entry.segment_offset);
            push_u64(&mut output, entry.expected_file_length);
            output.extend_from_slice(&entry.retirement_nonce);
            push_physical_key(&mut output, entry.target_key);
            push_path(&mut output, &entry.canonical_path);
            push_optional_path(&mut output, entry.tombstone_path.as_ref());
            Ok(output)
        }
        SnapshotEntry::Quarantine(entry) => {
            validate_quarantine(entry, base_sequence)?;
            let mut output = Vec::new();
            output.push(quarantine_entity_kind_wire(entry.entity_kind));
            output.push(quarantine_reason_wire(entry.reason));
            let flags = u16::from(entry.physical_key.is_some())
                | (u16::from(entry.content_fingerprint.is_some()) << 1)
                | (u16::from(entry.destination_path.is_some()) << 2);
            push_u16(&mut output, flags);
            push_u64(&mut output, entry.sequence_at_observation);
            match entry.physical_key {
                Some(key) => push_physical_key(&mut output, key),
                None => output.extend_from_slice(&[0; 32]),
            }
            if let Some(fingerprint) = entry.content_fingerprint {
                push_u64(&mut output, fingerprint.length);
                push_u32(&mut output, fingerprint.crc32);
            } else {
                push_u64(&mut output, 0);
                push_u32(&mut output, 0);
            }
            push_u32(&mut output, 0);
            push_path(&mut output, &entry.source_path);
            push_optional_path(&mut output, entry.destination_path.as_ref());
            Ok(output)
        }
    }
}

pub(super) fn decode_payload(
    kind: u16,
    payload: &[u8],
    store_uuid: StoreUuid,
    base_sequence: u64,
) -> Result<SnapshotEntry, SidecarError> {
    let mut decoder = PayloadDecoder::new(kind, payload);
    let entry = match kind {
        INCARNATION_KIND => {
            let incarnation = decoder.take_incarnation()?;
            let phase = phase_from_wire(decoder.take_u8()?)?;
            let flags = decoder.take_u8()?;
            if flags & !1 != 0 {
                return Err(SidecarError::InvalidFlags {
                    field: "incarnation_snapshot",
                    value: u64::from(flags),
                });
            }
            decoder.require_zero_u16("incarnation_snapshot.reserved")?;
            let segment_offset = decoder.take_u64()?;
            let expected_file_length = decoder.take_u64()?;
            let create_nonce = decoder.take_array()?;
            let physical_bytes = decoder.take_array::<32>()?;
            let physical_key = if flags & 1 == 0 {
                require_absent_key(&physical_bytes)?;
                None
            } else {
                Some(decode_physical_key(physical_bytes)?)
            };
            let canonical_path = decoder.take_path("canonical_path")?;
            let create_file_path = decoder.take_path("create_file_path")?;
            let entry = IncarnationSnapshotEntry {
                incarnation,
                phase,
                segment_offset,
                expected_file_length,
                create_nonce,
                physical_key,
                canonical_path,
                create_file_path,
            };
            validate_incarnation(&entry, store_uuid)?;
            SnapshotEntry::Incarnation(entry)
        }
        RETIREMENT_TICKET_KIND => {
            let ticket_id = decoder.take_ticket()?;
            let incarnation = decoder.take_incarnation()?;
            let stage = stage_from_wire(decoder.take_u8()?)?;
            let flags = decoder.take_u8()?;
            if flags & !0x07 != 0 {
                return Err(SidecarError::InvalidFlags {
                    field: "retirement_ticket_snapshot",
                    value: u64::from(flags),
                });
            }
            let reason = retirement_reason_from_wire(decoder.take_u16()?)?;
            let stage_sequence = decoder.take_u64()?;
            let mapping_generation = decoder.take_u64()?;
            let segment_offset = decoder.take_u64()?;
            let expected_file_length = decoder.take_u64()?;
            let retirement_nonce = decoder.take_array()?;
            let target_key = decode_physical_key(decoder.take_array()?)?;
            let canonical_path = decoder.take_path("canonical_path")?;
            let tombstone_path = decoder.take_optional_path("tombstone_path", flags & 1 != 0)?;
            let entry = RetirementTicketSnapshotEntry {
                ticket_id,
                incarnation,
                stage,
                superseded_path_observed: flags & 0x02 != 0,
                quarantined: flags & 0x04 != 0,
                reason,
                stage_sequence,
                mapping_generation,
                segment_offset,
                expected_file_length,
                retirement_nonce,
                target_key,
                canonical_path,
                tombstone_path,
            };
            validate_retirement(&entry, store_uuid, base_sequence)?;
            SnapshotEntry::RetirementTicket(entry)
        }
        QUARANTINE_KIND => {
            let entity_kind = quarantine_entity_kind_from_wire(decoder.take_u8()?)?;
            let reason = quarantine_reason_from_wire(decoder.take_u8()?)?;
            let flags = decoder.take_u16()?;
            if flags & !0x07 != 0 {
                return Err(SidecarError::InvalidQuarantineFields { flags });
            }
            let sequence_at_observation = decoder.take_u64()?;
            let physical_bytes = decoder.take_array::<32>()?;
            let physical_key = if flags & 1 == 0 {
                require_absent_key(&physical_bytes)?;
                None
            } else {
                Some(decode_physical_key(physical_bytes)?)
            };
            let content_length = decoder.take_u64()?;
            let content_crc32 = decoder.take_u32()?;
            decoder.require_zero_u32("quarantine_snapshot.reserved")?;
            let content_fingerprint = if flags & 0x02 == 0 {
                if content_length != 0 || content_crc32 != 0 {
                    return Err(SidecarError::InvalidQuarantineFields { flags });
                }
                None
            } else {
                Some(ContentFingerprint {
                    length: content_length,
                    crc32: content_crc32,
                })
            };
            let source_path = decoder.take_path("source_path")?;
            let destination_path = decoder.take_optional_path("destination_path", flags & 0x04 != 0)?;
            let entry = QuarantineSnapshotEntry {
                entity_kind,
                reason,
                sequence_at_observation,
                physical_key,
                content_fingerprint,
                source_path,
                destination_path,
            };
            validate_quarantine(&entry, base_sequence)?;
            SnapshotEntry::Quarantine(entry)
        }
        _ => return Err(SidecarError::InvalidSnapshotEntryKind { kind }),
    };
    decoder.finish()?;
    Ok(entry)
}

fn validate_incarnation(entry: &IncarnationSnapshotEntry, store_uuid: StoreUuid) -> Result<(), SidecarError> {
    if entry.incarnation.store_uuid() != store_uuid {
        return Err(SidecarError::SnapshotStoreUuidMismatch);
    }
    if entry.expected_file_length == 0 {
        return Err(SidecarError::ZeroExpectedFileLength);
    }
    require_nonzero_id("create_nonce", &entry.create_nonce)?;
    entry
        .canonical_path
        .validate_segment_binding(entry.segment_offset)
        .map_err(|source| SidecarError::InvalidIdentity {
            field: "canonical_path",
            source,
        })?;
    entry
        .canonical_path
        .validate_create_binding(
            &entry.create_file_path,
            entry.incarnation,
            entry.segment_offset,
            &entry.create_nonce,
        )
        .map_err(|source| SidecarError::InvalidIdentity {
            field: "create_file_path",
            source,
        })?;
    let key_matches = matches!(entry.phase, IncarnationPhase::Allocated) == entry.physical_key.is_none();
    if !key_matches {
        return Err(SidecarError::IncarnationPhaseKeyMismatch);
    }
    Ok(())
}

fn validate_retirement(
    entry: &RetirementTicketSnapshotEntry,
    store_uuid: StoreUuid,
    base_sequence: u64,
) -> Result<(), SidecarError> {
    if entry.incarnation.store_uuid() != store_uuid {
        return Err(SidecarError::SnapshotStoreUuidMismatch);
    }
    if entry.stage_sequence == 0 || entry.stage_sequence > base_sequence {
        return Err(SidecarError::StageSequenceOutOfRange {
            sequence: entry.stage_sequence,
            base_sequence,
        });
    }
    if entry.mapping_generation == 0 {
        return Err(SidecarError::ZeroMappingGeneration);
    }
    if entry.expected_file_length == 0 {
        return Err(SidecarError::ZeroExpectedFileLength);
    }
    require_nonzero_id("retirement_nonce", &entry.retirement_nonce)?;
    entry
        .canonical_path
        .validate_segment_binding(entry.segment_offset)
        .map_err(|source| SidecarError::InvalidIdentity {
            field: "canonical_path",
            source,
        })?;
    if let Some(tombstone_path) = &entry.tombstone_path {
        entry
            .canonical_path
            .validate_tombstone_binding(
                tombstone_path,
                entry.ticket_id,
                entry.incarnation,
                entry.segment_offset,
                entry.mapping_generation,
                &entry.retirement_nonce,
            )
            .map_err(|source| SidecarError::InvalidIdentity {
                field: "tombstone_path",
                source,
            })?;
    }
    let tombstone_matches = match entry.stage {
        RetirementStage::IntentDurable | RetirementStage::LogicalRemoved => entry.tombstone_path.is_none(),
        RetirementStage::Tombstoned => entry.tombstone_path.is_some(),
        RetirementStage::NamespaceAbsent | RetirementStage::CompletedRetained => true,
    };
    if !tombstone_matches {
        return Err(SidecarError::RetirementTombstoneStageMismatch);
    }
    Ok(())
}

fn validate_quarantine(entry: &QuarantineSnapshotEntry, base_sequence: u64) -> Result<(), SidecarError> {
    if entry.sequence_at_observation == 0 || entry.sequence_at_observation > base_sequence {
        return Err(SidecarError::ObservationSequenceOutOfRange {
            sequence: entry.sequence_at_observation,
            base_sequence,
        });
    }
    Ok(())
}

const fn phase_wire(value: IncarnationPhase) -> u8 {
    match value {
        IncarnationPhase::Allocated => 1,
        IncarnationPhase::Bound => 2,
        IncarnationPhase::Published => 3,
    }
}

fn phase_from_wire(value: u8) -> Result<IncarnationPhase, SidecarError> {
    match value {
        1 => Ok(IncarnationPhase::Allocated),
        2 => Ok(IncarnationPhase::Bound),
        3 => Ok(IncarnationPhase::Published),
        value => Err(SidecarError::InvalidEnumValue {
            field: "incarnation_phase",
            value: u64::from(value),
        }),
    }
}

const fn stage_wire(value: RetirementStage) -> u8 {
    match value {
        RetirementStage::IntentDurable => 1,
        RetirementStage::LogicalRemoved => 2,
        RetirementStage::Tombstoned => 3,
        RetirementStage::NamespaceAbsent => 4,
        RetirementStage::CompletedRetained => 5,
    }
}

fn stage_from_wire(value: u8) -> Result<RetirementStage, SidecarError> {
    match value {
        1 => Ok(RetirementStage::IntentDurable),
        2 => Ok(RetirementStage::LogicalRemoved),
        3 => Ok(RetirementStage::Tombstoned),
        4 => Ok(RetirementStage::NamespaceAbsent),
        5 => Ok(RetirementStage::CompletedRetained),
        value => Err(SidecarError::InvalidEnumValue {
            field: "retirement_stage",
            value: u64::from(value),
        }),
    }
}

const fn retirement_reason_wire(value: RetirementReason) -> u16 {
    match value {
        RetirementReason::TtlExpired => 1,
        RetirementReason::OffsetTruncate => 2,
        RetirementReason::Reset => 3,
        RetirementReason::DeleteLast => 4,
        RetirementReason::StoreDestroy => 5,
        RetirementReason::AllocationOrphan => 6,
        RetirementReason::TopicRetirement => 7,
        RetirementReason::DerivedFileRetirement => 8,
        RetirementReason::AuditedOperatorRequest => 9,
    }
}

fn retirement_reason_from_wire(value: u16) -> Result<RetirementReason, SidecarError> {
    match value {
        1 => Ok(RetirementReason::TtlExpired),
        2 => Ok(RetirementReason::OffsetTruncate),
        3 => Ok(RetirementReason::Reset),
        4 => Ok(RetirementReason::DeleteLast),
        5 => Ok(RetirementReason::StoreDestroy),
        6 => Ok(RetirementReason::AllocationOrphan),
        7 => Ok(RetirementReason::TopicRetirement),
        8 => Ok(RetirementReason::DerivedFileRetirement),
        9 => Ok(RetirementReason::AuditedOperatorRequest),
        value => Err(SidecarError::InvalidEnumValue {
            field: "retirement_reason",
            value: u64::from(value),
        }),
    }
}

const fn quarantine_entity_kind_wire(value: QuarantineEntityKind) -> u8 {
    match value {
        QuarantineEntityKind::Create => 1,
        QuarantineEntityKind::Tombstone => 2,
        QuarantineEntityKind::Sidecar => 3,
        QuarantineEntityKind::Canonical => 4,
    }
}

fn quarantine_entity_kind_from_wire(value: u8) -> Result<QuarantineEntityKind, SidecarError> {
    match value {
        1 => Ok(QuarantineEntityKind::Create),
        2 => Ok(QuarantineEntityKind::Tombstone),
        3 => Ok(QuarantineEntityKind::Sidecar),
        4 => Ok(QuarantineEntityKind::Canonical),
        value => Err(SidecarError::InvalidEnumValue {
            field: "quarantine_entity_kind",
            value: u64::from(value),
        }),
    }
}

const fn quarantine_reason_wire(value: QuarantineReason) -> u8 {
    match value {
        QuarantineReason::UnknownOwner => 1,
        QuarantineReason::KeyMismatch => 2,
        QuarantineReason::MalformedName => 3,
        QuarantineReason::RestoreRebindRequired => 4,
    }
}

fn quarantine_reason_from_wire(value: u8) -> Result<QuarantineReason, SidecarError> {
    match value {
        1 => Ok(QuarantineReason::UnknownOwner),
        2 => Ok(QuarantineReason::KeyMismatch),
        3 => Ok(QuarantineReason::MalformedName),
        4 => Ok(QuarantineReason::RestoreRebindRequired),
        value => Err(SidecarError::InvalidEnumValue {
            field: "quarantine_reason",
            value: u64::from(value),
        }),
    }
}

fn push_incarnation(output: &mut Vec<u8>, incarnation: FileIncarnationId) {
    output.extend_from_slice(incarnation.store_uuid().as_bytes());
    push_u64(output, incarnation.create_seq());
}

fn push_physical_key(output: &mut Vec<u8>, key: PhysicalFileKey) {
    match key {
        PhysicalFileKey::Unix(key) => {
            output.push(1);
            output.extend_from_slice(&[0; 7]);
            push_u64(output, key.device());
            push_u64(output, key.inode());
            push_u64(output, 0);
        }
        PhysicalFileKey::Windows(key) => {
            output.push(2);
            output.extend_from_slice(&[0; 7]);
            push_u64(output, key.volume_serial());
            output.extend_from_slice(&key.file_id());
        }
    }
}

fn decode_physical_key(bytes: [u8; 32]) -> Result<PhysicalFileKey, SidecarError> {
    if bytes[1..8].iter().any(|byte| *byte != 0) {
        return Err(SidecarError::NonZeroPhysicalFileKeyReserved);
    }
    match bytes[0] {
        1 => {
            if bytes[24..32].iter().any(|byte| *byte != 0) {
                return Err(SidecarError::NonZeroPhysicalFileKeyReserved);
            }
            Ok(PhysicalFileKey::unix(
                u64::from_le_bytes(
                    bytes[8..16]
                        .try_into()
                        .map_err(|_| SidecarError::SnapshotLengthOverflow)?,
                ),
                u64::from_le_bytes(
                    bytes[16..24]
                        .try_into()
                        .map_err(|_| SidecarError::SnapshotLengthOverflow)?,
                ),
            ))
        }
        2 => Ok(PhysicalFileKey::windows(
            u64::from_le_bytes(
                bytes[8..16]
                    .try_into()
                    .map_err(|_| SidecarError::SnapshotLengthOverflow)?,
            ),
            bytes[16..32]
                .try_into()
                .map_err(|_| SidecarError::SnapshotLengthOverflow)?,
        )),
        kind => Err(SidecarError::InvalidPhysicalFileKeyKind { kind }),
    }
}

fn require_absent_key(bytes: &[u8; 32]) -> Result<(), SidecarError> {
    if *bytes != [0; 32] {
        return Err(SidecarError::InvalidAbsentPhysicalFileKey);
    }
    Ok(())
}

fn require_nonzero_id(field: &'static str, value: &[u8; 16]) -> Result<(), SidecarError> {
    if *value == [0; 16] {
        return Err(SidecarError::ZeroOpaqueIdentifier { field });
    }
    Ok(())
}

fn push_path(output: &mut Vec<u8>, path: &StoreRelativePath) {
    let bytes = path.as_bytes();
    push_u16(output, bytes.len() as u16);
    output.extend_from_slice(bytes);
}

fn push_optional_path(output: &mut Vec<u8>, path: Option<&StoreRelativePath>) {
    if let Some(path) = path {
        push_path(output, path);
    } else {
        push_u16(output, 0);
    }
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

struct PayloadDecoder<'a> {
    kind: u16,
    input: &'a [u8],
    offset: usize,
}

impl<'a> PayloadDecoder<'a> {
    const fn new(kind: u16, input: &'a [u8]) -> Self {
        Self { kind, input, offset: 0 }
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], SidecarError> {
        let bytes = self.take(N)?;
        bytes.try_into().map_err(|_| SidecarError::SnapshotLengthOverflow)
    }

    fn take_u8(&mut self) -> Result<u8, SidecarError> {
        Ok(self.take(1)?[0])
    }

    fn take_u16(&mut self) -> Result<u16, SidecarError> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    fn take_u32(&mut self) -> Result<u32, SidecarError> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    fn take_u64(&mut self) -> Result<u64, SidecarError> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }

    fn take_incarnation(&mut self) -> Result<FileIncarnationId, SidecarError> {
        let uuid = StoreUuid::new(self.take_array()?).map_err(|source| SidecarError::InvalidIdentity {
            field: "store_uuid",
            source,
        })?;
        FileIncarnationId::new(uuid, self.take_u64()?).map_err(|source| SidecarError::InvalidIdentity {
            field: "file_incarnation_id",
            source,
        })
    }

    fn take_ticket(&mut self) -> Result<TicketId, SidecarError> {
        TicketId::new(self.take_u64()?).map_err(|source| SidecarError::InvalidIdentity {
            field: "ticket_id",
            source,
        })
    }

    fn take_path(&mut self, field: &'static str) -> Result<StoreRelativePath, SidecarError> {
        let length = usize::from(self.take_u16()?);
        let bytes = self.take(length)?;
        let value = std::str::from_utf8(bytes).map_err(|_| SidecarError::InvalidUtf8Path { field })?;
        StoreRelativePath::new(value).map_err(|source| SidecarError::InvalidIdentity { field, source })
    }

    fn take_optional_path(
        &mut self,
        field: &'static str,
        present: bool,
    ) -> Result<Option<StoreRelativePath>, SidecarError> {
        let length = usize::from(self.take_u16()?);
        if (length != 0) != present {
            return Err(SidecarError::OptionalPathFlagMismatch { field });
        }
        if length == 0 {
            return Ok(None);
        }
        let bytes = self.take(length)?;
        let value = std::str::from_utf8(bytes).map_err(|_| SidecarError::InvalidUtf8Path { field })?;
        StoreRelativePath::new(value)
            .map(Some)
            .map_err(|source| SidecarError::InvalidIdentity { field, source })
    }

    fn require_zero_u16(&mut self, field: &'static str) -> Result<(), SidecarError> {
        let value = self.take_u16()?;
        if value != 0 {
            return Err(SidecarError::NonZeroReserved {
                field,
                value: u64::from(value),
            });
        }
        Ok(())
    }

    fn require_zero_u32(&mut self, field: &'static str) -> Result<(), SidecarError> {
        let value = self.take_u32()?;
        if value != 0 {
            return Err(SidecarError::NonZeroReserved {
                field,
                value: u64::from(value),
            });
        }
        Ok(())
    }

    fn finish(self) -> Result<(), SidecarError> {
        if self.offset != self.input.len() {
            return Err(SidecarError::TrailingSnapshotPayload {
                kind: self.kind,
                remaining: self.input.len() - self.offset,
            });
        }
        Ok(())
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], SidecarError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SidecarError::SnapshotLengthOverflow)?;
        let Some(bytes) = self.input.get(self.offset..end) else {
            return Err(SidecarError::UnexpectedSnapshotPayloadEnd {
                kind: self.kind,
                offset: self.offset,
                needed: length,
                remaining: self.input.len().saturating_sub(self.offset),
            });
        };
        self.offset = end;
        Ok(bytes)
    }
}
