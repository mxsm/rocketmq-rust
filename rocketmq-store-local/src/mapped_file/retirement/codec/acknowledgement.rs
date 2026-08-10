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

use super::super::identity::StoreUuid;
use super::crc32;
use super::decode_next_frame;
use super::read_array;
use super::read_u16;
use super::read_u32;
use super::read_u64;
use super::CodecError;
use super::DecodeOutcome;
use super::DecodedFrame;
use super::ACKNOWLEDGEMENT_FILE_LENGTH;
use super::ACKNOWLEDGEMENT_SLOT_LENGTH;
use super::COMMIT_SEAL_LENGTH;
use super::FORMAT_MAJOR;
use super::FORMAT_MINOR;

const ACKNOWLEDGEMENT_MAGIC: [u8; 4] = *b"RMAC";
const COMMIT_SEAL_MAGIC: [u8; 4] = *b"RMCS";
const ACTIVATED_FLAG: u8 = 1;

/// One populated `ACKNOWLEDGED.v1` slot without its derived stored CRC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcknowledgementSlot {
    pub(crate) slot_index: u8,
    pub(crate) activated: bool,
    pub(crate) store_uuid: StoreUuid,
    pub(crate) bootstrap_id: [u8; 16],
    pub(crate) acknowledgement_epoch: u64,
    pub(crate) marker_epoch: u64,
    pub(crate) log_generation: u64,
    pub(crate) frame_sequence: u64,
    pub(crate) frame_end_offset: u64,
    pub(crate) frame_crc32: u32,
}

impl AcknowledgementSlot {
    pub(crate) fn next_acknowledgement_epoch(&self) -> Result<u64, CodecError> {
        self.acknowledgement_epoch
            .checked_add(1)
            .ok_or(CodecError::AcknowledgementEpochOverflow)
    }

    pub(crate) fn sealed_log_length(&self) -> Result<u64, CodecError> {
        self.frame_end_offset
            .checked_add(COMMIT_SEAL_LENGTH as u64)
            .ok_or(CodecError::SealedLogLengthOverflow)
    }

    fn validate(&self) -> Result<(), CodecError> {
        validate_acknowledgement_identity(
            self.slot_index,
            self.activated,
            self.acknowledgement_epoch,
            self.marker_epoch,
            self.frame_sequence,
        )?;
        if self.bootstrap_id == [0; 16] {
            return Err(CodecError::ZeroOpaqueIdentifier { field: "bootstrap_id" });
        }
        self.sealed_log_length()?;
        Ok(())
    }
}

/// Physical slot state; an all-zero slot is the only valid unused representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AcknowledgementSlotState {
    Unused,
    Populated(AcknowledgementSlot),
}

/// Structurally validated two-slot acknowledgement history and its authoritative slot.
///
/// This type proves slot CRCs, physical position, epoch adjacency, identity, and activation
/// monotonicity. It does not prove the corresponding log's frame/sequence/generation/marker chain;
/// replay must validate those relationships against commit seals and the selected marker before
/// treating the authoritative slot as a durable watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcknowledgementFile {
    slots: [AcknowledgementSlotState; 2],
    authoritative_index: Option<usize>,
}

impl AcknowledgementFile {
    pub(crate) fn authoritative(&self) -> Option<&AcknowledgementSlot> {
        let index = self.authoritative_index?;
        match self.slots.get(index)? {
            AcknowledgementSlotState::Unused => None,
            AcknowledgementSlotState::Populated(slot) => Some(slot),
        }
    }

    pub(crate) fn slots(&self) -> &[AcknowledgementSlotState; 2] {
        &self.slots
    }
}

/// Deterministic witness immediately following one acknowledged frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitSeal {
    pub(crate) acknowledgement_slot_index: u8,
    pub(crate) activated: bool,
    pub(crate) acknowledgement_epoch: u64,
    pub(crate) marker_epoch: u64,
    pub(crate) log_generation: u64,
    pub(crate) frame_sequence: u64,
    pub(crate) frame_end_offset: u64,
    pub(crate) frame_crc32: u32,
    pub(crate) acknowledgement_slot_crc32: u32,
}

impl CommitSeal {
    pub(crate) fn from_acknowledgement_slot(
        slot: &AcknowledgementSlot,
        encoded_slot: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    ) -> Result<Self, CodecError> {
        match decode_acknowledgement_slot(encoded_slot)? {
            AcknowledgementSlotState::Populated(decoded) if decoded == *slot => {}
            _ => return Err(CodecError::CommitSealSlotMismatch),
        }
        let acknowledgement_slot_crc32 = read_u32(encoded_slot, 100).ok_or(CodecError::FrameLengthOverflow)?;
        Ok(Self {
            acknowledgement_slot_index: slot.slot_index,
            activated: slot.activated,
            acknowledgement_epoch: slot.acknowledgement_epoch,
            marker_epoch: slot.marker_epoch,
            log_generation: slot.log_generation,
            frame_sequence: slot.frame_sequence,
            frame_end_offset: slot.frame_end_offset,
            frame_crc32: slot.frame_crc32,
            acknowledgement_slot_crc32,
        })
    }

    fn validate(&self) -> Result<(), CodecError> {
        validate_acknowledgement_identity(
            self.acknowledgement_slot_index,
            self.activated,
            self.acknowledgement_epoch,
            self.marker_epoch,
            self.frame_sequence,
        )?;
        self.frame_end_offset
            .checked_add(COMMIT_SEAL_LENGTH as u64)
            .ok_or(CodecError::SealedLogLengthOverflow)?;
        Ok(())
    }
}

pub(crate) fn encode_acknowledgement_slot(
    slot: &AcknowledgementSlot,
) -> Result<[u8; ACKNOWLEDGEMENT_SLOT_LENGTH], CodecError> {
    slot.validate()?;
    let mut encoded = [0_u8; ACKNOWLEDGEMENT_SLOT_LENGTH];
    encoded[0..4].copy_from_slice(&ACKNOWLEDGEMENT_MAGIC);
    encoded[4..6].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    encoded[6..8].copy_from_slice(&FORMAT_MINOR.to_le_bytes());
    encoded[8..10].copy_from_slice(&(ACKNOWLEDGEMENT_SLOT_LENGTH as u16).to_le_bytes());
    encoded[10] = slot.slot_index;
    encoded[11] = u8::from(slot.activated);
    encoded[16..32].copy_from_slice(slot.store_uuid.as_bytes());
    encoded[32..48].copy_from_slice(&slot.bootstrap_id);
    encoded[48..56].copy_from_slice(&slot.acknowledgement_epoch.to_le_bytes());
    encoded[56..64].copy_from_slice(&slot.marker_epoch.to_le_bytes());
    encoded[64..72].copy_from_slice(&slot.log_generation.to_le_bytes());
    encoded[72..80].copy_from_slice(&slot.frame_sequence.to_le_bytes());
    encoded[80..88].copy_from_slice(&slot.frame_end_offset.to_le_bytes());
    encoded[88..92].copy_from_slice(&slot.frame_crc32.to_le_bytes());
    encoded[92..100].copy_from_slice(&slot.sealed_log_length()?.to_le_bytes());
    let stored_crc = crc32(&encoded[..100]);
    encoded[100..104].copy_from_slice(&stored_crc.to_le_bytes());
    Ok(encoded)
}

pub(crate) fn decode_acknowledgement_slot(encoded: &[u8]) -> Result<AcknowledgementSlotState, CodecError> {
    if encoded.len() != ACKNOWLEDGEMENT_SLOT_LENGTH {
        return Err(CodecError::InvalidFixedStructureLength {
            structure: "acknowledgement slot",
            expected: ACKNOWLEDGEMENT_SLOT_LENGTH,
            actual: encoded.len(),
        });
    }
    if encoded.iter().all(|byte| *byte == 0) {
        return Ok(AcknowledgementSlotState::Unused);
    }
    let magic = read_array(encoded, 0).ok_or(CodecError::FrameLengthOverflow)?;
    if magic != ACKNOWLEDGEMENT_MAGIC {
        return Err(CodecError::InvalidAcknowledgementMagic { found: magic });
    }
    validate_version_and_length(encoded, ACKNOWLEDGEMENT_SLOT_LENGTH, "acknowledgement slot")?;
    let flags = *encoded.get(11).ok_or(CodecError::FrameLengthOverflow)?;
    if flags & !ACTIVATED_FLAG != 0 {
        return Err(CodecError::InvalidAcknowledgementFlags { flags });
    }
    require_zero(encoded, 12..16, "acknowledgement_reserved")?;
    let store_uuid =
        StoreUuid::new(read_array(encoded, 16).ok_or(CodecError::FrameLengthOverflow)?).map_err(|source| {
            CodecError::InvalidIdentity {
                field: "acknowledgement_store_uuid",
                source,
            }
        })?;
    let bootstrap_id = read_array(encoded, 32).ok_or(CodecError::FrameLengthOverflow)?;
    let slot = AcknowledgementSlot {
        slot_index: *encoded.get(10).ok_or(CodecError::FrameLengthOverflow)?,
        activated: flags & ACTIVATED_FLAG != 0,
        store_uuid,
        bootstrap_id,
        acknowledgement_epoch: read_u64(encoded, 48).ok_or(CodecError::FrameLengthOverflow)?,
        marker_epoch: read_u64(encoded, 56).ok_or(CodecError::FrameLengthOverflow)?,
        log_generation: read_u64(encoded, 64).ok_or(CodecError::FrameLengthOverflow)?,
        frame_sequence: read_u64(encoded, 72).ok_or(CodecError::FrameLengthOverflow)?,
        frame_end_offset: read_u64(encoded, 80).ok_or(CodecError::FrameLengthOverflow)?,
        frame_crc32: read_u32(encoded, 88).ok_or(CodecError::FrameLengthOverflow)?,
    };
    slot.validate()?;
    let stored_sealed_length = read_u64(encoded, 92).ok_or(CodecError::FrameLengthOverflow)?;
    let expected_sealed_length = slot.sealed_log_length()?;
    if stored_sealed_length != expected_sealed_length {
        return Err(CodecError::SealedLogLengthMismatch {
            expected: expected_sealed_length,
            actual: stored_sealed_length,
        });
    }
    let expected_crc = read_u32(encoded, 100).ok_or(CodecError::FrameLengthOverflow)?;
    let actual_crc = crc32(&encoded[..100]);
    if expected_crc != actual_crc {
        return Err(CodecError::AcknowledgementSlotCrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    Ok(AcknowledgementSlotState::Populated(slot))
}

pub(crate) fn decode_acknowledgement_file(encoded: &[u8]) -> Result<AcknowledgementFile, CodecError> {
    if encoded.len() != ACKNOWLEDGEMENT_FILE_LENGTH {
        return Err(CodecError::InvalidFixedStructureLength {
            structure: "acknowledgement file",
            expected: ACKNOWLEDGEMENT_FILE_LENGTH,
            actual: encoded.len(),
        });
    }
    let first = decode_acknowledgement_slot(&encoded[..ACKNOWLEDGEMENT_SLOT_LENGTH])?;
    let second = decode_acknowledgement_slot(&encoded[ACKNOWLEDGEMENT_SLOT_LENGTH..])?;
    let slots = [first, second];
    validate_slot_position(&slots[0], 0)?;
    validate_slot_position(&slots[1], 1)?;
    let authoritative_index = select_authoritative(&slots)?;
    Ok(AcknowledgementFile {
        slots,
        authoritative_index,
    })
}

pub(crate) fn encode_acknowledgement_file(
    slots: &[AcknowledgementSlotState; 2],
) -> Result<[u8; ACKNOWLEDGEMENT_FILE_LENGTH], CodecError> {
    let mut encoded = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    for (physical_index, state) in slots.iter().enumerate() {
        validate_slot_position(state, physical_index)?;
        let AcknowledgementSlotState::Populated(slot) = state else {
            continue;
        };
        let start = physical_index * ACKNOWLEDGEMENT_SLOT_LENGTH;
        let end = start + ACKNOWLEDGEMENT_SLOT_LENGTH;
        encoded[start..end].copy_from_slice(&encode_acknowledgement_slot(slot)?);
    }
    decode_acknowledgement_file(&encoded)?;
    Ok(encoded)
}

pub(crate) fn encode_commit_seal(seal: &CommitSeal) -> Result<[u8; COMMIT_SEAL_LENGTH], CodecError> {
    seal.validate()?;
    let mut encoded = [0_u8; COMMIT_SEAL_LENGTH];
    encoded[0..4].copy_from_slice(&COMMIT_SEAL_MAGIC);
    encoded[4..6].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    encoded[6..8].copy_from_slice(&FORMAT_MINOR.to_le_bytes());
    encoded[8..10].copy_from_slice(&(COMMIT_SEAL_LENGTH as u16).to_le_bytes());
    encoded[10] = seal.acknowledgement_slot_index;
    encoded[11] = u8::from(seal.activated);
    encoded[16..24].copy_from_slice(&seal.acknowledgement_epoch.to_le_bytes());
    encoded[24..32].copy_from_slice(&seal.marker_epoch.to_le_bytes());
    encoded[32..40].copy_from_slice(&seal.log_generation.to_le_bytes());
    encoded[40..48].copy_from_slice(&seal.frame_sequence.to_le_bytes());
    encoded[48..56].copy_from_slice(&seal.frame_end_offset.to_le_bytes());
    encoded[56..60].copy_from_slice(&seal.frame_crc32.to_le_bytes());
    encoded[60..64].copy_from_slice(&seal.acknowledgement_slot_crc32.to_le_bytes());
    let stored_crc = crc32(&encoded[..68]);
    encoded[68..72].copy_from_slice(&stored_crc.to_le_bytes());
    Ok(encoded)
}

pub(crate) fn decode_commit_seal(encoded: &[u8]) -> Result<CommitSeal, CodecError> {
    if encoded.len() != COMMIT_SEAL_LENGTH {
        return Err(CodecError::InvalidFixedStructureLength {
            structure: "commit seal",
            expected: COMMIT_SEAL_LENGTH,
            actual: encoded.len(),
        });
    }
    let magic = read_array(encoded, 0).ok_or(CodecError::FrameLengthOverflow)?;
    if magic != COMMIT_SEAL_MAGIC {
        return Err(CodecError::InvalidCommitSealMagic { found: magic });
    }
    validate_version_and_length(encoded, COMMIT_SEAL_LENGTH, "commit seal")?;
    let flags = *encoded.get(11).ok_or(CodecError::FrameLengthOverflow)?;
    if flags & !ACTIVATED_FLAG != 0 {
        return Err(CodecError::InvalidAcknowledgementFlags { flags });
    }
    require_zero(encoded, 12..16, "commit_seal_reserved")?;
    require_zero(encoded, 64..68, "commit_seal_reserved")?;
    let seal = CommitSeal {
        acknowledgement_slot_index: *encoded.get(10).ok_or(CodecError::FrameLengthOverflow)?,
        activated: flags & ACTIVATED_FLAG != 0,
        acknowledgement_epoch: read_u64(encoded, 16).ok_or(CodecError::FrameLengthOverflow)?,
        marker_epoch: read_u64(encoded, 24).ok_or(CodecError::FrameLengthOverflow)?,
        log_generation: read_u64(encoded, 32).ok_or(CodecError::FrameLengthOverflow)?,
        frame_sequence: read_u64(encoded, 40).ok_or(CodecError::FrameLengthOverflow)?,
        frame_end_offset: read_u64(encoded, 48).ok_or(CodecError::FrameLengthOverflow)?,
        frame_crc32: read_u32(encoded, 56).ok_or(CodecError::FrameLengthOverflow)?,
        acknowledgement_slot_crc32: read_u32(encoded, 60).ok_or(CodecError::FrameLengthOverflow)?,
    };
    seal.validate()?;
    let expected_crc = read_u32(encoded, 68).ok_or(CodecError::FrameLengthOverflow)?;
    let actual_crc = crc32(&encoded[..68]);
    if expected_crc != actual_crc {
        return Err(CodecError::CommitSealCrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    Ok(seal)
}

pub(crate) fn validate_commit_seal_against_slot(
    seal: &CommitSeal,
    slot: &AcknowledgementSlot,
    encoded_slot: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
) -> Result<(), CodecError> {
    let expected = CommitSeal::from_acknowledgement_slot(slot, encoded_slot)?;
    if *seal != expected {
        return Err(CodecError::CommitSealSlotMismatch);
    }
    Ok(())
}

pub(crate) fn validate_acknowledged_frame(
    frame: &DecodedFrame<'_>,
    encoded_frame: &[u8],
    frame_start_offset: u64,
    slot: &AcknowledgementSlot,
    seal: &CommitSeal,
    encoded_slot: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
) -> Result<(), CodecError> {
    if encoded_frame.len() != frame.encoded_len() {
        return Err(CodecError::AcknowledgedFrameBindingMismatch {
            field: "encoded_frame_length",
        });
    }
    let DecodeOutcome::Frame(decoded) = decode_next_frame(encoded_frame, frame.sequence(), frame.log_generation())?
    else {
        return Err(CodecError::AcknowledgedFrameBindingMismatch {
            field: "encoded_frame_completeness",
        });
    };
    if decoded.encoded_len() != encoded_frame.len()
        || decoded.record_type() != frame.record_type()
        || decoded.payload() != frame.payload()
    {
        return Err(CodecError::AcknowledgedFrameBindingMismatch { field: "decoded_frame" });
    }
    if slot.log_generation != frame.log_generation() {
        return Err(CodecError::AcknowledgedFrameBindingMismatch {
            field: "log_generation",
        });
    }
    if slot.frame_sequence != frame.sequence() {
        return Err(CodecError::AcknowledgedFrameBindingMismatch {
            field: "frame_sequence",
        });
    }
    let encoded_length = u64::try_from(encoded_frame.len()).map_err(|_| CodecError::AcknowledgedFrameOffsetOverflow)?;
    let expected_end = frame_start_offset
        .checked_add(encoded_length)
        .ok_or(CodecError::AcknowledgedFrameOffsetOverflow)?;
    if slot.frame_end_offset != expected_end {
        return Err(CodecError::AcknowledgedFrameBindingMismatch {
            field: "frame_end_offset",
        });
    }
    if slot.frame_crc32 != crc32(encoded_frame) {
        return Err(CodecError::AcknowledgedFrameBindingMismatch { field: "frame_crc32" });
    }
    validate_commit_seal_against_slot(seal, slot, encoded_slot)
}

fn validate_slot_position(state: &AcknowledgementSlotState, physical_index: usize) -> Result<(), CodecError> {
    let AcknowledgementSlotState::Populated(slot) = state else {
        return Ok(());
    };
    let physical_slot_index = u8::try_from(physical_index)
        .map_err(|_| CodecError::InvalidAcknowledgementSlotIndex { slot_index: u8::MAX })?;
    if slot.slot_index != physical_slot_index {
        return Err(CodecError::AcknowledgementSlotPositionMismatch {
            physical_slot_index,
            encoded_slot_index: slot.slot_index,
        });
    }
    Ok(())
}

fn select_authoritative(slots: &[AcknowledgementSlotState; 2]) -> Result<Option<usize>, CodecError> {
    match (&slots[0], &slots[1]) {
        (AcknowledgementSlotState::Unused, AcknowledgementSlotState::Unused) => Ok(None),
        (AcknowledgementSlotState::Populated(slot), AcknowledgementSlotState::Unused) => select_single_slot(0, slot),
        (AcknowledgementSlotState::Unused, AcknowledgementSlotState::Populated(slot)) => select_single_slot(1, slot),
        (AcknowledgementSlotState::Populated(first), AcknowledgementSlotState::Populated(second)) => {
            if first.store_uuid != second.store_uuid || first.bootstrap_id != second.bootstrap_id {
                return Err(CodecError::AcknowledgementStoreIdentityMismatch);
            }
            let (older, newer, newer_index) = if first.acknowledgement_epoch < second.acknowledgement_epoch {
                (first, second, 1)
            } else {
                (second, first, 0)
            };
            if older.acknowledgement_epoch.checked_add(1) != Some(newer.acknowledgement_epoch) {
                return Err(CodecError::NonConsecutiveAcknowledgementEpochs {
                    first_epoch: first.acknowledgement_epoch,
                    second_epoch: second.acknowledgement_epoch,
                });
            }
            if older.activated && !newer.activated {
                return Err(CodecError::AcknowledgementActivationRegressed);
            }
            Ok(Some(newer_index))
        }
    }
}

fn select_single_slot(index: usize, slot: &AcknowledgementSlot) -> Result<Option<usize>, CodecError> {
    if slot.acknowledgement_epoch != 1 {
        return Err(CodecError::AcknowledgementHistoryMissing {
            acknowledgement_epoch: slot.acknowledgement_epoch,
        });
    }
    Ok(Some(index))
}

fn validate_acknowledgement_identity(
    slot_index: u8,
    activated: bool,
    acknowledgement_epoch: u64,
    marker_epoch: u64,
    frame_sequence: u64,
) -> Result<(), CodecError> {
    if slot_index > 1 {
        return Err(CodecError::InvalidAcknowledgementSlotIndex { slot_index });
    }
    if acknowledgement_epoch == 0 {
        return Err(CodecError::ZeroAcknowledgementEpoch);
    }
    let expected_slot_index = ((acknowledgement_epoch - 1) & 1) as u8;
    if slot_index != expected_slot_index {
        return Err(CodecError::AcknowledgementSlotParityMismatch {
            acknowledgement_epoch,
            expected_slot_index,
            actual_slot_index: slot_index,
        });
    }
    if activated != (marker_epoch != 0) {
        return Err(CodecError::AcknowledgementActivationMarkerMismatch);
    }
    if frame_sequence == 0 {
        return Err(CodecError::ZeroSequence);
    }
    Ok(())
}

fn validate_version_and_length(
    encoded: &[u8],
    expected_length: usize,
    structure: &'static str,
) -> Result<(), CodecError> {
    let major = read_u16(encoded, 4).ok_or(CodecError::FrameLengthOverflow)?;
    let minor = read_u16(encoded, 6).ok_or(CodecError::FrameLengthOverflow)?;
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(CodecError::UnsupportedFormatVersion { major, minor });
    }
    let actual_length = usize::from(read_u16(encoded, 8).ok_or(CodecError::FrameLengthOverflow)?);
    if actual_length != expected_length {
        return Err(CodecError::InvalidFixedStructureLength {
            structure,
            expected: expected_length,
            actual: actual_length,
        });
    }
    Ok(())
}

fn require_zero(encoded: &[u8], range: std::ops::Range<usize>, field: &'static str) -> Result<(), CodecError> {
    let bytes = encoded.get(range).ok_or(CodecError::FrameLengthOverflow)?;
    if let Some(value) = bytes.iter().copied().find(|value| *value != 0) {
        return Err(CodecError::NonZeroReserved {
            field,
            value: u64::from(value),
        });
    }
    Ok(())
}
