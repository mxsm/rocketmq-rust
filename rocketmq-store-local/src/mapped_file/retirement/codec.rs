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

use thiserror::Error;

use super::identity::IdentityViolation;

const FRAME_MAGIC: [u8; 4] = *b"RMLC";
const FORMAT_MAJOR: u16 = 1;
const FORMAT_MINOR: u16 = 0;
const RECORD_VERSION: u16 = 1;
const CRITICAL_FLAG: u16 = 1;
pub(crate) const MIN_HEADER_LENGTH: usize = 40;
pub(crate) const MAX_HEADER_LENGTH: usize = 256;
pub(crate) const MAX_PAYLOAD_LENGTH: usize = 16_384;
pub(crate) const MAX_FRAME_LENGTH: usize = MAX_HEADER_LENGTH + MAX_PAYLOAD_LENGTH + 4;
pub(crate) const ACKNOWLEDGEMENT_SLOT_LENGTH: usize = 104;
pub(crate) const ACKNOWLEDGEMENT_FILE_LENGTH: usize = ACKNOWLEDGEMENT_SLOT_LENGTH * 2;
pub(crate) const COMMIT_SEAL_LENGTH: usize = 72;
pub(crate) const MAX_SEALED_RECORD_UNIT_LENGTH: usize = MAX_FRAME_LENGTH + COMMIT_SEAL_LENGTH;

/// A v1 ledger record type, or a validated skippable noncritical extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordType {
    StoreInitialized,
    BootstrapInstalled,
    LogOpened,
    GenerationPrepared,
    GenerationAborted,
    MarkerCommitted,
    AllocateIncarnation,
    BindIncarnation,
    PublishIncarnation,
    RetirementIntent,
    LogicalRemoved,
    Tombstoned,
    NamespaceAbsent,
    Completed,
    SupersededPath,
    Quarantined,
    Unknown(u16),
}

impl RecordType {
    fn from_wire(value: u16) -> Result<Self, CodecViolation> {
        match value {
            0x0000 => Err(CodecViolation::InvalidRecordTypeZero),
            0x0001 => Ok(Self::StoreInitialized),
            0x0002 => Ok(Self::BootstrapInstalled),
            0x0003 => Ok(Self::LogOpened),
            0x0004 => Ok(Self::GenerationPrepared),
            0x0005 => Ok(Self::GenerationAborted),
            0x0006 => Ok(Self::MarkerCommitted),
            0x0010 => Ok(Self::AllocateIncarnation),
            0x0011 => Ok(Self::BindIncarnation),
            0x0012 => Ok(Self::PublishIncarnation),
            0x0020 => Ok(Self::RetirementIntent),
            0x0021 => Ok(Self::LogicalRemoved),
            0x0022 => Ok(Self::Tombstoned),
            0x0023 => Ok(Self::NamespaceAbsent),
            0x0024 => Ok(Self::Completed),
            0x0025 => Ok(Self::SupersededPath),
            0x0030 => Ok(Self::Quarantined),
            value => Ok(Self::Unknown(value)),
        }
    }

    const fn wire_value(self) -> u16 {
        match self {
            Self::StoreInitialized => 0x0001,
            Self::BootstrapInstalled => 0x0002,
            Self::LogOpened => 0x0003,
            Self::GenerationPrepared => 0x0004,
            Self::GenerationAborted => 0x0005,
            Self::MarkerCommitted => 0x0006,
            Self::AllocateIncarnation => 0x0010,
            Self::BindIncarnation => 0x0011,
            Self::PublishIncarnation => 0x0012,
            Self::RetirementIntent => 0x0020,
            Self::LogicalRemoved => 0x0021,
            Self::Tombstoned => 0x0022,
            Self::NamespaceAbsent => 0x0023,
            Self::Completed => 0x0024,
            Self::SupersededPath => 0x0025,
            Self::Quarantined => 0x0030,
            Self::Unknown(value) => value,
        }
    }

    const fn is_known(self) -> bool {
        !matches!(self, Self::Unknown(_))
    }
}

mod types;
pub(crate) use types::ContentFingerprint;
pub(crate) use types::GenerationAbortReason;
pub(crate) use types::LedgerRecord;
pub(crate) use types::OpenReason;
pub(crate) use types::QuarantineEntityKind;
pub(crate) use types::QuarantineReason;
pub(crate) use types::RetirementReason;

mod acknowledgement;
pub(crate) type AcknowledgementFile = acknowledgement::AcknowledgementFile;
#[allow(
    unused_imports,
    reason = "M3 exposes the ACK/seal facade before the M3.2 durable writer consumes it"
)]
pub(crate) use acknowledgement::{
    decode_acknowledgement_file, decode_acknowledgement_slot, decode_commit_seal, encode_acknowledgement_file,
    encode_acknowledgement_slot, encode_commit_seal, validate_acknowledged_frame, validate_commit_seal_against_slot,
    AcknowledgementSlot, AcknowledgementSlotState, CommitSeal,
};

/// One completely validated frame and its borrowed payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedFrame<'a> {
    record_type: RecordType,
    sequence: u64,
    log_generation: u64,
    payload: &'a [u8],
    encoded_len: usize,
}

impl<'a> DecodedFrame<'a> {
    pub(crate) const fn record_type(&self) -> RecordType {
        self.record_type
    }

    pub(crate) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) const fn log_generation(&self) -> u64 {
        self.log_generation
    }

    pub(crate) const fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub(crate) fn next_sequence(&self) -> Result<u64, CodecViolation> {
        self.sequence.checked_add(1).ok_or(CodecViolation::SequenceOverflow)
    }

    /// Decodes the typed v1 payload, or returns `None` for a validated unknown noncritical record.
    pub(crate) fn decode_record(&self) -> Result<Option<LedgerRecord>, CodecViolation> {
        if !self.record_type.is_known() {
            return Ok(None);
        }
        decode_record_payload(self.record_type, self.sequence, self.log_generation, self.payload).map(Some)
    }
}

/// Result of reading at most one frame from a log suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodeOutcome<'a> {
    EndOfInput,
    TrailingPartial(TrailingPartial),
    Frame(DecodedFrame<'a>),
}

/// A syntactically possible but incomplete frame at physical EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrailingPartial {
    pub(crate) available: usize,
    pub(crate) required: usize,
}

/// A bounded v1 frame encoding or decoding failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum CodecViolation {
    #[error("ledger frame magic is invalid: {found:02x?}")]
    InvalidMagic { found: [u8; 4] },
    #[error("incomplete ledger header diverges from the required v1 prefix at byte {offset}")]
    InvalidHeaderPrefix { offset: usize },
    #[error("incomplete ledger frame field {field} diverges from its required value at byte {offset}")]
    InvalidFieldPrefix { field: &'static str, offset: usize },
    #[error("unsupported ledger format version {major}.{minor}")]
    UnsupportedFormatVersion { major: u16, minor: u16 },
    #[error("record type zero is reserved")]
    InvalidRecordTypeZero,
    #[error("unknown critical record type {record_type:#06x}")]
    UnknownCriticalRecordType { record_type: u16 },
    #[error("known record type {record_type:#06x} has unsupported version {version}")]
    UnsupportedRecordVersion { record_type: u16, version: u16 },
    #[error("invalid record flags {flags:#06x} for record type {record_type:#06x}")]
    InvalidRecordFlags { record_type: u16, flags: u16 },
    #[error("invalid frame header length {length}; expected {minimum}..={maximum}")]
    InvalidHeaderLength {
        length: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error("payload length {length} exceeds maximum {maximum}")]
    PayloadTooLarge { length: usize, maximum: usize },
    #[error("frame length overflow")]
    FrameLengthOverflow,
    #[error("frame length {length} exceeds maximum {maximum}")]
    FrameTooLarge { length: usize, maximum: usize },
    #[error("ledger sequence must be nonzero")]
    ZeroSequence,
    #[error("expected ledger sequence must be nonzero")]
    ZeroExpectedSequence,
    #[error("ledger sequence mismatch: expected {expected}, found {actual}")]
    SequenceMismatch { expected: u64, actual: u64 },
    #[error("ledger sequence domain is exhausted")]
    SequenceOverflow,
    #[error("log generation mismatch: expected {expected}, found {actual}")]
    LogGenerationMismatch { expected: u64, actual: u64 },
    #[error("header CRC mismatch: expected {expected:#010x}, computed {actual:#010x}")]
    HeaderCrcMismatch { expected: u32, actual: u32 },
    #[error("payload CRC mismatch: expected {expected:#010x}, computed {actual:#010x}")]
    PayloadCrcMismatch { expected: u32, actual: u32 },
    #[error("v1 writers cannot encode unknown record type {record_type:#06x}")]
    CannotEncodeUnknownRecordType { record_type: u16 },
    #[error("invalid {field} enum value {value}")]
    InvalidEnumValue { field: &'static str, value: u64 },
    #[error("{field} must not be all zero")]
    ZeroOpaqueIdentifier { field: &'static str },
    #[error("invalid {field}: {source}")]
    InvalidIdentity {
        field: &'static str,
        source: IdentityViolation,
    },
    #[error("{field} is not valid UTF-8")]
    InvalidUtf8Path { field: &'static str },
    #[error(
        "record {record_type:#06x} payload ended at offset {offset}; needed {needed} bytes, only {remaining} remain"
    )]
    UnexpectedPayloadEnd {
        record_type: u16,
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    #[error("record {record_type:#06x} has {remaining} trailing payload bytes at offset {offset}")]
    TrailingPayloadBytes {
        record_type: u16,
        offset: usize,
        remaining: usize,
    },
    #[error("record {record_type:#06x} payload length is {actual}; expected exactly {expected}")]
    InvalidPayloadLength {
        record_type: u16,
        expected: usize,
        actual: usize,
    },
    #[error("record {record_type:#06x} payload length is {actual}; expected {minimum}..={maximum}")]
    InvalidVariablePayloadLength {
        record_type: u16,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    #[error("reserved field {field} must be zero, found {value}")]
    NonZeroReserved { field: &'static str, value: u64 },
    #[error("physical file key kind {kind} is invalid")]
    InvalidPhysicalFileKeyKind { kind: u8 },
    #[error("physical file key reserved bytes must be zero")]
    NonZeroPhysicalFileKeyReserved,
    #[error("absent physical file key must be encoded as 32 zero bytes")]
    InvalidAbsentPhysicalFileKey,
    #[error("optional path {field} presence does not match flags")]
    OptionalPathFlagMismatch { field: &'static str },
    #[error("invalid proof flags {flags:#010x} for {field}")]
    InvalidProofFlags { field: &'static str, flags: u32 },
    #[error("invalid quarantine flags {flags:#06x}")]
    InvalidQuarantineFlags { flags: u16 },
    #[error("generation relationship is invalid: {detail}")]
    InvalidGenerationRelationship { detail: &'static str },
    #[error("record envelope relationship is invalid: {detail}")]
    InvalidEnvelopeRelationship { detail: &'static str },
    #[error("tail-repair fields do not match open reason")]
    InvalidTailRepairFields,
    #[error("mapping generation must be nonzero")]
    ZeroMappingGeneration,
    #[error("expected mapped-file length must be nonzero")]
    ZeroExpectedFileLength,
    #[error("prerequisite NamespaceAbsent sequence must be nonzero and precede Completed")]
    InvalidNamespaceAbsentSequence,
    #[error("marker epoch must be nonzero")]
    ZeroMarkerEpoch,
    #[error("physical marker slot index {slot_index} is invalid")]
    InvalidMarkerSlotIndex { slot_index: u8 },
    #[error("marker epoch {marker_epoch} belongs to slot {expected_slot_index}, not {actual_slot_index}")]
    MarkerSlotParityMismatch {
        marker_epoch: u64,
        expected_slot_index: u8,
        actual_slot_index: u8,
    },
    #[error("{structure} length is {actual}; expected exactly {expected}")]
    InvalidFixedStructureLength {
        structure: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("acknowledgement slot magic is invalid: {found:02x?}")]
    InvalidAcknowledgementMagic { found: [u8; 4] },
    #[error("commit seal magic is invalid: {found:02x?}")]
    InvalidCommitSealMagic { found: [u8; 4] },
    #[error("physical acknowledgement slot index {slot_index} is invalid")]
    InvalidAcknowledgementSlotIndex { slot_index: u8 },
    #[error("acknowledgement flags {flags:#04x} are invalid")]
    InvalidAcknowledgementFlags { flags: u8 },
    #[error("acknowledgement epoch must be nonzero")]
    ZeroAcknowledgementEpoch,
    #[error(
        "acknowledgement epoch {acknowledgement_epoch} belongs to slot {expected_slot_index}, not {actual_slot_index}"
    )]
    AcknowledgementSlotParityMismatch {
        acknowledgement_epoch: u64,
        expected_slot_index: u8,
        actual_slot_index: u8,
    },
    #[error("acknowledgement bytes at physical slot {physical_slot_index} encode slot {encoded_slot_index}")]
    AcknowledgementSlotPositionMismatch {
        physical_slot_index: u8,
        encoded_slot_index: u8,
    },
    #[error("acknowledgement epoch domain is exhausted")]
    AcknowledgementEpochOverflow,
    #[error("acknowledgement activation flag and marker epoch are inconsistent")]
    AcknowledgementActivationMarkerMismatch,
    #[error("acknowledged frame end offset plus seal length overflows u64")]
    SealedLogLengthOverflow,
    #[error("sealed log length mismatch: expected {expected}, found {actual}")]
    SealedLogLengthMismatch { expected: u64, actual: u64 },
    #[error("acknowledgement slot CRC mismatch: expected {expected:#010x}, computed {actual:#010x}")]
    AcknowledgementSlotCrcMismatch { expected: u32, actual: u32 },
    #[error("commit seal CRC mismatch: expected {expected:#010x}, computed {actual:#010x}")]
    CommitSealCrcMismatch { expected: u32, actual: u32 },
    #[error("commit seal does not exactly match its acknowledgement slot")]
    CommitSealSlotMismatch,
    #[error("acknowledgement slot history is missing before epoch {acknowledgement_epoch}")]
    AcknowledgementHistoryMissing { acknowledgement_epoch: u64 },
    #[error("acknowledgement epochs {first_epoch} and {second_epoch} are not consecutive")]
    NonConsecutiveAcknowledgementEpochs { first_epoch: u64, second_epoch: u64 },
    #[error("acknowledgement activation regressed in the newer slot")]
    AcknowledgementActivationRegressed,
    #[error("acknowledgement slots belong to different store identities")]
    AcknowledgementStoreIdentityMismatch,
    #[error("acknowledged frame binding differs at {field}")]
    AcknowledgedFrameBindingMismatch { field: &'static str },
    #[error("acknowledged frame end offset overflows u64")]
    AcknowledgedFrameOffsetOverflow,
}

/// Computes the v1 CRC-32/ISO-HDLC checksum without external dependencies.
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    crc32_parts(&[bytes])
}

fn crc32_parts(parts: &[&[u8]]) -> u32 {
    let mut state = u32::MAX;
    for part in parts {
        for byte in *part {
            state ^= u32::from(*byte);
            for _ in 0..8 {
                state = if state & 1 == 0 {
                    state >> 1
                } else {
                    (state >> 1) ^ 0xedb8_8320
                };
            }
        }
    }
    !state
}

/// Encodes one typed v1 record and validates its relationships to the frame envelope.
pub(crate) fn encode_ledger_frame(
    record: &LedgerRecord,
    sequence: u64,
    log_generation: u64,
) -> Result<Vec<u8>, CodecViolation> {
    validate_envelope_relationships(record, sequence, log_generation)?;
    let payload = encode_record_payload(record, sequence)?;
    encode_frame(record.record_type(), sequence, log_generation, &payload)
}

mod semantics;
use semantics::validate_envelope_relationships;

mod payload;
mod record;
use record::decode_record_payload;
use record::encode_record_payload;

mod schema;
use schema::validate_known_payload_length;
use schema::validate_known_payload_length_prefix;
use schema::validate_known_payload_prefix;

/// Encodes one known v1 record into a fixed-header frame.
fn encode_frame(
    record_type: RecordType,
    sequence: u64,
    log_generation: u64,
    payload: &[u8],
) -> Result<Vec<u8>, CodecViolation> {
    if !record_type.is_known() {
        return Err(CodecViolation::CannotEncodeUnknownRecordType {
            record_type: record_type.wire_value(),
        });
    }
    if sequence == 0 {
        return Err(CodecViolation::ZeroSequence);
    }
    if payload.len() > MAX_PAYLOAD_LENGTH {
        return Err(CodecViolation::PayloadTooLarge {
            length: payload.len(),
            maximum: MAX_PAYLOAD_LENGTH,
        });
    }

    let frame_length = MIN_HEADER_LENGTH
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(4))
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    if frame_length > MAX_FRAME_LENGTH {
        return Err(CodecViolation::FrameTooLarge {
            length: frame_length,
            maximum: MAX_FRAME_LENGTH,
        });
    }
    let payload_length = u32::try_from(payload.len()).map_err(|_| CodecViolation::PayloadTooLarge {
        length: payload.len(),
        maximum: MAX_PAYLOAD_LENGTH,
    })?;

    let mut encoded = Vec::with_capacity(frame_length);
    encoded.extend_from_slice(&FRAME_MAGIC);
    encoded.extend_from_slice(&FORMAT_MAJOR.to_le_bytes());
    encoded.extend_from_slice(&FORMAT_MINOR.to_le_bytes());
    encoded.extend_from_slice(&record_type.wire_value().to_le_bytes());
    encoded.extend_from_slice(&RECORD_VERSION.to_le_bytes());
    encoded.extend_from_slice(&CRITICAL_FLAG.to_le_bytes());
    encoded.extend_from_slice(&(MIN_HEADER_LENGTH as u16).to_le_bytes());
    encoded.extend_from_slice(&payload_length.to_le_bytes());
    encoded.extend_from_slice(&sequence.to_le_bytes());
    encoded.extend_from_slice(&log_generation.to_le_bytes());
    encoded.extend_from_slice(&crc32(&encoded).to_le_bytes());
    encoded.extend_from_slice(payload);
    encoded.extend_from_slice(&crc32(payload).to_le_bytes());
    Ok(encoded)
}

/// Decodes one bounded frame while distinguishing clean EOF from a valid incomplete tail.
pub(crate) fn decode_next_frame(
    input: &[u8],
    expected_sequence: u64,
    expected_log_generation: u64,
) -> Result<DecodeOutcome<'_>, CodecViolation> {
    if input.is_empty() {
        return Ok(DecodeOutcome::EndOfInput);
    }
    if expected_sequence == 0 {
        return Err(CodecViolation::ZeroExpectedSequence);
    }

    validate_fixed_prefix(input)?;
    if input.len() < 8 {
        return Ok(partial(input.len(), MIN_HEADER_LENGTH));
    }

    let major = read_u16(input, 4).ok_or(CodecViolation::FrameLengthOverflow)?;
    let minor = read_u16(input, 6).ok_or(CodecViolation::FrameLengthOverflow)?;
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(CodecViolation::UnsupportedFormatVersion { major, minor });
    }
    if input.len() < 10 {
        return Ok(partial(input.len(), MIN_HEADER_LENGTH));
    }
    let record_type_wire = read_u16(input, 8).ok_or(CodecViolation::FrameLengthOverflow)?;
    let record_type = RecordType::from_wire(record_type_wire)?;
    semantics::validate_initial_record_envelope(record_type, expected_sequence, expected_log_generation)?;
    if input.len() < 12 {
        if record_type.is_known() && matches!(input.get(10), Some(low_byte) if *low_byte != RECORD_VERSION as u8) {
            return Err(CodecViolation::InvalidHeaderPrefix { offset: 10 });
        }
        return Ok(partial(input.len(), MIN_HEADER_LENGTH));
    }
    let record_version = read_u16(input, 10).ok_or(CodecViolation::FrameLengthOverflow)?;
    if record_type.is_known() && record_version != RECORD_VERSION {
        return Err(CodecViolation::UnsupportedRecordVersion {
            record_type: record_type_wire,
            version: record_version,
        });
    }
    if input.len() < 14 {
        let required_low_byte = if record_type.is_known() { CRITICAL_FLAG as u8 } else { 0 };
        if matches!(input.get(12), Some(low_byte) if *low_byte != required_low_byte) {
            return Err(CodecViolation::InvalidHeaderPrefix { offset: 12 });
        }
        return Ok(partial(input.len(), MIN_HEADER_LENGTH));
    }
    let flags = read_u16(input, 12).ok_or(CodecViolation::FrameLengthOverflow)?;
    if flags & !CRITICAL_FLAG != 0 || (record_type.is_known() && flags != CRITICAL_FLAG) {
        return Err(CodecViolation::InvalidRecordFlags {
            record_type: record_type_wire,
            flags,
        });
    }
    if !record_type.is_known() && flags & CRITICAL_FLAG != 0 {
        return Err(CodecViolation::UnknownCriticalRecordType {
            record_type: record_type_wire,
        });
    }
    if input.len() < 16 {
        if let Some(low_byte) = input.get(14).copied() {
            let possible = if record_type.is_known() {
                usize::from(low_byte) == MIN_HEADER_LENGTH
            } else {
                low_byte == 0 || usize::from(low_byte) >= MIN_HEADER_LENGTH
            };
            if !possible {
                return Err(CodecViolation::InvalidHeaderPrefix { offset: 14 });
            }
        }
        return Ok(partial(input.len(), MIN_HEADER_LENGTH));
    }
    let header_length = usize::from(read_u16(input, 14).ok_or(CodecViolation::FrameLengthOverflow)?);
    let maximum_header_length = if record_type.is_known() {
        MIN_HEADER_LENGTH
    } else {
        MAX_HEADER_LENGTH
    };
    if !(MIN_HEADER_LENGTH..=maximum_header_length).contains(&header_length) {
        return Err(CodecViolation::InvalidHeaderLength {
            length: header_length,
            minimum: MIN_HEADER_LENGTH,
            maximum: maximum_header_length,
        });
    }
    if input.len() < 20 {
        let available = input.len() - 16;
        let mut lower_bound_bytes = [0_u8; 4];
        let prefix = input.get(16..).ok_or(CodecViolation::FrameLengthOverflow)?;
        lower_bound_bytes[..available].copy_from_slice(prefix);
        let lower_bound = u32::from_le_bytes(lower_bound_bytes) as usize;
        if lower_bound > MAX_PAYLOAD_LENGTH {
            return Err(CodecViolation::PayloadTooLarge {
                length: lower_bound,
                maximum: MAX_PAYLOAD_LENGTH,
            });
        }
        validate_known_payload_length_prefix(record_type, prefix)?;
        return Ok(partial(input.len(), header_length));
    }
    let payload_length =
        usize::try_from(read_u32(input, 16).ok_or(CodecViolation::FrameLengthOverflow)?).map_err(|_| {
            CodecViolation::PayloadTooLarge {
                length: usize::MAX,
                maximum: MAX_PAYLOAD_LENGTH,
            }
        })?;
    if payload_length > MAX_PAYLOAD_LENGTH {
        return Err(CodecViolation::PayloadTooLarge {
            length: payload_length,
            maximum: MAX_PAYLOAD_LENGTH,
        });
    }
    validate_known_payload_length(record_type, payload_length)?;
    let frame_length = header_length
        .checked_add(payload_length)
        .and_then(|value| value.checked_add(4))
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    if frame_length > MAX_FRAME_LENGTH {
        return Err(CodecViolation::FrameTooLarge {
            length: frame_length,
            maximum: MAX_FRAME_LENGTH,
        });
    }
    if input.len() < 28 {
        validate_available_field_prefix(input, 20, &expected_sequence.to_le_bytes(), "sequence")?;
        return Ok(partial(input.len(), frame_length));
    }
    let sequence = read_u64(input, 20).ok_or(CodecViolation::FrameLengthOverflow)?;
    if sequence == 0 {
        return Err(CodecViolation::ZeroSequence);
    }
    if sequence != expected_sequence {
        return Err(CodecViolation::SequenceMismatch {
            expected: expected_sequence,
            actual: sequence,
        });
    }
    if input.len() < 36 {
        validate_available_field_prefix(input, 28, &expected_log_generation.to_le_bytes(), "log_generation")?;
        return Ok(partial(input.len(), frame_length));
    }
    let log_generation = read_u64(input, 28).ok_or(CodecViolation::FrameLengthOverflow)?;
    if log_generation != expected_log_generation {
        return Err(CodecViolation::LogGenerationMismatch {
            expected: expected_log_generation,
            actual: log_generation,
        });
    }
    if input.len() < header_length {
        if header_length == MIN_HEADER_LENGTH {
            let header_crc = crc32(input.get(..36).ok_or(CodecViolation::FrameLengthOverflow)?);
            validate_available_field_prefix(input, 36, &header_crc.to_le_bytes(), "header_crc32")?;
        }
        return Ok(partial(input.len(), frame_length));
    }

    let expected_header_crc = read_u32(input, 36).ok_or(CodecViolation::FrameLengthOverflow)?;
    let extension = input
        .get(40..header_length)
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    let actual_header_crc = crc32_parts(&[&input[..36], extension]);
    if expected_header_crc != actual_header_crc {
        return Err(CodecViolation::HeaderCrcMismatch {
            expected: expected_header_crc,
            actual: actual_header_crc,
        });
    }
    let payload_end = header_length
        .checked_add(payload_length)
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    if input.len() < payload_end {
        let available_payload = input
            .get(header_length..input.len())
            .ok_or(CodecViolation::FrameLengthOverflow)?;
        validate_known_payload_prefix(record_type, sequence, log_generation, payload_length, available_payload)?;
        return Ok(partial(input.len(), frame_length));
    }
    let payload = input
        .get(header_length..payload_end)
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    let actual_payload_crc = crc32(payload);
    if input.len() < frame_length {
        validate_known_payload_prefix(record_type, sequence, log_generation, payload_length, payload)?;
        validate_available_field_prefix(input, payload_end, &actual_payload_crc.to_le_bytes(), "payload_crc32")?;
        return Ok(partial(input.len(), frame_length));
    }
    let expected_payload_crc = read_u32(input, payload_end).ok_or(CodecViolation::FrameLengthOverflow)?;
    if expected_payload_crc != actual_payload_crc {
        return Err(CodecViolation::PayloadCrcMismatch {
            expected: expected_payload_crc,
            actual: actual_payload_crc,
        });
    }

    Ok(DecodeOutcome::Frame(DecodedFrame {
        record_type,
        sequence,
        log_generation,
        payload,
        encoded_len: frame_length,
    }))
}

fn validate_fixed_prefix(input: &[u8]) -> Result<(), CodecViolation> {
    if input.len() < FRAME_MAGIC.len() {
        if let Some(offset) = input
            .iter()
            .zip(FRAME_MAGIC)
            .position(|(actual, expected)| *actual != expected)
        {
            return Err(CodecViolation::InvalidHeaderPrefix { offset });
        }
        return Ok(());
    }
    let found = read_array::<4>(input, 0).ok_or(CodecViolation::FrameLengthOverflow)?;
    if found != FRAME_MAGIC {
        return Err(CodecViolation::InvalidMagic { found });
    }

    let fixed_version_prefix = [1_u8, 0, 0, 0];
    let available_version_bytes = input.len().saturating_sub(4).min(fixed_version_prefix.len());
    let version_prefix = input
        .get(4..4 + available_version_bytes)
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    if let Some(relative_offset) = version_prefix
        .iter()
        .zip(fixed_version_prefix)
        .position(|(actual, expected)| *actual != expected)
    {
        if input.len() < 8 {
            return Err(CodecViolation::InvalidHeaderPrefix {
                offset: 4 + relative_offset,
            });
        }
    }
    Ok(())
}

fn validate_available_field_prefix(
    input: &[u8],
    offset: usize,
    expected: &[u8],
    field: &'static str,
) -> Result<(), CodecViolation> {
    let available = input.len().saturating_sub(offset).min(expected.len());
    let actual = input
        .get(offset..offset + available)
        .ok_or(CodecViolation::FrameLengthOverflow)?;
    if let Some(relative_offset) = actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected)
    {
        return Err(CodecViolation::InvalidFieldPrefix {
            field,
            offset: offset + relative_offset,
        });
    }
    Ok(())
}

const fn partial(available: usize, required: usize) -> DecodeOutcome<'static> {
    DecodeOutcome::TrailingPartial(TrailingPartial { available, required })
}

fn read_array<const N: usize>(input: &[u8], offset: usize) -> Option<[u8; N]> {
    let end = offset.checked_add(N)?;
    input.get(offset..end)?.try_into().ok()
}

fn read_u16(input: &[u8], offset: usize) -> Option<u16> {
    read_array(input, offset).map(u16::from_le_bytes)
}

fn read_u32(input: &[u8], offset: usize) -> Option<u32> {
    read_array(input, offset).map(u32::from_le_bytes)
}

fn read_u64(input: &[u8], offset: usize) -> Option<u64> {
    read_array(input, offset).map(u64::from_le_bytes)
}

#[cfg(test)]
mod tests;
