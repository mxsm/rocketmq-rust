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

use std::cmp::Ordering;

use super::super::codec::crc32;
use super::super::identity::StoreUuid;
use super::snapshot_payload::decode_payload;
use super::snapshot_payload::encode_payload;
use super::snapshot_payload::entry_kind;
use super::snapshot_payload::kind_payload_max;
use super::types::LifecycleSnapshot;
use super::types::SnapshotEntry;
use super::types::SnapshotMode;
use super::SidecarError;
use super::MAX_SNAPSHOT_BODY_LENGTH;
use super::MAX_SNAPSHOT_ENTRY_COUNT;
use super::MAX_SNAPSHOT_ENTRY_PAYLOAD_LENGTH;
use super::MIN_SNAPSHOT_FILE_LENGTH;
use super::SNAPSHOT_HEADER_LENGTH;

const SNAPSHOT_MAGIC: [u8; 4] = *b"RMSN";
const FORMAT_MAJOR: u16 = 1;
const FORMAT_MINOR: u16 = 0;
const ENTRY_VERSION: u16 = 1;
const ENTRY_HEADER_LENGTH: usize = 8;
const ENTRY_CRC_LENGTH: usize = 4;
const MIN_FRAMED_ENTRY_LENGTH: usize = ENTRY_HEADER_LENGTH + ENTRY_CRC_LENGTH;

pub(crate) fn encode_snapshot(snapshot: &LifecycleSnapshot) -> Result<Vec<u8>, SidecarError> {
    validate_header_relationships(snapshot)?;
    let entry_count = validate_entry_count(snapshot.entries.len())?;

    let mut entries = snapshot.entries.clone();
    entries.sort_by(compare_entries);
    validate_canonical_entries(&entries)?;
    validate_high_waters(snapshot, &entries)?;

    let mut body = Vec::new();
    for entry in &entries {
        let kind = entry_kind(entry);
        let payload = encode_payload(entry, snapshot.store_uuid, snapshot.base_sequence)?;
        validate_payload_length(kind, payload.len())?;
        let payload_length = u32::try_from(payload.len()).map_err(|_| SidecarError::SnapshotEntryPayloadTooLarge {
            kind,
            length: payload.len(),
            maximum: MAX_SNAPSHOT_ENTRY_PAYLOAD_LENGTH,
        })?;
        let entry_length = ENTRY_HEADER_LENGTH
            .checked_add(payload.len())
            .and_then(|value| value.checked_add(ENTRY_CRC_LENGTH))
            .ok_or(SidecarError::SnapshotLengthOverflow)?;
        let prospective_body_length = body
            .len()
            .checked_add(entry_length)
            .ok_or(SidecarError::SnapshotLengthOverflow)?;
        validate_body_length(prospective_body_length as u64)?;

        let entry_start = body.len();
        push_u16(&mut body, kind);
        push_u16(&mut body, ENTRY_VERSION);
        push_u32(&mut body, payload_length);
        body.extend_from_slice(&payload);
        let checksum = crc32(&body[entry_start..]);
        push_u32(&mut body, checksum);
    }

    let total_length = MIN_SNAPSHOT_FILE_LENGTH
        .checked_add(body.len())
        .ok_or(SidecarError::SnapshotLengthOverflow)?;
    let total_length_u64 = u64::try_from(total_length).map_err(|_| SidecarError::SnapshotLengthOverflow)?;
    let body_length_u64 = u64::try_from(body.len()).map_err(|_| SidecarError::SnapshotLengthOverflow)?;

    let mut output = Vec::with_capacity(total_length);
    output.extend_from_slice(&SNAPSHOT_MAGIC);
    push_u16(&mut output, FORMAT_MAJOR);
    push_u16(&mut output, FORMAT_MINOR);
    push_u16(&mut output, SNAPSHOT_HEADER_LENGTH as u16);
    push_u16(&mut output, mode_flags(snapshot.mode));
    push_u64(&mut output, total_length_u64);
    output.extend_from_slice(snapshot.store_uuid.as_bytes());
    push_u64(&mut output, snapshot.generation);
    push_u64(&mut output, snapshot.log_generation);
    push_u64(&mut output, snapshot.predecessor_log_generation);
    push_u64(&mut output, snapshot.base_sequence);
    push_u64(&mut output, snapshot.create_high_water);
    push_u64(&mut output, snapshot.ticket_high_water);
    push_u32(&mut output, entry_count);
    push_u64(&mut output, body_length_u64);
    push_u32(&mut output, 0);
    let header_checksum = crc32(&output);
    push_u32(&mut output, header_checksum);
    output.extend_from_slice(&body);
    push_u32(&mut output, crc32(&body));
    Ok(output)
}

pub(crate) fn decode_snapshot(input: &[u8]) -> Result<LifecycleSnapshot, SidecarError> {
    if input.len() < MIN_SNAPSHOT_FILE_LENGTH {
        return Err(SidecarError::SnapshotTooShort {
            actual: input.len(),
            minimum: MIN_SNAPSHOT_FILE_LENGTH,
        });
    }
    let magic = read_array::<4>(input, 0)?;
    if magic != SNAPSHOT_MAGIC {
        return Err(SidecarError::InvalidMagic {
            structure: "snapshot",
            found: magic,
        });
    }
    let major = read_u16(input, 4)?;
    let minor = read_u16(input, 6)?;
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(SidecarError::UnsupportedVersion {
            structure: "snapshot",
            major,
            minor,
        });
    }
    let header_length = read_u16(input, 8)?;
    if usize::from(header_length) != SNAPSHOT_HEADER_LENGTH {
        return Err(SidecarError::InvalidLengthField {
            structure: "snapshot header",
            expected: SNAPSHOT_HEADER_LENGTH as u64,
            actual: u64::from(header_length),
        });
    }
    let mode = mode_from_flags(read_u16(input, 10)?)?;
    let total_length = read_u64(input, 12)?;
    let body_length = read_u64(input, 88)?;
    validate_body_length(body_length)?;
    let entry_count = read_u32(input, 84)?;
    if entry_count > MAX_SNAPSHOT_ENTRY_COUNT {
        return Err(SidecarError::SnapshotEntryCountTooLarge {
            count: u64::from(entry_count),
            maximum: u64::from(MAX_SNAPSHOT_ENTRY_COUNT),
        });
    }
    let minimum_body_length = u64::from(entry_count)
        .checked_mul(MIN_FRAMED_ENTRY_LENGTH as u64)
        .ok_or(SidecarError::SnapshotLengthOverflow)?;
    if minimum_body_length > body_length {
        return Err(SidecarError::SnapshotEntryCountExceedsBody {
            count: u64::from(entry_count),
            body_length,
        });
    }
    let reserved = read_u32(input, 96)?;
    if reserved != 0 {
        return Err(SidecarError::NonZeroReserved {
            field: "snapshot_header.reserved",
            value: u64::from(reserved),
        });
    }
    require_crc("snapshot header", &input[..100], read_u32(input, 100)?)?;

    let expected_total = (MIN_SNAPSHOT_FILE_LENGTH as u64)
        .checked_add(body_length)
        .ok_or(SidecarError::SnapshotLengthOverflow)?;
    if total_length != expected_total {
        return Err(SidecarError::InvalidLengthField {
            structure: "snapshot total",
            expected: expected_total,
            actual: total_length,
        });
    }
    let expected_usize = usize::try_from(total_length).map_err(|_| SidecarError::SnapshotLengthOverflow)?;
    if input.len() != expected_usize {
        return Err(SidecarError::InvalidLength {
            structure: "snapshot",
            expected: expected_usize,
            actual: input.len(),
        });
    }

    let store_uuid = StoreUuid::new(read_array(input, 20)?).map_err(|source| SidecarError::InvalidIdentity {
        field: "store_uuid",
        source,
    })?;
    let snapshot = LifecycleSnapshot {
        mode,
        store_uuid,
        generation: read_u64(input, 36)?,
        log_generation: read_u64(input, 44)?,
        predecessor_log_generation: read_u64(input, 52)?,
        base_sequence: read_u64(input, 60)?,
        create_high_water: read_u64(input, 68)?,
        ticket_high_water: read_u64(input, 76)?,
        entries: Vec::new(),
    };
    validate_header_relationships(&snapshot)?;

    let body_length_usize = usize::try_from(body_length).map_err(|_| SidecarError::SnapshotLengthOverflow)?;
    let body_end = SNAPSHOT_HEADER_LENGTH
        .checked_add(body_length_usize)
        .ok_or(SidecarError::SnapshotLengthOverflow)?;
    let body = input
        .get(SNAPSHOT_HEADER_LENGTH..body_end)
        .ok_or(SidecarError::SnapshotLengthOverflow)?;
    let expected_body_crc = read_u32(input, body_end)?;
    let actual_body_crc = crc32(body);
    if expected_body_crc != actual_body_crc {
        return Err(SidecarError::SnapshotBodyChecksumMismatch {
            expected: expected_body_crc,
            actual: actual_body_crc,
        });
    }

    let entries = decode_entries(body, entry_count, store_uuid, snapshot.base_sequence)?;
    validate_high_waters(&snapshot, &entries)?;
    Ok(LifecycleSnapshot { entries, ..snapshot })
}

fn decode_entries(
    body: &[u8],
    entry_count: u32,
    store_uuid: StoreUuid,
    base_sequence: u64,
) -> Result<Vec<SnapshotEntry>, SidecarError> {
    let mut entries = Vec::new();
    let mut offset = 0_usize;
    for _ in 0..entry_count {
        let remaining = body.len().saturating_sub(offset);
        if remaining < ENTRY_HEADER_LENGTH {
            return Err(SidecarError::TruncatedSnapshotEntry {
                kind: 0,
                offset,
                needed: ENTRY_HEADER_LENGTH,
                remaining,
            });
        }
        let kind = read_u16(body, offset)?;
        if kind_payload_max(kind).is_none() {
            return Err(SidecarError::InvalidSnapshotEntryKind { kind });
        }
        let version = read_u16(body, offset + 2)?;
        if version != ENTRY_VERSION {
            return Err(SidecarError::UnsupportedSnapshotEntryVersion { kind, version });
        }
        let payload_length =
            usize::try_from(read_u32(body, offset + 4)?).map_err(|_| SidecarError::SnapshotLengthOverflow)?;
        validate_payload_length(kind, payload_length)?;
        let entry_end = offset
            .checked_add(ENTRY_HEADER_LENGTH)
            .and_then(|value| value.checked_add(payload_length))
            .and_then(|value| value.checked_add(ENTRY_CRC_LENGTH))
            .ok_or(SidecarError::SnapshotLengthOverflow)?;
        if entry_end > body.len() {
            return Err(SidecarError::TruncatedSnapshotEntry {
                kind,
                offset,
                needed: entry_end - offset,
                remaining,
            });
        }
        let crc_offset = entry_end - ENTRY_CRC_LENGTH;
        let expected_crc = read_u32(body, crc_offset)?;
        let actual_crc = crc32(&body[offset..crc_offset]);
        if expected_crc != actual_crc {
            return Err(SidecarError::SnapshotEntryChecksumMismatch {
                kind,
                expected: expected_crc,
                actual: actual_crc,
            });
        }
        let payload_start = offset + ENTRY_HEADER_LENGTH;
        let entry = decode_payload(kind, &body[payload_start..crc_offset], store_uuid, base_sequence)?;
        if let Some(previous) = entries.last() {
            match compare_entries(previous, &entry) {
                Ordering::Greater => return Err(SidecarError::NonCanonicalSnapshotOrder),
                Ordering::Equal => return Err(SidecarError::DuplicateSnapshotEntry { kind }),
                Ordering::Less => {}
            }
        }
        entries.push(entry);
        offset = entry_end;
    }
    if offset != body.len() {
        return Err(SidecarError::TrailingSnapshotBody {
            remaining: body.len() - offset,
        });
    }
    Ok(entries)
}

fn validate_header_relationships(snapshot: &LifecycleSnapshot) -> Result<(), SidecarError> {
    if snapshot.generation != snapshot.log_generation {
        return Err(SidecarError::SnapshotGenerationMismatch {
            snapshot: snapshot.generation,
            log: snapshot.log_generation,
        });
    }
    let expected_predecessor = if snapshot.generation == 0 {
        u64::MAX
    } else {
        snapshot.generation - 1
    };
    if snapshot.predecessor_log_generation != expected_predecessor {
        return Err(SidecarError::InvalidSnapshotPredecessor {
            generation: snapshot.generation,
            predecessor: snapshot.predecessor_log_generation,
        });
    }
    let valid_mode_generation = match snapshot.mode {
        SnapshotMode::BootstrapInventory => snapshot.generation == 0,
        SnapshotMode::OrdinaryCompaction | SnapshotMode::TailRepair => snapshot.generation > 0,
    };
    if !valid_mode_generation {
        return Err(SidecarError::SnapshotModeGenerationMismatch {
            mode: mode_name(snapshot.mode),
            generation: snapshot.generation,
        });
    }
    if snapshot.base_sequence == 0 {
        return Err(SidecarError::ZeroSnapshotBaseSequence);
    }
    Ok(())
}

fn validate_entry_count(length: usize) -> Result<u32, SidecarError> {
    if length > MAX_SNAPSHOT_ENTRY_COUNT as usize {
        return Err(SidecarError::SnapshotEntryCountTooLarge {
            count: u64::try_from(length).unwrap_or(u64::MAX),
            maximum: u64::from(MAX_SNAPSHOT_ENTRY_COUNT),
        });
    }
    u32::try_from(length).map_err(|_| SidecarError::SnapshotEntryCountTooLarge {
        count: u64::MAX,
        maximum: u64::from(MAX_SNAPSHOT_ENTRY_COUNT),
    })
}

fn validate_body_length(length: u64) -> Result<(), SidecarError> {
    if length > MAX_SNAPSHOT_BODY_LENGTH as u64 {
        return Err(SidecarError::SnapshotBodyTooLarge {
            length,
            maximum: MAX_SNAPSHOT_BODY_LENGTH as u64,
        });
    }
    Ok(())
}

fn validate_payload_length(kind: u16, length: usize) -> Result<(), SidecarError> {
    if length > MAX_SNAPSHOT_ENTRY_PAYLOAD_LENGTH {
        return Err(SidecarError::SnapshotEntryPayloadTooLarge {
            kind,
            length,
            maximum: MAX_SNAPSHOT_ENTRY_PAYLOAD_LENGTH,
        });
    }
    let maximum = kind_payload_max(kind).ok_or(SidecarError::InvalidSnapshotEntryKind { kind })?;
    if length > maximum {
        return Err(SidecarError::SnapshotEntryPayloadTooLarge { kind, length, maximum });
    }
    Ok(())
}

fn validate_canonical_entries(entries: &[SnapshotEntry]) -> Result<(), SidecarError> {
    for pair in entries.windows(2) {
        if compare_entries(&pair[0], &pair[1]) == Ordering::Equal {
            return Err(SidecarError::DuplicateSnapshotEntry {
                kind: entry_kind(&pair[0]),
            });
        }
    }
    Ok(())
}

fn validate_high_waters(snapshot: &LifecycleSnapshot, entries: &[SnapshotEntry]) -> Result<(), SidecarError> {
    for entry in entries {
        match entry {
            SnapshotEntry::Incarnation(entry) => {
                require_high_water(
                    "create_high_water",
                    snapshot.create_high_water,
                    entry.incarnation.create_seq(),
                )?;
            }
            SnapshotEntry::RetirementTicket(entry) => {
                require_high_water(
                    "create_high_water",
                    snapshot.create_high_water,
                    entry.incarnation.create_seq(),
                )?;
                require_high_water("ticket_high_water", snapshot.ticket_high_water, entry.ticket_id.get())?;
            }
            SnapshotEntry::Quarantine(_) => {}
        }
    }
    Ok(())
}

fn require_high_water(field: &'static str, high_water: u64, represented: u64) -> Result<(), SidecarError> {
    if high_water < represented {
        return Err(SidecarError::HighWaterBelowRepresented {
            field,
            high_water,
            represented,
        });
    }
    Ok(())
}

fn compare_entries(left: &SnapshotEntry, right: &SnapshotEntry) -> Ordering {
    match (left, right) {
        (SnapshotEntry::Incarnation(left), SnapshotEntry::Incarnation(right)) => {
            left.incarnation.cmp(&right.incarnation)
        }
        (SnapshotEntry::RetirementTicket(left), SnapshotEntry::RetirementTicket(right)) => {
            left.ticket_id.cmp(&right.ticket_id)
        }
        (SnapshotEntry::Quarantine(left), SnapshotEntry::Quarantine(right)) => {
            left.source_path.as_bytes().cmp(right.source_path.as_bytes())
        }
        _ => entry_rank(left).cmp(&entry_rank(right)),
    }
}

const fn entry_rank(entry: &SnapshotEntry) -> u8 {
    match entry {
        SnapshotEntry::Incarnation(_) => 1,
        SnapshotEntry::RetirementTicket(_) => 2,
        SnapshotEntry::Quarantine(_) => 3,
    }
}

const fn mode_flags(mode: SnapshotMode) -> u16 {
    match mode {
        SnapshotMode::OrdinaryCompaction => 0,
        SnapshotMode::BootstrapInventory => 1,
        SnapshotMode::TailRepair => 2,
    }
}

const fn mode_name(mode: SnapshotMode) -> &'static str {
    match mode {
        SnapshotMode::OrdinaryCompaction => "ordinary_compaction",
        SnapshotMode::BootstrapInventory => "bootstrap_inventory",
        SnapshotMode::TailRepair => "tail_repair",
    }
}

fn mode_from_flags(flags: u16) -> Result<SnapshotMode, SidecarError> {
    match flags {
        0 => Ok(SnapshotMode::OrdinaryCompaction),
        1 => Ok(SnapshotMode::BootstrapInventory),
        2 => Ok(SnapshotMode::TailRepair),
        value => Err(SidecarError::InvalidFlags {
            field: "snapshot_header",
            value: u64::from(value),
        }),
    }
}

fn require_crc(structure: &'static str, covered: &[u8], expected: u32) -> Result<(), SidecarError> {
    let actual = crc32(covered);
    if actual != expected {
        return Err(SidecarError::ChecksumMismatch {
            structure,
            expected,
            actual,
        });
    }
    Ok(())
}

fn read_array<const N: usize>(input: &[u8], offset: usize) -> Result<[u8; N], SidecarError> {
    let end = offset.checked_add(N).ok_or(SidecarError::SnapshotLengthOverflow)?;
    input
        .get(offset..end)
        .ok_or(SidecarError::SnapshotLengthOverflow)?
        .try_into()
        .map_err(|_| SidecarError::SnapshotLengthOverflow)
}

fn read_u16(input: &[u8], offset: usize) -> Result<u16, SidecarError> {
    Ok(u16::from_le_bytes(read_array(input, offset)?))
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, SidecarError> {
    Ok(u32::from_le_bytes(read_array(input, offset)?))
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64, SidecarError> {
    Ok(u64::from_le_bytes(read_array(input, offset)?))
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
