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

use super::super::codec::crc32;
use super::super::identity::StoreUuid;
use super::SidecarViolation;
use super::ENABLED_MARKER_FILE_LENGTH;
use super::ENABLED_MARKER_SLOT_LENGTH;
use super::MIN_SNAPSHOT_FILE_LENGTH;
use super::STORE_META_LENGTH;

const FORMAT_MAJOR: u16 = 1;
const FORMAT_MINOR: u16 = 0;
const STORE_META_MAGIC: [u8; 4] = *b"RMSM";
const ENABLED_MARKER_MAGIC: [u8; 4] = *b"RMEN";
const ENABLED_FLAG: u8 = 1;
const MANAGED_RETIREMENT_FEATURE: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreMeta {
    pub(crate) store_uuid: StoreUuid,
    pub(crate) creation_time_ns: u64,
    pub(crate) bootstrap_id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnabledMarkerSlot {
    pub(crate) slot_index: u8,
    pub(crate) store_uuid: StoreUuid,
    pub(crate) bootstrap_id: [u8; 16],
    pub(crate) marker_epoch: u64,
    pub(crate) snapshot_generation: u64,
    pub(crate) log_generation: u64,
    pub(crate) anchor_sequence: u64,
    pub(crate) snapshot_file_length: u64,
    pub(crate) snapshot_file_crc32: u32,
    pub(crate) anchor_frame_crc32: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnabledMarkerFile {
    pub(crate) slots: [Option<EnabledMarkerSlot>; 2],
}

impl EnabledMarkerFile {
    pub(crate) fn selected_slot(&self) -> Result<&EnabledMarkerSlot, SidecarViolation> {
        for (physical, slot) in self.slots.iter().enumerate() {
            if let Some(slot) = slot {
                validate_marker_slot(slot, physical as u8)?;
            }
        }

        match (&self.slots[0], &self.slots[1]) {
            (None, None) => Err(SidecarViolation::NoValidMarkerSlot),
            (Some(slot), None) if slot.marker_epoch == 1 && slot.snapshot_generation == 0 => Ok(slot),
            (Some(_), None) | (None, Some(_)) => Err(SidecarViolation::InvalidMarkerSlotHistory),
            (Some(first), Some(second)) => {
                if first.store_uuid != second.store_uuid || first.bootstrap_id != second.bootstrap_id {
                    return Err(SidecarViolation::MarkerIdentityMismatch);
                }
                let distance = first.marker_epoch.abs_diff(second.marker_epoch);
                if distance != 1 {
                    return Err(SidecarViolation::NonConsecutiveMarkerEpochs {
                        first: first.marker_epoch,
                        second: second.marker_epoch,
                    });
                }
                let (older, newer) = if first.marker_epoch < second.marker_epoch {
                    (first, second)
                } else {
                    (second, first)
                };
                let expected_generation = older.snapshot_generation.checked_add(1).ok_or(
                    SidecarViolation::NonConsecutiveMarkerGenerations {
                        older: older.snapshot_generation,
                        newer: newer.snapshot_generation,
                    },
                )?;
                if newer.snapshot_generation != expected_generation {
                    return Err(SidecarViolation::NonConsecutiveMarkerGenerations {
                        older: older.snapshot_generation,
                        newer: newer.snapshot_generation,
                    });
                }
                if newer.anchor_sequence <= older.anchor_sequence {
                    return Err(SidecarViolation::NonIncreasingMarkerAnchorSequence {
                        older: older.anchor_sequence,
                        newer: newer.anchor_sequence,
                    });
                }
                Ok(newer)
            }
        }
    }
}

pub(crate) fn encode_store_meta(meta: &StoreMeta) -> Result<[u8; STORE_META_LENGTH], SidecarViolation> {
    validate_nonzero_id("bootstrap_id", &meta.bootstrap_id)?;

    let mut encoded = [0_u8; STORE_META_LENGTH];
    encoded[0..4].copy_from_slice(&STORE_META_MAGIC);
    write_u16(&mut encoded, 4, FORMAT_MAJOR);
    write_u16(&mut encoded, 6, FORMAT_MINOR);
    write_u32(&mut encoded, 8, STORE_META_LENGTH as u32);
    encoded[16..32].copy_from_slice(meta.store_uuid.as_bytes());
    write_u64(&mut encoded, 32, meta.creation_time_ns);
    encoded[40..56].copy_from_slice(&meta.bootstrap_id);
    let checksum = crc32(&encoded[..60]);
    write_u32(&mut encoded, 60, checksum);
    Ok(encoded)
}

pub(crate) fn decode_store_meta(input: &[u8]) -> Result<StoreMeta, SidecarViolation> {
    require_exact_length("store.meta", input, STORE_META_LENGTH)?;
    require_magic("store.meta", input, STORE_META_MAGIC)?;
    require_version("store.meta", input)?;
    require_length_field("store.meta", read_u32(input, 8)?.into(), STORE_META_LENGTH as u64)?;
    let flags = read_u32(input, 12)?;
    if flags != 0 {
        return Err(SidecarViolation::InvalidFlags {
            field: "store.meta",
            value: u64::from(flags),
        });
    }
    require_zero("store.meta.reserved", read_u32(input, 56)?.into())?;
    require_crc("store.meta", &input[..60], read_u32(input, 60)?)?;

    let uuid_bytes = read_array::<16>(input, 16)?;
    let store_uuid = StoreUuid::new(uuid_bytes).map_err(|source| SidecarViolation::InvalidIdentity {
        field: "store_uuid",
        source,
    })?;
    let bootstrap_id = read_array::<16>(input, 40)?;
    validate_nonzero_id("bootstrap_id", &bootstrap_id)?;
    Ok(StoreMeta {
        store_uuid,
        creation_time_ns: read_u64(input, 32)?,
        bootstrap_id,
    })
}

pub(crate) fn encode_enabled_marker_slot(
    slot: &EnabledMarkerSlot,
) -> Result<[u8; ENABLED_MARKER_SLOT_LENGTH], SidecarViolation> {
    validate_marker_slot(slot, slot.slot_index)?;

    let mut encoded = [0_u8; ENABLED_MARKER_SLOT_LENGTH];
    encoded[0..4].copy_from_slice(&ENABLED_MARKER_MAGIC);
    write_u16(&mut encoded, 4, FORMAT_MAJOR);
    write_u16(&mut encoded, 6, FORMAT_MINOR);
    write_u16(&mut encoded, 8, ENABLED_MARKER_SLOT_LENGTH as u16);
    encoded[10] = slot.slot_index;
    encoded[11] = ENABLED_FLAG;
    write_u32(&mut encoded, 12, MANAGED_RETIREMENT_FEATURE);
    encoded[16..32].copy_from_slice(slot.store_uuid.as_bytes());
    encoded[32..48].copy_from_slice(&slot.bootstrap_id);
    write_u64(&mut encoded, 48, slot.marker_epoch);
    write_u64(&mut encoded, 56, slot.snapshot_generation);
    write_u64(&mut encoded, 64, slot.log_generation);
    write_u64(&mut encoded, 72, slot.anchor_sequence);
    write_u64(&mut encoded, 80, slot.snapshot_file_length);
    write_u32(&mut encoded, 88, slot.snapshot_file_crc32);
    write_u32(&mut encoded, 92, slot.anchor_frame_crc32);
    let checksum = crc32(&encoded[..100]);
    write_u32(&mut encoded, 100, checksum);
    Ok(encoded)
}

pub(crate) fn decode_enabled_marker_slot(
    input: &[u8],
    physical_slot_index: u8,
) -> Result<EnabledMarkerSlot, SidecarViolation> {
    if physical_slot_index > 1 {
        return Err(SidecarViolation::InvalidMarkerSlotIndex {
            slot_index: physical_slot_index,
        });
    }
    require_exact_length("ENABLED.v1 slot", input, ENABLED_MARKER_SLOT_LENGTH)?;
    require_magic("ENABLED.v1 slot", input, ENABLED_MARKER_MAGIC)?;
    require_version("ENABLED.v1 slot", input)?;
    require_length_field(
        "ENABLED.v1 slot",
        read_u16(input, 8)?.into(),
        ENABLED_MARKER_SLOT_LENGTH as u64,
    )?;
    let declared_slot_index = input[10];
    if declared_slot_index > 1 {
        return Err(SidecarViolation::InvalidMarkerSlotIndex {
            slot_index: declared_slot_index,
        });
    }
    if declared_slot_index != physical_slot_index {
        return Err(SidecarViolation::MarkerSlotPositionMismatch {
            declared: declared_slot_index,
            physical: physical_slot_index,
        });
    }
    if input[11] != ENABLED_FLAG {
        return Err(SidecarViolation::InvalidFlags {
            field: "ENABLED.v1 slot",
            value: u64::from(input[11]),
        });
    }
    let features = read_u32(input, 12)?;
    if features != MANAGED_RETIREMENT_FEATURE {
        return Err(SidecarViolation::InvalidMarkerFeatures { value: features });
    }
    require_zero("ENABLED.v1 slot.reserved", read_u32(input, 96)?.into())?;
    require_crc("ENABLED.v1 slot", &input[..100], read_u32(input, 100)?)?;

    let store_uuid = StoreUuid::new(read_array(input, 16)?).map_err(|source| SidecarViolation::InvalidIdentity {
        field: "store_uuid",
        source,
    })?;
    let slot = EnabledMarkerSlot {
        slot_index: declared_slot_index,
        store_uuid,
        bootstrap_id: read_array(input, 32)?,
        marker_epoch: read_u64(input, 48)?,
        snapshot_generation: read_u64(input, 56)?,
        log_generation: read_u64(input, 64)?,
        anchor_sequence: read_u64(input, 72)?,
        snapshot_file_length: read_u64(input, 80)?,
        snapshot_file_crc32: read_u32(input, 88)?,
        anchor_frame_crc32: read_u32(input, 92)?,
    };
    validate_marker_slot(&slot, physical_slot_index)?;
    Ok(slot)
}

pub(crate) fn encode_enabled_marker_file(
    marker: &EnabledMarkerFile,
) -> Result<[u8; ENABLED_MARKER_FILE_LENGTH], SidecarViolation> {
    marker.selected_slot()?;
    let mut encoded = [0_u8; ENABLED_MARKER_FILE_LENGTH];
    for (index, slot) in marker.slots.iter().enumerate() {
        if let Some(slot) = slot {
            validate_marker_slot(slot, index as u8)?;
            let slot_bytes = encode_enabled_marker_slot(slot)?;
            let start = index * ENABLED_MARKER_SLOT_LENGTH;
            encoded[start..start + ENABLED_MARKER_SLOT_LENGTH].copy_from_slice(&slot_bytes);
        }
    }
    Ok(encoded)
}

pub(crate) fn decode_enabled_marker_file(input: &[u8]) -> Result<EnabledMarkerFile, SidecarViolation> {
    require_exact_length("ENABLED.v1", input, ENABLED_MARKER_FILE_LENGTH)?;
    let decode_slot = |index: usize| {
        let start = index * ENABLED_MARKER_SLOT_LENGTH;
        let bytes = &input[start..start + ENABLED_MARKER_SLOT_LENGTH];
        if bytes.iter().all(|byte| *byte == 0) {
            Ok(None)
        } else {
            decode_enabled_marker_slot(bytes, index as u8).map(Some)
        }
    };
    let slots = [decode_slot(0)?, decode_slot(1)?];
    let marker = EnabledMarkerFile { slots };
    if marker.slots.iter().any(Option::is_some) {
        marker.selected_slot()?;
    }
    Ok(marker)
}

fn validate_marker_slot(slot: &EnabledMarkerSlot, physical: u8) -> Result<(), SidecarViolation> {
    if slot.slot_index > 1 {
        return Err(SidecarViolation::InvalidMarkerSlotIndex {
            slot_index: slot.slot_index,
        });
    }
    if slot.slot_index != physical {
        return Err(SidecarViolation::MarkerSlotPositionMismatch {
            declared: slot.slot_index,
            physical,
        });
    }
    validate_nonzero_id("bootstrap_id", &slot.bootstrap_id)?;
    if slot.marker_epoch == 0 {
        return Err(SidecarViolation::ZeroMarkerEpoch);
    }
    let expected_physical = ((slot.marker_epoch - 1) & 1) as u8;
    if slot.slot_index != expected_physical {
        return Err(SidecarViolation::InvalidMarkerSlotHistory);
    }
    let expected_generation = slot.marker_epoch - 1;
    if slot.snapshot_generation != expected_generation {
        return Err(SidecarViolation::InvalidMarkerSlotHistory);
    }
    if slot.snapshot_generation != slot.log_generation {
        return Err(SidecarViolation::MarkerGenerationMismatch {
            snapshot: slot.snapshot_generation,
            log: slot.log_generation,
        });
    }
    if slot.anchor_sequence == 0 {
        return Err(SidecarViolation::ZeroSnapshotAnchorSequence);
    }
    if slot.marker_epoch == 1 && slot.anchor_sequence != 2 {
        return Err(SidecarViolation::InvalidMarkerSlotHistory);
    }
    if slot.snapshot_file_length < MIN_SNAPSHOT_FILE_LENGTH as u64 {
        return Err(SidecarViolation::MarkerSnapshotTooShort {
            actual: slot.snapshot_file_length,
            minimum: MIN_SNAPSHOT_FILE_LENGTH as u64,
        });
    }
    Ok(())
}

fn validate_nonzero_id(field: &'static str, value: &[u8; 16]) -> Result<(), SidecarViolation> {
    if *value == [0; 16] {
        return Err(SidecarViolation::ZeroOpaqueIdentifier { field });
    }
    Ok(())
}

fn require_exact_length(structure: &'static str, input: &[u8], expected: usize) -> Result<(), SidecarViolation> {
    if input.len() != expected {
        return Err(SidecarViolation::InvalidLength {
            structure,
            expected,
            actual: input.len(),
        });
    }
    Ok(())
}

fn require_magic(structure: &'static str, input: &[u8], expected: [u8; 4]) -> Result<(), SidecarViolation> {
    let found = read_array(input, 0)?;
    if found != expected {
        return Err(SidecarViolation::InvalidMagic { structure, found });
    }
    Ok(())
}

fn require_version(structure: &'static str, input: &[u8]) -> Result<(), SidecarViolation> {
    let major = read_u16(input, 4)?;
    let minor = read_u16(input, 6)?;
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(SidecarViolation::UnsupportedVersion {
            structure,
            major,
            minor,
        });
    }
    Ok(())
}

fn require_length_field(structure: &'static str, actual: u64, expected: u64) -> Result<(), SidecarViolation> {
    if actual != expected {
        return Err(SidecarViolation::InvalidLengthField {
            structure,
            expected,
            actual,
        });
    }
    Ok(())
}

fn require_zero(field: &'static str, value: u64) -> Result<(), SidecarViolation> {
    if value != 0 {
        return Err(SidecarViolation::NonZeroReserved { field, value });
    }
    Ok(())
}

fn require_crc(structure: &'static str, covered: &[u8], expected: u32) -> Result<(), SidecarViolation> {
    let actual = crc32(covered);
    if actual != expected {
        return Err(SidecarViolation::ChecksumMismatch {
            structure,
            expected,
            actual,
        });
    }
    Ok(())
}

fn read_array<const N: usize>(input: &[u8], offset: usize) -> Result<[u8; N], SidecarViolation> {
    let end = offset
        .checked_add(N)
        .ok_or(SidecarViolation::UnexpectedFixedSidecarEnd {
            offset,
            needed: N,
            remaining: input.len().saturating_sub(offset),
        })?;
    input
        .get(offset..end)
        .ok_or(SidecarViolation::UnexpectedFixedSidecarEnd {
            offset,
            needed: N,
            remaining: input.len().saturating_sub(offset),
        })?
        .try_into()
        .map_err(|_| SidecarViolation::UnexpectedFixedSidecarEnd {
            offset,
            needed: N,
            remaining: input.len().saturating_sub(offset),
        })
}

fn read_u16(input: &[u8], offset: usize) -> Result<u16, SidecarViolation> {
    Ok(u16::from_le_bytes(read_array(input, offset)?))
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, SidecarViolation> {
    Ok(u32::from_le_bytes(read_array(input, offset)?))
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64, SidecarViolation> {
    Ok(u64::from_le_bytes(read_array(input, offset)?))
}

fn write_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
