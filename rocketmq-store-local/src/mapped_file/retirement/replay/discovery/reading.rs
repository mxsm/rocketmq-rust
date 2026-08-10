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

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::sidecar::MAX_SNAPSHOT_BODY_LENGTH;
use crate::mapped_file::retirement::sidecar::MAX_SNAPSHOT_ENTRY_COUNT;
use crate::mapped_file::retirement::sidecar::MIN_SNAPSHOT_FILE_LENGTH;
use crate::mapped_file::retirement::sidecar::SNAPSHOT_HEADER_LENGTH;

use super::corruption;
use super::io_error;
use super::limit_error;
use super::platform;
use super::ManagedLifecycleReadError;
use super::ManagedLifecycleReadErrorKind;
use super::ManagedLifecycleReadSource;

pub(super) fn validate_snapshot_prefix(
    entry: &mut platform::OpenedEntry,
) -> Result<[u8; SNAPSHOT_HEADER_LENGTH], ManagedLifecycleReadError> {
    let length = entry.length();
    let maximum = (MIN_SNAPSHOT_FILE_LENGTH + MAX_SNAPSHOT_BODY_LENGTH) as u64;
    if length < MIN_SNAPSHOT_FILE_LENGTH as u64 || length > maximum {
        return Err(corruption(format!("snapshot length {length} is outside v1 bounds")));
    }
    let mut header = [0_u8; SNAPSHOT_HEADER_LENGTH];
    entry.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    entry.file.read_exact(&mut header).map_err(io_error)?;
    if &header[..4] != b"RMSN" {
        return Err(corruption("snapshot magic is invalid"));
    }
    let major = u16::from_le_bytes(header[4..6].try_into().map_err(|_| corruption("snapshot version"))?);
    let minor = u16::from_le_bytes(header[6..8].try_into().map_err(|_| corruption("snapshot version"))?);
    if (major, minor) != (1, 0) {
        return Err(ManagedLifecycleReadError::new(
            ManagedLifecycleReadErrorKind::UnknownVersionCorruption,
            ManagedLifecycleReadSource::UnknownVersion(format!("snapshot {major}.{minor}")),
        ));
    }
    let total = u64::from_le_bytes(header[12..20].try_into().map_err(|_| corruption("snapshot length"))?);
    let entry_count = u32::from_le_bytes(header[84..88].try_into().map_err(|_| corruption("entry count"))?);
    let body = u64::from_le_bytes(header[88..96].try_into().map_err(|_| corruption("body length"))?);
    if body > MAX_SNAPSHOT_BODY_LENGTH as u64
        || entry_count > MAX_SNAPSHOT_ENTRY_COUNT
        || u64::from(entry_count).saturating_mul(12) > body
        || total != length
        || total != (MIN_SNAPSHOT_FILE_LENGTH as u64).saturating_add(body)
    {
        return Err(corruption("snapshot header length/count bounds are invalid"));
    }
    let stored_crc = u32::from_le_bytes(header[100..104].try_into().map_err(|_| corruption("header CRC"))?);
    if stored_crc != crc32(&header[..100]) {
        return Err(corruption("snapshot header CRC is invalid"));
    }
    Ok(header)
}

pub(super) fn read_snapshot_file(
    entry: &mut platform::OpenedEntry,
    header: &[u8; SNAPSHOT_HEADER_LENGTH],
    total_read: &mut u64,
    max_total_read_bytes: u64,
) -> Result<Vec<u8>, ManagedLifecycleReadError> {
    let length = entry.length();
    let next_total = total_read
        .checked_add(length)
        .ok_or_else(|| limit_error("total read bytes", u64::MAX, max_total_read_bytes))?;
    if next_total > max_total_read_bytes {
        return Err(limit_error("total read bytes", next_total, max_total_read_bytes));
    }
    let length_usize = usize::try_from(length).map_err(|_| {
        limit_error(
            "snapshot bytes",
            length,
            MIN_SNAPSHOT_FILE_LENGTH + MAX_SNAPSHOT_BODY_LENGTH,
        )
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length_usize).map_err(|_| {
        limit_error(
            "snapshot bytes",
            length,
            MIN_SNAPSHOT_FILE_LENGTH + MAX_SNAPSHOT_BODY_LENGTH,
        )
    })?;
    bytes.resize(length_usize, 0);
    bytes[..SNAPSHOT_HEADER_LENGTH].copy_from_slice(header);
    entry
        .file
        .seek(SeekFrom::Start(SNAPSHOT_HEADER_LENGTH as u64))
        .map_err(io_error)?;
    entry
        .file
        .read_exact(&mut bytes[SNAPSHOT_HEADER_LENGTH..])
        .map_err(io_error)?;
    let mut extra = [0_u8; 1];
    if entry.file.read(&mut extra).map_err(io_error)? != 0 {
        return Err(ManagedLifecycleReadError::new(
            ManagedLifecycleReadErrorKind::InventoryChanged,
            ManagedLifecycleReadSource::InventoryChanged("snapshot grew while it was read".to_owned()),
        ));
    }
    *total_read = next_total;
    Ok(bytes)
}

pub(super) fn read_exact_file(
    entry: &mut platform::OpenedEntry,
    exact: Option<usize>,
    maximum: u64,
    total_read: &mut u64,
    max_total_read_bytes: u64,
) -> Result<Vec<u8>, ManagedLifecycleReadError> {
    let length = entry.length();
    if exact.is_some_and(|expected| length != expected as u64) {
        return Err(corruption(format!("fixed sidecar length {length} is invalid")));
    }
    if length > maximum {
        return Err(limit_error("file bytes", length, maximum));
    }
    let next_total = total_read
        .checked_add(length)
        .ok_or_else(|| limit_error("total read bytes", u64::MAX, max_total_read_bytes))?;
    if next_total > max_total_read_bytes {
        return Err(limit_error("total read bytes", next_total, max_total_read_bytes));
    }
    let length_usize = usize::try_from(length).map_err(|_| limit_error("file bytes", length, maximum))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length_usize)
        .map_err(|_| limit_error("file bytes", length, maximum))?;
    bytes.resize(length_usize, 0);
    entry.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    entry.file.read_exact(&mut bytes).map_err(io_error)?;
    let mut extra = [0_u8; 1];
    if entry.file.read(&mut extra).map_err(io_error)? != 0 {
        return Err(ManagedLifecycleReadError::new(
            ManagedLifecycleReadErrorKind::InventoryChanged,
            ManagedLifecycleReadSource::InventoryChanged("sidecar grew while it was read".to_owned()),
        ));
    }
    *total_read = next_total;
    Ok(bytes)
}
