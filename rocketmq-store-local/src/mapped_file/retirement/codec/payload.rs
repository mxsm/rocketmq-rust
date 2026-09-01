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

use super::super::identity::FileIncarnationId;
use super::super::identity::IdentityViolation;
use super::super::identity::PhysicalFileKey;
use super::super::identity::StoreRelativePath;
use super::super::identity::StoreUuid;
use super::super::identity::TicketId;
use super::CodecViolation;
use super::RecordType;

pub(super) fn validate_opaque_id(field: &'static str, value: &[u8; 16]) -> Result<(), CodecViolation> {
    if *value == [0; 16] {
        return Err(CodecViolation::ZeroOpaqueIdentifier { field });
    }
    Ok(())
}

pub(super) fn push_store_uuid(output: &mut Vec<u8>, value: StoreUuid) {
    output.extend_from_slice(value.as_bytes());
}

pub(super) fn push_incarnation(output: &mut Vec<u8>, value: FileIncarnationId) {
    push_store_uuid(output, value.store_uuid());
    push_u64(output, value.create_seq());
}

pub(super) fn push_ticket(output: &mut Vec<u8>, value: TicketId) {
    push_u64(output, value.get());
}

pub(super) fn push_physical_key(output: &mut Vec<u8>, value: PhysicalFileKey) {
    match value {
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

pub(super) fn push_optional_physical_key(output: &mut Vec<u8>, value: Option<PhysicalFileKey>) {
    if let Some(value) = value {
        push_physical_key(output, value);
    } else {
        output.extend_from_slice(&[0; 32]);
    }
}

pub(super) fn push_path(output: &mut Vec<u8>, value: &StoreRelativePath) -> Result<(), CodecViolation> {
    let length = u16::try_from(value.as_bytes().len()).map_err(|_| CodecViolation::PayloadTooLarge {
        length: value.as_bytes().len(),
        maximum: StoreRelativePath::MAX_BYTES,
    })?;
    push_u16(output, length);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(super) fn push_optional_path(
    output: &mut Vec<u8>,
    value: Option<&StoreRelativePath>,
) -> Result<(), CodecViolation> {
    if let Some(value) = value {
        push_path(output, value)
    } else {
        push_u16(output, 0);
        Ok(())
    }
}

pub(super) fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub(super) struct PayloadDecoder<'a> {
    record_type: RecordType,
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PayloadDecoder<'a> {
    pub(super) const fn new(record_type: RecordType, bytes: &'a [u8]) -> Self {
        Self {
            record_type,
            bytes,
            offset: 0,
        }
    }

    pub(super) fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecViolation> {
        let end = self.offset.checked_add(N).ok_or(CodecViolation::FrameLengthOverflow)?;
        let Some(bytes) = self.bytes.get(self.offset..end) else {
            return Err(CodecViolation::UnexpectedPayloadEnd {
                record_type: self.record_type.wire_value(),
                offset: self.offset,
                needed: N,
                remaining: self.bytes.len().saturating_sub(self.offset),
            });
        };
        self.offset = end;
        bytes.try_into().map_err(|_| CodecViolation::UnexpectedPayloadEnd {
            record_type: self.record_type.wire_value(),
            offset: self.offset.saturating_sub(N),
            needed: N,
            remaining: bytes.len(),
        })
    }

    pub(super) fn take_u8(&mut self) -> Result<u8, CodecViolation> {
        Ok(self.take::<1>()?[0])
    }

    pub(super) fn take_u16(&mut self) -> Result<u16, CodecViolation> {
        self.take().map(u16::from_le_bytes)
    }

    pub(super) fn take_u32(&mut self) -> Result<u32, CodecViolation> {
        self.take().map(u32::from_le_bytes)
    }

    pub(super) fn take_u64(&mut self) -> Result<u64, CodecViolation> {
        self.take().map(u64::from_le_bytes)
    }

    pub(super) fn take_store_uuid(&mut self, field: &'static str) -> Result<StoreUuid, CodecViolation> {
        StoreUuid::new(self.take()?).map_err(|source| CodecViolation::InvalidIdentity { field, source })
    }

    pub(super) fn take_incarnation(&mut self) -> Result<FileIncarnationId, CodecViolation> {
        let store_uuid = self.take_store_uuid("incarnation_store_uuid")?;
        let create_seq = self.take_u64()?;
        FileIncarnationId::new(store_uuid, create_seq).map_err(|source| CodecViolation::InvalidIdentity {
            field: "file_incarnation",
            source,
        })
    }

    pub(super) fn take_ticket(&mut self) -> Result<TicketId, CodecViolation> {
        TicketId::new(self.take_u64()?).map_err(|source| CodecViolation::InvalidIdentity {
            field: "ticket_id",
            source,
        })
    }

    pub(super) fn take_opaque_id(&mut self, field: &'static str) -> Result<[u8; 16], CodecViolation> {
        let value = self.take()?;
        validate_opaque_id(field, &value)?;
        Ok(value)
    }

    pub(super) fn take_physical_key(&mut self) -> Result<PhysicalFileKey, CodecViolation> {
        decode_physical_key(self.take()?)
    }

    pub(super) fn take_optional_physical_key(
        &mut self,
        present: bool,
    ) -> Result<Option<PhysicalFileKey>, CodecViolation> {
        let bytes = self.take()?;
        if present {
            decode_physical_key(bytes).map(Some)
        } else if bytes == [0; 32] {
            Ok(None)
        } else {
            Err(CodecViolation::InvalidAbsentPhysicalFileKey)
        }
    }

    pub(super) fn take_required_path(&mut self, field: &'static str) -> Result<StoreRelativePath, CodecViolation> {
        self.take_path(field, false)?.ok_or(CodecViolation::InvalidIdentity {
            field,
            source: IdentityViolation::EmptyStoreRelativePath,
        })
    }

    pub(super) fn take_optional_path(
        &mut self,
        field: &'static str,
    ) -> Result<Option<StoreRelativePath>, CodecViolation> {
        self.take_path(field, true)
    }

    pub(super) fn take_path(
        &mut self,
        field: &'static str,
        optional: bool,
    ) -> Result<Option<StoreRelativePath>, CodecViolation> {
        let length = usize::from(self.take_u16()?);
        if length == 0 && optional {
            return Ok(None);
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
        let Some(bytes) = self.bytes.get(self.offset..end) else {
            return Err(CodecViolation::UnexpectedPayloadEnd {
                record_type: self.record_type.wire_value(),
                offset: self.offset,
                needed: length,
                remaining: self.bytes.len().saturating_sub(self.offset),
            });
        };
        self.offset = end;
        let value = std::str::from_utf8(bytes).map_err(|_| CodecViolation::InvalidUtf8Path { field })?;
        StoreRelativePath::new(value)
            .map(Some)
            .map_err(|source| CodecViolation::InvalidIdentity { field, source })
    }

    pub(super) fn require_u16(&mut self, field: &'static str, expected: u16) -> Result<(), CodecViolation> {
        let value = self.take_u16()?;
        if value != expected {
            return Err(CodecViolation::NonZeroReserved {
                field,
                value: u64::from(value),
            });
        }
        Ok(())
    }

    pub(super) fn require_u32(&mut self, field: &'static str, expected: u32) -> Result<(), CodecViolation> {
        let value = self.take_u32()?;
        if value != expected {
            return Err(CodecViolation::NonZeroReserved {
                field,
                value: u64::from(value),
            });
        }
        Ok(())
    }

    pub(super) fn require_u64(&mut self, field: &'static str, expected: u64) -> Result<(), CodecViolation> {
        let value = self.take_u64()?;
        if value != expected {
            return Err(CodecViolation::NonZeroReserved { field, value });
        }
        Ok(())
    }

    pub(super) fn require_zero_bytes(&mut self, field: &'static str, length: usize) -> Result<(), CodecViolation> {
        for _ in 0..length {
            let value = self.take_u8()?;
            if value != 0 {
                return Err(CodecViolation::NonZeroReserved {
                    field,
                    value: u64::from(value),
                });
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<(), CodecViolation> {
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

pub(super) fn decode_physical_key(bytes: [u8; 32]) -> Result<PhysicalFileKey, CodecViolation> {
    if bytes[1..8] != [0; 7] {
        return Err(CodecViolation::NonZeroPhysicalFileKeyReserved);
    }
    let first = u64::from_le_bytes(
        bytes[8..16]
            .try_into()
            .map_err(|_| CodecViolation::FrameLengthOverflow)?,
    );
    match bytes[0] {
        1 => {
            if bytes[24..32] != [0; 8] {
                return Err(CodecViolation::NonZeroPhysicalFileKeyReserved);
            }
            let inode = u64::from_le_bytes(
                bytes[16..24]
                    .try_into()
                    .map_err(|_| CodecViolation::FrameLengthOverflow)?,
            );
            Ok(PhysicalFileKey::unix(first, inode))
        }
        2 => {
            let file_id = bytes[16..32]
                .try_into()
                .map_err(|_| CodecViolation::FrameLengthOverflow)?;
            Ok(PhysicalFileKey::windows(first, file_id))
        }
        kind => Err(CodecViolation::InvalidPhysicalFileKeyKind { kind }),
    }
}
