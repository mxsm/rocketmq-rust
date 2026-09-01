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

use std::mem::size_of;

use super::CodecViolation;
use super::RecordType;

mod prefix;
pub(super) use prefix::validate_known_payload_prefix;

#[derive(Clone, Copy)]
enum PayloadSchema {
    Exact(usize),
    Range { minimum: usize, maximum: usize },
}

impl PayloadSchema {
    const fn for_record(record_type: RecordType) -> Option<Self> {
        match record_type {
            RecordType::StoreInitialized => Some(Self::Exact(64)),
            RecordType::BootstrapInstalled => Some(Self::Exact(88)),
            RecordType::LogOpened => Some(Self::Exact(104)),
            RecordType::GenerationPrepared => Some(Self::Exact(56)),
            RecordType::GenerationAborted => Some(Self::Exact(48)),
            RecordType::MarkerCommitted => Some(Self::Exact(56)),
            RecordType::AllocateIncarnation => Some(Self::Range {
                minimum: 62,
                maximum: 8_252,
            }),
            RecordType::BindIncarnation | RecordType::PublishIncarnation => Some(Self::Range {
                minimum: 70,
                maximum: 8_260,
            }),
            RecordType::RetirementIntent => Some(Self::Range {
                minimum: 111,
                maximum: 4_206,
            }),
            RecordType::LogicalRemoved => Some(Self::Range {
                minimum: 67,
                maximum: 4_162,
            }),
            RecordType::Tombstoned => Some(Self::Range {
                minimum: 86,
                maximum: 8_276,
            }),
            RecordType::NamespaceAbsent => Some(Self::Range {
                minimum: 81,
                maximum: 8_272,
            }),
            RecordType::Completed => Some(Self::Exact(56)),
            RecordType::SupersededPath => Some(Self::Range {
                minimum: 99,
                maximum: 4_194,
            }),
            RecordType::Quarantined => Some(Self::Range {
                minimum: 65,
                maximum: 8_256,
            }),
            RecordType::Unknown(_) => None,
        }
    }

    fn validate(self, record_type: RecordType, actual: usize) -> Result<(), CodecViolation> {
        match self {
            Self::Exact(expected) if actual != expected => Err(CodecViolation::InvalidPayloadLength {
                record_type: record_type.wire_value(),
                expected,
                actual,
            }),
            Self::Range { minimum, maximum } if !(minimum..=maximum).contains(&actual) => {
                Err(CodecViolation::InvalidVariablePayloadLength {
                    record_type: record_type.wire_value(),
                    minimum,
                    maximum,
                    actual,
                })
            }
            _ => Ok(()),
        }
    }

    fn accepts_length_prefix(self, prefix: &[u8]) -> bool {
        match self {
            Self::Exact(expected) => match u32::try_from(expected) {
                Ok(expected) => expected.to_le_bytes().starts_with(prefix),
                Err(_) => false,
            },
            Self::Range { minimum, maximum } => range_contains_le_prefix(minimum, maximum, prefix),
        }
    }
}

pub(super) fn validate_known_payload_length(record_type: RecordType, actual: usize) -> Result<(), CodecViolation> {
    match PayloadSchema::for_record(record_type) {
        Some(schema) => schema.validate(record_type, actual),
        None => Ok(()),
    }
}

pub(super) fn validate_known_payload_length_prefix(
    record_type: RecordType,
    prefix: &[u8],
) -> Result<(), CodecViolation> {
    let Some(schema) = PayloadSchema::for_record(record_type) else {
        return Ok(());
    };
    if schema.accepts_length_prefix(prefix) {
        return Ok(());
    }
    Err(CodecViolation::InvalidFieldPrefix {
        field: "payload_length",
        offset: 16 + prefix.len().saturating_sub(1),
    })
}

fn range_contains_le_prefix(minimum: usize, maximum: usize, prefix: &[u8]) -> bool {
    if prefix.len() > size_of::<u32>() {
        return false;
    }
    let (Ok(minimum), Ok(maximum)) = (u32::try_from(minimum), u32::try_from(maximum)) else {
        return false;
    };
    if prefix.len() == size_of::<u32>() {
        let Ok(bytes) = prefix.try_into() else {
            return false;
        };
        return (minimum..=maximum).contains(&u32::from_le_bytes(bytes));
    }

    let mut residue = 0_u64;
    for (index, byte) in prefix.iter().enumerate() {
        residue |= u64::from(*byte) << (index * 8);
    }
    let modulus = 1_u64 << (prefix.len() * 8);
    let minimum = u64::from(minimum);
    let maximum = u64::from(maximum);
    let distance = (residue + modulus - minimum % modulus) % modulus;
    minimum + distance <= maximum
}
