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

use super::super::codec::LedgerRecord;
use super::super::identity::StoreUuid;
use super::super::sidecar::LifecycleSnapshot;
use super::super::sidecar::StoreMeta;
#[cfg(test)]
use super::types::BootstrapPlanViolation;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct CanonicalStoreMetaEvidence {
    pub(super) meta: StoreMeta,
    pub(super) canonical_bytes: [u8; 64],
    pub(super) stored_crc32: u32,
}

/// Opaque proof for frozen-format section 12 steps 1-5.
///
/// A future foundation verifier inside this module may mint it only after verifying the external
/// activation fence, allocator/queue barrier, lifecycle directory, canonical `store.meta`, zeroed
/// fixed-size acknowledgement file, and empty generation-0 log. There is intentionally no
/// crate-visible constructor.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct BootstrapFoundationEvidence {
    pub(super) store_meta: CanonicalStoreMetaEvidence,
}

/// Opaque proof that a no-follow inventory scan produced this exact canonical snapshot.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct BootstrapInventoryEvidence {
    pub(super) store_uuid: StoreUuid,
    pub(super) snapshot: LifecycleSnapshot,
    pub(super) canonical_snapshot: Vec<u8>,
    pub(super) snapshot_crc32: u32,
    pub(super) inventory_count: u64,
    pub(super) create_high_water: u64,
    pub(super) ticket_high_water: u64,
}

/// Opaque proof for a generation switch foundation.
///
/// Both variants certify the authoritative predecessor frontier, the canonical target snapshot,
/// and an exact unsealed `LogOpened` frame that has already been published without replacement,
/// synced, reopened, and verified. The plan therefore cannot synthesize an anchor from raw
/// parameters.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum GenerationSwitchFoundationEvidence {
    Compaction(CompactionFoundationEvidence),
    TailRepair(TailRepairFoundationEvidence),
}

impl GenerationSwitchFoundationEvidence {
    pub(super) const fn common(&self) -> &GenerationSwitchCommonEvidence {
        match self {
            Self::Compaction(evidence) => &evidence.common,
            Self::TailRepair(evidence) => &evidence.common,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct CompactionFoundationEvidence {
    pub(super) common: GenerationSwitchCommonEvidence,
    pub(super) prepared: GenerationPreparedEvidence,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct TailRepairFoundationEvidence {
    pub(super) common: GenerationSwitchCommonEvidence,
    pub(super) tail: TailEvidence,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct GenerationSwitchCommonEvidence {
    pub(super) store_meta: CanonicalStoreMetaEvidence,
    pub(super) marker_epoch: u64,
    pub(super) predecessor_generation: u64,
    pub(super) predecessor_terminal_sequence: u64,
    pub(super) predecessor_acknowledgement_epoch: u64,
    pub(super) predecessor_sealed_prefix_length: u64,
    pub(super) predecessor_prefix_crc32: u32,
    pub(super) snapshot: LifecycleSnapshot,
    pub(super) canonical_snapshot: Vec<u8>,
    pub(super) snapshot_crc32: u32,
    pub(super) log_opened_record: LedgerRecord,
    pub(super) canonical_log_opened_frame: Vec<u8>,
    pub(super) log_opened_frame_crc32: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct GenerationPreparedEvidence {
    pub(super) record: LedgerRecord,
    pub(super) sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) sealed_log_length: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct TailEvidence {
    pub(super) suffix_offset: u64,
    pub(super) suffix: Vec<u8>,
    pub(super) suffix_crc32: u32,
}

#[cfg(test)]
mod test_constructors {
    use super::super::super::codec::crc32;
    use super::super::super::codec::encode_ledger_frame;
    use super::super::super::codec::OpenReason;
    use super::super::super::sidecar::decode_snapshot;
    use super::super::super::sidecar::decode_store_meta;
    use super::super::super::sidecar::encode_snapshot;
    use super::super::super::sidecar::encode_store_meta;
    use super::*;

    impl BootstrapFoundationEvidence {
        pub(in crate::mapped_file::retirement::bootstrap) fn verified_for_test(
            meta: &StoreMeta,
        ) -> Result<Self, BootstrapPlanViolation> {
            let canonical_bytes = encode_store_meta(meta)?;
            Self::from_bytes_for_test(canonical_bytes, meta)
        }

        pub(in crate::mapped_file::retirement::bootstrap) fn from_bytes_for_test(
            canonical_bytes: [u8; 64],
            expected_meta: &StoreMeta,
        ) -> Result<Self, BootstrapPlanViolation> {
            let store_meta = canonical_store_meta(canonical_bytes)?;
            if store_meta.meta != *expected_meta {
                return Err(BootstrapPlanViolation::FoundationStoreMetaMismatch);
            }
            Ok(Self { store_meta })
        }
    }

    impl BootstrapInventoryEvidence {
        pub(in crate::mapped_file::retirement::bootstrap) fn verified_for_test(
            snapshot: &LifecycleSnapshot,
        ) -> Result<Self, BootstrapPlanViolation> {
            let canonical_snapshot = encode_snapshot(snapshot)?;
            let decoded = decode_snapshot(&canonical_snapshot)?;
            if encode_snapshot(&decoded)? != canonical_snapshot {
                return Err(BootstrapPlanViolation::InvalidSnapshot {
                    reason: "inventory snapshot is not canonical",
                });
            }
            let inventory_count =
                u64::try_from(decoded.entries.len()).map_err(|_| BootstrapPlanViolation::ArithmeticOverflow {
                    field: "bootstrap inventory count",
                })?;
            Ok(Self {
                store_uuid: decoded.store_uuid,
                snapshot_crc32: crc32(&canonical_snapshot),
                canonical_snapshot,
                inventory_count,
                create_high_water: decoded.create_high_water,
                ticket_high_water: decoded.ticket_high_water,
                snapshot: decoded,
            })
        }
    }

    impl GenerationSwitchFoundationEvidence {
        pub(in crate::mapped_file::retirement::bootstrap) fn compaction_for_test(
            meta: &StoreMeta,
            snapshot: &LifecycleSnapshot,
            predecessor_sealed_prefix_length: u64,
            predecessor_prefix_crc32: u32,
        ) -> Result<Self, BootstrapPlanViolation> {
            let common = generation_common(
                meta,
                snapshot,
                predecessor_sealed_prefix_length,
                predecessor_prefix_crc32,
                OpenReason::Compaction,
                &[],
            )?;
            let prepared = GenerationPreparedEvidence {
                record: LedgerRecord::GenerationPrepared {
                    store_uuid: meta.store_uuid,
                    source_generation: snapshot.predecessor_log_generation,
                    target_generation: snapshot.generation,
                    target_snapshot_generation: snapshot.generation,
                    open_reason: OpenReason::Compaction,
                },
                sequence: snapshot.base_sequence,
                acknowledgement_epoch: snapshot.base_sequence,
                sealed_log_length: predecessor_sealed_prefix_length,
            };
            Ok(Self::Compaction(CompactionFoundationEvidence { common, prepared }))
        }

        pub(in crate::mapped_file::retirement::bootstrap) fn tail_repair_for_test(
            meta: &StoreMeta,
            snapshot: &LifecycleSnapshot,
            predecessor_sealed_prefix_length: u64,
            predecessor_prefix_crc32: u32,
            suffix: Vec<u8>,
        ) -> Result<Self, BootstrapPlanViolation> {
            let common = generation_common(
                meta,
                snapshot,
                predecessor_sealed_prefix_length,
                predecessor_prefix_crc32,
                OpenReason::TailRepair,
                &suffix,
            )?;
            let tail = TailEvidence {
                suffix_offset: predecessor_sealed_prefix_length,
                suffix_crc32: crc32(&suffix),
                suffix,
            };
            Ok(Self::TailRepair(TailRepairFoundationEvidence { common, tail }))
        }

        pub(in crate::mapped_file::retirement::bootstrap) fn common_mut_for_test(
            &mut self,
        ) -> &mut GenerationSwitchCommonEvidence {
            match self {
                Self::Compaction(evidence) => &mut evidence.common,
                Self::TailRepair(evidence) => &mut evidence.common,
            }
        }
    }

    fn canonical_store_meta(bytes: [u8; 64]) -> Result<CanonicalStoreMetaEvidence, BootstrapPlanViolation> {
        let meta = decode_store_meta(&bytes)?;
        if encode_store_meta(&meta)? != bytes {
            return Err(BootstrapPlanViolation::FoundationStoreMetaMismatch);
        }
        let stored_crc32 = u32::from_le_bytes([bytes[60], bytes[61], bytes[62], bytes[63]]);
        Ok(CanonicalStoreMetaEvidence {
            meta,
            canonical_bytes: bytes,
            stored_crc32,
        })
    }

    fn generation_common(
        meta: &StoreMeta,
        snapshot: &LifecycleSnapshot,
        predecessor_sealed_prefix_length: u64,
        predecessor_prefix_crc32: u32,
        open_reason: OpenReason,
        suffix: &[u8],
    ) -> Result<GenerationSwitchCommonEvidence, BootstrapPlanViolation> {
        let canonical_store_meta_bytes = encode_store_meta(meta)?;
        let store_meta = canonical_store_meta(canonical_store_meta_bytes)?;
        let canonical_snapshot = encode_snapshot(snapshot)?;
        let decoded_snapshot = decode_snapshot(&canonical_snapshot)?;
        let marker_epoch = decoded_snapshot
            .generation
            .checked_add(1)
            .ok_or(BootstrapPlanViolation::ArithmeticOverflow { field: "marker epoch" })?;
        let anchor_sequence =
            decoded_snapshot
                .base_sequence
                .checked_add(1)
                .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                    field: "LogOpened sequence",
                })?;
        let suffix_length =
            u32::try_from(suffix.len()).map_err(|_| BootstrapPlanViolation::InvalidGenerationSwitch {
                reason: "tail suffix length exceeds u32",
            })?;
        let suffix_crc32 = if suffix.is_empty() { 0 } else { crc32(suffix) };
        let snapshot_crc32 = crc32(&canonical_snapshot);
        let log_opened_record = LedgerRecord::LogOpened {
            store_uuid: meta.store_uuid,
            generation: decoded_snapshot.generation,
            snapshot_generation: decoded_snapshot.generation,
            predecessor_log_generation: decoded_snapshot.predecessor_log_generation,
            predecessor_terminal_acknowledged_sequence: decoded_snapshot.base_sequence,
            snapshot_base_sequence: decoded_snapshot.base_sequence,
            snapshot_file_length: u64::try_from(canonical_snapshot.len()).map_err(|_| {
                BootstrapPlanViolation::ArithmeticOverflow {
                    field: "snapshot file length",
                }
            })?,
            snapshot_file_crc32: snapshot_crc32,
            predecessor_prefix_crc32,
            validated_prefix_length: predecessor_sealed_prefix_length,
            unacknowledged_suffix_length: suffix_length,
            unacknowledged_suffix_crc32: suffix_crc32,
            open_reason,
            predecessor_acknowledgement_epoch: decoded_snapshot.base_sequence,
        };
        let canonical_log_opened_frame =
            encode_ledger_frame(&log_opened_record, anchor_sequence, decoded_snapshot.generation)?;
        Ok(GenerationSwitchCommonEvidence {
            store_meta,
            marker_epoch,
            predecessor_generation: decoded_snapshot.predecessor_log_generation,
            predecessor_terminal_sequence: decoded_snapshot.base_sequence,
            predecessor_acknowledgement_epoch: decoded_snapshot.base_sequence,
            predecessor_sealed_prefix_length,
            predecessor_prefix_crc32,
            snapshot: decoded_snapshot,
            canonical_snapshot,
            snapshot_crc32,
            log_opened_record,
            log_opened_frame_crc32: crc32(&canonical_log_opened_frame),
            canonical_log_opened_frame,
        })
    }
}
