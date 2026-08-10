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

use super::CodecError;
use super::LedgerRecord;
use super::OpenReason;
use super::RecordType;
use super::MAX_SEALED_RECORD_UNIT_LENGTH;

pub(super) fn validate_initial_record_envelope(
    record_type: RecordType,
    sequence: u64,
    log_generation: u64,
) -> Result<(), CodecError> {
    match record_type {
        RecordType::StoreInitialized if sequence != 1 || log_generation != 0 => {
            Err(CodecError::InvalidEnvelopeRelationship {
                detail: "StoreInitialized must be sequence 1 in generation 0",
            })
        }
        RecordType::BootstrapInstalled if sequence != 2 || log_generation != 0 => {
            Err(CodecError::InvalidEnvelopeRelationship {
                detail: "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
            })
        }
        _ => Ok(()),
    }
}

pub(super) fn validate_envelope_relationships(
    record: &LedgerRecord,
    sequence: u64,
    log_generation: u64,
) -> Result<(), CodecError> {
    validate_initial_record_envelope(record.record_type(), sequence, log_generation)?;
    match record {
        LedgerRecord::BootstrapInstalled {
            snapshot_generation,
            snapshot_base_sequence,
            ..
        } if *snapshot_generation != 0 || *snapshot_base_sequence != 1 => {
            Err(CodecError::InvalidEnvelopeRelationship {
                detail: "BootstrapInstalled must bind generation 0 at base sequence 1 and frame sequence 2",
            })
        }
        LedgerRecord::GenerationPrepared {
            source_generation,
            target_generation,
            target_snapshot_generation,
            open_reason,
            ..
        } => {
            let expected_target =
                source_generation
                    .checked_add(1)
                    .ok_or(CodecError::InvalidGenerationRelationship {
                        detail: "GenerationPrepared source generation cannot advance",
                    })?;
            if *source_generation != log_generation {
                return Err(CodecError::InvalidEnvelopeRelationship {
                    detail: "GenerationPrepared source must equal the containing log generation",
                });
            }
            if *target_generation != expected_target || *target_snapshot_generation != *target_generation {
                return Err(CodecError::InvalidGenerationRelationship {
                    detail: "GenerationPrepared target and snapshot must equal source + 1",
                });
            }
            if *open_reason != OpenReason::Compaction {
                return Err(CodecError::InvalidEnvelopeRelationship {
                    detail: "GenerationPrepared v1 open reason must be compaction",
                });
            }
            Ok(())
        }
        LedgerRecord::GenerationAborted {
            source_generation,
            target_generation,
            prepared_sequence,
            ..
        } => {
            let expected_target =
                source_generation
                    .checked_add(1)
                    .ok_or(CodecError::InvalidGenerationRelationship {
                        detail: "GenerationAborted source generation cannot advance",
                    })?;
            let expected_sequence =
                prepared_sequence
                    .checked_add(1)
                    .ok_or(CodecError::InvalidEnvelopeRelationship {
                        detail: "GenerationAborted prepared sequence cannot advance",
                    })?;
            if *source_generation != log_generation || *target_generation != expected_target {
                return Err(CodecError::InvalidGenerationRelationship {
                    detail: "GenerationAborted target must equal containing source generation + 1",
                });
            }
            if sequence != expected_sequence {
                return Err(CodecError::InvalidEnvelopeRelationship {
                    detail: "GenerationAborted must immediately follow its GenerationPrepared",
                });
            }
            Ok(())
        }
        LedgerRecord::MarkerCommitted {
            marker_epoch,
            snapshot_generation,
            log_generation: selected_log_generation,
            anchor_sequence,
            slot_index,
            ..
        } => {
            if *marker_epoch == 0 {
                return Err(CodecError::ZeroMarkerEpoch);
            }
            if *slot_index > 1 {
                return Err(CodecError::InvalidMarkerSlotIndex {
                    slot_index: *slot_index,
                });
            }
            let expected_slot_index = ((*marker_epoch - 1) & 1) as u8;
            if *slot_index != expected_slot_index {
                return Err(CodecError::MarkerSlotParityMismatch {
                    marker_epoch: *marker_epoch,
                    expected_slot_index,
                    actual_slot_index: *slot_index,
                });
            }
            if *snapshot_generation != log_generation || *selected_log_generation != log_generation {
                return Err(CodecError::InvalidGenerationRelationship {
                    detail: "MarkerCommitted snapshot and log must equal the containing generation",
                });
            }
            if anchor_sequence.checked_add(1) != Some(sequence) {
                return Err(CodecError::InvalidEnvelopeRelationship {
                    detail: "MarkerCommitted must immediately follow its anchor",
                });
            }
            Ok(())
        }
        LedgerRecord::LogOpened {
            generation,
            snapshot_generation,
            predecessor_log_generation,
            predecessor_terminal_acknowledged_sequence,
            snapshot_base_sequence,
            unacknowledged_suffix_length,
            unacknowledged_suffix_crc32,
            open_reason,
            predecessor_acknowledgement_epoch,
            ..
        } => {
            let expected_generation =
                predecessor_log_generation
                    .checked_add(1)
                    .ok_or(CodecError::InvalidGenerationRelationship {
                        detail: "LogOpened predecessor generation cannot advance",
                    })?;
            let expected_sequence =
                snapshot_base_sequence
                    .checked_add(1)
                    .ok_or(CodecError::InvalidEnvelopeRelationship {
                        detail: "LogOpened snapshot base sequence cannot advance",
                    })?;
            if *generation != log_generation
                || *snapshot_generation != *generation
                || *generation != expected_generation
            {
                return Err(CodecError::InvalidGenerationRelationship {
                    detail: "LogOpened generation and snapshot must equal predecessor + 1 and the containing log",
                });
            }
            if sequence != expected_sequence {
                return Err(CodecError::InvalidEnvelopeRelationship {
                    detail: "LogOpened must be the first sequence after its snapshot base",
                });
            }
            if *predecessor_terminal_acknowledged_sequence == 0
                || *predecessor_terminal_acknowledged_sequence != *snapshot_base_sequence
            {
                return Err(CodecError::InvalidEnvelopeRelationship {
                    detail: "LogOpened predecessor terminal sequence must be nonzero and equal its snapshot base",
                });
            }
            if *predecessor_acknowledgement_epoch == 0 {
                return Err(CodecError::ZeroAcknowledgementEpoch);
            }
            predecessor_acknowledgement_epoch
                .checked_add(1)
                .ok_or(CodecError::AcknowledgementEpochOverflow)?;
            let suffix_valid = match open_reason {
                OpenReason::Compaction => *unacknowledged_suffix_length == 0 && *unacknowledged_suffix_crc32 == 0,
                OpenReason::TailRepair => {
                    *unacknowledged_suffix_length != 0
                        && u64::from(*unacknowledged_suffix_length) < MAX_SEALED_RECORD_UNIT_LENGTH as u64
                }
            };
            if !suffix_valid {
                return Err(CodecError::InvalidTailRepairFields);
            }
            Ok(())
        }
        LedgerRecord::Completed {
            namespace_absent_sequence,
            ..
        } if *namespace_absent_sequence == 0 || *namespace_absent_sequence >= sequence => {
            Err(CodecError::InvalidNamespaceAbsentSequence)
        }
        _ => Ok(()),
    }
}
