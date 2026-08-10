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

use std::collections::BTreeMap;

use thiserror::Error;

use super::super::types::BootstrapRecord;
use super::super::types::DurableUnitProgress;
use super::super::types::DurableUnitStep;
use super::super::types::PlannedAcknowledgedUnit;
use crate::mapped_file::retirement::io::LedgerIo;
use crate::mapped_file::retirement::io::LedgerIoError;

#[derive(Debug, Error)]
pub(in crate::mapped_file::retirement::bootstrap) enum DurableUnitError {
    #[error(transparent)]
    Io(#[from] LedgerIoError),
    #[error("bootstrap durable unit is not an exact prefix of its canonical bytes: {0}")]
    NonCanonical(&'static str),
    #[error("bootstrap durable-unit offset arithmetic overflowed")]
    OffsetOverflow,
    #[error("bootstrap durable-unit action does not match the inspected frontier")]
    InvalidStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct UnitIdentity {
    record: BootstrapRecord,
    sequence: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct EphemeralDurability {
    frame_synced: bool,
    acknowledgement_synced: bool,
    acknowledgement_verified: bool,
    seal_synced: bool,
    seal_verified: bool,
}

/// Exact frame/ACK/seal executor over an already verified handle-relative ledger backend.
///
/// Sync and reread facts are deliberately process-local. A newly constructed machine starts with
/// every fact false and safely repeats sync/reread operations before advancing durable bytes.
pub(in crate::mapped_file::retirement::bootstrap) struct DurableUnitMachine<I> {
    io: I,
    durability: BTreeMap<UnitIdentity, EphemeralDurability>,
}

impl<I> DurableUnitMachine<I>
where
    I: LedgerIo,
{
    pub(in crate::mapped_file::retirement::bootstrap) fn new(io: I) -> Self {
        Self {
            io,
            durability: BTreeMap::new(),
        }
    }

    pub(in crate::mapped_file::retirement::bootstrap) fn inspect(
        &mut self,
        record: BootstrapRecord,
        planned: &PlannedAcknowledgedUnit,
    ) -> Result<DurableUnitProgress, DurableUnitError> {
        let durability = self.durability(record, planned.sequence);
        let log_length = self.io.log_len()?;
        if log_length < planned.frame_start_offset {
            return Err(DurableUnitError::NonCanonical("log ends before planned frame start"));
        }
        if log_length < planned.frame_end_offset {
            let prefix_length = usize::try_from(log_length - planned.frame_start_offset)
                .map_err(|_| DurableUnitError::OffsetOverflow)?;
            self.require_log_bytes(planned.frame_start_offset, &planned.frame[..prefix_length])?;
            return Ok(if prefix_length == 0 {
                DurableUnitProgress::Missing
            } else {
                DurableUnitProgress::ExactFramePrefix
            });
        }
        self.require_log_bytes(planned.frame_start_offset, &planned.frame)?;

        let sealed_end = planned.sealed_log_length;
        if log_length > sealed_end {
            self.require_log_bytes(planned.frame_end_offset, &planned.seal)?;
            return Ok(DurableUnitProgress::Committed);
        }

        let acknowledgement = self.io.read_acknowledgement_slot(slot_index(planned)?)?;
        if log_length == planned.frame_end_offset {
            if !durability.frame_synced {
                return Ok(DurableUnitProgress::FrameWritten);
            }
            if acknowledgement != planned.acknowledgement_slot {
                return Ok(DurableUnitProgress::FrameSynced);
            }
            if !durability.acknowledgement_synced {
                return Ok(DurableUnitProgress::AcknowledgementWritten);
            }
            if !durability.acknowledgement_verified {
                return Ok(DurableUnitProgress::AcknowledgementSynced);
            }
            return Ok(DurableUnitProgress::AcknowledgementVerified);
        }

        if acknowledgement != planned.acknowledgement_slot {
            return Err(DurableUnitError::NonCanonical(
                "seal bytes exist without the exact acknowledgement slot",
            ));
        }
        if log_length < sealed_end {
            let prefix_length =
                usize::try_from(log_length - planned.frame_end_offset).map_err(|_| DurableUnitError::OffsetOverflow)?;
            self.require_log_bytes(planned.frame_end_offset, &planned.seal[..prefix_length])?;
            return Ok(if prefix_length == 0 {
                DurableUnitProgress::AcknowledgementVerified
            } else {
                DurableUnitProgress::ExactSealPrefix
            });
        }
        self.require_log_bytes(planned.frame_end_offset, &planned.seal)?;
        if durability.seal_verified {
            Ok(DurableUnitProgress::Committed)
        } else if !durability.seal_synced {
            Ok(DurableUnitProgress::SealWritten)
        } else {
            Ok(DurableUnitProgress::SealSynced)
        }
    }

    pub(in crate::mapped_file::retirement::bootstrap) fn advance(
        &mut self,
        record: BootstrapRecord,
        planned: &PlannedAcknowledgedUnit,
        step: DurableUnitStep,
    ) -> Result<(), DurableUnitError> {
        let identity = UnitIdentity {
            record,
            sequence: planned.sequence,
        };
        self.durability.entry(identity).or_default();
        match step {
            DurableUnitStep::AppendFrame | DurableUnitStep::CompleteFrame => {
                self.append_remaining(planned.frame_start_offset, &planned.frame)?;
            }
            DurableUnitStep::SyncFrame => {
                self.io.sync_log()?;
                self.durability.entry(identity).or_default().frame_synced = true;
            }
            DurableUnitStep::WriteAcknowledgementSlot => {
                self.io
                    .write_acknowledgement_slot(slot_index(planned)?, &planned.acknowledgement_slot)?;
            }
            DurableUnitStep::SyncAcknowledgementSlot => {
                self.io.sync_acknowledgement_file()?;
                self.durability.entry(identity).or_default().acknowledgement_synced = true;
            }
            DurableUnitStep::VerifyAcknowledgementSlot => {
                let actual = self.io.read_acknowledgement_slot(slot_index(planned)?)?;
                if actual != planned.acknowledgement_slot {
                    return Err(DurableUnitError::NonCanonical("acknowledgement reread mismatch"));
                }
                self.durability.entry(identity).or_default().acknowledgement_verified = true;
            }
            DurableUnitStep::AppendSeal | DurableUnitStep::CompleteSeal => {
                self.append_remaining(planned.frame_end_offset, &planned.seal)?;
            }
            DurableUnitStep::SyncSeal => {
                self.io.sync_log()?;
                self.durability.entry(identity).or_default().seal_synced = true;
            }
            DurableUnitStep::VerifySealAndEof => {
                self.require_log_bytes(planned.frame_end_offset, &planned.seal)?;
                if self.io.log_len()? != planned.sealed_log_length {
                    return Err(DurableUnitError::NonCanonical("sealed log EOF mismatch"));
                }
                self.durability.entry(identity).or_default().seal_verified = true;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement::bootstrap) const fn io_for_test(&self) -> &I {
        &self.io
    }

    fn durability(&mut self, record: BootstrapRecord, sequence: u64) -> EphemeralDurability {
        let identity = UnitIdentity { record, sequence };
        *self.durability.entry(identity).or_default()
    }

    fn append_remaining(&mut self, start: u64, expected: &[u8]) -> Result<(), DurableUnitError> {
        let actual = self.io.log_len()?;
        let end = start
            .checked_add(u64::try_from(expected.len()).map_err(|_| DurableUnitError::OffsetOverflow)?)
            .ok_or(DurableUnitError::OffsetOverflow)?;
        if actual < start || actual > end {
            return Err(DurableUnitError::NonCanonical(
                "append frontier is outside planned bytes",
            ));
        }
        let consumed = usize::try_from(actual - start).map_err(|_| DurableUnitError::OffsetOverflow)?;
        self.require_log_bytes(start, &expected[..consumed])?;
        self.io.append_log(actual, &expected[consumed..])?;
        Ok(())
    }

    fn require_log_bytes(&mut self, offset: u64, expected: &[u8]) -> Result<(), DurableUnitError> {
        if expected.is_empty() {
            return Ok(());
        }
        let mut actual = Vec::new();
        actual
            .try_reserve_exact(expected.len())
            .map_err(|_| DurableUnitError::OffsetOverflow)?;
        actual.resize(expected.len(), 0);
        self.io.read_log_exact(offset, &mut actual)?;
        if actual != expected {
            return Err(DurableUnitError::NonCanonical("log byte mismatch"));
        }
        Ok(())
    }
}

fn slot_index(planned: &PlannedAcknowledgedUnit) -> Result<u8, DurableUnitError> {
    u8::try_from(
        planned
            .acknowledgement_epoch
            .checked_sub(1)
            .ok_or(DurableUnitError::OffsetOverflow)?
            & 1,
    )
    .map_err(|_| DurableUnitError::OffsetOverflow)
}
