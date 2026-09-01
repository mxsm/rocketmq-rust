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

use std::io;

use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_FILE_LENGTH;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use crate::mapped_file::retirement::io::IoOperation;
use crate::mapped_file::retirement::io::LedgerIo;
use crate::mapped_file::retirement::io::LedgerIoFailure;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelIoEvent {
    AppendLog { expected_offset: u64, length: usize },
    SyncLog,
    WriteAcknowledgementSlot { slot_index: u8 },
    SyncAcknowledgementFile,
    ReadAcknowledgementSlot { slot_index: u8 },
    ReadLog { offset: u64, length: usize },
    ReadLogLength,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelFaultAction {
    ErrorBefore,
    PartialWrite { length: usize },
    CorruptRead,
    ReportExtraEof { extra: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ModelFault {
    pub(crate) operation_index: usize,
    pub(crate) action: ModelFaultAction,
}

#[derive(Debug)]
pub(crate) struct ModelLedgerIo {
    log: Vec<u8>,
    acknowledgement: [u8; ACKNOWLEDGEMENT_FILE_LENGTH],
    events: Vec<ModelIoEvent>,
    fault: Option<ModelFault>,
}

impl ModelLedgerIo {
    pub(crate) fn empty() -> Self {
        Self {
            log: Vec::new(),
            acknowledgement: [0; ACKNOWLEDGEMENT_FILE_LENGTH],
            events: Vec::new(),
            fault: None,
        }
    }

    pub(crate) fn with_fault(mut self, operation_index: usize, action: ModelFaultAction) -> Self {
        self.fault = Some(ModelFault {
            operation_index,
            action,
        });
        self
    }

    pub(crate) fn log(&self) -> &[u8] {
        &self.log
    }

    pub(crate) fn acknowledgement(&self) -> &[u8; ACKNOWLEDGEMENT_FILE_LENGTH] {
        &self.acknowledgement
    }

    pub(crate) fn events(&self) -> &[ModelIoEvent] {
        &self.events
    }

    fn begin(&mut self, event: ModelIoEvent) -> Option<ModelFaultAction> {
        let operation_index = self.events.len();
        self.events.push(event);
        self.fault
            .filter(|fault| fault.operation_index == operation_index)
            .map(|fault| fault.action)
    }

    fn injected(operation: IoOperation) -> LedgerIoFailure {
        LedgerIoFailure::io(operation, io::Error::other("injected model I/O failure"))
    }
}

impl LedgerIo for ModelLedgerIo {
    fn append_log(&mut self, expected_offset: u64, bytes: &[u8]) -> Result<(), LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::AppendLog {
            expected_offset,
            length: bytes.len(),
        });
        if matches!(action, Some(ModelFaultAction::ErrorBefore)) {
            return Err(Self::injected(IoOperation::AppendLog));
        }
        let actual =
            u64::try_from(self.log.len()).map_err(|_| LedgerIoFailure::LengthOverflow { object: "model log" })?;
        if actual != expected_offset {
            return Err(LedgerIoFailure::OffsetMismatch {
                object: "model log",
                expected: expected_offset,
                actual,
            });
        }
        if let Some(ModelFaultAction::PartialWrite { length }) = action {
            self.log.extend_from_slice(&bytes[..length.min(bytes.len())]);
            return Err(Self::injected(IoOperation::AppendLog));
        }
        self.log.extend_from_slice(bytes);
        Ok(())
    }

    fn sync_log(&mut self) -> Result<(), LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::SyncLog);
        if action.is_some() {
            return Err(Self::injected(IoOperation::SyncLog));
        }
        Ok(())
    }

    fn write_acknowledgement_slot(
        &mut self,
        slot_index: u8,
        bytes: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    ) -> Result<(), LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::WriteAcknowledgementSlot { slot_index });
        if matches!(action, Some(ModelFaultAction::ErrorBefore)) {
            return Err(Self::injected(IoOperation::WriteAcknowledgementSlot));
        }
        let start = usize::from(slot_index) * ACKNOWLEDGEMENT_SLOT_LENGTH;
        let end = start
            .checked_add(ACKNOWLEDGEMENT_SLOT_LENGTH)
            .ok_or(LedgerIoFailure::LengthOverflow {
                object: "model acknowledgement slot",
            })?;
        let Some(destination) = self.acknowledgement.get_mut(start..end) else {
            return Err(LedgerIoFailure::OffsetMismatch {
                object: "model acknowledgement slot",
                expected: ACKNOWLEDGEMENT_FILE_LENGTH as u64,
                actual: start as u64,
            });
        };
        if let Some(ModelFaultAction::PartialWrite { length }) = action {
            let length = length.min(bytes.len());
            destination[..length].copy_from_slice(&bytes[..length]);
            return Err(Self::injected(IoOperation::WriteAcknowledgementSlot));
        }
        destination.copy_from_slice(bytes);
        Ok(())
    }

    fn sync_acknowledgement_file(&mut self) -> Result<(), LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::SyncAcknowledgementFile);
        if action.is_some() {
            return Err(Self::injected(IoOperation::SyncAcknowledgementFile));
        }
        Ok(())
    }

    fn read_acknowledgement_slot(
        &mut self,
        slot_index: u8,
    ) -> Result<[u8; ACKNOWLEDGEMENT_SLOT_LENGTH], LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::ReadAcknowledgementSlot { slot_index });
        if matches!(action, Some(ModelFaultAction::ErrorBefore)) {
            return Err(Self::injected(IoOperation::ReadAcknowledgementSlot));
        }
        let start = usize::from(slot_index) * ACKNOWLEDGEMENT_SLOT_LENGTH;
        let end = start
            .checked_add(ACKNOWLEDGEMENT_SLOT_LENGTH)
            .ok_or(LedgerIoFailure::LengthOverflow {
                object: "model acknowledgement slot",
            })?;
        let mut bytes: [u8; ACKNOWLEDGEMENT_SLOT_LENGTH] = self
            .acknowledgement
            .get(start..end)
            .ok_or(LedgerIoFailure::OffsetMismatch {
                object: "model acknowledgement slot",
                expected: ACKNOWLEDGEMENT_FILE_LENGTH as u64,
                actual: start as u64,
            })?
            .try_into()
            .map_err(|_| LedgerIoFailure::LengthOverflow {
                object: "model acknowledgement slot",
            })?;
        if matches!(action, Some(ModelFaultAction::CorruptRead)) {
            bytes[0] ^= 1;
        }
        Ok(bytes)
    }

    fn read_log_exact(&mut self, offset: u64, output: &mut [u8]) -> Result<(), LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::ReadLog {
            offset,
            length: output.len(),
        });
        if matches!(action, Some(ModelFaultAction::ErrorBefore)) {
            return Err(Self::injected(IoOperation::ReadLog));
        }
        let start = usize::try_from(offset).map_err(|_| LedgerIoFailure::LengthOverflow {
            object: "model log offset",
        })?;
        let end = start.checked_add(output.len()).ok_or(LedgerIoFailure::LengthOverflow {
            object: "model log read",
        })?;
        output.copy_from_slice(self.log.get(start..end).ok_or(LedgerIoFailure::OffsetMismatch {
            object: "model log read",
            expected: end as u64,
            actual: self.log.len() as u64,
        })?);
        if matches!(action, Some(ModelFaultAction::CorruptRead)) && !output.is_empty() {
            output[0] ^= 1;
        }
        Ok(())
    }

    fn log_len(&mut self) -> Result<u64, LedgerIoFailure> {
        let action = self.begin(ModelIoEvent::ReadLogLength);
        if matches!(action, Some(ModelFaultAction::ErrorBefore)) {
            return Err(Self::injected(IoOperation::ReadLogLength));
        }
        let length =
            u64::try_from(self.log.len()).map_err(|_| LedgerIoFailure::LengthOverflow { object: "model log" })?;
        if let Some(ModelFaultAction::ReportExtraEof { extra }) = action {
            return length
                .checked_add(extra)
                .ok_or(LedgerIoFailure::LengthOverflow { object: "model log" });
        }
        Ok(length)
    }
}
