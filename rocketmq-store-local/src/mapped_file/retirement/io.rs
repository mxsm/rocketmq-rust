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

#[cfg(any(unix, windows, test))]
use std::io;

use thiserror::Error;

use super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;

#[cfg(unix)]
mod unix;
#[cfg(not(any(unix, windows)))]
mod unsupported;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
#[allow(
    unused_imports,
    reason = "the platform facade is staged for managed lifecycle activation wiring"
)]
pub(super) use unix::managed_lifecycle_writer_supported;
#[cfg(unix)]
pub(super) use unix::FileLedgerIo;
#[cfg(not(any(unix, windows)))]
#[allow(
    unused_imports,
    reason = "the platform facade is staged for managed lifecycle activation wiring"
)]
pub(super) use unsupported::managed_lifecycle_writer_supported;
#[cfg(not(any(unix, windows)))]
#[allow(
    unused_imports,
    reason = "the handle backend facade is staged for managed lifecycle activation wiring"
)]
pub(super) use unsupported::FileLedgerIo;
#[cfg(windows)]
#[allow(
    unused_imports,
    reason = "the platform facade is staged for managed lifecycle activation wiring"
)]
pub(super) use windows::managed_lifecycle_writer_supported;
#[cfg(windows)]
#[allow(
    unused_imports,
    reason = "the handle-relative backend is staged for managed lifecycle activation wiring"
)]
pub(super) use windows::FileLedgerIo;

mod private {
    pub trait Sealed {}
}

impl private::Sealed for FileLedgerIo {}

#[cfg(test)]
impl private::Sealed for super::writer::model_io::ModelLedgerIo {}

/// One blocking filesystem operation in the durable acknowledgement protocol.
#[cfg(any(unix, windows, test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IoOperation {
    AppendLog,
    SyncLog,
    WriteAcknowledgementSlot,
    SyncAcknowledgementFile,
    ReadAcknowledgementSlot,
    ReadLog,
    ReadLogLength,
    #[cfg(any(unix, windows))]
    OpenLifecycleDirectory,
    #[cfg(any(unix, windows))]
    OpenLog,
    #[cfg(any(unix, windows))]
    OpenAcknowledgementFile,
    #[cfg(any(unix, windows))]
    InspectHandle,
}

/// Failure from a handle-relative ledger storage backend.
#[derive(Debug, Error)]
pub(super) enum LedgerIoError {
    #[cfg(any(unix, windows, test))]
    #[error("ledger I/O operation {operation:?} failed: {source}")]
    Io {
        operation: IoOperation,
        #[source]
        source: io::Error,
    },
    #[cfg(any(unix, windows, test))]
    #[error("{object} offset mismatch: expected {expected}, found {actual}")]
    OffsetMismatch {
        object: &'static str,
        expected: u64,
        actual: u64,
    },
    #[cfg(any(unix, windows, test))]
    #[error("{object} length overflow")]
    LengthOverflow { object: &'static str },
    #[cfg(any(unix, windows))]
    #[error("{object} is not a regular file")]
    NotRegularFile { object: &'static str },
    #[cfg(any(unix, windows))]
    #[error("{object} must have exactly one hard link, found {actual}")]
    UnexpectedLinkCount { object: &'static str, actual: u64 },
    #[cfg(any(unix, windows))]
    #[error("{object} is not a directory")]
    NotDirectory { object: &'static str },
    #[cfg(any(unix, windows))]
    #[error("{object} has invalid length: expected {expected}, found {actual}")]
    InvalidLength {
        object: &'static str,
        expected: u64,
        actual: u64,
    },
    #[cfg(any(unix, windows))]
    #[error("invalid acknowledgement slot index {slot_index}")]
    InvalidAcknowledgementSlotIndex { slot_index: u8 },
    #[cfg(unix)]
    #[error("lifecycle directory escaped the Store filesystem")]
    CrossDeviceLifecycleDirectory,
    #[cfg(unix)]
    #[error("{object} escaped the lifecycle filesystem")]
    CrossDeviceObject { object: &'static str },
    #[cfg(any(unix, windows))]
    #[error("{object} namespace binding changed after its handle was retained")]
    BindingChanged { object: &'static str },
    #[cfg(windows)]
    #[error("{object} is a reparse point")]
    ReparsePoint { object: &'static str },
    #[error("managed lifecycle writer capability is unsupported on {platform}: {reason}")]
    UnsupportedPlatform {
        platform: &'static str,
        reason: &'static str,
    },
}

impl LedgerIoError {
    #[cfg(any(unix, windows, test))]
    pub(super) fn io(operation: IoOperation, source: io::Error) -> Self {
        Self::Io { operation, source }
    }
}

/// Synchronous ledger I/O run as one operation on the injected storage blocking executor.
///
/// Implementations must use already-opened handles or handle-relative, no-follow opens. A method
/// error is always ambiguous to the writer: it may have changed durable bytes before failing.
pub(super) trait LedgerIo: private::Sealed {
    fn append_log(&mut self, expected_offset: u64, bytes: &[u8]) -> Result<(), LedgerIoError>;

    fn sync_log(&mut self) -> Result<(), LedgerIoError>;

    fn write_acknowledgement_slot(
        &mut self,
        slot_index: u8,
        bytes: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    ) -> Result<(), LedgerIoError>;

    fn sync_acknowledgement_file(&mut self) -> Result<(), LedgerIoError>;

    fn read_acknowledgement_slot(&mut self, slot_index: u8)
        -> Result<[u8; ACKNOWLEDGEMENT_SLOT_LENGTH], LedgerIoError>;

    fn read_log_exact(&mut self, offset: u64, output: &mut [u8]) -> Result<(), LedgerIoError>;

    fn log_len(&mut self) -> Result<u64, LedgerIoError>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn ledger_io_implementation_surface_is_sealed_inside_the_io_module() {
        let source = include_str!("io.rs").replace("\r\n", "\n");
        let production = source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .expect("visibility test follows production I/O code")
            .0;
        let declaration = production
            .lines()
            .find(|line| line.contains("trait LedgerIo"))
            .expect("LedgerIo declaration exists");

        assert!(declaration.trim_start().starts_with("pub(super)"));
        assert!(declaration.contains(": private::Sealed"));
    }
}
