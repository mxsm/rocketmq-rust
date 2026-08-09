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

use std::fs::File;

use super::LedgerIo;
use super::LedgerIoError;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;

const UNSUPPORTED_REASON: &str = "no audited handle-relative lifecycle backend exists for this target";

pub(in crate::mapped_file::retirement) struct FileLedgerIo {
    _private: (),
}

impl FileLedgerIo {
    pub(in crate::mapped_file::retirement) fn open_from_store_root(
        _store_root: &File,
        _log_generation: u64,
    ) -> Result<Self, LedgerIoError> {
        Err(unsupported())
    }
}

pub(in crate::mapped_file::retirement) const fn managed_lifecycle_writer_supported() -> bool {
    false
}

impl LedgerIo for FileLedgerIo {
    fn append_log(&mut self, _expected_offset: u64, _bytes: &[u8]) -> Result<(), LedgerIoError> {
        Err(unsupported())
    }

    fn sync_log(&mut self) -> Result<(), LedgerIoError> {
        Err(unsupported())
    }

    fn write_acknowledgement_slot(
        &mut self,
        _slot_index: u8,
        _bytes: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    ) -> Result<(), LedgerIoError> {
        Err(unsupported())
    }

    fn sync_acknowledgement_file(&mut self) -> Result<(), LedgerIoError> {
        Err(unsupported())
    }

    fn read_acknowledgement_slot(
        &mut self,
        _slot_index: u8,
    ) -> Result<[u8; ACKNOWLEDGEMENT_SLOT_LENGTH], LedgerIoError> {
        Err(unsupported())
    }

    fn read_log_exact(&mut self, _offset: u64, _output: &mut [u8]) -> Result<(), LedgerIoError> {
        Err(unsupported())
    }

    fn log_len(&mut self) -> Result<u64, LedgerIoError> {
        Err(unsupported())
    }
}

fn unsupported() -> LedgerIoError {
    LedgerIoError::UnsupportedPlatform {
        platform: "unsupported target",
        reason: UNSUPPORTED_REASON,
    }
}
