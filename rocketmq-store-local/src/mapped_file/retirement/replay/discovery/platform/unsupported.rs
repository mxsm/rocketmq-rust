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

use super::FileStamp;
use super::InventoryEntry;
use super::InventorySnapshot;
use super::OpenedEntry;
use super::PlatformError;

pub(in crate::mapped_file::retirement) struct LifecycleDirectory;

impl LifecycleDirectory {
    pub(in crate::mapped_file::retirement) fn open(_root: &File, _name: &str) -> Result<Option<Self>, PlatformError> {
        Err(PlatformError::unsupported())
    }

    pub(in crate::mapped_file::retirement) fn enumerate(
        &self,
        _maximum: usize,
    ) -> Result<InventorySnapshot, PlatformError> {
        Err(PlatformError::unsupported())
    }

    pub(in crate::mapped_file::retirement) fn open_entry(
        &self,
        _entry: &InventoryEntry,
    ) -> Result<OpenedEntry, PlatformError> {
        Err(PlatformError::unsupported())
    }
}

pub(super) fn stamp(_file: &File) -> Result<FileStamp, PlatformError> {
    Err(PlatformError::unsupported())
}

pub(super) fn enumerate_directory(_directory: &File, _maximum: usize) -> Result<Vec<InventoryEntry>, PlatformError> {
    Err(PlatformError::unsupported())
}

pub(super) fn open_entry(_parent: &File, _entry: &InventoryEntry) -> Result<OpenedEntry, PlatformError> {
    Err(PlatformError::unsupported())
}
