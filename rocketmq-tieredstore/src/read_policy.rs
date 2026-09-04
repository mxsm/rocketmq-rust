// Copyright 2023 The RocketMQ Rust Authors
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

use rocketmq_store_api::StoreError;

use crate::TieredStorageLevel;

/// Residency of the LocalFile candidate considered by a Tiered read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieredLocalResidency {
    Memory,
    Disk,
    Missing,
}

/// Inputs that select the owner for one read operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieredReadContext {
    local_residency: TieredLocalResidency,
    force_local: bool,
    remote_only: bool,
}

impl TieredReadContext {
    pub const fn new(local_residency: TieredLocalResidency) -> Self {
        Self {
            local_residency,
            force_local: false,
            remote_only: false,
        }
    }

    pub const fn force_local(mut self) -> Self {
        self.force_local = true;
        self.remote_only = false;
        self
    }

    pub const fn remote_only(mut self) -> Self {
        self.remote_only = true;
        self.force_local = false;
        self
    }

    pub const fn local_available(self) -> bool {
        !matches!(self.local_residency, TieredLocalResidency::Missing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieredReadSource {
    Local,
    Tiered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieredReadErrorDisposition {
    FallbackToLocal,
    Miss,
    Fatal,
}

/// Closed read routing and error classification policy shared by all Tiered
/// read APIs.
#[derive(Debug, Clone, Copy)]
pub struct TieredReadPolicy {
    storage_level: TieredStorageLevel,
}

impl TieredReadPolicy {
    pub const fn new(storage_level: TieredStorageLevel) -> Self {
        Self { storage_level }
    }

    pub const fn select(self, context: TieredReadContext) -> TieredReadSource {
        if context.force_local {
            return TieredReadSource::Local;
        }
        if context.remote_only {
            return TieredReadSource::Tiered;
        }
        match (self.storage_level, context.local_residency) {
            (TieredStorageLevel::Disable, _) => TieredReadSource::Local,
            (TieredStorageLevel::NotInDisk, TieredLocalResidency::Missing) => TieredReadSource::Tiered,
            (TieredStorageLevel::NotInDisk, _) => TieredReadSource::Local,
            (TieredStorageLevel::NotInMem, TieredLocalResidency::Memory) => TieredReadSource::Local,
            (TieredStorageLevel::NotInMem, _) | (TieredStorageLevel::Force, _) => TieredReadSource::Tiered,
        }
    }

    pub fn classify_error(error: &StoreError, local_available: bool) -> TieredReadErrorDisposition {
        if local_available
            && matches!(
                error.descriptor(),
                descriptor
                    if descriptor == &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE
                        || descriptor == &rocketmq_error::STORAGE_OPERATION_TIMED_OUT
                        || descriptor == &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED
                        || descriptor == &rocketmq_error::STORAGE_READ_FAILED
                        || descriptor == &rocketmq_error::STORAGE_IO_FAILED
            )
        {
            TieredReadErrorDisposition::FallbackToLocal
        } else {
            TieredReadErrorDisposition::Fatal
        }
    }
}
