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

use super::MappedFileAdmissionState;

/// Result of one mapped-file namespace deletion attempt.
///
/// This type deliberately separates a live-reference deferral from a filesystem failure. A
/// successful namespace deletion does not claim that mmap or file owners have been physically
/// dropped; that remains owner-lifetime driven.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum MappedFileDestroyOutcome {
    /// The mapped-file path was removed from the filesystem namespace.
    NamespaceRemoved,
    /// Logical cleanup is waiting for outstanding compatibility references.
    CleanupPending {
        /// Compatibility reference count observed after the shutdown attempt.
        ref_count: i64,
    },
    /// The namespace deletion attempt failed.
    DeleteFailed {
        /// Portable I/O error category.
        kind: io::ErrorKind,
        /// Platform-specific error code, when provided by the operating system.
        raw_os_error: Option<i32>,
    },
}

impl MappedFileDestroyOutcome {
    /// Returns whether this attempt verified namespace removal.
    #[inline]
    pub fn is_namespace_removed(&self) -> bool {
        matches!(self, Self::NamespaceRemoved)
    }
}

/// Result of one in-process physical-owner detach attempt.
///
/// Detaching removes the mapping and file owners from the mapped-file slots. It does not claim
/// that an external owner has already been dropped, that the operating system has completed an
/// unmap/last-close, or that the path was removed from the filesystem namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum MappedFileDetachOutcome {
    /// This call detached both owner slots.
    Detached {
        /// Published mapping generation removed from the slot, when one existed.
        mapping_generation: Option<u64>,
        /// Whether the file-owner slot contained an owner.
        had_file_owner: bool,
    },
    /// A previous call already detached both slots.
    AlreadyDetached,
    /// Another caller currently owns the detach transition.
    InProgress,
    /// Closing has not drained every admitted operation.
    Pending {
        /// Admission state observed while attempting detach.
        state: MappedFileAdmissionState,
        /// Number of admitted operations that must still complete.
        active_leases: usize,
    },
}

impl MappedFileDetachOutcome {
    /// Returns whether both physical owner slots are detached after this result.
    #[inline]
    pub fn is_detached(self) -> bool {
        matches!(self, Self::Detached { .. } | Self::AlreadyDetached)
    }
}
