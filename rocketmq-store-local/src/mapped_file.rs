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

#[doc(hidden)]
pub mod allocation_policy;
#[doc(hidden)]
pub mod allocation_request;
mod contract;
mod default_mapped_file;
mod direct_io;
pub mod file;
mod flush_strategy;
mod generation;
#[doc(hidden)]
pub mod lifecycle;
mod lifecycle_model;
mod lifecycle_outcome;
mod mapped_buffer;
mod mapped_file_error;
mod memory;
mod metrics;
mod select_result;

pub mod io_uring_impl;
#[doc(hidden)]
pub mod kernel;
pub mod mapping;
#[doc(hidden)]
pub mod queue_allocation;
#[doc(hidden)]
pub mod queue_index;
#[doc(hidden)]
pub mod queue_io;
#[doc(hidden)]
pub mod queue_lifecycle;
#[doc(hidden)]
pub mod queue_maintenance;
#[doc(hidden)]
pub mod queue_metrics;
#[doc(hidden)]
pub mod queue_state;
#[doc(hidden)]
pub mod queue_storage;
pub mod raw;
pub(crate) mod retirement;

#[doc(hidden)]
pub use retirement::activation::{
    prepare_managed_lifecycle_activation, ManagedLifecycleActivationError, ManagedLifecycleActivationErrorKind,
    ManagedQueueDescriptor, PreparedManagedLifecycleActivation,
};
#[doc(hidden)]
pub use retirement::bootstrap::{
    bootstrap_managed_lifecycle_under_exclusive_lock, InitialBootstrapCompletion, ManagedLifecycleBootstrapError,
    ManagedLifecycleBootstrapErrorKind,
};
#[doc(hidden)]
pub use retirement::registry::ManagedMappedFileQueueGeneration;
#[doc(hidden)]
pub use retirement::registry::MappedFileQueueGeneration;
#[doc(hidden)]
pub use retirement::registry::MappedFileQueueSnapshot;
#[doc(hidden)]
pub use retirement::replay::{
    inspect_managed_lifecycle_read_only, inspect_managed_lifecycle_read_only_with_limits,
    inspect_managed_lifecycle_under_exclusive_lock, LockedManagedLifecycleInspection, ManagedLifecycleReadError,
    ManagedLifecycleReadErrorKind, ManagedLifecycleReadLimits, ManagedLifecycleReadOutcome,
    ManagedLifecycleRecoveryReason, ManagedLifecycleSession,
};
#[doc(hidden)]
pub use retirement::service::{
    ManagedIncarnationCreateRequest, ManagedIncarnationCreation, ManagedIncarnationCreationError,
    ManagedIncarnationCreationErrorKind, ManagedLifecycleRuntime, ManagedRetirementBatchReport,
    ManagedRetirementReason, ManagedRetirementStage, ManagedRetirementSubmission, ManagedRetirementSubmissionError,
    ManagedRetirementSubmissionErrorKind,
};
#[doc(hidden)]
pub use retirement::state::reconciliation::{
    ManagedReconciliationDisposition, ManagedReconciliationError, ManagedReconciliationErrorKind,
    ManagedReconciliationLimits, ManagedRecoverySession, ManagedSegmentClaimError, ReconciledLifecycleSession,
    ReconciledSegmentFile,
};

/// Exercises every bounded mapped-file lifecycle decoder with an arbitrary byte slice.
///
/// This hidden entry point exists for the standalone recovery-record fuzz target. It deliberately
/// discards typed decode errors because the fuzzing invariant is total, bounded decoding without
/// panics or out-of-bounds reads.
#[doc(hidden)]
pub fn fuzz_decode_mapped_file_lifecycle(input: &[u8]) {
    retirement::fuzz_decode_lifecycle(input);
}

pub use contract::MappedFile;
pub use contract::MappedWriteLease;
pub use default_mapped_file::DefaultMappedFile;
pub use default_mapped_file::DefaultMappedWriteLease;
pub use default_mapped_file::LazyMmapStats;
pub use default_mapped_file::OS_PAGE_SIZE;
pub use direct_io::DirectIoBuffer;
pub use direct_io::DirectIoRequest;
pub use direct_io::DirectIoValidationError;
pub use flush_strategy::FlushStrategy;
pub use generation::MappedReadLease;
pub use generation::MappingGenerationId;
pub use io_uring_impl::io_uring_backend_status;
pub use io_uring_impl::IoUringBackendStatus;
pub use lifecycle::MappedFileAdmissionState;
pub use lifecycle::MappedFileLifecycleSnapshot;
pub use lifecycle::MappedFileOperation;
pub use lifecycle_outcome::MappedFileDestroyOutcome;
pub use lifecycle_outcome::MappedFileDetachOutcome;
pub use mapped_buffer::MappedBuffer;
pub use mapped_file_error::MappedFileError;
pub use mapped_file_error::MappedFileResult;
pub use memory::MappedMemory;
pub use memory::MmapRangeError;
pub use memory::NativeMappedMemory;
pub use memory::NativeReadOnlyMappedMemory;
pub use memory::ReadOnlyMappedMemory;
/// Legacy type name for the new owner-bound native read-only mapped lease.
///
/// Construction intentionally moved to [`DefaultMappedFile::try_mapped_read_lease`]; the former
/// raw writable-mmap constructor is not retained.
pub type MmapRegionSlice = MappedReadLease<NativeReadOnlyMappedMemory>;
pub use metrics::MappedFileMetrics;
pub use raw::MappedFileRawCore;
pub use select_result::SelectMappedBufferCacheState;
pub use select_result::SelectMappedBufferResult;
pub use select_result::SelectMappedBufferSourceKind;
