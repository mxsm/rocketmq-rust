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
mod read_range;
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
    prepare_managed_lifecycle_activation, ManagedQueueDescriptor, PreparedManagedLifecycleActivation,
};
#[doc(hidden)]
pub use retirement::bootstrap::{bootstrap_managed_lifecycle_under_exclusive_lock, InitialBootstrapCompletion};
#[doc(hidden)]
pub use retirement::registry::ManagedMappedFileQueueGeneration;
#[doc(hidden)]
pub use retirement::registry::MappedFileQueueGeneration;
#[doc(hidden)]
pub use retirement::registry::MappedFileQueueSnapshot;
#[doc(hidden)]
pub use retirement::replay::{
    inspect_managed_lifecycle_read_only_for_store, inspect_managed_lifecycle_read_only_with_limits_for_store,
    inspect_managed_lifecycle_under_exclusive_lock_for_store, LockedManagedLifecycleInspection,
    ManagedLifecycleReadLimits, ManagedLifecycleReadOutcome, ManagedLifecycleRecoveryReason, ManagedLifecycleSession,
};
#[doc(hidden)]
pub use retirement::service::{
    ManagedIncarnationCreateRequest, ManagedIncarnationCreation, ManagedLifecycleRuntime, ManagedRetirementBatchReport,
    ManagedRetirementReason, ManagedRetirementStage, ManagedRetirementSubmission,
};
#[doc(hidden)]
pub use retirement::state::reconciliation::{
    ManagedReconciliationDisposition, ManagedReconciliationLimits, ManagedRecoverySession, ReconciledLifecycleSession,
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
pub use flush_strategy::FlushStrategy;
pub use generation::MappedReadLease;
pub use generation::MappingGenerationId;
pub use io_uring_impl::io_uring_backend_status;
pub use io_uring_impl::IoUringBackendStatus;
pub use lifecycle::LifecycleAcquireOutcome;
pub use lifecycle::LifecycleAcquireRejection;
pub use lifecycle::MappedFileAdmissionState;
pub use lifecycle::MappedFileLifecycleSnapshot;
pub use lifecycle::MappedFileOperation;
pub use lifecycle_outcome::MappedFileDestroyOutcome;
pub use lifecycle_outcome::MappedFileDetachOutcome;
pub use mapped_buffer::MappedBuffer;
pub(crate) use mapped_file_error::MappedFileFailure;
pub use memory::MappedMemory;
pub use memory::NativeMappedMemory;
pub use memory::NativeReadOnlyMappedMemory;
pub use memory::ReadOnlyMappedMemory;
/// Legacy type name for the new owner-bound native read-only mapped lease.
///
/// Construction intentionally moved to `DefaultMappedFile::try_mapped_read_lease`; the former
/// raw writable-mmap constructor is not retained.
pub type MmapRegionSlice = MappedReadLease<NativeReadOnlyMappedMemory>;
pub use metrics::MappedFileMetrics;
pub use raw::MappedFileRawCore;
pub use read_range::MappedReadRange;
pub use select_result::SelectMappedBufferCacheState;
pub use select_result::SelectMappedBufferResult;
pub use select_result::SelectMappedBufferSourceKind;

#[cfg(test)]
mod kernel_scenarios;

#[cfg(test)]
mod platform_delete_unix_tests;

#[cfg(test)]
mod queue_lifecycle_scenarios;

#[cfg(test)]
mod write_lease_miri_tests;

#[cfg(test)]
mod write_lease_scenarios;

#[cfg(test)]
mod mmap_region_bounds_tests;

#[cfg(test)]
mod physical_owner_scenarios;

#[cfg(test)]
mod read_range_scenarios;

#[cfg(test)]
mod mapping_scenarios;

#[cfg(test)]
mod admission_scenarios;
