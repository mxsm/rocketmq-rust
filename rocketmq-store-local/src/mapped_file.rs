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
pub mod queue_maintenance;
#[doc(hidden)]
pub mod queue_state;
#[doc(hidden)]
pub mod queue_storage;
pub mod raw;

pub use contract::MappedFile;
pub use default_mapped_file::DefaultMappedFile;
pub use default_mapped_file::LazyMmapStats;
pub use default_mapped_file::OS_PAGE_SIZE;
pub use direct_io::DirectIoBuffer;
pub use direct_io::DirectIoRequest;
pub use direct_io::DirectIoValidationError;
pub use flush_strategy::FlushStrategy;
pub use io_uring_impl::io_uring_backend_status;
pub use io_uring_impl::IoUringBackendStatus;
pub use mapped_buffer::MappedBuffer;
pub use mapped_file_error::MappedFileError;
pub use mapped_file_error::MappedFileResult;
pub use memory::MappedMemory;
pub use memory::MmapRegionSlice;
pub use memory::NativeMappedMemory;
pub use metrics::MappedFileMetrics;
pub use raw::MappedFileRawCore;
pub use select_result::SelectMappedBufferCacheState;
pub use select_result::SelectMappedBufferResult;
pub use select_result::SelectMappedBufferSourceKind;
