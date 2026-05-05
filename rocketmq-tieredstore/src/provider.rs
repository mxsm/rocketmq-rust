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

pub mod memory_file_segment;
pub mod posix_file_segment;
pub mod provider_impl;

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;

pub use memory_file_segment::MemoryProvider;
pub use posix_file_segment::PosixProvider;
pub use provider_impl::ProviderKind;

#[trait_variant::make(TieredStoreProvider: Send)]
pub trait TieredStoreProviderInner: Sync + Clone + 'static {
    async fn create_segment(
        &self,
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, RocketMQError>
    where
        Self: Sized;

    async fn segment_size(&self, path: String) -> Result<u64, RocketMQError>;

    async fn read(&self, path: String, position: u64, length: usize) -> Result<Bytes, RocketMQError>;

    async fn write(&self, path: String, position: u64, data: Bytes) -> Result<usize, RocketMQError>;

    async fn delete(&self, path: String) -> Result<(), RocketMQError>;
}
