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

pub mod factory;
pub mod memory_file_segment;
pub mod posix_file_segment;
pub mod provider_impl;

use std::future::Future;

use bytes::Bytes;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;

pub use factory::BuiltinTieredStoreProviderFactory;
pub use factory::MemoryProviderFactory;
pub use factory::PosixProviderFactory;
pub use factory::TieredProviderCapabilities;
pub use factory::TieredProviderCapability;
pub use factory::TieredProviderDescriptor;
pub use factory::TieredProviderPersistence;
pub use factory::TieredStoreProviderFactory;
pub use memory_file_segment::MemoryProvider;
pub use posix_file_segment::PosixProvider;
pub use posix_file_segment::PosixProviderIoSnapshot;
pub use provider_impl::ProviderKind;

#[trait_variant::make(TieredStoreProvider: Send)]
pub trait TieredStoreProviderInner: Sync + Clone + 'static {
    async fn create_segment(
        &self,
        operation: StoreOperation,
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, StoreError>
    where
        Self: Sized;

    async fn segment_size(&self, operation: StoreOperation, path: String) -> Result<u64, StoreError>;

    async fn read(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        length: usize,
    ) -> Result<Bytes, StoreError>;

    async fn write(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        data: Bytes,
    ) -> Result<usize, StoreError>;

    async fn delete(&self, operation: StoreOperation, path: String) -> Result<(), StoreError>;

    /// Makes prior writes to `path` durable when the backend exposes an explicit sync operation.
    ///
    /// Remote providers whose successful write is already durable may keep the default no-op.
    fn sync(&self, _operation: StoreOperation, _path: String) -> impl Future<Output = Result<(), StoreError>> {
        async { Ok(()) }
    }

    /// Renames one file or directory prefix without exposing a partially copied destination.
    fn rename(
        &self,
        operation: StoreOperation,
        _source: String,
        _destination: String,
    ) -> impl Future<Output = Result<(), StoreError>> {
        async move { Err(crate::error::unsupported(operation)) }
    }

    /// Lists provider paths rooted at `prefix`.
    fn list(
        &self,
        _operation: StoreOperation,
        _prefix: String,
    ) -> impl Future<Output = Result<Vec<String>, StoreError>> {
        async { Ok(Vec::new()) }
    }

    /// Deletes a file or directory prefix and all paths below it.
    fn delete_prefix(&self, operation: StoreOperation, prefix: String) -> impl Future<Output = Result<(), StoreError>> {
        async move {
            for path in self.list(operation, prefix.clone()).await? {
                self.delete(operation, path).await?;
            }
            self.delete(operation, prefix).await
        }
    }

    /// Publishes a small metadata file with atomic replacement semantics.
    fn atomic_write(
        &self,
        operation: StoreOperation,
        path: String,
        data: Bytes,
    ) -> impl Future<Output = Result<(), StoreError>> {
        async move {
            let temporary = format!("{path}.tmp");
            self.delete_prefix(operation, temporary.clone()).await?;
            let expected = data.len();
            let written = self.write(operation, temporary.clone(), 0, data).await?;
            if written != expected {
                return Err(crate::error::contract_error(
                    &rocketmq_error::STORAGE_WRITE_FAILED,
                    operation,
                ));
            }
            self.sync(operation, temporary.clone()).await?;
            self.rename(operation, temporary, path).await
        }
    }
}
