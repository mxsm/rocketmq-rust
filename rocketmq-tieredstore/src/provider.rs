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

use std::future::Future;

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;

pub use memory_file_segment::MemoryProvider;
pub use posix_file_segment::PosixProvider;
pub use posix_file_segment::PosixProviderIoSnapshot;
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

    /// Makes prior writes to `path` durable when the backend exposes an explicit sync operation.
    ///
    /// Remote providers whose successful write is already durable may keep the default no-op.
    fn sync(&self, _path: String) -> impl Future<Output = Result<(), RocketMQError>> {
        async { Ok(()) }
    }

    /// Renames one file or directory prefix without exposing a partially copied destination.
    fn rename(&self, source: String, _destination: String) -> impl Future<Output = Result<(), RocketMQError>> {
        async move {
            Err(RocketMQError::storage_write_failed(
                source,
                "tiered provider does not support atomic rename",
            ))
        }
    }

    /// Lists provider paths rooted at `prefix`.
    fn list(&self, _prefix: String) -> impl Future<Output = Result<Vec<String>, RocketMQError>> {
        async { Ok(Vec::new()) }
    }

    /// Deletes a file or directory prefix and all paths below it.
    fn delete_prefix(&self, prefix: String) -> impl Future<Output = Result<(), RocketMQError>> {
        async move {
            for path in self.list(prefix.clone()).await? {
                self.delete(path).await?;
            }
            self.delete(prefix).await
        }
    }

    /// Publishes a small metadata file with atomic replacement semantics.
    fn atomic_write(&self, path: String, data: Bytes) -> impl Future<Output = Result<(), RocketMQError>> {
        async move {
            let temporary = format!("{path}.tmp");
            self.delete_prefix(temporary.clone()).await?;
            let expected = data.len();
            let written = self.write(temporary.clone(), 0, data).await?;
            if written != expected {
                return Err(RocketMQError::storage_write_failed(
                    temporary,
                    format!("partial metadata write: expected {expected}, wrote {written}"),
                ));
            }
            self.sync(temporary.clone()).await?;
            self.rename(temporary, path).await
        }
    }
}
