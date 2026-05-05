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

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::provider::MemoryProvider;
use crate::provider::PosixProvider;
use crate::provider::TieredStoreProvider;

#[derive(Clone)]
pub enum ProviderKind {
    Posix(PosixProvider),
    Memory(MemoryProvider),
}

impl ProviderKind {
    pub fn from_config(config: &TieredStoreConfig) -> Result<Self, RocketMQError> {
        match config.backend_provider.as_str() {
            "posix" => Ok(Self::Posix(PosixProvider::new(config.store_path_root_dir.clone()))),
            "memory" => Ok(Self::Memory(MemoryProvider::default())),
            provider => Err(RocketMQError::illegal_argument(format!(
                "unsupported tiered backend provider: {provider}"
            ))),
        }
    }
}

impl TieredStoreProvider for ProviderKind {
    async fn create_segment(
        &self,
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, RocketMQError>
    where
        Self: Sized,
    {
        let metadata = crate::metadata::FileSegmentMetadata::new(path.clone(), segment_type, base_offset);
        Ok(TieredFileSegment::new(
            path,
            segment_type,
            base_offset,
            max_size,
            metadata,
            self.clone(),
        ))
    }

    async fn segment_size(&self, path: String) -> Result<u64, RocketMQError> {
        match self {
            Self::Posix(provider) => provider.segment_size(path).await,
            Self::Memory(provider) => provider.segment_size(path).await,
        }
    }

    async fn read(&self, path: String, position: u64, length: usize) -> Result<Bytes, RocketMQError> {
        match self {
            Self::Posix(provider) => provider.read(path, position, length).await,
            Self::Memory(provider) => provider.read(path, position, length).await,
        }
    }

    async fn write(&self, path: String, position: u64, data: Bytes) -> Result<usize, RocketMQError> {
        match self {
            Self::Posix(provider) => provider.write(path, position, data).await,
            Self::Memory(provider) => provider.write(path, position, data).await,
        }
    }

    async fn delete(&self, path: String) -> Result<(), RocketMQError> {
        match self {
            Self::Posix(provider) => provider.delete(path).await,
            Self::Memory(provider) => provider.delete(path).await,
        }
    }
}
