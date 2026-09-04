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
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::config::TieredStoreConfig;
use crate::factory::TieredProviderOpenPlan;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::provider::BuiltinTieredStoreProviderFactory;
use crate::provider::MemoryProvider;
use crate::provider::PosixProvider;
use crate::provider::TieredStoreProvider;
use crate::provider::TieredStoreProviderFactory;

#[derive(Clone)]
pub enum ProviderKind {
    Posix(PosixProvider),
    Memory(MemoryProvider),
}

impl ProviderKind {
    pub fn from_config(config: &TieredStoreConfig) -> Result<Option<Self>, StoreError> {
        let Some(factory) = BuiltinTieredStoreProviderFactory::select(config) else {
            return Ok(None);
        };
        let Some(plan) = TieredProviderOpenPlan::try_new(config.clone(), factory) else {
            return Ok(None);
        };
        let (store_plan, factory) = plan.into_parts();
        factory.create(&store_plan).map(Some)
    }
}

impl TieredStoreProvider for ProviderKind {
    async fn create_segment(
        &self,
        _operation: StoreOperation,
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, StoreError>
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

    async fn segment_size(&self, operation: StoreOperation, path: String) -> Result<u64, StoreError> {
        match self {
            Self::Posix(provider) => provider.segment_size(operation, path).await,
            Self::Memory(provider) => provider.segment_size(operation, path).await,
        }
    }

    async fn read(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        length: usize,
    ) -> Result<Bytes, StoreError> {
        match self {
            Self::Posix(provider) => provider.read(operation, path, position, length).await,
            Self::Memory(provider) => provider.read(operation, path, position, length).await,
        }
    }

    async fn write(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        data: Bytes,
    ) -> Result<usize, StoreError> {
        match self {
            Self::Posix(provider) => provider.write(operation, path, position, data).await,
            Self::Memory(provider) => provider.write(operation, path, position, data).await,
        }
    }

    async fn delete(&self, operation: StoreOperation, path: String) -> Result<(), StoreError> {
        match self {
            Self::Posix(provider) => provider.delete(operation, path).await,
            Self::Memory(provider) => provider.delete(operation, path).await,
        }
    }

    async fn sync(&self, operation: StoreOperation, path: String) -> Result<(), StoreError> {
        match self {
            Self::Posix(provider) => provider.sync(operation, path).await,
            Self::Memory(provider) => provider.sync(operation, path).await,
        }
    }

    async fn rename(&self, operation: StoreOperation, source: String, destination: String) -> Result<(), StoreError> {
        match self {
            Self::Posix(provider) => provider.rename(operation, source, destination).await,
            Self::Memory(provider) => provider.rename(operation, source, destination).await,
        }
    }

    async fn list(&self, operation: StoreOperation, prefix: String) -> Result<Vec<String>, StoreError> {
        match self {
            Self::Posix(provider) => provider.list(operation, prefix).await,
            Self::Memory(provider) => provider.list(operation, prefix).await,
        }
    }

    async fn delete_prefix(&self, operation: StoreOperation, prefix: String) -> Result<(), StoreError> {
        match self {
            Self::Posix(provider) => provider.delete_prefix(operation, prefix).await,
            Self::Memory(provider) => provider.delete_prefix(operation, prefix).await,
        }
    }

    async fn atomic_write(&self, operation: StoreOperation, path: String, data: Bytes) -> Result<(), StoreError> {
        match self {
            Self::Posix(provider) => provider.atomic_write(operation, path, data).await,
            Self::Memory(provider) => provider.atomic_write(operation, path, data).await,
        }
    }
}
