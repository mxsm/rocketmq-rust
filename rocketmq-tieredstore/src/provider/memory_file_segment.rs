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

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use parking_lot::RwLock;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::metadata::FileSegmentMetadata;
use crate::provider::TieredStoreProvider;

#[derive(Clone, Default)]
pub struct MemoryProvider {
    files: Arc<RwLock<HashMap<String, BytesMut>>>,
}

impl TieredStoreProvider for MemoryProvider {
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
        let metadata = FileSegmentMetadata::new(path.clone(), segment_type, base_offset);
        Ok(TieredFileSegment::new(
            path,
            segment_type,
            base_offset,
            max_size,
            metadata,
            self.clone(),
        ))
    }

    async fn segment_size(&self, _operation: StoreOperation, path: String) -> Result<u64, StoreError> {
        let files = self.files.read();
        Ok(files.get(&path).map(|bytes| bytes.len() as u64).unwrap_or(0))
    }

    async fn read(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        length: usize,
    ) -> Result<Bytes, StoreError> {
        let files = self.files.read();
        let Some(bytes) = files.get(&path) else {
            return Ok(Bytes::new());
        };
        let start = usize::try_from(position).map_err(|_| crate::error::request_invalid(operation))?;
        if start >= bytes.len() {
            return Ok(Bytes::new());
        }
        let end = start.saturating_add(length).min(bytes.len());
        Ok(Bytes::copy_from_slice(&bytes[start..end]))
    }

    async fn write(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        data: Bytes,
    ) -> Result<usize, StoreError> {
        let mut files = self.files.write();
        let bytes = files.entry(path).or_default();
        let start = usize::try_from(position).map_err(|_| crate::error::request_invalid(operation))?;
        if bytes.len() < start {
            bytes.resize(start, 0);
        }
        let end = start
            .checked_add(data.len())
            .ok_or_else(|| crate::error::request_invalid(operation))?;
        if bytes.len() < end {
            bytes.resize(end, 0);
        }
        bytes[start..end].copy_from_slice(&data);
        Ok(data.len())
    }

    async fn delete(&self, _operation: StoreOperation, path: String) -> Result<(), StoreError> {
        self.files.write().remove(&path);
        Ok(())
    }

    async fn rename(
        &self,
        operation: StoreOperation,
        source_root: String,
        destination: String,
    ) -> Result<(), StoreError> {
        let mut files = self.files.write();
        let source_prefix = format!("{source_root}/");
        let mut replacements = files
            .keys()
            .filter(|path| **path == source_root || path.starts_with(&source_prefix))
            .cloned()
            .collect::<Vec<_>>();
        replacements.sort();
        if replacements.is_empty() {
            return Err(crate::error::contract_error(
                &rocketmq_error::STORAGE_WRITE_FAILED,
                operation,
            ));
        }

        let destination_prefix = format!("{destination}/");
        files.retain(|path, _| *path != destination && !path.starts_with(&destination_prefix));
        for source_path in replacements {
            let Some(bytes) = files.remove(&source_path) else {
                continue;
            };
            let suffix = source_path.strip_prefix(&source_root).unwrap_or_default();
            files.insert(format!("{destination}{suffix}"), bytes);
        }
        Ok(())
    }

    async fn list(&self, _operation: StoreOperation, prefix: String) -> Result<Vec<String>, StoreError> {
        let prefix_with_separator = format!("{prefix}/");
        let mut paths = self
            .files
            .read()
            .keys()
            .filter(|path| **path == prefix || path.starts_with(&prefix_with_separator))
            .cloned()
            .collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
    }

    async fn delete_prefix(&self, _operation: StoreOperation, prefix: String) -> Result<(), StoreError> {
        let prefix_with_separator = format!("{prefix}/");
        self.files
            .write()
            .retain(|path, _| *path != prefix && !path.starts_with(&prefix_with_separator));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_store_api::StoreError;
    use rocketmq_store_api::StoreOperation;

    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;

    #[tokio::test]
    async fn write_read_size_and_delete() -> Result<(), StoreError> {
        let provider = MemoryProvider::default();
        provider
            .write(
                StoreOperation::Append,
                "segment".to_owned(),
                0,
                Bytes::from_static(b"abc"),
            )
            .await?;
        provider
            .write(
                StoreOperation::Append,
                "segment".to_owned(),
                3,
                Bytes::from_static(b"def"),
            )
            .await?;

        assert_eq!(
            provider
                .segment_size(StoreOperation::Read, "segment".to_owned())
                .await?,
            6
        );
        assert_eq!(
            provider.read(StoreOperation::Read, "segment".to_owned(), 1, 4).await?,
            Bytes::from_static(b"bcde")
        );

        provider
            .delete(StoreOperation::AppendDerived, "segment".to_owned())
            .await?;
        assert_eq!(
            provider
                .segment_size(StoreOperation::Read, "segment".to_owned())
                .await?,
            0
        );
        Ok(())
    }
}
