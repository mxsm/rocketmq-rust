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
use rocketmq_error::RocketMQError;

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
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, RocketMQError>
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

    async fn segment_size(&self, path: String) -> Result<u64, RocketMQError> {
        let files = self.files.read();
        Ok(files.get(&path).map(|bytes| bytes.len() as u64).unwrap_or(0))
    }

    async fn read(&self, path: String, position: u64, length: usize) -> Result<Bytes, RocketMQError> {
        let files = self.files.read();
        let Some(bytes) = files.get(&path) else {
            return Ok(Bytes::new());
        };
        let start = position as usize;
        if start >= bytes.len() {
            return Ok(Bytes::new());
        }
        let end = start.saturating_add(length).min(bytes.len());
        Ok(Bytes::copy_from_slice(&bytes[start..end]))
    }

    async fn write(&self, path: String, position: u64, data: Bytes) -> Result<usize, RocketMQError> {
        let mut files = self.files.write();
        let bytes = files.entry(path).or_default();
        let start = position as usize;
        if bytes.len() < start {
            bytes.resize(start, 0);
        }
        let end = start.saturating_add(data.len());
        if bytes.len() < end {
            bytes.resize(end, 0);
        }
        bytes[start..end].copy_from_slice(&data);
        Ok(data.len())
    }

    async fn delete(&self, path: String) -> Result<(), RocketMQError> {
        self.files.write().remove(&path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_error::RocketMQError;

    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;

    #[tokio::test]
    async fn write_read_size_and_delete() -> Result<(), RocketMQError> {
        let provider = MemoryProvider::default();
        provider
            .write("segment".to_owned(), 0, Bytes::from_static(b"abc"))
            .await?;
        provider
            .write("segment".to_owned(), 3, Bytes::from_static(b"def"))
            .await?;

        assert_eq!(provider.segment_size("segment".to_owned()).await?, 6);
        assert_eq!(
            provider.read("segment".to_owned(), 1, 4).await?,
            Bytes::from_static(b"bcde")
        );

        provider.delete("segment".to_owned()).await?;
        assert_eq!(provider.segment_size("segment".to_owned()).await?, 0);
        Ok(())
    }
}
