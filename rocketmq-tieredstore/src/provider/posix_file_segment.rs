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

use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

use crate::error;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::metadata::FileSegmentMetadata;
use crate::provider::TieredStoreProvider;

#[derive(Debug, Clone)]
pub struct PosixProvider {
    root: PathBuf,
}

impl PosixProvider {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn resolve(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

impl TieredStoreProvider for PosixProvider {
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
        match tokio::fs::metadata(self.resolve(&path)).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(err) => Err(error::storage_read_failed(path, err.to_string())),
        }
    }

    async fn read(&self, path: String, position: u64, length: usize) -> Result<Bytes, RocketMQError> {
        let full_path = self.resolve(&path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&full_path)
            .await
            .map_err(|err| error::storage_read_failed(path_to_string(&full_path), err.to_string()))?;
        file.seek(SeekFrom::Start(position))
            .await
            .map_err(|err| error::storage_read_failed(path_to_string(&full_path), err.to_string()))?;
        let mut buffer = vec![0_u8; length];
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|err| error::storage_read_failed(path_to_string(&full_path), err.to_string()))?;
        buffer.truncate(read);
        Ok(Bytes::from(buffer))
    }

    async fn write(&self, path: String, position: u64, data: Bytes) -> Result<usize, RocketMQError> {
        let full_path = self.resolve(&path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| error::storage_write_failed(path_to_string(parent), err.to_string()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&full_path)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))?;
        file.seek(SeekFrom::Start(position))
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))?;
        file.write_all(&data)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))?;
        file.flush()
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))?;
        Ok(data.len())
    }

    async fn delete(&self, path: String) -> Result<(), RocketMQError> {
        let full_path = self.resolve(&path);
        match tokio::fs::remove_file(&full_path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(error::storage_write_failed(path_to_string(&full_path), err.to_string())),
        }
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_error::RocketMQError;

    use super::PosixProvider;
    use crate::file::FileSegment;
    use crate::file::FileSegmentType;
    use crate::file::TieredFileSegment;
    use crate::metadata::FileSegmentMetadata;
    use crate::provider::TieredStoreProvider;

    #[tokio::test]
    async fn write_read_size_and_delete() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let provider = PosixProvider::new(temp_dir.path().to_path_buf());

        provider
            .write("topic/0/commitlog/000".to_owned(), 0, Bytes::from_static(b"abc"))
            .await?;
        provider
            .write("topic/0/commitlog/000".to_owned(), 3, Bytes::from_static(b"def"))
            .await?;

        assert_eq!(provider.segment_size("topic/0/commitlog/000".to_owned()).await?, 6);
        assert_eq!(
            provider.read("topic/0/commitlog/000".to_owned(), 1, 4).await?,
            Bytes::from_static(b"bcde")
        );

        provider.delete("topic/0/commitlog/000".to_owned()).await?;
        assert_eq!(provider.segment_size("topic/0/commitlog/000".to_owned()).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn create_append_commit_and_recover_segment() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let provider = PosixProvider::new(temp_dir.path().to_path_buf());
        let path = "topic/0/commitlog/00000000000000000000".to_owned();
        let segment = provider
            .create_segment(path.clone(), FileSegmentType::CommitLog, 0, 64)
            .await?;

        segment.append(Bytes::from_static(b"hello"), 100).await?;
        segment.append(Bytes::from_static(b"-posix"), 101).await?;
        assert_eq!(segment.append_position(), 11);
        assert_eq!(segment.commit_position(), 0);

        segment.commit().await?;
        assert_eq!(segment.commit_position(), 11);
        assert_eq!(segment.read(0..11).await?, Bytes::from_static(b"hello-posix"));

        let mut metadata = FileSegmentMetadata::new(path.clone(), FileSegmentType::CommitLog, 0);
        metadata.size = provider.segment_size(path.clone()).await?;
        metadata.begin_timestamp = 100;
        metadata.end_timestamp = 101;
        let recovered = TieredFileSegment::new(path, FileSegmentType::CommitLog, 0, 64, metadata, provider);

        assert_eq!(recovered.commit_position(), 11);
        assert_eq!(recovered.read(6..11).await?, Bytes::from_static(b"posix"));
        Ok(())
    }
}
