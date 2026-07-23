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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(windows)]
use std::ffi::OsStr;
#[cfg(windows)]
use std::iter;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;

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
    io_counters: Arc<PosixProviderIoCounters>,
}

#[derive(Debug, Default)]
struct PosixProviderIoCounters {
    read_operations: AtomicU64,
    write_operations: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

/// Read-only cumulative POSIX provider I/O counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PosixProviderIoSnapshot {
    /// Provider read calls attempted by this provider and its clones.
    pub read_operations: u64,
    /// Provider write calls attempted by this provider and its clones.
    pub write_operations: u64,
    /// Bytes returned by successful provider reads.
    pub bytes_read: u64,
    /// Bytes accepted by successful provider writes.
    pub bytes_written: u64,
}

impl PosixProvider {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            io_counters: Arc::new(PosixProviderIoCounters::default()),
        }
    }

    fn resolve(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }

    /// Returns a clone-shared cumulative snapshot suitable for measured-window deltas.
    pub fn io_snapshot(&self) -> PosixProviderIoSnapshot {
        PosixProviderIoSnapshot {
            read_operations: self.io_counters.read_operations.load(Ordering::Relaxed),
            write_operations: self.io_counters.write_operations.load(Ordering::Relaxed),
            bytes_read: self.io_counters.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.io_counters.bytes_written.load(Ordering::Relaxed),
        }
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
        self.io_counters.read_operations.fetch_add(1, Ordering::Relaxed);
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
        let mut read = 0;
        while read < length {
            let chunk = file
                .read(&mut buffer[read..])
                .await
                .map_err(|err| error::storage_read_failed(path_to_string(&full_path), err.to_string()))?;
            if chunk == 0 {
                break;
            }
            read += chunk;
        }
        buffer.truncate(read);
        self.io_counters.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        Ok(Bytes::from(buffer))
    }

    async fn write(&self, path: String, position: u64, data: Bytes) -> Result<usize, RocketMQError> {
        self.io_counters.write_operations.fetch_add(1, Ordering::Relaxed);
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
        self.io_counters
            .bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
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

    async fn sync(&self, path: String) -> Result<(), RocketMQError> {
        let full_path = self.resolve(&path);
        let metadata = match tokio::fs::metadata(&full_path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(error::storage_write_failed(path_to_string(&full_path), err.to_string())),
        };
        if metadata.is_dir() {
            return sync_directory(&full_path).await;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&full_path)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))?;
        file.sync_all()
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))
    }

    async fn rename(&self, source: String, destination: String) -> Result<(), RocketMQError> {
        let source_path = self.resolve(&source);
        let destination_path = self.resolve(&destination);
        if let Some(parent) = destination_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| error::storage_write_failed(path_to_string(parent), err.to_string()))?;
        }
        rename_path(&source_path, &destination_path)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&destination_path), err.to_string()))?;
        if let Some(parent) = destination_path.parent() {
            sync_directory(parent).await?;
        }
        Ok(())
    }

    async fn list(&self, prefix: String) -> Result<Vec<String>, RocketMQError> {
        let root = self.resolve(&prefix);
        let metadata = match tokio::fs::metadata(&root).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(error::storage_read_failed(path_to_string(&root), err.to_string())),
        };
        if metadata.is_file() {
            return Ok(vec![prefix]);
        }

        let mut directories = vec![root];
        let mut paths = Vec::new();
        while let Some(directory) = directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory)
                .await
                .map_err(|err| error::storage_read_failed(path_to_string(&directory), err.to_string()))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|err| error::storage_read_failed(path_to_string(&directory), err.to_string()))?
            {
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(|err| error::storage_read_failed(path_to_string(&path), err.to_string()))?;
                if file_type.is_dir() {
                    directories.push(path);
                } else if file_type.is_file() {
                    paths.push(relative_provider_path(&self.root, &path)?);
                }
            }
        }
        paths.sort();
        Ok(paths)
    }

    async fn delete_prefix(&self, prefix: String) -> Result<(), RocketMQError> {
        let full_path = self.resolve(&prefix);
        let metadata = match tokio::fs::metadata(&full_path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(error::storage_write_failed(path_to_string(&full_path), err.to_string())),
        };
        let result = if metadata.is_dir() {
            tokio::fs::remove_dir_all(&full_path).await
        } else {
            tokio::fs::remove_file(&full_path).await
        };
        result.map_err(|err| error::storage_write_failed(path_to_string(&full_path), err.to_string()))
    }

    async fn atomic_write(&self, path: String, data: Bytes) -> Result<(), RocketMQError> {
        let destination = self.resolve(&path);
        let temporary = destination.with_extension("atomic.tmp");
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| error::storage_write_failed(path_to_string(parent), err.to_string()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&temporary), err.to_string()))?;
        file.write_all(&data)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&temporary), err.to_string()))?;
        file.sync_all()
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&temporary), err.to_string()))?;
        drop(file);

        replace_file(&temporary, &destination)
            .await
            .map_err(|err| error::storage_write_failed(path_to_string(&destination), err.to_string()))?;
        if let Some(parent) = destination.parent() {
            sync_directory(parent).await?;
        }
        Ok(())
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn relative_provider_path(root: &Path, path: &Path) -> Result<String, RocketMQError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|err| error::storage_read_failed(path_to_string(path), err.to_string()))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

#[cfg(unix)]
async fn sync_directory(path: &Path) -> Result<(), RocketMQError> {
    let directory = OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .map_err(|err| error::storage_write_failed(path_to_string(path), err.to_string()))?;
    directory
        .sync_all()
        .await
        .map_err(|err| error::storage_write_failed(path_to_string(path), err.to_string()))
}

#[cfg(windows)]
async fn sync_directory(_path: &Path) -> Result<(), RocketMQError> {
    // MoveFileExW/ReplaceFileW with WRITE_THROUGH provide the Windows metadata durability boundary.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
async fn sync_directory(_path: &Path) -> Result<(), RocketMQError> {
    Ok(())
}

#[cfg(not(windows))]
async fn rename_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    tokio::fs::rename(source, destination).await
}

#[cfg(windows)]
async fn rename_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    let destination = wide_path(destination);
    let source = wide_path(source);
    // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the duration of MoveFileExW.
    let renamed = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
        )
    };
    if renamed == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
async fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    tokio::fs::rename(source, destination).await
}

#[cfg(windows)]
async fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    if !tokio::fs::try_exists(destination).await? {
        return tokio::fs::rename(source, destination).await;
    }
    let destination = wide_path(destination);
    let source = wide_path(source);
    // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the duration of the call;
    // backup/exclude/reserved pointers are intentionally null as allowed by ReplaceFileW.
    let replaced = unsafe {
        windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            std::ptr::null(),
            windows_sys::Win32::Storage::FileSystem::REPLACEFILE_WRITE_THROUGH,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn wide_path(path: &Path) -> Vec<u16> {
    OsStr::new(path).encode_wide().chain(iter::once(0)).collect()
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
        assert_eq!(
            provider.io_snapshot(),
            super::PosixProviderIoSnapshot {
                read_operations: 1,
                write_operations: 2,
                bytes_read: 4,
                bytes_written: 6,
            }
        );
        assert_eq!(provider.clone().io_snapshot(), provider.io_snapshot());

        provider.delete("topic/0/commitlog/000".to_owned()).await?;
        assert_eq!(provider.segment_size("topic/0/commitlog/000".to_owned()).await?, 0);
        assert!(provider.read("topic/0/commitlog/000".to_owned(), 0, 1).await.is_err());
        let after_failed_read = provider.io_snapshot();
        assert_eq!(after_failed_read.read_operations, 2);
        assert_eq!(after_failed_read.bytes_read, 4);
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
