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
use std::path::Component;
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
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

use crate::error;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::metadata::FileSegmentMetadata;
use crate::provider::TieredStoreProvider;

#[derive(Clone)]
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

#[derive(Clone, Copy)]
enum PathAccess {
    Read,
    Write,
}

impl PathAccess {
    fn io_error(self, operation: StoreOperation, source: std::io::Error) -> StoreError {
        let _ = self;
        error::io_failed(operation, source)
    }
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

    pub(crate) fn validate_root(root: &Path, operation: StoreOperation) -> Result<(), StoreError> {
        if !Self::root_is_valid(root) {
            return Err(error::request_invalid(operation));
        }
        Ok(())
    }

    pub(crate) fn root_is_valid(root: &Path) -> bool {
        if root.as_os_str().is_empty() {
            return false;
        }
        let has_normal_leaf = matches!(root.components().next_back(), Some(Component::Normal(_)));
        let safe_components = if root.is_absolute() {
            root.components().all(|component| {
                matches!(
                    component,
                    Component::Prefix(_) | Component::RootDir | Component::Normal(_)
                )
            })
        } else {
            root.components()
                .all(|component| matches!(component, Component::Normal(_)))
        };
        if !has_normal_leaf || !safe_components {
            return false;
        }
        true
    }

    fn resolve_lexical(&self, operation: StoreOperation, path: &str) -> Result<(PathBuf, PathBuf), StoreError> {
        Self::validate_root(&self.root, operation)?;
        let provider_path = Path::new(path);
        let mut relative = PathBuf::new();
        for component in provider_path.components() {
            match component {
                Component::Normal(component) => relative.push(component),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(error::request_invalid(operation));
                }
            }
        }
        if relative.as_os_str().is_empty() {
            return Err(error::request_invalid(operation));
        }
        let full_path = self.root.join(&relative);
        Ok((relative, full_path))
    }

    async fn resolve_existing(
        &self,
        operation: StoreOperation,
        path: &str,
        access: PathAccess,
    ) -> Result<PathBuf, StoreError> {
        let (relative, full_path) = self.resolve_lexical(operation, path)?;
        let Some(canonical_root) = self.canonical_root(operation, false, access).await? else {
            return Ok(full_path);
        };

        let mut current = self.root.clone();
        let mut components = relative.components().peekable();
        while let Some(component) = components.next() {
            current.push(component.as_os_str());
            let metadata = match tokio::fs::symlink_metadata(&current).await {
                Ok(metadata) => metadata,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => break,
                Err(source) => return Err(access.io_error(operation, source)),
            };
            self.validate_existing_component(
                &current,
                &metadata,
                components.peek().is_some(),
                &canonical_root,
                access,
                operation,
            )
            .await?;
        }
        Ok(full_path)
    }

    async fn resolve_for_write(&self, operation: StoreOperation, path: &str) -> Result<PathBuf, StoreError> {
        let (relative, full_path) = self.resolve_lexical(operation, path)?;
        let canonical_root = self
            .canonical_root(operation, true, PathAccess::Write)
            .await?
            .ok_or_else(|| error::internal_failure(operation))?;

        let mut current = self.root.clone();
        if let Some(parent) = relative.parent() {
            for component in parent.components() {
                current.push(component.as_os_str());
                let metadata = match tokio::fs::symlink_metadata(&current).await {
                    Ok(metadata) => metadata,
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                        match tokio::fs::create_dir(&current).await {
                            Ok(()) => {}
                            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {}
                            Err(source) => return Err(PathAccess::Write.io_error(operation, source)),
                        }
                        tokio::fs::symlink_metadata(&current)
                            .await
                            .map_err(|source| PathAccess::Write.io_error(operation, source))?
                    }
                    Err(source) => return Err(PathAccess::Write.io_error(operation, source)),
                };
                self.validate_existing_component(
                    &current,
                    &metadata,
                    true,
                    &canonical_root,
                    PathAccess::Write,
                    operation,
                )
                .await?;
            }
        }

        match tokio::fs::symlink_metadata(&full_path).await {
            Ok(metadata) => {
                self.validate_existing_component(
                    &full_path,
                    &metadata,
                    false,
                    &canonical_root,
                    PathAccess::Write,
                    operation,
                )
                .await?;
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(PathAccess::Write.io_error(operation, source)),
        }
        Ok(full_path)
    }

    async fn canonical_root(
        &self,
        operation: StoreOperation,
        create: bool,
        access: PathAccess,
    ) -> Result<Option<PathBuf>, StoreError> {
        Self::validate_root(&self.root, operation)?;
        let metadata = match tokio::fs::symlink_metadata(&self.root).await {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound && !create => return Ok(None),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::create_dir_all(&self.root)
                    .await
                    .map_err(|source| access.io_error(operation, source))?;
                tokio::fs::symlink_metadata(&self.root)
                    .await
                    .map_err(|source| access.io_error(operation, source))?
            }
            Err(source) => return Err(access.io_error(operation, source)),
        };
        validate_path_metadata(&self.root, &metadata, true, operation)?;
        let canonical_root = tokio::fs::canonicalize(&self.root)
            .await
            .map_err(|source| access.io_error(operation, source))?;
        let confirmed = tokio::fs::symlink_metadata(&self.root)
            .await
            .map_err(|source| access.io_error(operation, source))?;
        validate_path_metadata(&self.root, &confirmed, true, operation)?;
        Ok(Some(canonical_root))
    }

    async fn validate_existing_component(
        &self,
        path: &Path,
        metadata: &std::fs::Metadata,
        must_be_directory: bool,
        canonical_root: &Path,
        access: PathAccess,
        operation: StoreOperation,
    ) -> Result<(), StoreError> {
        validate_path_metadata(path, metadata, must_be_directory, operation)?;
        let canonical = tokio::fs::canonicalize(path)
            .await
            .map_err(|source| access.io_error(operation, source))?;
        if !canonical.starts_with(canonical_root) {
            return Err(error::request_invalid(operation));
        }
        let confirmed = tokio::fs::symlink_metadata(path)
            .await
            .map_err(|source| access.io_error(operation, source))?;
        validate_path_metadata(path, &confirmed, must_be_directory, operation)
    }

    async fn validate_directory_tree(
        &self,
        root: &Path,
        canonical_root: &Path,
        access: PathAccess,
        operation: StoreOperation,
    ) -> Result<(), StoreError> {
        let mut directories = vec![root.to_path_buf()];
        while let Some(directory) = directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory)
                .await
                .map_err(|source| access.io_error(operation, source))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|source| access.io_error(operation, source))?
            {
                let path = entry.path();
                let metadata = tokio::fs::symlink_metadata(&path)
                    .await
                    .map_err(|source| access.io_error(operation, source))?;
                self.validate_existing_component(&path, &metadata, false, canonical_root, access, operation)
                    .await?;
                if metadata.is_dir() {
                    directories.push(path);
                }
            }
        }
        Ok(())
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
        operation: StoreOperation,
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, StoreError>
    where
        Self: Sized,
    {
        self.resolve_lexical(operation, &path)?;
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

    async fn segment_size(&self, operation: StoreOperation, path: String) -> Result<u64, StoreError> {
        let full_path = self.resolve_existing(operation, &path, PathAccess::Read).await?;
        match tokio::fs::metadata(full_path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(source) => Err(error::io_failed(operation, source)),
        }
    }

    async fn read(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        length: usize,
    ) -> Result<Bytes, StoreError> {
        let full_path = self.resolve_existing(operation, &path, PathAccess::Read).await?;
        self.io_counters.read_operations.fetch_add(1, Ordering::Relaxed);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&full_path)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.seek(SeekFrom::Start(position))
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        let mut buffer = vec![0_u8; length];
        let mut read = 0;
        while read < length {
            let chunk = file
                .read(&mut buffer[read..])
                .await
                .map_err(|source| error::io_failed(operation, source))?;
            if chunk == 0 {
                break;
            }
            read += chunk;
        }
        buffer.truncate(read);
        self.io_counters.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        Ok(Bytes::from(buffer))
    }

    async fn write(
        &self,
        operation: StoreOperation,
        path: String,
        position: u64,
        data: Bytes,
    ) -> Result<usize, StoreError> {
        let full_path = self.resolve_for_write(operation, &path).await?;
        self.io_counters.write_operations.fetch_add(1, Ordering::Relaxed);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&full_path)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.seek(SeekFrom::Start(position))
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.write_all(&data)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.flush()
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        self.io_counters
            .bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data.len())
    }

    async fn delete(&self, operation: StoreOperation, path: String) -> Result<(), StoreError> {
        let full_path = self.resolve_existing(operation, &path, PathAccess::Write).await?;
        match tokio::fs::remove_file(&full_path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(error::io_failed(operation, source)),
        }
    }

    async fn sync(&self, operation: StoreOperation, path: String) -> Result<(), StoreError> {
        let full_path = self.resolve_existing(operation, &path, PathAccess::Write).await?;
        let metadata = match tokio::fs::metadata(&full_path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => return Err(error::io_failed(operation, source)),
        };
        if metadata.is_dir() {
            return sync_directory(operation, &full_path).await;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&full_path)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.sync_all()
            .await
            .map_err(|source| error::io_failed(operation, source))
    }

    async fn rename(&self, operation: StoreOperation, source: String, destination: String) -> Result<(), StoreError> {
        let source_path = self.resolve_existing(operation, &source, PathAccess::Write).await?;
        let destination_path = self.resolve_for_write(operation, &destination).await?;
        rename_path(&source_path, &destination_path)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        if let Some(parent) = destination_path.parent() {
            sync_directory(operation, parent).await?;
        }
        Ok(())
    }

    async fn list(&self, operation: StoreOperation, prefix: String) -> Result<Vec<String>, StoreError> {
        let root = self.resolve_existing(operation, &prefix, PathAccess::Read).await?;
        let metadata = match tokio::fs::symlink_metadata(&root).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(error::io_failed(operation, source)),
        };
        let canonical_root = self
            .canonical_root(operation, false, PathAccess::Read)
            .await?
            .ok_or_else(|| error::internal_failure(operation))?;
        self.validate_existing_component(&root, &metadata, false, &canonical_root, PathAccess::Read, operation)
            .await?;
        if metadata.is_file() {
            return Ok(vec![prefix]);
        }

        let mut directories = vec![root];
        let mut paths = Vec::new();
        while let Some(directory) = directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory)
                .await
                .map_err(|source| error::io_failed(operation, source))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|source| error::io_failed(operation, source))?
            {
                let path = entry.path();
                let metadata = tokio::fs::symlink_metadata(&path)
                    .await
                    .map_err(|source| error::io_failed(operation, source))?;
                self.validate_existing_component(&path, &metadata, false, &canonical_root, PathAccess::Read, operation)
                    .await?;
                if metadata.is_dir() {
                    directories.push(path);
                } else if metadata.is_file() {
                    paths.push(relative_provider_path(operation, &self.root, &path)?);
                }
            }
        }
        paths.sort();
        Ok(paths)
    }

    async fn delete_prefix(&self, operation: StoreOperation, prefix: String) -> Result<(), StoreError> {
        let full_path = self.resolve_existing(operation, &prefix, PathAccess::Write).await?;
        let metadata = match tokio::fs::symlink_metadata(&full_path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => return Err(error::io_failed(operation, source)),
        };
        let canonical_root = self
            .canonical_root(operation, false, PathAccess::Write)
            .await?
            .ok_or_else(|| error::internal_failure(operation))?;
        self.validate_existing_component(
            &full_path,
            &metadata,
            false,
            &canonical_root,
            PathAccess::Write,
            operation,
        )
        .await?;
        let result = if metadata.is_dir() {
            self.validate_directory_tree(&full_path, &canonical_root, PathAccess::Write, operation)
                .await?;
            tokio::fs::remove_dir_all(&full_path).await
        } else {
            tokio::fs::remove_file(&full_path).await
        };
        result.map_err(|source| error::io_failed(operation, source))
    }

    async fn atomic_write(&self, operation: StoreOperation, path: String, data: Bytes) -> Result<(), StoreError> {
        let destination = self.resolve_for_write(operation, &path).await?;
        let temporary_key = Path::new(&path).with_extension("atomic.tmp");
        let temporary = self
            .resolve_for_write(operation, &path_to_string(&temporary_key))
            .await?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.write_all(&data)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        file.sync_all()
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        drop(file);

        replace_file(&temporary, &destination)
            .await
            .map_err(|source| error::io_failed(operation, source))?;
        if let Some(parent) = destination.parent() {
            sync_directory(operation, parent).await?;
        }
        Ok(())
    }
}

fn validate_path_metadata(
    _path: &Path,
    metadata: &std::fs::Metadata,
    must_be_directory: bool,
    operation: StoreOperation,
) -> Result<(), StoreError> {
    if is_symlink_or_reparse(metadata) {
        return Err(error::request_invalid(operation));
    }
    if must_be_directory && !metadata.is_dir() {
        return Err(error::request_invalid(operation));
    }
    Ok(())
}

#[cfg(not(windows))]
fn is_symlink_or_reparse(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn is_symlink_or_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    metadata.file_type().is_symlink()
        || metadata.file_attributes() & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT != 0
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn relative_provider_path(operation: StoreOperation, root: &Path, path: &Path) -> Result<String, StoreError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|source| error::state_corrupted_source(operation, source))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

#[cfg(unix)]
async fn sync_directory(operation: StoreOperation, path: &Path) -> Result<(), StoreError> {
    let directory = OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .map_err(|source| error::io_failed(operation, source))?;
    directory
        .sync_all()
        .await
        .map_err(|source| error::io_failed(operation, source))
}

impl std::fmt::Debug for PosixProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PosixProvider { root_present: true, redacted: true }")
    }
}

#[cfg(windows)]
async fn sync_directory(_operation: StoreOperation, _path: &Path) -> Result<(), StoreError> {
    // MoveFileExW/ReplaceFileW with WRITE_THROUGH provide the Windows metadata durability boundary.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
async fn sync_directory(_operation: StoreOperation, _path: &Path) -> Result<(), StoreError> {
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
    use std::path::PathBuf;

    use bytes::Bytes;
    use rocketmq_store_api::StoreError;
    use rocketmq_store_api::StoreOperation;

    use super::PosixProvider;
    use crate::file::FileSegment;
    use crate::file::FileSegmentType;
    use crate::file::TieredFileSegment;
    use crate::metadata::FileSegmentMetadata;
    use crate::provider::TieredStoreProvider;

    #[test]
    fn debug_redacts_provider_root() {
        let provider = PosixProvider::new(PathBuf::from("sensitive-posix-root-canary"));

        let debug = format!("{provider:?}");

        assert_eq!(debug, "PosixProvider { root_present: true, redacted: true }");
        assert!(!debug.contains("sensitive-posix-root-canary"));
    }

    #[tokio::test]
    async fn write_read_size_and_delete() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider = PosixProvider::new(temp_dir.path().to_path_buf());

        provider
            .write(
                StoreOperation::AppendDerived,
                "topic/0/commitlog/000".to_owned(),
                0,
                Bytes::from_static(b"abc"),
            )
            .await?;
        provider
            .write(
                StoreOperation::AppendDerived,
                "topic/0/commitlog/000".to_owned(),
                3,
                Bytes::from_static(b"def"),
            )
            .await?;

        assert_eq!(
            provider
                .segment_size(StoreOperation::Read, "topic/0/commitlog/000".to_owned())
                .await?,
            6
        );
        assert_eq!(
            provider
                .read(StoreOperation::Read, "topic/0/commitlog/000".to_owned(), 1, 4)
                .await?,
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

        provider
            .delete(StoreOperation::AppendDerived, "topic/0/commitlog/000".to_owned())
            .await?;
        assert_eq!(
            provider
                .segment_size(StoreOperation::Read, "topic/0/commitlog/000".to_owned())
                .await?,
            0
        );
        assert!(provider
            .read(StoreOperation::Read, "topic/0/commitlog/000".to_owned(), 0, 1)
            .await
            .is_err());
        let after_failed_read = provider.io_snapshot();
        assert_eq!(after_failed_read.read_operations, 2);
        assert_eq!(after_failed_read.bytes_read, 4);
        Ok(())
    }

    #[tokio::test]
    async fn create_append_commit_and_recover_segment() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider = PosixProvider::new(temp_dir.path().to_path_buf());
        let path = "topic/0/commitlog/00000000000000000000".to_owned();
        let segment = provider
            .create_segment(
                StoreOperation::AppendDerived,
                path.clone(),
                FileSegmentType::CommitLog,
                0,
                64,
            )
            .await?;

        segment.append(Bytes::from_static(b"hello"), 100).await?;
        segment.append(Bytes::from_static(b"-posix"), 101).await?;
        assert_eq!(segment.append_position(), 11);
        assert_eq!(segment.commit_position(), 0);

        segment.commit().await?;
        assert_eq!(segment.commit_position(), 11);
        assert_eq!(segment.read(0..11).await?, Bytes::from_static(b"hello-posix"));

        let mut metadata = FileSegmentMetadata::new(path.clone(), FileSegmentType::CommitLog, 0);
        metadata.size = provider.segment_size(StoreOperation::Read, path.clone()).await?;
        metadata.begin_timestamp = 100;
        metadata.end_timestamp = 101;
        let recovered = TieredFileSegment::new(path, FileSegmentType::CommitLog, 0, 64, metadata, provider);

        assert_eq!(recovered.commit_position(), 11);
        assert_eq!(recovered.read(6..11).await?, Bytes::from_static(b"posix"));
        Ok(())
    }

    #[tokio::test]
    async fn unsafe_provider_paths_are_rejected_before_io() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider_root = temp_dir.path().join("provider");
        let outside_root = temp_dir.path().join("outside");
        std::fs::create_dir_all(&provider_root).map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        std::fs::create_dir_all(&outside_root).map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let sentinel = outside_root.join("sentinel");
        std::fs::write(&sentinel, b"outside").map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider = PosixProvider::new(provider_root);

        assert_unsafe_paths_rejected(
            &provider,
            PathBuf::from("..").join("outside").join("sentinel"),
            PathBuf::from("..").join("outside"),
        )
        .await;
        assert_unsafe_paths_rejected(&provider, sentinel.clone(), outside_root).await;

        assert_eq!(
            std::fs::read(&sentinel).map_err(|source| crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source
            ))?,
            b"outside"
        );
        assert_eq!(provider.io_snapshot(), super::PosixProviderIoSnapshot::default());
        Ok(())
    }

    #[tokio::test]
    async fn empty_provider_root_is_rejected_before_io() {
        let provider = PosixProvider::new(PathBuf::new());

        let error = provider
            .write(
                StoreOperation::AppendDerived,
                "topic/0/commitlog/000".to_owned(),
                0,
                Bytes::from_static(b"unsafe"),
            )
            .await
            .expect_err("an empty provider root must be rejected");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
        assert_eq!(provider.io_snapshot(), super::PosixProviderIoSnapshot::default());
    }

    #[test]
    fn dangerous_provider_roots_are_rejected() {
        let temp_dir = tempfile::tempdir().expect("create temporary directory");
        let filesystem_root = temp_dir
            .path()
            .ancestors()
            .last()
            .expect("temporary path has a filesystem root")
            .to_path_buf();
        let roots = [
            PathBuf::from("."),
            PathBuf::from(".."),
            filesystem_root,
            temp_dir.path().join("..").join("unstable-root"),
        ];

        for root in roots {
            let error = PosixProvider::validate_root(&root, StoreOperation::Load)
                .expect_err("dangerous roots must fail closed");
            assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID, "{root:?}");
        }

        #[cfg(windows)]
        {
            let root = PathBuf::from(r"C:drive-relative");
            let error = PosixProvider::validate_root(&root, StoreOperation::Load)
                .expect_err("drive-relative roots must fail closed");
            assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID, "{root:?}");
        }
    }

    #[test]
    fn normalized_relative_provider_roots_are_accepted() {
        for root in [PathBuf::from("provider"), PathBuf::from("relative/provider")] {
            PosixProvider::validate_root(&root, StoreOperation::Load)
                .expect("normalized relative roots remain compatible");
        }
    }

    #[tokio::test]
    async fn empty_provider_key_cannot_delete_the_configured_root() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider_root = temp_dir.path().join("provider");
        std::fs::create_dir_all(&provider_root).map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let sentinel = provider_root.join("sentinel");
        std::fs::write(&sentinel, b"inside-root").map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider = PosixProvider::new(provider_root);

        let error = provider
            .delete_prefix(StoreOperation::AppendDerived, String::new())
            .await
            .expect_err("an empty provider key must not delete the configured root");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
        assert_eq!(
            std::fs::read(&sentinel).map_err(|source| crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source
            ))?,
            b"inside-root"
        );
        assert_eq!(provider.io_snapshot(), super::PosixProviderIoSnapshot::default());
        Ok(())
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn symlink_escape_is_rejected_before_provider_io() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let provider_root = temp_dir.path().join("provider");
        let outside_root = temp_dir.path().join("outside");
        std::fs::create_dir_all(&provider_root).map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        std::fs::create_dir_all(&outside_root).map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let sentinel = outside_root.join("sentinel");
        std::fs::write(&sentinel, b"outside").map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let escape = provider_root.join("escape");
        if let Err(source) = create_directory_symlink(&outside_root, &escape) {
            if symlink_creation_is_unavailable(&source) {
                return Ok(());
            }
            return Err(crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            ));
        }
        let provider = PosixProvider::new(provider_root);

        assert_unsafe_paths_rejected(
            &provider,
            PathBuf::from("escape").join("sentinel"),
            PathBuf::from("escape"),
        )
        .await;

        assert_eq!(
            std::fs::read(&sentinel).map_err(|source| crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source
            ))?,
            b"outside"
        );
        assert_eq!(provider.io_snapshot(), super::PosixProviderIoSnapshot::default());
        Ok(())
    }

    async fn assert_unsafe_paths_rejected(provider: &PosixProvider, file_path: PathBuf, prefix_path: PathBuf) {
        let file_path = file_path.to_string_lossy().into_owned();
        let prefix_path = prefix_path.to_string_lossy().into_owned();

        let read_error = provider
            .read(StoreOperation::Read, file_path.clone(), 0, 1)
            .await
            .expect_err("unsafe reads must be rejected");
        assert_eq!(read_error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);

        let write_error = provider
            .write(
                StoreOperation::AppendDerived,
                file_path.clone(),
                0,
                Bytes::from_static(b"unsafe"),
            )
            .await
            .expect_err("unsafe writes must be rejected");
        assert_eq!(write_error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);

        let list_error = provider
            .list(StoreOperation::Read, prefix_path.clone())
            .await
            .expect_err("unsafe prefix listings must be rejected");
        assert_eq!(list_error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);

        let delete_error = provider
            .delete(StoreOperation::AppendDerived, file_path)
            .await
            .expect_err("unsafe deletes must be rejected");
        assert_eq!(delete_error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);

        let delete_prefix_error = provider
            .delete_prefix(StoreOperation::AppendDerived, prefix_path)
            .await
            .expect_err("unsafe recursive deletes must be rejected");
        assert_eq!(
            delete_prefix_error.descriptor(),
            &rocketmq_error::STORAGE_REQUEST_INVALID
        );
    }

    #[cfg(unix)]
    fn create_directory_symlink(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_symlink(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(unix)]
    const fn symlink_creation_is_unavailable(_error: &std::io::Error) -> bool {
        false
    }

    #[cfg(windows)]
    fn symlink_creation_is_unavailable(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
        ) || error.raw_os_error() == Some(1314)
    }
}
