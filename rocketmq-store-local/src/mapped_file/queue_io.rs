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

//! Local mapped-file queue discovery, loading, and creation I/O.

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use cheetah_string::CheetahString;
use tracing::error;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::mapped_file::retirement::registry::ManagedQueueMember;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::ManagedMappedFileQueueGeneration;
use crate::mapped_file::MappedFile;
use crate::mapped_file::ReconciledSegmentFile;

/// Files loaded before a queue load completed or failed.
#[doc(hidden)]
pub struct MappedFileQueueLoadOutcome {
    success: bool,
    mapped_files: Vec<Arc<DefaultMappedFile>>,
}

impl MappedFileQueueLoadOutcome {
    fn new(success: bool, mapped_files: Vec<Arc<DefaultMappedFile>>) -> Self {
        Self { success, mapped_files }
    }

    /// Reports whether every candidate file was accepted.
    #[doc(hidden)]
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Returns files loaded before the terminal outcome.
    #[doc(hidden)]
    pub fn into_mapped_files(self) -> Vec<Arc<DefaultMappedFile>> {
        self.mapped_files
    }
}

/// Discovers and loads the files in a mapped-file queue directory.
#[doc(hidden)]
pub fn load_mapped_file_queue_path(store_path: &str, mapped_file_size: u64) -> MappedFileQueueLoadOutcome {
    let entries = match fs::read_dir(Path::new(store_path)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return MappedFileQueueLoadOutcome::new(true, Vec::new());
        }
        Err(error) => {
            error!(store_path, %error, "Failed to enumerate mapped-file queue directory");
            return MappedFileQueueLoadOutcome::new(false, Vec::new());
        }
    };

    load_mapped_file_queue_entries(
        store_path,
        entries.map(|entry| entry.map(|entry| entry.path())),
        mapped_file_size,
    )
}

fn load_mapped_file_queue_entries<I>(store_path: &str, entries: I, mapped_file_size: u64) -> MappedFileQueueLoadOutcome
where
    I: IntoIterator<Item = io::Result<PathBuf>>,
{
    let mut files = Vec::new();
    for entry in entries {
        match entry {
            Ok(path) => files.push(path),
            Err(error) => {
                error!(store_path, %error, "Failed to enumerate an entry in mapped-file queue directory");
                return MappedFileQueueLoadOutcome::new(false, Vec::new());
            }
        }
    }
    load_mapped_file_queue_files(files, mapped_file_size)
}

/// Loads an explicit mapped-file queue candidate list in ascending file-name order.
#[doc(hidden)]
pub fn load_mapped_file_queue_files(files: Vec<PathBuf>, mapped_file_size: u64) -> MappedFileQueueLoadOutcome {
    load_mapped_file_queue_files_with_remover(files, mapped_file_size, |path| fs::remove_file(path))
}

/// Loads one fully reconciled queue generation from retained file handles.
///
/// This function performs no namespace discovery, pathname open, resize, preallocation, or
/// deletion. Any failed segment rejects the complete generation; callers never receive a partial
/// managed publication candidate.
#[doc(hidden)]
pub fn load_reconciled_mapped_file_queue(
    store_root: &Path,
    mut segments: Vec<ReconciledSegmentFile>,
) -> io::Result<ManagedMappedFileQueueGeneration<DefaultMappedFile>> {
    segments.sort_by_key(ReconciledSegmentFile::segment_offset);
    if segments
        .windows(2)
        .any(|pair| pair[0].segment_offset() == pair[1].segment_offset())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "duplicate durable segment offsets block managed queue loading",
        ));
    }

    let mut mapped_files = Vec::new();
    mapped_files
        .try_reserve_exact(segments.len())
        .map_err(|_| io::Error::other("failed to reserve the reconciled mapped-file generation"))?;
    for segment in segments {
        let incarnation = segment.incarnation();
        let physical_key = segment.physical_key();
        let canonical_path = segment.canonical_path().clone();
        let segment_offset = segment.segment_offset();
        let expected_length = segment.expected_length();
        let relative_path = segment.relative_path().to_owned();
        let mapped_file = DefaultMappedFile::try_new_reconciled(store_root, segment).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to construct reconciled mapped file {relative_path}: {error}"),
            )
        })?;
        let file_size = mapped_file.get_file_size();
        mapped_file.set_wrote_position(file_size as i32);
        mapped_file.set_flushed_position(file_size as i32);
        mapped_file.set_committed_position(file_size as i32);
        let mapping_generation = mapped_file.current_mapping_generation_id().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "reconciled mapped file has no published mapping generation",
            )
        })?;
        mapped_files.push(ManagedQueueMember::new(
            Arc::new(mapped_file),
            incarnation,
            physical_key,
            canonical_path,
            segment_offset,
            expected_length,
            mapping_generation,
        )?);
    }

    ManagedMappedFileQueueGeneration::from_reconciled_members(mapped_files)
}

fn load_mapped_file_queue_files_with_remover<R>(
    mut files: Vec<PathBuf>,
    mapped_file_size: u64,
    mut remove_file: R,
) -> MappedFileQueueLoadOutcome
where
    R: FnMut(&Path) -> io::Result<()>,
{
    files.sort_by(|left, right| left.file_name().cmp(&right.file_name()));

    // Validate the complete namespace before opening or deleting any segment. Unknown entries are
    // retained, but they block loading so callers cannot mistake an incomplete view for success.
    let mut candidates = Vec::with_capacity(files.len());
    for file in files {
        let metadata = match fs::symlink_metadata(&file) {
            Ok(metadata) => metadata,
            Err(error) => {
                error!(path = %file.display(), %error, "Failed to get mapped-file queue entry metadata");
                return MappedFileQueueLoadOutcome::new(false, Vec::new());
            }
        };
        if !metadata.file_type().is_file() {
            warn!(
                path = %file.display(),
                "Unknown non-file entry blocks mapped-file queue loading and was retained"
            );
            return MappedFileQueueLoadOutcome::new(false, Vec::new());
        }
        if let Err(error) = crate::mapped_file::file::try_parse_file_from_offset(&file) {
            warn!(
                path = %file.display(),
                %error,
                "Unknown file identity blocks mapped-file queue loading and was retained"
            );
            return MappedFileQueueLoadOutcome::new(false, Vec::new());
        }
        candidates.push((file, metadata));
    }

    let mut mapped_files = Vec::new();
    for (index, (file, metadata)) in candidates.iter().enumerate() {
        if metadata.len() == 0 && index == candidates.len() - 1 {
            match remove_file(file) {
                Ok(()) => warn!("{} size is 0, auto deleted.", file.display()),
                Err(error) => {
                    warn!(
                        path = %file.display(),
                        %error,
                        "Failed to delete zero-length mapped-file queue tail"
                    );
                    return MappedFileQueueLoadOutcome::new(false, mapped_files);
                }
            }
            continue;
        }

        if metadata.len() != mapped_file_size {
            warn!(
                "{} length not matched message store config value, please check it manually",
                file.display()
            );
            return MappedFileQueueLoadOutcome::new(false, mapped_files);
        }

        let mapped_file = match DefaultMappedFile::try_new(
            CheetahString::from_string(file.to_string_lossy().into_owned()),
            mapped_file_size,
        ) {
            Ok(mapped_file) => mapped_file,
            Err(error) => {
                error!("Failed to load mapped file {}: {}", file.display(), error);
                return MappedFileQueueLoadOutcome::new(false, mapped_files);
            }
        };
        mapped_file.set_wrote_position(mapped_file_size as i32);
        mapped_file.set_flushed_position(mapped_file_size as i32);
        mapped_file.set_committed_position(mapped_file_size as i32);
        mapped_files.push(Arc::new(mapped_file));
    }

    MappedFileQueueLoadOutcome::new(true, mapped_files)
}

/// Creates one queue segment through the configured allocation service.
///
/// Synchronous creation is used when no allocation service is configured, or for a never-started
/// service that has no path fence. Falling back from a live, stopped, or retiring service would
/// bypass its in-flight path ownership and can let an old request delete a newly published segment.
#[doc(hidden)]
pub fn create_mapped_file_for_queue(
    allocate_service: Option<&AllocateMappedFileService>,
    file_path: &Path,
    next_file_path: &Path,
    mapped_file_size: u64,
    first_in_queue: bool,
) -> Option<Arc<DefaultMappedFile>> {
    let file_path_text = file_path.to_string_lossy().into_owned();
    let mut mapped_file = if let Some(service) = allocate_service.filter(|service| service.is_started()) {
        match service.allocate_mapped_file_blocking(file_path_text, mapped_file_size) {
            Ok(pre_allocated) => {
                service.submit_request_in_background(next_file_path.to_string_lossy().into_owned(), mapped_file_size);
                pre_allocated
            }
            Err(error) => {
                warn!(
                    "Pre-allocation failed: {}; synchronous same-path fallback is disabled",
                    error
                );
                return None;
            }
        }
    } else if allocate_service.is_none_or(|service| service.allows_synchronous_fallback(file_path)) {
        create_mapped_file_synchronously(file_path_text, mapped_file_size)?
    } else {
        warn!(
            file_path = %file_path.display(),
            "Mapped-file synchronous fallback is fenced by a started, stopped, or retiring allocation service"
        );
        return None;
    };

    if first_in_queue {
        if let Some(mapped_file) = Arc::get_mut(&mut mapped_file) {
            mapped_file.set_first_create_in_queue(true);
        }
    }
    Some(mapped_file)
}

fn create_mapped_file_synchronously(file_path: String, mapped_file_size: u64) -> Option<Arc<DefaultMappedFile>> {
    match DefaultMappedFile::try_new(CheetahString::from_string(file_path.clone()), mapped_file_size) {
        Ok(mapped_file) => Some(Arc::new(mapped_file)),
        Err(error) => {
            error!("Failed to create mapped file {}: {}", file_path, error);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use tempfile::tempdir;

    use super::*;
    use crate::mapped_file::retirement::identity::StoreRelativePath;
    use crate::mapped_file::retirement::platform::physical_file_key;

    #[test]
    fn queue_entry_enumeration_failure_fails_closed() {
        let outcome = load_mapped_file_queue_entries(
            "queue",
            [Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected enumeration failure",
            ))],
            16,
        );

        assert!(!outcome.is_success());
        assert!(outcome.into_mapped_files().is_empty());
    }

    #[test]
    fn reconciled_queue_load_is_handle_authoritative_and_all_or_nothing() {
        let root = tempdir().expect("create Store root");
        std::fs::create_dir(root.path().join("commitlog")).expect("create commitlog directory");
        let first = reconciled_segment(root.path(), 0, 16, 16);
        let second = reconciled_segment(root.path(), 16, 16, 16);

        let loaded = load_reconciled_mapped_file_queue(root.path(), vec![second, first])
            .expect("exact reconciled generation loads")
            .snapshot();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].get_file_from_offset(), 0);
        assert_eq!(loaded[1].get_file_from_offset(), 16);

        let valid = reconciled_segment(root.path(), 32, 16, 16);
        let invalid = reconciled_segment(root.path(), 48, 8, 16);
        assert!(load_reconciled_mapped_file_queue(root.path(), vec![valid, invalid]).is_err());
    }

    fn reconciled_segment(root: &Path, offset: u64, actual_length: u64, expected_length: u64) -> ReconciledSegmentFile {
        let relative =
            StoreRelativePath::new(&format!("commitlog/{offset:020}")).expect("test segment path is canonical");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(relative.join_under(root))
            .expect("create test segment");
        file.set_len(actual_length).expect("size test segment");
        let key = physical_file_key(&file).expect("capture test physical identity");
        ReconciledSegmentFile::for_test(relative, key, expected_length, offset, file)
    }

    #[test]
    fn zero_length_tail_delete_failure_fails_closed_and_retains_file() {
        let temp_dir = tempdir().expect("temp dir");
        let first = temp_dir.path().join("00000000000000000000");
        let empty_tail = temp_dir.path().join("00000000000000000016");
        File::create(&first)
            .expect("create first segment")
            .set_len(16)
            .expect("size first segment");
        File::create(&empty_tail).expect("create empty tail");

        let outcome = load_mapped_file_queue_files_with_remover(vec![empty_tail.clone(), first], 16, |_| {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected delete failure",
            ))
        });

        assert!(!outcome.is_success());
        assert_eq!(outcome.into_mapped_files().len(), 1);
        assert!(empty_tail.exists());
    }
}
