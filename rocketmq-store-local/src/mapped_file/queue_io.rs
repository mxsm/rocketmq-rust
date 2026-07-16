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
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use cheetah_string::CheetahString;
use tracing::error;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;

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
    let Ok(entries) = fs::read_dir(Path::new(store_path)) else {
        return MappedFileQueueLoadOutcome::new(true, Vec::new());
    };
    let files = entries.filter_map(Result::ok).map(|entry| entry.path()).collect();
    load_mapped_file_queue_files(files, mapped_file_size)
}

/// Loads an explicit mapped-file queue candidate list in ascending file-name order.
#[doc(hidden)]
pub fn load_mapped_file_queue_files(mut files: Vec<PathBuf>, mapped_file_size: u64) -> MappedFileQueueLoadOutcome {
    files.sort_by(|left, right| left.file_name().cmp(&right.file_name()));

    let mut mapped_files = Vec::new();
    for (index, file) in files.iter().enumerate() {
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                error!("Failed to get metadata for file {:?}: {}", file, error);
                return MappedFileQueueLoadOutcome::new(false, mapped_files);
            }
        };

        if metadata.is_dir() {
            continue;
        }

        if metadata.len() == 0 && index == files.len() - 1 {
            match fs::remove_file(file) {
                Ok(()) => warn!("{} size is 0, auto deleted.", file.display()),
                Err(error) => warn!("Failed to delete file {}: {}", file.display(), error),
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

/// Creates one queue segment through the allocation service or the synchronous fallback.
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
        match service.allocate_mapped_file_blocking(file_path_text.clone(), mapped_file_size) {
            Ok(pre_allocated) => {
                service.submit_request_in_background(next_file_path.to_string_lossy().into_owned(), mapped_file_size);
                pre_allocated
            }
            Err(error) => {
                warn!("Pre-allocation failed: {}, using sync creation", error);
                create_mapped_file_synchronously(file_path_text, mapped_file_size)?
            }
        }
    } else {
        create_mapped_file_synchronously(file_path_text, mapped_file_size)?
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
