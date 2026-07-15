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

//! Local mapped-file queue deletion, swap, and lifecycle side effects.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tracing::info;
use tracing::warn;

use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;

/// Files deleted by one queue maintenance operation.
#[doc(hidden)]
pub struct MappedFileQueueDeletion {
    deleted_count: i32,
    mapped_files: Vec<Arc<DefaultMappedFile>>,
}

impl MappedFileQueueDeletion {
    fn new(deleted_count: i32, mapped_files: Vec<Arc<DefaultMappedFile>>) -> Self {
        Self {
            deleted_count,
            mapped_files,
        }
    }

    /// Returns the number of successfully destroyed files.
    #[doc(hidden)]
    pub fn deleted_count(&self) -> i32 {
        self.deleted_count
    }

    /// Returns the destroyed files that the collection owner must remove.
    #[doc(hidden)]
    pub fn into_mapped_files(self) -> Vec<Arc<DefaultMappedFile>> {
        self.mapped_files
    }
}

/// Destroys the newest mapped file for recovery rollback.
#[doc(hidden)]
pub fn destroy_last_mapped_file(files: &[Arc<DefaultMappedFile>]) -> Option<Arc<DefaultMappedFile>> {
    let last_mapped_file = files.last()?.clone();
    last_mapped_file.destroy(1000);
    info!(
        "on recover, destroy a logic mapped file {}",
        last_mapped_file.get_file_name()
    );
    Some(last_mapped_file)
}

/// Produces a collection snapshot with existing removal candidates excluded.
#[doc(hidden)]
pub fn mapped_files_after_removal(
    current_files: &[Arc<DefaultMappedFile>],
    removal_candidates: &[Arc<DefaultMappedFile>],
) -> Vec<Arc<DefaultMappedFile>> {
    let existing_candidates: Vec<_> = removal_candidates
        .iter()
        .filter(|candidate| current_files.contains(candidate))
        .collect();
    current_files
        .iter()
        .filter(|mapped_file| !existing_candidates.contains(mapped_file))
        .cloned()
        .collect()
}

/// Destroys expired mapped files in oldest-first order while retaining the newest file.
#[doc(hidden)]
pub fn delete_expired_mapped_files_by_time<N>(
    files: &[Arc<DefaultMappedFile>],
    expired_time: i64,
    delete_files_interval: i32,
    interval_forcibly: i64,
    clean_immediately: bool,
    delete_file_batch_max: i32,
    mut now_millis: N,
) -> MappedFileQueueDeletion
where
    N: FnMut() -> i64,
{
    let candidate_count = files.len().saturating_sub(1);
    let mut deleted_files = Vec::new();

    for (index, mapped_file) in files.iter().enumerate().take(candidate_count) {
        let live_max_timestamp = mapped_file.get_last_modified_timestamp() as i64 + expired_time;
        if now_millis() >= live_max_timestamp || clean_immediately {
            if mapped_file.destroy(interval_forcibly as u64) {
                deleted_files.push(mapped_file.clone());
                if deleted_files.len() >= delete_file_batch_max as usize {
                    break;
                }
                if delete_files_interval > 0 && index + 1 < candidate_count {
                    thread::sleep(Duration::from_millis(delete_files_interval as u64));
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }

    MappedFileQueueDeletion::new(deleted_files.len() as i32, deleted_files)
}

/// Destroys consume-queue files whose last physical offset precedes the retained offset.
#[doc(hidden)]
pub fn delete_expired_mapped_files_by_offset(
    files: &[Arc<DefaultMappedFile>],
    mapped_file_size: u64,
    offset: i64,
    unit_size: i32,
) -> MappedFileQueueDeletion {
    let candidate_count = files.len().saturating_sub(1);
    let mut deleted_files = Vec::new();

    for mapped_file in files.iter().take(candidate_count) {
        let mut destroy = false;
        if let Some(result) = mapped_file.select_mapped_buffer((mapped_file_size - unit_size as u64) as i32, unit_size)
        {
            if let Some(ref buffer) = result.bytes {
                if buffer.len() >= 8 {
                    let max_offset_in_logic_queue = i64::from_be_bytes(buffer[0..8].try_into().unwrap_or([0; 8]));
                    destroy = max_offset_in_logic_queue < offset;
                    if destroy {
                        info!(
                            "physic min offset {}, logics in current mappedFile max offset {}, delete it",
                            offset, max_offset_in_logic_queue
                        );
                    }
                }
            }
        } else if !mapped_file.is_available() {
            warn!("Found a hanged consume queue file, attempting to delete it.");
            destroy = true;
        } else {
            warn!("this being not executed forever.");
            break;
        }

        if destroy && mapped_file.destroy(1000 * 60) {
            deleted_files.push(mapped_file.clone());
        } else {
            break;
        }
    }

    MappedFileQueueDeletion::new(deleted_files.len() as i32, deleted_files)
}

/// Retries destruction of an unavailable first file.
#[doc(hidden)]
pub fn retry_delete_first_mapped_file(
    first: Option<&Arc<DefaultMappedFile>>,
    interval_forcibly: i64,
) -> MappedFileQueueDeletion {
    let Some(first) = first.filter(|mapped_file| !mapped_file.is_available()) else {
        return MappedFileQueueDeletion::new(0, Vec::new());
    };
    warn!(
        "The mappedFile was destroyed once, but still alive: {}",
        first.get_file_name()
    );
    if first.destroy(interval_forcibly as u64) {
        info!("The mappedFile re-delete OK: {}", first.get_file_name());
        MappedFileQueueDeletion::new(1, vec![first.clone()])
    } else {
        warn!("The mappedFile re-delete failed: {}", first.get_file_name());
        MappedFileQueueDeletion::new(0, Vec::new())
    }
}

/// Swaps old mapped-file buffers according to the legacy reserve and interval policy.
#[doc(hidden)]
pub fn swap_mapped_file_queue<N>(
    files: &[Arc<DefaultMappedFile>],
    reserve_num: i32,
    force_swap_interval_ms: i64,
    normal_swap_interval_ms: i64,
    mut now_millis: N,
) where
    N: FnMut() -> i64,
{
    if files.is_empty() {
        return;
    }
    let reserve_num = reserve_num.max(3);
    let files_len = files.len() as i32;
    for index in (0..=(files_len - reserve_num - 1)).rev() {
        if index < 0 {
            break;
        }
        let mapped_file = &files[index as usize];
        let elapsed = now_millis() - mapped_file.get_recent_swap_map_time();
        if elapsed > force_swap_interval_ms {
            mapped_file.swap_map();
            continue;
        }
        if elapsed > normal_swap_interval_ms && mapped_file.get_mapped_byte_buffer_access_count_since_last_swap() > 0 {
            mapped_file.swap_map();
        }
    }
}

/// Re-applies swap cleanup to old mappings after the force-clean interval.
#[doc(hidden)]
pub fn clean_swapped_mapped_file_queue<N>(
    files: &[Arc<DefaultMappedFile>],
    force_clean_swap_interval_ms: i64,
    mut now_millis: N,
) where
    N: FnMut() -> i64,
{
    if files.is_empty() {
        return;
    }
    let reserve_num = 3;
    let files_len = files.len() as i32;
    for index in (0..=(files_len - reserve_num - 1)).rev() {
        if index < 0 {
            break;
        }
        let mapped_file = &files[index as usize];
        if now_millis() - mapped_file.get_recent_swap_map_time() > force_clean_swap_interval_ms {
            let _ = mapped_file.swap_map();
        }
    }
}

/// Shuts down every mapped file in the current collection snapshot.
#[doc(hidden)]
pub fn shutdown_mapped_file_queue(files: &[Arc<DefaultMappedFile>], interval_forcibly: u64) {
    for mapped_file in files {
        mapped_file.shutdown(interval_forcibly);
    }
}

/// Destroys every mapped file and removes the queue directory when it exists.
#[doc(hidden)]
pub fn destroy_mapped_file_queue(files: &[Arc<DefaultMappedFile>], store_path: &str) {
    for mapped_file in files {
        mapped_file.destroy(1000 * 3);
    }
    let path = Path::new(store_path);
    if path.is_dir() {
        let _ = fs::remove_dir_all(path);
    }
}
