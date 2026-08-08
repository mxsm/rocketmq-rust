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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use tracing::info;
use tracing::warn;

use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::MappedFileDestroyOutcome;

/// Returns whether `modified` is at least `retention` older than `now`.
///
/// Future modification times are treated as fresh so wall-clock rollback cannot trigger deletion.
#[doc(hidden)]
pub fn is_expired(modified: SystemTime, now: SystemTime, retention: Duration) -> bool {
    now.duration_since(modified).is_ok_and(|age| age >= retention)
}

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

    /// Returns the number of paths removed from the filesystem namespace.
    #[doc(hidden)]
    pub fn deleted_count(&self) -> i32 {
        self.deleted_count
    }

    /// Returns files whose paths were removed and that the collection owner may now untrack.
    #[doc(hidden)]
    pub fn into_mapped_files(self) -> Vec<Arc<DefaultMappedFile>> {
        self.mapped_files
    }
}

/// Destroys the newest mapped file for recovery rollback.
#[doc(hidden)]
pub fn destroy_last_mapped_file(files: &[Arc<DefaultMappedFile>]) -> Option<Arc<DefaultMappedFile>> {
    let last_mapped_file = files.last()?.clone();
    if last_mapped_file.try_destroy(1000).is_namespace_removed() {
        info!(
            "on recover, removed a logic mapped file {}",
            last_mapped_file.get_file_name()
        );
        Some(last_mapped_file)
    } else {
        warn!(
            "on recover, mapped file remains tracked for deletion retry: {}",
            last_mapped_file.get_file_name()
        );
        None
    }
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
    now_millis: N,
) -> MappedFileQueueDeletion
where
    N: FnMut() -> i64,
{
    delete_expired_mapped_files_by_time_before(
        files,
        expired_time,
        delete_files_interval,
        interval_forcibly,
        clean_immediately,
        delete_file_batch_max,
        None,
        now_millis,
    )
}

/// Destroys expired mapped files without crossing an optional derived-engine WAL pin.
#[doc(hidden)]
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors the legacy cleanup policy plus one hard WAL boundary"
)]
pub fn delete_expired_mapped_files_by_time_before<N>(
    files: &[Arc<DefaultMappedFile>],
    expired_time: i64,
    delete_files_interval: i32,
    interval_forcibly: i64,
    clean_immediately: bool,
    delete_file_batch_max: i32,
    pinned_file_offset: Option<u64>,
    mut now_millis: N,
) -> MappedFileQueueDeletion
where
    N: FnMut() -> i64,
{
    let candidate_count = files.len().saturating_sub(1);
    let mut deleted_files = Vec::new();
    let Ok(retention_millis) = u64::try_from(expired_time) else {
        warn!(
            expired_time,
            "negative mapped-file retention is invalid; skipping cleanup"
        );
        return MappedFileQueueDeletion::new(0, deleted_files);
    };
    let Ok(interval_forcibly) = u64::try_from(interval_forcibly) else {
        warn!(
            interval_forcibly,
            "negative force-clean interval is invalid; skipping cleanup"
        );
        return MappedFileQueueDeletion::new(0, deleted_files);
    };
    let Ok(delete_files_interval) = u64::try_from(delete_files_interval) else {
        warn!(
            delete_files_interval,
            "negative mapped-file delete interval is invalid; skipping cleanup"
        );
        return MappedFileQueueDeletion::new(0, deleted_files);
    };
    if delete_file_batch_max <= 0 {
        warn!(delete_file_batch_max, "mapped-file delete batch limit must be positive");
        return MappedFileQueueDeletion::new(0, deleted_files);
    }
    let retention = Duration::from_millis(retention_millis);

    for (index, mapped_file) in files.iter().enumerate().take(candidate_count) {
        if pinned_file_offset.is_some_and(|pinned| mapped_file.get_file_from_offset() >= pinned) {
            break;
        }
        let expired = if clean_immediately {
            true
        } else {
            let now_millis = now_millis();
            let Ok(now_millis) = u64::try_from(now_millis) else {
                warn!(
                    now_millis,
                    "negative cleanup clock value; stopping oldest-first cleanup"
                );
                break;
            };
            let Some(now) = UNIX_EPOCH.checked_add(Duration::from_millis(now_millis)) else {
                warn!(
                    now_millis,
                    "cleanup clock value is out of range; stopping oldest-first cleanup"
                );
                break;
            };
            let modified = match mapped_file.try_last_modified_time() {
                Ok(modified) => modified,
                Err(error) => {
                    warn!(
                        file_name = %mapped_file.get_file_name(),
                        error = ?error,
                        "failed to read modification time; stopping oldest-first cleanup"
                    );
                    break;
                }
            };
            is_expired(modified, now, retention)
        };
        if expired {
            if mapped_file.try_destroy(interval_forcibly).is_namespace_removed() {
                deleted_files.push(mapped_file.clone());
                if deleted_files.len() >= delete_file_batch_max as usize {
                    break;
                }
                if delete_files_interval > 0 && index + 1 < candidate_count {
                    thread::sleep(Duration::from_millis(delete_files_interval));
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
            if let Some(buffer) = result.get_bytes_ref() {
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

        if destroy && mapped_file.try_destroy(1000 * 60).is_namespace_removed() {
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
    let Ok(interval_forcibly) = u64::try_from(interval_forcibly) else {
        warn!(
            interval_forcibly,
            "negative force-clean interval is invalid; skipping retry"
        );
        return MappedFileQueueDeletion::new(0, Vec::new());
    };
    if first.try_destroy(interval_forcibly).is_namespace_removed() {
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
    if force_swap_interval_ms < 0 || normal_swap_interval_ms < 0 {
        warn!(
            force_swap_interval_ms,
            normal_swap_interval_ms, "negative mapped-file swap intervals are invalid; skipping swap"
        );
        return;
    }
    let reserve_num = reserve_num.max(3);
    let files_len = files.len() as i32;
    for index in (0..=(files_len - reserve_num - 1)).rev() {
        if index < 0 {
            break;
        }
        let mapped_file = &files[index as usize];
        let elapsed = now_millis().saturating_sub(mapped_file.get_recent_swap_map_time());
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
    if force_clean_swap_interval_ms < 0 {
        warn!(
            force_clean_swap_interval_ms,
            "negative mapped-file clean-swap interval is invalid; skipping cleanup"
        );
        return;
    }
    let reserve_num = 3;
    let files_len = files.len() as i32;
    for index in (0..=(files_len - reserve_num - 1)).rev() {
        if index < 0 {
            break;
        }
        let mapped_file = &files[index as usize];
        let snapshot = mapped_file.swap_generation_snapshot();
        if now_millis().saturating_sub(snapshot.time_millis()) > force_clean_swap_interval_ms {
            let _ = mapped_file.try_clean_swapped_generation(snapshot, true);
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

/// Destroys mapped files in oldest-first order and removes an empty queue directory.
///
/// The first deferred or failed file stops the operation so callers retain a contiguous retry
/// identity. Unknown directory entries are never removed recursively.
#[doc(hidden)]
pub fn destroy_mapped_file_queue(files: &[Arc<DefaultMappedFile>], store_path: &str) -> MappedFileQueueDeletion {
    // Close the complete generation before the first destructive attempt. If one file remains
    // leased, later files must not stay writable while the queue waits for a retry.
    shutdown_mapped_file_queue(files, 1000 * 3);
    let mut deleted_files = Vec::new();
    for mapped_file in files {
        match mapped_file.try_destroy(1000 * 3) {
            MappedFileDestroyOutcome::NamespaceRemoved => deleted_files.push(mapped_file.clone()),
            MappedFileDestroyOutcome::CleanupPending { ref_count } => {
                warn!(
                    file_name = %mapped_file.get_file_name(),
                    ref_count,
                    "mapped-file queue destroy deferred; retaining this and later identities"
                );
                break;
            }
            MappedFileDestroyOutcome::DeleteFailed { kind, raw_os_error } => {
                warn!(
                    file_name = %mapped_file.get_file_name(),
                    ?kind,
                    ?raw_os_error,
                    "mapped-file queue destroy failed; retaining this and later identities"
                );
                break;
            }
        }
    }
    let path = Path::new(store_path);
    if deleted_files.len() == files.len() && path.is_dir() {
        match fs::remove_dir(path) {
            Ok(()) => info!(path = %path.display(), "removed empty mapped-file queue directory"),
            Err(error) => warn!(
                path = %path.display(),
                error = ?error,
                "mapped-file queue directory remains non-empty or unavailable"
            ),
        }
    }
    MappedFileQueueDeletion::new(deleted_files.len() as i32, deleted_files)
}
