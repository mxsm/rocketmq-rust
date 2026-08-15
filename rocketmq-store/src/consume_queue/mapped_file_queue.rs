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

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all::offset_to_file_name;
use rocketmq_store_local::flush::queue::commit_mapped_file_queue;
use rocketmq_store_local::flush::queue::try_flush_mapped_file_queue;
use rocketmq_store_local::flush::queue::SegmentCommitProgress;
use rocketmq_store_local::flush::queue::SegmentFlushProgress;
use rocketmq_store_local::mapped_file::queue_allocation::plan_mapped_file_queue_creation;
use rocketmq_store_local::mapped_file::queue_allocation::plan_mapped_file_queue_preallocation;
use rocketmq_store_local::mapped_file::queue_allocation::MappedFileQueueLastFile;
use rocketmq_store_local::mapped_file::queue_allocation::MappedFileQueueRollFile;
use rocketmq_store_local::mapped_file::queue_index::file_index_by_offset;
use rocketmq_store_local::mapped_file::queue_index::file_index_by_timestamp;
use rocketmq_store_local::mapped_file::queue_index::for_each_discontinuous_pair;
use rocketmq_store_local::mapped_file::queue_index::overlapping_file_range;
use rocketmq_store_local::mapped_file::queue_index::MappedFileQueueIndex;
use rocketmq_store_local::mapped_file::queue_io::create_mapped_file_for_queue;
use rocketmq_store_local::mapped_file::queue_io::create_mapped_file_for_queue_without_preallocation;
use rocketmq_store_local::mapped_file::queue_io::load_mapped_file_queue_files;
use rocketmq_store_local::mapped_file::queue_io::load_mapped_file_queue_path;
use rocketmq_store_local::mapped_file::queue_io::MappedFileQueueLoadOutcome;
use rocketmq_store_local::mapped_file::queue_lifecycle::clean_swapped_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_offset;
use rocketmq_store_local::mapped_file::queue_lifecycle::delete_expired_mapped_files_by_time_before;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_last_mapped_file_for_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::destroy_mapped_files_in_order;
use rocketmq_store_local::mapped_file::queue_lifecycle::retry_delete_first_mapped_file;
use rocketmq_store_local::mapped_file::queue_lifecycle::select_expired_mapped_files_by_offset;
use rocketmq_store_local::mapped_file::queue_lifecycle::select_expired_mapped_files_by_time_before;
use rocketmq_store_local::mapped_file::queue_lifecycle::shutdown_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::swap_mapped_file_queue;
use rocketmq_store_local::mapped_file::queue_lifecycle::MappedFileQueueDeletion;
use rocketmq_store_local::mapped_file::queue_maintenance::mapped_file_queue_truncate_action;
use rocketmq_store_local::mapped_file::queue_maintenance::plan_mapped_file_queue_reset;
use rocketmq_store_local::mapped_file::queue_maintenance::MappedFileQueueResetLastFile;
use rocketmq_store_local::mapped_file::queue_maintenance::MappedFileQueueTruncateAction;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_available_memory_size;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_fall_behind;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_io_stats;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_lazy_mmap_stats;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_max_offset;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_max_wrote_position;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_min_offset;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_selection_stats;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_should_roll;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_total_size;
use rocketmq_store_local::mapped_file::queue_metrics::mapped_file_queue_warmup_stats;
pub use rocketmq_store_local::mapped_file::queue_metrics::MappedFileIoStats;
pub use rocketmq_store_local::mapped_file::queue_metrics::MappedFileSelectionStats;
pub use rocketmq_store_local::mapped_file::queue_metrics::MappedFileWarmupStats;
use rocketmq_store_local::mapped_file::queue_state::MappedFileQueueRuntimeState;
use rocketmq_store_local::mapped_file::queue_storage::MappedFileQueueStorage;
use rocketmq_store_local::mapped_file::ManagedLifecycleRuntime;
use rocketmq_store_local::mapped_file::ManagedMappedFileQueueGeneration;
use rocketmq_store_local::mapped_file::ManagedRetirementReason;
use rocketmq_store_local::mapped_file::MappedFileQueueGeneration;
use rocketmq_store_local::mapped_file::MappedFileQueueSnapshot;
use tracing::error;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::base::select_result::SelectMappedBufferResult;
use crate::log_file::commit_log::CommitLogReadHandle;
use crate::log_file::commit_log_path_set::CommitLogPathSet;
use crate::log_file::commit_log_path_set::StoreFaultPoint;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::default_mapped_file_impl::LazyMmapStats;
use crate::log_file::mapped_file::MappedFile;
use crate::log_file::mapped_file::MappedFileResult;
use crate::queue::single_consume_queue::CQ_STORE_UNIT_SIZE;

enum MappedFileGenerationState {
    Legacy(MappedFileQueueGeneration<DefaultMappedFile>),
    Managed {
        generation: ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        runtime: Option<ManagedLifecycleRuntime>,
    },
}

#[derive(Clone)]
struct MappedFileGeneration {
    state: Arc<ArcSwap<MappedFileGenerationState>>,
}

impl MappedFileGeneration {
    fn legacy() -> Self {
        Self {
            state: Arc::new(ArcSwap::from_pointee(MappedFileGenerationState::Legacy(
                MappedFileQueueGeneration::new(),
            ))),
        }
    }

    fn managed(runtime: ManagedLifecycleRuntime) -> Self {
        let generation = runtime.empty_queue_generation();
        Self {
            state: Arc::new(ArcSwap::from_pointee(MappedFileGenerationState::Managed {
                generation,
                runtime: Some(runtime),
            })),
        }
    }

    #[cfg(test)]
    fn from_legacy_recovery_files(files: Vec<Arc<DefaultMappedFile>>) -> Self {
        Self {
            state: Arc::new(ArcSwap::from_pointee(MappedFileGenerationState::Legacy(
                MappedFileQueueGeneration::from_recovery_files(files),
            ))),
        }
    }

    fn snapshot(&self) -> MappedFileQueueSnapshot<DefaultMappedFile> {
        match self.state.load().as_ref() {
            MappedFileGenerationState::Legacy(generation) => generation.snapshot(),
            MappedFileGenerationState::Managed { generation, .. } => generation.snapshot(),
        }
    }

    fn allows_legacy_namespace_mutation(&self) -> bool {
        matches!(self.state.load().as_ref(), MappedFileGenerationState::Legacy(_))
    }

    fn managed_generation(&self) -> Option<ManagedMappedFileQueueGeneration<DefaultMappedFile>> {
        match self.state.load().as_ref() {
            MappedFileGenerationState::Legacy(_) => None,
            MappedFileGenerationState::Managed { generation, .. } => Some(generation.clone()),
        }
    }

    fn managed_runtime_generation(
        &self,
    ) -> Option<(
        ManagedLifecycleRuntime,
        ManagedMappedFileQueueGeneration<DefaultMappedFile>,
    )> {
        match self.state.load().as_ref() {
            MappedFileGenerationState::Legacy(_) => None,
            MappedFileGenerationState::Managed {
                generation,
                runtime: Some(runtime),
            } => Some((runtime.clone(), generation.clone())),
            MappedFileGenerationState::Managed { runtime: None, .. } => None,
        }
    }

    fn publish_legacy_created_file(&self, mapped_file: Arc<DefaultMappedFile>) -> bool {
        let state = self.state.load();
        let MappedFileGenerationState::Legacy(generation) = state.as_ref() else {
            return false;
        };
        generation.publish_legacy_created_file(mapped_file);
        true
    }

    fn apply_legacy_namespace_removal(&self, deletion: MappedFileQueueDeletion) -> i32 {
        match self.state.load().as_ref() {
            MappedFileGenerationState::Legacy(generation) => generation.apply_legacy_namespace_removal(deletion),
            MappedFileGenerationState::Managed { .. } => 0,
        }
    }

    fn extend_legacy_recovery_generation(&self, files: Vec<Arc<DefaultMappedFile>>) -> bool {
        let state = self.state.load();
        let MappedFileGenerationState::Legacy(generation) = state.as_ref() else {
            return false;
        };
        generation.extend_recovery_generation(files);
        true
    }

    fn install_legacy_recovery_generation(&self, files: Vec<Arc<DefaultMappedFile>>) -> bool {
        let state = self.state.load();
        let MappedFileGenerationState::Legacy(generation) = state.as_ref() else {
            return false;
        };
        generation.install_recovery_generation(files);
        true
    }

    fn install_reconciled_generation(&self, generation: ManagedMappedFileQueueGeneration<DefaultMappedFile>) -> bool {
        let current = self.state.load_full();
        if !matches!(current.as_ref(), MappedFileGenerationState::Legacy(_)) {
            return false;
        }
        let previous = self.state.compare_and_swap(
            &current,
            Arc::new(MappedFileGenerationState::Managed {
                generation,
                runtime: None,
            }),
        );
        Arc::ptr_eq(&previous, &current)
    }

    fn bind_managed_runtime(&self, runtime: ManagedLifecycleRuntime) -> bool {
        let current = self.state.load_full();
        let MappedFileGenerationState::Managed {
            generation,
            runtime: None,
        } = current.as_ref()
        else {
            return false;
        };
        let next = Arc::new(MappedFileGenerationState::Managed {
            generation: generation.clone(),
            runtime: Some(runtime.clone()),
        });
        let previous = self.state.compare_and_swap(&current, next);
        let installed = Arc::ptr_eq(&previous, &current);
        if installed {
            runtime.track_queue_generation(generation);
        }
        installed
    }
}

/// Narrow, cloneable capability used by commit-log readers.
///
/// The handle observes the atomically published mapped-file generation and
/// shared flush progress without exposing allocation, recovery, truncation, or
/// destruction operations.
#[derive(Clone)]
pub(crate) struct MappedFileQueueReadHandle {
    mapped_files: MappedFileGeneration,
    mapped_file_size: u64,
    runtime_state: MappedFileQueueRuntimeState,
}

impl MappedFileQueueReadHandle {
    fn find_mapped_file_by_offset(
        &self,
        offset: i64,
        return_first_on_not_found: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        let files = self.mapped_files.snapshot();
        let first = files.first()?.clone();
        let last = files.last()?.clone();
        match file_index_by_offset(
            files.as_ref(),
            self.mapped_file_size,
            offset,
            return_first_on_not_found,
            first.get_file_from_offset(),
            last.get_file_from_offset(),
            |file| file.get_file_from_offset(),
        ) {
            Some(MappedFileQueueIndex::First) => Some(first),
            Some(MappedFileQueueIndex::Indexed(index)) => Some(files[index].clone()),
            None => None,
        }
    }

    pub(crate) fn get_max_offset(&self) -> i64 {
        let files = self.mapped_files.snapshot();
        mapped_file_queue_max_offset(files.last())
    }

    pub(crate) fn get_min_offset(&self) -> i64 {
        match self.mapped_files.snapshot().first() {
            None => -1,
            Some(mapped_file) if mapped_file.is_available() => mapped_file.get_file_from_offset() as i64,
            Some(mapped_file) => self.roll_next_file(mapped_file.get_file_from_offset() as i64),
        }
    }

    pub(crate) fn get_flushed_where(&self) -> i64 {
        self.runtime_state.flushed_where()
    }

    pub(crate) fn check_self(&self) {
        let mapped_files = self.mapped_files.snapshot();
        for_each_discontinuous_pair(
            mapped_files.as_ref(),
            self.mapped_file_size,
            |file| file.get_file_from_offset(),
            |previous, current| {
                error!(
                    "[BUG] The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre \
                     file {}, cur file {}",
                    mapped_files[previous].get_file_name(),
                    mapped_files[current].get_file_name()
                );
            },
        );
    }

    pub(crate) fn roll_next_file(&self, offset: i64) -> i64 {
        let mapped_file_size = self.mapped_file_size as i64;
        offset + mapped_file_size - (offset % mapped_file_size)
    }

    pub(crate) fn get_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        let mapped_file = self.find_mapped_file_by_offset(offset, offset == 0)?;
        let position = (offset % self.mapped_file_size as i64) as i32;
        let mut result = mapped_file.select_mapped_buffer_with_position(position);
        if let Some(result) = result.as_mut() {
            result.try_attach_mapped_file(mapped_file);
        }
        result
    }

    pub(crate) fn get_data_bounded(&self, offset: i64, max_bytes: usize) -> Option<SelectMappedBufferResult> {
        if max_bytes == 0 {
            return None;
        }
        let mapped_file = self.find_mapped_file_by_offset(offset, offset == 0)?;
        let position = (offset % self.mapped_file_size as i64) as i32;
        let readable = mapped_file.get_read_position().checked_sub(position)?;
        if readable <= 0 {
            return None;
        }
        let size = usize::try_from(readable).ok()?.min(max_bytes).min(i32::MAX as usize) as i32;
        let mut result = mapped_file.select_mapped_buffer(position, size);
        if let Some(result) = result.as_mut() {
            result.try_attach_mapped_file(mapped_file);
        }
        result
    }

    pub(crate) fn get_message(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        let mapped_file = self.find_mapped_file_by_offset(offset, offset == 0)?;
        let position = (offset % self.mapped_file_size as i64) as i32;
        let mut result = mapped_file.select_mapped_buffer(position, size);
        if let Some(result) = result.as_mut() {
            result.try_attach_mapped_file(mapped_file);
        }
        result
    }

    pub(crate) fn get_message_for_transfer(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        if offset < 0 || size <= 0 {
            return None;
        }
        let mapped_file = self.find_mapped_file_by_offset(offset, offset == 0)?;
        let position = usize::try_from(offset % self.mapped_file_size as i64).ok()?;
        mapped_file
            .try_file_range_selection(position, size as usize)
            .ok()
            .flatten()
    }

    pub(crate) fn get_bulk_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        if size <= 0 {
            return Some(Vec::new());
        }

        let mapped_file_size = self.mapped_file_size as i64;
        let mut current_offset = offset;
        let mut remaining = size as usize;
        let mut results = Vec::new();

        while remaining > 0 {
            let mapped_file = self.find_mapped_file_by_offset(current_offset, current_offset == offset)?;
            let pos = (current_offset % mapped_file_size) as i32;
            let readable = mapped_file.get_read_position().checked_sub(pos)?;
            if readable <= 0 {
                return None;
            }
            let take = usize::try_from(readable).ok()?.min(remaining);
            let mut result = mapped_file.select_mapped_buffer(pos, take as i32)?;
            result.try_attach_mapped_file(mapped_file);

            results.push(result);
            current_offset += take as i64;
            remaining -= take;
        }

        Some(results)
    }

    pub(crate) fn get_bulk_transfer_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        if offset < 0 {
            return None;
        }
        if size <= 0 {
            return Some(Vec::new());
        }

        let mapped_file_size = self.mapped_file_size as i64;
        let mut current_offset = offset;
        let mut remaining = size as usize;
        let mut results = Vec::new();

        while remaining > 0 {
            let mapped_file = self.find_mapped_file_by_offset(current_offset, current_offset == offset)?;
            let position = usize::try_from(current_offset % mapped_file_size).ok()?;
            let readable = usize::try_from(mapped_file.get_read_position())
                .ok()?
                .checked_sub(position)?;
            if readable == 0 {
                return None;
            }
            let take = readable.min(remaining);
            let result = mapped_file.try_file_range_selection(position, take).ok().flatten()?;
            results.push(result);
            current_offset = current_offset.checked_add(take as i64)?;
            remaining -= take;
        }

        Some(results)
    }
}

/// Narrow, cloneable capability used by replica appenders.
///
/// The handle shares the authoritative mapped-file generation, allocator, and
/// maintenance lock without exposing recovery, truncation, or destruction.
#[derive(Clone)]
pub(crate) struct MappedFileQueueAppendHandle {
    mapped_files: MappedFileGeneration,
    store_path: String,
    mapped_file_size: u64,
    allocate_mapped_file_service: Option<AllocateMappedFileService>,
    runtime_state: MappedFileQueueRuntimeState,
    commit_log_paths: Option<Arc<CommitLogPathSet>>,
}

impl MappedFileQueueAppendHandle {
    pub(crate) fn fence_writes(&self) {
        let _guard = self.runtime_state.commit_lock().lock();
        self.runtime_state.begin_close();
    }

    /// Returns the current absolute append position without exposing the mapped-file generation.
    pub(crate) fn current_append_offset(&self) -> u64 {
        self.mapped_files
            .snapshot()
            .last()
            .map(|mapped_file| {
                mapped_file
                    .get_file_from_offset()
                    .saturating_add(mapped_file.get_wrote_position().max(0) as u64)
            })
            .unwrap_or_default()
    }

    pub(crate) fn get_last_mapped_file(&self, start_offset: u64, need_create: bool) -> Option<Arc<DefaultMappedFile>> {
        get_last_mapped_file_for_append(
            &self.mapped_files,
            &self.store_path,
            self.mapped_file_size,
            self.allocate_mapped_file_service.as_ref(),
            &self.runtime_state,
            self.commit_log_paths.as_ref(),
            start_offset,
            need_create,
        )
    }
}

fn get_last_mapped_file_for_append(
    mapped_files: &MappedFileGeneration,
    store_path: &str,
    mapped_file_size: u64,
    allocate_mapped_file_service: Option<&AllocateMappedFileService>,
    runtime_state: &MappedFileQueueRuntimeState,
    commit_log_paths: Option<&Arc<CommitLogPathSet>>,
    start_offset: u64,
    need_create: bool,
) -> Option<Arc<DefaultMappedFile>> {
    if runtime_state.is_closing() {
        return None;
    }
    let mapped_file_last = mapped_files.snapshot().last().cloned();
    if mapped_file_last.as_ref().is_some_and(|file| !file.is_available()) {
        return None;
    }
    if let (Some(paths), Some(file)) = (commit_log_paths, mapped_file_last.as_ref()) {
        if paths.should_fail(StoreFaultPoint::Append, Path::new(file.get_file_name().as_str())) {
            let _guard = runtime_state.commit_lock().lock();
            runtime_state.begin_close();
            return None;
        }
    }
    if let Some(file) = mapped_file_last.as_ref() {
        let last_file =
            MappedFileQueueLastFile::new(file.get_file_from_offset(), file.get_wrote_position(), file.is_full());
        if mapped_files.allows_legacy_namespace_mutation() {
            if let Some(next_offset) = plan_mapped_file_queue_preallocation(mapped_file_size, last_file) {
                trigger_mapped_file_preallocation(
                    mapped_files,
                    store_path,
                    mapped_file_size,
                    allocate_mapped_file_service,
                    runtime_state,
                    commit_log_paths,
                    next_offset,
                );
            }
        }
    }
    let roll_file = mapped_file_last
        .as_ref()
        .map(|file| MappedFileQueueRollFile::new(file.get_file_from_offset(), file.is_full()));
    if let Some(create_offset) = plan_mapped_file_queue_creation(start_offset, mapped_file_size, roll_file, need_create)
    {
        return create_and_publish_mapped_file(
            mapped_files,
            store_path,
            mapped_file_size,
            allocate_mapped_file_service,
            runtime_state,
            commit_log_paths,
            create_offset,
        );
    }
    mapped_file_last
}

fn trigger_mapped_file_preallocation(
    mapped_files: &MappedFileGeneration,
    store_path: &str,
    mapped_file_size: u64,
    allocate_mapped_file_service: Option<&AllocateMappedFileService>,
    runtime_state: &MappedFileQueueRuntimeState,
    commit_log_paths: Option<&Arc<CommitLogPathSet>>,
    next_offset: u64,
) {
    let _maintenance_guard = runtime_state.commit_lock().lock();
    if runtime_state.is_closing() || !mapped_files.allows_legacy_namespace_mutation() {
        return;
    }
    let Some(service) = allocate_mapped_file_service else {
        return;
    };
    if !service.is_started() {
        return;
    }

    let root = match commit_log_paths {
        Some(paths) => match paths.creation_candidates(StoreFaultPoint::Preallocate) {
            Ok(candidates) => candidates.into_iter().next(),
            Err(_) => None,
        },
        None => Some(PathBuf::from(store_path)),
    };
    let Some(root) = root else {
        return;
    };
    let next_file_path = root.join(offset_to_file_name(next_offset));
    service.submit_request_in_background(next_file_path.to_string_lossy().to_string(), mapped_file_size);
}

fn create_and_publish_mapped_file(
    mapped_files: &MappedFileGeneration,
    store_path: &str,
    mapped_file_size: u64,
    allocate_mapped_file_service: Option<&AllocateMappedFileService>,
    runtime_state: &MappedFileQueueRuntimeState,
    commit_log_paths: Option<&Arc<CommitLogPathSet>>,
    create_offset: u64,
) -> Option<Arc<DefaultMappedFile>> {
    create_and_publish_mapped_file_with_before_lock(
        mapped_files,
        store_path,
        mapped_file_size,
        allocate_mapped_file_service,
        runtime_state,
        commit_log_paths,
        create_offset,
        || {},
    )
}

fn create_and_publish_mapped_file_with_before_lock<F>(
    mapped_files: &MappedFileGeneration,
    store_path: &str,
    mapped_file_size: u64,
    allocate_mapped_file_service: Option<&AllocateMappedFileService>,
    runtime_state: &MappedFileQueueRuntimeState,
    commit_log_paths: Option<&Arc<CommitLogPathSet>>,
    create_offset: u64,
    before_lock: F,
) -> Option<Arc<DefaultMappedFile>>
where
    F: FnOnce(),
{
    before_lock();
    let _maintenance_guard = runtime_state.commit_lock().lock();
    if runtime_state.is_closing() {
        return None;
    }
    let current_files = mapped_files.snapshot();
    if let Some(existing) = current_files
        .iter()
        .find(|mapped_file| mapped_file.get_file_from_offset() == create_offset)
    {
        return Some(Arc::clone(existing));
    }
    if let Some(generation) = mapped_files.managed_generation() {
        let service = allocate_mapped_file_service?;
        return match service.put_managed_request_and_return_mapped_file_blocking(
            generation,
            Path::new(store_path),
            create_offset,
            mapped_file_size,
        ) {
            Ok(mapped_file) => Some(mapped_file),
            Err(error) => {
                error!(
                    queue = store_path,
                    create_offset,
                    %error,
                    "managed mapped-file allocation failed closed"
                );
                None
            }
        };
    }

    let is_first = current_files.is_empty();
    let candidates = match commit_log_paths {
        Some(paths) => paths.creation_candidates(StoreFaultPoint::CreateSegment).ok()?,
        None => vec![PathBuf::from(store_path)],
    };
    let mut mapped_file = None;
    for root in candidates {
        let next_file_path = root.join(offset_to_file_name(create_offset));
        let created = if commit_log_paths.is_some() {
            create_mapped_file_for_queue_without_preallocation(
                allocate_mapped_file_service,
                &next_file_path,
                mapped_file_size,
                is_first,
            )
        } else {
            let next_next_file_path = root.join(offset_to_file_name(create_offset + mapped_file_size));
            create_mapped_file_for_queue(
                allocate_mapped_file_service,
                &next_file_path,
                &next_next_file_path,
                mapped_file_size,
                is_first,
            )
        };
        if let Some(created) = created {
            if let Some(paths) = commit_log_paths {
                paths.record_selected(&root);
            }
            mapped_file = Some(created);
            break;
        }
        if let Some(paths) = commit_log_paths {
            paths.mark_unhealthy(&root);
        }
    }
    let mapped_file = mapped_file?;

    mapped_files
        .publish_legacy_created_file(Arc::clone(&mapped_file))
        .then_some(mapped_file)
}

/// Narrow, cloneable capability used by background commit-log cleanup.
///
/// The capability owns only the atomically published mapped-file generation.
/// It deliberately excludes the queue's allocation and runtime state so a
/// scheduled cleanup task never needs shared access to the composition-root-owned
/// `MappedFileQueue` facade.
#[derive(Clone)]
pub(crate) struct MappedFileQueueCleanupHandle {
    mapped_files: MappedFileGeneration,
    mapped_file_size: u64,
    runtime_state: MappedFileQueueRuntimeState,
}

impl MappedFileQueueCleanupHandle {
    fn check_self(&self) {
        let mapped_files = self.mapped_files.snapshot();
        for_each_discontinuous_pair(
            mapped_files.as_ref(),
            self.mapped_file_size,
            |file| file.get_file_from_offset(),
            |previous, current| {
                error!(
                    "[BUG] The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre \
                     file {}, cur file {}",
                    mapped_files[previous].get_file_name(),
                    mapped_files[current].get_file_name()
                );
            },
        );
    }

    pub(crate) fn get_min_offset(&self) -> i64 {
        match self.mapped_files.snapshot().first() {
            None => -1,
            Some(mapped_file) if mapped_file.is_available() => mapped_file.get_file_from_offset() as i64,
            Some(mapped_file) => {
                let offset = mapped_file.get_file_from_offset() as i64;
                let mapped_file_size = self.mapped_file_size as i64;
                offset + mapped_file_size - (offset % mapped_file_size)
            }
        }
    }

    pub(crate) fn delete_expired_files_by_time_before(
        &self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
        pinned_file_offset: Option<u64>,
    ) -> i32 {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        let files = self.mapped_files.snapshot();
        self.check_self();
        if let Some((runtime, generation)) = self.mapped_files.managed_runtime_generation() {
            let candidates = select_expired_mapped_files_by_time_before(
                files.as_ref(),
                expired_time,
                clean_immediately,
                delete_file_batch_max,
                pinned_file_offset,
                || i64::try_from(current_millis()).unwrap_or(i64::MAX),
            );
            return submit_managed_retirements(&runtime, &generation, candidates, ManagedRetirementReason::TtlExpired);
        }
        let deletion = delete_expired_mapped_files_by_time_before(
            files.as_ref(),
            expired_time,
            delete_files_interval,
            interval_forcibly,
            clean_immediately,
            delete_file_batch_max,
            pinned_file_offset,
            || i64::try_from(current_millis()).unwrap_or(i64::MAX),
        );
        self.mapped_files.apply_legacy_namespace_removal(deletion)
    }

    pub(crate) fn retry_delete_first_file(&self, interval_forcibly: i64) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        if self.mapped_files.managed_generation().is_some() {
            // Managed retirement retries are owned by the durable reaper. The legacy helper has
            // no eligibility proof and therefore must never retire an arbitrary queue head.
            return false;
        }
        let first = self.mapped_files.snapshot().first().cloned();
        let deletion = retry_delete_first_mapped_file(first.as_ref(), interval_forcibly);
        let deleted = deletion.deleted_count() > 0;
        self.mapped_files.apply_legacy_namespace_removal(deletion);
        deleted
    }
}

fn submit_managed_retirements(
    runtime: &ManagedLifecycleRuntime,
    generation: &ManagedMappedFileQueueGeneration<DefaultMappedFile>,
    candidates: Vec<Arc<DefaultMappedFile>>,
    reason: ManagedRetirementReason,
) -> i32 {
    let mut submitted = 0i32;
    for owner in candidates {
        if let Err(error) = runtime.submit_retirement(generation, &owner, reason, managed_retirement_nonce()) {
            error!(%error, "managed mapped-file retirement submission failed closed");
            break;
        }
        submitted = submitted.saturating_add(1);
    }
    submitted
}

fn managed_retirement_nonce() -> [u8; 16] {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let value = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut nonce = [0u8; 16];
    nonce[..8].copy_from_slice(&value.to_le_bytes());
    nonce[8..].copy_from_slice(&current_millis().to_le_bytes());
    if nonce == [0; 16] {
        nonce[0] = 1;
    }
    nonce
}

/// Narrow, cloneable capability used by commit-log flush workers.
///
/// The handle shares only the atomically published mapped-file generation and
/// queue progress state. Allocation, recovery, authoritative replacement, and
/// destruction remain exclusively owned by `MappedFileQueue`.
#[derive(Clone)]
pub(crate) struct MappedFileQueueFlushHandle {
    mapped_files: MappedFileGeneration,
    mapped_file_size: u64,
    runtime_state: MappedFileQueueRuntimeState,
}

impl MappedFileQueueFlushHandle {
    fn find_mapped_file_by_offset(
        &self,
        offset: i64,
        return_first_on_not_found: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        let files = self.mapped_files.snapshot();
        let first = files.first()?.clone();
        let last = files.last()?.clone();
        match file_index_by_offset(
            files.as_ref(),
            self.mapped_file_size,
            offset,
            return_first_on_not_found,
            first.get_file_from_offset(),
            last.get_file_from_offset(),
            |file| file.get_file_from_offset(),
        ) {
            Some(MappedFileQueueIndex::First) => Some(first),
            Some(MappedFileQueueIndex::Indexed(index)) => Some(files[index].clone()),
            None => None,
        }
    }

    #[inline]
    fn get_max_offset(&self) -> i64 {
        let files = self.mapped_files.snapshot();
        mapped_file_queue_max_offset(files.last())
    }

    pub(crate) fn commit(&self, commit_least_pages: i32) -> bool {
        let _lock = self.runtime_state.commit_lock().lock();
        let progress = commit_mapped_file_queue(
            self.runtime_state.committed_where(),
            |offset, return_first_on_not_found| {
                self.find_mapped_file_by_offset(offset, return_first_on_not_found)
                    .map(|mapped_file| {
                        SegmentCommitProgress::new(
                            mapped_file.get_file_from_offset(),
                            mapped_file.commit(commit_least_pages),
                        )
                    })
            },
        );
        self.runtime_state.set_committed_where(progress.committed());
        progress.legacy_commit_result()
    }

    #[inline]
    pub(crate) fn get_flushed_where(&self) -> i64 {
        self.runtime_state.flushed_where()
    }

    #[inline]
    pub(crate) fn get_store_timestamp(&self) -> u64 {
        self.runtime_state.store_timestamp()
    }

    pub(crate) fn try_flush(&self, flush_least_pages: i32) -> MappedFileResult<FlushProgress> {
        let durable_before = self.runtime_state.flushed_where();
        let progress = try_flush_mapped_file_queue(
            self.get_max_offset(),
            durable_before,
            self.runtime_state.store_timestamp(),
            flush_least_pages,
            |offset, return_first_on_not_found| {
                self.find_mapped_file_by_offset(offset, return_first_on_not_found)
                    .map(|mapped_file| {
                        mapped_file.try_flush(flush_least_pages).map(|flushed_position| {
                            SegmentFlushProgress::new(
                                mapped_file.get_file_from_offset(),
                                flushed_position,
                                mapped_file.get_store_timestamp(),
                            )
                        })
                    })
                    .transpose()
            },
        )?;
        self.runtime_state.set_flushed_where(progress.durable);
        self.runtime_state.set_store_timestamp(progress.store_timestamp);
        Ok(progress)
    }
}

pub struct MappedFileQueue {
    storage: MappedFileQueueStorage<MappedFileGeneration>,

    pub(crate) allocate_mapped_file_service: Option<AllocateMappedFileService>,

    runtime_state: MappedFileQueueRuntimeState,
    commit_log_paths: Option<Arc<CommitLogPathSet>>,
}

pub use rocketmq_store_local::flush::FlushProgress;

impl Default for MappedFileQueue {
    fn default() -> Self {
        Self {
            storage: MappedFileQueueStorage::new(String::new(), 0, MappedFileGeneration::legacy()),
            allocate_mapped_file_service: None,
            runtime_state: MappedFileQueueRuntimeState::default(),
            commit_log_paths: None,
        }
    }
}

impl MappedFileQueue {
    pub(crate) fn is_multipath_commit_log(&self) -> bool {
        self.commit_log_paths.as_ref().is_some_and(|paths| paths.is_multipath())
    }

    pub(crate) fn fence_writes(&self) {
        let _guard = self.runtime_state.commit_lock().lock();
        self.runtime_state.begin_close();
    }

    pub(crate) fn is_write_fenced(&self) -> bool {
        self.runtime_state.is_closing()
    }

    pub(crate) fn runtime_state_handle(&self) -> MappedFileQueueRuntimeState {
        self.runtime_state.clone()
    }

    #[inline]
    pub fn new(
        store_path: String,
        mapped_file_size: u64,
        allocate_mapped_file_service: Option<AllocateMappedFileService>,
    ) -> MappedFileQueue {
        MappedFileQueue {
            storage: MappedFileQueueStorage::new(store_path, mapped_file_size, MappedFileGeneration::legacy()),
            allocate_mapped_file_service,
            runtime_state: MappedFileQueueRuntimeState::default(),
            commit_log_paths: None,
        }
    }

    pub(crate) fn new_commit_log(
        path_set: Arc<CommitLogPathSet>,
        mapped_file_size: u64,
        allocate_mapped_file_service: Option<AllocateMappedFileService>,
    ) -> Self {
        let primary = path_set.primary_writable_root().to_string_lossy().into_owned();
        Self {
            storage: MappedFileQueueStorage::new(primary, mapped_file_size, MappedFileGeneration::legacy()),
            allocate_mapped_file_service,
            runtime_state: MappedFileQueueRuntimeState::default(),
            commit_log_paths: Some(path_set),
        }
    }

    /// Creates an empty managed queue whose first owner must be published through the durable
    /// allocation worker.
    pub(crate) fn new_managed(
        store_path: String,
        mapped_file_size: u64,
        allocate_mapped_file_service: AllocateMappedFileService,
        runtime: ManagedLifecycleRuntime,
    ) -> Self {
        Self {
            storage: MappedFileQueueStorage::new(store_path, mapped_file_size, MappedFileGeneration::managed(runtime)),
            allocate_mapped_file_service: Some(allocate_mapped_file_service),
            runtime_state: MappedFileQueueRuntimeState::default(),
            commit_log_paths: None,
        }
    }
}

impl MappedFileQueue {
    #[inline]
    pub fn load(&mut self) -> bool {
        if !self.storage.mapped_files().allows_legacy_namespace_mutation() {
            return false;
        }
        let outcome = if let Some(paths) = self.commit_log_paths.as_ref() {
            match paths.scan_segments(self.storage.mapped_file_size()) {
                Ok(segments) => load_mapped_file_queue_files(
                    segments.into_iter().map(|segment| segment.path).collect(),
                    self.storage.mapped_file_size(),
                ),
                Err(error) => {
                    error!(%error, "multipath CommitLog inventory failed closed");
                    self.fence_writes();
                    return false;
                }
            }
        } else {
            load_mapped_file_queue_path(self.storage.store_path(), self.storage.mapped_file_size())
        };
        let loaded = self.apply_load_outcome(outcome);
        if !loaded && self.is_multipath_commit_log() {
            self.fence_writes();
        }
        loaded
    }

    /// Commit data to file system cache
    ///
    ///
    /// # Arguments
    /// * `commit_least_pages` - Minimum pages to commit
    ///
    /// # Returns
    /// Returns true if commit succeeded
    #[inline]
    pub fn commit(&self, commit_least_pages: i32) -> bool {
        self.flush_handle().commit(commit_least_pages)
    }

    #[inline]
    pub fn get_committed_where(&self) -> i64 {
        self.runtime_state.committed_where()
    }

    pub fn check_self(&self) {
        let mapped_files = self.storage.mapped_files().snapshot();
        for_each_discontinuous_pair(
            mapped_files.as_ref(),
            self.storage.mapped_file_size(),
            |file| file.get_file_from_offset(),
            |previous, current| {
                error!(
                    "[BUG] The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre \
                     file {}, cur file {}",
                    mapped_files[previous].get_file_name(),
                    mapped_files[current].get_file_name()
                );
            },
        );
    }

    pub fn do_load(&mut self, files: Vec<std::path::PathBuf>) -> bool {
        if !self.storage.mapped_files().allows_legacy_namespace_mutation() {
            return false;
        }
        let outcome = load_mapped_file_queue_files(files, self.storage.mapped_file_size());
        self.apply_load_outcome(outcome)
    }

    fn apply_load_outcome(&mut self, outcome: MappedFileQueueLoadOutcome) -> bool {
        let success = outcome.is_success();
        let loaded_files = outcome.into_mapped_files();
        self.storage
            .mapped_files()
            .extend_legacy_recovery_generation(loaded_files)
            && success
    }

    #[inline]
    pub fn get_last_mapped_file(&self) -> Option<Arc<DefaultMappedFile>> {
        self.storage.mapped_files().snapshot().last().cloned()
    }

    #[inline]
    pub fn get_first_mapped_file(&self) -> Option<Arc<DefaultMappedFile>> {
        self.storage.mapped_files().snapshot().first().cloned()
    }

    #[inline]
    pub fn get_last_mapped_file_mut_start_offset(
        &self,
        start_offset: u64,
        need_create: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        get_last_mapped_file_for_append(
            self.storage.mapped_files(),
            self.storage.store_path(),
            self.storage.mapped_file_size(),
            self.allocate_mapped_file_service.as_ref(),
            &self.runtime_state,
            self.commit_log_paths.as_ref(),
            start_offset,
            need_create,
        )
    }

    #[inline]
    pub fn try_create_mapped_file(&self, create_offset: u64) -> Option<Arc<DefaultMappedFile>> {
        create_and_publish_mapped_file(
            self.storage.mapped_files(),
            self.storage.store_path(),
            self.storage.mapped_file_size(),
            self.allocate_mapped_file_service.as_ref(),
            &self.runtime_state,
            self.commit_log_paths.as_ref(),
            create_offset,
        )
    }

    /// Replaces the complete mapped-file generation during load/recovery.
    ///
    /// The caller must have exclusive lifecycle ownership: append and cleanup
    /// publishers must not be running while this authoritative generation is
    /// installed.
    #[inline]
    pub(crate) fn replace_mapped_files_exclusive(&mut self, mapped_files: Vec<Arc<DefaultMappedFile>>) {
        let _ = self
            .storage
            .mapped_files()
            .install_legacy_recovery_generation(mapped_files);
    }

    /// Installs a proof-gated managed generation before queue workers are started.
    pub(crate) fn install_reconciled_generation(
        &self,
        generation: ManagedMappedFileQueueGeneration<DefaultMappedFile>,
    ) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        if self.runtime_state.is_closing() {
            return false;
        }
        self.storage.mapped_files().install_reconciled_generation(generation)
    }

    pub(crate) fn bind_managed_runtime(&self, runtime: ManagedLifecycleRuntime) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        if self.runtime_state.is_closing() {
            return false;
        }
        self.storage.mapped_files().bind_managed_runtime(runtime)
    }

    pub(crate) fn allows_legacy_namespace_mutation(&self) -> bool {
        self.storage.mapped_files().allows_legacy_namespace_mutation()
    }

    pub(crate) fn is_managed(&self) -> bool {
        !self.storage.mapped_files().allows_legacy_namespace_mutation()
    }

    /// Returns an immutable owner snapshot without exposing the publication primitive.
    #[inline]
    pub fn get_mapped_files(&self) -> MappedFileQueueSnapshot<DefaultMappedFile> {
        self.storage.mapped_files().snapshot()
    }

    #[inline]
    pub(crate) fn read_handle(&self) -> MappedFileQueueReadHandle {
        MappedFileQueueReadHandle {
            mapped_files: self.storage.mapped_files().clone(),
            mapped_file_size: self.storage.mapped_file_size(),
            runtime_state: self.runtime_state.clone(),
        }
    }

    #[inline]
    pub(crate) fn append_handle(&self) -> MappedFileQueueAppendHandle {
        MappedFileQueueAppendHandle {
            mapped_files: self.storage.mapped_files().clone(),
            store_path: self.storage.store_path().to_owned(),
            mapped_file_size: self.storage.mapped_file_size(),
            allocate_mapped_file_service: self.allocate_mapped_file_service.clone(),
            runtime_state: self.runtime_state.clone(),
            commit_log_paths: self.commit_log_paths.clone(),
        }
    }

    #[inline]
    pub(crate) fn cleanup_handle(&self) -> MappedFileQueueCleanupHandle {
        MappedFileQueueCleanupHandle {
            mapped_files: self.storage.mapped_files().clone(),
            mapped_file_size: self.storage.mapped_file_size(),
            runtime_state: self.runtime_state.clone(),
        }
    }

    #[inline]
    pub(crate) fn flush_handle(&self) -> MappedFileQueueFlushHandle {
        MappedFileQueueFlushHandle {
            mapped_files: self.storage.mapped_files().clone(),
            mapped_file_size: self.storage.mapped_file_size(),
            runtime_state: self.runtime_state.clone(),
        }
    }

    #[inline]
    pub fn get_mapped_files_size(&self) -> usize {
        self.storage.mapped_files().snapshot().len()
    }

    pub fn warmup_stats(&self) -> MappedFileWarmupStats {
        let mapped_files = self.storage.mapped_files().snapshot();
        mapped_file_queue_warmup_stats(mapped_files.as_ref())
    }

    /// Returns a read-only I/O counter aggregate for the current file generation.
    pub fn io_stats(&self) -> MappedFileIoStats {
        let mapped_files = self.storage.mapped_files().snapshot();
        mapped_file_queue_io_stats(mapped_files.as_ref())
    }

    pub fn selection_stats(&self) -> MappedFileSelectionStats {
        let mapped_files = self.storage.mapped_files().snapshot();
        mapped_file_queue_selection_stats(mapped_files.as_ref())
    }

    pub fn lazy_mmap_stats(&self) -> LazyMmapStats {
        let mapped_files = self.storage.mapped_files().snapshot();
        mapped_file_queue_lazy_mmap_stats(mapped_files.as_ref())
    }

    #[inline]
    pub fn set_flushed_where(&self, flushed_where: i64) {
        self.runtime_state.set_flushed_where(flushed_where);
    }

    #[inline]
    pub fn set_committed_where(&self, committed_where: i64) {
        self.runtime_state.set_committed_where(committed_where);
    }

    /// Truncate dirty files beyond the specified offset
    ///
    /// # Arguments
    /// * `offset` - The offset beyond which files are considered dirty
    #[inline]
    pub fn truncate_dirty_files(&self, offset: i64) {
        let _ = self.try_truncate_dirty_files(offset);
    }

    /// Truncates dirty data after every tail segment is safely retired.
    ///
    /// Legacy queues return success after namespace removal. Managed queues return success after
    /// every tail segment has a durable intent, has left the exact queue generation, and has a
    /// durable `LogicalRemoved` record; the owned reaper completes namespace removal.
    #[must_use]
    pub fn try_truncate_dirty_files(&self, offset: i64) -> bool {
        self.try_truncate_dirty_files_with_before_lock(offset, || {})
    }

    fn try_truncate_dirty_files_with_before_lock<F>(&self, offset: i64, before_lock: F) -> bool
    where
        F: FnOnce(),
    {
        let began_in_legacy_mode = self.allows_legacy_namespace_mutation();
        before_lock();
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        if began_in_legacy_mode && self.is_managed() {
            return false;
        }
        let mut removal_candidates = Vec::new();
        let mut position_updates = Vec::new();

        let current_files = self.storage.mapped_files().snapshot();
        for mapped_file in current_files.iter() {
            match mapped_file_queue_truncate_action(
                offset,
                self.storage.mapped_file_size(),
                mapped_file.get_file_from_offset(),
            ) {
                MappedFileQueueTruncateAction::Retain => {}
                MappedFileQueueTruncateAction::Truncate(position) => {
                    position_updates.push((Arc::clone(mapped_file), position));
                }
                MappedFileQueueTruncateAction::Remove => {
                    removal_candidates.push(mapped_file.clone());
                }
            }
        }

        if self.is_managed() && removal_candidates.is_empty() {
            for (mapped_file, position) in position_updates {
                mapped_file.set_wrote_position(position);
                mapped_file.set_committed_position(position);
                mapped_file.set_flushed_position(position);
            }
            return true;
        }
        if let Some((runtime, generation)) = self.storage.mapped_files().managed_runtime_generation() {
            let ordered_candidates: Vec<_> = removal_candidates.into_iter().rev().collect();
            let expected = ordered_candidates.len();
            let submitted = submit_managed_retirements(
                &runtime,
                &generation,
                ordered_candidates,
                ManagedRetirementReason::OffsetTruncate,
            );
            if usize::try_from(submitted).ok() != Some(expected) {
                warn!(
                    offset,
                    expected,
                    submitted,
                    "managed tail retirement remains pending; retained positions were not truncated"
                );
                return false;
            }
            for (mapped_file, position) in position_updates {
                mapped_file.set_wrote_position(position);
                mapped_file.set_committed_position(position);
                mapped_file.set_flushed_position(position);
            }
            return true;
        }
        if self.is_managed() {
            return false;
        }
        for (mapped_file, position) in position_updates {
            mapped_file.set_wrote_position(position);
            mapped_file.set_committed_position(position);
            mapped_file.set_flushed_position(position);
        }

        // Tail deletion runs newest-to-oldest. If one attempt is deferred, any already removed
        // candidates still form a contiguous suffix and the remaining identities stay tracked.
        let ordered_candidates: Vec<_> = removal_candidates.into_iter().rev().collect();
        let (deletion, complete) = destroy_mapped_files_in_order(&ordered_candidates, 1000);
        self.apply_namespace_removal_under_maintenance_lock(deletion);
        complete
    }

    #[inline]
    pub fn get_max_offset(&self) -> i64 {
        let last = self.get_last_mapped_file();
        mapped_file_queue_max_offset(last.as_ref())
    }

    #[inline]
    pub fn delete_last_mapped_file(&mut self) {
        let _ = self.try_delete_last_mapped_file();
    }

    /// Deletes the newest mapped file and reports whether its namespace was removed.
    #[must_use]
    pub fn try_delete_last_mapped_file(&mut self) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        let files = self.storage.mapped_files().snapshot();
        let Some(last_mapped_file) = files.last().cloned() else {
            return true;
        };
        if let Some((runtime, generation)) = self.storage.mapped_files().managed_runtime_generation() {
            return submit_managed_retirements(
                &runtime,
                &generation,
                vec![last_mapped_file],
                ManagedRetirementReason::DeleteLast,
            ) == 1;
        }
        if !self.allows_legacy_namespace_mutation() {
            return false;
        }
        let deletion = destroy_last_mapped_file_for_queue(files.as_ref());
        let removed = deletion.deleted_count() > 0;
        self.apply_namespace_removal_under_maintenance_lock(deletion);
        removed
    }

    #[inline]
    fn apply_namespace_removal_under_maintenance_lock(&self, deletion: MappedFileQueueDeletion) -> i32 {
        self.storage.mapped_files().apply_legacy_namespace_removal(deletion)
    }

    /// Retires an exact caller-selected legacy candidate set under the queue transition lock.
    ///
    /// This narrow adapter exists for the consume-queue extension index. It rejects stale or
    /// duplicate candidates before namespace I/O and never exposes the raw generation mutation
    /// primitive outside this module.
    pub(crate) fn try_retire_legacy_candidates(
        &self,
        candidates: &[Arc<DefaultMappedFile>],
        interval_forcibly: u64,
    ) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        if !self.allows_legacy_namespace_mutation() {
            return false;
        }
        let current = self.storage.mapped_files().snapshot();
        let is_current_prefix = candidates.len() <= current.len()
            && candidates
                .iter()
                .zip(current.iter())
                .all(|(candidate, published)| Arc::ptr_eq(candidate, published));
        if !is_current_prefix {
            return false;
        }

        let (deletion, complete) = destroy_mapped_files_in_order(candidates, interval_forcibly);
        self.apply_namespace_removal_under_maintenance_lock(deletion);
        complete
    }

    /// Delete expired files by time
    ///
    /// This is a critical method for disk space management.
    ///
    /// # Arguments
    /// * `expired_time` - Maximum time a file can exist (milliseconds)
    /// * `delete_files_interval` - Interval between deleting files (milliseconds)
    /// * `interval_forcibly` - Force deletion interval (milliseconds)
    /// * `clean_immediately` - Whether to clean immediately
    /// * `delete_file_batch_max` - Maximum files to delete in one batch
    ///
    /// # Returns
    /// Number of files deleted
    pub fn delete_expired_file_by_time(
        &self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
    ) -> i32 {
        self.delete_expired_file_by_time_before(
            expired_time,
            delete_files_interval,
            interval_forcibly,
            clean_immediately,
            delete_file_batch_max,
            None,
        )
    }

    pub fn delete_expired_file_by_time_before(
        &self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
        pinned_file_offset: Option<u64>,
    ) -> i32 {
        self.cleanup_handle().delete_expired_files_by_time_before(
            expired_time,
            delete_files_interval,
            interval_forcibly,
            clean_immediately,
            delete_file_batch_max,
            pinned_file_offset,
        )
    }

    /// Delete expired files by offset
    ///
    /// This method deletes files based on consumption progress.
    ///
    /// # Arguments
    /// * `offset` - The minimum physical offset still needed
    /// * `unit_size` - Size of each unit (for ConsumeQueue, typically CQ_STORE_UNIT_SIZE)
    ///
    /// # Returns
    /// Number of files deleted
    pub fn delete_expired_file_by_offset(&self, offset: i64, unit_size: i32) -> i32 {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        let files = self.storage.mapped_files().snapshot();
        if let Some((runtime, generation)) = self.storage.mapped_files().managed_runtime_generation() {
            let candidates = select_expired_mapped_files_by_offset(
                files.as_ref(),
                self.storage.mapped_file_size(),
                offset,
                unit_size,
            );
            return submit_managed_retirements(
                &runtime,
                &generation,
                candidates,
                ManagedRetirementReason::DerivedFileRetirement,
            );
        }
        let deletion =
            delete_expired_mapped_files_by_offset(files.as_ref(), self.storage.mapped_file_size(), offset, unit_size);
        self.apply_namespace_removal_under_maintenance_lock(deletion)
    }

    /// Reset offset to specified position
    ///
    /// Used in recovery scenarios.
    ///
    /// # Arguments
    /// * `offset` - Target offset to reset to
    ///
    /// # Returns
    /// true if reset succeeded, false if offset is too far back
    pub fn reset_offset(&self, offset: i64) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        let last_file = self.get_last_mapped_file().map(|mapped_file| {
            MappedFileQueueResetLastFile::new(mapped_file.get_file_from_offset(), mapped_file.get_wrote_position())
        });

        let current_files = self.storage.mapped_files().snapshot();
        let Some(plan) = plan_mapped_file_queue_reset(
            offset,
            self.storage.mapped_file_size(),
            last_file,
            current_files.as_ref(),
            |file| file.get_file_from_offset(),
            |file| file.get_file_size(),
        ) else {
            return false;
        };

        let removal_candidates: Vec<_> = plan
            .remove_indices()
            .iter()
            .map(|&index| Arc::clone(&current_files[index]))
            .collect();
        if let Some((runtime, generation)) = self.storage.mapped_files().managed_runtime_generation() {
            let expected = removal_candidates.len();
            let submitted = submit_managed_retirements(
                &runtime,
                &generation,
                removal_candidates,
                ManagedRetirementReason::Reset,
            );
            if usize::try_from(submitted).ok() != Some(expected) {
                return false;
            }
            if let Some((index, position)) = plan.target() {
                let mapped_file = &current_files[index];
                mapped_file.set_flushed_position(position);
                mapped_file.set_wrote_position(position);
                mapped_file.set_committed_position(position);
            }
            return true;
        }
        if !self.allows_legacy_namespace_mutation() {
            return false;
        }
        let (deletion, complete) = destroy_mapped_files_in_order(&removal_candidates, 1000);
        self.apply_namespace_removal_under_maintenance_lock(deletion);
        if !complete {
            return false;
        }

        if let Some((index, position)) = plan.target() {
            let mapped_file = &current_files[index];
            mapped_file.set_flushed_position(position);
            mapped_file.set_wrote_position(position);
            mapped_file.set_committed_position(position);
        }
        true
    }

    /// Check if mapped files list is empty
    #[inline]
    pub fn is_mapped_files_empty(&self) -> bool {
        self.storage.mapped_files().snapshot().is_empty()
    }

    /// Check if list is empty or current file is full
    #[inline]
    pub fn is_empty_or_current_file_full(&self) -> bool {
        let last = self.get_last_mapped_file();
        mapped_file_queue_should_roll(last.as_ref(), 0)
    }

    /// Check if should roll to next file
    ///
    /// # Arguments
    /// * `msg_size` - Size of message to write
    ///
    /// # Returns
    /// true if should create new file
    #[inline]
    pub fn should_roll(&self, msg_size: i32) -> bool {
        let last = self.get_last_mapped_file();
        mapped_file_queue_should_roll(last.as_ref(), msg_size)
    }

    /// Get minimum offset in the queue
    ///
    /// Returns -1 if no mapped files exist
    #[inline]
    pub fn get_min_offset(&self) -> i64 {
        let first = self.get_first_mapped_file();
        mapped_file_queue_min_offset(first.as_ref())
    }

    /// Get total mapped memory size
    ///
    /// Only counts available (not destroyed) mapped files
    #[inline]
    pub fn get_mapped_memory_size(&self) -> i64 {
        let files = self.storage.mapped_files().snapshot();
        mapped_file_queue_available_memory_size(files.as_ref(), self.storage.mapped_file_size())
    }

    /// Get how much data has fallen behind
    ///
    /// Returns the difference between max wrote position and flushed position
    #[inline]
    pub fn how_much_fall_behind(&self) -> i64 {
        let last = self.get_last_mapped_file();
        mapped_file_queue_fall_behind(last.as_ref(), self.get_flushed_where())
    }

    /// Gracefully shutdown all mapped files
    ///
    /// # Arguments
    /// * `interval_forcibly` - Force shutdown interval (milliseconds)
    pub fn shutdown(&self, interval_forcibly: u64) {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        self.runtime_state.begin_close();
        if let Some(service) = &self.allocate_mapped_file_service {
            let _ = service.retire_directory(Path::new(self.storage.store_path()));
        }
        let files = self.storage.mapped_files().snapshot();
        shutdown_mapped_file_queue(files.as_ref(), interval_forcibly);
    }

    /// Create a snapshot of current mapped files
    ///
    /// Returns a safe copy of all mapped files that can be iterated
    /// without holding the lock.
    ///
    /// # Returns
    /// Vector of Arc-wrapped mapped files
    ///
    /// # Example
    /// ```ignore
    /// let snapshot = queue.snapshot();
    /// for file in snapshot {
    ///     // Process each file without holding locks
    /// }
    /// ```
    #[inline]
    pub fn snapshot(&self) -> Vec<Arc<DefaultMappedFile>> {
        self.storage.mapped_files().snapshot().as_ref().to_vec()
    }

    /// Create an iterator over mapped files
    ///
    /// # Returns
    /// Iterator that yields Arc<DefaultMappedFile>
    ///
    /// # Example
    /// ```ignore
    /// for file in queue.iter() {
    ///     println!("File: {}", file.get_file_name());
    /// }
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = Arc<DefaultMappedFile>> {
        self.snapshot().into_iter()
    }

    /// Create a reversed iterator over mapped files
    ///
    /// Iterates from newest to oldest file.
    ///
    /// # Returns
    /// Reversed iterator that yields Arc<DefaultMappedFile>
    ///
    /// # Example
    /// ```ignore
    /// for file in queue.iter_reversed() {
    ///     println!("File (reversed): {}", file.get_file_name());
    /// }
    /// ```
    pub fn iter_reversed(&self) -> impl Iterator<Item = Arc<DefaultMappedFile>> {
        let mut snapshot = self.snapshot();
        snapshot.reverse();
        snapshot.into_iter()
    }

    /// Get mapped files within a specific offset range
    ///
    /// Returns files whose range [fileFromOffset, fileFromOffset + fileSize)
    /// intersects with the requested range [from, to).
    ///
    /// # Arguments
    /// * `from` - Start offset (inclusive)
    /// * `to` - End offset (exclusive)
    ///
    /// # Returns
    /// Vector of mapped files in the range
    ///
    /// # Example
    /// ```ignore
    /// let files = queue.range(1024, 4096);
    /// for file in files {
    ///     // Process files that overlap with [1024, 4096)
    /// }
    /// ```
    pub fn range(&self, from: i64, to: i64) -> Vec<Arc<DefaultMappedFile>> {
        let files = self.storage.mapped_files().snapshot();
        let range = overlapping_file_range(files.as_ref(), from, to, |file| {
            let file_from = file.get_file_from_offset() as i64;
            (file_from, file_from + file.get_file_size() as i64)
        });
        files[range].to_vec()
    }

    /// Get total file size (all mapped files combined)
    ///
    /// # Returns
    /// Total size in bytes
    #[inline]
    pub fn get_total_file_size(&self) -> i64 {
        let files = self.storage.mapped_files().snapshot();
        mapped_file_queue_total_size(files.len(), self.storage.mapped_file_size())
    }

    /// Get the number of mapped files
    ///
    /// # Returns
    /// Count of mapped files in the queue
    #[inline]
    pub fn get_mapped_file_count(&self) -> usize {
        self.storage.mapped_files().snapshot().len()
    }

    /// Get store path
    ///
    /// # Returns
    /// The storage directory path
    #[inline]
    pub fn get_store_path(&self) -> &str {
        self.storage.store_path()
    }

    /// Get mapped file size
    ///
    /// # Returns
    /// Size of each mapped file in bytes
    #[inline]
    pub fn get_mapped_file_size_config(&self) -> u64 {
        self.storage.mapped_file_size()
    }

    /// Retry deleting the first file
    ///
    /// Attempts to delete the first file if it's not available.
    /// This is used for cleanup when a file is destroyed but still in the list.
    ///
    /// # Arguments
    /// * `interval_forcibly` - Force deletion timeout (milliseconds)
    ///
    /// # Returns
    /// true if deletion succeeded, false otherwise
    pub fn retry_delete_first_file(&self, interval_forcibly: i64) -> bool {
        self.cleanup_handle().retry_delete_first_file(interval_forcibly)
    }

    /// Records legacy mapped-buffer swap eligibility.
    ///
    /// The current compatibility implementation updates swap bookkeeping only. It does not unmap
    /// buffers or reduce resident memory; physical generation replacement is handled separately.
    ///
    /// # Arguments
    /// * `reserve_num` - Number of recent files to keep in memory (minimum 3)
    /// * `force_swap_interval_ms` - Force swap after this interval (milliseconds)
    /// * `normal_swap_interval_ms` - Normal swap interval if file was accessed
    ///
    /// # Note
    /// - Only files older than `reserve_num` are candidates for swapping
    /// - Files are swapped if they exceed the swap interval
    ///
    /// # Example
    /// ```ignore
    /// // Swap buffers for files older than 10 minutes
    /// queue.swap_map(3, 1000 * 60 * 10, 1000 * 60 * 5);
    /// ```
    pub fn swap_map(&self, reserve_num: i32, force_swap_interval_ms: i64, normal_swap_interval_ms: i64) {
        let files = self.storage.mapped_files().snapshot();
        swap_mapped_file_queue(
            files.as_ref(),
            reserve_num,
            force_swap_interval_ms,
            normal_swap_interval_ms,
            || i64::try_from(current_millis()).unwrap_or(i64::MAX),
        );
    }

    /// Records compatibility cleanup for previously selected swap candidates.
    ///
    /// This operation only updates cleanup metrics and access bookkeeping. It does not reclaim an
    /// mmap or file owner.
    ///
    /// # Arguments
    /// * `force_clean_swap_interval_ms` - Clean buffers swapped longer than this
    ///
    /// # Note
    /// - Keeps the 3 most recent files from cleaning
    /// - Only cleans buffers that have been swapped for a long time
    ///
    /// # Example
    /// ```ignore
    /// // Clean buffers swapped for more than 1 hour
    /// queue.clean_swapped_map(1000 * 60 * 60);
    /// ```
    pub fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        let files = self.storage.mapped_files().snapshot();
        clean_swapped_mapped_file_queue(files.as_ref(), force_clean_swap_interval_ms, || {
            i64::try_from(current_millis()).unwrap_or(i64::MAX)
        });
    }

    /// Get mapped file by timestamp
    ///
    /// Finds the mapped file that contains data for the given timestamp.
    /// This is a simple lookup based on file modification time.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to search for (milliseconds)
    ///
    /// # Returns
    /// The mapped file containing data for that timestamp, or the last file
    pub fn get_mapped_file_by_time(&self, timestamp: i64) -> Option<Arc<DefaultMappedFile>> {
        let files = self.snapshot();
        file_index_by_timestamp(&files, timestamp, |file| {
            i64::try_from(file.get_last_modified_timestamp()).unwrap_or(i64::MAX)
        })
        .map(|index| files[index].clone())
    }

    // ============ Additional Helper Methods ============

    /// Copy mapped files to array
    ///
    /// Internal method to create a snapshot of files, optionally reserving
    /// some files from the copy.
    ///
    /// # Arguments
    /// * `reserved_mapped_files` - Number of files to exclude from the end
    ///
    /// # Returns
    /// Vector of mapped files, or None if insufficient files
    fn copy_mapped_files(&self, reserved_mapped_files: usize) -> Option<Vec<Arc<DefaultMappedFile>>> {
        let files = self.storage.mapped_files().snapshot();

        if files.len() <= reserved_mapped_files {
            return None;
        }

        Some(files.as_ref().to_vec())
    }

    /// Stream access to mapped files
    ///
    /// Returns an iterator that can be used with standard iterator combinators.
    #[inline]
    pub fn stream(&self) -> impl Iterator<Item = Arc<DefaultMappedFile>> {
        self.iter()
    }

    /// Destroy all mapped files and cleanup resources
    ///
    /// This method destroys all mapped files and removes the storage directory.
    /// It should only be called during shutdown or cleanup operations.
    #[inline]
    pub fn destroy(&mut self) {
        let _ = self.destroy_with_outcome();
    }

    /// Begins permanent queue retirement and reports whether every tracked path was removed.
    ///
    /// A failed attempt leaves the queue closed to new appends while retaining the remaining
    /// identities for a later retry.
    #[inline]
    pub fn destroy_with_outcome(&mut self) -> bool {
        let _maintenance_guard = self.runtime_state.commit_lock().lock();
        self.runtime_state.begin_close();
        if !self.allows_legacy_namespace_mutation() {
            return false;
        }
        let store_path = PathBuf::from(self.storage.store_path());
        let roots = self
            .commit_log_paths
            .as_ref()
            .map(|paths| paths.roots().to_vec())
            .unwrap_or_else(|| vec![store_path.clone()]);
        if let Some(service) = &self.allocate_mapped_file_service {
            for root in &roots {
                let _ = service.retire_directory(root);
            }
        }
        let files = self.storage.mapped_files().snapshot();
        let deletion = destroy_mapped_file_queue(files.as_ref(), self.storage.store_path());
        self.apply_namespace_removal_under_maintenance_lock(deletion);
        for root in roots.iter().filter(|root| **root != store_path) {
            if std::fs::read_dir(root).is_ok_and(|mut entries| entries.next().is_none()) {
                let _ = std::fs::remove_dir(root);
            }
        }
        let allocator_retirement_complete = self
            .allocate_mapped_file_service
            .as_ref()
            .is_none_or(|service| roots.iter().all(|root| service.is_directory_retirement_complete(root)));
        let namespace_absent = roots
            .iter()
            .all(|root| matches!(std::fs::metadata(root), Err(error) if error.kind() == io::ErrorKind::NotFound));
        let destroyed = self.is_mapped_files_empty() && allocator_retirement_complete && namespace_absent;
        if destroyed {
            self.set_flushed_where(0);
            if let Some(service) = &self.allocate_mapped_file_service {
                for root in &roots {
                    let _ = service.complete_directory_retirement(root);
                }
            }
        }
        destroyed
    }

    /// Find mapped file by offset
    ///
    /// Locates the mapped file containing the specified offset.
    /// Uses direct indexing for O(1) lookup when possible.
    ///
    /// # Arguments
    /// * `offset` - The offset to search for
    /// * `return_first_on_not_found` - Return first file if offset not found
    ///
    /// # Returns
    /// The mapped file containing the offset, or None
    pub fn find_mapped_file_by_offset(
        &self,
        offset: i64,
        return_first_on_not_found: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        let first = self.get_first_mapped_file()?;
        let last = self.get_last_mapped_file()?;
        let files = self.storage.mapped_files().snapshot();
        match file_index_by_offset(
            files.as_ref(),
            self.storage.mapped_file_size(),
            offset,
            return_first_on_not_found,
            first.get_file_from_offset(),
            last.get_file_from_offset(),
            |file| file.get_file_from_offset(),
        ) {
            Some(MappedFileQueueIndex::First) => Some(first),
            Some(MappedFileQueueIndex::Indexed(index)) => Some(files[index].clone()),
            None => None,
        }
    }

    #[inline]
    pub fn get_flushed_where(&self) -> i64 {
        self.runtime_state.flushed_where()
    }

    #[inline]
    pub fn set_store_timestamp(&self, store_timestamp: u64) {
        self.runtime_state.set_store_timestamp(store_timestamp);
    }

    #[inline]
    pub fn get_store_timestamp(&self) -> u64 {
        self.runtime_state.store_timestamp()
    }

    #[inline]
    pub fn flush(&self, flush_least_pages: i32) -> bool {
        match self.try_flush(flush_least_pages) {
            Ok(progress) => progress.durable == progress.durable_before,
            Err(error) => {
                error!(error = %error, "failed to flush mapped file queue");
                true
            }
        }
    }

    pub fn try_flush(&self, flush_least_pages: i32) -> MappedFileResult<FlushProgress> {
        self.flush_handle().try_flush(flush_least_pages)
    }

    #[inline]
    pub fn remain_how_many_data_to_commit(&self) -> i64 {
        self.get_max_wrote_position() - self.get_committed_where()
    }

    #[inline]
    pub fn remain_how_many_data_to_flush(&self) -> i64 {
        self.get_max_offset() - self.get_flushed_where()
    }
    #[inline]
    fn get_max_wrote_position(&self) -> i64 {
        let last = self.get_last_mapped_file();
        mapped_file_queue_max_wrote_position(last.as_ref())
    }

    /// Gets a consume queue mapped file by timestamp.
    ///
    /// This method finds the appropriate mapped file based on the given timestamp
    /// and boundary type. It ensures each mapped file in the consume queue has
    /// accurate start and stop timestamps based on the commit log.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp to search for
    /// * `commit_log` - Reference to the commit log for looking up message store times
    /// * `boundary_type` - The boundary type (Lower or Upper)
    ///
    /// # Returns
    ///
    /// The mapped file that matches the criteria, or None if not found
    pub(crate) fn get_consume_queue_mapped_file_by_time(
        &self,
        timestamp: i64,
        commit_log: &CommitLogReadHandle,
        boundary_type: BoundaryType,
    ) -> Option<Arc<DefaultMappedFile>> {
        let mapped_files = self.storage.mapped_files().snapshot();
        if mapped_files.is_empty() {
            return None;
        }

        let mfs: Vec<Arc<DefaultMappedFile>> = mapped_files.iter().cloned().collect();

        let mfs_len = mfs.len();

        for i in (0..mfs_len).rev() {
            let mapped_file = &mfs[i];

            if mapped_file.get_start_timestamp() < 0 {
                if let Some(select_result) = mapped_file.select_mapped_buffer(0, CQ_STORE_UNIT_SIZE) {
                    if let Some(buffer) = select_result.get_bytes_ref() {
                        if buffer.len() >= 12 {
                            let physical_offset = i64::from_be_bytes(buffer[0..8].try_into().unwrap());
                            let message_size = i32::from_be_bytes(buffer[8..12].try_into().unwrap());
                            let message_store_time = commit_log.pickup_store_timestamp(physical_offset, message_size);
                            if message_store_time > 0 {
                                mapped_file.set_start_timestamp(message_store_time);
                            }
                        }
                    }
                }
            }

            if i < mfs_len - 1 && mapped_file.get_stop_timestamp() < 0 {
                let last_unit_offset = self.storage.mapped_file_size() as i32 - CQ_STORE_UNIT_SIZE;
                if let Some(select_result) = mapped_file.select_mapped_buffer(last_unit_offset, CQ_STORE_UNIT_SIZE) {
                    if let Some(buffer) = select_result.get_bytes_ref() {
                        if buffer.len() >= 12 {
                            let physical_offset = i64::from_be_bytes(buffer[0..8].try_into().unwrap());
                            let message_size = i32::from_be_bytes(buffer[8..12].try_into().unwrap());
                            let message_store_time = commit_log.pickup_store_timestamp(physical_offset, message_size);
                            if message_store_time > 0 {
                                mapped_file.set_stop_timestamp(message_store_time);
                            }
                        }
                    }
                }
            }
        }

        match boundary_type {
            BoundaryType::Lower => {
                for (i, mapped_file) in mfs.iter().enumerate() {
                    if i < mfs_len - 1 {
                        if mapped_file.get_stop_timestamp() >= timestamp {
                            return Some(mapped_file.clone());
                        }
                    } else {
                        return Some(mapped_file.clone());
                    }
                }
            }
            BoundaryType::Upper => {
                for mapped_file in mfs.iter().rev() {
                    if mapped_file.get_start_timestamp() <= timestamp {
                        return Some(mapped_file.clone());
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::thread;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn test_load_empty_dir() {
        let mut queue = MappedFileQueue::new(String::from("/path/to/empty/dir"), 0, None);
        assert!(queue.load());
        assert!(queue.storage.mapped_files().snapshot().is_empty());
    }

    #[test]
    fn test_load_with_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file1_path = temp_dir.path().join("1111");
        let file2_path = temp_dir.path().join("2222");
        fs::File::create(&file1_path).unwrap();
        fs::File::create(&file2_path).unwrap();

        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 0, None);
        assert!(queue.load());
        assert_eq!(queue.storage.mapped_files().snapshot().len(), 1);
    }

    #[test]
    fn test_load_with_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("1111");
        fs::File::create(&file_path).unwrap();

        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 0, None);
        assert!(queue.load());
        assert!(queue.storage.mapped_files().snapshot().is_empty());
    }

    #[test]
    fn test_load_with_invalid_file_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("invalid_file.txt");
        fs::write(&file_path, "Some data").unwrap();

        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 0, None);
        assert!(!queue.load());
        assert!(queue.storage.mapped_files().snapshot().is_empty());
    }

    #[test]
    fn do_load_applies_files_loaded_before_failure() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let valid_path = temp_dir.path().join("00000000000000000000");
        let invalid_path = temp_dir.path().join("00000000000000000016");
        fs::File::create(&valid_path)
            .expect("valid file")
            .set_len(16)
            .expect("valid size");
        fs::File::create(&invalid_path)
            .expect("invalid file")
            .set_len(8)
            .expect("invalid size");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 16, None);

        assert!(!queue.do_load(vec![invalid_path, valid_path]));
        let files = queue.storage.mapped_files().snapshot();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].get_file_from_offset(), 0);
    }

    #[test]
    fn test_load_with_correct_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("1111");
        fs::write(&file_path, vec![0u8; 1024]).unwrap();

        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        assert!(queue.load());
        assert_eq!(queue.storage.mapped_files().snapshot().len(), 1);
    }

    #[test]
    fn warmup_stats_aggregates_mapped_file_metrics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let first_path = temp_dir.path().join(offset_to_file_name(0));
        let second_path = temp_dir.path().join(offset_to_file_name(1024));
        let first_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(first_path.to_string_lossy().to_string()),
                1024,
            )
            .expect("first mapped file"),
        );
        let second_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(second_path.to_string_lossy().to_string()),
                1024,
            )
            .expect("second mapped file"),
        );
        first_file
            .get_metrics()
            .unwrap()
            .record_warm_with_latency(1024, std::time::Duration::from_millis(4));
        second_file
            .get_metrics()
            .unwrap()
            .record_warm_with_latency(2048, std::time::Duration::from_millis(5));

        let queue = MappedFileQueue {
            storage: MappedFileQueueStorage::new(
                String::new(),
                1024,
                MappedFileGeneration::from_legacy_recovery_files(vec![first_file, second_file]),
            ),
            ..MappedFileQueue::default()
        };

        let stats = queue.warmup_stats();
        assert_eq!(stats.operations, 2);
        assert_eq!(stats.bytes, 3072);
        assert_eq!(stats.total_millis, 9);
        assert_eq!(stats.last_millis, 5);
    }

    #[test]
    fn io_stats_aggregate_mapped_file_metrics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let first_path = temp_dir.path().join(offset_to_file_name(0));
        let second_path = temp_dir.path().join(offset_to_file_name(1024));
        let first_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(first_path.to_string_lossy().to_string()),
                1024,
            )
            .expect("first mapped file"),
        );
        let second_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(second_path.to_string_lossy().to_string()),
                1024,
            )
            .expect("second mapped file"),
        );
        first_file.get_metrics().unwrap().record_write(128);
        first_file.get_metrics().unwrap().record_read(64, true);
        first_file
            .get_metrics()
            .unwrap()
            .record_flush(std::time::Duration::from_micros(10));
        second_file.get_metrics().unwrap().record_write(256);
        second_file.get_metrics().unwrap().record_read(96, false);
        second_file
            .get_metrics()
            .unwrap()
            .record_flush(std::time::Duration::from_micros(20));

        let queue = MappedFileQueue {
            storage: MappedFileQueueStorage::new(
                String::new(),
                1024,
                MappedFileGeneration::from_legacy_recovery_files(vec![first_file, second_file]),
            ),
            ..MappedFileQueue::default()
        };

        assert_eq!(
            queue.io_stats(),
            MappedFileIoStats {
                write_operations: 2,
                bytes_written: 384,
                flush_operations: 2,
                read_operations: 2,
                bytes_read: 160,
            }
        );
    }

    #[tokio::test]
    async fn trigger_pre_allocation_submits_background_request() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mapped_file_size = 1024;
        let first_file_path = temp_dir.path().join(offset_to_file_name(0));
        let next_file_path = temp_dir.path().join(offset_to_file_name(mapped_file_size));
        let first_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(first_file_path.to_string_lossy().to_string()),
                mapped_file_size,
            )
            .expect("first mapped file"),
        );
        first_file.set_wrote_position(820);

        let service = AllocateMappedFileService::new_with_config(
            None,
            false,
            false,
            crate::runtime::test_scope("mapped-file-queue-preallocation-test").mapped_file_allocation_budget(),
        );
        service.start();
        let mut queue = MappedFileQueue::new(
            temp_dir.path().to_string_lossy().to_string(),
            mapped_file_size,
            Some(service.clone()),
        );
        queue.replace_mapped_files_exclusive(vec![first_file]);

        assert!(queue.get_last_mapped_file_mut_start_offset(0, true).is_some());
        let preallocated = service
            .allocate_mapped_file_blocking(next_file_path.to_string_lossy().into_owned(), mapped_file_size)
            .expect("background preallocation completes");
        assert_eq!(preallocated.get_file_from_offset(), mapped_file_size);
        assert!(next_file_path.exists());

        service.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preallocation_rechecks_closing_under_the_maintenance_lock() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mapped_file_size = 1024;
        let next_file_path = temp_dir.path().join(offset_to_file_name(mapped_file_size));
        let service = AllocateMappedFileService::new_with_config(
            None,
            false,
            false,
            crate::runtime::test_scope("mapped-file-preallocation-close-race-test").mapped_file_allocation_budget(),
        );
        service.start();
        let runtime_state = MappedFileQueueRuntimeState::default();
        let mapped_files = MappedFileGeneration::legacy();
        let maintenance_guard = runtime_state.commit_lock().lock();
        let thread_state = runtime_state.clone();
        let thread_mapped_files = mapped_files.clone();
        let thread_service = service.clone();
        let thread_store_path = temp_dir.path().to_string_lossy().into_owned();
        let barrier = Arc::new(Barrier::new(2));
        let thread_barrier = Arc::clone(&barrier);

        let submitter = thread::spawn(move || {
            thread_barrier.wait();
            trigger_mapped_file_preallocation(
                &thread_mapped_files,
                &thread_store_path,
                mapped_file_size,
                Some(&thread_service),
                &thread_state,
                None,
                mapped_file_size,
            );
        });
        barrier.wait();
        assert!(runtime_state.begin_close());
        drop(maintenance_guard);
        submitter.join().expect("preallocation submitter");

        let snapshot = service.queue_snapshot();
        assert_eq!(snapshot.current_count, 0);
        assert_eq!(snapshot.pending_cleanup_count, 0);
        assert!(!next_file_path.exists());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn queue_retirement_cancels_and_drains_background_preallocation() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mapped_file_size = 1024;
        let first_file_path = temp_dir.path().join(offset_to_file_name(0));
        let next_file_path = temp_dir.path().join(offset_to_file_name(mapped_file_size));
        let first_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(first_file_path.to_string_lossy().into_owned()),
                mapped_file_size,
            )
            .expect("first mapped file"),
        );
        first_file.set_wrote_position(820);

        let runtime_scope = crate::runtime::test_scope("mapped-file-queue-retirement-test");
        let service = AllocateMappedFileService::new_with_config_and_storage_io(
            None,
            false,
            false,
            runtime_scope.mapped_file_allocation_budget(),
            runtime_scope.storage_io(),
        );
        service.start();
        let mut queue = MappedFileQueue::new(
            temp_dir.path().to_string_lossy().into_owned(),
            mapped_file_size,
            Some(service.clone()),
        );
        queue.replace_mapped_files_exclusive(vec![first_file]);

        assert!(queue.get_last_mapped_file_mut_start_offset(0, true).is_some());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !next_file_path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background preallocation should materialize");

        let _ = queue.destroy_with_outcome();
        service.shutdown().await;
        assert!(queue.destroy_with_outcome());
        assert!(!first_file_path.exists());
        assert!(!next_file_path.exists());
        assert!(!temp_dir.path().exists());
    }

    #[test]
    fn prebuilt_handles_observe_an_exclusive_queue_generation_switch() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let file_path = temp_dir.path().join(offset_to_file_name(0));
        let mapped_file = Arc::new(
            DefaultMappedFile::try_new(
                CheetahString::from_string(file_path.to_string_lossy().into_owned()),
                1024,
            )
            .expect("mapped file"),
        );
        let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let read_handle = queue.read_handle();
        let append_handle = queue.append_handle();
        mapped_file.set_wrote_position(512);

        queue
            .storage
            .mapped_files()
            .state
            .store(Arc::new(MappedFileGenerationState::Legacy(
                MappedFileQueueGeneration::from_recovery_files(vec![Arc::clone(&mapped_file)]),
            )));

        assert_eq!(read_handle.get_max_offset(), mapped_file.get_read_position() as i64);
        assert_eq!(append_handle.current_append_offset(), 512);
        assert!(Arc::ptr_eq(
            queue.get_mapped_files().first().expect("published file"),
            &mapped_file
        ));
    }

    #[test]
    fn queued_legacy_creation_rechecks_managed_mode_before_namespace_io() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mapped_file_size = 1024;
        let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), mapped_file_size, None);
        let mapped_files = queue.storage.mapped_files().clone();
        let runtime_state = queue.runtime_state.clone();
        let store_path = queue.storage.store_path().to_owned();
        let expected_path = temp_dir.path().join(offset_to_file_name(0));
        let (preflight_done_tx, preflight_done_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();

        let creator = thread::spawn(move || {
            create_and_publish_mapped_file_with_before_lock(
                &mapped_files,
                &store_path,
                mapped_file_size,
                None,
                &runtime_state,
                None,
                0,
                || {
                    preflight_done_tx.send(()).expect("signal legacy preflight");
                    resume_rx.recv().expect("resume after managed publication");
                },
            )
        });
        preflight_done_rx.recv().expect("creator observed legacy mode");

        let managed =
            rocketmq_store_local::mapped_file::queue_io::load_reconciled_mapped_file_queue(temp_dir.path(), Vec::new())
                .expect("construct an empty reconciled generation");
        assert!(queue.install_reconciled_generation(managed));
        resume_tx.send(()).expect("release queued creator");

        assert!(creator.join().expect("creator completes").is_none());
        assert!(
            !expected_path.exists(),
            "managed activation must precede every later namespace write"
        );
        assert!(queue.get_mapped_files().is_empty());
    }

    #[test]
    fn mapped_file_snapshot_retains_its_generation_during_concurrent_publication() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mapped_file_size = 1024;
        let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), mapped_file_size, None);
        let first = queue.try_create_mapped_file(0).expect("first mapped file");
        let second = queue
            .try_create_mapped_file(mapped_file_size)
            .expect("second mapped file");
        let queue = Arc::new(queue);
        let observed = queue.get_mapped_files();
        let publisher = Arc::clone(&queue);
        let tail = thread::spawn(move || {
            publisher
                .try_create_mapped_file(mapped_file_size * 2)
                .expect("tail mapped file")
        })
        .join()
        .expect("publisher completes");

        assert_eq!(observed.len(), 2);
        assert!(Arc::ptr_eq(&observed[0], &first));
        assert!(Arc::ptr_eq(&observed[1], &second));
        let current = queue.get_mapped_files();
        assert_eq!(current.len(), 3);
        assert!(Arc::ptr_eq(&current[2], &tail));
        drop((observed, current, first, second, tail));

        let mut queue = Arc::try_unwrap(queue).unwrap_or_else(|_| panic!("release shared queue handles"));
        queue.destroy();
    }

    #[test]
    fn concurrent_mapped_file_creation_reuses_published_file() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let queue = Arc::new(MappedFileQueue::new(
            temp_dir.path().to_string_lossy().into_owned(),
            1024,
            None,
        ));
        let start = Arc::new(Barrier::new(3));

        let first_queue = Arc::clone(&queue);
        let first_start = Arc::clone(&start);
        let first = thread::spawn(move || {
            first_start.wait();
            first_queue.try_create_mapped_file(0).expect("first mapped file")
        });

        let second_queue = Arc::clone(&queue);
        let second_start = Arc::clone(&start);
        let second = thread::spawn(move || {
            second_start.wait();
            second_queue.try_create_mapped_file(0).expect("second mapped file")
        });

        start.wait();
        let first = first.join().expect("first creator completes");
        let second = second.join().expect("second creator completes");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(queue.get_mapped_file_count(), 1);

        drop(first);
        drop(second);
        let mut queue = match Arc::try_unwrap(queue) {
            Ok(queue) => queue,
            Err(_) => panic!("creator threads released the queue"),
        };
        queue.destroy();
    }

    #[test]
    fn cloned_append_handle_cannot_publish_after_queue_retirement_begins() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let first = queue.try_create_mapped_file(0).expect("first mapped file");
        let append_handle = queue.append_handle();
        assert!(first.hold());

        assert!(!queue.destroy_with_outcome());
        assert!(append_handle.get_last_mapped_file(1024, true).is_none());
        assert_eq!(queue.get_mapped_file_count(), 1);

        first.release();
        assert!(queue.destroy_with_outcome());
    }

    #[test]
    fn empty_queue_shutdown_closes_all_append_and_create_handles() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let append_handle = queue.append_handle();

        queue.shutdown(0);

        assert!(queue.try_create_mapped_file(0).is_none());
        assert!(append_handle.get_last_mapped_file(0, true).is_none());
        assert_eq!(queue.get_mapped_file_count(), 0);
    }

    #[test]
    fn flush_handle_observes_queue_generation_and_shared_progress() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let flush_handle = queue.flush_handle();

        queue.set_flushed_where(17);
        queue.set_store_timestamp(23);
        assert_eq!(flush_handle.get_flushed_where(), 17);
        assert_eq!(flush_handle.get_store_timestamp(), 23);

        let mapped_file = queue.try_create_mapped_file(0).expect("create mapped file");
        assert!(mapped_file.append_message_bytes(b"flush-handle"));
        assert_eq!(flush_handle.get_max_offset(), 12);

        drop(flush_handle);
        queue.destroy();
    }

    #[test]
    fn read_handle_observes_queue_generation_and_shared_progress() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let read_handle = queue.read_handle();

        assert_eq!(read_handle.get_min_offset(), -1);
        queue.set_flushed_where(7);
        let mapped_file = queue.try_create_mapped_file(0).expect("create mapped file");
        assert!(mapped_file.append_message_bytes(b"read-handle"));

        assert_eq!(read_handle.get_min_offset(), 0);
        assert_eq!(read_handle.get_max_offset(), 11);
        assert_eq!(read_handle.get_flushed_where(), 7);
        assert_eq!(read_handle.get_data(0).expect("read mapped data").size(), 11);
        assert_eq!(read_handle.get_message(0, 4).expect("read message slice").size(), 4);
        assert_eq!(read_handle.roll_next_file(12), 1024);

        drop(read_handle);
        queue.destroy();
    }

    #[test]
    fn try_flush_reports_appended_and_durable_watermarks_for_empty_queue() {
        let queue = MappedFileQueue::default();

        let progress = queue.try_flush(0).expect("empty queue flush should succeed");

        assert_eq!(progress.appended, 0);
        assert_eq!(progress.durable_before, 0);
        assert_eq!(progress.durable, 0);
        assert_eq!(progress.store_timestamp, 0);
    }

    #[test]
    fn truncate_dirty_files_delegates_positions_and_removals_to_local_plan() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), 1024, None);
        let first = queue.try_create_mapped_file(0).expect("first mapped file");
        let target = queue.try_create_mapped_file(1024).expect("target mapped file");
        let removed = queue.try_create_mapped_file(2048).expect("removed mapped file");
        for file in [&first, &target, &removed] {
            file.set_wrote_position(1024);
            file.set_committed_position(1024);
            file.set_flushed_position(1024);
        }

        let shared_queue = &queue;
        shared_queue.truncate_dirty_files(1536);

        assert_eq!(queue.get_mapped_file_count(), 2);
        assert_eq!(first.get_wrote_position(), 1024);
        assert_eq!(target.get_wrote_position(), 512);
        assert_eq!(target.get_committed_position(), 512);
        assert_eq!(target.get_flushed_position(), 512);
        assert!(!removed.is_available());
        queue.destroy();
    }

    #[test]
    fn queued_legacy_truncation_rechecks_managed_mode_before_namespace_io() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mapped_file_size = 1024;
        let queue = Arc::new(MappedFileQueue::new(
            temp_dir.path().to_string_lossy().into_owned(),
            mapped_file_size,
            None,
        ));
        let first = queue.try_create_mapped_file(0).expect("first mapped file");
        let tail = queue
            .try_create_mapped_file(mapped_file_size)
            .expect("tail mapped file");
        let first_path = PathBuf::from(first.get_file_name().as_str());
        let tail_path = PathBuf::from(tail.get_file_name().as_str());
        let (preflight_done_tx, preflight_done_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();

        let truncating_queue = Arc::clone(&queue);
        let truncation = thread::spawn(move || {
            truncating_queue.try_truncate_dirty_files_with_before_lock(512, || {
                preflight_done_tx.send(()).expect("signal legacy preflight");
                resume_rx.recv().expect("resume after managed publication");
            })
        });
        preflight_done_rx.recv().expect("truncation observed legacy mode");

        let managed =
            rocketmq_store_local::mapped_file::queue_io::load_reconciled_mapped_file_queue(temp_dir.path(), Vec::new())
                .expect("construct an empty reconciled generation");
        assert!(queue.install_reconciled_generation(managed));
        resume_tx.send(()).expect("release queued truncation");

        assert!(!truncation.join().expect("truncation completes"));
        assert!(first_path.exists());
        assert!(tail_path.exists());
        assert!(queue.get_mapped_files().is_empty());
    }

    #[test]
    fn empty_managed_queue_accepts_non_destructive_recovery_position_reset() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let managed =
            rocketmq_store_local::mapped_file::queue_io::load_reconciled_mapped_file_queue(temp_dir.path(), Vec::new())
                .expect("construct an empty reconciled generation");
        assert!(queue.install_reconciled_generation(managed));

        assert!(queue.try_truncate_dirty_files(0));
        assert!(queue.is_managed());
        assert!(queue.get_mapped_files().is_empty());
    }

    #[test]
    fn legacy_candidate_retirement_rejects_non_prefix_without_namespace_io() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().into_owned(), 1024, None);
        let first = queue.try_create_mapped_file(0).expect("first mapped file");
        let second = queue.try_create_mapped_file(1024).expect("second mapped file");
        let first_path = PathBuf::from(first.get_file_name().as_str());
        let second_path = PathBuf::from(second.get_file_name().as_str());

        assert!(!queue.try_retire_legacy_candidates(std::slice::from_ref(&second), 1000));
        assert!(first_path.exists());
        assert!(second_path.exists());
        assert_eq!(queue.get_mapped_file_count(), 2);

        drop((first, second));
        queue.destroy();
    }

    #[test]
    fn reset_offset_delegates_target_and_removal_order_to_local_plan() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), 1024, None);
        let first = queue.try_create_mapped_file(0).expect("first mapped file");
        let target = queue.try_create_mapped_file(1024).expect("target mapped file");
        let removed = queue.try_create_mapped_file(2048).expect("removed mapped file");

        let shared_queue = &queue;
        assert!(shared_queue.reset_offset(1536));

        assert_eq!(queue.get_mapped_file_count(), 2);
        assert!(Arc::ptr_eq(
            &queue.get_first_mapped_file().expect("first remains"),
            &first
        ));
        assert!(Arc::ptr_eq(
            &queue.get_last_mapped_file().expect("target remains"),
            &target
        ));
        assert_eq!(target.get_wrote_position(), 512);
        assert_eq!(target.get_committed_position(), 512);
        assert_eq!(target.get_flushed_position(), 512);
        assert!(!removed.is_available());
        assert!(!temp_dir.path().join(format!("{:020}", 2048)).exists());
        queue.destroy();
    }
}
