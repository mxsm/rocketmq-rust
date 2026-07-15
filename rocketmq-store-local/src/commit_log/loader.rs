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

//! CommitLog load orchestration over a narrow local mapping target.

use std::io;
use std::path::Path;
use std::sync::Arc;

use rayon::prelude::*;

use super::load::apply_recovery_file_prefetch;
use super::load::apply_recovery_mmap_advice;
use super::load::collect_commit_log_metadata;
use super::load::discover_commit_log_files;
use super::load::record_file_prefetch;
use super::load::record_mmap_advice;
use super::load::CommitLogFileDiscovery;
use super::load::CommitLogMappingEntry;
use super::load::CommitLogMappingExecution;
use super::load::CommitLogMappingMode;
use super::load::CommitLogMappingOptions;
use super::load::CommitLogMappingPlan;
use super::load::CommitLogMetadataCollectionOptions;
use super::load::HintOutcome;
use super::load::LoadStatistics;
use super::load::RecoveryFilePrefetch;
use super::load::RecoveryMmapAdvice;

/// Function table connecting [`CommitLogLoader`] to one concrete mapped-file target.
///
/// The loader owns orchestration while callers retain their concrete target
/// representation. A target opened in [`CommitLogMappingMode::LazyReadOnly`]
/// may return `None` from `recovery_mapping` until its mapping is initialized;
/// the loader treats that state as an intentionally skipped hint attempt.
pub type RecoveryMapping<T> = for<'a> fn(&'a T) -> Option<(&'a [u8], &'a str)>;

pub struct CommitLogLoadAdapter<T> {
    /// Opens one validated CommitLog segment in the selected mapping mode.
    ///
    /// `path` has already passed metadata collection and size validation.
    /// `file_size` is the configured CommitLog segment size, not a newly
    /// queried filesystem value.
    pub open: fn(path: &Path, file_size: u64, mode: CommitLogMappingMode) -> io::Result<T>,

    /// Returns the initialized mapping and stable filename used by recovery hints.
    ///
    /// Lazy mappings that have not been initialized must return `None`.
    pub recovery_mapping: RecoveryMapping<T>,

    /// Marks the existing segment as fully written, flushed, and committed.
    pub mark_fully_loaded: fn(&T, position: i32),
}

/// Discovers, validates, maps, and initializes ordered CommitLog segments.
///
/// Filesystem metadata for every discovered segment is collected and
/// validated before any target adapter is opened. Parallel mapping
/// preserves the discovery order in the returned vector and in statistics
/// reduction.
pub struct CommitLogLoader {
    store_path: String,
    mapped_file_size: u64,
    enable_parallel: bool,
    recovery_mmap_advice: RecoveryMmapAdvice,
    recovery_file_prefetch: RecoveryFilePrefetch,
    lazy_mmap_enable: bool,
}

impl CommitLogLoader {
    /// Creates a loader with sequential mmap advice and file prefetch disabled.
    pub fn new(store_path: String, mapped_file_size: u64, enable_parallel: bool) -> Self {
        Self::new_with_recovery_mmap_advice(
            store_path,
            mapped_file_size,
            enable_parallel,
            RecoveryMmapAdvice::Sequential,
        )
    }

    /// Creates a loader with explicit mmap advice and file prefetch disabled.
    pub fn new_with_recovery_mmap_advice(
        store_path: String,
        mapped_file_size: u64,
        enable_parallel: bool,
        recovery_mmap_advice: RecoveryMmapAdvice,
    ) -> Self {
        Self::new_with_recovery_hints(
            store_path,
            mapped_file_size,
            enable_parallel,
            recovery_mmap_advice,
            RecoveryFilePrefetch::Disabled,
        )
    }

    /// Creates a loader with explicit non-fatal recovery hints.
    pub fn new_with_recovery_hints(
        store_path: String,
        mapped_file_size: u64,
        enable_parallel: bool,
        recovery_mmap_advice: RecoveryMmapAdvice,
        recovery_file_prefetch: RecoveryFilePrefetch,
    ) -> Self {
        Self {
            store_path,
            mapped_file_size,
            enable_parallel,
            recovery_mmap_advice,
            recovery_file_prefetch,
            lazy_mmap_enable: false,
        }
    }

    /// Enables or disables lazy read-only mappings for historical segments.
    ///
    /// The newest retained segment is always opened eagerly.
    pub fn with_lazy_mmap(mut self, lazy_mmap_enable: bool) -> Self {
        self.lazy_mmap_enable = lazy_mmap_enable;
        self
    }

    /// Loads validated CommitLog files and returns them in filename order.
    ///
    /// A missing directory returns empty results without recording elapsed
    /// load time. An existing empty directory records elapsed load time.
    /// Recovery-hint failures remain non-fatal and are captured in statistics.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when directory discovery, metadata collection,
    /// validation, or target opening fails.
    pub fn load_optimized<T: Send + Sync>(
        &self,
        adapter: CommitLogLoadAdapter<T>,
    ) -> io::Result<(Vec<Arc<T>>, LoadStatistics)> {
        let start = std::time::Instant::now();
        let mut stats = LoadStatistics {
            recovery_mmap_advice: self.recovery_mmap_advice,
            recovery_file_prefetch: self.recovery_file_prefetch,
            ..LoadStatistics::default()
        };

        let file_paths = match discover_commit_log_files(Path::new(&self.store_path))? {
            CommitLogFileDiscovery::DirectoryMissing => {
                tracing::warn!(
                    target: "rocketmq_store::log_file::commit_log_loader",
                    "CommitLog directory does not exist: {}",
                    self.store_path
                );
                return Ok((Vec::new(), stats));
            }
            CommitLogFileDiscovery::NoFiles => {
                tracing::info!(
                    target: "rocketmq_store::log_file::commit_log_loader",
                    "No commit log files found in {}",
                    self.store_path
                );
                stats.total_load_time_ms = start.elapsed().as_millis();
                return Ok((Vec::new(), stats));
            }
            CommitLogFileDiscovery::Files(file_paths) => file_paths,
        };

        let parallel_start = std::time::Instant::now();
        let file_metadata = collect_commit_log_metadata(
            &file_paths,
            CommitLogMetadataCollectionOptions {
                expected_file_size: self.mapped_file_size,
                parallel_enabled: self.enable_parallel,
            },
        )?;

        stats.parallel_load_time_ms = parallel_start.elapsed().as_millis();
        stats.total_files = file_metadata.len();
        stats.total_size_bytes = file_metadata.iter().map(|metadata| metadata.size).sum();

        let mapping_plan = CommitLogMappingPlan::new(
            file_metadata,
            CommitLogMappingOptions {
                parallel_enabled: self.enable_parallel,
                lazy_mmap_enabled: self.lazy_mmap_enable,
            },
        );
        let mapped_files = match mapping_plan.execution() {
            CommitLogMappingExecution::Parallel => {
                self.create_mapped_files_parallel(mapping_plan.entries(), &mut stats, &adapter)?
            }
            CommitLogMappingExecution::Sequential => {
                self.create_mapped_files_sequential(mapping_plan.entries(), &mut stats, &adapter)?
            }
        };

        stats.total_load_time_ms = start.elapsed().as_millis();
        stats.log_summary();

        Ok((mapped_files, stats))
    }

    fn create_mapped_files_parallel<T: Send + Sync>(
        &self,
        entries: &[CommitLogMappingEntry],
        statistics: &mut LoadStatistics,
        adapter: &CommitLogLoadAdapter<T>,
    ) -> io::Result<Vec<Arc<T>>> {
        let results: Result<Vec<_>, io::Error> = entries
            .par_iter()
            .map(|entry| {
                let mapped_file = self.create_mapped_file(entry, adapter)?;
                let (mmap_advice_outcome, file_prefetch_outcome) = self.apply_memory_hints(&mapped_file, adapter);
                (adapter.mark_fully_loaded)(&mapped_file, self.mapped_file_size as i32);
                Ok((Arc::new(mapped_file), mmap_advice_outcome, file_prefetch_outcome))
            })
            .collect();

        let results = results?;
        let mut mapped_files = Vec::with_capacity(results.len());
        for (mapped_file, mmap_advice_outcome, file_prefetch_outcome) in results {
            record_mmap_advice(statistics, mmap_advice_outcome);
            record_file_prefetch(statistics, file_prefetch_outcome);
            mapped_files.push(mapped_file);
        }
        Ok(mapped_files)
    }

    fn create_mapped_files_sequential<T: Send + Sync>(
        &self,
        entries: &[CommitLogMappingEntry],
        statistics: &mut LoadStatistics,
        adapter: &CommitLogLoadAdapter<T>,
    ) -> io::Result<Vec<Arc<T>>> {
        let mut mapped_files = Vec::with_capacity(entries.len());

        for entry in entries {
            let mapped_file = self.create_mapped_file(entry, adapter)?;
            let (mmap_advice_outcome, file_prefetch_outcome) = self.apply_memory_hints(&mapped_file, adapter);
            record_mmap_advice(statistics, mmap_advice_outcome);
            record_file_prefetch(statistics, file_prefetch_outcome);
            (adapter.mark_fully_loaded)(&mapped_file, self.mapped_file_size as i32);
            mapped_files.push(Arc::new(mapped_file));
        }

        Ok(mapped_files)
    }

    fn create_mapped_file<T>(&self, entry: &CommitLogMappingEntry, adapter: &CommitLogLoadAdapter<T>) -> io::Result<T> {
        (adapter.open)(entry.metadata().path.as_path(), self.mapped_file_size, entry.mode())
    }

    fn apply_memory_hints<T>(&self, mapped_file: &T, adapter: &CommitLogLoadAdapter<T>) -> (HintOutcome, HintOutcome) {
        let Some((mmap, file_name)) = (adapter.recovery_mapping)(mapped_file) else {
            return (HintOutcome::not_attempted(), HintOutcome::not_attempted());
        };
        let mmap_advice_outcome = apply_recovery_mmap_advice(self.recovery_mmap_advice, mmap, file_name);
        let file_prefetch_outcome = apply_recovery_file_prefetch(self.recovery_file_prefetch, mmap, file_name);
        (mmap_advice_outcome, file_prefetch_outcome)
    }
}
