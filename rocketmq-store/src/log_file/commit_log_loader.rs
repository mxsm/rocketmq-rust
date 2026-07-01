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

//! Optimized CommitLog loader with parallel I/O and zero-copy strategies.
//!
//! This module provides high-performance file loading for CommitLog recovery,
//! utilizing:
//! - Parallel file metadata collection and validation
//! - Batched mmap creation with optimal memory hints
//! - Zero-copy buffer reuse
//! - Platform-specific optimizations (madvise on Unix, PrefetchVirtualMemory on Windows)

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rayon::prelude::*;
use tracing::info;
use tracing::warn;

use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::utils::ffi::prefetch_virtual_memory;

/// Metadata for a single commit log file, collected during parallel scan
#[derive(Debug, Clone)]
struct FileMetadata {
    path: PathBuf,
    size: u64,
    file_name: String,
}

/// Statistics for load operation
#[derive(Debug, Clone, Default)]
pub struct LoadStatistics {
    pub total_files: usize,
    pub total_size_bytes: u64,
    pub files_removed: usize,
    pub parallel_load_time_ms: u128,
    pub total_load_time_ms: u128,
    pub recovery_mmap_advice: RecoveryMmapAdvice,
    pub mmap_advice_attempts: u64,
    pub mmap_advice_successes: u64,
    pub mmap_advice_failures: u64,
    pub mmap_advice_elapsed_ms: u64,
    pub recovery_file_prefetch: RecoveryFilePrefetch,
    pub file_prefetch_attempts: u64,
    pub file_prefetch_successes: u64,
    pub file_prefetch_failures: u64,
    pub file_prefetch_elapsed_ms: u64,
}

impl LoadStatistics {
    pub fn log_summary(&self) {
        info!(
            "CommitLog load completed: {} files ({:.2} GB), {} removed, parallel: {}ms, total: {}ms, mmapAdvice={}, \
             mmapAdviceAttempts={}, mmapAdviceSuccesses={}, mmapAdviceFailures={}, mmapAdviceElapsedMs={}, \
             filePrefetch={}, filePrefetchAttempts={}, filePrefetchSuccesses={}, filePrefetchFailures={}, \
             filePrefetchElapsedMs={}",
            self.total_files,
            self.total_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
            self.files_removed,
            self.parallel_load_time_ms,
            self.total_load_time_ms,
            self.recovery_mmap_advice.as_str(),
            self.mmap_advice_attempts,
            self.mmap_advice_successes,
            self.mmap_advice_failures,
            self.mmap_advice_elapsed_ms,
            self.recovery_file_prefetch.as_str(),
            self.file_prefetch_attempts,
            self.file_prefetch_successes,
            self.file_prefetch_failures,
            self.file_prefetch_elapsed_ms
        );
    }

    fn record_mmap_advice(&mut self, result: HintResult) {
        if !result.attempted {
            return;
        }
        self.mmap_advice_attempts = self.mmap_advice_attempts.saturating_add(1);
        if result.succeeded {
            self.mmap_advice_successes = self.mmap_advice_successes.saturating_add(1);
        } else {
            self.mmap_advice_failures = self.mmap_advice_failures.saturating_add(1);
        }
        self.mmap_advice_elapsed_ms = self
            .mmap_advice_elapsed_ms
            .saturating_add(duration_to_millis(result.elapsed));
    }

    fn record_file_prefetch(&mut self, result: HintResult) {
        if !result.attempted {
            return;
        }
        self.file_prefetch_attempts = self.file_prefetch_attempts.saturating_add(1);
        if result.succeeded {
            self.file_prefetch_successes = self.file_prefetch_successes.saturating_add(1);
        } else {
            self.file_prefetch_failures = self.file_prefetch_failures.saturating_add(1);
        }
        self.file_prefetch_elapsed_ms = self
            .file_prefetch_elapsed_ms
            .saturating_add(duration_to_millis(result.elapsed));
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecoveryMmapAdvice {
    #[default]
    Disabled,
    Sequential,
}

impl RecoveryMmapAdvice {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Sequential => "sequential",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct HintResult {
    attempted: bool,
    succeeded: bool,
    elapsed: Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecoveryFilePrefetch {
    #[default]
    Disabled,
    Sequential,
}

impl RecoveryFilePrefetch {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Sequential => "sequential",
        }
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Optimized loader for CommitLog files
pub struct CommitLogLoader {
    store_path: String,
    mapped_file_size: u64,
    enable_parallel: bool,
    recovery_mmap_advice: RecoveryMmapAdvice,
    recovery_file_prefetch: RecoveryFilePrefetch,
    lazy_mmap_enable: bool,
}

impl CommitLogLoader {
    /// Create a new loader
    pub fn new(store_path: String, mapped_file_size: u64, enable_parallel: bool) -> Self {
        Self::new_with_recovery_mmap_advice(
            store_path,
            mapped_file_size,
            enable_parallel,
            RecoveryMmapAdvice::Sequential,
        )
    }

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

    pub fn with_lazy_mmap(mut self, lazy_mmap_enable: bool) -> Self {
        self.lazy_mmap_enable = lazy_mmap_enable;
        self
    }

    /// Load files with optimizations enabled
    ///
    /// # Performance Optimizations
    /// - Phase 1: Parallel metadata collection (fs::metadata + filtering)
    /// - Phase 2: Batch validation (size checks, empty file removal)
    /// - Phase 3: Parallel mmap creation with memory hints
    ///
    /// # Returns
    /// `Ok((files, stats))` on success, `Err(io::Error)` on failure
    pub fn load_optimized(&self) -> io::Result<(Vec<Arc<DefaultMappedFile>>, LoadStatistics)> {
        let start = std::time::Instant::now();
        let mut stats = LoadStatistics {
            recovery_mmap_advice: self.recovery_mmap_advice,
            recovery_file_prefetch: self.recovery_file_prefetch,
            ..LoadStatistics::default()
        };

        let dir = Path::new(&self.store_path);
        if !dir.exists() {
            warn!("CommitLog directory does not exist: {}", self.store_path);
            return Ok((Vec::new(), stats));
        }

        let mut file_paths: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();

        // Sort by filename (ensures ordering consistency)
        file_paths.sort_by(|a, b| {
            a.file_name()
                .and_then(|n| n.to_str())
                .cmp(&b.file_name().and_then(|n| n.to_str()))
        });

        if file_paths.is_empty() {
            info!("No commit log files found in {}", self.store_path);
            stats.total_load_time_ms = start.elapsed().as_millis();
            return Ok((Vec::new(), stats));
        }

        let parallel_start = std::time::Instant::now();
        let file_metadata: Vec<FileMetadata> = if self.enable_parallel && file_paths.len() > 4 {
            self.collect_metadata_parallel(&file_paths)?
        } else {
            self.collect_metadata_sequential(&file_paths)?
        };

        stats.parallel_load_time_ms = parallel_start.elapsed().as_millis();
        stats.total_files = file_metadata.len();
        stats.total_size_bytes = file_metadata.iter().map(|m| m.size).sum();

        let (mapped_files, mmap_advice_stats) = if self.enable_parallel && file_metadata.len() > 4 {
            self.create_mapped_files_parallel(&file_metadata)?
        } else {
            self.create_mapped_files_sequential(&file_metadata)?
        };
        stats.mmap_advice_attempts = mmap_advice_stats.mmap_advice_attempts;
        stats.mmap_advice_successes = mmap_advice_stats.mmap_advice_successes;
        stats.mmap_advice_failures = mmap_advice_stats.mmap_advice_failures;
        stats.mmap_advice_elapsed_ms = mmap_advice_stats.mmap_advice_elapsed_ms;
        stats.file_prefetch_attempts = mmap_advice_stats.file_prefetch_attempts;
        stats.file_prefetch_successes = mmap_advice_stats.file_prefetch_successes;
        stats.file_prefetch_failures = mmap_advice_stats.file_prefetch_failures;
        stats.file_prefetch_elapsed_ms = mmap_advice_stats.file_prefetch_elapsed_ms;

        stats.total_load_time_ms = start.elapsed().as_millis();
        stats.log_summary();

        Ok((mapped_files, stats))
    }

    /// Collect metadata in parallel using rayon
    ///
    /// # Safety
    /// Uses rayon's thread pool for parallel fs::metadata calls.
    /// Each call is independent and thread-safe.
    fn collect_metadata_parallel(&self, paths: &[PathBuf]) -> io::Result<Vec<FileMetadata>> {
        let expected_size = self.mapped_file_size;
        let is_last_file_idx = paths.len().saturating_sub(1);

        let results: Result<Vec<_>, _> = paths
            .par_iter()
            .enumerate()
            .map(|(idx, path)| {
                let metadata = fs::metadata(path)
                    .map_err(|e| io::Error::new(e.kind(), format!("Failed to get metadata for {:?}: {}", path, e)))?;

                let size = metadata.len();
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Validate file size - CRITICAL: Only the last file in the sorted array can be
                // empty
                if size == 0 && idx == is_last_file_idx {
                    // Last file can be empty, remove it (matches original do_load behavior)
                    if let Err(e) = fs::remove_file(path) {
                        warn!("Failed to delete empty file {:?}: {}", path, e);
                    } else {
                        warn!("{} size is 0, auto deleted.", path.display());
                    }
                    return Ok(None);
                } else if size != expected_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} length {} not matched expected size {}, please check it manually",
                            path.display(),
                            size,
                            expected_size
                        ),
                    ));
                }

                Ok(Some(FileMetadata {
                    path: path.clone(),
                    size,
                    file_name,
                }))
            })
            .collect();

        results.map(|v| v.into_iter().flatten().collect())
    }

    /// Fallback: sequential metadata collection
    fn collect_metadata_sequential(&self, paths: &[PathBuf]) -> io::Result<Vec<FileMetadata>> {
        let mut metadata_list = Vec::with_capacity(paths.len());
        let expected_size = self.mapped_file_size;
        let last_file_idx = paths.len().saturating_sub(1);

        for (idx, path) in paths.iter().enumerate() {
            let metadata = fs::metadata(path)?;
            let size = metadata.len();
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            // Only the last file in the sorted list can be empty (matches original do_load)
            if size == 0 && idx == last_file_idx {
                if let Err(e) = fs::remove_file(path) {
                    warn!("Failed to delete empty file {:?}: {}", path, e);
                } else {
                    warn!("{} size is 0, auto deleted.", path.display());
                }
                continue;
            } else if size != expected_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} length {} not matched expected size {}, please check it manually",
                        path.display(),
                        size,
                        expected_size
                    ),
                ));
            }

            metadata_list.push(FileMetadata {
                path: path.clone(),
                size,
                file_name,
            });
        }

        Ok(metadata_list)
    }

    /// Create mapped files in parallel (with synchronization for Vec::push)
    fn create_mapped_files_parallel(
        &self,
        metadata: &[FileMetadata],
    ) -> io::Result<(Vec<Arc<DefaultMappedFile>>, LoadStatistics)> {
        // Parallel creation with ordered collection
        let file_count = metadata.len();
        let results: Result<Vec<_>, io::Error> = metadata
            .par_iter()
            .enumerate()
            .map(|(idx, meta)| {
                let mapped_file = self.create_mapped_file(meta, idx, file_count)?;

                // Apply memory hints for sequential access
                let (mmap_advice_result, file_prefetch_result) = self.apply_memory_hints(&mapped_file);

                // Set positions (all full since we're loading existing files)
                mapped_file.set_wrote_position(self.mapped_file_size as i32);
                mapped_file.set_flushed_position(self.mapped_file_size as i32);
                mapped_file.set_committed_position(self.mapped_file_size as i32);

                Ok((Arc::new(mapped_file), mmap_advice_result, file_prefetch_result))
            })
            .collect();

        // Convert to sequential Vec (maintains order from par_iter)
        let results = results?;
        let mut mmap_advice_stats = LoadStatistics {
            recovery_mmap_advice: self.recovery_mmap_advice,
            recovery_file_prefetch: self.recovery_file_prefetch,
            ..LoadStatistics::default()
        };
        let mut mapped_files = Vec::with_capacity(results.len());
        for (mapped_file, mmap_advice_result, file_prefetch_result) in results {
            mmap_advice_stats.record_mmap_advice(mmap_advice_result);
            mmap_advice_stats.record_file_prefetch(file_prefetch_result);
            mapped_files.push(mapped_file);
        }
        Ok((mapped_files, mmap_advice_stats))
    }

    /// Fallback: sequential mapped file creation
    fn create_mapped_files_sequential(
        &self,
        metadata: &[FileMetadata],
    ) -> io::Result<(Vec<Arc<DefaultMappedFile>>, LoadStatistics)> {
        let mut mapped_files = Vec::with_capacity(metadata.len());
        let mut mmap_advice_stats = LoadStatistics {
            recovery_mmap_advice: self.recovery_mmap_advice,
            recovery_file_prefetch: self.recovery_file_prefetch,
            ..LoadStatistics::default()
        };

        let file_count = metadata.len();
        for (idx, meta) in metadata.iter().enumerate() {
            let mapped_file = self.create_mapped_file(meta, idx, file_count)?;

            let (mmap_advice_result, file_prefetch_result) = self.apply_memory_hints(&mapped_file);
            mmap_advice_stats.record_mmap_advice(mmap_advice_result);
            mmap_advice_stats.record_file_prefetch(file_prefetch_result);

            mapped_file.set_wrote_position(self.mapped_file_size as i32);
            mapped_file.set_flushed_position(self.mapped_file_size as i32);
            mapped_file.set_committed_position(self.mapped_file_size as i32);

            mapped_files.push(Arc::new(mapped_file));
        }

        Ok((mapped_files, mmap_advice_stats))
    }

    fn create_mapped_file(&self, meta: &FileMetadata, idx: usize, file_count: usize) -> io::Result<DefaultMappedFile> {
        let file_name = CheetahString::from_string(meta.path.to_string_lossy().to_string());
        if self.lazy_mmap_enable && idx + 1 < file_count {
            DefaultMappedFile::try_new_lazy_read_only(file_name, self.mapped_file_size)
        } else {
            DefaultMappedFile::try_new(file_name, self.mapped_file_size)
        }
    }

    /// Apply platform-specific memory access hints
    ///
    /// # Platform-specific behavior
    /// - **Linux/Unix**: `madvise(MADV_SEQUENTIAL)` - kernel prefetch optimization
    /// - **Windows**: Currently relies on OS default (no explicit hints)
    ///
    /// # Implementation
    /// Uses `memmap2::Mmap::advise()` to provide sequential access hints to the kernel,
    /// which can improve performance by optimizing readahead and page cache behavior.
    fn apply_memory_hints(&self, mapped_file: &DefaultMappedFile) -> (HintResult, HintResult) {
        if mapped_file.is_lazy_mmap_enabled() && !mapped_file.is_mapped() {
            return (HintResult::default(), HintResult::default());
        }

        let mmap_advice_result = self.apply_mmap_advice(mapped_file);
        let file_prefetch_result = self.apply_file_prefetch(mapped_file);
        (mmap_advice_result, file_prefetch_result)
    }

    fn apply_mmap_advice(&self, mapped_file: &DefaultMappedFile) -> HintResult {
        if self.recovery_mmap_advice == RecoveryMmapAdvice::Disabled {
            return HintResult::default();
        }

        #[cfg(unix)]
        {
            use memmap2::Advice;

            // Access the underlying mmap through the public API
            let start = std::time::Instant::now();
            let mmap = mapped_file.get_mapped_file();
            match self.recovery_mmap_advice {
                RecoveryMmapAdvice::Disabled => HintResult::default(),
                RecoveryMmapAdvice::Sequential => {
                    if let Err(e) = mmap.advise(Advice::Sequential) {
                        // Non-fatal: madvise failure doesn't affect correctness
                        warn!(
                            "Failed to apply sequential memory hint for {}: {}",
                            mapped_file.get_file_name(),
                            e
                        );
                        HintResult {
                            attempted: true,
                            succeeded: false,
                            elapsed: start.elapsed(),
                        }
                    } else {
                        #[cfg(debug_assertions)]
                        tracing::debug!("Applied MADV_SEQUENTIAL hint to {}", mapped_file.get_file_name());
                        HintResult {
                            attempted: true,
                            succeeded: true,
                            elapsed: start.elapsed(),
                        }
                    }
                }
            }
        }

        #[cfg(not(unix))]
        {
            let _ = mapped_file;
            HintResult::default()
        }
    }

    fn apply_file_prefetch(&self, mapped_file: &DefaultMappedFile) -> HintResult {
        if self.recovery_file_prefetch == RecoveryFilePrefetch::Disabled {
            return HintResult::default();
        }

        #[cfg(windows)]
        {
            let start = std::time::Instant::now();
            let mmap = mapped_file.get_mapped_file();
            match self.recovery_file_prefetch {
                RecoveryFilePrefetch::Disabled => HintResult::default(),
                RecoveryFilePrefetch::Sequential => match prefetch_virtual_memory(mmap.as_ptr(), mmap.len()) {
                    Ok(true) => HintResult {
                        attempted: true,
                        succeeded: true,
                        elapsed: start.elapsed(),
                    },
                    Ok(false) => HintResult::default(),
                    Err(error) => {
                        warn!(
                            "Failed to prefetch recovery mapped file {}: {}",
                            mapped_file.get_file_name(),
                            error
                        );
                        HintResult {
                            attempted: true,
                            succeeded: false,
                            elapsed: start.elapsed(),
                        }
                    }
                },
            }
        }

        #[cfg(not(windows))]
        {
            let _ = mapped_file;
            HintResult::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_load_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let loader = CommitLogLoader::new(temp_dir.path().to_string_lossy().to_string(), 1024 * 1024, true);

        let result = loader.load_optimized();
        assert!(result.is_ok());
        let (files, stats) = result.unwrap();
        assert_eq!(files.len(), 0);
        assert_eq!(stats.total_files, 0);
    }

    #[test]
    fn test_load_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_size = 1024 * 1024u64;

        // Create a test file
        std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();

        let loader = CommitLogLoader::new(temp_dir.path().to_string_lossy().to_string(), file_size, false);

        let result = loader.load_optimized();
        assert!(result.is_ok());
        let (files, stats) = result.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(stats.total_files, 1);
        assert_eq!(stats.total_size_bytes, file_size);
    }

    #[test]
    fn disabled_recovery_mmap_advice_records_no_attempts() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_size = 1024 * 1024u64;
        std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();

        let loader = CommitLogLoader::new_with_recovery_mmap_advice(
            temp_dir.path().to_string_lossy().to_string(),
            file_size,
            false,
            RecoveryMmapAdvice::Disabled,
        );

        let (_, stats) = loader.load_optimized().unwrap();
        assert_eq!(stats.recovery_mmap_advice, RecoveryMmapAdvice::Disabled);
        assert_eq!(stats.mmap_advice_attempts, 0);
        assert_eq!(stats.mmap_advice_successes, 0);
        assert_eq!(stats.mmap_advice_failures, 0);
    }

    #[test]
    fn sequential_recovery_mmap_advice_records_supported_attempts() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_size = 1024 * 1024u64;
        std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();

        let loader = CommitLogLoader::new_with_recovery_mmap_advice(
            temp_dir.path().to_string_lossy().to_string(),
            file_size,
            false,
            RecoveryMmapAdvice::Sequential,
        );

        let (_, stats) = loader.load_optimized().unwrap();
        let expected_attempts = if cfg!(unix) { 1 } else { 0 };
        assert_eq!(stats.recovery_mmap_advice, RecoveryMmapAdvice::Sequential);
        assert_eq!(stats.mmap_advice_attempts, expected_attempts);
        assert_eq!(
            stats.mmap_advice_attempts,
            stats.mmap_advice_successes.saturating_add(stats.mmap_advice_failures)
        );
    }

    #[test]
    fn disabled_recovery_file_prefetch_records_no_attempts() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_size = 1024 * 1024u64;
        std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();

        let loader = CommitLogLoader::new_with_recovery_hints(
            temp_dir.path().to_string_lossy().to_string(),
            file_size,
            false,
            RecoveryMmapAdvice::Disabled,
            RecoveryFilePrefetch::Disabled,
        );

        let (_, stats) = loader.load_optimized().unwrap();
        assert_eq!(stats.recovery_file_prefetch, RecoveryFilePrefetch::Disabled);
        assert_eq!(stats.file_prefetch_attempts, 0);
        assert_eq!(stats.file_prefetch_successes, 0);
        assert_eq!(stats.file_prefetch_failures, 0);
    }

    #[test]
    fn sequential_recovery_file_prefetch_records_windows_attempts() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_size = 1024 * 1024u64;
        std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();

        let loader = CommitLogLoader::new_with_recovery_hints(
            temp_dir.path().to_string_lossy().to_string(),
            file_size,
            false,
            RecoveryMmapAdvice::Disabled,
            RecoveryFilePrefetch::Sequential,
        );

        let (_, stats) = loader.load_optimized().unwrap();
        let expected_attempts = if cfg!(windows) { 1 } else { 0 };
        assert_eq!(stats.recovery_file_prefetch, RecoveryFilePrefetch::Sequential);
        assert_eq!(stats.file_prefetch_attempts, expected_attempts);
        assert_eq!(
            stats.file_prefetch_attempts,
            stats
                .file_prefetch_successes
                .saturating_add(stats.file_prefetch_failures)
        );
    }

    #[test]
    fn lazy_mmap_marks_only_historical_commitlog_files() {
        let temp_dir = TempDir::new().unwrap();
        let file_size = 1024 * 1024u64;
        for i in 0..3 {
            let file_path = temp_dir.path().join(format!("{:020}", i * file_size));
            std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();
        }

        let loader = CommitLogLoader::new_with_recovery_hints(
            temp_dir.path().to_string_lossy().to_string(),
            file_size,
            false,
            RecoveryMmapAdvice::Disabled,
            RecoveryFilePrefetch::Disabled,
        )
        .with_lazy_mmap(true);

        let (files, stats) = loader.load_optimized().unwrap();

        assert_eq!(files.len(), 3);
        assert!(files[0].is_lazy_mmap_enabled());
        assert!(files[1].is_lazy_mmap_enabled());
        assert!(!files[2].is_lazy_mmap_enabled());
        assert!(!files[0].is_mapped());
        assert!(!files[1].is_mapped());
        assert!(files[2].is_mapped());
        assert_eq!(stats.mmap_advice_attempts, 0);
        assert_eq!(stats.file_prefetch_attempts, 0);
    }

    #[test]
    fn disabled_lazy_mmap_keeps_recovery_load_eager() {
        let temp_dir = TempDir::new().unwrap();
        let file_size = 1024 * 1024u64;
        for i in 0..3 {
            let file_path = temp_dir.path().join(format!("{:020}", i * file_size));
            std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();
        }

        let loader = CommitLogLoader::new_with_recovery_hints(
            temp_dir.path().to_string_lossy().to_string(),
            file_size,
            false,
            RecoveryMmapAdvice::Disabled,
            RecoveryFilePrefetch::Disabled,
        )
        .with_lazy_mmap(false);

        let (files, _) = loader.load_optimized().unwrap();

        assert_eq!(files.len(), 3);
        assert!(files.iter().all(|file| !file.is_lazy_mmap_enabled()));
        assert!(files.iter().all(|file| file.is_mapped()));
    }

    #[test]
    fn test_load_multiple_files_parallel() {
        let temp_dir = TempDir::new().unwrap();
        let file_size = 1024 * 1024u64;
        let num_files = 10;

        // Create test files
        for i in 0..num_files {
            let file_path = temp_dir.path().join(format!("{:020}", i * file_size));
            std::fs::write(&file_path, vec![0u8; file_size as usize]).unwrap();
        }

        let loader = CommitLogLoader::new(temp_dir.path().to_string_lossy().to_string(), file_size, true);

        let result = loader.load_optimized();
        assert!(result.is_ok());
        let (files, stats) = result.unwrap();
        assert_eq!(files.len(), num_files as usize);
        assert_eq!(stats.total_files, num_files as usize);
        assert_eq!(stats.total_size_bytes, file_size * num_files);
    }

    #[test]
    fn test_reject_mismatched_size() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("00000000000000000000");
        let expected_size = 1024 * 1024u64;
        let actual_size = 512 * 1024u64;

        std::fs::write(&file_path, vec![0u8; actual_size as usize]).unwrap();

        let loader = CommitLogLoader::new(temp_dir.path().to_string_lossy().to_string(), expected_size, false);

        let result = loader.load_optimized();
        assert!(result.is_err());
    }
}
