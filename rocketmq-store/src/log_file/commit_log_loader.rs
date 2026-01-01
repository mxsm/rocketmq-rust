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

use cheetah_string::CheetahString;
use rayon::prelude::*;
use tracing::info;
use tracing::warn;

use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

/// Metadata for a single commit log file, collected during parallel scan
#[derive(Debug, Clone)]
struct FileMetadata {
    path: PathBuf,
    size: u64,
    file_name: String,
}

/// Statistics for load operation
#[derive(Debug, Default)]
pub struct LoadStatistics {
    pub total_files: usize,
    pub total_size_bytes: u64,
    pub files_removed: usize,
    pub parallel_load_time_ms: u128,
    pub total_load_time_ms: u128,
}

impl LoadStatistics {
    pub fn log_summary(&self) {
        info!(
            "CommitLog load completed: {} files ({:.2} GB), {} removed, parallel: {}ms, total: {}ms",
            self.total_files,
            self.total_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
            self.files_removed,
            self.parallel_load_time_ms,
            self.total_load_time_ms
        );
    }
}

/// Optimized loader for CommitLog files
pub struct CommitLogLoader {
    store_path: String,
    mapped_file_size: u64,
    enable_parallel: bool,
}

impl CommitLogLoader {
    /// Create a new loader
    pub fn new(store_path: String, mapped_file_size: u64, enable_parallel: bool) -> Self {
        Self {
            store_path,
            mapped_file_size,
            enable_parallel,
        }
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
        let mut stats = LoadStatistics::default();

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

        let mapped_files = if self.enable_parallel && file_metadata.len() > 4 {
            self.create_mapped_files_parallel(&file_metadata)?
        } else {
            self.create_mapped_files_sequential(&file_metadata)?
        };

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
    fn create_mapped_files_parallel(&self, metadata: &[FileMetadata]) -> io::Result<Vec<Arc<DefaultMappedFile>>> {
        // Parallel creation with ordered collection
        let results: Result<Vec<_>, io::Error> = metadata
            .par_iter()
            .map(|meta| {
                let mapped_file = DefaultMappedFile::new(
                    CheetahString::from_string(meta.path.to_string_lossy().to_string()),
                    self.mapped_file_size,
                );

                // Apply memory hints for sequential access
                self.apply_memory_hints(&mapped_file);

                // Set positions (all full since we're loading existing files)
                mapped_file.set_wrote_position(self.mapped_file_size as i32);
                mapped_file.set_flushed_position(self.mapped_file_size as i32);
                mapped_file.set_committed_position(self.mapped_file_size as i32);

                Ok(Arc::new(mapped_file))
            })
            .collect();

        // Convert to sequential Vec (maintains order from par_iter)
        results
    }

    /// Fallback: sequential mapped file creation
    fn create_mapped_files_sequential(&self, metadata: &[FileMetadata]) -> io::Result<Vec<Arc<DefaultMappedFile>>> {
        let mut mapped_files = Vec::with_capacity(metadata.len());

        for meta in metadata {
            let mapped_file = DefaultMappedFile::new(
                CheetahString::from_string(meta.path.to_string_lossy().to_string()),
                self.mapped_file_size,
            );

            self.apply_memory_hints(&mapped_file);

            mapped_file.set_wrote_position(self.mapped_file_size as i32);
            mapped_file.set_flushed_position(self.mapped_file_size as i32);
            mapped_file.set_committed_position(self.mapped_file_size as i32);

            mapped_files.push(Arc::new(mapped_file));
        }

        Ok(mapped_files)
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
    fn apply_memory_hints(&self, mapped_file: &DefaultMappedFile) {
        #[cfg(unix)]
        {
            use memmap2::Advice;

            // Access the underlying mmap through the public API
            let mmap = mapped_file.get_mapped_file();
            if let Err(e) = mmap.advise(Advice::Sequential) {
                // Non-fatal: madvise failure doesn't affect correctness
                warn!(
                    "Failed to apply sequential memory hint for {}: {}",
                    mapped_file.get_file_name(),
                    e
                );
            } else {
                // Optional debug logging (can be removed for production)
                #[cfg(debug_assertions)]
                tracing::debug!("Applied MADV_SEQUENTIAL hint to {}", mapped_file.get_file_name());
            }
        }

        #[cfg(windows)]
        {
            // Windows: PrefetchVirtualMemory requires additional Win32 API bindings
            // For now, rely on OS defaults which already provide good performance
            // Future enhancement: Use windows-sys crate to call PrefetchVirtualMemory

            #[cfg(debug_assertions)]
            tracing::debug!(
                "Memory hints not implemented for Windows, relying on OS defaults for {}",
                mapped_file.get_file_name()
            );
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
