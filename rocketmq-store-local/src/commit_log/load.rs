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

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;
use tracing::info;

/// Filesystem metadata required to validate and open one CommitLog segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogFileMetadata {
    /// Filesystem path of the CommitLog segment.
    pub path: PathBuf,
    /// Current length of the CommitLog segment in bytes.
    pub size: u64,
}

/// Store-side action selected after validating one CommitLog segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogFileLoadDecision {
    /// Keep the segment and continue loading it.
    Load,
    /// Remove an empty final segment and omit it from the load result.
    RemoveEmptyLast,
}

/// A CommitLog segment whose length violates the configured segment size.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "{} length {actual} not matched expected size {expected}, please check it manually",
    path.display()
)]
pub struct CommitLogFileValidationError {
    /// Filesystem path of the invalid CommitLog segment.
    pub path: PathBuf,
    /// Observed segment length in bytes.
    pub actual: u64,
    /// Configured segment length in bytes.
    pub expected: u64,
}

/// Validates one CommitLog segment without performing filesystem I/O.
pub fn validate_commit_log_file(
    metadata: &CommitLogFileMetadata,
    expected: u64,
    is_last: bool,
) -> Result<CommitLogFileLoadDecision, CommitLogFileValidationError> {
    if metadata.size == 0 && is_last {
        return Ok(CommitLogFileLoadDecision::RemoveEmptyLast);
    }
    if metadata.size != expected {
        return Err(CommitLogFileValidationError {
            path: metadata.path.clone(),
            actual: metadata.size,
            expected,
        });
    }
    Ok(CommitLogFileLoadDecision::Load)
}

/// Loader options used to decide how validated CommitLog segments are mapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogMappingOptions {
    /// Whether mapping may use the parallel execution path.
    pub parallel_enabled: bool,
    /// Whether historical segments may defer read-only mmap creation.
    pub lazy_mmap_enabled: bool,
}

/// Execution strategy selected for mapping validated CommitLog segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogMappingExecution {
    /// Map segments in their input order on the current thread.
    Sequential,
    /// Map segments in parallel while preserving their input order.
    Parallel,
}

/// Mapping mode selected for one validated CommitLog segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogMappingMode {
    /// Create the mapping immediately.
    Eager,
    /// Defer creation of a read-only mapping until the segment is read.
    LazyReadOnly,
}

/// Ordered mapping plan for a fully validated set of CommitLog segments.
#[derive(Debug)]
pub struct CommitLogMappingPlan {
    execution: CommitLogMappingExecution,
    entries: Vec<CommitLogMappingEntry>,
}

/// One validated CommitLog segment and its selected mapping mode.
#[derive(Debug)]
pub struct CommitLogMappingEntry {
    metadata: CommitLogFileMetadata,
    mode: CommitLogMappingMode,
}

impl CommitLogMappingPlan {
    /// Builds a plan without performing filesystem or mmap operations.
    pub fn new(metadata: Vec<CommitLogFileMetadata>, options: CommitLogMappingOptions) -> Self {
        let execution = if options.parallel_enabled && metadata.len() > 4 {
            CommitLogMappingExecution::Parallel
        } else {
            CommitLogMappingExecution::Sequential
        };
        let last_index = metadata.len().saturating_sub(1);
        let entries = metadata
            .into_iter()
            .enumerate()
            .map(|(index, metadata)| {
                let mode = if options.lazy_mmap_enabled && index < last_index {
                    CommitLogMappingMode::LazyReadOnly
                } else {
                    CommitLogMappingMode::Eager
                };
                CommitLogMappingEntry { metadata, mode }
            })
            .collect();
        Self { execution, entries }
    }

    /// Returns the selected execution strategy.
    pub fn execution(&self) -> CommitLogMappingExecution {
        self.execution
    }

    /// Returns the ordered segment entries.
    pub fn entries(&self) -> &[CommitLogMappingEntry] {
        &self.entries
    }
}

impl CommitLogMappingEntry {
    /// Returns the complete metadata value collected for this segment.
    pub fn metadata(&self) -> &CommitLogFileMetadata {
        &self.metadata
    }

    /// Returns the selected mapping mode.
    pub fn mode(&self) -> CommitLogMappingMode {
        self.mode
    }
}

/// Result of attempting one platform-specific recovery hint.
#[derive(Debug)]
pub struct HintOutcome {
    attempted: bool,
    succeeded: bool,
    elapsed: Duration,
}

impl HintOutcome {
    /// Reports that the hint was disabled, unsupported, or intentionally skipped.
    pub fn not_attempted() -> Self {
        Self {
            attempted: false,
            succeeded: false,
            elapsed: Duration::ZERO,
        }
    }

    /// Reports a successful platform hint attempt.
    pub fn success(elapsed: Duration) -> Self {
        Self {
            attempted: true,
            succeeded: true,
            elapsed,
        }
    }

    /// Reports a failed platform hint attempt.
    pub fn failure(elapsed: Duration) -> Self {
        Self {
            attempted: true,
            succeeded: false,
            elapsed,
        }
    }
}

/// Statistics for load operation.
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
            target: "rocketmq_store::log_file::commit_log_loader",
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
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Records one mmap-advice outcome in the canonical load statistics.
pub fn record_mmap_advice(statistics: &mut LoadStatistics, outcome: HintOutcome) {
    if !outcome.attempted {
        return;
    }
    statistics.mmap_advice_attempts = statistics.mmap_advice_attempts.saturating_add(1);
    if outcome.succeeded {
        statistics.mmap_advice_successes = statistics.mmap_advice_successes.saturating_add(1);
    } else {
        statistics.mmap_advice_failures = statistics.mmap_advice_failures.saturating_add(1);
    }
    statistics.mmap_advice_elapsed_ms = statistics
        .mmap_advice_elapsed_ms
        .saturating_add(duration_to_millis(outcome.elapsed));
}

/// Records one file-prefetch outcome in the canonical load statistics.
pub fn record_file_prefetch(statistics: &mut LoadStatistics, outcome: HintOutcome) {
    if !outcome.attempted {
        return;
    }
    statistics.file_prefetch_attempts = statistics.file_prefetch_attempts.saturating_add(1);
    if outcome.succeeded {
        statistics.file_prefetch_successes = statistics.file_prefetch_successes.saturating_add(1);
    } else {
        statistics.file_prefetch_failures = statistics.file_prefetch_failures.saturating_add(1);
    }
    statistics.file_prefetch_elapsed_ms = statistics
        .file_prefetch_elapsed_ms
        .saturating_add(duration_to_millis(outcome.elapsed));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_statistics_default_preserves_disabled_recovery_hints() {
        let statistics = LoadStatistics::default();
        assert_eq!(statistics.total_files, 0);
        assert_eq!(statistics.recovery_mmap_advice, RecoveryMmapAdvice::Disabled);
        assert_eq!(statistics.recovery_file_prefetch, RecoveryFilePrefetch::Disabled);
    }

    #[test]
    fn recovery_hint_vocabulary_is_stable() {
        assert_eq!(RecoveryMmapAdvice::Disabled.as_str(), "disabled");
        assert_eq!(RecoveryMmapAdvice::Sequential.as_str(), "sequential");
        assert_eq!(RecoveryFilePrefetch::Disabled.as_str(), "disabled");
        assert_eq!(RecoveryFilePrefetch::Sequential.as_str(), "sequential");
    }
}
