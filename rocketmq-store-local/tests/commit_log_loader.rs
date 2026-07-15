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
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rocketmq_store_local::commit_log::load::CommitLogMappingMode;
use rocketmq_store_local::commit_log::load::RecoveryFilePrefetch;
use rocketmq_store_local::commit_log::load::RecoveryMmapAdvice;
use rocketmq_store_local::commit_log::loader::CommitLogLoadAdapter;
use rocketmq_store_local::commit_log::loader::CommitLogLoader;
use tempfile::TempDir;

static VALIDATION_OPEN_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct FakeTarget {
    path: PathBuf,
    mode: CommitLogMappingMode,
    fully_loaded_position: AtomicI32,
}

fn open_fake_target(path: &Path, _file_size: u64, mode: CommitLogMappingMode) -> io::Result<FakeTarget> {
    Ok(FakeTarget {
        path: path.to_path_buf(),
        mode,
        fully_loaded_position: AtomicI32::new(0),
    })
}

fn fake_recovery_mapping(_target: &FakeTarget) -> Option<(&[u8], &str)> {
    None
}

fn mark_fake_fully_loaded(target: &FakeTarget, position: i32) {
    target.fully_loaded_position.store(position, Ordering::SeqCst);
}

fn fake_target_adapter() -> CommitLogLoadAdapter<FakeTarget> {
    CommitLogLoadAdapter {
        open: open_fake_target,
        recovery_mapping: fake_recovery_mapping,
        mark_fully_loaded: mark_fake_fully_loaded,
    }
}

#[derive(Debug)]
struct CountingTarget(FakeTarget);

fn open_counting_target(path: &Path, file_size: u64, mode: CommitLogMappingMode) -> io::Result<CountingTarget> {
    VALIDATION_OPEN_COUNT.fetch_add(1, Ordering::SeqCst);
    open_fake_target(path, file_size, mode).map(CountingTarget)
}

fn counting_recovery_mapping(_target: &CountingTarget) -> Option<(&[u8], &str)> {
    None
}

fn mark_counting_fully_loaded(target: &CountingTarget, position: i32) {
    mark_fake_fully_loaded(&target.0, position);
}

fn counting_target_adapter() -> CommitLogLoadAdapter<CountingTarget> {
    CommitLogLoadAdapter {
        open: open_counting_target,
        recovery_mapping: counting_recovery_mapping,
        mark_fully_loaded: mark_counting_fully_loaded,
    }
}

fn create_segment(directory: &Path, name: &str, size: usize) -> PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, vec![0_u8; size]).unwrap();
    path
}

#[test]
fn loader_handles_missing_empty_and_single_directories() {
    let directory = TempDir::new().unwrap();
    let missing = directory.path().join("missing");
    let loader = CommitLogLoader::new(missing.to_string_lossy().into_owned(), 16, false);
    let (targets, statistics) = loader.load_optimized(fake_target_adapter()).unwrap();
    assert!(targets.is_empty());
    assert_eq!(statistics.total_load_time_ms, 0);

    let empty = TempDir::new().unwrap();
    let loader = CommitLogLoader::new(empty.path().to_string_lossy().into_owned(), 16, false);
    let (targets, statistics) = loader.load_optimized(fake_target_adapter()).unwrap();
    assert!(targets.is_empty());
    assert_eq!(statistics.total_files, 0);

    let single = TempDir::new().unwrap();
    let path = create_segment(single.path(), "00000000000000000000", 16);
    let loader = CommitLogLoader::new(single.path().to_string_lossy().into_owned(), 16, false);
    let (targets, statistics) = loader.load_optimized(fake_target_adapter()).unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].path, path);
    assert_eq!(targets[0].mode, CommitLogMappingMode::Eager);
    assert_eq!(targets[0].fully_loaded_position.load(Ordering::SeqCst), 16);
    assert_eq!(statistics.total_files, 1);
    assert_eq!(statistics.total_size_bytes, 16);
}

#[test]
fn parallel_loader_preserves_order_and_only_lazily_maps_historical_segments() {
    let directory = TempDir::new().unwrap();
    let expected_paths: Vec<_> = (0..6)
        .map(|index| create_segment(directory.path(), &format!("{index:020}"), 16))
        .collect();
    let loader = CommitLogLoader::new_with_recovery_hints(
        directory.path().to_string_lossy().into_owned(),
        16,
        true,
        RecoveryMmapAdvice::Sequential,
        RecoveryFilePrefetch::Sequential,
    )
    .with_lazy_mmap(true);

    let (targets, statistics) = loader.load_optimized(fake_target_adapter()).unwrap();
    let actual_paths: Vec<_> = targets.iter().map(|target| target.path.clone()).collect();
    assert_eq!(actual_paths, expected_paths);
    assert!(targets[..5]
        .iter()
        .all(|target| target.mode == CommitLogMappingMode::LazyReadOnly));
    assert_eq!(targets[5].mode, CommitLogMappingMode::Eager);
    assert!(targets
        .iter()
        .all(|target| target.fully_loaded_position.load(Ordering::SeqCst) == 16));
    assert_eq!(statistics.mmap_advice_attempts, 0);
    assert_eq!(statistics.file_prefetch_attempts, 0);
}

#[test]
fn validation_and_empty_last_processing_finish_before_any_target_is_opened() {
    let invalid = TempDir::new().unwrap();
    create_segment(invalid.path(), "00000000000000000000", 16);
    create_segment(invalid.path(), "00000000000000000016", 8);
    VALIDATION_OPEN_COUNT.store(0, Ordering::SeqCst);
    let loader = CommitLogLoader::new(invalid.path().to_string_lossy().into_owned(), 16, true);
    let error = loader.load_optimized(counting_target_adapter()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(VALIDATION_OPEN_COUNT.load(Ordering::SeqCst), 0);

    let empty_last = TempDir::new().unwrap();
    let retained = create_segment(empty_last.path(), "00000000000000000000", 16);
    let removed = create_segment(empty_last.path(), "00000000000000000016", 0);
    VALIDATION_OPEN_COUNT.store(0, Ordering::SeqCst);
    let loader = CommitLogLoader::new(empty_last.path().to_string_lossy().into_owned(), 16, true);
    let (targets, statistics) = loader.load_optimized(counting_target_adapter()).unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].0.path, retained);
    assert_eq!(statistics.total_files, 1);
    assert_eq!(VALIDATION_OPEN_COUNT.load(Ordering::SeqCst), 1);
    assert!(!removed.exists());
}
