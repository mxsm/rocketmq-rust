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

use std::fs::File;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;

use cheetah_string::CheetahString;
use memmap2::Mmap;
use memmap2::MmapMut;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::LazyMmapStats;
use rocketmq_store_local::mapped_file::MappedMemory;
use rocketmq_store_local::mapped_file::NativeMappedMemory;
use rocketmq_store_local::mapped_file::ReadOnlyMappedMemory;

fn mapped_path(directory: &tempfile::TempDir) -> CheetahString {
    CheetahString::from(
        directory
            .path()
            .join("00000000000000000000")
            .to_string_lossy()
            .into_owned(),
    )
}

#[test]
fn eager_mapping_starts_initialized_without_lazy_statistics() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mapped_file =
        DefaultMappedFile::<NativeMappedMemory>::try_new(mapped_path(&directory), 8).expect("eager mapped file");

    assert!(!mapped_file.is_lazy_mmap_enabled());
    assert!(mapped_file.is_mapped());
    assert_eq!(mapped_file.lazy_mmap_stats(), LazyMmapStats::default());
}

#[test]
fn lazy_mapping_starts_eligible_and_uninitialized() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mapped_file = DefaultMappedFile::<NativeMappedMemory>::try_new_lazy_read_only(mapped_path(&directory), 8)
        .expect("lazy mapped file");

    assert!(mapped_file.is_lazy_mmap_enabled());
    assert!(!mapped_file.is_mapped());
    assert_eq!(
        mapped_file.lazy_mmap_stats(),
        LazyMmapStats {
            eligible_files: 1,
            mapped_files: 0,
            map_operations: 0,
            map_failures: 0,
            total_millis: 0,
            last_millis: 0,
        }
    );
}

#[test]
fn concurrent_lazy_callers_publish_one_generation() {
    const CALLERS: usize = 8;
    let directory = tempfile::tempdir().expect("temporary directory");
    let mapped_file = Arc::new(
        DefaultMappedFile::<NativeMappedMemory>::try_new_lazy_read_only(mapped_path(&directory), 8)
            .expect("lazy mapped file"),
    );
    let start = Arc::new(Barrier::new(CALLERS + 1));

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let mapped_file = Arc::clone(&mapped_file);
            let start = Arc::clone(&start);
            handles.push(scope.spawn(move || {
                start.wait();
                mapped_file.with_mapped_slice(<[u8]>::len)
            }));
        }

        start.wait();
        for handle in handles {
            assert_eq!(
                handle.join().expect("caller does not panic").expect("mapping succeeds"),
                8
            );
        }
    });

    assert!(mapped_file.is_mapped());
    assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 1);
    assert_eq!(mapped_file.lazy_mmap_stats().map_failures, 0);
}

static RETRY_MAP_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

struct RetryMappedMemory(MmapMut);
struct RetryReadOnlyMappedMemory(Mmap);

// SAFETY: the mapping remains stable for the value lifetime and DefaultMappedFile serializes all
// mutation through its writer fence.
unsafe impl MappedMemory for RetryMappedMemory {
    type ReadOnly = RetryReadOnlyMappedMemory;

    unsafe fn map_mut(file: &File) -> io::Result<Self> {
        if RETRY_MAP_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(io::Error::other("injected first mapping failure"));
        }
        // SAFETY: DefaultMappedFile sizes and owns the file for the mapping lifetime.
        unsafe { MmapMut::map_mut(file).map(Self) }
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn as_mut_ptr(&self) -> *mut u8 {
        self.0.as_ptr().cast_mut()
    }

    fn flush(&self) -> io::Result<()> {
        self.0.flush()
    }

    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()> {
        self.0.flush_range(offset, len)
    }
}

// SAFETY: this mapping is immutable, stable, and exposes no mutation path.
unsafe impl ReadOnlyMappedMemory for RetryReadOnlyMappedMemory {
    unsafe fn map(file: &File) -> io::Result<Self> {
        // SAFETY: DefaultMappedFile keeps the segment size stable while this generation is live.
        unsafe { Mmap::map(file).map(Self) }
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[test]
fn failed_lazy_initialization_is_retryable_and_accounted() {
    RETRY_MAP_ATTEMPTS.store(0, Ordering::SeqCst);
    let directory = tempfile::tempdir().expect("temporary directory");
    let mapped_file = DefaultMappedFile::<RetryMappedMemory>::try_new_lazy_read_only(mapped_path(&directory), 8)
        .expect("lazy mapped file");

    assert!(mapped_file.with_mapped_slice(<[u8]>::len).is_err());
    assert!(!mapped_file.is_mapped());
    assert_eq!(mapped_file.lazy_mmap_stats().map_failures, 1);

    assert_eq!(mapped_file.with_mapped_slice(<[u8]>::len).expect("retry maps"), 8);
    assert_eq!(RETRY_MAP_ATTEMPTS.load(Ordering::SeqCst), 2);
    assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 1);
    assert_eq!(mapped_file.lazy_mmap_stats().map_failures, 1);
}
