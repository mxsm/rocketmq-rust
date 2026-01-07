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

use std::hint::black_box;

use bytes::Bytes;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::MmapRegionSlice;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tempfile::TempDir;

fn create_test_file(size: usize) -> (TempDir, DefaultMappedFile) {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("00000000000000000000");
    let file_name = CheetahString::from(file_path.to_str().unwrap());

    let mapped_file = DefaultMappedFile::new(file_name, (size * 2) as u64);

    // Write some test data and update write position
    let test_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    mapped_file.get_mapped_file_mut()[0..size].copy_from_slice(&test_data);
    MappedFile::set_wrote_position(&mapped_file, size as i32);

    (temp_dir, mapped_file)
}

// Current implementation: copies data
fn get_bytes_with_copy(mapped_file: &DefaultMappedFile, pos: usize, size: usize) -> Bytes {
    let mmap = mapped_file.get_mapped_file();
    let slice = &mmap[pos..pos + size];
    let is_aligned = (slice.as_ptr() as usize) % 64 == 0;

    if (8192..=65536).contains(&size) && is_aligned {
        let mut vec = vec![0u8; size];
        unsafe {
            std::ptr::copy_nonoverlapping(slice.as_ptr(), vec.as_mut_ptr(), size);
        }
        Bytes::from(vec)
    } else {
        Bytes::copy_from_slice(slice)
    }
}

// Proposed implementation: true zero-copy
fn get_bytes_zero_copy(mapped_file: &DefaultMappedFile, pos: usize, size: usize) -> Bytes {
    Bytes::from_owner(MmapRegionSlice::new(mapped_file.get_mapped_file_arcmut(), pos, size))
}

fn benchmark_zero_copy_comparison(c: &mut Criterion) {
    let sizes = vec![
        ("4KB", 4 * 1024),
        ("8KB", 8 * 1024),
        ("16KB", 16 * 1024),
        ("32KB", 32 * 1024),
        ("64KB", 64 * 1024),
        ("128KB", 128 * 1024),
    ];

    for (name, size) in sizes {
        let mut group = c.benchmark_group(format!("zero_copy_comparison/{}", name));
        group.throughput(Throughput::Bytes(size as u64));

        let (_temp_dir, mapped_file) = create_test_file(size * 2);

        // Benchmark: Current implementation (with copy)
        group.bench_function("with_copy", |b| {
            b.iter(|| {
                let bytes = get_bytes_with_copy(&mapped_file, 0, black_box(size));
                black_box(bytes);
            });
        });

        // Benchmark: Proposed implementation (true zero-copy)
        group.bench_function("true_zero_copy", |b| {
            b.iter(|| {
                let bytes = get_bytes_zero_copy(&mapped_file, 0, black_box(size));
                black_box(bytes);
            });
        });

        group.finish();
    }
}

fn benchmark_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage/16KB");
    let size = 16 * 1024;

    let (_temp_dir, mapped_file) = create_test_file(size * 100);

    // Benchmark: Current - Multiple reads (allocates Vec each time)
    group.bench_function("with_copy_10_reads", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for i in 0..10 {
                let bytes = get_bytes_with_copy(&mapped_file, i * size, black_box(size));
                results.push(bytes);
            }
            black_box(results);
        });
    });

    // Benchmark: Proposed - Multiple reads (only reference counting)
    group.bench_function("true_zero_copy_10_reads", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for i in 0..10 {
                let bytes = get_bytes_zero_copy(&mapped_file, i * size, black_box(size));
                results.push(bytes);
            }
            black_box(results);
        });
    });

    group.finish();
}

fn benchmark_data_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_access/16KB");
    let size = 16 * 1024;

    let (_temp_dir, mapped_file) = create_test_file(size * 2);

    // Test data access after read
    group.bench_function("with_copy_access", |b| {
        b.iter(|| {
            let bytes = get_bytes_with_copy(&mapped_file, 0, black_box(size));
            // Access some data
            let sum: u64 = bytes[0..100].iter().map(|&b| b as u64).sum();
            black_box(sum);
        });
    });

    group.bench_function("true_zero_copy_access", |b| {
        b.iter(|| {
            let bytes = get_bytes_zero_copy(&mapped_file, 0, black_box(size));
            // Access some data
            let sum: u64 = bytes[0..100].iter().map(|&b| b as u64).sum();
            black_box(sum);
        });
    });

    group.finish();
}

fn benchmark_clone_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("clone_cost/16KB");
    let size = 16 * 1024;

    let (_temp_dir, mapped_file) = create_test_file(size * 2);

    // Benchmark: Cloning Bytes created with copy
    group.bench_function("with_copy_clone", |b| {
        let bytes = get_bytes_with_copy(&mapped_file, 0, size);
        b.iter(|| {
            let cloned = black_box(bytes.clone());
            black_box(cloned);
        });
    });

    // Benchmark: Cloning Bytes created with zero-copy
    group.bench_function("true_zero_copy_clone", |b| {
        let bytes = get_bytes_zero_copy(&mapped_file, 0, size);
        b.iter(|| {
            let cloned = black_box(bytes.clone());
            black_box(cloned);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_zero_copy_comparison,
    benchmark_memory_usage,
    benchmark_data_access,
    benchmark_clone_cost
);
criterion_main!(benches);
