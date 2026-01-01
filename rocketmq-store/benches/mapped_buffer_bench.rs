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
use std::io::Write as IoWrite;
use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use memmap2::MmapMut;
use parking_lot::RwLock;
use rocketmq_store::log_file::mapped_file::MappedBuffer;
use tempfile::NamedTempFile;

/// Create a test mmap of specified size
fn create_test_mmap(size: usize) -> (NamedTempFile, Arc<RwLock<MmapMut>>) {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&vec![0u8; size]).unwrap();
    file.flush().unwrap();

    let file_handle = file.reopen().unwrap();
    let mmap = unsafe { MmapMut::map_mut(&file_handle).unwrap() };

    (file, Arc::new(RwLock::new(mmap)))
}

/// Benchmark sequential writes of different sizes
fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (_file, mmap) = create_test_mmap(1024 * 1024);
            let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();
            let data = vec![0xAAu8; size];

            b.iter(|| {
                for offset in (0..1024 * 1024 - size).step_by(size) {
                    buffer.write(offset, &data).unwrap();
                }
            });
        });
    }

    group.finish();
}

/// Benchmark random writes
fn bench_random_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_write");
    let size = 1024;

    group.throughput(Throughput::Bytes(size as u64));
    group.bench_function("random_1kb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();
        let data = vec![0xBBu8; size];

        // Pre-generate random offsets
        let offsets: Vec<usize> = (0..1000).map(|i| (i * 113) % (1024 * 1024 - size)).collect();

        b.iter(|| {
            for &offset in &offsets {
                buffer.write(offset, &data).unwrap();
            }
        });
    });

    group.finish();
}

/// Benchmark batch writes vs individual writes
fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");

    // Individual writes
    group.bench_function("individual_100x1kb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();
        let data = vec![0xCCu8; 1024];

        b.iter(|| {
            for i in 0..100 {
                buffer.write(i * 1024, &data).unwrap();
            }
        });
    });

    // Batch writes
    group.bench_function("batch_100x1kb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();
        let data = vec![0xCCu8; 1024];

        b.iter(|| {
            let writes: Vec<(usize, &[u8])> = (0..100).map(|i| (i * 1024, data.as_slice())).collect();
            buffer.batch_write(writes).unwrap();
        });
    });

    group.finish();
}

/// Benchmark reads (copy vs zero-copy)
fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    for size in [1024, 4096, 16384, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        // Copy read
        group.bench_with_input(BenchmarkId::new("copy", size), size, |b, &size| {
            let (_file, mmap) = create_test_mmap(1024 * 1024);
            let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();

            b.iter(|| {
                let data = buffer.read(0..size).unwrap();
                black_box(data);
            });
        });

        // Zero-copy read
        group.bench_with_input(BenchmarkId::new("zero_copy", size), size, |b, &size| {
            let (_file, mmap) = create_test_mmap(1024 * 1024);
            let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();

            b.iter(|| {
                let data = buffer.read_zero_copy(0..size).unwrap();
                black_box(data);
            });
        });
    }

    group.finish();
}

/// Benchmark flush operations
fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush");

    // Full flush
    group.bench_function("full_1mb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();

        // Write some data
        let data = vec![0xDDu8; 1024];
        for i in 0..1024 {
            buffer.write(i * 1024, &data).unwrap();
        }

        b.iter(|| {
            buffer.flush().unwrap();
        });
    });

    // Range flush
    group.bench_function("range_4kb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::new(mmap, 0, 1024 * 1024).unwrap();

        // Write some data
        let data = vec![0xDDu8; 4096];
        buffer.write(0, &data).unwrap();

        b.iter(|| {
            buffer.flush_range(0..4096).unwrap();
        });
    });

    group.finish();
}

/// Benchmark concurrent access patterns
fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");

    group.bench_function("8_threads_write", |b| {
        let (_file, mmap) = create_test_mmap(8 * 1024 * 1024);

        b.iter(|| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let mmap_clone = Arc::clone(&mmap);
                    std::thread::spawn(move || {
                        let offset = i * 1024 * 1024;
                        let buffer = MappedBuffer::new(mmap_clone, offset, 1024 * 1024).unwrap();
                        let data = vec![i as u8; 1024];

                        for j in 0..1024 {
                            buffer.write(j * 1024, &data).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_random_write,
    bench_batch_write,
    bench_read,
    bench_flush,
    bench_concurrent
);
criterion_main!(benches);
