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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use memmap2::MmapMut;
use rocketmq_store::MappedBuffer;
use tempfile::NamedTempFile;

const MAPPED_BUFFER_SIZE: usize = 1024 * 1024;
const RANDOM_WRITE_COUNT: usize = 1000;

fn sequential_write_bytes_per_iteration(buffer_size: usize, write_size: usize) -> u64 {
    assert!(write_size > 0, "write size must be non-zero");
    (0..buffer_size.saturating_sub(write_size)).step_by(write_size).count() as u64 * write_size as u64
}

/// Create a test mmap of specified size
fn create_test_mmap(size: usize) -> (NamedTempFile, MmapMut) {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&vec![0u8; size]).unwrap();
    file.flush().unwrap();

    let file_handle = file.reopen().unwrap();
    let mmap = unsafe { MmapMut::map_mut(&file_handle).unwrap() };

    (file, mmap)
}

/// Benchmark sequential writes of different sizes
fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        group.throughput(Throughput::Bytes(sequential_write_bytes_per_iteration(
            MAPPED_BUFFER_SIZE,
            *size,
        )));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (_file, mmap) = create_test_mmap(MAPPED_BUFFER_SIZE);
            let buffer = MappedBuffer::from_mmap(mmap, 0, MAPPED_BUFFER_SIZE).unwrap();
            let data = vec![0xAAu8; size];

            b.iter(|| {
                for offset in (0..MAPPED_BUFFER_SIZE - size).step_by(size) {
                    assert!(buffer.write(offset, &data));
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

    group.throughput(Throughput::Bytes((size * RANDOM_WRITE_COUNT) as u64));
    group.bench_function("random_1kb", |b| {
        let (_file, mmap) = create_test_mmap(MAPPED_BUFFER_SIZE);
        let buffer = MappedBuffer::from_mmap(mmap, 0, MAPPED_BUFFER_SIZE).unwrap();
        let data = vec![0xBBu8; size];

        // Pre-generate random offsets
        let offsets: Vec<usize> = (0..RANDOM_WRITE_COUNT)
            .map(|i| (i * 113) % (MAPPED_BUFFER_SIZE - size))
            .collect();

        b.iter(|| {
            for &offset in &offsets {
                assert!(buffer.write(offset, &data));
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
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024 * 1024).unwrap();
        let data = vec![0xCCu8; 1024];

        b.iter(|| {
            for i in 0..100 {
                assert!(buffer.write(i * 1024, &data));
            }
        });
    });

    // Batch writes
    group.bench_function("batch_100x1kb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024 * 1024).unwrap();
        let data = vec![0xCCu8; 1024];

        b.iter(|| {
            let writes: Vec<(usize, &[u8])> = (0..100).map(|i| (i * 1024, data.as_slice())).collect();
            buffer.batch_write(writes).unwrap();
        });
    });

    group.finish();
}

/// Benchmark owning copied reads
fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("copied_read");

    for size in [1024, 4096, 16384, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("bytes_copy", size), size, |b, &size| {
            let (_file, mmap) = create_test_mmap(1024 * 1024);
            let buffer = MappedBuffer::from_mmap(mmap, 0, 1024 * 1024).unwrap();

            b.iter(|| {
                let data = buffer.read_copy(0..size).unwrap();
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
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024 * 1024).unwrap();

        // Write some data
        let data = vec![0xDDu8; 1024];
        for i in 0..1024 {
            assert!(buffer.write(i * 1024, &data));
        }

        b.iter(|| {
            buffer.flush().unwrap();
        });
    });

    // Range flush
    group.bench_function("range_4kb", |b| {
        let (_file, mmap) = create_test_mmap(1024 * 1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024 * 1024).unwrap();

        // Write some data
        let data = vec![0xDDu8; 4096];
        assert!(buffer.write(0, &data));

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
        let root = MappedBuffer::from_mmap(mmap, 0, 8 * 1024 * 1024).unwrap();

        b.iter(|| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let root = root.clone();
                    std::thread::spawn(move || {
                        let offset = i * 1024 * 1024;
                        let buffer = root.region(offset..offset + 1024 * 1024).unwrap();
                        let data = vec![i as u8; 1024];

                        for j in 0..1024 {
                            assert!(buffer.write(j * 1024, &data));
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
