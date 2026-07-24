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

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use rocketmq_store::log_file::mapped_file::MappedFile;
use rocketmq_store_local::mapped_file::MappedWriteLease;
use tempfile::TempDir;

fn create_test_file(size: usize) -> (TempDir, DefaultMappedFile) {
    let temp_dir = TempDir::new().expect("temporary benchmark directory");
    let file_path = temp_dir.path().join("00000000000000000000");
    let file_name = CheetahString::from(file_path.to_string_lossy().into_owned());
    let mapped_file = DefaultMappedFile::new(file_name, size as u64);
    (temp_dir, mapped_file)
}

fn benchmark_mapped_write_lease(c: &mut Criterion) {
    let mut group = c.benchmark_group("mapped_write_lease");

    for size in [256_usize, 1024, 4096, 16 * 1024, 64 * 1024] {
        let payload = vec![0x5a; size];
        group.throughput(Throughput::Bytes(size as u64));

        let (_facade_dir, facade_file) = create_test_file(size);
        group.bench_with_input(BenchmarkId::new("append_facade", size), &size, |b, _| {
            b.iter(|| {
                facade_file.set_wrote_position(0);
                black_box(facade_file.append_message_bytes(black_box(&payload)));
            });
        });

        let (_lease_dir, lease_file) = create_test_file(size);
        group.bench_with_input(BenchmarkId::new("explicit_lease", size), &size, |b, _| {
            b.iter(|| {
                lease_file.set_wrote_position(0);
                let mut lease = lease_file.reserve_write(size).expect("write reservation");
                lease.buffer_mut().copy_from_slice(black_box(&payload));
                black_box(lease.commit(size, None).expect("write commit"));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_mapped_write_lease);
criterion_main!(benches);
