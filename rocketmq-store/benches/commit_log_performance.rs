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

//! CommitLog performance baselines for the default local-file store path.
//!
//! Run with:
//! `cargo bench -p rocketmq-store --bench commit_log_performance`

use std::hint::black_box;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use tempfile::TempDir;
use tokio::runtime::Runtime;

struct BenchStore {
    store: ArcMut<LocalFileMessageStore>,
    _temp_dir: TempDir,
}

fn new_async_flush_bench_store() -> BenchStore {
    let temp_dir = TempDir::new().expect("create temp dir for benchmark");
    let mut message_store_config = MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        flush_disk_type: FlushDiskType::AsyncFlush,
        ..MessageStoreConfig::default()
    };
    message_store_config.mapped_file_size_commit_log = 1024 * 1024 * 256;

    let mut store = ArcMut::new(LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
        None,
        false,
    ));
    let store_clone = store.clone();
    store.set_message_store_arc(store_clone);

    BenchStore {
        store,
        _temp_dir: temp_dir,
    }
}

fn create_test_message(topic: &str, queue_id: i32, body_size: usize, key_seed: u64) -> MessageExtBrokerInner {
    let body = vec![b'X'; body_size];
    let mut message = Message::builder()
        .topic(CheetahString::from(topic))
        .body(body)
        .build_unchecked();
    message.set_tags(CheetahString::from_static_str("BenchTag"));
    message.set_keys(CheetahString::from(format!("bench-key-{key_seed}")));

    let mut inner = MessageExtBrokerInner {
        message_ext_inner: MessageExt {
            message,
            ..Default::default()
        },
        ..Default::default()
    };
    inner.message_ext_inner.set_queue_id(queue_id);
    inner
}

fn bench_async_flush_single_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase5/async_flush_single_message");
    group.sample_size(20);

    for body_size in [256_usize, 1024, 4096] {
        group.throughput(Throughput::Bytes(body_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{body_size}B")),
            &body_size,
            |b, &body_size| {
                let runtime = Runtime::new().expect("create runtime");
                let bench_store = new_async_flush_bench_store();
                let counter = AtomicU64::new(0);

                b.iter(|| {
                    let key_seed = counter.fetch_add(1, Ordering::Relaxed);
                    let msg = create_test_message("BenchTopic", 0, body_size, key_seed);
                    let status = runtime.block_on(async {
                        bench_store
                            .store
                            .clone()
                            .mut_from_ref()
                            .put_message(msg)
                            .await
                            .put_message_status()
                    });
                    black_box(status);
                });
            },
        );
    }

    group.finish();
}

fn bench_async_flush_multi_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase5/async_flush_multi_queue");
    group.sample_size(10);

    for queue_count in [4_usize, 8, 16] {
        group.throughput(Throughput::Elements(queue_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{queue_count}_queues")),
            &queue_count,
            |b, &queue_count| {
                let runtime = Runtime::new().expect("create runtime");
                let bench_store = new_async_flush_bench_store();
                let counter = AtomicU64::new(0);

                b.iter(|| {
                    runtime.block_on(async {
                        let mut handles = Vec::with_capacity(queue_count);
                        for queue_id in 0..queue_count {
                            let store = bench_store.store.clone();
                            let key_seed = counter.fetch_add(1, Ordering::Relaxed);
                            let msg = create_test_message("BenchConcurrentTopic", queue_id as i32, 1024, key_seed);
                            handles.push(tokio::spawn(async move {
                                store.mut_from_ref().put_message(msg).await.put_message_status()
                            }));
                        }

                        for handle in handles {
                            black_box(handle.await.expect("join benchmark task"));
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_reput_once_after_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase5/reput_once_after_batch");
    group.sample_size(10);

    for batch_size in [32_usize, 64, 128] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{batch_size}_messages")),
            &batch_size,
            |b, &batch_size| {
                let runtime = Runtime::new().expect("create runtime");
                let counter = AtomicU64::new(0);

                b.iter_batched(
                    || {
                        let bench_store = new_async_flush_bench_store();
                        runtime.block_on(async {
                            for _ in 0..batch_size {
                                let key_seed = counter.fetch_add(1, Ordering::Relaxed);
                                let msg = create_test_message("BenchReputTopic", 0, 512, key_seed);
                                black_box(
                                    bench_store
                                        .store
                                        .clone()
                                        .mut_from_ref()
                                        .put_message(msg)
                                        .await
                                        .put_message_status(),
                                );
                            }
                        });
                        bench_store
                    },
                    |bench_store| {
                        runtime.block_on(async {
                            bench_store.store.mut_from_ref().reput_once().await;
                        });
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_async_flush_single_message,
    bench_async_flush_multi_queue,
    bench_reput_once_after_batch,
);
criterion_main!(benches);
