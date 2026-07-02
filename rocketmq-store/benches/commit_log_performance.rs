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

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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
use rocketmq_store::base::message_status_enum::PutMessageStatus;
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

#[derive(Clone, Copy)]
struct LockProfileScenario {
    name: &'static str,
    flush_disk_type: FlushDiskType,
    queue_count: usize,
    body_size: usize,
    messages_per_queue: u64,
}

const LOCK_PROFILE_ARTIFACT_SCENARIOS: [LockProfileScenario; 8] = [
    LockProfileScenario {
        name: "async_1q_256b",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 1,
        body_size: 256,
        messages_per_queue: 32,
    },
    LockProfileScenario {
        name: "async_1q_1kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 1,
        body_size: 1024,
        messages_per_queue: 32,
    },
    LockProfileScenario {
        name: "async_1q_4kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 1,
        body_size: 4096,
        messages_per_queue: 32,
    },
    LockProfileScenario {
        name: "async_4q_1kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 4,
        body_size: 1024,
        messages_per_queue: 16,
    },
    LockProfileScenario {
        name: "async_8q_1kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 8,
        body_size: 1024,
        messages_per_queue: 16,
    },
    LockProfileScenario {
        name: "async_16q_1kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 16,
        body_size: 1024,
        messages_per_queue: 16,
    },
    LockProfileScenario {
        name: "sync_1q_1kb",
        flush_disk_type: FlushDiskType::SyncFlush,
        queue_count: 1,
        body_size: 1024,
        messages_per_queue: 8,
    },
    LockProfileScenario {
        name: "sync_4q_1kb",
        flush_disk_type: FlushDiskType::SyncFlush,
        queue_count: 4,
        body_size: 1024,
        messages_per_queue: 4,
    },
];

const LOCK_PROFILE_CRITERION_SCENARIOS: [LockProfileScenario; 3] = [
    LockProfileScenario {
        name: "async_1q_1kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 1,
        body_size: 1024,
        messages_per_queue: 1,
    },
    LockProfileScenario {
        name: "async_16q_1kb",
        flush_disk_type: FlushDiskType::AsyncFlush,
        queue_count: 16,
        body_size: 1024,
        messages_per_queue: 1,
    },
    LockProfileScenario {
        name: "sync_1q_1kb",
        flush_disk_type: FlushDiskType::SyncFlush,
        queue_count: 1,
        body_size: 1024,
        messages_per_queue: 1,
    },
];

fn new_async_flush_bench_store() -> BenchStore {
    new_bench_store(FlushDiskType::AsyncFlush)
}

fn new_sync_flush_bench_store() -> BenchStore {
    new_bench_store(FlushDiskType::SyncFlush)
}

fn new_bench_store(flush_disk_type: FlushDiskType) -> BenchStore {
    let temp_dir = TempDir::new().expect("create temp dir for benchmark");
    let mut message_store_config = MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        flush_disk_type,
        ha_listen_port: 0,
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

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-store should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

async fn start_store_if_needed(bench_store: &BenchStore, flush_disk_type: FlushDiskType) {
    if flush_disk_type != FlushDiskType::SyncFlush {
        return;
    }

    bench_store
        .store
        .clone()
        .mut_from_ref()
        .init()
        .await
        .expect("init sync flush benchmark store");
    assert!(bench_store.store.clone().mut_from_ref().load().await, "load store");
    bench_store
        .store
        .clone()
        .mut_from_ref()
        .start()
        .await
        .expect("start sync flush benchmark store");
}

fn run_lock_profile_messages(runtime: &Runtime, bench_store: &BenchStore, scenario: LockProfileScenario) -> u64 {
    runtime.block_on(async {
        let mut put_ok_total = 0_u64;
        for round in 0..scenario.messages_per_queue {
            if scenario.queue_count == 1 {
                let msg = create_test_message("BenchLockProfileTopic", 0, scenario.body_size, round);
                let status = bench_store
                    .store
                    .clone()
                    .mut_from_ref()
                    .put_message(msg)
                    .await
                    .put_message_status();
                assert_eq!(status, PutMessageStatus::PutOk);
                put_ok_total += 1;
                continue;
            }

            let mut handles = Vec::with_capacity(scenario.queue_count);
            for queue_id in 0..scenario.queue_count {
                let store = bench_store.store.clone();
                let key_seed = round * scenario.queue_count as u64 + queue_id as u64;
                let msg = create_test_message("BenchLockProfileTopic", queue_id as i32, scenario.body_size, key_seed);
                handles.push(tokio::spawn(async move {
                    store.mut_from_ref().put_message(msg).await.put_message_status()
                }));
            }

            for handle in handles {
                let status = handle.await.expect("join lock profile benchmark task");
                assert_eq!(status, PutMessageStatus::PutOk);
                put_ok_total += 1;
            }
        }
        put_ok_total
    })
}

fn put_message_lock_profile_json(bench_store: &BenchStore) -> serde_json::Value {
    let runtime_info = bench_store.store.get_runtime_info();
    serde_json::json!({
        "acquire_total": runtime_info["putMessageLockAcquireTotal"].parse::<u64>().expect("parse acquire total"),
        "wait_total_millis": runtime_info["putMessageLockWaitTotalMillis"].parse::<u64>().expect("parse wait total"),
        "wait_max_millis": runtime_info["putMessageLockWaitMaxMillis"].parse::<u64>().expect("parse wait max"),
        "hold_total_millis": runtime_info["putMessageLockHoldTotalMillis"].parse::<u64>().expect("parse hold total"),
        "hold_max_millis": runtime_info["putMessageLockHoldMaxMillis"].parse::<u64>().expect("parse hold max"),
    })
}

fn write_commit_log_lock_profile_artifact() {
    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let mut cases = Vec::with_capacity(LOCK_PROFILE_ARTIFACT_SCENARIOS.len());

    for scenario in LOCK_PROFILE_ARTIFACT_SCENARIOS {
        let runtime = Runtime::new().expect("create runtime");
        let bench_store = new_bench_store(scenario.flush_disk_type);
        runtime.block_on(start_store_if_needed(&bench_store, scenario.flush_disk_type));
        let put_ok_total = run_lock_profile_messages(&runtime, &bench_store, scenario);
        let profile = put_message_lock_profile_json(&bench_store);
        assert_eq!(profile["acquire_total"], serde_json::json!(put_ok_total));
        cases.push(serde_json::json!({
            "name": scenario.name,
            "flush_disk_type": scenario.flush_disk_type.get_flush_disk_type(),
            "queue_count": scenario.queue_count,
            "body_size": scenario.body_size,
            "messages_per_queue": scenario.messages_per_queue,
            "put_ok_total": put_ok_total,
            "lock_profile": profile,
        }));
    }

    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("CommitLog benchmark artifact directory should be created");
    let payload = serde_json::json!({
        "case": "commit_log_put_message_lock_profile",
        "generated_at_unix_ms": generated_at_unix_ms,
        "cases": cases,
    });
    let path = output_dir.join("commit-log-lock-profile-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("CommitLog lock profile report should serialize"),
    )
    .expect("CommitLog lock profile report should be written");
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

fn bench_commit_log_lock_profile_contention(c: &mut Criterion) {
    write_commit_log_lock_profile_artifact();

    let mut group = c.benchmark_group("phase5/commit_log_lock_profile_contention");
    group.sample_size(10);

    for scenario in LOCK_PROFILE_CRITERION_SCENARIOS {
        group.throughput(Throughput::Elements(
            scenario.queue_count as u64 * scenario.messages_per_queue,
        ));
        group.bench_with_input(BenchmarkId::from_parameter(scenario.name), &scenario, |b, &scenario| {
            let runtime = Runtime::new().expect("create runtime");
            let bench_store = new_bench_store(scenario.flush_disk_type);
            runtime.block_on(start_store_if_needed(&bench_store, scenario.flush_disk_type));

            b.iter_custom(|iters| {
                let started = Instant::now();
                for _ in 0..iters {
                    black_box(run_lock_profile_messages(&runtime, &bench_store, scenario));
                }
                let elapsed = started.elapsed();
                black_box(put_message_lock_profile_json(&bench_store));
                elapsed
            });
        });
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

fn bench_sync_flush_tail_latency_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase6/sync_flush_tail_latency_baseline");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_1KiB_message", |b| {
        let runtime = Runtime::new().expect("create runtime");
        let bench_store = new_sync_flush_bench_store();
        runtime.block_on(async {
            bench_store
                .store
                .clone()
                .mut_from_ref()
                .init()
                .await
                .expect("init sync flush benchmark store");
            assert!(bench_store.store.clone().mut_from_ref().load().await, "load store");
            bench_store
                .store
                .clone()
                .mut_from_ref()
                .start()
                .await
                .expect("start sync flush benchmark store");
        });
        let counter = AtomicU64::new(0);

        b.iter(|| {
            let key_seed = counter.fetch_add(1, Ordering::Relaxed);
            let msg = create_test_message("BenchSyncFlushTopic", 0, 1024, key_seed);
            let (status, elapsed) = runtime.block_on(async {
                let started = Instant::now();
                let status = bench_store
                    .store
                    .clone()
                    .mut_from_ref()
                    .put_message(msg)
                    .await
                    .put_message_status();
                (status, started.elapsed())
            });
            black_box((status, elapsed));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_async_flush_single_message,
    bench_async_flush_multi_queue,
    bench_commit_log_lock_profile_contention,
    bench_reput_once_after_batch,
    bench_sync_flush_tail_latency_baseline,
);
criterion_main!(benches);
