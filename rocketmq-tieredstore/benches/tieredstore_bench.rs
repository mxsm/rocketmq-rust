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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_error::RocketMQError;
use rocketmq_tieredstore::file::ConsumeQueueUnit;
use rocketmq_tieredstore::file::IndexFileSegment;
use rocketmq_tieredstore::file::TieredFlatFileStore;
use rocketmq_tieredstore::file::TieredIndexEntry;
use rocketmq_tieredstore::metadata::JsonMetadataStore;
use rocketmq_tieredstore::provider::MemoryProvider;
use rocketmq_tieredstore::TieredStoreConfig;
use tokio::runtime::Builder;

fn bench_flat_file_append_commit(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("failed to build benchmark runtime: {error}"));
    let temp_dir = tempfile::tempdir().unwrap_or_else(|error| panic!("failed to create temp dir: {error}"));
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: temp_dir.path().to_path_buf(),
        backend_provider: "memory".to_owned(),
        commit_log_segment_size: 1024 * 1024,
        consume_queue_segment_size: 20 * 1024,
        file_reserved_time: Duration::from_secs(60 * 60),
        max_pending_tasks: 65_536,
        ..TieredStoreConfig::default()
    });
    let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
    let flat_file_store = Arc::new(TieredFlatFileStore::new(
        config,
        metadata_store,
        MemoryProvider::default(),
    ));
    let flat_file = flat_file_store
        .get_or_create("BenchTopic".to_owned(), 0)
        .unwrap_or_else(|error| panic!("failed to create flat file: {error}"));
    let queue_offset = AtomicI64::new(0);
    let payload = Bytes::from(vec![b'a'; 1024]);

    c.bench_function("tieredstore_memory_flat_file_append_commit_1kb", |b| {
        b.iter(|| {
            let offset = queue_offset.fetch_add(1, Ordering::Relaxed);
            let body = black_box(payload.clone());
            runtime
                .block_on(async {
                    let commit_log_offset = flat_file.append_commit_log(body.clone(), offset).await?;
                    flat_file
                        .append_consume_queue(
                            offset,
                            ConsumeQueueUnit {
                                commit_log_offset: commit_log_offset as i64,
                                size: body.len() as i32,
                                tags_code: 0,
                            },
                            offset,
                        )
                        .await?;
                    if offset % 64 == 63 {
                        flat_file.commit().await?;
                    }
                    Ok::<(), RocketMQError>(())
                })
                .unwrap_or_else(|error| panic!("flat file benchmark iteration failed: {error}"));
        });
    });

    runtime
        .block_on(flat_file.commit())
        .unwrap_or_else(|error| panic!("failed to commit benchmark flat file: {error}"));
}

fn bench_index_query(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("failed to build benchmark runtime: {error}"));
    let temp_dir = tempfile::tempdir().unwrap_or_else(|error| panic!("failed to create temp dir: {error}"));
    let config = TieredStoreConfig {
        store_path_root_dir: temp_dir.path().to_path_buf(),
        backend_provider: "memory".to_owned(),
        index_file_max_hash_slot_num: 4_096,
        index_file_max_index_num: 65_536,
        ..TieredStoreConfig::default()
    };
    let index_file = IndexFileSegment::with_limits(
        IndexFileSegment::<MemoryProvider>::default_path().to_owned(),
        MemoryProvider::default(),
        config.index_file_max_hash_slot_num as usize,
        config.index_file_max_index_num as usize,
    );

    runtime
        .block_on(async {
            for i in 0..10_000 {
                index_file
                    .append_entry(&TieredIndexEntry {
                        topic: "BenchTopic".to_owned(),
                        key: format!("key-{}", i % 128),
                        queue_id: 0,
                        queue_offset: i,
                        commit_log_offset: i as u64 * 1024,
                        message_size: 1024,
                        store_timestamp: i,
                    })
                    .await?;
            }
            Ok::<(), RocketMQError>(())
        })
        .unwrap_or_else(|error| panic!("failed to seed index benchmark: {error}"));

    c.bench_function("tieredstore_memory_index_query_10k", |b| {
        b.iter(|| {
            runtime
                .block_on(async {
                    let entries = index_file
                        .query_entries(black_box("BenchTopic"), black_box("key-42"), 32, 0, i64::MAX)
                        .await?;
                    black_box(entries);
                    Ok::<(), RocketMQError>(())
                })
                .unwrap_or_else(|error| panic!("index query benchmark iteration failed: {error}"));
        });
    });
}

criterion_group!(benches, bench_flat_file_append_commit, bench_index_query);
criterion_main!(benches);
