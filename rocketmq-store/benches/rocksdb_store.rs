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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::Criterion;
use rocketmq_store::rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store::rocksdb::column_family::RocksDbColumnFamily;
use rocketmq_store::rocksdb::config::RocksDbConfig;
use rocketmq_store::rocksdb::iterator::RocksDbScanOptions;
use rocketmq_store::rocksdb::key::ConsumeQueueKey;
use rocketmq_store::rocksdb::store::KeyValueStore;
use rocketmq_store::rocksdb::store::RocksDbStore;
use rocketmq_store::rocksdb::value::ConsumeQueueValue;
use std::hint::black_box;
use tempfile::TempDir;

const POINT_READ_KEYS: usize = 1024;
const PREFIX_SCAN_KEYS: usize = 1000;
const BATCH_SIZE: usize = 256;
const PREFIX_SCAN_LIMIT: usize = 100;

struct BenchRocksDb {
    store: RocksDbStore,
    _temp_dir: TempDir,
}

impl BenchRocksDb {
    fn open(name: &str) -> Self {
        let temp_dir = tempfile::tempdir().expect("create rocksdb benchmark temp directory");
        let config = RocksDbConfig {
            enabled: true,
            path: temp_dir.path().join(name),
            ..RocksDbConfig::default()
        };
        let store = RocksDbStore::open(config).expect("open rocksdb benchmark store");
        Self {
            store,
            _temp_dir: temp_dir,
        }
    }
}

fn encode_cq_key(offset: i64) -> Vec<u8> {
    let key = ConsumeQueueKey {
        topic: "BenchmarkTopic".to_string(),
        queue_id: 3,
        cq_offset: offset,
    };
    let mut bytes = Vec::with_capacity(key.encoded_len());
    key.encode(&mut bytes).expect("encode consume queue key");
    bytes
}

fn encode_cq_value(offset: i64) -> Vec<u8> {
    let value = ConsumeQueueValue {
        commit_log_physical_offset: offset.saturating_mul(128),
        body_size: 128,
        tag_hash_code: 0x1020_3040_5060_7080,
        msg_store_time: 1_700_000_000_000_i64.saturating_add(offset),
    };
    let mut bytes = Vec::with_capacity(ConsumeQueueValue::ENCODED_LEN);
    value.encode(&mut bytes).expect("encode consume queue value");
    bytes
}

fn bench_codec(c: &mut Criterion) {
    c.bench_function("rocksdb/cq_key_codec", |b| {
        let key = ConsumeQueueKey {
            topic: "BenchmarkTopic".to_string(),
            queue_id: 3,
            cq_offset: 123_456_789,
        };
        b.iter(|| {
            let mut bytes = Vec::with_capacity(key.encoded_len());
            key.encode(&mut bytes).expect("encode consume queue key");
            black_box(bytes);
        });
    });

    c.bench_function("rocksdb/cq_value_codec", |b| {
        let value = ConsumeQueueValue {
            commit_log_physical_offset: 9_876_543_210,
            body_size: 128,
            tag_hash_code: 0x1020_3040_5060_7080,
            msg_store_time: 1_700_000_000_000,
        };
        b.iter(|| {
            let mut bytes = Vec::with_capacity(ConsumeQueueValue::ENCODED_LEN);
            value.encode(&mut bytes).expect("encode consume queue value");
            let decoded = ConsumeQueueValue::decode(&bytes).expect("decode consume queue value");
            black_box(decoded);
        });
    });
}

fn bench_batch_write(c: &mut Criterion) {
    let db = BenchRocksDb::open("batch-write");
    let cf = RocksDbColumnFamily::Default.name();
    let mut next_offset = 0_i64;

    c.bench_function("rocksdb/batch_write_256", |b| {
        b.iter_batched(
            || {
                let start = next_offset;
                next_offset = next_offset.saturating_add(BATCH_SIZE as i64);
                let mut batch = RocksDbWriteBatch::with_capacity(BATCH_SIZE);
                for offset in start..start + BATCH_SIZE as i64 {
                    batch.put_cf(cf, encode_cq_key(offset), encode_cq_value(offset));
                }
                batch
            },
            |batch| {
                db.store.write_batch(&batch).expect("write rocksdb benchmark batch");
            },
            BatchSize::SmallInput,
        );
    });

    black_box(db);
}

fn bench_point_read(c: &mut Criterion) {
    let db = BenchRocksDb::open("point-read");
    let cf = RocksDbColumnFamily::Default.name();
    for offset in 0..POINT_READ_KEYS as i64 {
        db.store
            .put_cf(cf, &encode_cq_key(offset), &encode_cq_value(offset))
            .expect("prefill point read key");
    }

    let mut next = 0_usize;
    c.bench_function("rocksdb/point_read", |b| {
        b.iter(|| {
            let key = encode_cq_key(next as i64);
            next = (next + 1) % POINT_READ_KEYS;
            let value = db.store.get_cf(cf, &key).expect("read rocksdb benchmark key");
            black_box(value);
        });
    });

    black_box(db);
}

fn bench_prefix_scan(c: &mut Criterion) {
    let db = BenchRocksDb::open("prefix-scan");
    let cf = RocksDbColumnFamily::Default.name();
    let prefix = b"scan:";

    for offset in 0..PREFIX_SCAN_KEYS {
        let key = format!("scan:{offset:08}");
        let value = encode_cq_value(offset as i64);
        db.store
            .put_cf(cf, key.as_bytes(), &value)
            .expect("prefill prefix scan key");
    }

    let options = RocksDbScanOptions::prefix(cf, prefix.to_vec(), PREFIX_SCAN_LIMIT);
    c.bench_function("rocksdb/prefix_scan_100", |b| {
        b.iter(|| {
            let rows = db.store.prefix_scan(&options).expect("scan rocksdb benchmark prefix");
            black_box(rows);
        });
    });

    black_box(db);
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_codec(c);
    bench_batch_write(c);
    bench_point_read(c);
    bench_prefix_scan(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
