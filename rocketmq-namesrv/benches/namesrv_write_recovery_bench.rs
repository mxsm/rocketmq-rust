// Copyright 2026 The RocketMQ Rust Authors
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

//! Broker heartbeat and failure-recovery benchmarks for the NameServer write path.
//!
//! Run with:
//! `cargo bench -p rocketmq-namesrv --bench namesrv_write_recovery_bench`

use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::Arc;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_namesrv::config::ExpiryIndexMode;
use rocketmq_namesrv::route::tables::BrokerLiveInfo;
use rocketmq_namesrv::route::tables::BrokerLiveTable;
use rocketmq_namesrv::route::types::BrokerAddrInfo;
use rocketmq_protocol::protocol::DataVersion;

const BROKER_COUNT: usize = 10_000;
const CURRENT_TIME: u64 = 1_000_000;

fn broker(index: usize) -> Arc<BrokerAddrInfo> {
    Arc::new(BrokerAddrInfo::new(
        "benchmark-cluster",
        format!("10.0.{}:{}", index / 250 + 1, 10_000 + index % 250),
    ))
}

fn fixture(mode: ExpiryIndexMode, expired_count: usize) -> (BrokerLiveTable, Vec<Arc<BrokerAddrInfo>>) {
    let table = BrokerLiveTable::with_capacity_and_expiry_index(BROKER_COUNT, mode);
    let brokers = (0..BROKER_COUNT).map(broker).collect::<Vec<_>>();
    for (index, broker) in brokers.iter().enumerate() {
        let last_update = if index < expired_count { 0 } else { CURRENT_TIME };
        table.register(
            Arc::clone(broker),
            BrokerLiveInfo::new(
                last_update,
                DataVersion::default(),
                SocketAddr::from(([127, 0, 0, 1], (10_000 + index % 250) as u16)),
                CheetahString::from_string(format!("channel-{index}")),
            )
            .with_timeout(120_000),
        );
    }
    (table, brokers)
}

fn bench_expiry_lookup(c: &mut Criterion) {
    for (label, expired_count) in [("10pct", 1_000), ("50pct", 5_000), ("100pct", 10_000)] {
        let (full_scan, _) = fixture(ExpiryIndexMode::Off, expired_count);
        let (indexed, _) = fixture(ExpiryIndexMode::Active, expired_count);
        let mut group = c.benchmark_group(format!("namesrv-expiry-10k-brokers-{label}-expired"));
        group.bench_function("full-scan", |b| {
            b.iter(|| black_box(full_scan.get_expired_brokers(CURRENT_TIME)))
        });
        group.bench_function("deadline-index", |b| {
            b.iter(|| black_box(indexed.get_indexed_expired_brokers(CURRENT_TIME)))
        });
        group.finish();
    }
}

fn bench_heartbeat_update(c: &mut Criterion) {
    let (without_index, off_brokers) = fixture(ExpiryIndexMode::Off, 1_000);
    let (with_index, active_brokers) = fixture(ExpiryIndexMode::Active, 1_000);
    let mut sequence = 0_usize;
    let mut group = c.benchmark_group("namesrv-heartbeat-10k-brokers");
    group.bench_function("atomic-only", |b| {
        b.iter(|| {
            let index = sequence % BROKER_COUNT;
            sequence = sequence.wrapping_add(1);
            black_box(without_index.update_heartbeat(&off_brokers[index], CURRENT_TIME + sequence as u64))
        })
    });
    group.bench_function("atomic-plus-deadline-index", |b| {
        b.iter(|| {
            let index = sequence % BROKER_COUNT;
            sequence = sequence.wrapping_add(1);
            black_box(with_index.update_heartbeat(&active_brokers[index], CURRENT_TIME + sequence as u64))
        })
    });
    group.finish();
}

criterion_group!(benches, bench_expiry_lookup, bench_heartbeat_update);
criterion_main!(benches);
