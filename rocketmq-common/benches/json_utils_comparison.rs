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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
#[cfg(feature = "simd")]
use rocketmq_common::utils::simd_json_utils::SimdJsonUtils;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct BrokerData {
    broker_name: String,
    broker_addrs: Vec<String>,
    cluster: String,
    enable_acting_master: bool,
    zone_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct QueueData {
    broker_name: String,
    read_queue_nums: i32,
    write_queue_nums: i32,
    perm: i32,
    topic_sys_flag: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct TopicRouteData {
    order_topic_conf: String,
    queue_datas: Vec<QueueData>,
    broker_datas: Vec<BrokerData>,
}

fn create_small_data() -> TopicRouteData {
    TopicRouteData {
        order_topic_conf: "".to_string(),
        queue_datas: vec![QueueData {
            broker_name: "broker-a".to_string(),
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            topic_sys_flag: 0,
        }],
        broker_datas: vec![BrokerData {
            broker_name: "broker-a".to_string(),
            broker_addrs: vec!["192.168.1.100:10911".to_string()],
            cluster: "DefaultCluster".to_string(),
            enable_acting_master: false,
            zone_name: "zone-1".to_string(),
        }],
    }
}

fn create_medium_data() -> TopicRouteData {
    TopicRouteData {
        order_topic_conf: "".to_string(),
        queue_datas: vec![
            QueueData {
                broker_name: "broker-a".to_string(),
                read_queue_nums: 8,
                write_queue_nums: 8,
                perm: 6,
                topic_sys_flag: 0,
            },
            QueueData {
                broker_name: "broker-b".to_string(),
                read_queue_nums: 8,
                write_queue_nums: 8,
                perm: 6,
                topic_sys_flag: 0,
            },
            QueueData {
                broker_name: "broker-c".to_string(),
                read_queue_nums: 8,
                write_queue_nums: 8,
                perm: 6,
                topic_sys_flag: 0,
            },
            QueueData {
                broker_name: "broker-d".to_string(),
                read_queue_nums: 8,
                write_queue_nums: 8,
                perm: 6,
                topic_sys_flag: 0,
            },
        ],
        broker_datas: vec![
            BrokerData {
                broker_name: "broker-a".to_string(),
                broker_addrs: vec!["192.168.1.100:10911".to_string(), "192.168.1.101:10911".to_string()],
                cluster: "DefaultCluster".to_string(),
                enable_acting_master: false,
                zone_name: "zone-1".to_string(),
            },
            BrokerData {
                broker_name: "broker-b".to_string(),
                broker_addrs: vec!["192.168.1.102:10911".to_string(), "192.168.1.103:10911".to_string()],
                cluster: "DefaultCluster".to_string(),
                enable_acting_master: false,
                zone_name: "zone-2".to_string(),
            },
            BrokerData {
                broker_name: "broker-c".to_string(),
                broker_addrs: vec!["192.168.1.104:10911".to_string(), "192.168.1.105:10911".to_string()],
                cluster: "DefaultCluster".to_string(),
                enable_acting_master: false,
                zone_name: "zone-3".to_string(),
            },
            BrokerData {
                broker_name: "broker-d".to_string(),
                broker_addrs: vec!["192.168.1.106:10911".to_string(), "192.168.1.107:10911".to_string()],
                cluster: "DefaultCluster".to_string(),
                enable_acting_master: false,
                zone_name: "zone-4".to_string(),
            },
        ],
    }
}

fn create_large_data() -> TopicRouteData {
    let mut queue_datas = Vec::new();
    let mut broker_datas = Vec::new();

    // Create enough data to reach ~1MB JSON
    // Approximate calculation: each entry is ~130 bytes, need ~8000 entries
    for i in 0..4000 {
        queue_datas.push(QueueData {
            broker_name: format!("broker-{:04}", i),
            read_queue_nums: 16,
            write_queue_nums: 16,
            perm: 6,
            topic_sys_flag: 0,
        });

        broker_datas.push(BrokerData {
            broker_name: format!("broker-{:04}", i),
            broker_addrs: vec![
                format!("192.168.1.{}:10911", i * 2),
                format!("192.168.1.{}:10911", i * 2 + 1),
            ],
            cluster: "DefaultCluster".to_string(),
            enable_acting_master: false,
            zone_name: format!("zone-{:04}", i),
        });
    }

    TopicRouteData {
        order_topic_conf: "".to_string(),
        queue_datas,
        broker_datas,
    }
}

fn benchmark_serde_json_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde_json_serialize");

    let small_data = create_small_data();
    let medium_data = create_medium_data();
    let large_data = create_large_data();

    group.throughput(Throughput::Elements(1));

    group.bench_with_input(BenchmarkId::new("small", ""), &small_data, |b, data| {
        b.iter(|| {
            let _json = SerdeJsonUtils::serialize_json(black_box(data)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("medium", ""), &medium_data, |b, data| {
        b.iter(|| {
            let _json = SerdeJsonUtils::serialize_json(black_box(data)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("large", ""), &large_data, |b, data| {
        b.iter(|| {
            let _json = SerdeJsonUtils::serialize_json(black_box(data)).unwrap();
        });
    });

    group.finish();
}

fn benchmark_serde_json_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde_json_deserialize");

    let small_json = SerdeJsonUtils::serialize_json(&create_small_data()).unwrap();
    let medium_json = SerdeJsonUtils::serialize_json(&create_medium_data()).unwrap();
    let large_json = SerdeJsonUtils::serialize_json(&create_large_data()).unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_with_input(BenchmarkId::new("small", ""), &small_json, |b, json| {
        b.iter(|| {
            let _data: TopicRouteData = SerdeJsonUtils::from_json_str(black_box(json)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("medium", ""), &medium_json, |b, json| {
        b.iter(|| {
            let _data: TopicRouteData = SerdeJsonUtils::from_json_str(black_box(json)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("large", ""), &large_json, |b, json| {
        b.iter(|| {
            let _data: TopicRouteData = SerdeJsonUtils::from_json_str(black_box(json)).unwrap();
        });
    });

    group.finish();
}

fn benchmark_serde_json_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde_json_roundtrip");

    let small_data = create_small_data();
    let medium_data = create_medium_data();
    let large_data = create_large_data();

    group.throughput(Throughput::Elements(1));

    group.bench_with_input(BenchmarkId::new("small", ""), &small_data, |b, data| {
        b.iter(|| {
            let json = SerdeJsonUtils::serialize_json(black_box(data)).unwrap();
            let _data: TopicRouteData = SerdeJsonUtils::from_json_str(&json).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("medium", ""), &medium_data, |b, data| {
        b.iter(|| {
            let json = SerdeJsonUtils::serialize_json(black_box(data)).unwrap();
            let _data: TopicRouteData = SerdeJsonUtils::from_json_str(&json).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("large", ""), &large_data, |b, data| {
        b.iter(|| {
            let json = SerdeJsonUtils::serialize_json(black_box(data)).unwrap();
            let _data: TopicRouteData = SerdeJsonUtils::from_json_str(&json).unwrap();
        });
    });

    group.finish();
}

#[cfg(feature = "simd")]
fn benchmark_simd_json_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_json_serialize");

    let small_data = create_small_data();
    let medium_data = create_medium_data();
    let large_data = create_large_data();

    group.throughput(Throughput::Elements(1));

    group.bench_with_input(BenchmarkId::new("small", ""), &small_data, |b, data| {
        b.iter(|| {
            let _json = SimdJsonUtils::serialize_json(black_box(data)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("medium", ""), &medium_data, |b, data| {
        b.iter(|| {
            let _json = SimdJsonUtils::serialize_json(black_box(data)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("large", ""), &large_data, |b, data| {
        b.iter(|| {
            let _json = SimdJsonUtils::serialize_json(black_box(data)).unwrap();
        });
    });

    group.finish();
}

#[cfg(feature = "simd")]
fn benchmark_simd_json_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_json_deserialize");

    let small_json = SimdJsonUtils::serialize_json(&create_small_data()).unwrap();
    let medium_json = SimdJsonUtils::serialize_json(&create_medium_data()).unwrap();
    let large_json = SimdJsonUtils::serialize_json(&create_large_data()).unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_with_input(BenchmarkId::new("small", ""), &small_json, |b, json| {
        b.iter(|| {
            let _data: TopicRouteData = SimdJsonUtils::from_json_str(black_box(json)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("medium", ""), &medium_json, |b, json| {
        b.iter(|| {
            let _data: TopicRouteData = SimdJsonUtils::from_json_str(black_box(json)).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("large", ""), &large_json, |b, json| {
        b.iter(|| {
            let _data: TopicRouteData = SimdJsonUtils::from_json_str(black_box(json)).unwrap();
        });
    });

    group.finish();
}

#[cfg(feature = "simd")]
fn benchmark_simd_json_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_json_roundtrip");

    let small_data = create_small_data();
    let medium_data = create_medium_data();
    let large_data = create_large_data();

    group.throughput(Throughput::Elements(1));

    group.bench_with_input(BenchmarkId::new("small", ""), &small_data, |b, data| {
        b.iter(|| {
            let json = SimdJsonUtils::serialize_json(black_box(data)).unwrap();
            let _data: TopicRouteData = SimdJsonUtils::from_json_str(&json).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("medium", ""), &medium_data, |b, data| {
        b.iter(|| {
            let json = SimdJsonUtils::serialize_json(black_box(data)).unwrap();
            let _data: TopicRouteData = SimdJsonUtils::from_json_str(&json).unwrap();
        });
    });

    group.bench_with_input(BenchmarkId::new("large", ""), &large_data, |b, data| {
        b.iter(|| {
            let json = SimdJsonUtils::serialize_json(black_box(data)).unwrap();
            let _data: TopicRouteData = SimdJsonUtils::from_json_str(&json).unwrap();
        });
    });

    group.finish();
}

#[cfg(feature = "simd")]
criterion_group!(
    benches,
    benchmark_serde_json_serialize,
    benchmark_serde_json_deserialize,
    benchmark_serde_json_roundtrip,
    benchmark_simd_json_serialize,
    benchmark_simd_json_deserialize,
    benchmark_simd_json_roundtrip,
);

#[cfg(not(feature = "simd"))]
criterion_group!(
    benches,
    benchmark_serde_json_serialize,
    benchmark_serde_json_deserialize,
    benchmark_serde_json_roundtrip,
);

criterion_main!(benches);
