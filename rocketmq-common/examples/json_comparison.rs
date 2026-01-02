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

//! Quick performance comparison between serde_json_utils and simd_json_utils
//!
//! Run with:
//!   cargo run --release --example json_comparison
//!   cargo run --release --example json_comparison --features simd

use std::time::Instant;

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

fn create_test_data() -> TopicRouteData {
    let mut queue_datas = Vec::new();
    let mut broker_datas = Vec::new();

    // Create ~1MB JSON data (approximately 4000 entries)
    for i in 0..40000 {
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

fn main() -> rocketmq_error::RocketMQResult<()> {
    println!("JSON Utils Performance Comparison");
    println!("=================================\n");

    let test_data = create_test_data();
    let iterations = 100; // Reduced from 10000 for large data

    // Benchmark serde_json_utils
    println!("[serde_json_utils - Baseline]");

    // Serialize
    let start = Instant::now();
    let mut json_string = String::new();
    for _ in 0..iterations {
        json_string = SerdeJsonUtils::serialize_json(&test_data)?;
    }
    let serde_serialize_time = start.elapsed();
    let json_size = json_string.len();

    // Deserialize
    let start = Instant::now();
    for _ in 0..iterations {
        let _: TopicRouteData = SerdeJsonUtils::from_json_str(&json_string)?;
    }
    let serde_deserialize_time = start.elapsed();

    // Roundtrip
    let start = Instant::now();
    for _ in 0..iterations {
        let json = SerdeJsonUtils::serialize_json(&test_data)?;
        let _: TopicRouteData = SerdeJsonUtils::from_json_str(&json)?;
    }
    let serde_roundtrip_time = start.elapsed();

    println!(
        "  Serialization:   {:?} ({:.2}µs per op)",
        serde_serialize_time,
        serde_serialize_time.as_secs_f64() * 1_000_000.0 / iterations as f64
    );
    println!(
        "  Deserialization: {:?} ({:.2}µs per op)",
        serde_deserialize_time,
        serde_deserialize_time.as_secs_f64() * 1_000_000.0 / iterations as f64
    );
    println!(
        "  Roundtrip:       {:?} ({:.2}µs per op)",
        serde_roundtrip_time,
        serde_roundtrip_time.as_secs_f64() * 1_000_000.0 / iterations as f64
    );
    println!(
        "  Throughput:      {:.0} ops/sec",
        iterations as f64 / serde_roundtrip_time.as_secs_f64()
    );
    println!("  JSON size:       {} bytes", json_size);
    println!();

    #[cfg(feature = "simd")]
    {
        println!("[simd_json_utils - SIMD Accelerated]");

        // Serialize
        let start = Instant::now();
        let mut json_string = String::new();
        for _ in 0..iterations {
            json_string = SimdJsonUtils::serialize_json(&test_data)?;
        }
        let simd_serialize_time = start.elapsed();

        // Deserialize
        let start = Instant::now();
        for _ in 0..iterations {
            let _: TopicRouteData = SimdJsonUtils::from_json_str(&json_string)?;
        }
        let simd_deserialize_time = start.elapsed();

        // Roundtrip
        let start = Instant::now();
        for _ in 0..iterations {
            let json = SimdJsonUtils::serialize_json(&test_data)?;
            let _: TopicRouteData = SimdJsonUtils::from_json_str(&json)?;
        }
        let simd_roundtrip_time = start.elapsed();

        println!(
            "  Serialization:   {:?} ({:.2}µs per op)",
            simd_serialize_time,
            simd_serialize_time.as_secs_f64() * 1_000_000.0 / iterations as f64
        );
        println!(
            "  Deserialization: {:?} ({:.2}µs per op)",
            simd_deserialize_time,
            simd_deserialize_time.as_secs_f64() * 1_000_000.0 / iterations as f64
        );
        println!(
            "  Roundtrip:       {:?} ({:.2}µs per op)",
            simd_roundtrip_time,
            simd_roundtrip_time.as_secs_f64() * 1_000_000.0 / iterations as f64
        );
        println!(
            "  Throughput:      {:.0} ops/sec",
            iterations as f64 / simd_roundtrip_time.as_secs_f64()
        );
        println!("  JSON size:       {} bytes", json_string.len());
        println!();

        println!("[Performance Improvement]");
        let serialize_improvement =
            (1.0 - simd_serialize_time.as_secs_f64() / serde_serialize_time.as_secs_f64()) * 100.0;
        let deserialize_improvement =
            (1.0 - simd_deserialize_time.as_secs_f64() / serde_deserialize_time.as_secs_f64()) * 100.0;
        let roundtrip_improvement =
            (1.0 - simd_roundtrip_time.as_secs_f64() / serde_roundtrip_time.as_secs_f64()) * 100.0;
        let throughput_improvement = ((iterations as f64 / simd_roundtrip_time.as_secs_f64())
            / (iterations as f64 / serde_roundtrip_time.as_secs_f64())
            - 1.0)
            * 100.0;

        println!("  Serialization:   {:+.2}%", serialize_improvement);
        println!("  Deserialization: {:+.2}%", deserialize_improvement);
        println!("  Roundtrip:       {:+.2}%", roundtrip_improvement);
        println!("  Throughput:      {:+.2}%", throughput_improvement);
    }

    #[cfg(not(feature = "simd"))]
    {
        println!("Note: Run with --features simd to see SIMD performance");
    }

    Ok(())
}
