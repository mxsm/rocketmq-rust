/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Demonstration of SIMD JSON utilities for high-performance JSON processing
//!
//! This example shows how to use SimdJsonUtils for faster JSON serialization
//! and deserialization compared to standard serde_json.
//!
//! Run with: cargo run --example simd_json_demo --features simd

#[cfg(feature = "simd")]
use rocketmq_common::utils::simd_json_utils::SimdJsonUtils;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct BrokerData {
    broker_name: String,
    broker_addrs: Vec<String>,
    cluster: String,
    enable_acting_master: bool,
    zone_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TopicRouteData {
    order_topic_conf: String,
    queue_datas: Vec<QueueData>,
    broker_datas: Vec<BrokerData>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct QueueData {
    broker_name: String,
    read_queue_nums: i32,
    write_queue_nums: i32,
    perm: i32,
    topic_sys_flag: i32,
}

fn main() -> rocketmq_error::RocketMQResult<()> {
    println!("RocketMQ SIMD JSON Utilities Demo\n");

    // Create sample data
    let route_data = TopicRouteData {
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
        ],
        broker_datas: vec![
            BrokerData {
                broker_name: "broker-a".to_string(),
                broker_addrs: vec![
                    "192.168.1.100:10911".to_string(),
                    "192.168.1.101:10911".to_string(),
                ],
                cluster: "DefaultCluster".to_string(),
                enable_acting_master: false,
                zone_name: "zone-1".to_string(),
            },
            BrokerData {
                broker_name: "broker-b".to_string(),
                broker_addrs: vec![
                    "192.168.1.102:10911".to_string(),
                    "192.168.1.103:10911".to_string(),
                ],
                cluster: "DefaultCluster".to_string(),
                enable_acting_master: false,
                zone_name: "zone-2".to_string(),
            },
        ],
    };

    println!("Original Data:");
    println!("{:#?}\n", route_data);
    #[cfg(feature = "simd")]
    {
        // ========================================================================
        // Example 1: Serialize to JSON string
        // ========================================================================
        println!("Example 1: Serialize to JSON string");
        let start = Instant::now();
        let json_string = SimdJsonUtils::serialize_json(&route_data)?;
        let duration = start.elapsed();
        println!("   Serialization time: {:?}", duration);
        println!("   JSON string length: {} bytes", json_string.len());
        println!(
            "   JSON preview: {}...\n",
            &json_string[..100.min(json_string.len())]
        );

        // ========================================================================
        // Example 2: Deserialize from JSON string
        // ========================================================================
        println!("Example 2: Deserialize from JSON string");
        let start = Instant::now();
        let deserialized: TopicRouteData = SimdJsonUtils::from_json_str(&json_string)?;
        let duration = start.elapsed();
        println!("   Deserialization time: {:?}", duration);
        println!("   Data matches original: {}\n", deserialized == route_data);

        // ========================================================================
        // Example 3: Serialize to bytes
        // ========================================================================
        println!("Example 3: Serialize to byte vector");
        let start = Instant::now();
        let json_bytes = SimdJsonUtils::serialize_json_vec(&route_data)?;
        let duration = start.elapsed();
        println!("   Serialization time: {:?}", duration);
        println!("   Byte vector length: {} bytes\n", json_bytes.len());

        // ========================================================================
        // Example 4: Deserialize from bytes (mutable)
        // ========================================================================
        println!("Example 4: Deserialize from mutable byte slice");
        let mut json_bytes_mut = json_bytes.clone();
        let start = Instant::now();
        let deserialized: TopicRouteData = SimdJsonUtils::from_json_bytes(&mut json_bytes_mut)?;
        let duration = start.elapsed();
        println!("   Deserialization time: {:?}", duration);
        println!("   Data matches original: {}\n", deserialized == route_data);

        // ========================================================================
        // Example 5: Pretty print
        // ========================================================================
        println!("Example 5: Pretty-printed JSON");
        let pretty_json = SimdJsonUtils::serialize_json_pretty(&route_data)?;
        println!("{}...\n", &pretty_json[..200.min(pretty_json.len())]);

        // ========================================================================
        // Example 6: Write to existing buffer
        // ========================================================================
        println!("Example 6: Write to pre-allocated buffer");
        let mut buffer = Vec::with_capacity(1024);
        let start = Instant::now();
        SimdJsonUtils::serialize_json_to_writer(&mut buffer, &route_data)?;
        let duration = start.elapsed();
        println!("   Write time: {:?}", duration);
        println!("   Buffer capacity: {} bytes", buffer.capacity());
        println!("   Buffer length: {} bytes\n", buffer.len());

        // ========================================================================
        // Performance Comparison
        // ========================================================================
        println!("Performance Comparison (1000 iterations)");
        let iterations = 1000;

        // SIMD JSON performance
        let start = Instant::now();
        for _ in 0..iterations {
            let json = SimdJsonUtils::serialize_json(&route_data)?;
            let _: TopicRouteData = SimdJsonUtils::from_json_str(&json)?;
        }
        let simd_duration = start.elapsed();

        println!("   SIMD JSON roundtrip: {:?}", simd_duration);
        println!("   Average per operation: {:?}", simd_duration / iterations);
        println!(
            "   Throughput: {:.0} ops/sec\n",
            iterations as f64 / simd_duration.as_secs_f64()
        );

        // ========================================================================
        // Example 7: Error handling
        // ========================================================================
        println!("Example 7: Error handling");
        let invalid_json = r#"{"invalid": json syntax"#;
        match SimdJsonUtils::from_json_str::<TopicRouteData>(invalid_json) {
            Ok(_) => println!("   Unexpected success!"),
            Err(e) => println!("   Caught error (as expected): {}\n", e),
        }

        // ========================================================================
        // Example 8: Working with simple types
        // ========================================================================
        println!("Example 8: Simple types");
        let numbers = vec![1, 2, 3, 4, 5];
        let json = SimdJsonUtils::serialize_json(&numbers)?;
        println!("   Vector serialized: {}", json);

        let strings = vec!["hello", "world", "rocketmq"];
        let json = SimdJsonUtils::serialize_json(&strings)?;
        println!("   String vector: {}", json);

        let deserialized_strings: Vec<String> = SimdJsonUtils::from_json_str(&json)?;
        println!("   Deserialized: {:?}\n", deserialized_strings);
    }

    println!("Demo completed successfully!");

    Ok(())
}
