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

//! Quick performance test for SIMD JSON encoding/decoding
//!
//! Run without SIMD:
//!   cargo run --release --example quick_perf_test
//!
//! Run with SIMD:
//!   cargo run --release --example quick_perf_test --features simd

use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::LanguageCode;
use rocketmq_remoting::protocol::SerializeType;

fn create_test_command() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    for i in 0..20 {
        ext_fields.insert(
            CheetahString::from(format!("field_{}", i)),
            CheetahString::from(format!("value_{}_with_some_content", i)),
        );
    }

    RemotingCommand::create_remoting_command(200)
        .set_code(200)
        .set_language(LanguageCode::JAVA)
        .set_opaque(12345)
        .set_flag(1)
        .set_remark("Test message for performance evaluation")
        .set_ext_fields(ext_fields)
        .set_body(Bytes::from(vec![0u8; 4096]))
        .set_serialize_type(SerializeType::JSON)
}

fn main() {
    #[cfg(feature = "simd")]
    println!("=== Testing with SIMD-JSON ===\n");

    #[cfg(not(feature = "simd"))]
    println!("=== Testing with serde_json ===\n");

    const ITERATIONS: usize = 10_000;

    // Warm up
    println!("Warming up...");
    for _ in 0..1000 {
        let mut cmd = create_test_command();
        let mut dst = BytesMut::new();
        cmd.fast_header_encode(&mut dst);
        let _ = RemotingCommand::decode(&mut dst).unwrap();
    }

    // Test encoding
    println!("Testing encoding ({} iterations)...", ITERATIONS);
    let mut cmds: Vec<_> = (0..ITERATIONS).map(|_| create_test_command()).collect();

    let start = Instant::now();
    for cmd in cmds.iter_mut() {
        let mut dst = BytesMut::with_capacity(8192);
        cmd.fast_header_encode(&mut dst);
    }
    let encode_duration = start.elapsed();

    println!("  Total time: {:?}", encode_duration);
    println!("  Per operation: {:?}", encode_duration / ITERATIONS as u32);
    println!(
        "  Throughput: {:.2} ops/sec\n",
        ITERATIONS as f64 / encode_duration.as_secs_f64()
    );

    // Test decoding
    println!("Testing decoding ({} iterations)...", ITERATIONS);

    // Prepare encoded data
    let mut cmd = create_test_command();
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut src = BytesMut::from(&encoded[..]);
        let _ = RemotingCommand::decode(&mut src).unwrap();
    }
    let decode_duration = start.elapsed();

    println!("  Total time: {:?}", decode_duration);
    println!("  Per operation: {:?}", decode_duration / ITERATIONS as u32);
    println!(
        "  Throughput: {:.2} ops/sec\n",
        ITERATIONS as f64 / decode_duration.as_secs_f64()
    );

    // Test roundtrip
    println!("Testing roundtrip ({} iterations)...", ITERATIONS);
    let mut cmds: Vec<_> = (0..ITERATIONS).map(|_| create_test_command()).collect();

    let start = Instant::now();
    for cmd in cmds.iter_mut() {
        let mut dst = BytesMut::new();
        cmd.fast_header_encode(&mut dst);
        let _ = RemotingCommand::decode(&mut dst).unwrap();
    }
    let roundtrip_duration = start.elapsed();

    println!("  Total time: {:?}", roundtrip_duration);
    println!("  Per operation: {:?}", roundtrip_duration / ITERATIONS as u32);
    println!(
        "  Throughput: {:.2} ops/sec\n",
        ITERATIONS as f64 / roundtrip_duration.as_secs_f64()
    );

    println!("\n=== Summary ===");
    println!("Encoding:  {:?} per op", encode_duration / ITERATIONS as u32);
    println!("Decoding:  {:?} per op", decode_duration / ITERATIONS as u32);
    println!("Roundtrip: {:?} per op", roundtrip_duration / ITERATIONS as u32);

    #[cfg(feature = "simd")]
    println!("\n✓ SIMD-JSON enabled");

    #[cfg(not(feature = "simd"))]
    println!("\n✓ Standard serde_json (baseline)");

    println!("\nTo compare:");
    println!("  cargo run --release --example quick_perf_test");
    println!("  cargo run --release --example quick_perf_test --features simd");
}
