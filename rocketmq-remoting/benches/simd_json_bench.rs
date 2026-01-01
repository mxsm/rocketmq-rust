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

//! Benchmark comparing simd-json vs serde_json performance for RemotingCommand
//!
//! Run benchmarks:
//! - Without SIMD: `cargo bench --bench simd_json_bench`
//! - With SIMD:    `cargo bench --bench simd_json_bench --features simd`
//!
//! Compare results:
//! - Run both commands above and compare the output
//! - Expected: 30-50% improvement with simd feature enabled

use std::collections::HashMap;
use std::hint::black_box;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::LanguageCode;
use rocketmq_remoting::protocol::SerializeType;

// Test data generators

fn create_small_command() -> RemotingCommand {
    RemotingCommand::create_remoting_command(100)
        .set_code(100)
        .set_language(LanguageCode::RUST)
        .set_opaque(1)
        .set_flag(0)
        .set_remark("Small message")
}

fn create_medium_command() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    for i in 0..10 {
        ext_fields.insert(
            CheetahString::from(format!("key_{}", i)),
            CheetahString::from(format!("value_{}", i)),
        );
    }

    RemotingCommand::create_remoting_command(200)
        .set_code(200)
        .set_language(LanguageCode::JAVA)
        .set_opaque(12345)
        .set_flag(1)
        .set_remark("Medium complexity message with moderate ext_fields")
        .set_ext_fields(ext_fields)
        .set_body(Bytes::from(vec![0u8; 2048]))
}

fn create_large_command() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    for i in 0..50 {
        ext_fields.insert(
            CheetahString::from(format!("field_name_{:03}", i)),
            CheetahString::from(format!("field_value_{:03}_with_longer_content_to_increase_size", i)),
        );
    }

    RemotingCommand::create_remoting_command(300)
        .set_code(300)
        .set_language(LanguageCode::JAVA)
        .set_opaque(67890)
        .set_flag(0)
        .set_remark("Large complexity message with many ext_fields and large body")
        .set_ext_fields(ext_fields)
        .set_body(Bytes::from(vec![0u8; 32768]))
}

fn create_extra_large_command() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    for i in 0..100 {
        ext_fields.insert(
            CheetahString::from(format!("xl_field_{:04}_key", i)),
            CheetahString::from(format!(
                "xl_value_{:04}_with_very_long_content_string_to_maximize_json_size_{}",
                i,
                "X".repeat(50)
            )),
        );
    }

    RemotingCommand::create_remoting_command(400)
        .set_code(400)
        .set_language(LanguageCode::JAVA)
        .set_opaque(99999)
        .set_flag(1)
        .set_remark(format!(
            "Extra large message for stress testing: {}",
            "LARGE".repeat(20)
        ))
        .set_ext_fields(ext_fields)
        .set_body(Bytes::from(vec![0u8; 65536]))
}

// JSON Encoding Benchmarks

fn bench_json_encode_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_encode");

    #[cfg(feature = "simd")]
    let label = "small_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "small_without_simd";

    group.bench_function(BenchmarkId::new(label, "small"), |b| {
        b.iter_batched(
            || create_small_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::with_capacity(512);
                cmd.fast_header_encode(&mut dst);
                black_box(dst)
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_json_encode_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_encode");

    #[cfg(feature = "simd")]
    let label = "medium_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "medium_without_simd";

    group.bench_function(BenchmarkId::new(label, "medium"), |b| {
        b.iter_batched(
            || create_medium_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::with_capacity(4096);
                cmd.fast_header_encode(&mut dst);
                black_box(dst)
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_json_encode_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_encode");

    #[cfg(feature = "simd")]
    let label = "large_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "large_without_simd";

    group.bench_function(BenchmarkId::new(label, "large"), |b| {
        b.iter_batched(
            || create_large_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::with_capacity(65536);
                cmd.fast_header_encode(&mut dst);
                black_box(dst)
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_json_encode_extra_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_encode");

    #[cfg(feature = "simd")]
    let label = "xlarge_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "xlarge_without_simd";

    group.bench_function(BenchmarkId::new(label, "xlarge"), |b| {
        b.iter_batched(
            || create_extra_large_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::with_capacity(131072);
                cmd.fast_header_encode(&mut dst);
                black_box(dst)
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// JSON Decoding Benchmarks

fn bench_json_decode_small(c: &mut Criterion) {
    let mut cmd = create_small_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    let mut group = c.benchmark_group("json_decode");

    #[cfg(feature = "simd")]
    let label = "small_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "small_without_simd";

    group.bench_function(BenchmarkId::new(label, "small"), |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| black_box(RemotingCommand::decode(&mut src).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_json_decode_medium(c: &mut Criterion) {
    let mut cmd = create_medium_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    let mut group = c.benchmark_group("json_decode");

    #[cfg(feature = "simd")]
    let label = "medium_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "medium_without_simd";

    group.bench_function(BenchmarkId::new(label, "medium"), |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| black_box(RemotingCommand::decode(&mut src).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_json_decode_large(c: &mut Criterion) {
    let mut cmd = create_large_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    let mut group = c.benchmark_group("json_decode");

    #[cfg(feature = "simd")]
    let label = "large_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "large_without_simd";

    group.bench_function(BenchmarkId::new(label, "large"), |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| black_box(RemotingCommand::decode(&mut src).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_json_decode_extra_large(c: &mut Criterion) {
    let mut cmd = create_extra_large_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    let mut group = c.benchmark_group("json_decode");

    #[cfg(feature = "simd")]
    let label = "xlarge_with_simd";
    #[cfg(not(feature = "simd"))]
    let label = "xlarge_without_simd";

    group.bench_function(BenchmarkId::new(label, "xlarge"), |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| black_box(RemotingCommand::decode(&mut src).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// Roundtrip Benchmarks (Encode + Decode)

fn bench_json_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_roundtrip");

    #[cfg(feature = "simd")]
    let backend = "simd";
    #[cfg(not(feature = "simd"))]
    let backend = "serde";

    for (size_name, create_fn) in [
        ("small", create_small_command as fn() -> RemotingCommand),
        ("medium", create_medium_command),
        ("large", create_large_command),
        ("xlarge", create_extra_large_command),
    ] {
        group.bench_function(BenchmarkId::new(backend, size_name), |b| {
            b.iter_batched(
                || create_fn().set_serialize_type(SerializeType::JSON),
                |mut cmd| {
                    let mut dst = BytesMut::new();
                    cmd.fast_header_encode(&mut dst);
                    let result = RemotingCommand::decode(&mut dst).unwrap();
                    black_box(result)
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// Throughput Benchmark

fn bench_json_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_throughput");

    #[cfg(feature = "simd")]
    let backend = "simd";
    #[cfg(not(feature = "simd"))]
    let backend = "serde";

    for ops_count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(ops_count as u64));
        group.bench_function(BenchmarkId::new(backend, ops_count), |b| {
            b.iter_batched(
                || {
                    (0..ops_count)
                        .map(|_| create_medium_command().set_serialize_type(SerializeType::JSON))
                        .collect::<Vec<_>>()
                },
                |cmds| {
                    let mut results = Vec::with_capacity(ops_count);
                    for mut cmd in cmds {
                        let mut dst = BytesMut::new();
                        cmd.fast_header_encode(&mut dst);
                        let decoded = RemotingCommand::decode(&mut dst).unwrap();
                        results.push(decoded);
                    }
                    black_box(results)
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// Header-only Benchmark (No body, focus on JSON parsing)

fn bench_json_header_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_header_only");

    #[cfg(feature = "simd")]
    let backend = "simd";
    #[cfg(not(feature = "simd"))]
    let backend = "serde";

    // Create commands with varying header complexity but no body
    for field_count in [5, 25, 50, 100] {
        let mut ext_fields = HashMap::new();
        for i in 0..field_count {
            ext_fields.insert(
                CheetahString::from(format!("field_{}", i)),
                CheetahString::from(format!("value_{}", i)),
            );
        }

        let cmd = RemotingCommand::create_remoting_command(500)
            .set_code(500)
            .set_language(LanguageCode::RUST)
            .set_ext_fields(ext_fields)
            .set_serialize_type(SerializeType::JSON);

        let mut dst = BytesMut::new();
        let mut cmd_clone = cmd.clone();
        cmd_clone.fast_header_encode(&mut dst);
        let encoded = dst.freeze();

        group.bench_function(BenchmarkId::new(backend, field_count), |b| {
            b.iter_batched(
                || BytesMut::from(&encoded[..]),
                |mut src| black_box(RemotingCommand::decode(&mut src).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    encode_benches,
    bench_json_encode_small,
    bench_json_encode_medium,
    bench_json_encode_large,
    bench_json_encode_extra_large
);

criterion_group!(
    decode_benches,
    bench_json_decode_small,
    bench_json_decode_medium,
    bench_json_decode_large,
    bench_json_decode_extra_large
);

criterion_group!(roundtrip_benches, bench_json_roundtrip);

criterion_group!(throughput_benches, bench_json_throughput);

criterion_group!(header_benches, bench_json_header_only);

criterion_main!(
    encode_benches,
    decode_benches,
    roundtrip_benches,
    throughput_benches,
    header_benches
);
