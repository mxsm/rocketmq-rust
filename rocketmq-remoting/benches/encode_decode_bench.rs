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

//! Comprehensive benchmarks for RemotingCommand encode/decode operations

use std::collections::HashMap;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::parse_request_header;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::LanguageCode;
use rocketmq_remoting::protocol::SerializeType;

/// Helper function to create a simple command (minimal fields)
fn create_simple_command() -> RemotingCommand {
    RemotingCommand::create_remoting_command(100)
        .set_code(100)
        .set_language(LanguageCode::RUST)
        .set_opaque(1)
        .set_flag(0)
}

/// Helper function to create a complex command (with ext_fields and body)
fn create_complex_command() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    ext_fields.insert("key1".into(), "value1".into());
    ext_fields.insert("key2".into(), "value2_longer_value".into());
    ext_fields.insert("key3".into(), "value3".into());
    ext_fields.insert("brokerAddr".into(), "127.0.0.1:10911".into());
    ext_fields.insert("clusterName".into(), "DefaultCluster".into());

    let body = vec![0u8; 1024]; // 1KB body

    RemotingCommand::create_remoting_command(105)
        .set_code(105)
        .set_language(LanguageCode::JAVA)
        .set_opaque(12345)
        .set_flag(1)
        .set_remark("Test remark message")
        .set_ext_fields(ext_fields)
        .set_body(Bytes::from(body))
}

/// Helper function to create a very complex command (with custom header)
fn create_very_complex_command() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    for i in 0..20 {
        ext_fields.insert(
            CheetahString::from(format!("field_{}", i)),
            CheetahString::from(format!("value_for_field_{}_with_longer_content", i)),
        );
    }

    let body = vec![0u8; 10 * 1024]; // 10KB body

    RemotingCommand::create_remoting_command(310)
        .set_code(310)
        .set_language(LanguageCode::JAVA)
        .set_opaque(67890)
        .set_flag(0)
        .set_remark("Complex command with custom header and large body")
        .set_ext_fields(ext_fields)
        .set_body(Bytes::from(body))
        .set_command_custom_header(GetRouteInfoRequestHeader::new("TestTopic_Complex", Some(true)))
}

fn create_send_message_header() -> SendMessageRequestHeader {
    SendMessageRequestHeader {
        producer_group: CheetahString::from_static_str("bench_producer_group"),
        topic: CheetahString::from_static_str("bench_topic"),
        default_topic: CheetahString::from_static_str("TBW102"),
        default_topic_queue_nums: 8,
        queue_id: 1,
        sys_flag: 0,
        born_timestamp: 1_700_000_000_000,
        flag: 0,
        properties: Some(CheetahString::from_static_str("KEYS=bench-key;TAGS=TagA")),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        batch: Some(false),
        max_reconsume_times: Some(16),
        topic_request_header: None,
    }
}

fn create_send_message_v1_command() -> RemotingCommand {
    let header = create_send_message_header();
    RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_ext_fields(header.to_map().unwrap())
}

fn create_send_message_v2_command() -> RemotingCommand {
    let header = create_send_message_header();
    let header_v2 = SendMessageRequestHeaderV2::create_send_message_request_header_v2(&header);
    RemotingCommand::create_remoting_command(RequestCode::SendMessageV2).set_ext_fields(header_v2.to_map().unwrap())
}

fn create_send_message_response_header() -> SendMessageResponseHeader {
    SendMessageResponseHeader::new(
        CheetahString::from_static_str("C0A8010100002A9F000000000001"),
        3,
        1_024_768,
        Some(CheetahString::from_static_str("TX-123456789")),
        None,
        Some(CheetahString::from_static_str("handle-v1-topic-broker-1700000000000")),
    )
}

fn create_send_message_response_command_fast() -> RemotingCommand {
    RemotingCommand::create_response_command_with_header(create_send_message_response_header())
        .set_opaque(12345)
        .set_serialize_type(SerializeType::ROCKETMQ)
}

fn create_send_message_response_command_ext_fields() -> RemotingCommand {
    let mut ext_fields = HashMap::new();
    ext_fields.insert(
        CheetahString::from_static_str("msgId"),
        CheetahString::from_static_str("C0A8010100002A9F000000000001"),
    );
    ext_fields.insert(
        CheetahString::from_static_str("queueId"),
        CheetahString::from_static_str("3"),
    );
    ext_fields.insert(
        CheetahString::from_static_str("queueOffset"),
        CheetahString::from_static_str("1024768"),
    );
    ext_fields.insert(
        CheetahString::from_static_str("transactionId"),
        CheetahString::from_static_str("TX-123456789"),
    );
    ext_fields.insert(
        CheetahString::from_static_str("recallHandle"),
        CheetahString::from_static_str("handle-v1-topic-broker-1700000000000"),
    );

    RemotingCommand::create_response_command()
        .set_opaque(12345)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(ext_fields)
}

/// Benchmark: Encode simple command (JSON)
fn bench_encode_json_simple(c: &mut Criterion) {
    c.bench_function("encode_json_simple", |b| {
        b.iter_batched(
            || create_simple_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Encode complex command (JSON)
fn bench_encode_json_complex(c: &mut Criterion) {
    c.bench_function("encode_json_complex", |b| {
        b.iter_batched(
            || create_complex_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Encode very complex command (JSON)
fn bench_encode_json_very_complex(c: &mut Criterion) {
    c.bench_function("encode_json_very_complex", |b| {
        b.iter_batched(
            || create_very_complex_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Encode simple command (ROCKETMQ binary)
fn bench_encode_rocketmq_simple(c: &mut Criterion) {
    c.bench_function("encode_rocketmq_simple", |b| {
        b.iter_batched(
            || create_simple_command().set_serialize_type(SerializeType::ROCKETMQ),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Encode complex command (ROCKETMQ binary)
fn bench_encode_rocketmq_complex(c: &mut Criterion) {
    c.bench_function("encode_rocketmq_complex", |b| {
        b.iter_batched(
            || create_complex_command().set_serialize_type(SerializeType::ROCKETMQ),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Encode very complex command (ROCKETMQ binary)
fn bench_encode_rocketmq_very_complex(c: &mut Criterion) {
    c.bench_function("encode_rocketmq_very_complex", |b| {
        b.iter_batched(
            || create_very_complex_command().set_serialize_type(SerializeType::ROCKETMQ),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Decode simple command (JSON)
fn bench_decode_json_simple(c: &mut Criterion) {
    let mut cmd = create_simple_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    c.bench_function("decode_json_simple", |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| RemotingCommand::decode(&mut src).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Decode complex command (JSON)
fn bench_decode_json_complex(c: &mut Criterion) {
    let mut cmd = create_complex_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    c.bench_function("decode_json_complex", |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| RemotingCommand::decode(&mut src).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Decode very complex command (JSON)
fn bench_decode_json_very_complex(c: &mut Criterion) {
    let mut cmd = create_very_complex_command().set_serialize_type(SerializeType::JSON);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    c.bench_function("decode_json_very_complex", |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| RemotingCommand::decode(&mut src).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Decode simple command (ROCKETMQ binary)
fn bench_decode_rocketmq_simple(c: &mut Criterion) {
    let mut cmd = create_simple_command().set_serialize_type(SerializeType::ROCKETMQ);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    c.bench_function("decode_rocketmq_simple", |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| RemotingCommand::decode(&mut src).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Decode complex command (ROCKETMQ binary)
fn bench_decode_rocketmq_complex(c: &mut Criterion) {
    let mut cmd = create_complex_command().set_serialize_type(SerializeType::ROCKETMQ);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    c.bench_function("decode_rocketmq_complex", |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| RemotingCommand::decode(&mut src).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Decode very complex command (ROCKETMQ binary)
fn bench_decode_rocketmq_very_complex(c: &mut Criterion) {
    let mut cmd = create_very_complex_command().set_serialize_type(SerializeType::ROCKETMQ);
    let mut dst = BytesMut::new();
    cmd.fast_header_encode(&mut dst);
    let encoded = dst.freeze();

    c.bench_function("decode_rocketmq_very_complex", |b| {
        b.iter_batched(
            || BytesMut::from(&encoded[..]),
            |mut src| RemotingCommand::decode(&mut src).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

fn bench_decode_send_message_header(c: &mut Criterion) {
    let mut group = c.benchmark_group("send_message_header_decode");
    group.throughput(Throughput::Elements(1));

    group.bench_function("v1_normal", |b| {
        b.iter_batched(
            create_send_message_v1_command,
            |cmd| {
                cmd.decode_command_custom_header::<SendMessageRequestHeader>()
                    .expect("decode V1 SendMessage header")
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("v1_fast", |b| {
        b.iter_batched(
            create_send_message_v1_command,
            |cmd| {
                cmd.decode_command_custom_header_fast::<SendMessageRequestHeader>()
                    .expect("fast decode V1 SendMessage header")
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("v2_fast_to_v1", |b| {
        b.iter_batched(
            create_send_message_v2_command,
            |cmd| parse_request_header(&cmd, RequestCode::SendMessageV2).expect("parse V2 SendMessage header"),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_encode_send_message_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("send_message_response_encode");
    group.throughput(Throughput::Elements(1));

    group.bench_function("typed_fast_header", |b| {
        b.iter_batched(
            create_send_message_response_command_fast,
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("ext_fields_map", |b| {
        b.iter_batched(
            create_send_message_response_command_ext_fields,
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                dst
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark: Full roundtrip (encode + decode) JSON
fn bench_roundtrip_json(c: &mut Criterion) {
    c.bench_function("roundtrip_json_complex", |b| {
        b.iter_batched(
            || create_complex_command().set_serialize_type(SerializeType::JSON),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                let _ = RemotingCommand::decode(&mut dst).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Full roundtrip (encode + decode) ROCKETMQ
fn bench_roundtrip_rocketmq(c: &mut Criterion) {
    c.bench_function("roundtrip_rocketmq_complex", |b| {
        b.iter_batched(
            || create_complex_command().set_serialize_type(SerializeType::ROCKETMQ),
            |mut cmd| {
                let mut dst = BytesMut::new();
                cmd.fast_header_encode(&mut dst);
                let _ = RemotingCommand::decode(&mut dst).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmark: Throughput with different body sizes (JSON)
fn bench_throughput_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_json");

    for size in [0, 1024, 4096, 16384, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(format!("body_{}_bytes", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut cmd = create_simple_command().set_serialize_type(SerializeType::JSON);
                    if size > 0 {
                        cmd = cmd.set_body(Bytes::from(vec![0u8; size]));
                    }
                    cmd
                },
                |mut cmd| {
                    let mut dst = BytesMut::new();
                    cmd.fast_header_encode(&mut dst);
                    if let Some(body) = cmd.take_body() {
                        dst.extend_from_slice(&body);
                    }
                    dst
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Benchmark: Throughput with different body sizes (ROCKETMQ)
fn bench_throughput_rocketmq(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_rocketmq");

    for size in [0, 1024, 4096, 16384, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(format!("body_{}_bytes", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut cmd = create_simple_command().set_serialize_type(SerializeType::ROCKETMQ);
                    if size > 0 {
                        cmd = cmd.set_body(Bytes::from(vec![0u8; size]));
                    }
                    cmd
                },
                |mut cmd| {
                    let mut dst = BytesMut::new();
                    cmd.fast_header_encode(&mut dst);
                    if let Some(body) = cmd.take_body() {
                        dst.extend_from_slice(&body);
                    }
                    dst
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(
    encode_benches,
    bench_encode_json_simple,
    bench_encode_json_complex,
    bench_encode_json_very_complex,
    bench_encode_rocketmq_simple,
    bench_encode_rocketmq_complex,
    bench_encode_rocketmq_very_complex,
    bench_encode_send_message_response
);

criterion_group!(
    decode_benches,
    bench_decode_json_simple,
    bench_decode_json_complex,
    bench_decode_json_very_complex,
    bench_decode_rocketmq_simple,
    bench_decode_rocketmq_complex,
    bench_decode_rocketmq_very_complex,
    bench_decode_send_message_header
);

criterion_group!(roundtrip_benches, bench_roundtrip_json, bench_roundtrip_rocketmq);

criterion_group!(throughput_benches, bench_throughput_json, bench_throughput_rocketmq);

criterion_main!(encode_benches, decode_benches, roundtrip_benches, throughput_benches);
