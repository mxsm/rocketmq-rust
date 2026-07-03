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

use std::collections::HashMap;
use std::hint::black_box;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::SerializeType;

fn ext_fields(count: usize) -> HashMap<CheetahString, CheetahString> {
    let mut fields = HashMap::with_capacity(count);
    fields.insert(
        CheetahString::from_static_str("producerGroup"),
        CheetahString::from_static_str("bench-producer"),
    );
    fields.insert(
        CheetahString::from_static_str("topic"),
        CheetahString::from_static_str("BenchmarkTopic"),
    );
    fields.insert(
        CheetahString::from_static_str("brokerName"),
        CheetahString::from_static_str("broker-a"),
    );

    for index in 3..count {
        fields.insert(
            CheetahString::from_string(format!("field{index}")),
            CheetahString::from_string(format!("value{index}-{}", "x".repeat(index % 24))),
        );
    }

    fields
}

fn send_header() -> SendMessageRequestHeader {
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

fn bench_ext_fields_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("remoting_ext_fields_lookup");

    for count in [8usize, 32, 128] {
        let fields = ext_fields(count);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("get_by_str", count), &fields, |b, fields| {
            b.iter(|| black_box(fields.get(black_box("topic"))))
        });

        group.bench_with_input(BenchmarkId::new("get_by_temp_cheetah", count), &fields, |b, fields| {
            b.iter(|| {
                let key = CheetahString::from_static_str("topic");
                black_box(fields.get(&key))
            })
        });
    }

    group.finish();
}

fn bench_send_header_to_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("remoting_send_header_to_map");
    let header = send_header();
    let header_v2 = SendMessageRequestHeaderV2::create_send_message_request_header_v2(&header);

    group.bench_function("v1_to_map", |b| b.iter(|| black_box(header.to_map().unwrap())));
    group.bench_function("v2_to_map", |b| b.iter(|| black_box(header_v2.to_map().unwrap())));

    group.finish();
}

fn bench_command_header_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("remoting_string_header_encode");

    for count in [8usize, 32, 128] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("rocketmq", count), &count, |b, count| {
            b.iter(|| {
                let mut dst = BytesMut::new();
                let mut command = RemotingCommand::create_remoting_command(RequestCode::SendMessage)
                    .set_serialize_type(SerializeType::ROCKETMQ)
                    .set_ext_fields(ext_fields(*count));
                command.fast_header_encode(&mut dst);
                black_box(dst)
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_ext_fields_lookup,
    bench_send_header_to_map,
    bench_command_header_encode
);
criterion_main!(benches);
