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

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::MessageDecoder::string_to_message_properties;
use rocketmq_common::MessageUtils::delete_property_to_cheetah_string;

fn encoded_properties(user_property_count: usize) -> CheetahString {
    let mut properties = HashMap::with_capacity(user_property_count + 4);
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
        CheetahString::from_static_str("order-1001"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TAGS),
        CheetahString::from_static_str("TagA"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
        CheetahString::from_static_str("AC14000100002A9F0000000000010001"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
        CheetahString::from_static_str("broker-reserved-checkpoint"),
    );

    for index in 0..user_property_count {
        properties.insert(
            CheetahString::from_string(format!("userKey{index}")),
            CheetahString::from_string(format!("userValue{index}-{}", "x".repeat(index % 32))),
        );
    }

    message_properties_to_string(&properties)
}

fn double_parse_delete_reserved_property(
    properties_string: &CheetahString,
    region_id: &str,
    trace_on: bool,
) -> HashMap<CheetahString, CheetahString> {
    let mut properties = string_to_message_properties(Some(properties_string));
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
        CheetahString::from_slice(region_id),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH),
        CheetahString::from_string(trace_on.to_string()),
    );

    let encoded = message_properties_to_string(&properties);
    let encoded = delete_property_to_cheetah_string(&encoded, MessageConst::PROPERTY_POP_CK);
    string_to_message_properties(Some(&encoded))
}

fn single_parse_delete_reserved_property(
    properties_string: &CheetahString,
    region_id: &str,
    trace_on: bool,
) -> HashMap<CheetahString, CheetahString> {
    let mut properties = string_to_message_properties(Some(properties_string));
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
        CheetahString::from_slice(region_id),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH),
        CheetahString::from_static_str(if trace_on { "true" } else { "false" }),
    );
    properties.remove(MessageConst::PROPERTY_POP_CK);
    properties
}

fn single_parse_delete_reserved_property_and_encode(
    properties_string: &CheetahString,
    region_id: &str,
    trace_on: bool,
) -> CheetahString {
    let properties = single_parse_delete_reserved_property(properties_string, region_id, trace_on);
    message_properties_to_string(&properties)
}

fn bench_message_property_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_property_transform");

    for user_property_count in [4usize, 16, 64] {
        let properties = encoded_properties(user_property_count);
        group.throughput(Throughput::Elements(user_property_count as u64));

        group.bench_with_input(
            BenchmarkId::new("double_parse_delete_reserved_property", user_property_count),
            &properties,
            |b, properties| {
                b.iter(|| {
                    black_box(double_parse_delete_reserved_property(
                        black_box(properties),
                        black_box("DefaultRegion"),
                        black_box(true),
                    ))
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("single_parse_delete_reserved_property", user_property_count),
            &properties,
            |b, properties| {
                b.iter(|| {
                    black_box(single_parse_delete_reserved_property(
                        black_box(properties),
                        black_box("DefaultRegion"),
                        black_box(true),
                    ))
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("single_parse_delete_reserved_property_and_encode", user_property_count),
            &properties,
            |b, properties| {
                b.iter(|| {
                    black_box(single_parse_delete_reserved_property_and_encode(
                        black_box(properties),
                        black_box("DefaultRegion"),
                        black_box(true),
                    ))
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_message_property_transform);
criterion_main!(benches);
