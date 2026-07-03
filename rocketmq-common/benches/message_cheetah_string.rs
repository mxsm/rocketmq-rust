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

use bytes::Bytes;
use cheetah_string::CheetahBuilder;
use cheetah_string::CheetahStr;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_common::common::message::broker_message::BrokerMessage;
use rocketmq_common::common::message::message_envelope::MessageEnvelope;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageDecoder;

fn sample_properties(count: usize) -> HashMap<CheetahString, CheetahString> {
    let mut properties = HashMap::with_capacity(count);
    properties.insert(
        CheetahString::from_static_str("KEYS"),
        CheetahString::from_static_str("order-1001"),
    );
    properties.insert(
        CheetahString::from_static_str("TAGS"),
        CheetahString::from_static_str("paid"),
    );
    properties.insert(
        CheetahString::from_static_str("UNIQ_KEY"),
        CheetahString::from_static_str("AC14000100002A9F0000000000010001"),
    );

    for i in 3..count {
        properties.insert(
            CheetahString::from_string(format!("K{i}")),
            CheetahString::from_string(format!("value-{i}-{}", "x".repeat(i % 31))),
        );
    }

    properties
}

fn sample_message(count: usize) -> Message {
    let mut message = Message::default();
    message.set_topic(CheetahString::from_static_str("BenchmarkTopic"));
    message.set_body(Some(Bytes::from_static(b"benchmark-message-body")));

    for (key, value) in sample_properties(count) {
        message.put_property(key, value);
    }

    message
}

fn sample_message_ext(count: usize) -> MessageExt {
    let mut message_ext = MessageExt::default();
    message_ext.set_topic(CheetahString::from_static_str("BenchmarkTopic"));
    message_ext.set_body(Bytes::from_static(b"benchmark-message-body"));
    message_ext.set_queue_id(3);
    message_ext.set_queue_offset(1024);
    message_ext.set_commit_log_offset(4096);
    message_ext.set_born_timestamp(1_720_000_000_000);
    message_ext.set_store_timestamp(1_720_000_000_123);

    for (key, value) in sample_properties(count) {
        message_ext.put_property(key, value);
    }

    message_ext
}

fn broker_message_with_properties() -> BrokerMessage {
    let mut envelope = MessageEnvelope::default();
    envelope.set_topic(CheetahString::from_static_str("BenchmarkTopic"));

    for (key, value) in sample_properties(32) {
        envelope.put_property(key, value);
    }

    BrokerMessage::from_envelope(envelope)
}

fn bench_message_properties_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_properties_encode");

    for count in [3usize, 32] {
        let properties = sample_properties(count);
        group.bench_with_input(BenchmarkId::from_parameter(count), &properties, |b, properties| {
            b.iter(|| MessageDecoder::message_properties_to_string(black_box(properties)))
        });
    }

    group.finish();
}

fn bench_message_properties_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_properties_decode");

    for count in [3usize, 32] {
        let encoded = MessageDecoder::message_properties_to_string(&sample_properties(count));

        group.bench_with_input(BenchmarkId::new("cheetah_string", count), &encoded, |b, encoded| {
            b.iter(|| MessageDecoder::string_to_message_properties(Some(black_box(encoded))))
        });

        group.bench_with_input(BenchmarkId::new("str", count), encoded.as_str(), |b, encoded| {
            b.iter(|| MessageDecoder::str_to_message_properties(Some(black_box(encoded))))
        });
    }

    group.finish();
}

fn bench_broker_message_property_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_message_property_lookup");
    let broker_message = broker_message_with_properties();

    group.bench_function("existing_key", |b| {
        b.iter(|| broker_message.property(black_box("KEYS")))
    });

    group.bench_function("missing_key", |b| {
        b.iter(|| broker_message.property(black_box("NOT_EXISTS")))
    });

    group.finish();
}

fn bench_message_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_decode");

    for count in [3usize, 32] {
        let inner_message_bytes = MessageDecoder::encode_message(&sample_message(count));
        group.bench_with_input(
            BenchmarkId::new("inner_message", count),
            &inner_message_bytes,
            |b, encoded| {
                b.iter(|| {
                    let mut bytes = encoded.clone();
                    MessageDecoder::decode_message(black_box(&mut bytes))
                })
            },
        );

        let commitlog_bytes =
            MessageDecoder::encode(&sample_message_ext(count), false).expect("sample message should encode");
        group.bench_with_input(
            BenchmarkId::new("commitlog_message", count),
            &commitlog_bytes,
            |b, encoded| {
                b.iter(|| {
                    let mut bytes = encoded.clone();
                    MessageDecoder::decode(black_box(&mut bytes), true, false, false, false, false)
                })
            },
        );
    }

    group.finish();
}

fn bench_metadata_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_clone");
    let broker_name = CheetahString::from_static_str("broker-a");
    let topic = CheetahString::from_static_str("BenchmarkTopic");
    let msg_id = CheetahString::from_static_str("AC14000100002A9F0000000000010001");

    group.bench_function("cheetah_string_clone", |b| {
        b.iter(|| {
            black_box((
                broker_name.clone(),
                topic.clone(),
                msg_id.clone(),
                msg_id.clone(),
                topic.clone(),
            ))
        })
    });

    let broker_name = CheetahStr::from_static_str("broker-a");
    let topic = CheetahStr::from_static_str("BenchmarkTopic");
    let msg_id = CheetahStr::from_static_str("AC14000100002A9F0000000000010001");

    group.bench_function("cheetah_str_clone_candidate", |b| {
        b.iter(|| {
            black_box((
                broker_name.clone(),
                topic.clone(),
                msg_id.clone(),
                msg_id.clone(),
                topic.clone(),
            ))
        })
    });

    group.finish();
}

fn bench_message_builder_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_builder_join");
    let keys = ["order-1001", "user-42", "payment-777", "trace-abc"];

    group.bench_function("keys_join", |b| {
        b.iter(|| {
            let capacity = keys.iter().map(|key| key.len()).sum::<usize>() + keys.len().saturating_sub(1);
            let mut builder = CheetahBuilder::with_capacity(capacity);
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    builder.push(' ');
                }
                builder.push_str(black_box(key));
            }
            builder.finish_string()
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_message_properties_encode,
    bench_message_properties_decode,
    bench_broker_message_property_lookup,
    bench_message_decode,
    bench_metadata_clone,
    bench_message_builder_join
);
criterion_main!(benches);
