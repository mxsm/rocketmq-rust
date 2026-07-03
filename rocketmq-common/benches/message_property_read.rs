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

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;

fn sample_message_ext() -> MessageExt {
    let mut message = MessageExt::default();
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
        CheetahString::from_string("order-1001 user-42 payment-777 trace-abcdef".to_owned()),
    );
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_CONTEXT),
        CheetahString::from_string(format!("trace-{}", "x".repeat(96))),
    );
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_SHARDING_KEY),
        CheetahString::from_string(format!("group-{}", "y".repeat(64))),
    );
    message
}

fn bench_message_property_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_property_read");
    let message = sample_message_ext();
    let keys = CheetahString::from_static_str(MessageConst::PROPERTY_KEYS);
    let trace_context = CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_CONTEXT);
    let sharding_key = CheetahString::from_static_str(MessageConst::PROPERTY_SHARDING_KEY);

    group.bench_function("owned_split_keys", |b| {
        b.iter(|| {
            message
                .property(black_box(&keys))
                .map(|value| value.split(MessageConst::KEY_SEPARATOR).count())
        })
    });

    group.bench_function("ref_split_keys", |b| {
        b.iter(|| {
            message
                .property_ref(black_box(&keys))
                .map(|value| value.split(MessageConst::KEY_SEPARATOR).count())
        })
    });

    group.bench_function("owned_to_string", |b| {
        b.iter(|| {
            message
                .property(black_box(&trace_context))
                .map(|value| value.to_string())
        })
    });

    group.bench_function("ref_to_string", |b| {
        b.iter(|| {
            message
                .property_ref(black_box(&trace_context))
                .map(|value| value.to_string())
        })
    });

    group.bench_function("owned_exists", |b| {
        b.iter(|| message.property(black_box(&sharding_key)).is_some())
    });

    group.bench_function("ref_exists", |b| {
        b.iter(|| message.property_ref(black_box(&sharding_key)).is_some())
    });

    group.finish();
}

criterion_group!(benches, bench_message_property_read);
criterion_main!(benches);
