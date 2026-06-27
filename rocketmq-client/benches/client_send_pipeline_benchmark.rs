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

//! Broker-free producer send pipeline baselines.
//!
//! These benchmarks measure local work performed before a request reaches the
//! network: message construction, request command construction, callback
//! dispatch, and async backpressure permit acquisition.

use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::producer::send_callback::ArcSendCallback;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tokio::sync::Semaphore;

fn build_message(body_size: usize) -> Message {
    Message::builder()
        .topic("BenchmarkTopic")
        .tags("BenchmarkTag")
        .keys(vec!["benchmark-key".to_string()])
        .body(vec![b'x'; body_size])
        .build()
        .expect("benchmark message should be valid")
}

fn build_send_header(message: &Message) -> SendMessageRequestHeader {
    SendMessageRequestHeader {
        producer_group: CheetahString::from_static_str("benchmark-producer-group"),
        topic: message.topic().clone(),
        default_topic: CheetahString::from_static_str("TBW102"),
        default_topic_queue_nums: 4,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: message.get_flag(),
        properties: Some(MessageDecoder::message_properties_to_string(message.get_properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        batch: Some(false),
        ..Default::default()
    }
}

fn build_send_request(mut message: Message) -> RemotingCommand {
    MessageClientIDSetter::set_uniq_id(&mut message);
    let header = build_send_header(&message);
    let body = message.get_body().cloned().unwrap_or_else(|| Bytes::from_static(b""));
    RemotingCommand::create_request_command(RequestCode::SendMessageV2, header).set_body(body)
}

fn bench_message_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_send_pipeline/message_construction");

    for body_size in [128usize, 1024, 16 * 1024, 128 * 1024] {
        group.throughput(Throughput::Bytes(body_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &body_size, |b, &body_size| {
            b.iter(|| black_box(build_message(body_size)));
        });
    }

    group.finish();
}

fn bench_request_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_send_pipeline/request_construction");

    for body_size in [128usize, 1024, 16 * 1024, 128 * 1024] {
        group.throughput(Throughput::Bytes(body_size as u64));
        let message = build_message(body_size);
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &message, |b, message| {
            b.iter_batched(
                || message.clone(),
                |message| black_box(build_send_request(message)),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_async_backpressure_envelope(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_send_pipeline/async_backpressure_envelope");

    for body_size in [128usize, 1024, 16 * 1024, 128 * 1024] {
        group.throughput(Throughput::Bytes(body_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &body_size, |b, &body_size| {
            let send_num = Arc::new(Semaphore::new(1024));
            let send_size = Arc::new(Semaphore::new(1024 * 1024));
            b.to_async(&runtime).iter(|| {
                let send_num = send_num.clone();
                let send_size = send_size.clone();
                async move {
                    let num_permit = send_num
                        .acquire_owned()
                        .await
                        .expect("benchmark semaphore should be open");
                    let size_permit = send_size
                        .acquire_many_owned(body_size as u32)
                        .await
                        .expect("benchmark semaphore should have capacity");
                    black_box((&num_permit, &size_permit));
                }
            });
        });
    }

    group.finish();
}

fn bench_callback_dispatch(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let send_result = SendResult::default();
    let callback: ArcSendCallback = Arc::new(|result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
        black_box(result.is_some());
        black_box(error.is_some());
    });

    let mut group = c.benchmark_group("client_send_pipeline/callback_dispatch");
    group.throughput(Throughput::Elements(1));

    group.bench_function("no_callback", |b| {
        b.iter(|| {
            let callback: Option<ArcSendCallback> = None;
            if let Some(callback) = callback {
                callback.on_success(black_box(&send_result));
            }
        });
    });

    group.bench_function("direct_callback", |b| {
        b.iter(|| {
            callback.on_success(black_box(&send_result));
        });
    });

    group.bench_function("executor_callback", |b| {
        b.to_async(&runtime).iter(|| {
            let callback = callback.clone();
            let send_result = send_result.clone();
            async move {
                let handle = tokio::spawn(async move {
                    callback.on_success(&send_result);
                });
                handle.await.expect("callback task should complete");
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_message_construction,
    bench_request_construction,
    bench_async_backpressure_envelope,
    bench_callback_dispatch,
);
criterion_main!(benches);
