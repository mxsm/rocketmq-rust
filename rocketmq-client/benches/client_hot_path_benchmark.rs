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

//! Client hot-path benchmarks for Java parity and production readiness.
//!
//! These benches are intentionally broker-free. They cover paths whose latency
//! and allocation behavior are fully local and are part of the Java alignment
//! surface: ACL request signing, queue selection, rebalance allocation, producer
//! batch construction/encoding, and LitePull owned-vs-zero-copy message batch
//! handling.

use std::collections::HashSet;
use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::common::session_credentials::SessionCredentials;
use rocketmq_client_rust::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_consistent_hash::AllocateMessageQueueConsistentHash;
use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelector;
use rocketmq_client_rust::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByHash;
use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByMachineRoom;
use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByRandom;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::runtime::RPCHook;

fn build_acl_request(body_size: usize) -> RemotingCommand {
    let mut request = RemotingCommand::create_remoting_command(10).set_body(Bytes::from(vec![b'a'; body_size]));
    request.ensure_ext_fields_initialized();
    request.add_ext_field("alpha", "first");
    request.add_ext_field("brokerName", "broker-a");
    request.add_ext_field("consumerGroup", "bench-consumer");
    request.add_ext_field("queueId", "3");
    request.add_ext_field("topic", "BenchTopic");
    request.add_ext_field("zeta", "last");
    request
}

fn bench_acl_signing(c: &mut Criterion) {
    let hook = AclClientRPCHook::new(SessionCredentials::with_token(
        "bench_access_key",
        "bench_secret_key",
        "bench_security_token",
    ));
    let remote_addr: SocketAddr = "127.0.0.1:9876".parse().unwrap();
    let mut group = c.benchmark_group("acl_signing");

    for body_size in [0usize, 128, 1024, 4096] {
        group.throughput(Throughput::Bytes(body_size as u64));
        let template = build_acl_request(body_size);
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &template, |b, request| {
            b.iter_batched(
                || request.clone(),
                |mut request| {
                    hook.do_before_request(remote_addr, &mut request).unwrap();
                    black_box(request)
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn build_queues(queue_count: i32) -> Vec<MessageQueue> {
    (0..queue_count)
        .map(|queue_id| {
            let broker = match queue_id % 4 {
                0 => "room-a@broker-a",
                1 => "room-b@broker-b",
                2 => "room-c@broker-c",
                _ => "broker-d",
            };
            MessageQueue::from_parts("BenchTopic", broker, queue_id)
        })
        .collect()
}

fn build_message() -> Message {
    Message::builder()
        .topic("BenchTopic")
        .body_slice(b"BenchBody")
        .build()
        .unwrap()
}

fn bench_queue_selectors(c: &mut Criterion) {
    let queues = build_queues(128);
    let msg = build_message();
    let hash_selector = SelectMessageQueueByHash;
    let random_selector = SelectMessageQueueByRandom;
    let machine_room_selector =
        SelectMessageQueueByMachineRoom::new(HashSet::from([CheetahString::from_static_str("room-b")]));

    let mut group = c.benchmark_group("queue_selectors");
    group.throughput(Throughput::Elements(1));

    group.bench_function("hash_selector", |b| {
        b.iter(|| {
            let order_id = black_box("order-123456789");
            black_box(hash_selector.select(&queues, &msg, &order_id))
        })
    });
    group.bench_function("random_selector", |b| {
        b.iter(|| black_box(random_selector.select(&queues, &msg, &())))
    });
    group.bench_function("machine_room_selector", |b| {
        b.iter(|| black_box(machine_room_selector.select(&queues, &msg, &())))
    });

    group.finish();
}

fn build_topic_publish_info(queue_count: i32) -> TopicPublishInfo {
    let mut info = TopicPublishInfo::new();
    info.have_topic_router_info = true;
    info.message_queue_list = build_queues(queue_count);
    info.topic_route_data = Some(TopicRouteData {
        queue_datas: (0..queue_count)
            .map(|queue_id| {
                QueueData::new(
                    CheetahString::from_string(format!("broker-{queue_id}")),
                    1,
                    1,
                    PermName::PERM_READ | PermName::PERM_WRITE,
                    0,
                )
            })
            .collect(),
        ..Default::default()
    });
    info
}

fn bench_producer_route_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("producer_route_snapshot");
    group.throughput(Throughput::Elements(1));

    for queue_count in [4i32, 64, 256, 1024] {
        let owned_info = build_topic_publish_info(queue_count);
        group.bench_with_input(
            BenchmarkId::new("owned_clone_and_select", queue_count),
            &owned_info,
            |b, info| {
                b.iter(|| {
                    let info = black_box(info).clone();
                    black_box(info.select_one_message_queue())
                })
            },
        );

        let shared_info = Arc::new(build_topic_publish_info(queue_count));
        group.bench_with_input(
            BenchmarkId::new("arc_snapshot_clone_and_select", queue_count),
            &shared_info,
            |b, info| {
                b.iter(|| {
                    let info = Arc::clone(black_box(info));
                    black_box(info.select_one_message_queue())
                })
            },
        );
    }

    group.finish();
}

fn bench_consistent_hash_rebalance(c: &mut Criterion) {
    let strategy = AllocateMessageQueueConsistentHash::default();
    let group_name = CheetahString::from_static_str("bench-group");
    let consumers = (0..16)
        .map(|index| CheetahString::from_string(format!("client-{index:02}")))
        .collect::<Vec<_>>();
    let queues = (0..1024)
        .map(|queue_id| MessageQueue::from_parts("BenchTopic", "broker-a", queue_id))
        .collect::<Vec<_>>();

    c.bench_function("consistent_hash_rebalance_1024_queues_16_consumers", |b| {
        b.iter(|| {
            let allocation = strategy
                .allocate(
                    &group_name,
                    black_box(&consumers[0]),
                    black_box(&queues),
                    black_box(&consumers),
                )
                .unwrap();
            black_box(allocation)
        })
    });
}

fn build_producer_batch_messages(batch_size: usize, body_size: usize) -> Vec<Message> {
    (0..batch_size)
        .map(|index| {
            Message::builder()
                .topic("BenchTopic")
                .tags("TagA")
                .keys(vec![format!("key-{index}")])
                .body(vec![b'b'; body_size])
                .build()
                .unwrap()
        })
        .collect()
}

fn bench_producer_batch_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("producer_batch_encode");

    for (batch_size, body_size) in [(8usize, 128usize), (32, 1024), (128, 1024)] {
        let bytes_per_batch = (batch_size * body_size) as u64;
        group.throughput(Throughput::Bytes(bytes_per_batch));

        let messages = build_producer_batch_messages(batch_size, body_size);
        group.bench_with_input(
            BenchmarkId::new("generate_from_messages", format!("{batch_size}x{body_size}")),
            &messages,
            |b, messages| {
                b.iter_batched(
                    || messages.clone(),
                    |messages| black_box(MessageBatch::generate_from_messages(messages).unwrap()),
                    BatchSize::SmallInput,
                )
            },
        );

        let batch = MessageBatch::generate_from_messages(messages).unwrap();
        group.bench_with_input(
            BenchmarkId::new("encode", format!("{batch_size}x{body_size}")),
            &batch,
            |b, batch| b.iter(|| black_box(batch.encode())),
        );
    }

    group.finish();
}

fn build_message_ext_batch(batch_size: usize, body_size: usize) -> Vec<MessageExt> {
    (0..batch_size)
        .map(|index| {
            let message = Message::builder()
                .topic("BenchTopic")
                .body(vec![b'x'; body_size])
                .build()
                .unwrap();
            let mut message_ext = MessageExt::default();
            message_ext.set_message_inner(message);
            message_ext.set_broker_name(CheetahString::from_static_str("broker-a"));
            message_ext.set_queue_id((index % 16) as i32);
            message_ext.set_queue_offset(index as i64);
            message_ext
        })
        .collect()
}

fn bench_lite_pull_batch_clone(c: &mut Criterion) {
    let owned_messages = build_message_ext_batch(256, 1024);
    let shared_messages = owned_messages.iter().cloned().map(Arc::new).collect::<Vec<_>>();

    let mut group = c.benchmark_group("lite_pull_batch_clone");
    group.throughput(Throughput::Elements(owned_messages.len() as u64));

    group.bench_function("owned_message_ext_clone", |b| {
        b.iter(|| black_box(owned_messages.clone()))
    });
    group.bench_function("shared_arc_message_ext_clone", |b| {
        b.iter(|| black_box(shared_messages.clone()))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_acl_signing,
    bench_queue_selectors,
    bench_producer_route_snapshot,
    bench_consistent_hash_rebalance,
    bench_producer_batch_encode,
    bench_lite_pull_batch_clone,
);
criterion_main!(benches);
