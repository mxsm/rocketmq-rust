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
use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::BytesMut;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::v1::AdmissionClass;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::AdmissionResource;
use rocketmq_transport::api::v1::AdmissionScope;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::TransportClient;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::benchmark_support::FrameWriter;
use rocketmq_transport::benchmark_support::RemotingCommandCodec;
use tokio::io::AsyncWrite;
use tokio_util::codec::Decoder;

struct DiscardWriter;

impl AsyncWrite for DiscardWriter {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn encoded_command(body_bytes: usize) -> bytes::Bytes {
    EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(RequestCode::SendMessage)
            .set_opaque(1)
            .set_body(vec![0x5a; body_bytes]),
    )
    .expect("benchmark frame")
    .into_bytes()
}

fn benchmark_task_owner(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let context = runtime.block_on(async { RuntimeContext::from_current("p0-task-owner") });
    let service = context.service_context("p0-task-owner");
    let parent = service.task_group().clone();
    let mut group = c.benchmark_group("p0_task_owner");
    group.bench_function("baseline_clone_drop", |benchmark| {
        benchmark.iter(|| black_box(parent.clone()));
    });
    group.bench_function("candidate_child_create_drop", |benchmark| {
        benchmark.iter(|| {
            black_box(parent.try_child("p0-session").expect("create child"));
        });
    });
    group.finish();
    runtime.block_on(async {
        drop(parent);
        drop(service);
        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
}

fn benchmark_decode_admission(c: &mut Criterion) {
    let admission = AdmissionController::new(AdmissionLimits::default());
    let scope = AdmissionScope::new("127.0.0.1".parse().expect("loopback")).with_session(1);
    let mut group = c.benchmark_group("p0_decode_admission");
    for body_bytes in [128, 4 * 1024, 4 * 1024 * 1024] {
        let encoded = encoded_command(body_bytes);
        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("baseline_decode", body_bytes),
            &encoded,
            |benchmark, encoded| {
                benchmark.iter(|| {
                    let mut buffer = BytesMut::from(encoded.as_ref());
                    let command = RemotingCommandCodec::new()
                        .decode(&mut buffer)
                        .expect("decode frame")
                        .expect("complete frame");
                    black_box(command);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("candidate_partial_admission_decode", body_bytes),
            &encoded,
            |benchmark, encoded| {
                benchmark.iter(|| {
                    let partial = admission
                        .try_acquire(
                            AdmissionResource::PartialFrame,
                            scope,
                            encoded.len(),
                            AdmissionClass::Data,
                        )
                        .expect("partial frame admission");
                    let mut buffer = BytesMut::from(encoded.as_ref());
                    let command = RemotingCommandCodec::new()
                        .decode(&mut buffer)
                        .expect("decode frame")
                        .expect("complete frame");
                    let inflight = admission
                        .try_acquire(
                            AdmissionResource::Inflight,
                            scope,
                            encoded.len(),
                            AdmissionClass::for_request_code(command.code()),
                        )
                        .expect("inflight admission");
                    drop(partial);
                    drop(inflight);
                    black_box(command);
                });
            },
        );
    }
    group.finish();
}

fn benchmark_healthy_write_deadline(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let frame = EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(RequestCode::SendMessage)
            .set_opaque(1)
            .set_body(vec![0x5a; 128]),
    )
    .expect("benchmark frame");
    let mut group = c.benchmark_group("p0_healthy_writer");
    group.bench_function("baseline_direct", |benchmark| {
        benchmark.to_async(&runtime).iter(|| async {
            let mut writer = FrameWriter::plaintext(DiscardWriter);
            writer.write_frame(black_box(&frame)).await.expect("direct write");
        });
    });
    group.bench_function("candidate_hard_deadline", |benchmark| {
        benchmark.to_async(&runtime).iter(|| async {
            let mut writer = FrameWriter::plaintext(DiscardWriter);
            RequestDeadline::after(Duration::from_secs(30))
                .timeout(writer.write_frame(black_box(&frame)))
                .await
                .expect("healthy write deadline")
                .expect("deadline write");
        });
    });
    group.finish();
}

fn benchmark_oneway(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let (runtime_context, client, target, receiver) = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("benchmark listener");
        let target =
            cheetah_string::CheetahString::from_string(listener.local_addr().expect("benchmark address").to_string());
        let receiver = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("benchmark accept");
            let mut connection = rocketmq_transport::benchmark_support::Connection::new(socket);
            while let Some(Ok(command)) = connection.receive_command().await {
                black_box(command);
            }
        });
        let runtime_context = RuntimeContext::from_current("p0-oneway");
        let client = Arc::new(
            TransportClient::builder(
                Arc::new(TransportClientConfig::default()),
                DefaultRequestProcessor,
                runtime_context.service_context("p0-oneway"),
            )
            .build()
            .expect("valid transport client configuration"),
        );
        (runtime_context, client, target, receiver)
    });
    let next_opaque = AtomicI32::new(1);
    let mut group = c.benchmark_group("p0_oneway_128b");
    group.throughput(Throughput::Bytes(128));
    group.bench_function("candidate_concurrency_1", |benchmark| {
        benchmark.to_async(&runtime).iter(|| {
            let client = Arc::clone(&client);
            let target = target.clone();
            let opaque = next_opaque.fetch_add(1, Ordering::Relaxed);
            async move {
                client
                    .invoke_request_oneway(
                        &target,
                        RemotingCommand::create_remoting_command(RequestCode::SendMessage)
                            .set_opaque(opaque)
                            .set_body(vec![0x5a; 128]),
                        3_000,
                    )
                    .await
                    .expect("one-way benchmark send");
            }
        });
    });
    group.bench_function("candidate_concurrency_32", |benchmark| {
        benchmark.to_async(&runtime).iter(|| {
            let client = Arc::clone(&client);
            let target = target.clone();
            let start = next_opaque.fetch_add(32, Ordering::Relaxed);
            async move {
                let sends = (0..32).map(|offset| {
                    let client = Arc::clone(&client);
                    let target = target.clone();
                    async move {
                        client
                            .invoke_request_oneway(
                                &target,
                                RemotingCommand::create_remoting_command(RequestCode::SendMessage)
                                    .set_opaque(start.wrapping_add(offset))
                                    .set_body(vec![0x5a; 128]),
                                3_000,
                            )
                            .await
                            .expect("concurrent one-way benchmark send");
                    }
                });
                futures::future::join_all(sends).await;
            }
        });
    });
    group.finish();
    runtime.block_on(async {
        client.shutdown();
        receiver.await.expect("benchmark receiver");
        let report = runtime_context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
}

criterion_group!(
    benches,
    benchmark_task_owner,
    benchmark_decode_admission,
    benchmark_healthy_write_deadline,
    benchmark_oneway
);
criterion_main!(benches);
