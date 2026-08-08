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

use std::future::Future;
use std::hint::black_box;
use std::io;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::run_connected_session;
use rocketmq_transport::AdmissionController;
use rocketmq_transport::AdmissionLimits;
use rocketmq_transport::Connection;
use rocketmq_transport::ConnectionHandler;
use rocketmq_transport::FrameWriteMode;
use rocketmq_transport::FrameWriter;
use rocketmq_transport::SessionHandle;
use rocketmq_transport::TransportSecurity;
use tokio::io::AsyncWrite;

struct DiscardWriter;

impl AsyncWrite for DiscardWriter {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buffer.len()))
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

struct CountingWriter {
    checksum: Arc<AtomicU64>,
}

impl CountingWriter {
    fn touch(&self, bytes: &[u8]) {
        let checksum = bytes.iter().fold(0_u64, |sum, byte| {
            sum.wrapping_mul(16777619).wrapping_add(u64::from(*byte))
        });
        self.checksum.fetch_xor(black_box(checksum), Ordering::Relaxed);
    }
}

impl AsyncWrite for CountingWriter {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        self.touch(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        for buffer in buffers {
            self.touch(buffer);
        }
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

fn frame_with_body(body_bytes: usize) -> EncodedFrame {
    EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(10_100)
            .set_opaque(101)
            .set_body(vec![0x5a; body_bytes]),
    )
    .expect("benchmark command should encode")
}

fn benchmark_frame_write(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("transport_frame_write");
    for body_bytes in [
        128,
        4 * 1024,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
    ] {
        let frame = frame_with_body(body_bytes);
        group.throughput(Throughput::Bytes(frame.encoded_len() as u64));
        let mut plain_writer = FrameWriter::plaintext(DiscardWriter);
        group.bench_with_input(
            BenchmarkId::new("plain_vectored", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.iter(|| {
                    runtime
                        .block_on(plain_writer.write_frame(black_box(frame)))
                        .expect("discard plaintext write");
                });
            },
        );
        let checksum = Arc::new(AtomicU64::new(0));
        let mut counting_writer = FrameWriter::plaintext(CountingWriter {
            checksum: checksum.clone(),
        });
        group.bench_with_input(
            BenchmarkId::new("plain_counting", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.iter(|| {
                    runtime
                        .block_on(counting_writer.write_frame(black_box(frame)))
                        .expect("counting plaintext write");
                });
            },
        );
        black_box(checksum.load(Ordering::Relaxed));
        let mut tls_writer = FrameWriter::new(
            DiscardWriter,
            FrameWriteMode::TlsCoalesced {
                max_plaintext_frame_bytes: frame.encoded_len(),
            },
        )
        .expect("bounded TLS writer");
        group.bench_with_input(
            BenchmarkId::new("tls_coalesced", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.iter(|| {
                    runtime
                        .block_on(tls_writer.write_frame(black_box(frame)))
                        .expect("discard TLS write");
                });
            },
        );
    }
    group.finish();
}

struct RoundTripHandler;

impl ConnectionHandler for RoundTripHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut connection = session.connection();
            connection
                .send_command(RemotingCommand::create_response_command().set_opaque(request.opaque()))
                .await
                .expect("benchmark response");
        })
    }
}

fn benchmark_session_writer_round_trip(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let (runtime_context, service, peer, runner) = runtime.block_on(async {
        let runtime_context = RuntimeContext::from_current("transport-session-writer-benchmark");
        let service = runtime_context.service_context("transport-session-writer-benchmark");
        let (transport, peer) = tokio::io::duplex(1024 * 1024);
        let local_addr: SocketAddr = "127.0.0.1:19101".parse().expect("local address");
        let remote_addr: SocketAddr = "127.0.0.1:19102".parse().expect("remote address");
        let runner = tokio::spawn(run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            Arc::new(RoundTripHandler),
        ));
        (
            runtime_context,
            service,
            Connection::new_with_plaintext_stream(peer),
            runner,
        )
    });
    let peer = Arc::new(tokio::sync::Mutex::new(peer));

    c.bench_function("transport_session_writer/round_trip_256", |benchmark| {
        let mut next_opaque = 1_i32;
        benchmark.to_async(&runtime).iter(|| {
            next_opaque = next_opaque.wrapping_add(256);
            let start = next_opaque;
            let peer = Arc::clone(&peer);
            async move {
                let mut peer = peer.lock().await;
                for offset in 0..256 {
                    let opaque = start.wrapping_add(offset);
                    peer.send_command(RemotingCommand::create_remoting_command(10_100).set_opaque(opaque))
                        .await
                        .expect("benchmark request");
                    let response = peer
                        .receive_command()
                        .await
                        .expect("benchmark session")
                        .expect("benchmark response decode");
                    black_box(response);
                }
            }
        });
    });

    runtime.block_on(async {
        let peer = Arc::try_unwrap(peer)
            .unwrap_or_else(|_| unreachable!("benchmark iterations release their peer handle"))
            .into_inner();
        drop(peer);
        runner.await.expect("benchmark session runner");
        drop(service);
        let report = runtime_context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
}

criterion_group!(benches, benchmark_frame_write, benchmark_session_writer_round_trip);
criterion_main!(benches);
