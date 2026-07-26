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
use std::task::Context;
use std::task::Poll;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::FrameWriteMode;
use rocketmq_transport::FrameWriter;
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
    for body_bytes in [128, 4 * 1024, 64 * 1024] {
        let frame = frame_with_body(body_bytes);
        group.throughput(Throughput::Bytes(frame.encoded_len() as u64));
        group.bench_with_input(
            BenchmarkId::new("plain_vectored", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.to_async(&runtime).iter(|| async {
                    let mut writer = FrameWriter::plaintext(DiscardWriter);
                    writer
                        .write_frame(black_box(frame))
                        .await
                        .expect("discard plaintext write");
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tls_coalesced", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.to_async(&runtime).iter(|| async {
                    let mut writer = FrameWriter::new(
                        DiscardWriter,
                        FrameWriteMode::TlsCoalesced {
                            max_plaintext_frame_bytes: frame.encoded_len(),
                        },
                    )
                    .expect("bounded TLS writer");
                    writer.write_frame(black_box(frame)).await.expect("discard TLS write");
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_frame_write);
criterion_main!(benches);
