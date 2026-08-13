// Copyright 2026 The RocketMQ Rust Authors
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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::benchmark_support::Connection;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

#[path = "support/criterion_profile.rs"]
mod criterion_profile;

use criterion_profile::apply_remoting_command_baseline_profile;

#[derive(Default)]
struct WriteCounters {
    bytes: AtomicUsize,
    writes: AtomicUsize,
    flushes: AtomicUsize,
    checksum: AtomicU64,
}

struct CountingDuplex {
    counters: Arc<WriteCounters>,
}

impl CountingDuplex {
    fn new() -> (Self, Arc<WriteCounters>) {
        let counters = Arc::new(WriteCounters::default());
        (
            Self {
                counters: counters.clone(),
            },
            counters,
        )
    }

    fn touch(&self, bytes: &[u8]) {
        let checksum = bytes.iter().fold(0_u64, |sum, byte| {
            sum.wrapping_mul(16777619).wrapping_add(u64::from(*byte))
        });
        self.counters.checksum.fetch_xor(black_box(checksum), Ordering::Relaxed);
    }
}

impl AsyncRead for CountingDuplex {
    fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for CountingDuplex {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        self.touch(buffer);
        self.counters.bytes.fetch_add(buffer.len(), Ordering::Relaxed);
        self.counters.writes.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut bytes = 0;
        for buffer in buffers {
            self.touch(buffer);
            bytes += buffer.len();
        }
        self.counters.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.counters.writes.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(bytes))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn commands(body_bytes: usize, batch: usize) -> Vec<RemotingCommand> {
    (0..batch)
        .map(|opaque| {
            RemotingCommand::create_remoting_command(10_100)
                .set_opaque(opaque as i32)
                .set_body(vec![0x5a; body_bytes])
        })
        .collect()
}

fn benchmark_write_pipeline(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("write_pipeline");
    apply_remoting_command_baseline_profile(&mut group);

    for body_bytes in [
        128,
        4 * 1024,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
    ] {
        for batch in [1, 2, 8, 32] {
            let templates = commands(body_bytes, batch);
            let encoded_bytes = templates
                .iter()
                .map(|command| {
                    rocketmq_protocol::protocol::encoded_frame::EncodedFrame::from_command(command.clone())
                        .expect("benchmark frame")
                        .encoded_len()
                })
                .sum::<usize>();
            group.throughput(Throughput::Bytes(encoded_bytes as u64));

            let (baseline_io, baseline_counters) = CountingDuplex::new();
            let baseline = Arc::new(tokio::sync::Mutex::new(Connection::new_with_plaintext_stream(
                baseline_io,
            )));
            group.bench_with_input(
                BenchmarkId::new("sequential_flush", format!("{body_bytes}b-{batch}f")),
                &templates,
                |benchmark, templates| {
                    benchmark.to_async(&runtime).iter(|| {
                        let baseline = baseline.clone();
                        async move {
                            let mut baseline = baseline.lock().await;
                            for command in templates.clone() {
                                baseline.send_command(command).await.expect("sequential frame write");
                            }
                        }
                    });
                },
            );
            black_box(baseline_counters.checksum.load(Ordering::Relaxed));

            let (batch_io, batch_counters) = CountingDuplex::new();
            let candidate = Arc::new(tokio::sync::Mutex::new(Connection::new_with_plaintext_stream(batch_io)));
            group.bench_with_input(
                BenchmarkId::new("bounded_batch", format!("{body_bytes}b-{batch}f")),
                &templates,
                |benchmark, templates| {
                    benchmark.to_async(&runtime).iter(|| {
                        let candidate = candidate.clone();
                        async move {
                            candidate
                                .lock()
                                .await
                                .send_batch(templates.clone())
                                .await
                                .expect("bounded batch write");
                        }
                    });
                },
            );
            black_box(batch_counters.checksum.load(Ordering::Relaxed));
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark_write_pipeline);
criterion_main!(benches);
