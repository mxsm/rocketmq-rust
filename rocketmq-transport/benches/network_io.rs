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
use std::sync::Arc;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::benchmark_support;
use rocketmq_transport::Connection;
use rocketmq_transport::FrameWriteMode;
use rocketmq_transport::FrameWriter;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;

fn frame(body_bytes: usize) -> EncodedFrame {
    EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(10_100)
            .set_opaque(1)
            .set_body(vec![0x5a; body_bytes]),
    )
    .expect("benchmark frame")
}

async fn drain_socket(mut socket: impl tokio::io::AsyncRead + Unpin) {
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        match socket.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(bytes) => black_box(&buffer[..bytes]),
        };
    }
}

async fn tcp_connection(compat: bool) -> (Connection, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("TCP benchmark listener");
    let address = listener.local_addr().expect("TCP benchmark address");
    let receiver = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("TCP benchmark accept");
        drain_socket(socket).await;
    });
    let socket = TcpStream::connect(address).await.expect("TCP benchmark connect");
    let connection = if compat {
        Connection::new_with_plaintext_stream(socket)
    } else {
        Connection::new(socket)
    };
    (connection, receiver)
}

async fn tls_writer(mode: FrameWriteMode) -> (FrameWriter<TlsStream<TcpStream>>, JoinHandle<()>) {
    let acceptor = benchmark_support::tls_acceptor();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("TLS benchmark listener");
    let address = listener.local_addr().expect("TLS benchmark address");
    let receiver = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("TLS benchmark accept");
        let tls = acceptor.accept(socket).await.expect("TLS server handshake");
        drain_socket(tls).await;
    });
    let socket = TcpStream::connect(address).await.expect("TLS benchmark connect");
    let server_name = ServerName::try_from("localhost".to_string()).expect("TLS benchmark server name");
    let tls = benchmark_support::tls_connector()
        .connect(server_name, socket)
        .await
        .expect("TLS client handshake");
    let writer = FrameWriter::new(tls, mode).expect("TLS benchmark frame writer");
    (writer, receiver)
}

fn benchmark_network_io(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("network benchmark runtime");
    let mut tcp_group = c.benchmark_group("network_io_tcp");
    tcp_group.sample_size(10);
    tcp_group.warm_up_time(Duration::from_secs(1));
    tcp_group.measurement_time(Duration::from_secs(2));
    for body_bytes in [128, 4 * 1024, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let command = RemotingCommand::create_remoting_command(10_100)
            .set_opaque(1)
            .set_body(vec![0x5a; body_bytes]);
        let encoded_len = frame(body_bytes).encoded_len();
        tcp_group.throughput(Throughput::Bytes(encoded_len as u64));

        let (compat, compat_receiver) = runtime.block_on(tcp_connection(true));
        let compat = Arc::new(tokio::sync::Mutex::new(compat));
        tcp_group.bench_with_input(
            BenchmarkId::new("compat_boxed_split", body_bytes),
            &command,
            |benchmark, command| {
                benchmark.to_async(&runtime).iter(|| {
                    let compat = compat.clone();
                    async move {
                        compat
                            .lock()
                            .await
                            .send_command(black_box(command.clone()))
                            .await
                            .expect("compat TCP write");
                    }
                });
            },
        );
        runtime.block_on(async {
            compat.lock().await.shutdown().await.expect("compat TCP shutdown");
            compat_receiver.await.expect("compat TCP receiver");
        });

        let (specialized, specialized_receiver) = runtime.block_on(tcp_connection(false));
        let specialized = Arc::new(tokio::sync::Mutex::new(specialized));
        tcp_group.bench_with_input(
            BenchmarkId::new("tcp_owned_halves", body_bytes),
            &command,
            |benchmark, command| {
                benchmark.to_async(&runtime).iter(|| {
                    let specialized = specialized.clone();
                    async move {
                        specialized
                            .lock()
                            .await
                            .send_command(black_box(command.clone()))
                            .await
                            .expect("specialized TCP write");
                    }
                });
            },
        );
        runtime.block_on(async {
            specialized
                .lock()
                .await
                .shutdown()
                .await
                .expect("specialized TCP shutdown");
            specialized_receiver.await.expect("specialized TCP receiver");
        });
    }
    tcp_group.finish();

    let mut tls_group = c.benchmark_group("network_io_tls");
    tls_group.sample_size(10);
    tls_group.warm_up_time(Duration::from_secs(1));
    tls_group.measurement_time(Duration::from_secs(2));
    for body_bytes in [128, 4 * 1024, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let frame = frame(body_bytes);
        let max_plaintext_frame_bytes = frame.encoded_len();
        tls_group.throughput(Throughput::Bytes(max_plaintext_frame_bytes as u64));

        let (coalesced, coalesced_receiver) = runtime.block_on(tls_writer(FrameWriteMode::TlsCoalesced {
            max_plaintext_frame_bytes,
        }));
        let coalesced = Arc::new(tokio::sync::Mutex::new(coalesced));
        tls_group.bench_with_input(
            BenchmarkId::new("coalesced_real_tls", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.to_async(&runtime).iter(|| {
                    let coalesced = coalesced.clone();
                    async move {
                        coalesced
                            .lock()
                            .await
                            .write_frame(black_box(frame))
                            .await
                            .expect("coalesced TLS write");
                    }
                });
            },
        );
        runtime.block_on(async {
            coalesced.lock().await.shutdown().await.expect("coalesced TLS shutdown");
            coalesced_receiver.await.expect("coalesced TLS receiver");
        });

        let (vectored, vectored_receiver) = runtime.block_on(tls_writer(FrameWriteMode::TlsVectored {
            max_plaintext_frame_bytes,
        }));
        let vectored = Arc::new(tokio::sync::Mutex::new(vectored));
        tls_group.bench_with_input(
            BenchmarkId::new("vectored_real_tls", body_bytes),
            &frame,
            |benchmark, frame| {
                benchmark.to_async(&runtime).iter(|| {
                    let vectored = vectored.clone();
                    async move {
                        vectored
                            .lock()
                            .await
                            .write_frame(black_box(frame))
                            .await
                            .expect("vectored TLS write");
                    }
                });
            },
        );
        runtime.block_on(async {
            vectored.lock().await.shutdown().await.expect("vectored TLS shutdown");
            vectored_receiver.await.expect("vectored TLS receiver");
        });
    }
    tls_group.finish();
}

criterion_group!(benches, benchmark_network_io);
criterion_main!(benches);
