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

use std::process::Command;
use std::time::Duration;
use std::time::Instant;

use bytes::BytesMut;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::benchmark_support;
use rocketmq_transport::benchmark_support::FrameWriteMode;
use rocketmq_transport::benchmark_support::FrameWriter;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_util::sync::CancellationToken;

const DEFAULT_CONNECTIONS: usize = 1_000;
const DEFAULT_BODY_BYTES: usize = 4 * 1024 * 1024;
const TLS_SCRATCH_REUSE_CAP_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    RetainedBaseline,
    CappedCandidate,
}

impl Mode {
    fn parse(value: &str) -> Self {
        match value {
            "retained" => Self::RetainedBaseline,
            "capped" => Self::CappedCandidate,
            _ => panic!("--mode must be retained or capped"),
        }
    }
}

enum IdleClient {
    Retained {
        io: TlsStream<TcpStream>,
        scratch: BytesMut,
    },
    Capped(FrameWriter<TlsStream<TcpStream>>),
}

#[derive(Serialize)]
struct Report {
    commit: String,
    os: String,
    kernel: String,
    rust_version: String,
    mode: Mode,
    connections: usize,
    body_bytes: usize,
    encoded_frame_bytes: usize,
    scratch_reuse_cap_bytes: usize,
    rss_before_bytes: u64,
    idle_rss_bytes: u64,
    rss_delta_bytes: i128,
    elapsed_seconds: f64,
    notes: Vec<&'static str>,
}

fn argument(name: &str, default: &str) -> String {
    let mut arguments = std::env::args();
    while let Some(argument) = arguments.next() {
        if argument == name {
            return arguments.next().unwrap_or_else(|| default.to_string());
        }
    }
    default.to_string()
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn repository_commit() -> String {
    let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("transport crate has a workspace parent");
    Command::new("git")
        .args(["-c", "safe.directory=*"])
        .arg("-C")
        .arg(repository_root)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}

#[cfg(target_os = "linux")]
fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read /proc/self/statm");
    let pages = statm
        .split_whitespace()
        .nth(1)
        .expect("resident pages")
        .parse::<u64>()
        .expect("resident page count");
    // SAFETY: `_SC_PAGESIZE` is a read-only process query with no pointer arguments.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    pages.saturating_mul(u64::try_from(page_size).expect("positive page size"))
}

#[cfg(not(target_os = "linux"))]
fn rss_bytes() -> u64 {
    panic!("tls_scratch_rss_collector requires Linux /proc RSS accounting")
}

fn frame(body_bytes: usize) -> EncodedFrame {
    EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(10_100)
            .set_opaque(1)
            .set_body(vec![0x5a; body_bytes]),
    )
    .expect("benchmark frame")
}

async fn start_server(
    encoded_frame_bytes: usize,
) -> (
    std::net::SocketAddr,
    tokio::sync::mpsc::UnboundedReceiver<()>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("TLS listener");
    let address = listener.local_addr().expect("TLS listener address");
    let acceptor = benchmark_support::tls_acceptor();
    let (completed_tx, completed_rx) = tokio::sync::mpsc::unbounded_channel();
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.clone();
    let server = tokio::spawn(async move {
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                () = server_cancellation.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((socket, _)) = accepted else {
                        break;
                    };
                    let acceptor = acceptor.clone();
                    let completed_tx = completed_tx.clone();
                    let session_cancellation = server_cancellation.clone();
                    sessions.spawn(async move {
                        let mut tls = acceptor.accept(socket).await.expect("TLS server handshake");
                        let mut remaining = encoded_frame_bytes;
                        let mut buffer = vec![0_u8; 64 * 1024];
                        while remaining > 0 {
                            let read_bytes = remaining.min(buffer.len());
                            let bytes = tls
                                .read(&mut buffer[..read_bytes])
                                .await
                                .expect("TLS server read");
                            assert_ne!(bytes, 0, "client closed before the complete frame");
                            remaining -= bytes;
                        }
                        completed_tx.send(()).expect("RSS collector still running");
                        session_cancellation.cancelled().await;
                    });
                }
            }
        }
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    });
    (address, completed_rx, cancellation, server)
}

async fn connect_tls(address: std::net::SocketAddr) -> TlsStream<TcpStream> {
    let socket = TcpStream::connect(address).await.expect("TLS client connect");
    let server_name = ServerName::try_from("localhost".to_string()).expect("TLS server name");
    benchmark_support::tls_connector()
        .connect(server_name, socket)
        .await
        .expect("TLS client handshake")
}

async fn create_idle_client(address: std::net::SocketAddr, mode: Mode, frame: &EncodedFrame) -> IdleClient {
    let tls = connect_tls(address).await;
    match mode {
        Mode::RetainedBaseline => {
            let mut io = tls;
            let mut scratch = BytesMut::with_capacity(frame.encoded_len());
            frame.copy_to(&mut scratch);
            io.write_all(&scratch).await.expect("retained TLS write");
            io.flush().await.expect("retained TLS flush");
            IdleClient::Retained { io, scratch }
        }
        Mode::CappedCandidate => {
            let mut writer = FrameWriter::new(
                tls,
                FrameWriteMode::TlsCoalesced {
                    max_plaintext_frame_bytes: frame.encoded_len(),
                },
            )
            .expect("capped TLS writer");
            writer.write_frame(frame).await.expect("capped TLS write");
            IdleClient::Capped(writer)
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mode = Mode::parse(&argument("--mode", "capped"));
    let detected_commit = repository_commit();
    let commit = argument("--commit", &detected_commit);
    let connections = argument("--connections", &DEFAULT_CONNECTIONS.to_string())
        .parse::<usize>()
        .expect("--connections must be usize");
    let body_bytes = argument("--body-bytes", &DEFAULT_BODY_BYTES.to_string())
        .parse::<usize>()
        .expect("--body-bytes must be usize");
    let frame = frame(body_bytes);
    let encoded_frame_bytes = frame.encoded_len();
    let (address, mut completed, cancellation, server) = start_server(encoded_frame_bytes).await;
    let rss_before_bytes = rss_bytes();
    let started = Instant::now();
    let mut clients = Vec::with_capacity(connections);
    for _ in 0..connections {
        clients.push(create_idle_client(address, mode, &frame).await);
    }
    for _ in 0..connections {
        completed.recv().await.expect("server frame completion");
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let idle_rss_bytes = rss_bytes();
    let retained_bytes = clients.iter().fold(0usize, |total, client| match client {
        IdleClient::Retained { io, scratch } => {
            std::hint::black_box(io);
            total.saturating_add(scratch.capacity())
        }
        IdleClient::Capped(writer) => {
            std::hint::black_box(writer);
            total
        }
    });
    std::hint::black_box(retained_bytes);
    let report = Report {
        commit,
        os: std::env::consts::OS.to_string(),
        kernel: command_output("uname", &["-r"]),
        rust_version: command_output("rustc", &["--version"]),
        mode,
        connections,
        body_bytes,
        encoded_frame_bytes,
        scratch_reuse_cap_bytes: TLS_SCRATCH_REUSE_CAP_BYTES,
        rss_before_bytes,
        idle_rss_bytes,
        rss_delta_bytes: i128::from(idle_rss_bytes) - i128::from(rss_before_bytes),
        elapsed_seconds: started.elapsed().as_secs_f64(),
        notes: vec![
            "retained_baseline is a benchmark-only reconstruction of the removed unbounded scratch policy",
            "capped_candidate uses the production FrameWriter and releases scratch capacity above 512 KiB",
            "the measurement uses real tokio-rustls connections over WSL2 loopback and is not physical-NIC data",
        ],
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize RSS report")
    );
    cancellation.cancel();
    server.await.expect("TLS server");
    drop(clients);
}
