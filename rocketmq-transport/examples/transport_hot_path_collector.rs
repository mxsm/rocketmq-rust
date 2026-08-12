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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::benchmark_support::Connection;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

#[derive(Serialize)]
struct Sample {
    sample: usize,
    scenario: String,
    seconds: f64,
    operations: usize,
    operations_per_second: f64,
    p50_micros: u128,
    p99_micros: u128,
    connections: usize,
    rss_bytes: Option<u64>,
    cpu_seconds: Option<f64>,
    allocations_per_operation: Option<f64>,
    syscalls_per_operation: Option<f64>,
}

#[derive(Serialize)]
struct Report {
    commit: String,
    os: String,
    architecture: String,
    rust_version: String,
    tokio_workers: usize,
    notes: Vec<&'static str>,
    samples: Vec<Sample>,
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

#[cfg(target_os = "linux")]
fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    // SAFETY: `_SC_PAGESIZE` is a read-only process query with no pointer arguments.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(page_size)
        .ok()
        .and_then(|page_size| pages.checked_mul(page_size))
}

#[cfg(target_os = "windows")]
fn rss_bytes() -> Option<u64> {
    use windows::Win32::System::ProcessStatus::K32GetProcessMemoryInfo;
    use windows::Win32::System::ProcessStatus::PROCESS_MEMORY_COUNTERS;
    use windows::Win32::System::Threading::GetCurrentProcess;

    let mut counters = PROCESS_MEMORY_COUNTERS::default();
    // SAFETY: the current-process pseudo handle is always valid and `counters` is a writable,
    // correctly sized `PROCESS_MEMORY_COUNTERS` for the duration of this call.
    let succeeded = unsafe {
        K32GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    }
    .as_bool();
    succeeded.then_some(counters.WorkingSetSize as u64)
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn rss_bytes() -> Option<u64> {
    None
}

async fn collect_persistent_tcp(sample: usize, seconds: Duration, scenario: &str) -> Sample {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("collector listener");
    let address = listener.local_addr().expect("collector listener address");
    let connections = Arc::new(AtomicUsize::new(0));
    let server_connections = connections.clone();
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
                    server_connections.fetch_add(1, Ordering::Relaxed);
                    sessions.spawn(async move {
                        let mut connection = Connection::new(socket);
                        while let Some(Ok(request)) = connection.receive_command().await {
                            let response =
                                RemotingCommand::create_success_response_command().set_opaque(request.opaque());
                            if connection.send_command(response).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            }
        }
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    });
    let socket = TcpStream::connect(address).await.expect("collector connect");
    let mut client = Connection::new(socket);
    let started = Instant::now();
    let mut latencies = Vec::new();
    let mut opaque = 1_i32;
    while started.elapsed() < seconds {
        let operation_started = Instant::now();
        client
            .send_command(
                RemotingCommand::create_remoting_command(10_100)
                    .set_opaque(opaque)
                    .set_body(vec![0x5a; 128]),
            )
            .await
            .expect("collector request write");
        let response = client
            .receive_command()
            .await
            .expect("collector server closed")
            .expect("collector response decode");
        assert_eq!(response.opaque(), opaque);
        latencies.push(operation_started.elapsed().as_micros());
        opaque = opaque.wrapping_add(1);
    }
    let elapsed = started.elapsed();
    client.shutdown().await.expect("collector client shutdown");
    cancellation.cancel();
    server.await.expect("collector server");
    latencies.sort_unstable();
    let operations = latencies.len();
    let percentile = |numerator: usize| {
        let index = operations.saturating_sub(1).saturating_mul(numerator) / 100;
        latencies.get(index).copied().unwrap_or_default()
    };
    Sample {
        sample,
        scenario: scenario.to_string(),
        seconds: elapsed.as_secs_f64(),
        operations,
        operations_per_second: operations as f64 / elapsed.as_secs_f64(),
        p50_micros: percentile(50),
        p99_micros: percentile(99),
        connections: connections.load(Ordering::Relaxed),
        rss_bytes: rss_bytes(),
        cpu_seconds: None,
        allocations_per_operation: None,
        syscalls_per_operation: None,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let scenario = argument("--scenario", "persistent-tcp");
    let seconds = argument("--seconds", "30")
        .parse::<u64>()
        .expect("--seconds must be u64");
    let sample_count = argument("--samples", "3")
        .parse::<usize>()
        .expect("--samples must be usize");
    assert_eq!(scenario, "persistent-tcp", "supported scenario: persistent-tcp");
    let mut samples = Vec::with_capacity(sample_count);
    for sample in 1..=sample_count {
        samples.push(collect_persistent_tcp(sample, Duration::from_secs(seconds), &scenario).await);
    }
    let report = Report {
        commit: command_output("git", &["rev-parse", "HEAD"]),
        os: std::env::consts::OS.to_string(),
        architecture: std::env::consts::ARCH.to_string(),
        rust_version: command_output("rustc", &["--version"]),
        tokio_workers: std::thread::available_parallelism().map_or(1, usize::from),
        notes: vec![
            "CPU seconds and allocation counts require an external profiler and are null in the portable collector.",
            "Use perf stat/strace -c on Linux for cycles, CPU time, and syscall counts.",
        ],
        samples,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize collector report")
    );
}
