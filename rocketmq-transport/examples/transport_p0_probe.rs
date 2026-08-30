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

//! One-shot P0 correctness-overhead probe.
//!
//! ```text
//! cargo run -p rocketmq-transport --release --example transport_p0_probe -- \
//!   --scenario slowloris --connections 1000 --announced-bytes 4194304
//! ```

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::ResourceLimit;
use rocketmq_transport::api::v1::TlsConfig;
use rocketmq_transport::benchmark_support::connect_with_config;
use rocketmq_transport::benchmark_support::SessionProcessor;
use rocketmq_transport::benchmark_support::SessionTransportServer;
use rocketmq_transport::benchmark_support::SessionTransportServerConfig;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

const DEFAULT_CONNECTIONS: usize = 1_000;
const DEFAULT_ANNOUNCED_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PARTIAL_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy)]
struct ProbeConfig {
    connections: usize,
    announced_bytes: usize,
}

#[derive(Debug, Serialize)]
struct ResourceObservation {
    current_count: usize,
    current_bytes: usize,
    rejected_count: usize,
}

#[derive(Debug, Serialize)]
struct ProbeReport {
    scenario: &'static str,
    requested_connections: usize,
    opened_connections: usize,
    announced_frame_bytes: usize,
    announced_bytes_total: usize,
    configured_partial_count: usize,
    configured_partial_bytes: usize,
    bounded_partial: ResourceObservation,
    recovered_partial: ResourceObservation,
    recovered_connections: ResourceObservation,
    rss_start_bytes: u64,
    rss_peak_bytes: u64,
    rss_end_bytes: u64,
    cpu_time_millis: u64,
    fill_time_millis: u64,
    recovery_time_millis: u64,
    total_time_millis: u64,
    healthy_rpc_after_recovery: bool,
}

struct EchoProcessor;

impl SessionProcessor for EchoProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move { Ok(RemotingCommand::create_response_command_with_code(0).set_opaque(request.opaque())) })
    }
}

fn parse_config() -> Result<ProbeConfig> {
    let mut arguments = env::args().skip(1);
    let scenario = arguments.next().unwrap_or_else(|| "slowloris".to_string());
    if scenario != "--scenario" && scenario != "slowloris" {
        bail!("only the slowloris scenario is supported");
    }
    if scenario == "--scenario" {
        let value = arguments.next().context("--scenario requires a value")?;
        if value != "slowloris" {
            bail!("only the slowloris scenario is supported");
        }
    }
    let mut config = ProbeConfig {
        connections: DEFAULT_CONNECTIONS,
        announced_bytes: DEFAULT_ANNOUNCED_BYTES,
    };
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--connections" => {
                config.connections = arguments
                    .next()
                    .context("--connections requires a value")?
                    .parse()
                    .context("parse --connections")?;
            }
            "--announced-bytes" => {
                config.announced_bytes = arguments
                    .next()
                    .context("--announced-bytes requires a value")?
                    .parse()
                    .context("parse --announced-bytes")?;
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    if config.connections == 0 {
        bail!("--connections must be greater than zero");
    }
    if !(8..=FrameLimits::default().max_frame_bytes).contains(&config.announced_bytes) {
        bail!("--announced-bytes must be between 8 and the configured maximum frame bytes");
    }
    Ok(config)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let config = parse_config()?;
    let started_at = Instant::now();
    let cpu_start = process_cpu_time_millis()?;
    let rss_start = process_memory_bytes()?.0;
    let partial_capacity = config.connections.min(DEFAULT_PARTIAL_CAPACITY);
    let partial_bytes = config
        .announced_bytes
        .checked_mul(partial_capacity)
        .context("partial byte capacity overflow")?;
    let connection_bytes = config
        .connections
        .checked_mul(16 * 1024)
        .context("connection byte capacity overflow")?;
    let per_ip_bytes = partial_bytes
        .checked_add(connection_bytes)
        .and_then(|bytes| bytes.checked_add(16 * 1024 * 1024))
        .context("per-IP byte capacity overflow")?;
    let limits = AdmissionLimits {
        connections: ResourceLimit {
            count: config.connections.saturating_add(16),
            bytes: connection_bytes.saturating_add(16 * 16 * 1024),
        },
        partial_frames: ResourceLimit {
            count: partial_capacity,
            bytes: partial_bytes,
        },
        per_ip: ResourceLimit {
            count: config.connections.saturating_mul(2).saturating_add(128),
            bytes: per_ip_bytes,
        },
        per_session: ResourceLimit {
            count: 32,
            bytes: config.announced_bytes.saturating_add(1024 * 1024),
        },
        max_scope_keys: config.connections.saturating_mul(3).saturating_add(256),
        ..AdmissionLimits::default()
    };
    let runtime = RuntimeContext::from_current("transport-p0-probe");
    let service = runtime.service_context("transport-p0-probe");
    let admission = Arc::new(AdmissionController::new(limits));
    let server = SessionTransportServer::bind(
        service,
        SessionTransportServerConfig::loopback(),
        Arc::new(EchoProcessor),
        admission.clone(),
    )
    .await
    .context("bind P0 probe server")?;
    let address = server.local_addr();
    server.start().context("start P0 probe server")?;

    let fill_started_at = Instant::now();
    let total_field = u32::try_from(config.announced_bytes - 4).context("announced frame exceeds u32")?;
    let prefix = total_field.to_be_bytes();
    let mut sockets = Vec::with_capacity(config.connections);
    for _ in 0..config.connections {
        let mut socket = tokio::net::TcpStream::connect(address)
            .await
            .context("open slowloris connection")?;
        socket
            .write_all(&prefix)
            .await
            .context("write slowloris frame prefix")?;
        sockets.push(socket);
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let partial = admission.snapshot().partial_frames;
            if partial.current_count.saturating_add(partial.rejected_count) >= config.connections {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("wait for partial-frame accounting")?;
    let fill_time = fill_started_at.elapsed();
    let bounded = admission.snapshot().partial_frames;
    let (_, rss_peak) = process_memory_bytes()?;

    let recovery_started_at = Instant::now();
    let opened_connections = sockets.len();
    drop(sockets);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let snapshot = admission.snapshot();
            if snapshot.partial_frames.current_count == 0 && snapshot.connections.current_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("wait for slowloris capacity recovery")?;

    let mut healthy = connect_with_config(
        &address.to_string(),
        &TlsConfig::default(),
        FrameLimits::default(),
        RequestDeadline::after(Duration::from_secs(3)),
    )
    .await
    .context("connect recovery RPC")?
    .into_parts()
    .0;
    healthy
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(9_001))
        .await
        .context("send recovery RPC")?;
    let healthy_rpc_after_recovery = healthy
        .receive_command()
        .await
        .and_then(Result::ok)
        .is_some_and(|response| response.opaque() == 9_001);
    drop(healthy);
    let recovery_time = recovery_started_at.elapsed();

    let shutdown = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(3)))
        .await;
    if !shutdown.is_healthy() {
        bail!("probe server shutdown was unhealthy: {}", shutdown.to_json());
    }
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(3)).await;
    if !shutdown.is_healthy() {
        bail!("probe runtime shutdown was unhealthy: {}", shutdown.to_json());
    }
    let final_snapshot = admission.snapshot();
    let (rss_end, rss_final_peak) = process_memory_bytes()?;
    let cpu_time_millis = process_cpu_time_millis()?.saturating_sub(cpu_start);
    let report = ProbeReport {
        scenario: "slowloris",
        requested_connections: config.connections,
        opened_connections,
        announced_frame_bytes: config.announced_bytes,
        announced_bytes_total: config.announced_bytes.saturating_mul(config.connections),
        configured_partial_count: partial_capacity,
        configured_partial_bytes: partial_bytes,
        bounded_partial: observation(bounded),
        recovered_partial: observation(final_snapshot.partial_frames),
        recovered_connections: observation(final_snapshot.connections),
        rss_start_bytes: rss_start,
        rss_peak_bytes: rss_peak.max(rss_final_peak),
        rss_end_bytes: rss_end,
        cpu_time_millis,
        fill_time_millis: millis(fill_time),
        recovery_time_millis: millis(recovery_time),
        total_time_millis: millis(started_at.elapsed()),
        healthy_rpc_after_recovery,
    };
    serde_json::to_writer_pretty(std::io::stdout().lock(), &report).context("write P0 probe JSON")?;
    println!();
    Ok(())
}

fn observation(snapshot: rocketmq_transport::api::v1::ResourceSnapshot) -> ResourceObservation {
    ResourceObservation {
        current_count: snapshot.current_count,
        current_bytes: snapshot.current_bytes,
        rejected_count: snapshot.rejected_count,
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(windows)]
fn process_memory_bytes() -> Result<(u64, u64)> {
    use std::mem::size_of;

    use windows::Win32::System::ProcessStatus::GetProcessMemoryInfo;
    use windows::Win32::System::ProcessStatus::PROCESS_MEMORY_COUNTERS;
    use windows::Win32::System::Threading::GetCurrentProcess;

    let mut counters = PROCESS_MEMORY_COUNTERS::default();
    let structure_size = u32::try_from(size_of::<PROCESS_MEMORY_COUNTERS>()).context("process counter size")?;
    counters.cb = structure_size;
    // SAFETY: the current-process pseudo handle is valid, the counter storage is writable for the
    // declared structure size, and the operating system does not retain the pointer.
    unsafe {
        GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, structure_size)
            .context("query process memory counters")?;
    }
    Ok((
        u64::try_from(counters.WorkingSetSize).context("working set exceeds u64")?,
        u64::try_from(counters.PeakWorkingSetSize).context("peak working set exceeds u64")?,
    ))
}

#[cfg(windows)]
fn process_cpu_time_millis() -> Result<u64> {
    use windows::Win32::Foundation::FILETIME;
    use windows::Win32::System::Threading::GetCurrentProcess;
    use windows::Win32::System::Threading::GetProcessTimes;

    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: all FILETIME pointers refer to valid writable storage and the current-process pseudo
    // handle remains valid for the duration of the call.
    unsafe {
        GetProcessTimes(GetCurrentProcess(), &mut creation, &mut exit, &mut kernel, &mut user)
            .context("query process CPU time")?;
    }
    Ok((filetime_ticks(kernel).saturating_add(filetime_ticks(user))) / 10_000)
}

#[cfg(windows)]
fn filetime_ticks(value: windows::Win32::Foundation::FILETIME) -> u64 {
    (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime)
}

#[cfg(unix)]
fn process_memory_bytes() -> Result<(u64, u64)> {
    let peak = process_usage()?.0;
    let statm = std::fs::read_to_string("/proc/self/statm").context("read /proc/self/statm")?;
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .context("missing resident pages in /proc/self/statm")?
        .parse()
        .context("parse resident pages")?;
    // SAFETY: sysconf with _SC_PAGESIZE has no pointer arguments or caller-owned invariants.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        bail!("sysconf(_SC_PAGESIZE) failed");
    }
    Ok((resident_pages.saturating_mul(page_size as u64), peak))
}

#[cfg(unix)]
fn process_cpu_time_millis() -> Result<u64> {
    Ok(process_usage()?.1)
}

#[cfg(unix)]
fn process_usage() -> Result<(u64, u64)> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: usage points to valid writable storage and getrusage initializes it on success.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        bail!("getrusage failed: {}", std::io::Error::last_os_error());
    }
    // SAFETY: a zero result from getrusage guarantees initialization.
    let usage = unsafe { usage.assume_init() };
    let peak = u64::try_from(usage.ru_maxrss)
        .context("ru_maxrss was negative")?
        .saturating_mul(1024);
    let cpu_micros = timeval_micros(usage.ru_utime).saturating_add(timeval_micros(usage.ru_stime));
    Ok((peak, cpu_micros / 1_000))
}

#[cfg(unix)]
fn timeval_micros(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or_default()
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(value.tv_usec).unwrap_or_default())
}
