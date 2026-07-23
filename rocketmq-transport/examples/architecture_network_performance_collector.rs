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

//! Target-hardware collector for the M10 network performance profiles.
//!
//! Run one complete variant from the workspace root with:
//!
//! ```text
//! cargo run --release --quiet -p rocketmq-transport \
//!   --example architecture_network_performance_collector -- \
//!   connection-soak mixed-tls-churn
//!
//! cargo run --release --quiet -p rocketmq-transport \
//!   --example architecture_network_performance_collector -- \
//!   overload bounded-rejection
//! ```
//!
//! The public command always uses the frozen production profile parameters.
//! Test-only scaled specifications exercise the identical collection path
//! without weakening the target-hardware contract.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::collections::BTreeMap;
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::process;
use std::process::Command;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use futures_util::stream;
use futures_util::StreamExt;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::admission::AdmissionSnapshot;
use rocketmq_transport::admission::ResourceLimit;
use rocketmq_transport::client::TransportClient;
use rocketmq_transport::config::TlsClientConfig;
use rocketmq_transport::config::TlsConfig;
use rocketmq_transport::config::TlsMode;
use rocketmq_transport::connection::transport_io_snapshot;
use rocketmq_transport::server::RequestProcessor;
use rocketmq_transport::server::TransportServer;
use rocketmq_transport::server::TransportServerConfig;
use serde::Deserialize;
use serde::Serialize;
use tokio::runtime::Builder;

const CONNECTION_SOAK_PROFILE: &str = "connection-soak";
const CONNECTION_SOAK_VARIANT: &str = "mixed-tls-churn";
const OVERLOAD_PROFILE: &str = "overload";
const OVERLOAD_VARIANT: &str = "bounded-rejection";
const MESSAGE_SIZE_BYTES: usize = 1024;
const SAMPLE_COUNT: usize = 5;
const PRIMING_SAMPLE_COUNT: usize = 2;
const CONNECTION_SOAK_CONNECTIONS: usize = 10_000;
const CONNECTION_SOAK_DURATION: Duration = Duration::from_secs(900);
const CONNECTION_SOAK_COOLDOWN: Duration = Duration::from_secs(60);
const OVERLOAD_DURATION: Duration = Duration::from_secs(300);
const OVERLOAD_DATA_CAPACITY: usize = 16;
const OVERLOAD_CONTROL_RESERVE: usize = 2;
const OVERLOAD_PROCESSOR_DELAY: Duration = Duration::from_millis(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const OVERLOAD_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

// SAFETY: Every allocation operation is delegated unchanged to the process
// System allocator. The relaxed counter is observational and does not affect
// pointer provenance, layout, allocation, or deallocation behavior.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller supplies the GlobalAlloc contract and the same
        // layout is forwarded unchanged to the System allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller supplies the GlobalAlloc contract and the same
        // layout is forwarded unchanged to the System allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the delegated System
        // allocator and are forwarded without modification.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The pointer, original layout, and requested size are
        // forwarded unchanged to the System allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkloadKind {
    ConnectionSoak,
    Overload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Scenario {
    profile: &'static str,
    variant: &'static str,
    workload: WorkloadKind,
}

impl Scenario {
    fn parse(profile: &str, variant: &str) -> Result<Self> {
        match (profile, variant) {
            (CONNECTION_SOAK_PROFILE, CONNECTION_SOAK_VARIANT) => Ok(Self {
                profile: CONNECTION_SOAK_PROFILE,
                variant: CONNECTION_SOAK_VARIANT,
                workload: WorkloadKind::ConnectionSoak,
            }),
            (OVERLOAD_PROFILE, OVERLOAD_VARIANT) => Ok(Self {
                profile: OVERLOAD_PROFILE,
                variant: OVERLOAD_VARIANT,
                workload: WorkloadKind::Overload,
            }),
            _ => bail!("unsupported network performance profile/variant: {profile}/{variant}"),
        }
    }

    fn production_spec(self) -> RunSpec {
        match self.workload {
            WorkloadKind::ConnectionSoak => RunSpec {
                scenario: self,
                operation_count: CONNECTION_SOAK_CONNECTIONS,
                duration: CONNECTION_SOAK_DURATION,
                cooldown: CONNECTION_SOAK_COOLDOWN,
                data_capacity: 0,
                control_reserve: 0,
                processor_delay: Duration::ZERO,
                request_timeout: REQUEST_TIMEOUT,
                convergence_timeout: CONNECTION_SOAK_COOLDOWN,
            },
            WorkloadKind::Overload => RunSpec {
                scenario: self,
                operation_count: 0,
                duration: OVERLOAD_DURATION,
                cooldown: Duration::ZERO,
                data_capacity: OVERLOAD_DATA_CAPACITY,
                control_reserve: OVERLOAD_CONTROL_RESERVE,
                processor_delay: OVERLOAD_PROCESSOR_DELAY,
                request_timeout: REQUEST_TIMEOUT,
                convergence_timeout: OVERLOAD_CONVERGENCE_TIMEOUT,
            },
        }
    }

    fn includes_cooldown_metrics(self) -> bool {
        self.workload == WorkloadKind::ConnectionSoak
    }

    fn includes_control_ratio(self) -> bool {
        self.workload == WorkloadKind::Overload
    }
}

#[derive(Clone, Copy, Debug)]
struct RunSpec {
    scenario: Scenario,
    operation_count: usize,
    duration: Duration,
    cooldown: Duration,
    data_capacity: usize,
    control_reserve: usize,
    processor_delay: Duration,
    request_timeout: Duration,
    convergence_timeout: Duration,
}

impl RunSpec {
    fn validate(self) -> Result<()> {
        ensure!(!self.duration.is_zero(), "measurement duration must be positive");
        ensure!(!self.request_timeout.is_zero(), "request timeout must be positive");
        ensure!(
            !self.convergence_timeout.is_zero(),
            "resource convergence timeout must be positive"
        );
        match self.scenario.workload {
            WorkloadKind::ConnectionSoak => {
                ensure!(
                    self.operation_count > 0 && self.operation_count.is_multiple_of(2),
                    "connection soak requires a positive even connection count"
                );
                ensure!(!self.cooldown.is_zero(), "connection soak cooldown must be positive");
            }
            WorkloadKind::Overload => {
                ensure!(self.data_capacity > 0, "overload data capacity must be positive");
                ensure!(self.control_reserve > 0, "overload control reserve must be positive");
                ensure!(
                    !self.processor_delay.is_zero(),
                    "overload processor delay must be positive"
                );
                ensure!(self.operation_count == 0, "overload operation count is duration-driven");
            }
        }
        Ok(())
    }

    fn overload_concurrency(self) -> Result<usize> {
        ensure!(
            self.scenario.workload == WorkloadKind::Overload,
            "offered concurrency is only defined for overload"
        );
        self.data_capacity
            .checked_mul(2)
            .ok_or_else(|| anyhow!("overload concurrency overflowed"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Invocation {
    Collect(Scenario),
    Sample(Scenario),
}

fn parse_invocation(args: &[String]) -> Result<Invocation> {
    match args {
        [profile, variant] => Ok(Invocation::Collect(Scenario::parse(profile, variant)?)),
        [sample, profile, variant] if sample == "--sample" => {
            Ok(Invocation::Sample(Scenario::parse(profile, variant)?))
        }
        _ => bail!(
            "usage: architecture_network_performance_collector [--sample] <connection-soak|overload> \
             <mixed-tls-churn|bounded-rejection>"
        ),
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SampleObservation {
    throughput_per_second: f64,
    p99_latency_us: f64,
    peak_rss_bytes: f64,
    allocations_per_operation: f64,
    io_amplification_ratio: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    live_tasks_after_cooldown: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_requests_after_cooldown: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    control_plane_success_ratio: Option<f64>,
    data_rejections: u64,
    server_rejections: u64,
    resources_converged: bool,
    retained_child_groups: usize,
}

impl SampleObservation {
    fn validate(&self, scenario: Scenario) -> Result<()> {
        for (metric, value) in [
            ("throughput_per_second", self.throughput_per_second),
            ("p99_latency_us", self.p99_latency_us),
            ("peak_rss_bytes", self.peak_rss_bytes),
            ("allocations_per_operation", self.allocations_per_operation),
            ("io_amplification_ratio", self.io_amplification_ratio),
        ] {
            ensure!(
                value.is_finite() && value > 0.0,
                "{metric} must be finite and positive, got {value}"
            );
        }
        ensure!(
            self.live_tasks_after_cooldown.is_some() == scenario.includes_cooldown_metrics(),
            "{}/{} live task metric presence does not match the profile",
            scenario.profile,
            scenario.variant
        );
        ensure!(
            self.pending_requests_after_cooldown.is_some() == scenario.includes_cooldown_metrics(),
            "{}/{} pending request metric presence does not match the profile",
            scenario.profile,
            scenario.variant
        );
        for (metric, value) in [
            ("live_tasks_after_cooldown", self.live_tasks_after_cooldown),
            ("pending_requests_after_cooldown", self.pending_requests_after_cooldown),
        ] {
            if let Some(value) = value {
                ensure!(
                    value.is_finite() && value == 0.0,
                    "{metric} must converge to zero, got {value}"
                );
            }
        }
        ensure!(
            self.control_plane_success_ratio.is_some() == scenario.includes_control_ratio(),
            "{}/{} control-plane metric presence does not match the profile",
            scenario.profile,
            scenario.variant
        );
        if let Some(ratio) = self.control_plane_success_ratio {
            ensure!(
                ratio.is_finite() && ratio == 1.0,
                "control_plane_success_ratio must be 1.0, got {ratio}"
            );
        }
        ensure!(self.resources_converged, "transport resources did not converge");
        ensure!(
            self.retained_child_groups == 0,
            "transport retained {} per-operation child groups",
            self.retained_child_groups
        );
        match scenario.workload {
            WorkloadKind::ConnectionSoak => {
                ensure!(
                    self.data_rejections == 0 && self.server_rejections == 0,
                    "connection soak unexpectedly rejected traffic"
                );
            }
            WorkloadKind::Overload => {
                ensure!(
                    self.data_rejections > 0,
                    "overload did not observe a typed data rejection"
                );
                ensure!(
                    self.server_rejections > 0,
                    "server admission did not record an overload rejection"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct MetricSamples {
    samples: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct MeasurementEnvelope<'a> {
    schema_version: u8,
    profile: &'static str,
    variant: &'a str,
    metrics: BTreeMap<&'static str, MetricSamples>,
}

struct DelayedAckProcessor {
    data_delay: Duration,
}

impl RequestProcessor for DelayedAckProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            if request.code() == RequestCode::SendMessage.to_i32() && !self.data_delay.is_zero() {
                tokio::time::sleep(self.data_delay).await;
            }
            Ok(RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()))
        })
    }
}

struct TransportHarness {
    runtime: RuntimeContext,
    server: Arc<TransportServer>,
    client: Arc<TransportClient>,
    server_admission: Arc<AdmissionController>,
    baseline_tasks: usize,
    baseline_child_groups: usize,
}

impl TransportHarness {
    async fn start(spec: RunSpec) -> Result<Self> {
        let runtime = RuntimeContext::try_from_current("architecture-network-collector")
            .context("create collector runtime context")?;
        let mut server_config = TransportServerConfig::loopback();
        server_config.tls.test_mode_enable = true;
        server_config.tls.server.mode = TlsMode::Permissive;
        server_config.handshake_timeout = spec.request_timeout;
        server_config.request_timeout = spec.request_timeout;
        let server_admission = Arc::new(AdmissionController::new(server_admission_limits(spec)?));
        let server = TransportServer::bind(
            runtime.service_context("architecture-network-server"),
            server_config,
            Arc::new(DelayedAckProcessor {
                data_delay: spec.processor_delay,
            }),
            server_admission.clone(),
        )
        .await
        .context("bind architecture network collector server")?;
        server.start().context("start architecture network collector server")?;
        tokio::task::yield_now().await;
        let baseline_tasks = server.live_task_count();
        let baseline_child_groups = server.owned_child_group_count();
        ensure!(baseline_tasks > 0, "transport accept task did not start");

        let client = Arc::new(TransportClient::new(
            runtime.service_context("architecture-network-client"),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        Ok(Self {
            runtime,
            server,
            client,
            server_admission,
            baseline_tasks,
            baseline_child_groups,
        })
    }

    async fn shutdown(self) -> Result<()> {
        let server_report = self
            .server
            .shutdown_until(ShutdownDeadline::after(SHUTDOWN_TIMEOUT))
            .await;
        ensure!(
            server_report.is_healthy(),
            "transport server shutdown was unhealthy: {}",
            server_report.to_json()
        );
        let runtime_report = self.runtime.shutdown_tasks(SHUTDOWN_TIMEOUT).await;
        ensure!(
            runtime_report.is_healthy(),
            "collector runtime shutdown was unhealthy: {}",
            runtime_report.to_json()
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct ConvergenceSnapshot {
    live_task_delta: usize,
    pending_count: usize,
    pending_bytes: usize,
    retained_child_groups: usize,
    admission: AdmissionSnapshot,
}

impl ConvergenceSnapshot {
    fn is_idle(self) -> bool {
        self.live_task_delta == 0
            && self.pending_count == 0
            && self.pending_bytes == 0
            && self.retained_child_groups == 0
            && admission_is_idle(self.admission)
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("architecture network performance collector failed: {error:#}");
        process::exit(2);
    }
}

fn run() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match parse_invocation(&args)? {
        Invocation::Collect(scenario) => write_measurement(collect_samples(scenario)?)?,
        Invocation::Sample(scenario) => write_sample(collect_one_sample(scenario.production_spec())?)?,
    }
    Ok(())
}

fn collect_samples(scenario: Scenario) -> Result<MeasurementEnvelope<'static>> {
    let executable = env::current_exe().context("resolve collector executable")?;
    let mut observations = Vec::with_capacity(SAMPLE_COUNT);
    for sample_index in 0..SAMPLE_COUNT + PRIMING_SAMPLE_COUNT {
        let output = Command::new(&executable)
            .args(["--sample", scenario.profile, scenario.variant])
            .output()
            .with_context(|| format!("start isolated sample {sample_index}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "isolated sample {sample_index} exited with {}: {}",
                output.status,
                stderr.trim()
            );
        }
        let observation: SampleObservation = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("parse isolated sample {sample_index} JSON"))?;
        observation
            .validate(scenario)
            .with_context(|| format!("validate isolated sample {sample_index}"))?;
        if sample_index >= PRIMING_SAMPLE_COUNT {
            observations.push(observation);
        }
    }
    build_envelope(scenario, observations)
}

fn build_envelope(scenario: Scenario, observations: Vec<SampleObservation>) -> Result<MeasurementEnvelope<'static>> {
    ensure!(
        observations.len() == SAMPLE_COUNT,
        "expected {SAMPLE_COUNT} isolated samples, got {}",
        observations.len()
    );
    for observation in &observations {
        observation.validate(scenario)?;
    }
    let mut metrics = BTreeMap::new();
    metrics.insert(
        "throughput_per_second",
        MetricSamples {
            samples: observations.iter().map(|sample| sample.throughput_per_second).collect(),
        },
    );
    metrics.insert(
        "p99_latency_us",
        MetricSamples {
            samples: observations.iter().map(|sample| sample.p99_latency_us).collect(),
        },
    );
    metrics.insert(
        "peak_rss_bytes",
        MetricSamples {
            samples: observations.iter().map(|sample| sample.peak_rss_bytes).collect(),
        },
    );
    metrics.insert(
        "allocations_per_operation",
        MetricSamples {
            samples: observations
                .iter()
                .map(|sample| sample.allocations_per_operation)
                .collect(),
        },
    );
    metrics.insert(
        "io_amplification_ratio",
        MetricSamples {
            samples: observations
                .iter()
                .map(|sample| sample.io_amplification_ratio)
                .collect(),
        },
    );
    if scenario.includes_cooldown_metrics() {
        metrics.insert(
            "live_tasks_after_cooldown",
            MetricSamples {
                samples: observations
                    .iter()
                    .map(|sample| {
                        sample
                            .live_tasks_after_cooldown
                            .ok_or_else(|| anyhow!("connection soak sample omitted live task metric"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            },
        );
        metrics.insert(
            "pending_requests_after_cooldown",
            MetricSamples {
                samples: observations
                    .iter()
                    .map(|sample| {
                        sample
                            .pending_requests_after_cooldown
                            .ok_or_else(|| anyhow!("connection soak sample omitted pending request metric"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            },
        );
    }
    if scenario.includes_control_ratio() {
        metrics.insert(
            "control_plane_success_ratio",
            MetricSamples {
                samples: observations
                    .iter()
                    .map(|sample| {
                        sample
                            .control_plane_success_ratio
                            .ok_or_else(|| anyhow!("overload sample omitted control-plane metric"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            },
        );
    }
    Ok(MeasurementEnvelope {
        schema_version: 1,
        profile: scenario.profile,
        variant: scenario.variant,
        metrics,
    })
}

fn collect_one_sample(spec: RunSpec) -> Result<SampleObservation> {
    spec.validate()?;
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("architecture-network-collector")
        .build()
        .context("build collector Tokio runtime")?;
    runtime.block_on(collect_one_sample_async(spec))
}

async fn collect_one_sample_async(spec: RunSpec) -> Result<SampleObservation> {
    match spec.scenario.workload {
        WorkloadKind::ConnectionSoak => collect_connection_soak(spec).await,
        WorkloadKind::Overload => collect_overload(spec).await,
    }
}

async fn collect_connection_soak(spec: RunSpec) -> Result<SampleObservation> {
    let harness = TransportHarness::start(spec).await?;
    let result = collect_connection_soak_inner(spec, &harness).await;
    let shutdown = harness.shutdown().await;
    match result {
        Ok(observation) => {
            shutdown?;
            Ok(observation)
        }
        Err(error) => Err(error),
    }
}

async fn collect_connection_soak_inner(spec: RunSpec, harness: &TransportHarness) -> Result<SampleObservation> {
    let mut latencies = Vec::with_capacity(spec.operation_count);
    let io_before = transport_io_snapshot();
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let started = Instant::now();
    for operation in 0..spec.operation_count {
        let target = spec.duration.mul_f64(operation as f64 / spec.operation_count as f64);
        if let Some(delay) = target.checked_sub(started.elapsed()) {
            tokio::time::sleep(delay).await;
        }
        let tls = client_tls_config(operation.is_multiple_of(2));
        let request_started = Instant::now();
        let response = harness
            .client
            .invoke_with_config(
                harness.server.local_addr(),
                data_request(),
                &tls,
                ShutdownDeadline::after(spec.request_timeout),
            )
            .await
            .with_context(|| format!("connection soak request {operation}"))?;
        latencies.push(request_started.elapsed());
        ensure!(
            response.code() == ResponseCode::Success.to_i32(),
            "connection soak request {operation} returned response code {}",
            response.code()
        );
    }
    if let Some(delay) = spec.duration.checked_sub(started.elapsed()) {
        tokio::time::sleep(delay).await;
    }
    let measured_elapsed = started.elapsed();
    tokio::time::sleep(spec.cooldown).await;
    let convergence = convergence_snapshot(harness)?;
    ensure!(
        convergence.is_idle(),
        "connection soak did not converge after {:?}: {convergence:?}",
        spec.cooldown
    );
    let allocations = counter_delta(
        ALLOCATION_CALLS.load(Ordering::Relaxed),
        allocations_before,
        "allocation",
    )?;
    let encoded_bytes = counter_delta(
        transport_io_snapshot().encoded_bytes_written,
        io_before.encoded_bytes_written,
        "transport I/O",
    )?;
    let payload_bytes = payload_bytes(spec.operation_count)?;
    let server_rejections = total_rejections(convergence.admission);
    ensure!(encoded_bytes > 0, "connection soak recorded no successful encoded I/O");
    let observation = SampleObservation {
        throughput_per_second: spec.operation_count as f64 / measured_elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocations as f64 / spec.operation_count as f64,
        io_amplification_ratio: encoded_bytes as f64 / payload_bytes as f64,
        live_tasks_after_cooldown: Some(convergence.live_task_delta as f64),
        pending_requests_after_cooldown: Some(convergence.pending_count as f64),
        control_plane_success_ratio: None,
        data_rejections: 0,
        server_rejections,
        resources_converged: true,
        retained_child_groups: convergence.retained_child_groups,
    };
    observation.validate(spec.scenario)?;
    Ok(observation)
}

async fn collect_overload(spec: RunSpec) -> Result<SampleObservation> {
    let harness = TransportHarness::start(spec).await?;
    let result = collect_overload_inner(spec, &harness).await;
    let shutdown = harness.shutdown().await;
    match result {
        Ok(observation) => {
            shutdown?;
            Ok(observation)
        }
        Err(error) => Err(error),
    }
}

async fn collect_overload_inner(spec: RunSpec, harness: &TransportHarness) -> Result<SampleObservation> {
    let offered_concurrency = spec.overload_concurrency()?;
    let estimated_batches = (spec.duration.as_secs_f64() / spec.processor_delay.as_secs_f64()).ceil() as usize;
    let estimated_operations = estimated_batches.saturating_mul(offered_concurrency);
    let mut latencies = Vec::with_capacity(estimated_operations);
    let io_before = transport_io_snapshot();
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let started = Instant::now();
    let mut data_attempts = 0_usize;
    let mut data_rejections = 0_u64;
    let mut control_attempts = 0_u64;
    let mut control_successes = 0_u64;
    let mut batch_index = 0_u64;

    while started.elapsed() < spec.duration || data_attempts == 0 {
        let server_addr = harness.server.local_addr();
        let request_timeout = spec.request_timeout;
        let client = harness.client.clone();
        let data_batch = stream::iter(0..offered_concurrency)
            .map(move |offset| {
                let client = client.clone();
                async move {
                    let tls = client_tls_config(offset.is_multiple_of(2));
                    let request_started = Instant::now();
                    let result = client
                        .invoke_with_config(
                            server_addr,
                            data_request(),
                            &tls,
                            ShutdownDeadline::after(request_timeout),
                        )
                        .await;
                    (request_started.elapsed(), result)
                }
            })
            .buffer_unordered(offered_concurrency)
            .collect::<Vec<_>>();
        let control_client = harness.client.clone();
        let control_tls = client_tls_config(batch_index.is_multiple_of(2));
        let control = async move {
            control_client
                .invoke_with_config(
                    server_addr,
                    RemotingCommand::create_remoting_command(RequestCode::HeartBeat.to_i32()),
                    &control_tls,
                    ShutdownDeadline::after(request_timeout),
                )
                .await
        };
        let observe_admission = async {
            tokio::task::yield_now().await;
            harness.server_admission.snapshot()
        };
        let (data_results, control_result, observed_admission) = tokio::join!(data_batch, control, observe_admission);
        ensure_admission_within_limits(observed_admission, spec)?;

        for (latency, result) in data_results {
            latencies.push(latency);
            data_attempts += 1;
            match result.context("overload data invocation")?.code() {
                code if code == ResponseCode::Success.to_i32() => {}
                code if code == ResponseCode::SystemBusy.to_i32() => data_rejections += 1,
                code => bail!("overload data invocation returned unexpected response code {code}"),
            }
        }
        control_attempts += 1;
        let control_response = control_result.context("overload control-plane invocation")?;
        ensure!(
            control_response.code() == ResponseCode::Success.to_i32(),
            "overload control-plane invocation returned response code {}",
            control_response.code()
        );
        control_successes += 1;
        batch_index += 1;
    }
    let measured_elapsed = started.elapsed();
    let convergence = wait_for_convergence(harness, spec.convergence_timeout).await?;
    ensure_admission_within_limits(convergence.admission, spec)?;
    ensure!(data_rejections > 0, "overload produced no typed SystemBusy response");
    ensure!(control_attempts > 0, "overload issued no control-plane requests");
    let server_rejections = total_rejections(convergence.admission);
    ensure!(server_rejections > 0, "server admission recorded no overload rejection");

    let allocations = counter_delta(
        ALLOCATION_CALLS.load(Ordering::Relaxed),
        allocations_before,
        "allocation",
    )?;
    let encoded_bytes = counter_delta(
        transport_io_snapshot().encoded_bytes_written,
        io_before.encoded_bytes_written,
        "transport I/O",
    )?;
    let payload_bytes = payload_bytes(data_attempts)?;
    ensure!(encoded_bytes > 0, "overload recorded no successful encoded I/O");
    let observation = SampleObservation {
        throughput_per_second: data_attempts as f64 / measured_elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocations as f64 / data_attempts as f64,
        io_amplification_ratio: encoded_bytes as f64 / payload_bytes as f64,
        live_tasks_after_cooldown: None,
        pending_requests_after_cooldown: None,
        control_plane_success_ratio: Some(control_successes as f64 / control_attempts as f64),
        data_rejections,
        server_rejections,
        resources_converged: convergence.is_idle(),
        retained_child_groups: convergence.retained_child_groups,
    };
    observation.validate(spec.scenario)?;
    Ok(observation)
}

fn server_admission_limits(spec: RunSpec) -> Result<AdmissionLimits> {
    if spec.scenario.workload != WorkloadKind::Overload {
        return Ok(AdmissionLimits::default());
    }
    let total_processors = spec
        .data_capacity
        .checked_add(spec.control_reserve)
        .ok_or_else(|| anyhow!("overload processor capacity overflowed"))?;
    Ok(AdmissionLimits {
        processors: ResourceLimit {
            count: total_processors,
            bytes: 64 * 1024 * 1024,
        },
        control_reserve: ResourceLimit {
            count: spec.control_reserve,
            bytes: 1024 * 1024,
        },
        ..AdmissionLimits::default()
    })
}

fn ensure_admission_within_limits(snapshot: AdmissionSnapshot, spec: RunSpec) -> Result<()> {
    if spec.scenario.workload != WorkloadKind::Overload {
        return Ok(());
    }
    let limits = server_admission_limits(spec)?;
    for (resource, current_count, current_bytes, limit) in [
        (
            "connections",
            snapshot.connections.current_count,
            snapshot.connections.current_bytes,
            limits.connections,
        ),
        (
            "handshakes",
            snapshot.handshakes.current_count,
            snapshot.handshakes.current_bytes,
            limits.handshakes,
        ),
        (
            "inflight",
            snapshot.inflight.current_count,
            snapshot.inflight.current_bytes,
            limits.inflight,
        ),
        (
            "queued",
            snapshot.queued.current_count,
            snapshot.queued.current_bytes,
            limits.queued,
        ),
        (
            "processors",
            snapshot.processors.current_count,
            snapshot.processors.current_bytes,
            limits.processors,
        ),
    ] {
        ensure!(
            current_count <= limit.count && current_bytes <= limit.bytes,
            "{resource} admission exceeded its configured bound"
        );
    }
    Ok(())
}

fn convergence_snapshot(harness: &TransportHarness) -> Result<ConvergenceSnapshot> {
    let live_tasks = harness.server.live_task_count();
    let live_task_delta = live_tasks
        .checked_sub(harness.baseline_tasks)
        .ok_or_else(|| anyhow!("server accept task disappeared during measurement"))?;
    let child_groups = harness.server.owned_child_group_count();
    let retained_child_groups = child_groups
        .checked_sub(harness.baseline_child_groups)
        .ok_or_else(|| anyhow!("server baseline child ownership disappeared during measurement"))?;
    let pending = harness.client.pending_usage();
    Ok(ConvergenceSnapshot {
        live_task_delta,
        pending_count: pending.count,
        pending_bytes: pending.bytes,
        retained_child_groups,
        admission: harness.server_admission.snapshot(),
    })
}

async fn wait_for_convergence(harness: &TransportHarness, timeout: Duration) -> Result<ConvergenceSnapshot> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = convergence_snapshot(harness)?;
        if snapshot.is_idle() {
            return Ok(snapshot);
        }
        ensure!(
            Instant::now() < deadline,
            "transport resources did not converge within {timeout:?}: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn admission_is_idle(snapshot: AdmissionSnapshot) -> bool {
    [
        snapshot.connections,
        snapshot.handshakes,
        snapshot.inflight,
        snapshot.queued,
        snapshot.processors,
    ]
    .into_iter()
    .all(|resource| resource.current_count == 0 && resource.current_bytes == 0)
}

fn total_rejections(snapshot: AdmissionSnapshot) -> u64 {
    [
        snapshot.connections,
        snapshot.handshakes,
        snapshot.inflight,
        snapshot.queued,
        snapshot.processors,
    ]
    .into_iter()
    .map(|resource| resource.rejected_count as u64)
    .sum()
}

fn client_tls_config(enable: bool) -> TlsConfig {
    if !enable {
        return TlsConfig::default();
    }
    TlsConfig {
        enable: true,
        test_mode_enable: true,
        client: TlsClientConfig {
            auth_server: false,
            ..TlsClientConfig::default()
        },
        ..TlsConfig::default()
    }
}

fn data_request() -> RemotingCommand {
    RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_body(vec![b'N'; MESSAGE_SIZE_BYTES])
}

fn payload_bytes(operations: usize) -> Result<u64> {
    let bytes = operations
        .checked_mul(MESSAGE_SIZE_BYTES)
        .ok_or_else(|| anyhow!("logical payload bytes overflowed"))?;
    u64::try_from(bytes).context("logical payload bytes exceed u64")
}

fn counter_delta(after: u64, before: u64, counter: &str) -> Result<u64> {
    after
        .checked_sub(before)
        .ok_or_else(|| anyhow!("{counter} counter moved backwards"))
}

fn percentile_99(latencies: &[Duration]) -> Result<Duration> {
    ensure!(!latencies.is_empty(), "cannot compute p99 from an empty sample");
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let rank = (sorted.len() * 99).div_ceil(100).saturating_sub(1);
    Ok(sorted[rank])
}

#[cfg(windows)]
fn peak_rss_bytes() -> Result<u64> {
    use std::mem::size_of;

    use windows::Win32::System::ProcessStatus::GetProcessMemoryInfo;
    use windows::Win32::System::ProcessStatus::PROCESS_MEMORY_COUNTERS;
    use windows::Win32::System::Threading::GetCurrentProcess;

    let mut counters = PROCESS_MEMORY_COUNTERS::default();
    let structure_size =
        u32::try_from(size_of::<PROCESS_MEMORY_COUNTERS>()).context("PROCESS_MEMORY_COUNTERS size exceeds u32")?;
    counters.cb = structure_size;

    // SAFETY: GetCurrentProcess returns a valid pseudo-handle for this
    // process, counters points to writable storage of the declared size, and
    // the API does not retain the pointer after the call.
    unsafe {
        GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, structure_size)
            .context("query process peak working set")?;
    }
    u64::try_from(counters.PeakWorkingSetSize).context("peak working set exceeds u64")
}

#[cfg(unix)]
fn peak_rss_bytes() -> Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: usage points to valid writable storage for rusage, and
    // getrusage initializes it when returning zero.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    ensure!(
        status == 0,
        "getrusage failed with OS error {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: A zero return from getrusage guarantees the structure was
    // initialized.
    let usage = unsafe { usage.assume_init() };
    let peak = u64::try_from(usage.ru_maxrss).context("ru_maxrss was negative")?;

    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        peak.checked_mul(1024)
            .ok_or_else(|| anyhow!("ru_maxrss byte conversion overflowed"))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        Ok(peak)
    }
}

fn write_sample(sample: SampleObservation) -> Result<()> {
    serde_json::to_writer(std::io::stdout().lock(), &sample).context("write isolated sample JSON")
}

fn write_measurement(measurement: MeasurementEnvelope<'_>) -> Result<()> {
    serde_json::to_writer(std::io::stdout().lock(), &measurement).context("write measurement JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scaled_connection_spec() -> RunSpec {
        RunSpec {
            scenario: Scenario::parse(CONNECTION_SOAK_PROFILE, CONNECTION_SOAK_VARIANT).expect("connection scenario"),
            operation_count: 4,
            duration: Duration::from_millis(20),
            cooldown: Duration::from_millis(100),
            data_capacity: 0,
            control_reserve: 0,
            processor_delay: Duration::ZERO,
            request_timeout: Duration::from_secs(2),
            convergence_timeout: Duration::from_secs(1),
        }
    }

    fn scaled_overload_spec() -> RunSpec {
        RunSpec {
            scenario: Scenario::parse(OVERLOAD_PROFILE, OVERLOAD_VARIANT).expect("overload scenario"),
            operation_count: 0,
            duration: Duration::from_millis(100),
            cooldown: Duration::ZERO,
            data_capacity: 2,
            control_reserve: 1,
            processor_delay: Duration::from_millis(25),
            request_timeout: Duration::from_secs(2),
            convergence_timeout: Duration::from_secs(1),
        }
    }

    fn observation(scenario: Scenario, value: f64) -> SampleObservation {
        SampleObservation {
            throughput_per_second: value,
            p99_latency_us: value + 1.0,
            peak_rss_bytes: value + 2.0,
            allocations_per_operation: value + 3.0,
            io_amplification_ratio: value + 4.0,
            live_tasks_after_cooldown: scenario.includes_cooldown_metrics().then_some(0.0),
            pending_requests_after_cooldown: scenario.includes_cooldown_metrics().then_some(0.0),
            control_plane_success_ratio: scenario.includes_control_ratio().then_some(1.0),
            data_rejections: u64::from(scenario.workload == WorkloadKind::Overload),
            server_rejections: u64::from(scenario.workload == WorkloadKind::Overload),
            resources_converged: true,
            retained_child_groups: 0,
        }
    }

    #[test]
    fn parses_only_frozen_public_variants() {
        assert_eq!(
            parse_invocation(&[CONNECTION_SOAK_PROFILE.to_owned(), CONNECTION_SOAK_VARIANT.to_owned()])
                .expect("connection invocation"),
            Invocation::Collect(Scenario {
                profile: CONNECTION_SOAK_PROFILE,
                variant: CONNECTION_SOAK_VARIANT,
                workload: WorkloadKind::ConnectionSoak,
            })
        );
        assert_eq!(
            parse_invocation(&[
                "--sample".to_owned(),
                OVERLOAD_PROFILE.to_owned(),
                OVERLOAD_VARIANT.to_owned(),
            ])
            .expect("overload sample invocation"),
            Invocation::Sample(Scenario {
                profile: OVERLOAD_PROFILE,
                variant: OVERLOAD_VARIANT,
                workload: WorkloadKind::Overload,
            })
        );
        assert!(parse_invocation(&["connection-soak".to_owned(), "short".to_owned()]).is_err());
        assert!(parse_invocation(&["overload".to_owned(), "unbounded".to_owned()]).is_err());
        assert!(parse_invocation(&["connection-soak".to_owned()]).is_err());
    }

    #[test]
    fn production_specs_match_the_policy_exactly() {
        let connection = Scenario::parse(CONNECTION_SOAK_PROFILE, CONNECTION_SOAK_VARIANT)
            .expect("connection scenario")
            .production_spec();
        assert_eq!(connection.operation_count, 10_000);
        assert_eq!(connection.duration, Duration::from_secs(900));
        assert_eq!(connection.cooldown, Duration::from_secs(60));
        assert!(connection.operation_count.is_multiple_of(2));

        let overload = Scenario::parse(OVERLOAD_PROFILE, OVERLOAD_VARIANT)
            .expect("overload scenario")
            .production_spec();
        assert_eq!(overload.duration, Duration::from_secs(300));
        assert_eq!(overload.overload_concurrency().expect("offered concurrency"), 32);
        assert_eq!(
            overload.overload_concurrency().expect("offered concurrency") / overload.data_capacity,
            2
        );
    }

    #[test]
    fn envelopes_have_exact_profile_metric_inventory() {
        let connection =
            Scenario::parse(CONNECTION_SOAK_PROFILE, CONNECTION_SOAK_VARIANT).expect("connection scenario");
        let connection_envelope = build_envelope(
            connection,
            (1..=SAMPLE_COUNT)
                .map(|value| observation(connection, value as f64))
                .collect(),
        )
        .expect("connection envelope");
        assert_eq!(
            connection_envelope.metrics.keys().copied().collect::<Vec<_>>(),
            vec![
                "allocations_per_operation",
                "io_amplification_ratio",
                "live_tasks_after_cooldown",
                "p99_latency_us",
                "peak_rss_bytes",
                "pending_requests_after_cooldown",
                "throughput_per_second",
            ]
        );

        let overload = Scenario::parse(OVERLOAD_PROFILE, OVERLOAD_VARIANT).expect("overload scenario");
        let overload_envelope = build_envelope(
            overload,
            (1..=SAMPLE_COUNT)
                .map(|value| observation(overload, value as f64))
                .collect(),
        )
        .expect("overload envelope");
        assert_eq!(
            overload_envelope.metrics.keys().copied().collect::<Vec<_>>(),
            vec![
                "allocations_per_operation",
                "control_plane_success_ratio",
                "io_amplification_ratio",
                "p99_latency_us",
                "peak_rss_bytes",
                "throughput_per_second",
            ]
        );
    }

    #[test]
    fn invalid_samples_and_counter_regressions_fail_closed() {
        let connection =
            Scenario::parse(CONNECTION_SOAK_PROFILE, CONNECTION_SOAK_VARIANT).expect("connection scenario");
        let mut leaked = observation(connection, 1.0);
        leaked.live_tasks_after_cooldown = Some(1.0);
        assert!(leaked.validate(connection).is_err());
        let mut missing = observation(connection, 1.0);
        missing.pending_requests_after_cooldown = None;
        assert!(missing.validate(connection).is_err());

        let overload = Scenario::parse(OVERLOAD_PROFILE, OVERLOAD_VARIANT).expect("overload scenario");
        let mut no_rejection = observation(overload, 1.0);
        no_rejection.data_rejections = 0;
        assert!(no_rejection.validate(overload).is_err());
        let mut failed_control = observation(overload, 1.0);
        failed_control.control_plane_success_ratio = Some(0.99);
        assert!(failed_control.validate(overload).is_err());
        let mut non_finite = observation(overload, 1.0);
        non_finite.io_amplification_ratio = f64::NAN;
        assert!(non_finite.validate(overload).is_err());

        assert!(build_envelope(
            connection,
            (1..SAMPLE_COUNT)
                .map(|value| observation(connection, value as f64))
                .collect(),
        )
        .is_err());
        assert!(counter_delta(9, 10, "test").is_err());
    }

    #[test]
    fn computes_p99_and_rejects_empty_latency_sets() {
        let values = (1..=100).map(Duration::from_micros).collect::<Vec<_>>();
        assert_eq!(percentile_99(&values).expect("p99"), Duration::from_micros(99));
        assert!(percentile_99(&[]).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scaled_connection_soak_uses_real_plaintext_and_tls_and_converges() {
        let spec = scaled_connection_spec();
        spec.validate().expect("scaled connection spec");
        let observation = collect_connection_soak(spec).await.expect("scaled connection soak");
        observation.validate(spec.scenario).expect("connection observation");
        assert_eq!(observation.live_tasks_after_cooldown, Some(0.0));
        assert_eq!(observation.pending_requests_after_cooldown, Some(0.0));
        assert!(observation.io_amplification_ratio > 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scaled_overload_rejects_data_and_preserves_control_plane() {
        let spec = scaled_overload_spec();
        spec.validate().expect("scaled overload spec");
        let observation = collect_overload(spec).await.expect("scaled overload");
        observation.validate(spec.scenario).expect("overload observation");
        assert!(observation.data_rejections > 0);
        assert!(observation.server_rejections > 0);
        assert_eq!(observation.control_plane_success_ratio, Some(1.0));
        assert!(observation.resources_converged);
    }
}
