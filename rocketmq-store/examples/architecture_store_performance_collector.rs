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

//! Target-hardware collector for the M10 store performance profiles.
//!
//! Run a variant from the workspace root with:
//!
//! ```text
//! cargo run --release --quiet -p rocketmq-store \
//!   --example architecture_store_performance_collector -- local-append producers-1
//!
//! cargo run --release --quiet -p rocketmq-store \
//!   --example architecture_store_performance_collector -- sync-flush concurrency-64
//!
//! cargo run --release --quiet -p rocketmq-store \
//!   --example architecture_store_performance_collector -- local-pull batch-32
//! ```
//!
//! The command writes one sidecar-compatible JSON object to stdout. Each raw
//! sample is collected in a fresh child process so operating-system lifetime
//! peak RSS is independent between samples.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::collections::BTreeMap;
use std::env;
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
use cheetah_string::CheetahString;
use dashmap::DashMap;
use futures_util::future::join_all;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store_local::mapped_file::SelectMappedBufferCacheState;
use rocketmq_store_local::mapped_file::SelectMappedBufferSourceKind;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
use tokio::runtime::Builder;

const LOCAL_APPEND_PROFILE: &str = "local-append";
const LOCAL_PULL_PROFILE: &str = "local-pull";
const SYNC_FLUSH_PROFILE: &str = "sync-flush";
const MESSAGE_SIZE_BYTES: usize = 1024;
const SAMPLE_COUNT: usize = 5;
const PRIMING_SAMPLE_COUNT: usize = 2;
const LOCAL_APPEND_OPERATIONS_PER_SAMPLE: usize = 131_072;
const LOCAL_APPEND_WARMUP_OPERATIONS: usize = 32_768;
const LOCAL_PULL_BATCH_MESSAGES: usize = 32;
const LOCAL_PULL_OPERATIONS_PER_SAMPLE: usize = 4096;
const LOCAL_PULL_WARMUP_OPERATIONS: usize = 512;
const CQ_ALLOCATION_PROBE_REPETITIONS: usize = 128;
const SYNC_FLUSH_OPERATIONS_PER_SAMPLE: usize = 8192;
const SYNC_FLUSH_WARMUP_OPERATIONS: usize = 1024;

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

#[derive(Clone, Copy, Debug, PartialEq)]
enum WorkloadKind {
    Append,
    LocalPull,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Scenario {
    profile: &'static str,
    variant: &'static str,
    workload_kind: WorkloadKind,
    producers: usize,
    flush_disk_type: FlushDiskType,
    operations_per_sample: usize,
    warmup_operations: usize,
}

impl Scenario {
    fn parse(profile: &str, variant: &str) -> Result<Self> {
        match (profile, variant) {
            (LOCAL_APPEND_PROFILE, "producers-1") => Ok(Self {
                profile: LOCAL_APPEND_PROFILE,
                variant: "producers-1",
                workload_kind: WorkloadKind::Append,
                producers: 1,
                flush_disk_type: FlushDiskType::AsyncFlush,
                operations_per_sample: LOCAL_APPEND_OPERATIONS_PER_SAMPLE,
                warmup_operations: LOCAL_APPEND_WARMUP_OPERATIONS,
            }),
            (LOCAL_APPEND_PROFILE, "producers-8") => Ok(Self {
                profile: LOCAL_APPEND_PROFILE,
                variant: "producers-8",
                workload_kind: WorkloadKind::Append,
                producers: 8,
                flush_disk_type: FlushDiskType::AsyncFlush,
                operations_per_sample: LOCAL_APPEND_OPERATIONS_PER_SAMPLE,
                warmup_operations: LOCAL_APPEND_WARMUP_OPERATIONS,
            }),
            (LOCAL_APPEND_PROFILE, "producers-32") => Ok(Self {
                profile: LOCAL_APPEND_PROFILE,
                variant: "producers-32",
                workload_kind: WorkloadKind::Append,
                producers: 32,
                flush_disk_type: FlushDiskType::AsyncFlush,
                operations_per_sample: LOCAL_APPEND_OPERATIONS_PER_SAMPLE,
                warmup_operations: LOCAL_APPEND_WARMUP_OPERATIONS,
            }),
            (SYNC_FLUSH_PROFILE, "concurrency-64") => Ok(Self {
                profile: SYNC_FLUSH_PROFILE,
                variant: "concurrency-64",
                workload_kind: WorkloadKind::Append,
                producers: 64,
                flush_disk_type: FlushDiskType::SyncFlush,
                operations_per_sample: SYNC_FLUSH_OPERATIONS_PER_SAMPLE,
                warmup_operations: SYNC_FLUSH_WARMUP_OPERATIONS,
            }),
            (LOCAL_PULL_PROFILE, "batch-32") => Ok(Self {
                profile: LOCAL_PULL_PROFILE,
                variant: "batch-32",
                workload_kind: WorkloadKind::LocalPull,
                producers: 1,
                flush_disk_type: FlushDiskType::AsyncFlush,
                operations_per_sample: LOCAL_PULL_OPERATIONS_PER_SAMPLE,
                warmup_operations: LOCAL_PULL_WARMUP_OPERATIONS,
            }),
            _ => bail!("unsupported performance profile/variant: {profile}/{variant}"),
        }
    }

    fn requires_started_store(self) -> bool {
        self.flush_disk_type == FlushDiskType::SyncFlush
    }

    fn includes_fsync_per_ack(self) -> bool {
        self.profile == SYNC_FLUSH_PROFILE
    }

    fn includes_local_pull_observations(self) -> bool {
        self.workload_kind == WorkloadKind::LocalPull
    }

    fn payload_bytes_per_operation(self) -> usize {
        if self.includes_local_pull_observations() {
            LOCAL_PULL_BATCH_MESSAGES * MESSAGE_SIZE_BYTES
        } else {
            MESSAGE_SIZE_BYTES
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
            "usage: architecture_store_performance_collector [--sample] <local-append|local-pull|sync-flush> \
             <producers-1|producers-8|producers-32|batch-32|concurrency-64>"
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
    fsync_per_ack: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cq_unit_allocations_per_message: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_copies_per_message: Option<f64>,
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
                value.is_finite() && value >= 0.0,
                "{metric} must be finite and non-negative, got {value}"
            );
        }
        ensure!(
            self.fsync_per_ack.is_some() == scenario.includes_fsync_per_ack(),
            "{}/{} fsync_per_ack presence does not match the metric contract",
            scenario.profile,
            scenario.variant
        );
        if let Some(fsync_per_ack) = self.fsync_per_ack {
            ensure!(
                fsync_per_ack.is_finite() && fsync_per_ack >= 0.0,
                "fsync_per_ack must be finite and non-negative, got {fsync_per_ack}"
            );
        }
        for (metric, value) in [
            ("cq_unit_allocations_per_message", self.cq_unit_allocations_per_message),
            ("body_copies_per_message", self.body_copies_per_message),
        ] {
            ensure!(
                value.is_some() == scenario.includes_local_pull_observations(),
                "{}/{} {metric} presence does not match the metric contract",
                scenario.profile,
                scenario.variant
            );
            if let Some(value) = value {
                ensure!(
                    value.is_finite() && value >= 0.0,
                    "{metric} must be finite and non-negative, got {value}"
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

struct CollectorStore {
    store: LocalFileMessageStore,
    _temp_dir: TempDir,
}

struct WorkloadObservation {
    elapsed: Duration,
    latencies: Vec<Duration>,
    encoded_bytes: u64,
    body_copies: u64,
    returned_messages: u64,
}

#[derive(Clone, Copy)]
struct MessageLocation {
    offset: i64,
    size: i32,
}

struct LocalPullContext {
    group: CheetahString,
    topic: CheetahString,
    message_locations: Vec<MessageLocation>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("architecture store performance collector failed: {error:#}");
        process::exit(2);
    }
}

fn run() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match parse_invocation(&args)? {
        Invocation::Collect(scenario) => write_measurement(collect_samples(scenario)?)?,
        Invocation::Sample(scenario) => write_sample(collect_one_sample(scenario)?)?,
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
    if scenario.includes_fsync_per_ack() {
        let samples = observations
            .iter()
            .map(|sample| {
                sample
                    .fsync_per_ack
                    .ok_or_else(|| anyhow!("sync-flush sample omitted fsync_per_ack"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert("fsync_per_ack", MetricSamples { samples });
    }
    if scenario.includes_local_pull_observations() {
        let cq_unit_allocations = observations
            .iter()
            .map(|sample| {
                sample
                    .cq_unit_allocations_per_message
                    .ok_or_else(|| anyhow!("local-pull sample omitted cq_unit_allocations_per_message"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert(
            "cq_unit_allocations_per_message",
            MetricSamples {
                samples: cq_unit_allocations,
            },
        );
        let body_copies = observations
            .iter()
            .map(|sample| {
                sample
                    .body_copies_per_message
                    .ok_or_else(|| anyhow!("local-pull sample omitted body_copies_per_message"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert("body_copies_per_message", MetricSamples { samples: body_copies });
    }

    Ok(MeasurementEnvelope {
        schema_version: 1,
        profile: scenario.profile,
        variant: scenario.variant,
        metrics,
    })
}

fn collect_one_sample(scenario: Scenario) -> Result<SampleObservation> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create sample runtime")?;
    let mut collector_store = new_collector_store(scenario)?;
    let local_pull_context = if scenario.includes_local_pull_observations() {
        Some(
            runtime
                .block_on(seed_local_pull_store(&mut collector_store))
                .context("seed local-pull sample store")?,
        )
    } else {
        None
    };
    if scenario.requires_started_store() {
        runtime
            .block_on(start_collector_store(&mut collector_store))
            .context("start sync-flush sample store")?;
    }

    let warmup = match local_pull_context.as_ref() {
        Some(context) => runtime.block_on(run_local_pull_workload(
            &collector_store,
            context,
            scenario.warmup_operations,
            false,
        )),
        None => runtime.block_on(run_append_workload(
            &collector_store,
            scenario,
            scenario.warmup_operations,
            0,
        )),
    };
    warmup.context("run warm-up workload")?;

    let cq_unit_allocations_per_message = match local_pull_context.as_ref() {
        Some(context) => Some(
            runtime
                .block_on(measure_cq_unit_allocations_per_message(&collector_store, context))
                .context("measure local-pull CQ-unit allocations")?,
        ),
        None => None,
    };

    let flush_operations_before = collector_store
        .store
        .get_commit_log()
        .mapped_file_io_stats()
        .flush_operations;
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let workload = match local_pull_context.as_ref() {
        Some(context) => runtime.block_on(run_local_pull_workload(
            &collector_store,
            context,
            scenario.operations_per_sample,
            true,
        )),
        None => runtime.block_on(run_append_workload(
            &collector_store,
            scenario,
            scenario.operations_per_sample,
            scenario.warmup_operations as u64,
        )),
    }
    .context("run measured workload")?;
    let flush_operations_after = collector_store
        .store
        .get_commit_log()
        .mapped_file_io_stats()
        .flush_operations;
    let measured_flush_operations = flush_operations_after
        .checked_sub(flush_operations_before)
        .ok_or_else(|| anyhow!("mapped-file flush counter moved backwards"))?;
    let allocation_calls = ALLOCATION_CALLS
        .load(Ordering::Relaxed)
        .saturating_sub(allocations_before);
    if scenario.includes_fsync_per_ack() {
        ensure!(
            measured_flush_operations > 0,
            "sync-flush acknowledgements completed without a measured mapped-file flush"
        );
    }

    let observation = SampleObservation {
        throughput_per_second: scenario.operations_per_sample as f64 / workload.elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&workload.latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocation_calls as f64 / scenario.operations_per_sample as f64,
        io_amplification_ratio: workload.encoded_bytes as f64
            / (scenario.operations_per_sample * scenario.payload_bytes_per_operation()) as f64,
        fsync_per_ack: scenario
            .includes_fsync_per_ack()
            .then_some(measured_flush_operations as f64 / scenario.operations_per_sample as f64),
        cq_unit_allocations_per_message,
        body_copies_per_message: scenario
            .includes_local_pull_observations()
            .then(|| workload.body_copies as f64 / workload.returned_messages as f64),
    };
    observation.validate(scenario)?;
    if scenario.requires_started_store() {
        runtime
            .block_on(collector_store.store.shutdown_gracefully())
            .context("shut down sync-flush sample store")?;
    }
    Ok(observation)
}

fn new_collector_store(scenario: Scenario) -> Result<CollectorStore> {
    let temp_dir = TempDir::new().context("create store performance sample directory")?;
    let mut message_store_config = MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        flush_disk_type: scenario.flush_disk_type,
        ha_listen_port: 0,
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    };
    if scenario.includes_local_pull_observations() {
        message_store_config.mapped_file_size_commit_log = 4 * 1024 * 1024;
        message_store_config.mapped_file_size_consume_queue = 20 * 1024;
    } else {
        message_store_config.mapped_file_size_commit_log = 256 * 1024 * 1024;
    }

    let mut store = LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
    );
    store
        .wire_owned_root_dependencies()
        .context("wire owned store performance sample dependencies")?;

    Ok(CollectorStore {
        store,
        _temp_dir: temp_dir,
    })
}

async fn start_collector_store(collector_store: &mut CollectorStore) -> Result<()> {
    collector_store.store.init().await.context("initialize sample store")?;
    ensure!(collector_store.store.load().await, "load sample store");
    collector_store.store.start().await.context("launch sample store")
}

async fn run_append_workload(
    collector_store: &CollectorStore,
    scenario: Scenario,
    operation_count: usize,
    key_seed: u64,
) -> Result<WorkloadObservation> {
    ensure!(
        scenario.workload_kind == WorkloadKind::Append,
        "append workload received non-append scenario {}/{}",
        scenario.profile,
        scenario.variant
    );
    ensure!(
        operation_count.is_multiple_of(scenario.producers),
        "operation count {operation_count} must be divisible by producer count {}",
        scenario.producers
    );

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operation_count);
    let mut encoded_bytes = 0_u64;
    let mut next_key = key_seed;
    let topic = CheetahString::from_static_str("ArchitectureLocalAppend");

    for _ in 0..operation_count / scenario.producers {
        let commit_log = collector_store.store.get_commit_log();
        let mut puts = Vec::with_capacity(scenario.producers);
        for producer in 0..scenario.producers {
            let message = create_message(&topic, producer as i32, next_key);
            next_key += 1;
            puts.push(async move {
                let put_started = Instant::now();
                let result = commit_log.put_message(message).await;
                (result, put_started.elapsed())
            });
        }

        for (result, latency) in join_all(puts).await {
            ensure!(
                result.put_message_status() == PutMessageStatus::PutOk,
                "CommitLog append returned {:?}",
                result.put_message_status()
            );
            let append = result
                .append_message_result()
                .ok_or_else(|| anyhow!("successful CommitLog append omitted its encoded result"))?;
            let wrote_bytes =
                u64::try_from(append.wrote_bytes).context("successful CommitLog append reported negative bytes")?;
            encoded_bytes = encoded_bytes
                .checked_add(wrote_bytes)
                .ok_or_else(|| anyhow!("encoded byte counter overflowed"))?;
            latencies.push(latency);
        }
    }

    Ok(WorkloadObservation {
        elapsed: started.elapsed(),
        latencies,
        encoded_bytes,
        body_copies: 0,
        returned_messages: operation_count as u64,
    })
}

async fn seed_local_pull_store(collector_store: &mut CollectorStore) -> Result<LocalPullContext> {
    let topic = CheetahString::from_static_str("ArchitectureLocalPull");
    let group = CheetahString::from_static_str("ArchitectureGate");

    for key_seed in 0..LOCAL_PULL_BATCH_MESSAGES as u64 {
        let result = collector_store
            .store
            .put_message(create_message(&topic, 0, key_seed))
            .await;
        ensure!(
            result.put_message_status() == PutMessageStatus::PutOk,
            "local-pull seed append returned {:?}",
            result.put_message_status()
        );
    }
    collector_store.store.reput_once().await;

    let result = pull_local_batch(collector_store, &group, &topic, LOCAL_PULL_BATCH_MESSAGES, false)
        .await
        .context("read seeded local-pull batch")?;
    let message_locations = result
        .message_mapped_list()
        .iter()
        .map(|selection| {
            Ok(MessageLocation {
                offset: i64::try_from(selection.start_offset).context("local-pull message offset exceeds i64")?,
                size: selection.size,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(LocalPullContext {
        group,
        topic,
        message_locations,
    })
}

async fn run_local_pull_workload(
    collector_store: &CollectorStore,
    context: &LocalPullContext,
    operation_count: usize,
    require_hot: bool,
) -> Result<WorkloadObservation> {
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operation_count);
    let mut encoded_bytes = 0_u64;
    let mut body_copies = 0_u64;
    let mut returned_messages = 0_u64;

    for _ in 0..operation_count {
        let pull_started = Instant::now();
        let result = pull_local_batch(
            collector_store,
            &context.group,
            &context.topic,
            LOCAL_PULL_BATCH_MESSAGES,
            require_hot,
        )
        .await?;
        latencies.push(pull_started.elapsed());
        encoded_bytes = encoded_bytes
            .checked_add(u64::try_from(result.buffer_total_size()).context("pull returned negative encoded bytes")?)
            .ok_or_else(|| anyhow!("local-pull encoded byte counter overflowed"))?;
        body_copies = body_copies
            .checked_add(
                result
                    .message_mapped_list()
                    .iter()
                    .filter(|selection| selection.source_kind != SelectMappedBufferSourceKind::MappedFile)
                    .count() as u64,
            )
            .ok_or_else(|| anyhow!("local-pull body-copy counter overflowed"))?;
        returned_messages = returned_messages
            .checked_add(u64::try_from(result.message_count()).context("pull returned a negative message count")?)
            .ok_or_else(|| anyhow!("local-pull message counter overflowed"))?;
    }

    ensure!(returned_messages > 0, "local-pull workload returned no messages");
    Ok(WorkloadObservation {
        elapsed: started.elapsed(),
        latencies,
        encoded_bytes,
        body_copies,
        returned_messages,
    })
}

async fn pull_local_batch(
    collector_store: &CollectorStore,
    group: &CheetahString,
    topic: &CheetahString,
    batch_messages: usize,
    require_hot: bool,
) -> Result<GetMessageResult> {
    let batch_messages_i32 = i32::try_from(batch_messages).context("local-pull batch size exceeds i32")?;
    let result = collector_store
        .store
        .get_message(group, topic, 0, 0, batch_messages_i32, None)
        .await
        .ok_or_else(|| anyhow!("local-pull store omitted its GetMessageResult"))?;
    ensure!(
        result.status() == Some(GetMessageStatus::Found),
        "local-pull returned status {:?}",
        result.status()
    );
    ensure!(
        result.message_count() == batch_messages_i32,
        "local-pull returned {} messages, expected {batch_messages}",
        result.message_count()
    );
    ensure!(
        result.message_mapped_list().len() == batch_messages,
        "local-pull returned {} buffers, expected {batch_messages}",
        result.message_mapped_list().len()
    );
    if require_hot {
        ensure!(
            result
                .message_mapped_list()
                .iter()
                .all(|selection| selection.cache_state == SelectMappedBufferCacheState::Hot),
            "local-pull batch included a non-hot mapped buffer"
        );
    }
    Ok(result)
}

async fn measure_cq_unit_allocations_per_message(
    collector_store: &CollectorStore,
    context: &LocalPullContext,
) -> Result<f64> {
    ensure!(
        context.message_locations.len() == LOCAL_PULL_BATCH_MESSAGES,
        "local-pull direct control expected {LOCAL_PULL_BATCH_MESSAGES} message locations, got {}",
        context.message_locations.len()
    );

    let full_single = measure_full_pull_allocation_calls(collector_store, context, 1).await?;
    let full_batch = measure_full_pull_allocation_calls(collector_store, context, LOCAL_PULL_BATCH_MESSAGES).await?;
    let direct_single = measure_direct_commit_log_allocation_calls(collector_store, context, 1)?;
    let direct_batch = measure_direct_commit_log_allocation_calls(collector_store, context, LOCAL_PULL_BATCH_MESSAGES)?;

    matched_cq_allocation_rate(
        full_single,
        full_batch,
        direct_single,
        direct_batch,
        CQ_ALLOCATION_PROBE_REPETITIONS,
        LOCAL_PULL_BATCH_MESSAGES,
    )
}

async fn measure_full_pull_allocation_calls(
    collector_store: &CollectorStore,
    context: &LocalPullContext,
    batch_messages: usize,
) -> Result<u64> {
    let before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    for _ in 0..CQ_ALLOCATION_PROBE_REPETITIONS {
        drop(pull_local_batch(collector_store, &context.group, &context.topic, batch_messages, true).await?);
    }
    allocation_delta(before, "full local-pull allocation probe")
}

fn measure_direct_commit_log_allocation_calls(
    collector_store: &CollectorStore,
    context: &LocalPullContext,
    batch_messages: usize,
) -> Result<u64> {
    let before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    for _ in 0..CQ_ALLOCATION_PROBE_REPETITIONS {
        for location in context.message_locations.iter().take(batch_messages) {
            let selection = collector_store
                .store
                .get_commit_log()
                .get_message(location.offset, location.size)
                .ok_or_else(|| anyhow!("direct CommitLog control omitted a seeded message"))?;
            ensure!(
                selection.cache_state == SelectMappedBufferCacheState::Hot,
                "direct CommitLog control included a non-hot mapped buffer"
            );
        }
    }
    allocation_delta(before, "direct CommitLog allocation probe")
}

fn allocation_delta(before: u64, probe: &str) -> Result<u64> {
    ALLOCATION_CALLS
        .load(Ordering::Relaxed)
        .checked_sub(before)
        .ok_or_else(|| anyhow!("{probe} counter moved backwards"))
}

fn matched_cq_allocation_rate(
    full_single: u64,
    full_batch: u64,
    direct_single: u64,
    direct_batch: u64,
    repetitions: usize,
    batch_messages: usize,
) -> Result<f64> {
    ensure!(repetitions > 0, "CQ allocation probe repetitions must be positive");
    ensure!(
        batch_messages > 1,
        "CQ allocation probe batch must contain at least two messages"
    );
    let full_increment = full_batch
        .checked_sub(full_single)
        .ok_or_else(|| anyhow!("full local-pull allocation count decreased with the batch size"))?;
    let direct_increment = direct_batch
        .checked_sub(direct_single)
        .ok_or_else(|| anyhow!("direct CommitLog allocation count decreased with the batch size"))?;
    let cq_increment = full_increment.checked_sub(direct_increment).ok_or_else(|| {
        anyhow!("matched direct CommitLog control exceeded the full local-pull incremental allocation count")
    })?;
    let denominator = repetitions
        .checked_mul(batch_messages - 1)
        .ok_or_else(|| anyhow!("CQ allocation probe denominator overflowed"))?;
    Ok(cq_increment as f64 / denominator as f64)
}

fn create_message(topic: &CheetahString, queue_id: i32, key_seed: u64) -> MessageExtBrokerInner {
    let mut message = Message::builder()
        .topic(topic.clone())
        .body(vec![b'X'; MESSAGE_SIZE_BYTES])
        .build_unchecked();
    message.set_tags(CheetahString::from_static_str("ArchitectureGate"));
    message.set_keys(CheetahString::from(format!("architecture-gate-{key_seed}")));

    let mut inner = MessageExtBrokerInner {
        message_ext_inner: MessageExt {
            message,
            ..Default::default()
        },
        ..Default::default()
    };
    inner.message_ext_inner.set_queue_id(queue_id);
    inner
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

    fn sample(
        value: f64,
        fsync_per_ack: Option<f64>,
        cq_unit_allocations_per_message: Option<f64>,
        body_copies_per_message: Option<f64>,
    ) -> SampleObservation {
        SampleObservation {
            throughput_per_second: value,
            p99_latency_us: value + 1.0,
            peak_rss_bytes: value + 2.0,
            allocations_per_operation: value + 3.0,
            io_amplification_ratio: value + 4.0,
            fsync_per_ack,
            cq_unit_allocations_per_message,
            body_copies_per_message,
        }
    }

    #[test]
    fn parses_every_supported_variant() {
        for (variant, producers) in [("producers-1", 1), ("producers-8", 8), ("producers-32", 32)] {
            let Invocation::Collect(scenario) =
                parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), variant.to_owned()])
                    .expect("supported local append variant")
            else {
                panic!("expected collection invocation");
            };
            assert_eq!(scenario.profile, LOCAL_APPEND_PROFILE);
            assert_eq!(scenario.variant, variant);
            assert_eq!(scenario.producers, producers);
            assert_eq!(scenario.flush_disk_type, FlushDiskType::AsyncFlush);
        }

        let Invocation::Collect(sync_flush) =
            parse_invocation(&[SYNC_FLUSH_PROFILE.to_owned(), "concurrency-64".to_owned()])
                .expect("supported sync flush variant")
        else {
            panic!("expected collection invocation");
        };
        assert_eq!(sync_flush.profile, SYNC_FLUSH_PROFILE);
        assert_eq!(sync_flush.variant, "concurrency-64");
        assert_eq!(sync_flush.producers, 64);
        assert_eq!(sync_flush.flush_disk_type, FlushDiskType::SyncFlush);

        let Invocation::Collect(local_pull) = parse_invocation(&[LOCAL_PULL_PROFILE.to_owned(), "batch-32".to_owned()])
            .expect("supported local pull variant")
        else {
            panic!("expected collection invocation");
        };
        assert_eq!(local_pull.profile, LOCAL_PULL_PROFILE);
        assert_eq!(local_pull.variant, "batch-32");
        assert_eq!(local_pull.workload_kind, WorkloadKind::LocalPull);
        assert_eq!(
            local_pull.payload_bytes_per_operation(),
            LOCAL_PULL_BATCH_MESSAGES * MESSAGE_SIZE_BYTES
        );
    }

    #[test]
    fn rejects_unknown_profile_variant_and_shape() {
        assert!(parse_invocation(&["other".to_owned(), "producers-1".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), "producers-2".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), "concurrency-64".to_owned()]).is_err());
        assert!(parse_invocation(&[SYNC_FLUSH_PROFILE.to_owned(), "producers-32".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_PULL_PROFILE.to_owned(), "producers-32".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), "batch-32".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned()]).is_err());
        assert!(parse_invocation(&[
            "--sample".to_owned(),
            LOCAL_APPEND_PROFILE.to_owned(),
            "producers-8".to_owned(),
            "extra".to_owned(),
        ])
        .is_err());
    }

    #[test]
    fn builds_exact_sidecar_metric_inventory_with_five_samples() {
        let observations = (0..SAMPLE_COUNT)
            .map(|index| sample(index as f64, None, None, None))
            .collect();
        let scenario = Scenario::parse(LOCAL_APPEND_PROFILE, "producers-8").expect("local append scenario");
        let envelope = build_envelope(scenario, observations).expect("build measurement");
        let value = serde_json::to_value(envelope).expect("serialize measurement");

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["profile"], LOCAL_APPEND_PROFILE);
        assert_eq!(value["variant"], "producers-8");
        let metrics = value["metrics"].as_object().expect("metrics object");
        assert_eq!(
            metrics.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "allocations_per_operation",
                "io_amplification_ratio",
                "p99_latency_us",
                "peak_rss_bytes",
                "throughput_per_second",
            ]
        );
        assert!(metrics.values().all(|metric| metric["samples"]
            .as_array()
            .is_some_and(|samples| samples.len() == SAMPLE_COUNT)));
    }

    #[test]
    fn sync_flush_inventory_includes_fsync_per_ack() {
        let observations = (0..SAMPLE_COUNT)
            .map(|index| sample(index as f64, Some(1.0 / 64.0), None, None))
            .collect();
        let scenario = Scenario::parse(SYNC_FLUSH_PROFILE, "concurrency-64").expect("sync flush scenario");
        let value = serde_json::to_value(build_envelope(scenario, observations).expect("build measurement"))
            .expect("serialize measurement");

        assert_eq!(value["profile"], SYNC_FLUSH_PROFILE);
        assert_eq!(value["variant"], "concurrency-64");
        assert_eq!(
            value["metrics"]
                .as_object()
                .expect("metrics object")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec![
                "allocations_per_operation",
                "fsync_per_ack",
                "io_amplification_ratio",
                "p99_latency_us",
                "peak_rss_bytes",
                "throughput_per_second",
            ]
        );
    }

    #[test]
    fn local_pull_inventory_includes_exact_profile_observations() {
        let observations = (0..SAMPLE_COUNT)
            .map(|index| sample(index as f64, None, Some(0.0), Some(0.0)))
            .collect();
        let scenario = Scenario::parse(LOCAL_PULL_PROFILE, "batch-32").expect("local pull scenario");
        let value = serde_json::to_value(build_envelope(scenario, observations).expect("build measurement"))
            .expect("serialize measurement");

        assert_eq!(value["profile"], LOCAL_PULL_PROFILE);
        assert_eq!(value["variant"], "batch-32");
        assert_eq!(
            value["metrics"]
                .as_object()
                .expect("metrics object")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec![
                "allocations_per_operation",
                "body_copies_per_message",
                "cq_unit_allocations_per_message",
                "io_amplification_ratio",
                "p99_latency_us",
                "peak_rss_bytes",
                "throughput_per_second",
            ]
        );
    }

    #[test]
    fn rejects_partial_and_non_finite_samples() {
        let local_append = Scenario::parse(LOCAL_APPEND_PROFILE, "producers-1").expect("local append scenario");
        let partial = vec![sample(1.0, None, None, None), sample(2.0, None, None, None)];
        assert!(build_envelope(local_append, partial).is_err());

        let invalid = SampleObservation {
            throughput_per_second: f64::NAN,
            ..sample(1.0, None, None, None)
        };
        assert!(invalid.validate(local_append).is_err());

        let sync_flush = Scenario::parse(SYNC_FLUSH_PROFILE, "concurrency-64").expect("sync flush scenario");
        assert!(sample(1.0, None, None, None).validate(sync_flush).is_err());
        assert!(sample(1.0, Some(f64::INFINITY), None, None)
            .validate(sync_flush)
            .is_err());
        assert!(sample(1.0, Some(1.0 / 64.0), None, None)
            .validate(local_append)
            .is_err());

        let local_pull = Scenario::parse(LOCAL_PULL_PROFILE, "batch-32").expect("local pull scenario");
        assert!(sample(1.0, None, None, None).validate(local_pull).is_err());
        assert!(sample(1.0, None, Some(0.0), None).validate(local_pull).is_err());
        assert!(sample(1.0, None, Some(f64::INFINITY), Some(0.0))
            .validate(local_pull)
            .is_err());
        assert!(sample(1.0, None, Some(0.0), Some(0.0)).validate(local_append).is_err());
    }

    #[test]
    fn computes_nearest_rank_p99_and_rejects_empty_input() {
        let latencies = (1..=100).map(Duration::from_micros).collect::<Vec<_>>();
        assert_eq!(
            percentile_99(&latencies).expect("non-empty percentile"),
            Duration::from_micros(99)
        );
        assert!(percentile_99(&[]).is_err());
    }

    #[test]
    fn computes_matched_cq_allocation_rate_and_rejects_invalid_controls() {
        assert_eq!(
            matched_cq_allocation_rate(100, 348, 50, 298, 8, 32).expect("matched zero-allocation control"),
            0.0
        );
        assert_eq!(
            matched_cq_allocation_rate(100, 596, 50, 298, 8, 32).expect("matched one-allocation control"),
            1.0
        );
        assert!(matched_cq_allocation_rate(200, 100, 50, 60, 8, 32).is_err());
        assert!(matched_cq_allocation_rate(100, 200, 50, 300, 8, 32).is_err());
        assert!(matched_cq_allocation_rate(100, 200, 50, 100, 0, 32).is_err());
        assert!(matched_cq_allocation_rate(100, 200, 50, 100, 8, 1).is_err());
    }
}
