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
//!
//! cargo run --release --quiet -p rocketmq-store --features rocksdb_store \
//!   --example architecture_store_performance_collector -- rocks-pull batch-32
//!
//! cargo run --release --quiet -p rocketmq-store --features tieredstore \
//!   --example architecture_store_performance_collector -- tiered-append batch-64
//!
//! cargo run --release --quiet -p rocketmq-store --features tieredstore \
//!   --example architecture_store_performance_collector -- tiered-pull cold-32
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
#[cfg(feature = "tieredstore")]
use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use futures_util::future::join_all;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
#[cfg(feature = "rocksdb_store")]
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
#[cfg(feature = "rocksdb_store")]
use rocketmq_store::message_store::rocksdb_message_store::RocksDBMessageStore;
use rocketmq_store_local::mapped_file::SelectMappedBufferCacheState;
use rocketmq_store_local::mapped_file::SelectMappedBufferSourceKind;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::file::ConsumeQueueUnit;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::DefaultTieredMessageFetcher;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::JsonMetadataStore;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::PosixProvider;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::PosixProviderIoSnapshot;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredFlatFileStore;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredMessageFetcher;
#[cfg(feature = "tieredstore")]
use rocketmq_tieredstore::TieredStoreConfig;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
use tokio::runtime::Builder;

const LOCAL_APPEND_PROFILE: &str = "local-append";
const LOCAL_PULL_PROFILE: &str = "local-pull";
const ROCKS_PULL_PROFILE: &str = "rocks-pull";
const SYNC_FLUSH_PROFILE: &str = "sync-flush";
const TIERED_APPEND_PROFILE: &str = "tiered-append";
const TIERED_PULL_PROFILE: &str = "tiered-pull";
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
const TIERED_APPEND_BATCH_MESSAGES: usize = 64;
#[cfg(feature = "tieredstore")]
const TIERED_APPEND_OPERATIONS_PER_SAMPLE: usize = 64;
#[cfg(feature = "tieredstore")]
const TIERED_APPEND_WARMUP_OPERATIONS: usize = 8;
#[cfg(feature = "tieredstore")]
const TIERED_PULL_OPERATIONS_PER_SAMPLE: usize = 4096;

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
    RocksPull,
    #[cfg(feature = "tieredstore")]
    TieredAppend,
    #[cfg(feature = "tieredstore")]
    TieredPull,
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
            (ROCKS_PULL_PROFILE, "batch-32") => {
                #[cfg(feature = "rocksdb_store")]
                {
                    Ok(Self {
                        profile: ROCKS_PULL_PROFILE,
                        variant: "batch-32",
                        workload_kind: WorkloadKind::RocksPull,
                        producers: 1,
                        flush_disk_type: FlushDiskType::AsyncFlush,
                        operations_per_sample: LOCAL_PULL_OPERATIONS_PER_SAMPLE,
                        warmup_operations: LOCAL_PULL_WARMUP_OPERATIONS,
                    })
                }
                #[cfg(not(feature = "rocksdb_store"))]
                {
                    bail!("{ROCKS_PULL_PROFILE}/batch-32 requires the rocketmq-store rocksdb_store feature")
                }
            }
            (TIERED_APPEND_PROFILE, "batch-64") => {
                #[cfg(feature = "tieredstore")]
                {
                    Ok(Self {
                        profile: TIERED_APPEND_PROFILE,
                        variant: "batch-64",
                        workload_kind: WorkloadKind::TieredAppend,
                        producers: 1,
                        flush_disk_type: FlushDiskType::AsyncFlush,
                        operations_per_sample: TIERED_APPEND_OPERATIONS_PER_SAMPLE,
                        warmup_operations: TIERED_APPEND_WARMUP_OPERATIONS,
                    })
                }
                #[cfg(not(feature = "tieredstore"))]
                {
                    bail!("{TIERED_APPEND_PROFILE}/batch-64 requires the rocketmq-store tieredstore feature")
                }
            }
            (TIERED_PULL_PROFILE, variant @ ("cold-32" | "warm-32")) => {
                #[cfg(feature = "tieredstore")]
                {
                    Ok(Self {
                        profile: TIERED_PULL_PROFILE,
                        variant: if variant == "cold-32" { "cold-32" } else { "warm-32" },
                        workload_kind: WorkloadKind::TieredPull,
                        producers: 1,
                        flush_disk_type: FlushDiskType::AsyncFlush,
                        operations_per_sample: if variant == "cold-32" {
                            1
                        } else {
                            TIERED_PULL_OPERATIONS_PER_SAMPLE
                        },
                        warmup_operations: usize::from(variant == "warm-32"),
                    })
                }
                #[cfg(not(feature = "tieredstore"))]
                {
                    let _ = variant;
                    bail!("{TIERED_PULL_PROFILE}/cold-32 and warm-32 require the rocketmq-store tieredstore feature")
                }
            }
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

    fn includes_rocks_pull_observations(self) -> bool {
        self.workload_kind == WorkloadKind::RocksPull
    }

    fn includes_tiered_append_observations(self) -> bool {
        #[cfg(feature = "tieredstore")]
        {
            self.workload_kind == WorkloadKind::TieredAppend
        }
        #[cfg(not(feature = "tieredstore"))]
        {
            false
        }
    }

    fn includes_tiered_pull_observations(self) -> bool {
        #[cfg(feature = "tieredstore")]
        {
            self.workload_kind == WorkloadKind::TieredPull
        }
        #[cfg(not(feature = "tieredstore"))]
        {
            false
        }
    }

    fn is_tiered(self) -> bool {
        self.includes_tiered_append_observations() || self.includes_tiered_pull_observations()
    }

    fn is_pull(self) -> bool {
        matches!(self.workload_kind, WorkloadKind::LocalPull | WorkloadKind::RocksPull)
            || self.includes_tiered_pull_observations()
    }

    fn payload_bytes_per_operation(self) -> usize {
        if self.includes_tiered_append_observations() {
            TIERED_APPEND_BATCH_MESSAGES * MESSAGE_SIZE_BYTES
        } else if self.is_pull() {
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
            "usage: architecture_store_performance_collector [--sample] \
             <local-append|local-pull|rocks-pull|sync-flush|tiered-append|tiered-pull> \
             <producers-1|producers-8|producers-32|batch-32|concurrency-64|batch-64|cold-32|warm-32>"
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
    #[serde(skip_serializing_if = "Option::is_none")]
    native_read_calls_per_batch: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_writes_per_batch: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_commits_per_batch: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_reads_per_batch: Option<f64>,
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
        ensure!(
            self.native_read_calls_per_batch.is_some() == scenario.includes_rocks_pull_observations(),
            "{}/{} native_read_calls_per_batch presence does not match the metric contract",
            scenario.profile,
            scenario.variant
        );
        if let Some(value) = self.native_read_calls_per_batch {
            ensure!(
                value.is_finite() && value >= 0.0,
                "native_read_calls_per_batch must be finite and non-negative, got {value}"
            );
        }
        for (metric, value) in [
            ("provider_writes_per_batch", self.provider_writes_per_batch),
            ("metadata_commits_per_batch", self.metadata_commits_per_batch),
        ] {
            ensure!(
                value.is_some() == scenario.includes_tiered_append_observations(),
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
        ensure!(
            self.provider_reads_per_batch.is_some() == scenario.includes_tiered_pull_observations(),
            "{}/{} provider_reads_per_batch presence does not match the metric contract",
            scenario.profile,
            scenario.variant
        );
        if let Some(value) = self.provider_reads_per_batch {
            ensure!(
                value.is_finite() && value >= 0.0,
                "provider_reads_per_batch must be finite and non-negative, got {value}"
            );
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

enum CollectorBackend {
    Local(Box<LocalFileMessageStore>),
    #[cfg(feature = "rocksdb_store")]
    Rocks(Box<RocksDBMessageStore>),
}

struct CollectorStore {
    backend: CollectorBackend,
    _temp_dir: TempDir,
}

impl CollectorStore {
    fn local_file_store(&self) -> &LocalFileMessageStore {
        match &self.backend {
            CollectorBackend::Local(store) => store,
            #[cfg(feature = "rocksdb_store")]
            CollectorBackend::Rocks(store) => store.local_file_store(),
        }
    }

    fn local_file_store_mut(&mut self) -> &mut LocalFileMessageStore {
        match &mut self.backend {
            CollectorBackend::Local(store) => store,
            #[cfg(feature = "rocksdb_store")]
            CollectorBackend::Rocks(store) => store.local_file_store_mut(),
        }
    }

    async fn put_message(&mut self, message: MessageExtBrokerInner) -> PutMessageResult {
        match &mut self.backend {
            CollectorBackend::Local(store) => store.put_message(message).await,
            #[cfg(feature = "rocksdb_store")]
            CollectorBackend::Rocks(store) => store.put_message(message).await,
        }
    }

    async fn reput_once(&mut self) {
        self.local_file_store_mut().reput_once().await;
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        batch_messages: i32,
    ) -> Option<GetMessageResult> {
        match &self.backend {
            CollectorBackend::Local(store) => store.get_message(group, topic, 0, 0, batch_messages, None).await,
            #[cfg(feature = "rocksdb_store")]
            CollectorBackend::Rocks(store) => store.get_message(group, topic, 0, 0, batch_messages, None).await,
        }
    }

    fn rocks_read_counters(&self) -> Option<RocksReadCounters> {
        match &self.backend {
            CollectorBackend::Local(_) => None,
            #[cfg(feature = "rocksdb_store")]
            CollectorBackend::Rocks(store) => {
                let rocksdb = store.rocksdb_store();
                let operations = rocksdb.metrics();
                let ticker = rocksdb.ticker_metrics();
                Some(RocksReadCounters {
                    point_reads: operations.read_count,
                    range_scans: operations.scan_count,
                    bytes_read: ticker.bytes_read,
                    block_cache_misses: ticker.block_cache_miss,
                })
            }
        }
    }

    fn close_rocksdb(&self) {
        #[cfg(feature = "rocksdb_store")]
        if let CollectorBackend::Rocks(store) = &self.backend {
            store.close_rocksdb();
        }
    }
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

struct PullContext {
    group: CheetahString,
    topic: CheetahString,
    message_locations: Vec<MessageLocation>,
}

#[derive(Clone, Copy, Default)]
struct RocksReadCounters {
    point_reads: u64,
    range_scans: u64,
    bytes_read: u64,
    block_cache_misses: u64,
}

#[derive(Clone, Copy)]
struct RocksReadObservation {
    native_read_calls_per_batch: f64,
    bytes_read: u64,
}

#[cfg(feature = "tieredstore")]
struct TieredCollector {
    provider: PosixProvider,
    metadata_store: Arc<JsonMetadataStore>,
    flat_file_store: Arc<TieredFlatFileStore<PosixProvider>>,
    fetcher: DefaultTieredMessageFetcher<PosixProvider>,
    _temp_dir: TempDir,
}

#[cfg(feature = "tieredstore")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TieredIoDelta {
    read_operations: u64,
    write_operations: u64,
    bytes_read: u64,
    bytes_written: u64,
}

#[cfg(feature = "tieredstore")]
struct TimedTieredWorkload {
    elapsed: Duration,
    latencies: Vec<Duration>,
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
    if scenario.includes_rocks_pull_observations() {
        let samples = observations
            .iter()
            .map(|sample| {
                sample
                    .native_read_calls_per_batch
                    .ok_or_else(|| anyhow!("rocks-pull sample omitted native_read_calls_per_batch"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert("native_read_calls_per_batch", MetricSamples { samples });
    }
    if scenario.includes_tiered_append_observations() {
        let provider_writes = observations
            .iter()
            .map(|sample| {
                sample
                    .provider_writes_per_batch
                    .ok_or_else(|| anyhow!("tiered-append sample omitted provider_writes_per_batch"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert(
            "provider_writes_per_batch",
            MetricSamples {
                samples: provider_writes,
            },
        );
        let metadata_commits = observations
            .iter()
            .map(|sample| {
                sample
                    .metadata_commits_per_batch
                    .ok_or_else(|| anyhow!("tiered-append sample omitted metadata_commits_per_batch"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert(
            "metadata_commits_per_batch",
            MetricSamples {
                samples: metadata_commits,
            },
        );
    }
    if scenario.includes_tiered_pull_observations() {
        let samples = observations
            .iter()
            .map(|sample| {
                sample
                    .provider_reads_per_batch
                    .ok_or_else(|| anyhow!("tiered-pull sample omitted provider_reads_per_batch"))
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.insert("provider_reads_per_batch", MetricSamples { samples });
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
    #[cfg(feature = "tieredstore")]
    if scenario.is_tiered() {
        return runtime
            .block_on(collect_one_tiered_sample(scenario))
            .with_context(|| format!("collect {}/{} TieredStore sample", scenario.profile, scenario.variant));
    }
    ensure!(
        !scenario.is_tiered(),
        "TieredStore scenario requires the tieredstore feature"
    );
    let mut collector_store = new_collector_store(scenario)?;
    let pull_context = if scenario.is_pull() {
        Some(
            runtime
                .block_on(seed_pull_store(&mut collector_store, scenario))
                .with_context(|| format!("seed {}/{} sample store", scenario.profile, scenario.variant))?,
        )
    } else {
        None
    };
    if scenario.requires_started_store() {
        runtime
            .block_on(start_collector_store(&mut collector_store))
            .context("start sync-flush sample store")?;
    }

    let warmup = match pull_context.as_ref() {
        Some(context) => runtime.block_on(run_pull_workload(
            &collector_store,
            context,
            scenario,
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

    let cq_unit_allocations_per_message = match (scenario.includes_local_pull_observations(), pull_context.as_ref()) {
        (true, Some(context)) => Some(
            runtime
                .block_on(measure_cq_unit_allocations_per_message(&collector_store, context))
                .context("measure local-pull CQ-unit allocations")?,
        ),
        (false, _) => None,
        (true, None) => bail!("local-pull scenario omitted its seeded pull context"),
    };

    let flush_operations_before = collector_store
        .local_file_store()
        .get_commit_log()
        .mapped_file_io_stats()
        .flush_operations;
    let rocks_reads_before = collector_store.rocks_read_counters();
    ensure!(
        rocks_reads_before.is_some() == scenario.includes_rocks_pull_observations(),
        "{}/{} RocksDB counter presence does not match the workload",
        scenario.profile,
        scenario.variant
    );
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let workload = match pull_context.as_ref() {
        Some(context) => runtime.block_on(run_pull_workload(
            &collector_store,
            context,
            scenario,
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
    let allocation_calls = ALLOCATION_CALLS
        .load(Ordering::Relaxed)
        .checked_sub(allocations_before)
        .ok_or_else(|| anyhow!("allocation counter moved backwards"))?;
    let flush_operations_after = collector_store
        .local_file_store()
        .get_commit_log()
        .mapped_file_io_stats()
        .flush_operations;
    let measured_flush_operations = flush_operations_after
        .checked_sub(flush_operations_before)
        .ok_or_else(|| anyhow!("mapped-file flush counter moved backwards"))?;
    let rocks_read_observation = match (rocks_reads_before, collector_store.rocks_read_counters()) {
        (Some(before), Some(after)) => Some(measured_rocks_reads(before, after, scenario.operations_per_sample)?),
        (None, None) => None,
        _ => bail!("RocksDB counter presence changed during the measured workload"),
    };
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
        io_amplification_ratio: workload
            .encoded_bytes
            .checked_add(rocks_read_observation.map_or(0, |observation| observation.bytes_read))
            .ok_or_else(|| anyhow!("measured I/O byte counter overflowed"))? as f64
            / (scenario.operations_per_sample * scenario.payload_bytes_per_operation()) as f64,
        fsync_per_ack: scenario
            .includes_fsync_per_ack()
            .then_some(measured_flush_operations as f64 / scenario.operations_per_sample as f64),
        cq_unit_allocations_per_message,
        body_copies_per_message: scenario
            .includes_local_pull_observations()
            .then(|| workload.body_copies as f64 / workload.returned_messages as f64),
        native_read_calls_per_batch: rocks_read_observation.map(|observation| observation.native_read_calls_per_batch),
        provider_writes_per_batch: None,
        metadata_commits_per_batch: None,
        provider_reads_per_batch: None,
    };
    observation.validate(scenario)?;
    if scenario.requires_started_store() {
        runtime
            .block_on(collector_store.local_file_store_mut().shutdown_gracefully())
            .context("shut down sync-flush sample store")?;
    }
    collector_store.close_rocksdb();
    Ok(observation)
}

#[cfg(feature = "tieredstore")]
async fn collect_one_tiered_sample(scenario: Scenario) -> Result<SampleObservation> {
    let collector = new_tiered_collector()?;
    match scenario.workload_kind {
        WorkloadKind::TieredAppend => collect_one_tiered_append_sample(&collector, scenario).await,
        WorkloadKind::TieredPull => collect_one_tiered_pull_sample(&collector, scenario).await,
        _ => bail!(
            "TieredStore sample received non-tiered scenario {}/{}",
            scenario.profile,
            scenario.variant
        ),
    }
}

#[cfg(feature = "tieredstore")]
fn new_tiered_collector() -> Result<TieredCollector> {
    let temp_dir = TempDir::new().context("create TieredStore performance sample directory")?;
    let root = temp_dir.path().join("tieredstore");
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: root.clone(),
        backend_provider: "posix".to_owned(),
        commit_log_segment_size: 16 * 1024 * 1024,
        consume_queue_segment_size: 64 * 1024,
        read_ahead_cache_enable: true,
        read_ahead_cache_max_bytes: 4 * 1024 * 1024,
        read_ahead_message_count: LOCAL_PULL_BATCH_MESSAGES,
        read_ahead_message_size: LOCAL_PULL_BATCH_MESSAGES * MESSAGE_SIZE_BYTES,
        ..TieredStoreConfig::default()
    });
    let provider = PosixProvider::new(root);
    let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
    let flat_file_store = Arc::new(TieredFlatFileStore::new(
        config.clone(),
        metadata_store.clone(),
        provider.clone(),
    ));
    let fetcher = DefaultTieredMessageFetcher::new(config, flat_file_store.clone());
    Ok(TieredCollector {
        provider,
        metadata_store,
        flat_file_store,
        fetcher,
        _temp_dir: temp_dir,
    })
}

#[cfg(feature = "tieredstore")]
async fn collect_one_tiered_append_sample(
    collector: &TieredCollector,
    scenario: Scenario,
) -> Result<SampleObservation> {
    let flat_file = collector
        .flat_file_store
        .get_or_create("ArchitectureTieredAppend".to_owned(), 0)
        .context("create TieredStore append flat file")?;
    let warmup_messages = scenario
        .warmup_operations
        .checked_mul(TIERED_APPEND_BATCH_MESSAGES)
        .ok_or_else(|| anyhow!("TieredStore append warm-up message count overflowed"))?;
    run_tiered_append_batches(&flat_file, scenario.warmup_operations, 0)
        .await
        .context("run TieredStore append warm-up")?;

    let provider_before = collector.provider.io_snapshot();
    let metadata_before = collector.metadata_store.successful_persist_count();
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let workload = run_tiered_append_batches(
        &flat_file,
        scenario.operations_per_sample,
        i64::try_from(warmup_messages).context("TieredStore warm-up queue offset exceeds i64")?,
    )
    .await
    .context("run measured TieredStore append batches")?;
    let allocation_calls = allocation_delta(allocations_before, "TieredStore append allocation")?;
    let provider_delta = tiered_io_delta(provider_before, collector.provider.io_snapshot())?;
    let metadata_commits = collector
        .metadata_store
        .successful_persist_count()
        .checked_sub(metadata_before)
        .ok_or_else(|| anyhow!("TieredStore metadata persist counter moved backwards"))?;
    ensure!(
        provider_delta.write_operations > 0 && provider_delta.bytes_written > 0,
        "TieredStore append recorded no POSIX provider writes"
    );
    ensure!(metadata_commits > 0, "TieredStore append recorded no metadata persists");

    let operation_count = scenario.operations_per_sample as f64;
    let observation = SampleObservation {
        throughput_per_second: operation_count / workload.elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&workload.latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocation_calls as f64 / operation_count,
        io_amplification_ratio: provider_delta.bytes_written as f64
            / (scenario.operations_per_sample * scenario.payload_bytes_per_operation()) as f64,
        fsync_per_ack: None,
        cq_unit_allocations_per_message: None,
        body_copies_per_message: None,
        native_read_calls_per_batch: None,
        provider_writes_per_batch: Some(provider_delta.write_operations as f64 / operation_count),
        metadata_commits_per_batch: Some(metadata_commits as f64 / operation_count),
        provider_reads_per_batch: None,
    };
    observation.validate(scenario)?;
    Ok(observation)
}

#[cfg(feature = "tieredstore")]
async fn run_tiered_append_batches(
    flat_file: &Arc<rocketmq_tieredstore::TieredFlatFile<PosixProvider>>,
    batch_count: usize,
    first_queue_offset: i64,
) -> Result<TimedTieredWorkload> {
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(batch_count);
    for batch_index in 0..batch_count {
        let batch_started = Instant::now();
        let batch_base = batch_index
            .checked_mul(TIERED_APPEND_BATCH_MESSAGES)
            .ok_or_else(|| anyhow!("TieredStore append batch offset overflowed"))?;
        for message_index in 0..TIERED_APPEND_BATCH_MESSAGES {
            let relative_offset = batch_base
                .checked_add(message_index)
                .ok_or_else(|| anyhow!("TieredStore append message offset overflowed"))?;
            let queue_offset = first_queue_offset
                .checked_add(i64::try_from(relative_offset).context("TieredStore queue offset exceeds i64")?)
                .ok_or_else(|| anyhow!("TieredStore queue offset overflowed"))?;
            let body = Bytes::from(vec![(queue_offset & 0xff) as u8; MESSAGE_SIZE_BYTES]);
            let commit_log_offset = flat_file
                .append_commit_log(body.clone(), queue_offset)
                .await
                .context("append TieredStore CommitLog record")?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: i64::try_from(commit_log_offset)
                            .context("TieredStore CommitLog offset exceeds i64")?,
                        size: i32::try_from(body.len()).context("TieredStore message size exceeds i32")?,
                        tags_code: queue_offset,
                    },
                    queue_offset,
                )
                .await
                .context("append TieredStore ConsumeQueue unit")?;
        }
        flat_file.commit().await.context("commit TieredStore append batch")?;
        latencies.push(batch_started.elapsed());
    }
    Ok(TimedTieredWorkload {
        elapsed: started.elapsed(),
        latencies,
    })
}

#[cfg(feature = "tieredstore")]
async fn collect_one_tiered_pull_sample(collector: &TieredCollector, scenario: Scenario) -> Result<SampleObservation> {
    seed_tiered_pull(collector).await?;
    if scenario.variant == "warm-32" {
        let warmup = collector
            .fetcher
            .get_message(
                "ArchitectureTieredPull".to_owned(),
                0,
                0,
                LOCAL_PULL_BATCH_MESSAGES as i32,
            )
            .await
            .context("warm TieredStore read-ahead cache")?;
        ensure!(
            warmup.status == TieredGetMessageStatus::Found && warmup.messages.len() == LOCAL_PULL_BATCH_MESSAGES,
            "TieredStore warm-up pull returned an incomplete batch"
        );
    }

    let provider_before = collector.provider.io_snapshot();
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let workload = run_tiered_pull_batches(collector, scenario.operations_per_sample).await?;
    let allocation_calls = allocation_delta(allocations_before, "TieredStore pull allocation")?;
    let provider_delta = tiered_io_delta(provider_before, collector.provider.io_snapshot())?;
    if scenario.variant == "cold-32" {
        ensure!(
            provider_delta.read_operations > 0 && provider_delta.bytes_read > 0,
            "cold TieredStore pull recorded no POSIX provider reads"
        );
    } else {
        ensure!(
            provider_delta.read_operations == 0 && provider_delta.bytes_read == 0,
            "warm TieredStore pull escaped the read-ahead cache"
        );
    }

    let operation_count = scenario.operations_per_sample as f64;
    let observation = SampleObservation {
        throughput_per_second: operation_count / workload.elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&workload.latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocation_calls as f64 / operation_count,
        io_amplification_ratio: provider_delta.bytes_read as f64
            / (scenario.operations_per_sample * scenario.payload_bytes_per_operation()) as f64,
        fsync_per_ack: None,
        cq_unit_allocations_per_message: None,
        body_copies_per_message: None,
        native_read_calls_per_batch: None,
        provider_writes_per_batch: None,
        metadata_commits_per_batch: None,
        provider_reads_per_batch: Some(provider_delta.read_operations as f64 / operation_count),
    };
    observation.validate(scenario)?;
    Ok(observation)
}

#[cfg(feature = "tieredstore")]
async fn seed_tiered_pull(collector: &TieredCollector) -> Result<()> {
    let flat_file = collector
        .flat_file_store
        .get_or_create("ArchitectureTieredPull".to_owned(), 0)
        .context("create TieredStore pull flat file")?;
    for queue_offset in 0..LOCAL_PULL_BATCH_MESSAGES {
        let queue_offset = i64::try_from(queue_offset).context("TieredStore seed queue offset exceeds i64")?;
        let body = Bytes::from(vec![(queue_offset & 0xff) as u8; MESSAGE_SIZE_BYTES]);
        let commit_log_offset = flat_file
            .append_commit_log(body.clone(), queue_offset)
            .await
            .context("seed TieredStore CommitLog record")?;
        flat_file
            .append_consume_queue(
                queue_offset,
                ConsumeQueueUnit {
                    commit_log_offset: i64::try_from(commit_log_offset)
                        .context("TieredStore seed CommitLog offset exceeds i64")?,
                    size: i32::try_from(body.len()).context("TieredStore seed message size exceeds i32")?,
                    tags_code: queue_offset,
                },
                queue_offset,
            )
            .await
            .context("seed TieredStore ConsumeQueue unit")?;
    }
    flat_file.commit().await.context("commit TieredStore pull seed")
}

#[cfg(feature = "tieredstore")]
async fn run_tiered_pull_batches(collector: &TieredCollector, operation_count: usize) -> Result<TimedTieredWorkload> {
    ensure!(operation_count > 0, "TieredStore pull operation count must be positive");
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operation_count);
    for _ in 0..operation_count {
        let operation_started = Instant::now();
        let result = collector
            .fetcher
            .get_message(
                "ArchitectureTieredPull".to_owned(),
                0,
                0,
                LOCAL_PULL_BATCH_MESSAGES as i32,
            )
            .await
            .context("pull TieredStore batch")?;
        ensure!(
            result.status == TieredGetMessageStatus::Found && result.messages.len() == LOCAL_PULL_BATCH_MESSAGES,
            "TieredStore pull returned status {:?} with {} messages",
            result.status,
            result.messages.len()
        );
        ensure!(
            result
                .messages
                .iter()
                .all(|message| message.len() == MESSAGE_SIZE_BYTES),
            "TieredStore pull returned a message with the wrong size"
        );
        latencies.push(operation_started.elapsed());
    }
    Ok(TimedTieredWorkload {
        elapsed: started.elapsed(),
        latencies,
    })
}

#[cfg(feature = "tieredstore")]
fn tiered_io_delta(before: PosixProviderIoSnapshot, after: PosixProviderIoSnapshot) -> Result<TieredIoDelta> {
    Ok(TieredIoDelta {
        read_operations: after
            .read_operations
            .checked_sub(before.read_operations)
            .ok_or_else(|| anyhow!("TieredStore provider read-operation counter moved backwards"))?,
        write_operations: after
            .write_operations
            .checked_sub(before.write_operations)
            .ok_or_else(|| anyhow!("TieredStore provider write-operation counter moved backwards"))?,
        bytes_read: after
            .bytes_read
            .checked_sub(before.bytes_read)
            .ok_or_else(|| anyhow!("TieredStore provider byte-read counter moved backwards"))?,
        bytes_written: after
            .bytes_written
            .checked_sub(before.bytes_written)
            .ok_or_else(|| anyhow!("TieredStore provider byte-write counter moved backwards"))?,
    })
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
    if scenario.is_pull() {
        message_store_config.mapped_file_size_commit_log = 4 * 1024 * 1024;
        message_store_config.mapped_file_size_consume_queue = 20 * 1024;
    } else {
        message_store_config.mapped_file_size_commit_log = 256 * 1024 * 1024;
    }

    #[cfg(feature = "rocksdb_store")]
    if scenario.includes_rocks_pull_observations() {
        message_store_config.store_type = StoreType::RocksDB;
        message_store_config.timer_rocksdb_enable = false;
        message_store_config.trans_rocksdb_enable = false;
        let store = RocksDBMessageStore::try_new(
            Arc::new(message_store_config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
            None,
            false,
        )
        .context("create RocksDB store performance sample")?;
        return Ok(CollectorStore {
            backend: CollectorBackend::Rocks(Box::new(store)),
            _temp_dir: temp_dir,
        });
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
        backend: CollectorBackend::Local(Box::new(store)),
        _temp_dir: temp_dir,
    })
}

async fn start_collector_store(collector_store: &mut CollectorStore) -> Result<()> {
    let store = collector_store.local_file_store_mut();
    store.init().await.context("initialize sample store")?;
    ensure!(store.load().await, "load sample store");
    store.start().await.context("launch sample store")
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
        let commit_log = collector_store.local_file_store().get_commit_log();
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

async fn seed_pull_store(collector_store: &mut CollectorStore, scenario: Scenario) -> Result<PullContext> {
    ensure!(scenario.is_pull(), "seed pull store received a non-pull scenario");
    let topic = match scenario.workload_kind {
        WorkloadKind::LocalPull => CheetahString::from_static_str("ArchitectureLocalPull"),
        WorkloadKind::RocksPull => CheetahString::from_static_str("ArchitectureRocksPull"),
        WorkloadKind::Append => bail!("append scenario cannot seed a pull store"),
        #[cfg(feature = "tieredstore")]
        WorkloadKind::TieredAppend | WorkloadKind::TieredPull => {
            bail!("TieredStore scenarios use the dedicated POSIX fixture")
        }
    };
    let group = CheetahString::from_static_str("ArchitectureGate");

    for key_seed in 0..LOCAL_PULL_BATCH_MESSAGES as u64 {
        let result = collector_store.put_message(create_message(&topic, 0, key_seed)).await;
        ensure!(
            result.put_message_status() == PutMessageStatus::PutOk,
            "{}/{} seed append returned {:?}",
            scenario.profile,
            scenario.variant,
            result.put_message_status()
        );
    }
    collector_store.reput_once().await;

    let result = pull_batch(
        collector_store,
        &group,
        &topic,
        scenario.profile,
        LOCAL_PULL_BATCH_MESSAGES,
        false,
    )
    .await
    .with_context(|| format!("read seeded {}/{} batch", scenario.profile, scenario.variant))?;
    let message_locations = result
        .message_mapped_list()
        .iter()
        .map(|selection| {
            Ok(MessageLocation {
                offset: i64::try_from(selection.start_offset).context("pull message offset exceeds i64")?,
                size: selection.size,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(PullContext {
        group,
        topic,
        message_locations,
    })
}

async fn run_pull_workload(
    collector_store: &CollectorStore,
    context: &PullContext,
    scenario: Scenario,
    operation_count: usize,
    require_hot: bool,
) -> Result<WorkloadObservation> {
    ensure!(scenario.is_pull(), "pull workload received a non-pull scenario");
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operation_count);
    let mut encoded_bytes = 0_u64;
    let mut body_copies = 0_u64;
    let mut returned_messages = 0_u64;

    for _ in 0..operation_count {
        let pull_started = Instant::now();
        let result = pull_batch(
            collector_store,
            &context.group,
            &context.topic,
            scenario.profile,
            LOCAL_PULL_BATCH_MESSAGES,
            require_hot,
        )
        .await?;
        latencies.push(pull_started.elapsed());
        encoded_bytes = encoded_bytes
            .checked_add(u64::try_from(result.buffer_total_size()).context("pull returned negative encoded bytes")?)
            .ok_or_else(|| anyhow!("pull encoded byte counter overflowed"))?;
        body_copies = body_copies
            .checked_add(
                result
                    .message_mapped_list()
                    .iter()
                    .filter(|selection| selection.source_kind != SelectMappedBufferSourceKind::MappedFile)
                    .count() as u64,
            )
            .ok_or_else(|| anyhow!("pull body-copy counter overflowed"))?;
        returned_messages = returned_messages
            .checked_add(u64::try_from(result.message_count()).context("pull returned a negative message count")?)
            .ok_or_else(|| anyhow!("pull message counter overflowed"))?;
    }

    ensure!(returned_messages > 0, "pull workload returned no messages");
    Ok(WorkloadObservation {
        elapsed: started.elapsed(),
        latencies,
        encoded_bytes,
        body_copies,
        returned_messages,
    })
}

async fn pull_batch(
    collector_store: &CollectorStore,
    group: &CheetahString,
    topic: &CheetahString,
    profile: &str,
    batch_messages: usize,
    require_hot: bool,
) -> Result<GetMessageResult> {
    let batch_messages_i32 = i32::try_from(batch_messages).context("pull batch size exceeds i32")?;
    let result = collector_store
        .get_message(group, topic, batch_messages_i32)
        .await
        .ok_or_else(|| anyhow!("{profile} store omitted its GetMessageResult"))?;
    ensure!(
        result.status() == Some(GetMessageStatus::Found),
        "{profile} returned status {:?}",
        result.status()
    );
    ensure!(
        result.message_count() == batch_messages_i32,
        "{profile} returned {} messages, expected {batch_messages}",
        result.message_count()
    );
    ensure!(
        result.message_mapped_list().len() == batch_messages,
        "{profile} returned {} buffers, expected {batch_messages}",
        result.message_mapped_list().len()
    );
    if require_hot {
        ensure!(
            result
                .message_mapped_list()
                .iter()
                .all(|selection| selection.cache_state == SelectMappedBufferCacheState::Hot),
            "{profile} batch included a non-hot mapped buffer"
        );
    }
    Ok(result)
}

async fn measure_cq_unit_allocations_per_message(
    collector_store: &CollectorStore,
    context: &PullContext,
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
    context: &PullContext,
    batch_messages: usize,
) -> Result<u64> {
    let before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    for _ in 0..CQ_ALLOCATION_PROBE_REPETITIONS {
        drop(
            pull_batch(
                collector_store,
                &context.group,
                &context.topic,
                LOCAL_PULL_PROFILE,
                batch_messages,
                true,
            )
            .await?,
        );
    }
    allocation_delta(before, "full local-pull allocation probe")
}

fn measure_direct_commit_log_allocation_calls(
    collector_store: &CollectorStore,
    context: &PullContext,
    batch_messages: usize,
) -> Result<u64> {
    let before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    for _ in 0..CQ_ALLOCATION_PROBE_REPETITIONS {
        for location in context.message_locations.iter().take(batch_messages) {
            let selection = collector_store
                .local_file_store()
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

fn measured_rocks_reads(
    before: RocksReadCounters,
    after: RocksReadCounters,
    operation_count: usize,
) -> Result<RocksReadObservation> {
    ensure!(operation_count > 0, "RocksDB measured operation count must be positive");
    let point_reads = after
        .point_reads
        .checked_sub(before.point_reads)
        .ok_or_else(|| anyhow!("RocksDB point-read counter moved backwards"))?;
    let range_scans = after
        .range_scans
        .checked_sub(before.range_scans)
        .ok_or_else(|| anyhow!("RocksDB range-scan counter moved backwards"))?;
    let bytes_read = after
        .bytes_read
        .checked_sub(before.bytes_read)
        .ok_or_else(|| anyhow!("RocksDB byte-read counter moved backwards"))?;
    let cache_misses = after
        .block_cache_misses
        .checked_sub(before.block_cache_misses)
        .ok_or_else(|| anyhow!("RocksDB block-cache miss counter moved backwards"))?;
    ensure!(
        cache_misses == 0,
        "hot RocksDB pull recorded {cache_misses} block-cache misses"
    );
    let native_read_calls = point_reads
        .checked_add(range_scans)
        .ok_or_else(|| anyhow!("RocksDB native read-call counter overflowed"))?;
    ensure!(native_read_calls > 0, "RocksDB pull recorded no native read calls");
    Ok(RocksReadObservation {
        native_read_calls_per_batch: native_read_calls as f64 / operation_count as f64,
        bytes_read,
    })
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
            native_read_calls_per_batch: None,
            provider_writes_per_batch: None,
            metadata_commits_per_batch: None,
            provider_reads_per_batch: None,
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

        #[cfg(feature = "rocksdb_store")]
        {
            let Invocation::Collect(rocks_pull) =
                parse_invocation(&[ROCKS_PULL_PROFILE.to_owned(), "batch-32".to_owned()])
                    .expect("supported RocksDB pull variant")
            else {
                panic!("expected collection invocation");
            };
            assert_eq!(rocks_pull.profile, ROCKS_PULL_PROFILE);
            assert_eq!(rocks_pull.variant, "batch-32");
            assert_eq!(rocks_pull.workload_kind, WorkloadKind::RocksPull);
            assert!(rocks_pull.includes_rocks_pull_observations());
        }

        #[cfg(not(feature = "rocksdb_store"))]
        assert!(parse_invocation(&[ROCKS_PULL_PROFILE.to_owned(), "batch-32".to_owned()]).is_err());

        #[cfg(feature = "tieredstore")]
        {
            let Invocation::Collect(tiered_append) =
                parse_invocation(&[TIERED_APPEND_PROFILE.to_owned(), "batch-64".to_owned()])
                    .expect("supported TieredStore append variant")
            else {
                panic!("expected collection invocation");
            };
            assert_eq!(tiered_append.workload_kind, WorkloadKind::TieredAppend);
            assert_eq!(
                tiered_append.payload_bytes_per_operation(),
                TIERED_APPEND_BATCH_MESSAGES * MESSAGE_SIZE_BYTES
            );

            for variant in ["cold-32", "warm-32"] {
                let Invocation::Collect(tiered_pull) =
                    parse_invocation(&[TIERED_PULL_PROFILE.to_owned(), variant.to_owned()])
                        .expect("supported TieredStore pull variant")
                else {
                    panic!("expected collection invocation");
                };
                assert_eq!(tiered_pull.workload_kind, WorkloadKind::TieredPull);
                assert!(tiered_pull.includes_tiered_pull_observations());
            }
        }

        #[cfg(not(feature = "tieredstore"))]
        {
            assert!(parse_invocation(&[TIERED_APPEND_PROFILE.to_owned(), "batch-64".to_owned()]).is_err());
            assert!(parse_invocation(&[TIERED_PULL_PROFILE.to_owned(), "cold-32".to_owned()]).is_err());
            assert!(parse_invocation(&[TIERED_PULL_PROFILE.to_owned(), "warm-32".to_owned()]).is_err());
        }
    }

    #[test]
    fn rejects_unknown_profile_variant_and_shape() {
        assert!(parse_invocation(&["other".to_owned(), "producers-1".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), "producers-2".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), "concurrency-64".to_owned()]).is_err());
        assert!(parse_invocation(&[SYNC_FLUSH_PROFILE.to_owned(), "producers-32".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_PULL_PROFILE.to_owned(), "producers-32".to_owned()]).is_err());
        assert!(parse_invocation(&[LOCAL_APPEND_PROFILE.to_owned(), "batch-32".to_owned()]).is_err());
        assert!(parse_invocation(&[ROCKS_PULL_PROFILE.to_owned(), "producers-32".to_owned()]).is_err());
        assert!(parse_invocation(&[TIERED_APPEND_PROFILE.to_owned(), "cold-32".to_owned()]).is_err());
        assert!(parse_invocation(&[TIERED_PULL_PROFILE.to_owned(), "batch-64".to_owned()]).is_err());
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

    #[cfg(feature = "rocksdb_store")]
    #[test]
    fn rocks_pull_inventory_includes_native_read_calls() {
        let observations = (0..SAMPLE_COUNT)
            .map(|index| SampleObservation {
                native_read_calls_per_batch: Some(3.0),
                ..sample(index as f64, None, None, None)
            })
            .collect();
        let scenario = Scenario::parse(ROCKS_PULL_PROFILE, "batch-32").expect("RocksDB pull scenario");
        let value = serde_json::to_value(build_envelope(scenario, observations).expect("build measurement"))
            .expect("serialize measurement");

        assert_eq!(value["profile"], ROCKS_PULL_PROFILE);
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
                "io_amplification_ratio",
                "native_read_calls_per_batch",
                "p99_latency_us",
                "peak_rss_bytes",
                "throughput_per_second",
            ]
        );
    }

    #[cfg(feature = "tieredstore")]
    #[test]
    fn tiered_append_inventory_includes_provider_and_metadata_calls() {
        let observations = (0..SAMPLE_COUNT)
            .map(|index| SampleObservation {
                provider_writes_per_batch: Some(128.0),
                metadata_commits_per_batch: Some(3.0),
                ..sample(index as f64, None, None, None)
            })
            .collect();
        let scenario = Scenario::parse(TIERED_APPEND_PROFILE, "batch-64").expect("TieredStore append scenario");
        let value = serde_json::to_value(build_envelope(scenario, observations).expect("build measurement"))
            .expect("serialize measurement");

        assert_eq!(value["profile"], TIERED_APPEND_PROFILE);
        assert_eq!(value["variant"], "batch-64");
        assert_eq!(
            value["metrics"]
                .as_object()
                .expect("metrics object")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec![
                "allocations_per_operation",
                "io_amplification_ratio",
                "metadata_commits_per_batch",
                "p99_latency_us",
                "peak_rss_bytes",
                "provider_writes_per_batch",
                "throughput_per_second",
            ]
        );
    }

    #[cfg(feature = "tieredstore")]
    #[test]
    fn tiered_pull_inventory_includes_provider_reads() {
        for variant in ["cold-32", "warm-32"] {
            let observations = (0..SAMPLE_COUNT)
                .map(|index| SampleObservation {
                    provider_reads_per_batch: Some(if variant == "cold-32" { 2.0 } else { 0.0 }),
                    ..sample(index as f64, None, None, None)
                })
                .collect();
            let scenario = Scenario::parse(TIERED_PULL_PROFILE, variant).expect("TieredStore pull scenario");
            let value = serde_json::to_value(build_envelope(scenario, observations).expect("build measurement"))
                .expect("serialize measurement");

            assert_eq!(value["profile"], TIERED_PULL_PROFILE);
            assert_eq!(value["variant"], variant);
            assert_eq!(
                value["metrics"]
                    .as_object()
                    .expect("metrics object")
                    .keys()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                vec![
                    "allocations_per_operation",
                    "io_amplification_ratio",
                    "p99_latency_us",
                    "peak_rss_bytes",
                    "provider_reads_per_batch",
                    "throughput_per_second",
                ]
            );
        }
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

        #[cfg(feature = "rocksdb_store")]
        {
            let rocks_pull = Scenario::parse(ROCKS_PULL_PROFILE, "batch-32").expect("RocksDB pull scenario");
            assert!(sample(1.0, None, None, None).validate(rocks_pull).is_err());
            assert!(SampleObservation {
                native_read_calls_per_batch: Some(f64::NAN),
                ..sample(1.0, None, None, None)
            }
            .validate(rocks_pull)
            .is_err());
            assert!(SampleObservation {
                native_read_calls_per_batch: Some(3.0),
                ..sample(1.0, None, None, None)
            }
            .validate(local_append)
            .is_err());
        }

        #[cfg(feature = "tieredstore")]
        {
            let tiered_append =
                Scenario::parse(TIERED_APPEND_PROFILE, "batch-64").expect("TieredStore append scenario");
            assert!(sample(1.0, None, None, None).validate(tiered_append).is_err());
            assert!(SampleObservation {
                provider_writes_per_batch: Some(1.0),
                metadata_commits_per_batch: Some(f64::NAN),
                ..sample(1.0, None, None, None)
            }
            .validate(tiered_append)
            .is_err());

            let tiered_pull = Scenario::parse(TIERED_PULL_PROFILE, "cold-32").expect("TieredStore pull scenario");
            assert!(sample(1.0, None, None, None).validate(tiered_pull).is_err());
            assert!(SampleObservation {
                provider_reads_per_batch: Some(f64::INFINITY),
                ..sample(1.0, None, None, None)
            }
            .validate(tiered_pull)
            .is_err());
            assert!(SampleObservation {
                provider_reads_per_batch: Some(2.0),
                ..sample(1.0, None, None, None)
            }
            .validate(local_append)
            .is_err());
        }
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

    #[test]
    fn computes_rocks_read_observation_and_rejects_non_hot_or_regressed_counters() {
        let before = RocksReadCounters {
            point_reads: 10,
            range_scans: 4,
            bytes_read: 100,
            block_cache_misses: 2,
        };
        let observation = measured_rocks_reads(
            before,
            RocksReadCounters {
                point_reads: 74,
                range_scans: 36,
                bytes_read: 612,
                block_cache_misses: 2,
            },
            32,
        )
        .expect("valid RocksDB read observation");
        assert_eq!(observation.native_read_calls_per_batch, 3.0);
        assert_eq!(observation.bytes_read, 512);

        assert!(measured_rocks_reads(
            before,
            RocksReadCounters {
                block_cache_misses: 3,
                ..before
            },
            1
        )
        .is_err());
        assert!(measured_rocks_reads(
            before,
            RocksReadCounters {
                point_reads: 9,
                ..before
            },
            1
        )
        .is_err());
        assert!(measured_rocks_reads(before, before, 0).is_err());
    }

    #[cfg(feature = "tieredstore")]
    #[test]
    fn computes_tiered_io_delta_and_rejects_regressed_counters() {
        let before = PosixProviderIoSnapshot {
            read_operations: 2,
            write_operations: 4,
            bytes_read: 128,
            bytes_written: 256,
        };
        let after = PosixProviderIoSnapshot {
            read_operations: 5,
            write_operations: 10,
            bytes_read: 512,
            bytes_written: 1024,
        };
        assert_eq!(
            tiered_io_delta(before, after).expect("monotonic TieredStore I/O counters"),
            TieredIoDelta {
                read_operations: 3,
                write_operations: 6,
                bytes_read: 384,
                bytes_written: 768,
            }
        );
        assert!(tiered_io_delta(
            before,
            PosixProviderIoSnapshot {
                read_operations: 1,
                ..after
            }
        )
        .is_err());
    }
}
