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

//! Target-hardware collector for the M10 `local-append` performance profile.
//!
//! Run a variant from the workspace root with:
//!
//! ```text
//! cargo run --release --quiet -p rocketmq-store \
//!   --example architecture_local_append_collector -- local-append producers-1
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
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::flush_disk_type::FlushDiskType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;
use tokio::runtime::Builder;

const PROFILE: &str = "local-append";
const MESSAGE_SIZE_BYTES: usize = 1024;
const SAMPLE_COUNT: usize = 5;
const PRIMING_SAMPLE_COUNT: usize = 2;
const OPERATIONS_PER_SAMPLE: usize = 131_072;
const WARMUP_OPERATIONS: usize = 32_768;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Scenario {
    variant: &'static str,
    producers: usize,
}

impl Scenario {
    fn parse(profile: &str, variant: &str) -> Result<Self> {
        ensure!(profile == PROFILE, "unsupported performance profile: {profile}");
        match variant {
            "producers-1" => Ok(Self {
                variant: "producers-1",
                producers: 1,
            }),
            "producers-8" => Ok(Self {
                variant: "producers-8",
                producers: 8,
            }),
            "producers-32" => Ok(Self {
                variant: "producers-32",
                producers: 32,
            }),
            _ => bail!("unsupported {PROFILE} variant: {variant}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
            "usage: architecture_local_append_collector [--sample] local-append <producers-1|producers-8|producers-32>"
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
}

impl SampleObservation {
    fn validate(&self) -> Result<()> {
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
}

fn main() {
    if let Err(error) = run() {
        eprintln!("architecture local append collector failed: {error:#}");
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
            .args(["--sample", PROFILE, scenario.variant])
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
            .validate()
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

    Ok(MeasurementEnvelope {
        schema_version: 1,
        profile: PROFILE,
        variant: scenario.variant,
        metrics,
    })
}

fn collect_one_sample(scenario: Scenario) -> Result<SampleObservation> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create sample runtime")?;
    let collector_store = new_collector_store()?;

    runtime
        .block_on(run_workload(&collector_store, scenario, WARMUP_OPERATIONS, 0))
        .context("run warm-up workload")?;

    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let workload = runtime
        .block_on(run_workload(
            &collector_store,
            scenario,
            OPERATIONS_PER_SAMPLE,
            WARMUP_OPERATIONS as u64,
        ))
        .context("run measured workload")?;
    let allocation_calls = ALLOCATION_CALLS
        .load(Ordering::Relaxed)
        .saturating_sub(allocations_before);

    let observation = SampleObservation {
        throughput_per_second: OPERATIONS_PER_SAMPLE as f64 / workload.elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&workload.latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocation_calls as f64 / OPERATIONS_PER_SAMPLE as f64,
        io_amplification_ratio: workload.encoded_bytes as f64 / (OPERATIONS_PER_SAMPLE * MESSAGE_SIZE_BYTES) as f64,
    };
    observation.validate()?;
    Ok(observation)
}

fn new_collector_store() -> Result<CollectorStore> {
    let temp_dir = TempDir::new().context("create local append sample directory")?;
    let mut message_store_config = MessageStoreConfig {
        store_path_root_dir: temp_dir.path().to_string_lossy().to_string().into(),
        flush_disk_type: FlushDiskType::AsyncFlush,
        ha_listen_port: 0,
        ..MessageStoreConfig::default()
    };
    message_store_config.mapped_file_size_commit_log = 256 * 1024 * 1024;

    let mut store = LocalFileMessageStore::new(
        Arc::new(message_store_config),
        Arc::new(BrokerConfig::default()),
        Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
        None,
        false,
    );
    store
        .wire_owned_root_dependencies()
        .context("wire owned local append sample dependencies")?;

    Ok(CollectorStore {
        store,
        _temp_dir: temp_dir,
    })
}

async fn run_workload(
    collector_store: &CollectorStore,
    scenario: Scenario,
    operation_count: usize,
    key_seed: u64,
) -> Result<WorkloadObservation> {
    ensure!(
        operation_count.is_multiple_of(scenario.producers),
        "operation count {operation_count} must be divisible by producer count {}",
        scenario.producers
    );

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operation_count);
    let mut encoded_bytes = 0_u64;
    let mut next_key = key_seed;

    for _ in 0..operation_count / scenario.producers {
        let commit_log = collector_store.store.get_commit_log();
        let mut puts = Vec::with_capacity(scenario.producers);
        for producer in 0..scenario.producers {
            let message = create_message(producer as i32, next_key);
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
    })
}

fn create_message(queue_id: i32, key_seed: u64) -> MessageExtBrokerInner {
    let mut message = Message::builder()
        .topic(CheetahString::from_static_str("ArchitectureLocalAppend"))
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

    fn sample(value: f64) -> SampleObservation {
        SampleObservation {
            throughput_per_second: value,
            p99_latency_us: value + 1.0,
            peak_rss_bytes: value + 2.0,
            allocations_per_operation: value + 3.0,
            io_amplification_ratio: value + 4.0,
        }
    }

    #[test]
    fn parses_every_supported_variant() {
        for (variant, producers) in [("producers-1", 1), ("producers-8", 8), ("producers-32", 32)] {
            assert_eq!(
                parse_invocation(&[PROFILE.to_owned(), variant.to_owned()]).expect("supported variant"),
                Invocation::Collect(Scenario { variant, producers })
            );
        }
    }

    #[test]
    fn rejects_unknown_profile_variant_and_shape() {
        assert!(parse_invocation(&["other".to_owned(), "producers-1".to_owned()]).is_err());
        assert!(parse_invocation(&[PROFILE.to_owned(), "producers-2".to_owned()]).is_err());
        assert!(parse_invocation(&[PROFILE.to_owned()]).is_err());
        assert!(parse_invocation(&[
            "--sample".to_owned(),
            PROFILE.to_owned(),
            "producers-8".to_owned(),
            "extra".to_owned(),
        ])
        .is_err());
    }

    #[test]
    fn builds_exact_sidecar_metric_inventory_with_five_samples() {
        let observations = (0..SAMPLE_COUNT).map(|index| sample(index as f64)).collect();
        let envelope = build_envelope(
            Scenario {
                variant: "producers-8",
                producers: 8,
            },
            observations,
        )
        .expect("build measurement");
        let value = serde_json::to_value(envelope).expect("serialize measurement");

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["profile"], PROFILE);
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
    fn rejects_partial_and_non_finite_samples() {
        let partial = vec![sample(1.0), sample(2.0)];
        assert!(build_envelope(
            Scenario {
                variant: "producers-1",
                producers: 1,
            },
            partial,
        )
        .is_err());

        let invalid = SampleObservation {
            throughput_per_second: f64::NAN,
            ..sample(1.0)
        };
        assert!(invalid.validate().is_err());
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
}
