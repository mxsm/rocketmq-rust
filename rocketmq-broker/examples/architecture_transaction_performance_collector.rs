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

//! Target-hardware collector for transaction queue resolution with isolated I/O.
//!
//! Each queue resolves through the production operation-queue registry and
//! then writes to its own file. The workload makes lock scope, cross-queue
//! progress, file durability cost, allocations, and RSS observable without
//! treating this focused profile as production broker TPS.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::collections::BTreeMap;
use std::env;
use std::process;
use std::process::Command;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use rocketmq_broker::bench_support::run_transaction_queue_io_probe;
use serde::Deserialize;
use serde::Serialize;
use tokio::runtime::Builder;

const PROFILE: &str = "transaction-queue-io";
const VARIANT: &str = "queues-8";
const SAMPLE_COUNT: usize = 5;
const PRIMING_SAMPLE_COUNT: usize = 2;
const QUEUE_COUNT: usize = 8;
const OPERATIONS_PER_QUEUE: usize = 512;
const PAYLOAD_BYTES: usize = 1_024;
const SYNC_EVERY: usize = 32;

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

// SAFETY: All operations are forwarded unchanged to the system allocator.
// The relaxed counter is observational and does not affect allocation.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller supplies the GlobalAlloc contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller supplies the GlobalAlloc contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Pointer and layout came from the delegated allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: Pointer, layout, and size are forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

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
        for (name, value) in [
            ("throughput_per_second", self.throughput_per_second),
            ("p99_latency_us", self.p99_latency_us),
            ("peak_rss_bytes", self.peak_rss_bytes),
            ("allocations_per_operation", self.allocations_per_operation),
            ("io_amplification_ratio", self.io_amplification_ratio),
        ] {
            ensure!(value.is_finite() && value > 0.0, "{name} must be finite and positive");
        }
        ensure!(
            (self.io_amplification_ratio - 1.0).abs() <= f64::EPSILON,
            "transaction probe must write each logical payload byte once"
        );
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct MetricSamples {
    samples: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct MeasurementEnvelope {
    schema_version: u8,
    profile: &'static str,
    variant: &'static str,
    metrics: BTreeMap<&'static str, MetricSamples>,
}

fn parse_invocation(arguments: &[String]) -> Result<bool> {
    match arguments {
        [profile, variant] if profile == PROFILE && variant == VARIANT => Ok(false),
        [sample, profile, variant] if sample == "--sample" && profile == PROFILE && variant == VARIANT => Ok(true),
        _ => bail!("usage: architecture_transaction_performance_collector [--sample] {PROFILE} {VARIANT}"),
    }
}

async fn run_sample() -> Result<SampleObservation> {
    let root = tempfile::tempdir().context("create transaction performance directory")?;
    let allocation_start = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let started = Instant::now();
    let probe = run_transaction_queue_io_probe(
        root.path().to_path_buf(),
        QUEUE_COUNT,
        OPERATIONS_PER_QUEUE,
        PAYLOAD_BYTES,
        SYNC_EVERY,
    )
    .await
    .context("run production transaction queue I/O probe")?;
    let elapsed = started.elapsed();
    ensure!(probe.queue_count == QUEUE_COUNT, "transaction queue count changed");
    ensure!(
        probe.registry_entries == QUEUE_COUNT,
        "transaction queue registry lost entries"
    );
    ensure!(
        probe.operation_latencies_us.len() == probe.operation_count,
        "transaction latency inventory changed"
    );
    let expected_operations = QUEUE_COUNT
        .checked_mul(OPERATIONS_PER_QUEUE)
        .ok_or_else(|| anyhow!("transaction operation count overflowed"))?;
    ensure!(
        probe.operation_count == expected_operations,
        "transaction operation count changed"
    );
    let logical_bytes = u64::try_from(expected_operations)
        .ok()
        .and_then(|operations| {
            u64::try_from(PAYLOAD_BYTES)
                .ok()
                .and_then(|payload| operations.checked_mul(payload))
        })
        .ok_or_else(|| anyhow!("transaction logical byte count overflowed"))?;
    let allocations = ALLOCATION_CALLS
        .load(Ordering::Relaxed)
        .checked_sub(allocation_start)
        .ok_or_else(|| anyhow!("allocation counter moved backwards"))?;
    let observation = SampleObservation {
        throughput_per_second: expected_operations as f64 / elapsed.as_secs_f64(),
        p99_latency_us: percentile_99(&probe.operation_latencies_us)?,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocations as f64 / expected_operations as f64,
        io_amplification_ratio: probe.bytes_written as f64 / logical_bytes as f64,
    };
    observation.validate()?;
    Ok(observation)
}

fn collect_samples(executable: &std::path::Path) -> Result<Vec<SampleObservation>> {
    let mut observations = Vec::with_capacity(SAMPLE_COUNT);
    for index in 0..(PRIMING_SAMPLE_COUNT + SAMPLE_COUNT) {
        let output = Command::new(executable)
            .args(["--sample", PROFILE, VARIANT])
            .output()
            .context("run isolated transaction performance sample")?;
        ensure!(
            output.status.success(),
            "isolated transaction sample failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let sample: SampleObservation =
            serde_json::from_slice(&output.stdout).context("decode isolated transaction sample")?;
        sample.validate()?;
        if index >= PRIMING_SAMPLE_COUNT {
            observations.push(sample);
        }
    }
    Ok(observations)
}

fn measurement(observations: &[SampleObservation]) -> MeasurementEnvelope {
    let mut metrics = BTreeMap::new();
    for (name, values) in [
        (
            "throughput_per_second",
            observations.iter().map(|sample| sample.throughput_per_second).collect(),
        ),
        (
            "p99_latency_us",
            observations.iter().map(|sample| sample.p99_latency_us).collect(),
        ),
        (
            "peak_rss_bytes",
            observations.iter().map(|sample| sample.peak_rss_bytes).collect(),
        ),
        (
            "allocations_per_operation",
            observations
                .iter()
                .map(|sample| sample.allocations_per_operation)
                .collect(),
        ),
        (
            "io_amplification_ratio",
            observations
                .iter()
                .map(|sample| sample.io_amplification_ratio)
                .collect(),
        ),
    ] {
        metrics.insert(name, MetricSamples { samples: values });
    }
    MeasurementEnvelope {
        schema_version: 1,
        profile: PROFILE,
        variant: VARIANT,
        metrics,
    }
}

fn percentile_99(values: &[f64]) -> Result<f64> {
    ensure!(!values.is_empty(), "cannot compute p99 from an empty sample");
    let mut sorted = values.to_vec();
    ensure!(
        sorted.iter().all(|value| value.is_finite() && *value >= 0.0),
        "transaction latencies must be finite and non-negative"
    );
    sorted.sort_by(f64::total_cmp);
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
    let size = u32::try_from(size_of::<PROCESS_MEMORY_COUNTERS>()).context("process counter size exceeds u32")?;
    counters.cb = size;
    // SAFETY: The pseudo-handle is valid and counters is writable for size bytes.
    unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, size).context("query peak working set")? };
    u64::try_from(counters.PeakWorkingSetSize).context("peak working set exceeds u64")
}

#[cfg(unix)]
fn peak_rss_bytes() -> Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: usage points to writable storage initialized on a zero return.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    ensure!(status == 0, "getrusage failed: {}", std::io::Error::last_os_error());
    // SAFETY: getrusage returned zero and initialized usage.
    let usage = unsafe { usage.assume_init() };
    let peak = u64::try_from(usage.ru_maxrss).context("ru_maxrss was negative")?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        peak.checked_mul(1024)
            .ok_or_else(|| anyhow!("ru_maxrss conversion overflowed"))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        Ok(peak)
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ARCHITECTURE_TRANSACTION_PERFORMANCE_COLLECTOR_FAILED {error:#}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if parse_invocation(&arguments)? {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .thread_name("architecture-transaction-collector")
            .build()
            .context("build transaction collector runtime")?;
        serde_json::to_writer(std::io::stdout().lock(), &runtime.block_on(run_sample())?)
            .context("write isolated transaction sample")?;
    } else {
        let executable = env::current_exe().context("resolve transaction collector executable")?;
        let observations = collect_samples(&executable)?;
        serde_json::to_writer(std::io::stdout().lock(), &measurement(&observations))
            .context("write transaction measurement")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_is_fail_closed() {
        assert!(!parse_invocation(&[PROFILE.to_owned(), VARIANT.to_owned()]).expect("collection invocation"));
        assert!(
            parse_invocation(&["--sample".to_owned(), PROFILE.to_owned(), VARIANT.to_owned()])
                .expect("sample invocation")
        );
        assert!(parse_invocation(&["unknown".to_owned(), VARIANT.to_owned()]).is_err());
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        let values = (1..=100).map(f64::from).collect::<Vec<_>>();
        assert_eq!(percentile_99(&values).expect("p99"), 99.0);
    }
}
