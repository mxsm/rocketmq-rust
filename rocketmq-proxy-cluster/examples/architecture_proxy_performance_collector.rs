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

//! Target-hardware collector for Proxy Cluster head-of-line isolation.
//!
//! The workload holds one key in simulated remote I/O while unrelated keys
//! execute through the production count/byte admission and exact-key queues.
//! Throughput and p99 describe unrelated completion while the slow key is
//! blocked; they are not end-to-end Broker TPS.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::collections::BTreeMap;
use std::env;
use std::process;
use std::process::Command;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use rocketmq_proxy_cluster::bench_support::run_cluster_mixed_execution_probe;
use serde::Deserialize;
use serde::Serialize;

const PROFILE: &str = "proxy-mixed-execution";
const VARIANT: &str = "same-and-distinct-keys";
const SAMPLE_COUNT: usize = 5;
const PRIMING_SAMPLE_COUNT: usize = 2;
const ROUND_COUNT: usize = 512;
const UNRELATED_COMMANDS_PER_ROUND: usize = 64;
const UNRELATED_KEY_COUNT: usize = 16;
const MESSAGES_PER_COMMAND: usize = 32;
const MESSAGE_SIZE_BYTES: usize = 1_024;
const BLOCKED_IO_DURATION: Duration = Duration::from_millis(20);

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
            "every admitted command must be drained exactly once"
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
        _ => bail!("usage: architecture_proxy_performance_collector [--sample] {PROFILE} {VARIANT}"),
    }
}

fn run_sample() -> Result<SampleObservation> {
    let allocation_start = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let probe = run_cluster_mixed_execution_probe(
        ROUND_COUNT,
        UNRELATED_COMMANDS_PER_ROUND,
        UNRELATED_KEY_COUNT,
        MESSAGES_PER_COMMAND,
        MESSAGE_SIZE_BYTES,
        BLOCKED_IO_DURATION,
    )
    .context("run production Proxy Cluster blocked-key mixed-execution probe")?;
    let expected_drained = probe
        .unrelated_command_count
        .checked_add(ROUND_COUNT)
        .ok_or_else(|| anyhow!("proxy drain count overflowed"))?;
    ensure!(
        probe.drained_count == expected_drained,
        "Proxy command drain count changed"
    );
    ensure!(probe.diagnostics.active_keys == 0, "Proxy key registry did not retire");
    ensure!(
        probe.diagnostics.queued_and_active == 0 && probe.diagnostics.retained_bytes == 0,
        "Proxy admission budget did not return to zero"
    );
    ensure!(
        probe.diagnostics.rejected == 0,
        "Proxy admission unexpectedly rejected commands"
    );
    let measured_duration = probe
        .unrelated_completion_latencies
        .iter()
        .copied()
        .try_fold(Duration::ZERO, |total, latency| total.checked_add(latency))
        .ok_or_else(|| anyhow!("Proxy unrelated completion duration overflowed"))?;
    ensure!(
        !measured_duration.is_zero(),
        "Proxy unrelated completion duration was zero"
    );
    let allocations = ALLOCATION_CALLS
        .load(Ordering::Relaxed)
        .checked_sub(allocation_start)
        .ok_or_else(|| anyhow!("allocation counter moved backwards"))?;
    let observation = SampleObservation {
        throughput_per_second: probe.unrelated_operation_count as f64 / measured_duration.as_secs_f64(),
        p99_latency_us: percentile_99(&probe.unrelated_completion_latencies)?.as_secs_f64() * 1_000_000.0,
        peak_rss_bytes: peak_rss_bytes()? as f64,
        allocations_per_operation: allocations as f64 / probe.unrelated_operation_count as f64,
        io_amplification_ratio: probe.unrelated_operation_count as f64
            / (probe.drained_count - ROUND_COUNT) as f64
            / MESSAGES_PER_COMMAND as f64,
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
            .context("run isolated Proxy performance sample")?;
        ensure!(
            output.status.success(),
            "isolated Proxy sample failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let sample: SampleObservation =
            serde_json::from_slice(&output.stdout).context("decode isolated Proxy sample")?;
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
        eprintln!("ARCHITECTURE_PROXY_PERFORMANCE_COLLECTOR_FAILED {error:#}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if parse_invocation(&arguments)? {
        serde_json::to_writer(std::io::stdout().lock(), &run_sample()?).context("write isolated Proxy sample")?;
    } else {
        let executable = env::current_exe().context("resolve Proxy collector executable")?;
        let observations = collect_samples(&executable)?;
        serde_json::to_writer(std::io::stdout().lock(), &measurement(&observations))
            .context("write Proxy measurement")?;
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
        let values = (1..=100).map(Duration::from_micros).collect::<Vec<_>>();
        assert_eq!(percentile_99(&values).expect("p99"), Duration::from_micros(99));
    }
}
