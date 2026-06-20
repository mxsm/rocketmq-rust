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

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_common::common::thread::thread_service_tokio::ServiceThreadTokio;
use rocketmq_common::common::thread::Runnable;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Debug, Serialize)]
struct ServiceThreadProbe {
    service_count: usize,
    started: usize,
    completed: usize,
    joined_shutdown_elapsed_us: u128,
    timeout_shutdown_elapsed_us: u128,
    timed_out_thread_finished_after_release: bool,
    healthy: bool,
}

struct CountRunnable {
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl Runnable for CountRunnable {
    fn run(&mut self) {
        self.started.fetch_add(1, Ordering::Relaxed);
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReleaseRunnable {
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    release_rx: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl Runnable for ReleaseRunnable {
    fn run(&mut self) {
        self.started.fetch_add(1, Ordering::Release);
        let _ = self
            .release_rx
            .lock()
            .expect("release receiver mutex should not be poisoned")
            .recv();
        self.completed.fetch_add(1, Ordering::Release);
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-common should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn runtime_owner() -> RuntimeOwner {
    RuntimeOwner::new(RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        thread_name: "rocketmq-common-service-thread-bench".to_string(),
        shutdown_timeout: Duration::from_secs(1),
        ..RuntimeConfig::default()
    })
    .expect("service thread benchmark runtime should start")
}

fn wait_until(deadline: Duration, predicate: impl Fn() -> bool) -> bool {
    let started_at = Instant::now();
    while started_at.elapsed() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    predicate()
}

fn run_service_thread_probe(service_count: usize) -> ServiceThreadProbe {
    let owner = runtime_owner();
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let mut services = Vec::with_capacity(service_count);
    for service_index in 0..service_count {
        let runnable = CountRunnable {
            started: started.clone(),
            completed: completed.clone(),
        };
        let mut service = ServiceThreadTokio::new(
            format!("common-service-thread-bench-{service_index}"),
            Arc::new(Mutex::new(runnable)),
        );
        service.start();
        services.push(service);
    }
    assert!(
        wait_until(Duration::from_secs(1), || completed.load(Ordering::Acquire)
            == service_count),
        "all dedicated service threads should finish their quick run"
    );

    let shutdown_started_at = Instant::now();
    owner.block_on(async {
        for service in &mut services {
            service
                .shutdown_interrupt_with_timeout(false, Duration::from_secs(1))
                .await;
        }
    });
    let joined_shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();

    let timeout_started = Arc::new(AtomicUsize::new(0));
    let timeout_completed = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut timeout_service = ServiceThreadTokio::new(
        "common-service-thread-timeout-bench".to_string(),
        Arc::new(Mutex::new(ReleaseRunnable {
            started: timeout_started.clone(),
            completed: timeout_completed.clone(),
            release_rx: Arc::new(std::sync::Mutex::new(release_rx)),
        })),
    );
    timeout_service.start();
    assert!(
        wait_until(Duration::from_secs(1), || timeout_started.load(Ordering::Acquire) == 1),
        "timeout service thread should start"
    );
    let timeout_shutdown_started_at = Instant::now();
    owner.block_on(async {
        timeout_service
            .shutdown_interrupt_with_timeout(false, Duration::from_millis(10))
            .await;
    });
    let timeout_shutdown_elapsed_us = timeout_shutdown_started_at.elapsed().as_micros();
    release_tx
        .send(())
        .expect("release signal should reach timed-out service thread");
    let timed_out_thread_finished_after_release = wait_until(Duration::from_secs(1), || {
        timeout_completed.load(Ordering::Acquire) == 1
    });

    owner
        .shutdown_runtime_blocking()
        .expect("benchmark runtime should shut down");

    let started = started.load(Ordering::Relaxed);
    let completed = completed.load(Ordering::Relaxed);
    ServiceThreadProbe {
        service_count,
        started,
        completed,
        joined_shutdown_elapsed_us,
        timeout_shutdown_elapsed_us,
        timed_out_thread_finished_after_release,
        healthy: started == service_count
            && completed == service_count
            && timed_out_thread_finished_after_release
            && timeout_shutdown_elapsed_us < 250_000,
    }
}

fn write_service_thread_report_artifact() {
    let output = run_service_thread_probe(16);
    assert!(output.healthy, "{output:?}");
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "common_service_thread",
        "generated_at_unix_ms": generated_at_unix_ms,
        "probe": output,
    });
    let path = output_dir.join("common-service-thread-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("service thread benchmark artifact should serialize"),
    )
    .expect("service thread benchmark artifact should be written");
}

fn bench_common_service_thread(criterion: &mut Criterion) {
    write_service_thread_report_artifact();

    let mut group = criterion.benchmark_group("common_service_thread");
    for service_count in [4usize, 16] {
        group.bench_with_input(
            BenchmarkId::new("dedicated_thread_start_shutdown", service_count),
            &service_count,
            |bencher, service_count| {
                bencher.iter(|| {
                    let output = run_service_thread_probe(black_box(*service_count));
                    assert!(output.healthy, "{output:?}");
                    black_box(output.joined_shutdown_elapsed_us);
                    black_box(output.timeout_shutdown_elapsed_us);
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_common_service_thread
}
criterion_main!(benches);
