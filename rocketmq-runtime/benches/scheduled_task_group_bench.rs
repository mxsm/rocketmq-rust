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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ScheduleMode;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;

#[derive(Debug)]
struct ScheduledBenchOutput {
    snapshot: ScheduledTaskSnapshot,
    report: ShutdownReport,
    observed_runs: usize,
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        worker_threads: 2,
        max_blocking_threads: 2,
        shutdown_timeout: Duration::from_secs(5),
        thread_name: "rocketmq-scheduled-bench".to_string(),
        ..RuntimeConfig::default()
    }
}

fn run_scheduled_case(mode: ScheduleMode, period: Duration, run_for: Duration) -> ScheduledBenchOutput {
    let owner = RuntimeOwner::new(runtime_config()).expect("runtime owner should start");
    let context = owner.context().clone();
    let observed_runs = Arc::new(AtomicUsize::new(0));

    owner.block_on(async move {
        let scheduled = context.service_context("bench.scheduler").scheduled_tasks("scheduled");
        match mode {
            ScheduleMode::FixedDelay => {
                let runs = observed_runs.clone();
                scheduled
                    .schedule_fixed_delay(ScheduledTaskConfig::fixed_delay("fixed-delay", period), move || {
                        let runs = runs.clone();
                        async move {
                            runs.fetch_add(1, Ordering::Relaxed);
                            tokio::task::yield_now().await;
                        }
                    })
                    .expect("fixed-delay scheduled task should start");
            }
            ScheduleMode::FixedRateNoOverlap => {
                let runs = observed_runs.clone();
                scheduled
                    .schedule_fixed_rate_no_overlap(
                        ScheduledTaskConfig::fixed_rate_no_overlap("fixed-rate-no-overlap", period),
                        move || {
                            let runs = runs.clone();
                            async move {
                                runs.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(period * 4).await;
                            }
                        },
                    )
                    .expect("fixed-rate no-overlap scheduled task should start");
            }
            ScheduleMode::FixedRateAllowOverlap => {
                let runs = observed_runs.clone();
                scheduled
                    .schedule_fixed_rate(
                        ScheduledTaskConfig::fixed_rate("fixed-rate-overlap", period),
                        move || {
                            let runs = runs.clone();
                            async move {
                                runs.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(period * 4).await;
                            }
                        },
                    )
                    .expect("fixed-rate scheduled task should start");
            }
        }

        tokio::time::sleep(run_for).await;
        let snapshot = scheduled
            .snapshot()
            .into_iter()
            .next()
            .expect("scheduled task snapshot should be available");
        let report = context.shutdown_tasks(Duration::from_secs(5)).await;

        assert_eq!(snapshot.mode, mode);
        assert!(snapshot.runs > 0, "{snapshot:?}");
        assert_eq!(report.leaked, 0, "{}", report.to_json());
        assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());

        match mode {
            ScheduleMode::FixedDelay => {}
            ScheduleMode::FixedRateNoOverlap => {
                assert!(snapshot.skips > 0, "{snapshot:?}");
                assert_eq!(snapshot.overlaps, 0, "{snapshot:?}");
            }
            ScheduleMode::FixedRateAllowOverlap => {
                assert_eq!(snapshot.skips, 0, "{snapshot:?}");
                assert!(snapshot.overlaps > 0, "{snapshot:?}");
            }
        }

        ScheduledBenchOutput {
            snapshot,
            report,
            observed_runs: observed_runs.load(Ordering::Relaxed),
        }
    })
}

fn write_scheduled_report_artifact() {
    let period = Duration::from_millis(10);
    let run_for = Duration::from_millis(90);
    let cases = [
        run_scheduled_case(ScheduleMode::FixedDelay, period, run_for),
        run_scheduled_case(ScheduleMode::FixedRateNoOverlap, period, run_for),
        run_scheduled_case(ScheduleMode::FixedRateAllowOverlap, period, run_for),
    ];

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-runtime should live below workspace root")
        .to_path_buf();
    let output_dir = workspace_root.join("target/runtime-baseline/prototype");
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "scheduled_task_group",
        "generated_at_unix_ms": generated_at_unix_ms,
        "period_ms": period.as_millis(),
        "run_for_ms": run_for.as_millis(),
        "cases": cases.iter().map(|case| {
            serde_json::json!({
                "mode": case.snapshot.mode,
                "observed_runs": case.observed_runs,
                "snapshot": case.snapshot,
                "healthy": case.report.is_healthy(),
                "shutdown_report": case.report,
            })
        }).collect::<Vec<_>>(),
    });
    let path = output_dir.join("scheduled-task-group-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("scheduled benchmark artifact should serialize"),
    )
    .expect("scheduled benchmark artifact should be written");
}

fn bench_scheduled_task_group(criterion: &mut Criterion) {
    write_scheduled_report_artifact();

    let period = Duration::from_millis(10);
    let run_for = Duration::from_millis(90);
    let mut group = criterion.benchmark_group("scheduled_task_group");
    for mode in [
        ScheduleMode::FixedDelay,
        ScheduleMode::FixedRateNoOverlap,
        ScheduleMode::FixedRateAllowOverlap,
    ] {
        group.bench_with_input(
            BenchmarkId::new("run_shutdown", format!("{mode:?}")),
            &mode,
            |bencher, mode| {
                bencher.iter(|| {
                    let output = run_scheduled_case(black_box(*mode), period, run_for);
                    black_box(output.snapshot.runs);
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
    targets = bench_scheduled_task_group
}
criterion_main!(benches);
