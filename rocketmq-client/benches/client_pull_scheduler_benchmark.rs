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

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::ProcessQueue;
use rocketmq_client_rust::consumer::consumer_impl::pull_message_service::PullMessageService;
use rocketmq_client_rust::consumer::consumer_impl::pull_message_service::PullMessageServiceShardSnapshot;
use rocketmq_client_rust::consumer::consumer_impl::pull_request::PullRequest;
use rocketmq_client_rust::factory::mq_client_instance::MQClientInstance;
use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Serialize;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::Instant as TokioInstant;

#[derive(Debug, Serialize)]
struct PullSchedulerBenchOutput {
    request_count: usize,
    submitted_tasks: usize,
    p99_late_us: u128,
}

#[derive(Debug, Serialize)]
struct ShardedPullSchedulerBenchOutput {
    request_count: usize,
    shard_count: usize,
    worker_task_count: usize,
    submitted_count: u64,
    processed_count: u64,
    rejected_count: u64,
    pending_count: usize,
    elapsed_us: u128,
}

fn p99(mut values: Vec<u128>) -> u128 {
    values.sort_unstable();
    let index = values.len().saturating_mul(99).div_ceil(100).saturating_sub(1);
    values[index]
}

fn create_pull_request(index: usize) -> PullRequest {
    PullRequest::new(
        CheetahString::from_static_str("benchmark_group"),
        MessageQueue::from_parts(
            format!("benchmark_topic_{}", index % 128),
            "benchmark_broker",
            (index % 32) as i32,
        ),
        Arc::new(ProcessQueue::new()),
        index as i64,
    )
}

async fn run_one_shot_sleep_tasks(request_count: usize, delay: Duration) -> PullSchedulerBenchOutput {
    let (tx, mut rx) = mpsc::unbounded_channel();

    for _ in 0..request_count {
        let tx = tx.clone();
        let deadline = TokioInstant::now() + delay;
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let late_us = TokioInstant::now()
                .checked_duration_since(deadline)
                .unwrap_or_default()
                .as_micros();
            let _ = tx.send(late_us);
        });
    }
    drop(tx);

    let mut late_us = Vec::with_capacity(request_count);
    while let Some(value) = rx.recv().await {
        late_us.push(value);
        if late_us.len() == request_count {
            break;
        }
    }

    PullSchedulerBenchOutput {
        request_count,
        submitted_tasks: request_count,
        p99_late_us: p99(late_us),
    }
}

async fn run_shared_scheduler_tasks(request_count: usize, delay: Duration) -> PullSchedulerBenchOutput {
    let service = PullMessageService::new();
    let (tx, mut rx) = mpsc::unbounded_channel();

    for _ in 0..request_count {
        let tx = tx.clone();
        let deadline = TokioInstant::now() + delay;
        service.execute_task_later(
            move || {
                let late_us = TokioInstant::now()
                    .checked_duration_since(deadline)
                    .unwrap_or_default()
                    .as_micros();
                let _ = tx.send(late_us);
            },
            delay.as_millis() as u64,
        );
    }
    drop(tx);

    let mut late_us = Vec::with_capacity(request_count);
    while let Some(value) = rx.recv().await {
        late_us.push(value);
        if late_us.len() == request_count {
            break;
        }
    }

    let snapshot = service.delayed_scheduler_snapshot();
    service
        .shutdown(1_000)
        .await
        .expect("shared scheduler benchmark service should shutdown");

    PullSchedulerBenchOutput {
        request_count,
        submitted_tasks: snapshot.scheduler_task_count,
        p99_late_us: p99(late_us),
    }
}

async fn wait_for_sharded_pull_requests(
    service: &PullMessageService,
    request_count: usize,
) -> PullMessageServiceShardSnapshot {
    let deadline = TokioInstant::now() + Duration::from_secs(5);
    loop {
        let snapshot = service.shard_snapshot().await;
        if snapshot.processed_count + snapshot.rejected_count >= request_count as u64 {
            return snapshot;
        }
        assert!(
            TokioInstant::now() < deadline,
            "sharded pull scheduler benchmark timed out: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn run_sharded_pull_workers(request_count: usize, shard_count: usize) -> ShardedPullSchedulerBenchOutput {
    let client_config = ClientConfig {
        namesrv_addr: None,
        pull_message_service_shards: shard_count,
        ..Default::default()
    };
    let mut instance = MQClientInstance::new_arc(client_config, 0, "client-pull-scheduler-sharded-bench", None);
    let mut service = PullMessageService::with_capacity_and_shards(request_count.next_power_of_two(), shard_count);
    service
        .start(instance.clone())
        .await
        .expect("sharded pull scheduler benchmark service should start");

    let started_at = Instant::now();
    for index in 0..request_count {
        service
            .execute_pull_request_immediately(create_pull_request(index))
            .await;
    }
    let snapshot = wait_for_sharded_pull_requests(&service, request_count).await;
    let elapsed_us = started_at.elapsed().as_micros();

    service
        .shutdown(1_000)
        .await
        .expect("sharded pull scheduler benchmark service should shutdown");
    instance.shutdown().await;

    ShardedPullSchedulerBenchOutput {
        request_count,
        shard_count: snapshot.shard_count,
        worker_task_count: snapshot.worker_task_count,
        submitted_count: snapshot.submitted_count,
        processed_count: snapshot.processed_count,
        rejected_count: snapshot.rejected_count,
        pending_count: snapshot.pending_count,
        elapsed_us,
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-client should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/runtime-baseline/prototype")
}

fn write_pull_scheduler_report_artifact(runtime: &Runtime) {
    let request_count = 1000usize;
    let delay = Duration::from_millis(20);
    let one_shot = runtime.block_on(run_one_shot_sleep_tasks(request_count, delay));
    let shared_scheduler = runtime.block_on(run_shared_scheduler_tasks(request_count, delay));
    let sharded_pull_workers = runtime.block_on(run_sharded_pull_workers(request_count, 4));
    let output_dir = benchmark_artifact_dir();
    std::fs::create_dir_all(&output_dir).expect("pull scheduler benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "client_pull_scheduler_delayed_1000",
        "generated_at_unix_ms": generated_at_unix_ms,
        "one_shot_sleep_tasks": one_shot,
        "shared_scheduler": shared_scheduler,
        "sharded_pull_workers": sharded_pull_workers,
    });
    let path = output_dir.join("client-pull-scheduler-report.json");
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("pull scheduler artifact should serialize"),
    )
    .expect("pull scheduler artifact should be written");
}

fn bench_pull_scheduler(c: &mut Criterion) {
    let runtime = Runtime::new().expect("pull scheduler benchmark runtime should start");
    write_pull_scheduler_report_artifact(&runtime);

    let mut group = c.benchmark_group("client_pull_scheduler/delayed_1000");
    let request_count = 1000usize;
    let delay = Duration::from_millis(20);
    group.throughput(Throughput::Elements(request_count as u64));

    for (label, shared_scheduler) in [("OneShotSleepTasks", false), ("SharedScheduler", true)] {
        group.bench_with_input(
            BenchmarkId::new(label, request_count),
            &(request_count, shared_scheduler),
            |bencher, &(request_count, shared_scheduler)| {
                bencher.to_async(&runtime).iter(|| async move {
                    let output = if shared_scheduler {
                        run_shared_scheduler_tasks(request_count, delay).await
                    } else {
                        run_one_shot_sleep_tasks(request_count, delay).await
                    };
                    assert_eq!(output.request_count, request_count);
                    black_box(output.submitted_tasks);
                    black_box(output.p99_late_us);
                });
            },
        );
    }

    group.bench_with_input(
        BenchmarkId::new("ShardedPullWorkers", request_count),
        &request_count,
        |bencher, &request_count| {
            bencher.to_async(&runtime).iter(|| async move {
                let output = run_sharded_pull_workers(request_count, 4).await;
                assert_eq!(output.request_count, request_count);
                assert_eq!(output.processed_count, request_count as u64);
                black_box(output.worker_task_count);
                black_box(output.elapsed_us);
            });
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_pull_scheduler
}
criterion_main!(benches);
