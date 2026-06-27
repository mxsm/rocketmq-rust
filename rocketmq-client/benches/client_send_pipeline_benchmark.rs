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

//! Broker-free producer send pipeline baselines.
//!
//! These benchmarks measure local work performed before a request reaches the
//! network: message construction, request command construction, callback
//! dispatch, and async backpressure permit acquisition.

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::producer::send_callback::ArcSendCallback;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tokio::sync::Semaphore;

#[derive(Debug)]
struct AsyncSchedulingOutput {
    task_count: usize,
    elapsed: Duration,
    p99_latency: Duration,
    submitted_tasks: usize,
}

fn build_message(body_size: usize) -> Message {
    Message::builder()
        .topic("BenchmarkTopic")
        .tags("BenchmarkTag")
        .keys(vec!["benchmark-key".to_string()])
        .body(vec![b'x'; body_size])
        .build()
        .expect("benchmark message should be valid")
}

fn build_message_with_properties(body_size: usize, property_count: usize) -> Message {
    let mut message = Message::builder()
        .topic("BenchmarkTopic")
        .body(vec![b'x'; body_size])
        .build()
        .expect("benchmark message should be valid");
    for index in 0..property_count {
        MessageAccessor::put_property(
            &mut message,
            CheetahString::from_string(format!("property-key-{index}")),
            CheetahString::from_string(format!("property-value-{index}")),
        );
    }
    message
}

fn build_send_header(message: &Message) -> SendMessageRequestHeader {
    SendMessageRequestHeader {
        producer_group: CheetahString::from_static_str("benchmark-producer-group"),
        topic: message.topic().clone(),
        default_topic: CheetahString::from_static_str("TBW102"),
        default_topic_queue_nums: 4,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: message.get_flag(),
        properties: Some(MessageDecoder::message_properties_to_string(message.get_properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        batch: Some(false),
        ..Default::default()
    }
}

fn build_send_request(mut message: Message) -> RemotingCommand {
    MessageClientIDSetter::set_uniq_id(&mut message);
    let header = build_send_header(&message);
    let body = message.get_body().cloned().unwrap_or_else(|| Bytes::from_static(b""));
    RemotingCommand::create_request_command(RequestCode::SendMessageV2, header).set_body(body)
}

fn percentile_duration(samples: &mut [Duration], percentile: usize) -> Duration {
    assert!(!samples.is_empty(), "percentile requires at least one sample");
    assert!(percentile <= 100, "percentile must be between 0 and 100");
    samples.sort_unstable();
    let rank = (samples.len() * percentile).div_ceil(100);
    let index = rank.saturating_sub(1);
    samples[index]
}

fn run_async_send_scheduling(
    runtime: &tokio::runtime::Runtime,
    task_count: usize,
    nested_api_spawn: bool,
) -> AsyncSchedulingOutput {
    runtime.block_on(async move {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let started_at = Instant::now();

        for _ in 0..task_count {
            let tx = tx.clone();
            let submitted_at = Instant::now();
            if nested_api_spawn {
                tokio::spawn(async move {
                    tokio::spawn(async move {
                        let _ = tx.send(submitted_at.elapsed());
                    });
                });
            } else {
                tokio::spawn(async move {
                    let _ = tx.send(submitted_at.elapsed());
                });
            }
        }
        drop(tx);

        let mut latencies = Vec::with_capacity(task_count);
        while let Some(latency) = rx.recv().await {
            latencies.push(latency);
            if latencies.len() == task_count {
                break;
            }
        }

        assert_eq!(latencies.len(), task_count);
        let elapsed = started_at.elapsed();
        let p99_latency = percentile_duration(&mut latencies, 99);
        AsyncSchedulingOutput {
            task_count,
            elapsed,
            p99_latency,
            submitted_tasks: if nested_api_spawn { task_count * 2 } else { task_count },
        }
    })
}

fn scheduling_output_json(output: &AsyncSchedulingOutput) -> serde_json::Value {
    serde_json::json!({
        "task_count": output.task_count,
        "submitted_tasks": output.submitted_tasks,
        "elapsed_us": output.elapsed.as_micros(),
        "p99_latency_us": output.p99_latency.as_micros(),
    })
}

fn write_async_send_scheduling_report_artifact(runtime: &tokio::runtime::Runtime) {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-client should live below workspace root")
        .to_path_buf();
    let output_dir = workspace_root.join("target/runtime-baseline/prototype");
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let cases = [10_000usize, 100_000]
        .into_iter()
        .map(|task_count| {
            let single = run_async_send_scheduling(runtime, task_count, false);
            let nested = run_async_send_scheduling(runtime, task_count, true);
            let submitted_task_reduction_percent =
                100.0 * (nested.submitted_tasks - single.submitted_tasks) as f64 / nested.submitted_tasks as f64;
            serde_json::json!({
                "task_count": task_count,
                "single_spawn": scheduling_output_json(&single),
                "nested_spawn": scheduling_output_json(&nested),
                "submitted_task_reduction_percent": submitted_task_reduction_percent,
            })
        })
        .collect::<Vec<_>>();
    let payload = serde_json::json!({
        "case": "client_send_pipeline.async_send_scheduling_layers",
        "generated_at_unix_ms": generated_at_unix_ms,
        "cases": cases,
    });
    let path = output_dir.join("client-async-send-scheduling-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("async send scheduling artifact should serialize"),
    )
    .expect("async send scheduling artifact should be written");
}

fn bench_message_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_send_pipeline/message_construction");

    for body_size in [128usize, 1024, 16 * 1024, 128 * 1024] {
        group.throughput(Throughput::Bytes(body_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &body_size, |b, &body_size| {
            b.iter(|| black_box(build_message(body_size)));
        });
    }

    group.finish();
}

fn bench_request_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_send_pipeline/request_construction");

    for body_size in [128usize, 1024, 16 * 1024, 128 * 1024] {
        group.throughput(Throughput::Bytes(body_size as u64));
        let message = build_message(body_size);
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &message, |b, message| {
            b.iter_batched(
                || message.clone(),
                |message| black_box(build_send_request(message)),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_send_header_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_send_pipeline/send_header_construction");
    group.throughput(Throughput::Elements(1));

    for property_count in [0usize, 1, 4, 16] {
        let message = build_message_with_properties(128, property_count);
        group.bench_with_input(BenchmarkId::from_parameter(property_count), &message, |b, message| {
            b.iter(|| black_box(build_send_header(black_box(message))));
        });
    }

    group.finish();
}

fn bench_async_backpressure_envelope(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let mut group = c.benchmark_group("client_send_pipeline/async_backpressure_envelope");

    for body_size in [128usize, 1024, 16 * 1024, 128 * 1024] {
        group.throughput(Throughput::Bytes(body_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(body_size), &body_size, |b, &body_size| {
            let send_num = Arc::new(Semaphore::new(1024));
            let send_size = Arc::new(Semaphore::new(1024 * 1024));
            b.to_async(&runtime).iter(|| {
                let send_num = send_num.clone();
                let send_size = send_size.clone();
                async move {
                    let num_permit = send_num
                        .acquire_owned()
                        .await
                        .expect("benchmark semaphore should be open");
                    let size_permit = send_size
                        .acquire_many_owned(body_size as u32)
                        .await
                        .expect("benchmark semaphore should have capacity");
                    black_box((&num_permit, &size_permit));
                }
            });
        });
    }

    group.finish();
}

fn bench_callback_dispatch(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let send_result = SendResult::default();
    let callback: ArcSendCallback = Arc::new(|result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
        black_box(result.is_some());
        black_box(error.is_some());
    });

    let mut group = c.benchmark_group("client_send_pipeline/callback_dispatch");
    group.throughput(Throughput::Elements(1));

    group.bench_function("no_callback", |b| {
        b.iter(|| {
            let callback: Option<ArcSendCallback> = None;
            if let Some(callback) = callback {
                callback.on_success(black_box(&send_result));
            }
        });
    });

    group.bench_function("direct_callback", |b| {
        b.iter(|| {
            callback.on_success(black_box(&send_result));
        });
    });

    group.bench_function("executor_callback", |b| {
        b.to_async(&runtime).iter(|| {
            let callback = callback.clone();
            let send_result = send_result.clone();
            async move {
                let handle = tokio::spawn(async move {
                    callback.on_success(&send_result);
                });
                handle.await.expect("callback task should complete");
            }
        });
    });

    group.finish();
}

fn bench_async_send_scheduling_layers(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    write_async_send_scheduling_report_artifact(&runtime);

    let mut group = c.benchmark_group("client_send_pipeline/async_send_scheduling_layers");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    for task_count in [10_000usize, 100_000] {
        group.throughput(Throughput::Elements(task_count as u64));
        for (label, nested_api_spawn) in [("single_spawn", false), ("nested_spawn", true)] {
            group.bench_with_input(
                BenchmarkId::new(label, task_count),
                &(task_count, nested_api_spawn),
                |b, &(task_count, nested_api_spawn)| {
                    b.iter(|| {
                        let output = run_async_send_scheduling(&runtime, task_count, nested_api_spawn);
                        black_box((
                            output.task_count,
                            output.elapsed,
                            output.p99_latency,
                            output.submitted_tasks,
                        ));
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_message_construction,
    bench_request_construction,
    bench_send_header_construction,
    bench_async_backpressure_envelope,
    bench_callback_dispatch,
    bench_async_send_scheduling_layers,
);
criterion_main!(benches);
