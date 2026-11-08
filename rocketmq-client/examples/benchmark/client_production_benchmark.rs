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

#![recursion_limit = "256"]

#[path = "../support.rs"]
mod support;

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use parking_lot::Mutex;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::ClientError;
use rocketmq_client_rust::ClientResult;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::SendResult;
use rocketmq_client_rust::SendStatus;
use rocketmq_client_rust::SessionCredentials;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::message_single::Message;
use serde::Serialize;
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Sync,
    Async,
    Batch,
    LitePull,
}

impl Scenario {
    fn parse(value: &str) -> ClientResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "sync" | "producersync" => Ok(Self::Sync),
            "async" | "producerasync" => Ok(Self::Async),
            "batch" | "producerbatch" => Ok(Self::Batch),
            "lite-pull" | "litepull" | "litepullbenchmark" => Ok(Self::LitePull),
            other => Err(ClientError::illegal_argument(format!(
                "unknown scenario '{other}', expected sync, async, batch, or lite-pull"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Async => "async",
            Self::Batch => "batch",
            Self::LitePull => "lite-pull",
        }
    }

    fn operation(self) -> Operation {
        match self {
            Self::Sync | Self::Async | Self::Batch => Operation::Send,
            Self::LitePull => Operation::Consume,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Send,
    Consume,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::Send => "Send",
            Self::Consume => "Consume",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::Consume => "consume",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    namesrv_addr: String,
    topic: String,
    producer_group: String,
    scenario: Scenario,
    message_count: usize,
    message_size: usize,
    batch_size: usize,
    /// Async sends that may await their callbacks at once.
    inflight: usize,
    timeout_ms: u64,
    use_tls: bool,
    access_key: Option<String>,
    secret_key: Option<String>,
    security_token: Option<String>,
    output_json: Option<PathBuf>,
    run_id: String,
}

/// Default async window. A client instance admits at most 512 pending requests and rejects the
/// rest at once instead of queueing them, so the window stays below that with room for heartbeats
/// and route queries.
const DEFAULT_ASYNC_INFLIGHT: usize = 256;
/// Upper bound on one LitePull seed batch, well below the Broker's 4 MiB message limit.
const SEED_BATCH_MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Default)]
struct Stats {
    success_count: usize,
    send_failed_count: usize,
    response_failed_count: usize,
    /// Async messages without a completed callback when the wait deadline passed, including any
    /// left unsubmitted because the window never freed a slot. They also count as send failures.
    unfinished_count: usize,
    latency_us: Vec<u64>,
}

/// Wall-clock bounds of the run, so external samplers can attribute process CPU to the measured
/// phase only.
#[derive(Debug, Default, Clone, Copy, Serialize, PartialEq)]
struct Phases {
    seed_started_epoch_ms: Option<u128>,
    measure_started_epoch_ms: u128,
    measure_finished_epoch_ms: u128,
}

/// The measured window: it starts after clients are started (and LitePull messages are seeded).
struct MeasureWindow {
    started: Instant,
    started_epoch_ms: u128,
}

impl MeasureWindow {
    fn start() -> Self {
        let window = Self {
            started: Instant::now(),
            started_epoch_ms: epoch_ms(),
        };
        println!("[Phase] measure-start epoch_ms={}", window.started_epoch_ms);
        window
    }

    fn finish(self, seed_started_epoch_ms: Option<u128>) -> (Duration, Phases) {
        let elapsed = self.started.elapsed();
        let finished = epoch_ms();
        println!("[Phase] measure-end epoch_ms={finished}");
        (
            elapsed,
            Phases {
                seed_started_epoch_ms,
                measure_started_epoch_ms: self.started_epoch_ms,
                measure_finished_epoch_ms: finished,
            },
        )
    }
}

#[derive(Debug, Serialize, PartialEq)]
struct LatencySummary {
    samples: usize,
    average: f64,
    p50: u64,
    p95: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

#[derive(Debug, Serialize, PartialEq)]
struct BenchmarkTarget<'a> {
    namesrv_addr: &'a str,
    topic: &'a str,
}

#[derive(Debug, Serialize, PartialEq)]
struct BenchmarkWorkload {
    message_count: usize,
    message_size_bytes: usize,
    batch_size: usize,
}

#[derive(Debug, Serialize, PartialEq)]
struct BenchmarkResult {
    duration_us: u64,
    success_count: usize,
    send_failed_count: usize,
    response_failed_count: usize,
    unfinished_count: usize,
    throughput_messages_per_second: f64,
    payload_mib_per_second: f64,
    latency_us: LatencySummary,
}

#[derive(Debug, Serialize, PartialEq)]
struct BenchmarkReport<'a> {
    schema_version: u8,
    artifact_kind: &'static str,
    run_id: &'a str,
    generated_at_epoch_ms: u128,
    scenario: &'static str,
    operation: &'static str,
    target: BenchmarkTarget<'a>,
    workload: BenchmarkWorkload,
    /// The async in-flight window; absent for the other scenarios.
    async_inflight: Option<usize>,
    phases: Phases,
    result: BenchmarkResult,
}

#[tokio::main]
pub async fn main() -> ClientResult<()> {
    let example_runtime = support::ExampleClientRuntime::try_new("client-production-benchmark")?;
    let client_runtime = example_runtime.client_runtime();
    let config = Config::parse()?;
    let body = vec![b'a'; config.message_size];

    println!(
        "RocketMQ Rust client production benchmark namesrv={} topic={} scenario={} messageCount={} messageSize={} \
         batchSize={} inflight={} tls={} acl={}",
        config.namesrv_addr,
        config.topic,
        config.scenario.as_str(),
        config.message_count,
        config.message_size,
        config.batch_size,
        config.inflight,
        config.use_tls,
        config.access_key.is_some()
    );

    let (stats, elapsed, phases) = match config.scenario {
        Scenario::Sync | Scenario::Async | Scenario::Batch => {
            let mut producer = build_producer(client_runtime.clone(), &config)?;
            producer.start().await?;
            let window = MeasureWindow::start();
            let stats = match config.scenario {
                Scenario::Sync => run_sync(&mut producer, &config, &body).await,
                Scenario::Async => run_async(&mut producer, &config, &body).await,
                Scenario::Batch => run_batch(&mut producer, &config, &body).await,
                Scenario::LitePull => unreachable!("lite-pull is handled by the outer match"),
            };
            let (elapsed, phases) = window.finish(None);
            producer.shutdown().await;
            (stats?, elapsed, phases)
        }
        Scenario::LitePull => run_lite_pull(client_runtime, &config, &body).await?,
    };

    print_complete_summary(&stats, elapsed, config.scenario.operation());
    if let Some(output_json) = &config.output_json {
        write_json_report(output_json, &config, &stats, elapsed, phases)?;
    }
    example_runtime.shutdown().await;

    Ok(())
}

fn build_producer(client_runtime: Arc<ClientRuntime>, config: &Config) -> ClientResult<DefaultMQProducer> {
    let mut builder = DefaultMQProducer::builder(client_runtime.clone())
        .producer_group(config.producer_group.clone())
        .name_server_addr(config.namesrv_addr.clone())
        .send_msg_timeout(config.timeout_ms as u32)
        .use_tls(config.use_tls);

    if let (Some(access_key), Some(secret_key)) = (&config.access_key, &config.secret_key) {
        let credentials = match &config.security_token {
            Some(security_token) => {
                SessionCredentials::with_token(access_key.as_str(), secret_key.as_str(), security_token.as_str())
            }
            None => SessionCredentials::with_keys(access_key.as_str(), secret_key.as_str()),
        };
        builder = builder.rpc_hook(Arc::new(AclClientRPCHook::new(credentials)));
    }

    Ok(builder.build())
}

fn build_lite_pull_consumer(
    client_runtime: Arc<ClientRuntime>,
    config: &Config,
) -> ClientResult<DefaultLitePullConsumer> {
    let broker_suspend_ms = lite_pull_broker_suspend_ms(config.timeout_ms);
    let pull_timeout_ms = broker_suspend_ms.saturating_add(1_000);
    let mut builder = DefaultLitePullConsumer::builder(client_runtime.clone())
        .consumer_group(unique_consumer_group())
        .name_server_addr(config.namesrv_addr.clone())
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .pull_batch_size(config.batch_size.min(i32::MAX as usize) as i32)
        .broker_suspend_max_time_millis(broker_suspend_ms)
        .consumer_timeout_millis_when_suspend(pull_timeout_ms)
        .consumer_pull_timeout_millis(pull_timeout_ms)
        .poll_timeout_millis(config.timeout_ms.min(1_000))
        .auto_commit(false)
        .use_tls(config.use_tls);

    if let (Some(access_key), Some(secret_key)) = (&config.access_key, &config.secret_key) {
        let credentials = match &config.security_token {
            Some(security_token) => {
                SessionCredentials::with_token(access_key.as_str(), secret_key.as_str(), security_token.as_str())
            }
            None => SessionCredentials::with_keys(access_key.as_str(), secret_key.as_str()),
        };
        builder = builder.rpc_hook(Arc::new(AclClientRPCHook::new(credentials)));
    }

    builder.build()
}

fn lite_pull_broker_suspend_ms(operation_timeout_ms: u64) -> u64 {
    operation_timeout_ms.clamp(100, 1_000)
}

async fn run_sync(producer: &mut DefaultMQProducer, config: &Config, body: &[u8]) -> ClientResult<Stats> {
    let mut stats = Stats::default();
    for _ in 0..config.message_count {
        let begin = Instant::now();
        match producer
            .send_with_timeout(message(&config.topic, "RustSyncBenchmark", body), config.timeout_ms)
            .await
        {
            Ok(Some(send_result)) if send_result.send_status == SendStatus::SendOk => {
                record_success(&mut stats, begin.elapsed());
            }
            Ok(Some(_)) => {
                record_response_failure(&mut stats, begin.elapsed());
            }
            Ok(None) => {
                record_send_failure(&mut stats, begin.elapsed());
            }
            Err(error) => {
                record_send_failure(&mut stats, begin.elapsed());
                eprintln!("send failed: {error}");
            }
        }
    }
    Ok(stats)
}

async fn run_async(producer: &mut DefaultMQProducer, config: &Config, body: &[u8]) -> ClientResult<Stats> {
    let success_count = Arc::new(AtomicUsize::new(0));
    let send_failed_count = Arc::new(AtomicUsize::new(0));
    let response_failed_count = Arc::new(AtomicUsize::new(0));
    let latency_us = Arc::new(Mutex::new(Vec::with_capacity(config.message_count)));
    // Without a window every send is submitted at once, so callbacks queue behind the whole run
    // and the slowest ones miss the wait deadline; that measures overload, not throughput.
    let window = Arc::new(Semaphore::new(config.inflight));
    let permit_wait = Duration::from_millis(config.timeout_ms.saturating_mul(2));

    for _ in 0..config.message_count {
        let Ok(Ok(permit)) = tokio::time::timeout(permit_wait, Arc::clone(&window).acquire_owned()).await else {
            eprintln!("async send window did not free a slot within {permit_wait:?}; stopping submission");
            break;
        };
        // The callback is `Fn` and may be retained after it runs, so the slot is released
        // explicitly on completion instead of when the closure drops.
        let slot = Arc::new(Mutex::new(Some(permit)));
        let slot_inner = Arc::clone(&slot);
        let begin = Instant::now();
        let success_count_inner = Arc::clone(&success_count);
        let send_failed_count_inner = Arc::clone(&send_failed_count);
        let response_failed_count_inner = Arc::clone(&response_failed_count);
        let latency_us_inner = Arc::clone(&latency_us);
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_inner = Arc::clone(&completed);

        if let Err(error) = producer
            .send_with_callback_timeout(
                message(&config.topic, "RustAsyncBenchmark", body),
                move |result: Option<&SendResult>, error: Option<&ClientError>| {
                    if completed_inner.swap(true, Ordering::AcqRel) {
                        return;
                    }
                    slot_inner.lock().take();
                    latency_us_inner.lock().push(elapsed_us_u64(begin.elapsed()));

                    match (result, error) {
                        (Some(send_result), None) if send_result.send_status == SendStatus::SendOk => {
                            success_count_inner.fetch_add(1, Ordering::Relaxed);
                        }
                        (Some(_), None) => {
                            response_failed_count_inner.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            send_failed_count_inner.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                },
                config.timeout_ms,
            )
            .await
        {
            if !completed.swap(true, Ordering::AcqRel) {
                slot.lock().take();
                send_failed_count.fetch_add(1, Ordering::Relaxed);
                latency_us.lock().push(elapsed_us_u64(begin.elapsed()));
            }
            eprintln!("async send failed before callback: {error}");
        }
    }

    let deadline = Instant::now() + permit_wait;
    while observed_count(&success_count, &send_failed_count, &response_failed_count) < config.message_count
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let observed = observed_count(&success_count, &send_failed_count, &response_failed_count);
    let unfinished_count = config.message_count.saturating_sub(observed);
    if unfinished_count > 0 {
        send_failed_count.fetch_add(unfinished_count, Ordering::Relaxed);
        latency_us.lock().extend(std::iter::repeat_n(
            config.timeout_ms.saturating_mul(2).saturating_mul(1_000),
            unfinished_count,
        ));
    }

    let latency_us = latency_us.lock().clone();
    Ok(Stats {
        success_count: success_count.load(Ordering::Relaxed),
        send_failed_count: send_failed_count.load(Ordering::Relaxed),
        response_failed_count: response_failed_count.load(Ordering::Relaxed),
        unfinished_count,
        latency_us,
    })
}

async fn run_batch(producer: &mut DefaultMQProducer, config: &Config, body: &[u8]) -> ClientResult<Stats> {
    let mut stats = Stats::default();
    let mut remaining = config.message_count;
    while remaining > 0 {
        let batch_len = remaining.min(config.batch_size.max(1));
        let messages = (0..batch_len)
            .map(|_| message(&config.topic, "RustBatchBenchmark", body))
            .collect::<Vec<_>>();
        let begin = Instant::now();
        match producer.send_batch_with_timeout(messages, config.timeout_ms).await {
            Ok(send_result) if send_result.send_status == SendStatus::SendOk => {
                let elapsed = begin.elapsed();
                for _ in 0..batch_len {
                    record_success(&mut stats, elapsed);
                }
            }
            Ok(_) => {
                let elapsed = begin.elapsed();
                for _ in 0..batch_len {
                    record_response_failure(&mut stats, elapsed);
                }
            }
            Err(error) => {
                let elapsed = begin.elapsed();
                for _ in 0..batch_len {
                    record_send_failure(&mut stats, elapsed);
                }
                eprintln!("batch send failed: {error}");
            }
        }
        remaining -= batch_len;
    }
    Ok(stats)
}

async fn run_lite_pull(
    client_runtime: Arc<ClientRuntime>,
    config: &Config,
    body: &[u8],
) -> ClientResult<(Stats, Duration, Phases)> {
    let tag = format!("RustLitePullBenchmark-{}", config.run_id);

    let mut producer = build_producer(client_runtime.clone(), config)?;
    let consumer = build_lite_pull_consumer(client_runtime, config)?;

    producer.start().await?;
    consumer
        .set_sub_expression_for_assign(&config.topic, tag.as_str())
        .await?;
    consumer.start().await?;

    let queues = consumer.fetch_message_queues(&config.topic).await?;
    let queue = first_queue(queues)?;
    let seed_started_epoch_ms = epoch_ms();
    seed_lite_pull_messages(&mut producer, config, body, tag.as_str(), &queue).await?;
    consumer.assign(vec![queue]).await?;

    let window = MeasureWindow::start();
    let stats = consume_lite_pull_messages(&consumer, config, tag.as_str(), window.started).await;
    let (elapsed, phases) = window.finish(Some(seed_started_epoch_ms));

    consumer.commit().await;
    consumer.shutdown().await;
    producer.shutdown().await;

    Ok((stats, elapsed, phases))
}

/// Seeds the queue in batches: one synchronous send per message made the seed phase dominate the
/// run (about 50 s of 55 s for 200,000 messages), although only the consume phase is measured.
async fn seed_lite_pull_messages(
    producer: &mut DefaultMQProducer,
    config: &Config,
    body: &[u8],
    tag: &str,
    queue: &MessageQueue,
) -> ClientResult<()> {
    let mut remaining = config.message_count;
    while remaining > 0 {
        let batch_len = seed_batch_len(remaining, config.batch_size, config.message_size);
        let messages = (0..batch_len)
            .map(|_| message(&config.topic, tag, body))
            .collect::<Vec<_>>();
        let send_result = producer
            .send_batch_to_queue_with_timeout(messages, queue.clone(), config.timeout_ms)
            .await?;
        if send_result.send_status != SendStatus::SendOk {
            return Err(ClientError::illegal_argument(format!(
                "LitePull seed send status was {:?}",
                send_result.send_status
            )));
        }
        remaining -= batch_len;
    }
    Ok(())
}

/// Messages in the next seed batch: up to `batch_size`, within [`SEED_BATCH_MAX_BYTES`] of bodies,
/// and always at least one.
fn seed_batch_len(remaining: usize, batch_size: usize, message_size: usize) -> usize {
    let by_bytes = (SEED_BATCH_MAX_BYTES / message_size.max(1)).max(1);
    remaining.min(batch_size.max(1)).min(by_bytes)
}

async fn consume_lite_pull_messages(
    consumer: &DefaultLitePullConsumer,
    config: &Config,
    tag: &str,
    start: Instant,
) -> Stats {
    let mut stats = Stats::default();
    let deadline = start + Duration::from_millis(config.timeout_ms.saturating_mul(2).max(1_000));
    let poll_timeout = config.timeout_ms.min(1_000);

    while stats.success_count < config.message_count && Instant::now() < deadline {
        let begin = Instant::now();
        let messages = consumer.poll_with_timeout(poll_timeout).await;
        if messages.is_empty() {
            continue;
        }

        let elapsed = begin.elapsed();
        for message_ext in messages {
            if stats.success_count >= config.message_count {
                break;
            }
            if message_matches_tag(&message_ext, tag) {
                record_success(&mut stats, elapsed);
            }
        }
    }

    if stats.success_count < config.message_count {
        stats.send_failed_count = config.message_count - stats.success_count;
    }

    stats
}

fn message(topic: &str, tag: &str, body: &[u8]) -> Message {
    Message::builder()
        .topic(topic)
        .tags(tag)
        .body_slice(body)
        .build_unchecked()
}

fn first_queue(queues: Vec<MessageQueue>) -> ClientResult<MessageQueue> {
    queues
        .into_iter()
        .min()
        .ok_or_else(|| ClientError::illegal_argument("LitePull benchmark found no message queues for topic"))
}

fn message_matches_tag(message_ext: &MessageExt, expected_tag: &str) -> bool {
    message_ext
        .get_tags()
        .map(|tag| tag.as_str() == expected_tag)
        .unwrap_or(false)
}

fn record_success(stats: &mut Stats, elapsed: Duration) {
    stats.success_count += 1;
    record_latency(stats, elapsed);
}

fn record_send_failure(stats: &mut Stats, elapsed: Duration) {
    stats.send_failed_count += 1;
    record_latency(stats, elapsed);
}

fn record_response_failure(stats: &mut Stats, elapsed: Duration) {
    stats.response_failed_count += 1;
    record_latency(stats, elapsed);
}

fn record_latency(stats: &mut Stats, elapsed: Duration) {
    stats.latency_us.push(elapsed_us_u64(elapsed));
}

fn print_complete_summary(stats: &Stats, elapsed: Duration, operation: Operation) {
    let total = stats.success_count + stats.send_failed_count + stats.response_failed_count;
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let tps = (stats.success_count as f64 / elapsed_secs).round() as u64;
    let latency = latency_summary(&stats.latency_us);
    let label = operation.label();

    println!(
        "[Complete] {} Total: {} | {} TPS: {} | Max RT(ms): {} | Average RT(ms): {:.3} | {} Failed: {} | Response \
         Failed: {}",
        label,
        total,
        label,
        tps,
        latency.max as f64 / 1_000.0,
        latency.average / 1_000.0,
        label,
        stats.send_failed_count,
        stats.response_failed_count
    );
}

fn write_json_report(
    path: &PathBuf,
    config: &Config,
    stats: &Stats,
    elapsed: Duration,
    phases: Phases,
) -> ClientResult<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|error| ClientError::internal("create benchmark report directory", error))?;
    }
    let report = benchmark_report(config, stats, elapsed, phases);
    let body = serde_json::to_vec_pretty(&report)
        .map_err(|error| ClientError::internal("serialize benchmark report", error))?;
    std::fs::write(path, body).map_err(|error| ClientError::internal("write benchmark report", error))
}

fn benchmark_report<'a>(config: &'a Config, stats: &Stats, elapsed: Duration, phases: Phases) -> BenchmarkReport<'a> {
    let elapsed_secs = elapsed.as_secs_f64().max(0.000_001);
    let successful_bytes = stats.success_count.saturating_mul(config.message_size);
    BenchmarkReport {
        schema_version: 1,
        artifact_kind: "rocketmq_message_path_measurement",
        run_id: &config.run_id,
        generated_at_epoch_ms: epoch_ms(),
        scenario: config.scenario.as_str(),
        operation: config.scenario.operation().as_str(),
        target: BenchmarkTarget {
            namesrv_addr: &config.namesrv_addr,
            topic: &config.topic,
        },
        workload: BenchmarkWorkload {
            message_count: config.message_count,
            message_size_bytes: config.message_size,
            batch_size: config.batch_size,
        },
        async_inflight: (config.scenario == Scenario::Async).then_some(config.inflight),
        phases,
        result: BenchmarkResult {
            duration_us: elapsed_us_u64(elapsed),
            success_count: stats.success_count,
            send_failed_count: stats.send_failed_count,
            response_failed_count: stats.response_failed_count,
            unfinished_count: stats.unfinished_count,
            throughput_messages_per_second: stats.success_count as f64 / elapsed_secs,
            payload_mib_per_second: successful_bytes as f64 / (1024.0 * 1024.0) / elapsed_secs,
            latency_us: latency_summary(&stats.latency_us),
        },
    }
}

fn latency_summary(samples: &[u64]) -> LatencySummary {
    if samples.is_empty() {
        return LatencySummary {
            samples: 0,
            average: 0.0,
            p50: 0,
            p95: 0,
            p99: 0,
            p999: 0,
            max: 0,
        };
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let total = sorted
        .iter()
        .fold(0_u128, |sum, value| sum.saturating_add(*value as u128));
    LatencySummary {
        samples: sorted.len(),
        average: total as f64 / sorted.len() as f64,
        p50: percentile(&sorted, 50, 100),
        p95: percentile(&sorted, 95, 100),
        p99: percentile(&sorted, 99, 100),
        p999: percentile(&sorted, 999, 1_000),
        max: sorted.last().copied().unwrap_or_default(),
    }
}

fn percentile(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    if sorted.is_empty() || denominator == 0 {
        return 0;
    }
    let rank = sorted.len().saturating_mul(numerator).saturating_add(denominator - 1) / denominator;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn observed_count(
    success_count: &AtomicUsize,
    send_failed_count: &AtomicUsize,
    response_failed_count: &AtomicUsize,
) -> usize {
    success_count.load(Ordering::Relaxed)
        + send_failed_count.load(Ordering::Relaxed)
        + response_failed_count.load(Ordering::Relaxed)
}

fn elapsed_us_u64(elapsed: Duration) -> u64 {
    elapsed.as_micros().try_into().unwrap_or(u64::MAX)
}

fn epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

impl Config {
    fn parse() -> ClientResult<Self> {
        let mut config = Self {
            namesrv_addr: env_or("ROCKETMQ_NAMESRV_ADDR", "127.0.0.1:9876"),
            topic: env_or("ROCKETMQ_TEST_TOPIC", "TopicTest"),
            producer_group: unique_group(),
            scenario: Scenario::Sync,
            message_count: 100,
            message_size: 128,
            batch_size: 16,
            inflight: DEFAULT_ASYNC_INFLIGHT,
            timeout_ms: 30_000,
            use_tls: env_flag("ROCKETMQ_ENABLE_TLS_SMOKE"),
            access_key: env_non_empty("ROCKETMQ_ACL_ACCESS_KEY"),
            secret_key: env_non_empty("ROCKETMQ_ACL_SECRET_KEY"),
            security_token: env_non_empty("ROCKETMQ_ACL_SECURITY_TOKEN"),
            output_json: None,
            run_id: unique_run_id(),
        };

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--namesrv" => config.namesrv_addr = next_arg(&mut args, "--namesrv")?,
                "--topic" => config.topic = next_arg(&mut args, "--topic")?,
                "--producer-group" => config.producer_group = next_arg(&mut args, "--producer-group")?,
                "--scenario" => config.scenario = Scenario::parse(next_arg(&mut args, "--scenario")?.as_str())?,
                "--message-count" => {
                    config.message_count =
                        parse_positive_usize(next_arg(&mut args, "--message-count")?, "--message-count")?;
                }
                "--message-size" => {
                    config.message_size =
                        parse_positive_usize(next_arg(&mut args, "--message-size")?, "--message-size")?;
                }
                "--batch-size" => {
                    config.batch_size = parse_positive_usize(next_arg(&mut args, "--batch-size")?, "--batch-size")?;
                }
                "--inflight" => {
                    config.inflight = parse_positive_usize(next_arg(&mut args, "--inflight")?, "--inflight")?;
                }
                "--timeout-ms" => {
                    config.timeout_ms = parse_positive_u64(next_arg(&mut args, "--timeout-ms")?, "--timeout-ms")?;
                }
                "--tls" => config.use_tls = true,
                "--acl" => {
                    let access_key = config.access_key.take().ok_or_else(|| {
                        ClientError::illegal_argument("--acl requires --access-key or ROCKETMQ_ACL_ACCESS_KEY")
                    })?;
                    let secret_key = config.secret_key.take().ok_or_else(|| {
                        ClientError::illegal_argument("--acl requires --secret-key or ROCKETMQ_ACL_SECRET_KEY")
                    })?;
                    config.access_key = Some(access_key);
                    config.secret_key = Some(secret_key);
                }
                "--access-key" => config.access_key = Some(next_arg(&mut args, "--access-key")?),
                "--secret-key" => config.secret_key = Some(next_arg(&mut args, "--secret-key")?),
                "--security-token" => config.security_token = Some(next_arg(&mut args, "--security-token")?),
                "--output-json" => {
                    config.output_json = Some(PathBuf::from(next_arg(&mut args, "--output-json")?));
                }
                "--run-id" => config.run_id = next_arg(&mut args, "--run-id")?,
                other => return Err(ClientError::illegal_argument(format!("unknown argument: {other}"))),
            }
        }

        if config.namesrv_addr.trim().is_empty() {
            return Err(ClientError::illegal_argument("--namesrv must not be blank"));
        }
        if config.topic.trim().is_empty() {
            return Err(ClientError::illegal_argument("--topic must not be blank"));
        }
        if config.run_id.trim().is_empty() {
            return Err(ClientError::illegal_argument("--run-id must not be blank"));
        }
        if config
            .output_json
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ClientError::illegal_argument("--output-json must not be blank"));
        }
        if config.access_key.is_some() != config.secret_key.is_some() {
            return Err(ClientError::illegal_argument(
                "ACL benchmark requires both access key and secret key",
            ));
        }

        Ok(config)
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, option: &str) -> ClientResult<String> {
    args.next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| ClientError::illegal_argument(format!("{option} requires a value")))
}

fn parse_positive_usize(value: String, option: &str) -> ClientResult<usize> {
    value
        .parse::<usize>()
        .ok()
        .filter(|parsed| *parsed > 0)
        .ok_or_else(|| ClientError::illegal_argument(format!("{option} must be a positive integer")))
}

fn parse_positive_u64(value: String, option: &str) -> ClientResult<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|parsed| *parsed > 0)
        .ok_or_else(|| ClientError::illegal_argument(format!("{option} must be a positive integer")))
}

fn env_or(name: &str, default_value: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_value.to_string())
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn unique_group() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("rocketmq-rust-benchmark-producer-{millis}")
}

fn unique_consumer_group() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("rocketmq-rust-benchmark-lite-pull-{millis}")
}

fn unique_run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("message-path-{millis}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            namesrv_addr: "127.0.0.1:19876".to_string(),
            topic: "QualificationTopic".to_string(),
            producer_group: "qualification-producer".to_string(),
            scenario: Scenario::Sync,
            message_count: 4,
            message_size: 1_024,
            batch_size: 1,
            inflight: DEFAULT_ASYNC_INFLIGHT,
            timeout_ms: 3_000,
            use_tls: false,
            access_key: None,
            secret_key: None,
            security_token: None,
            output_json: None,
            run_id: "qualification-run".to_string(),
        }
    }

    #[test]
    fn latency_summary_uses_nearest_rank_percentiles() {
        let summary = latency_summary(&[100, 200, 300, 400, 500]);

        assert_eq!(5, summary.samples);
        assert_eq!(300, summary.p50);
        assert_eq!(500, summary.p95);
        assert_eq!(500, summary.p99);
        assert_eq!(500, summary.p999);
        assert_eq!(500, summary.max);
        assert_eq!(300.0, summary.average);
    }

    #[test]
    fn benchmark_report_has_stable_machine_readable_contract() {
        let config = test_config();
        let stats = Stats {
            success_count: 4,
            latency_us: vec![100, 200, 300, 400],
            ..Stats::default()
        };

        let report = benchmark_report(&config, &stats, Duration::from_secs(2), Phases::default());
        let value = serde_json::to_value(report).expect("benchmark report should serialize");

        assert_eq!(1, value["schema_version"]);
        assert_eq!("rocketmq_message_path_measurement", value["artifact_kind"]);
        assert_eq!("qualification-run", value["run_id"]);
        assert_eq!("sync", value["scenario"]);
        assert_eq!("send", value["operation"]);
        assert_eq!(4, value["result"]["success_count"]);
        assert_eq!(2.0, value["result"]["throughput_messages_per_second"]);
        assert_eq!(400, value["result"]["latency_us"]["p95"]);
    }

    #[test]
    fn report_adds_phases_without_changing_the_qualification_workload() {
        let config = Config {
            scenario: Scenario::Async,
            inflight: 64,
            ..test_config()
        };
        let stats = Stats {
            success_count: 3,
            send_failed_count: 1,
            unfinished_count: 1,
            latency_us: vec![100, 200, 300, 400],
            ..Stats::default()
        };
        let phases = Phases {
            seed_started_epoch_ms: None,
            measure_started_epoch_ms: 1_000,
            measure_finished_epoch_ms: 3_000,
        };

        let value = serde_json::to_value(benchmark_report(&config, &stats, Duration::from_secs(2), phases))
            .expect("benchmark report should serialize");

        // scripts/message_path_qualification.py compares the workload object for equality.
        let workload = value["workload"].as_object().expect("workload object");
        let mut keys = workload.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(vec!["batch_size", "message_count", "message_size_bytes"], keys);
        assert_eq!(64, value["async_inflight"]);
        assert_eq!(1_000, value["phases"]["measure_started_epoch_ms"]);
        assert_eq!(3_000, value["phases"]["measure_finished_epoch_ms"]);
        assert!(value["phases"]["seed_started_epoch_ms"].is_null());
        assert_eq!(1, value["result"]["unfinished_count"]);
        assert_eq!(1, value["result"]["send_failed_count"]);

        let sync_config = test_config();
        let sync = benchmark_report(&sync_config, &stats, Duration::from_secs(2), phases);
        assert_eq!(None, sync.async_inflight);
    }

    #[test]
    fn seed_batches_respect_batch_size_and_byte_bound() {
        assert_eq!(32, seed_batch_len(200_000, 32, 1_024));
        assert_eq!(5, seed_batch_len(5, 32, 1_024));
        // 64 KiB bodies: 16 fit in 1 MiB.
        assert_eq!(16, seed_batch_len(200_000, 32, 64 * 1024));
        // A body above the bound still seeds one message per batch.
        assert_eq!(1, seed_batch_len(200_000, 32, 4 * 1024 * 1024));
        assert_eq!(1, seed_batch_len(200_000, 0, 1_024));
    }

    #[test]
    fn lite_pull_uses_short_long_poll_with_rpc_safety_margin() {
        let short_suspend = lite_pull_broker_suspend_ms(50);
        let normal_suspend = lite_pull_broker_suspend_ms(30_000);

        assert_eq!(100, short_suspend);
        assert_eq!(1_000, normal_suspend);
        assert!(normal_suspend.saturating_add(1_000) > normal_suspend);
    }
}
