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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::SessionCredentials;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Sync,
    Async,
    Batch,
    LitePull,
}

impl Scenario {
    fn parse(value: &str) -> RocketMQResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "sync" | "producersync" => Ok(Self::Sync),
            "async" | "producerasync" => Ok(Self::Async),
            "batch" | "producerbatch" => Ok(Self::Batch),
            "lite-pull" | "litepull" | "litepullbenchmark" => Ok(Self::LitePull),
            other => Err(RocketMQError::illegal_argument(format!(
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
    timeout_ms: u64,
    use_tls: bool,
    access_key: Option<String>,
    secret_key: Option<String>,
    security_token: Option<String>,
}

#[derive(Debug, Default)]
struct Stats {
    success_count: usize,
    send_failed_count: usize,
    response_failed_count: usize,
    total_rt_ms: u128,
    max_rt_ms: u128,
}

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    let config = Config::parse()?;
    let body = vec![b'a'; config.message_size];

    println!(
        "RocketMQ Rust client production benchmark namesrv={} topic={} scenario={} messageCount={} messageSize={} \
         batchSize={} tls={} acl={}",
        config.namesrv_addr,
        config.topic,
        config.scenario.as_str(),
        config.message_count,
        config.message_size,
        config.batch_size,
        config.use_tls,
        config.access_key.is_some()
    );

    let start = Instant::now();
    let (stats, elapsed) = match config.scenario {
        Scenario::Sync | Scenario::Async | Scenario::Batch => {
            let mut producer = build_producer(&config)?;
            producer.start().await?;
            let stats = match config.scenario {
                Scenario::Sync => run_sync(&mut producer, &config, &body).await,
                Scenario::Async => run_async(&mut producer, &config, &body).await,
                Scenario::Batch => run_batch(&mut producer, &config, &body).await,
                Scenario::LitePull => unreachable!("lite-pull is handled by the outer match"),
            };
            let elapsed = start.elapsed();
            producer.shutdown().await;
            (stats?, elapsed)
        }
        Scenario::LitePull => run_lite_pull(&config, &body).await?,
    };

    print_complete_summary(stats, elapsed, config.scenario.operation());
    Ok(())
}

fn build_producer(config: &Config) -> RocketMQResult<DefaultMQProducer> {
    let mut builder = DefaultMQProducer::builder()
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

fn build_lite_pull_consumer(config: &Config) -> RocketMQResult<DefaultLitePullConsumer> {
    let mut builder = DefaultLitePullConsumer::builder()
        .consumer_group(unique_consumer_group())
        .name_server_addr(config.namesrv_addr.clone())
        .pull_batch_size(config.batch_size.min(i32::MAX as usize) as i32)
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

async fn run_sync(producer: &mut DefaultMQProducer, config: &Config, body: &[u8]) -> RocketMQResult<Stats> {
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

async fn run_async(producer: &mut DefaultMQProducer, config: &Config, body: &[u8]) -> RocketMQResult<Stats> {
    let success_count = Arc::new(AtomicUsize::new(0));
    let send_failed_count = Arc::new(AtomicUsize::new(0));
    let response_failed_count = Arc::new(AtomicUsize::new(0));
    let total_rt_ms = Arc::new(AtomicU64::new(0));
    let max_rt_ms = Arc::new(AtomicU64::new(0));

    for _ in 0..config.message_count {
        let begin = Instant::now();
        let success_count_inner = Arc::clone(&success_count);
        let send_failed_count_inner = Arc::clone(&send_failed_count);
        let response_failed_count_inner = Arc::clone(&response_failed_count);
        let total_rt_ms_inner = Arc::clone(&total_rt_ms);
        let max_rt_ms_inner = Arc::clone(&max_rt_ms);

        if let Err(error) = producer
            .send_with_callback_timeout(
                message(&config.topic, "RustAsyncBenchmark", body),
                move |result: Option<&SendResult>, error: Option<&RocketMQError>| {
                    let elapsed_ms = elapsed_ms_u64(begin.elapsed());
                    total_rt_ms_inner.fetch_add(elapsed_ms, Ordering::Relaxed);
                    update_max(&max_rt_ms_inner, elapsed_ms);

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
            send_failed_count.fetch_add(1, Ordering::Relaxed);
            eprintln!("async send failed before callback: {error}");
        }
    }

    let deadline = Instant::now() + Duration::from_millis(config.timeout_ms.saturating_mul(2));
    while observed_count(&success_count, &send_failed_count, &response_failed_count) < config.message_count
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let observed = observed_count(&success_count, &send_failed_count, &response_failed_count);
    if observed < config.message_count {
        send_failed_count.fetch_add(config.message_count - observed, Ordering::Relaxed);
    }

    Ok(Stats {
        success_count: success_count.load(Ordering::Relaxed),
        send_failed_count: send_failed_count.load(Ordering::Relaxed),
        response_failed_count: response_failed_count.load(Ordering::Relaxed),
        total_rt_ms: total_rt_ms.load(Ordering::Relaxed) as u128,
        max_rt_ms: max_rt_ms.load(Ordering::Relaxed) as u128,
    })
}

async fn run_batch(producer: &mut DefaultMQProducer, config: &Config, body: &[u8]) -> RocketMQResult<Stats> {
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

async fn run_lite_pull(config: &Config, body: &[u8]) -> RocketMQResult<(Stats, Duration)> {
    const TAG: &str = "RustLitePullBenchmark";

    let mut producer = build_producer(config)?;
    let consumer = build_lite_pull_consumer(config)?;

    producer.start().await?;
    consumer.set_sub_expression_for_assign(&config.topic, TAG).await?;
    consumer.start().await?;

    let queues = consumer.fetch_message_queues(&config.topic).await?;
    let queue = first_queue(queues)?;
    consumer.assign(vec![queue.clone()]).await?;
    let offset = producer.max_offset(&queue).await?.max(0);
    consumer.seek(&queue, offset).await?;

    seed_lite_pull_messages(&mut producer, config, body, TAG, &queue).await?;

    let start = Instant::now();
    let stats = consume_lite_pull_messages(&consumer, config, TAG, start).await;
    let elapsed = start.elapsed();

    consumer.commit().await;
    consumer.shutdown().await;
    producer.shutdown().await;

    Ok((stats, elapsed))
}

async fn seed_lite_pull_messages(
    producer: &mut DefaultMQProducer,
    config: &Config,
    body: &[u8],
    tag: &str,
    queue: &MessageQueue,
) -> RocketMQResult<()> {
    for _ in 0..config.message_count {
        match producer
            .send_to_queue_with_timeout(message(&config.topic, tag, body), queue.clone(), config.timeout_ms)
            .await?
        {
            Some(send_result) if send_result.send_status == SendStatus::SendOk => {}
            Some(send_result) => {
                return Err(RocketMQError::illegal_argument(format!(
                    "LitePull seed send status was {:?}",
                    send_result.send_status
                )));
            }
            None => {
                return Err(RocketMQError::illegal_argument(
                    "LitePull seed send returned no SendResult",
                ))
            }
        }
    }
    Ok(())
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

fn first_queue(queues: Vec<MessageQueue>) -> RocketMQResult<MessageQueue> {
    queues
        .into_iter()
        .min()
        .ok_or_else(|| RocketMQError::illegal_argument("LitePull benchmark found no message queues for topic"))
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
    let elapsed_ms = elapsed.as_millis();
    stats.total_rt_ms += elapsed_ms;
    stats.max_rt_ms = stats.max_rt_ms.max(elapsed_ms);
}

fn print_complete_summary(stats: Stats, elapsed: Duration, operation: Operation) {
    let total = stats.success_count + stats.send_failed_count;
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let tps = (stats.success_count as f64 / elapsed_secs).round() as u64;
    let average_rt = if stats.success_count == 0 {
        0.0
    } else {
        stats.total_rt_ms as f64 / stats.success_count as f64
    };
    let label = operation.label();

    println!(
        "[Complete] {} Total: {} | {} TPS: {} | Max RT(ms): {} | Average RT(ms): {:.3} | {} Failed: {} | Response \
         Failed: {}",
        label,
        total,
        label,
        tps,
        stats.max_rt_ms,
        average_rt,
        label,
        stats.send_failed_count,
        stats.response_failed_count
    );
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

fn elapsed_ms_u64(elapsed: Duration) -> u64 {
    elapsed.as_millis().try_into().unwrap_or(u64::MAX)
}

fn update_max(max_rt_ms: &AtomicU64, candidate: u64) {
    let mut current = max_rt_ms.load(Ordering::Relaxed);
    while candidate > current {
        match max_rt_ms.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

impl Config {
    fn parse() -> RocketMQResult<Self> {
        let mut config = Self {
            namesrv_addr: env_or("ROCKETMQ_NAMESRV_ADDR", "127.0.0.1:9876"),
            topic: env_or("ROCKETMQ_TEST_TOPIC", "TopicTest"),
            producer_group: unique_group(),
            scenario: Scenario::Sync,
            message_count: 100,
            message_size: 128,
            batch_size: 16,
            timeout_ms: 30_000,
            use_tls: env_flag("ROCKETMQ_ENABLE_TLS_SMOKE"),
            access_key: env_non_empty("ROCKETMQ_ACL_ACCESS_KEY"),
            secret_key: env_non_empty("ROCKETMQ_ACL_SECRET_KEY"),
            security_token: env_non_empty("ROCKETMQ_ACL_SECURITY_TOKEN"),
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
                "--timeout-ms" => {
                    config.timeout_ms = parse_positive_u64(next_arg(&mut args, "--timeout-ms")?, "--timeout-ms")?;
                }
                "--tls" => config.use_tls = true,
                "--acl" => {
                    let access_key = config.access_key.take().ok_or_else(|| {
                        RocketMQError::illegal_argument("--acl requires --access-key or ROCKETMQ_ACL_ACCESS_KEY")
                    })?;
                    let secret_key = config.secret_key.take().ok_or_else(|| {
                        RocketMQError::illegal_argument("--acl requires --secret-key or ROCKETMQ_ACL_SECRET_KEY")
                    })?;
                    config.access_key = Some(access_key);
                    config.secret_key = Some(secret_key);
                }
                "--access-key" => config.access_key = Some(next_arg(&mut args, "--access-key")?),
                "--secret-key" => config.secret_key = Some(next_arg(&mut args, "--secret-key")?),
                "--security-token" => config.security_token = Some(next_arg(&mut args, "--security-token")?),
                other => return Err(RocketMQError::illegal_argument(format!("unknown argument: {other}"))),
            }
        }

        if config.namesrv_addr.trim().is_empty() {
            return Err(RocketMQError::illegal_argument("--namesrv must not be blank"));
        }
        if config.topic.trim().is_empty() {
            return Err(RocketMQError::illegal_argument("--topic must not be blank"));
        }
        if config.access_key.is_some() != config.secret_key.is_some() {
            return Err(RocketMQError::illegal_argument(
                "ACL benchmark requires both access key and secret key",
            ));
        }

        Ok(config)
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, option: &str) -> RocketMQResult<String> {
    args.next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| RocketMQError::illegal_argument(format!("{option} requires a value")))
}

fn parse_positive_usize(value: String, option: &str) -> RocketMQResult<usize> {
    value
        .parse::<usize>()
        .ok()
        .filter(|parsed| *parsed > 0)
        .ok_or_else(|| RocketMQError::illegal_argument(format!("{option} must be a positive integer")))
}

fn parse_positive_u64(value: String, option: &str) -> RocketMQResult<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|parsed| *parsed > 0)
        .ok_or_else(|| RocketMQError::illegal_argument(format!("{option} must be a positive integer")))
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
