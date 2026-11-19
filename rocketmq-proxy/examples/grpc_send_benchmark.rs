// Copyright 2026 The RocketMQ Rust Authors
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

//! Sends messages to a Proxy over gRPC and reports throughput and latency.
//!
//! Each RPC carries one message, as a gRPC producer sends it. Every connection is a separate
//! client (its own client id), and each keeps `--inflight` sends outstanding. The release image
//! build uses it to train the Proxy's PGO profile; it also serves as a benchmark.
//!
//! ```text
//! grpc_send_benchmark --endpoint http://127.0.0.1:8081 --topic BenchTopic \
//!     --message-count 200000 --warmup-count 50000 --connections 4 --inflight 64
//! ```
//!
//! The topic must already exist. The summary is printed as JSON and, with `--output-json`,
//! written to a file.

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rocketmq_proxy::v2;
use rocketmq_proxy::v2::messaging_service_client::MessagingServiceClient;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Request;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct Options {
    endpoint: String,
    topic: String,
    message_count: usize,
    warmup_count: usize,
    connections: usize,
    inflight: usize,
    message_size: usize,
    timeout: Duration,
    output_json: Option<String>,
}

impl Options {
    fn parse() -> BenchResult<Self> {
        let mut options = Self {
            endpoint: "http://127.0.0.1:8081".to_owned(),
            topic: String::new(),
            message_count: 100_000,
            warmup_count: 0,
            connections: 1,
            inflight: 256,
            message_size: 1024,
            timeout: Duration::from_secs(10),
            output_json: None,
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let mut value = || args.next().ok_or_else(|| format!("{flag} needs a value"));
            match flag.as_str() {
                "--endpoint" => options.endpoint = value()?,
                "--topic" => options.topic = value()?,
                "--message-count" => options.message_count = value()?.parse()?,
                "--warmup-count" => options.warmup_count = value()?.parse()?,
                "--connections" => options.connections = value()?.parse()?,
                "--inflight" => options.inflight = value()?.parse()?,
                "--message-size" => options.message_size = value()?.parse()?,
                "--timeout-ms" => options.timeout = Duration::from_millis(value()?.parse()?),
                "--output-json" => options.output_json = Some(value()?),
                other => return Err(format!("unknown flag {other}").into()),
            }
        }
        if options.topic.is_empty() {
            return Err("--topic is required".into());
        }
        if options.connections == 0 || options.inflight == 0 {
            return Err("--connections and --inflight must be positive".into());
        }
        Ok(options)
    }
}

/// The messages of one connection share a body; each send gets its own message id.
struct Sender {
    client: MessagingServiceClient<Channel>,
    topic: String,
    body: Bytes,
    client_id: String,
    timeout: Duration,
}

impl Sender {
    fn request(&self, sequence: u64) -> BenchResult<Request<v2::SendMessageRequest>> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let message = v2::Message {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: self.topic.clone(),
            }),
            user_properties: HashMap::new(),
            system_properties: Some(v2::SystemProperties {
                message_id: format!("01{:08X}{:022X}", std::process::id(), sequence),
                body_encoding: v2::Encoding::Identity as i32,
                message_type: v2::MessageType::Normal as i32,
                born_timestamp: Some(prost_types::Timestamp {
                    seconds: i64::try_from(now.as_secs())?,
                    nanos: i32::try_from(now.subsec_nanos())?,
                }),
                born_host: "127.0.0.1".to_owned(),
                ..Default::default()
            }),
            body: self.body.clone(),
        };
        let mut request = Request::new(v2::SendMessageRequest {
            messages: vec![message],
        });
        let metadata = request.metadata_mut();
        metadata.insert("x-mq-client-id", MetadataValue::try_from(self.client_id.as_str())?);
        metadata.insert("x-mq-language", MetadataValue::from_static("JAVA"));
        metadata.insert("x-mq-protocol", MetadataValue::from_static("v2"));
        metadata.insert("x-mq-client-version", MetadataValue::from_static("5.0.7"));
        request.set_timeout(self.timeout);
        Ok(request)
    }

    /// Sends `count` messages with up to `inflight` outstanding; returns the latencies of the
    /// successful sends and the first failure, if any.
    async fn run(&self, first_sequence: u64, count: usize, inflight: usize) -> BenchResult<Outcome> {
        let mut outcome = Outcome {
            latencies_us: Vec::with_capacity(count),
            failed: 0,
            first_error: None,
        };
        let mut pending = FuturesUnordered::new();
        let mut issued = 0usize;
        loop {
            while issued < count && pending.len() < inflight {
                let mut client = self.client.clone();
                let request = self.request(first_sequence + issued as u64)?;
                issued += 1;
                pending.push(async move {
                    let started = Instant::now();
                    let result = client.send_message(request).await;
                    (started.elapsed(), result)
                });
            }
            let Some((elapsed, result)) = pending.next().await else {
                break;
            };
            let failure = match result {
                Ok(response) => {
                    let response = response.into_inner();
                    let ok = response.entries.len() == 1
                        && response.entries[0]
                            .status
                            .as_ref()
                            .is_some_and(|status| status.code == v2::Code::Ok as i32);
                    (!ok).then(|| format!("{response:?}"))
                }
                Err(status) => Some(format!("{status:?}")),
            };
            match failure {
                None => outcome
                    .latencies_us
                    .push(u32::try_from(elapsed.as_micros()).unwrap_or(u32::MAX)),
                Some(failure) => {
                    outcome.failed += 1;
                    outcome.first_error.get_or_insert(failure);
                }
            }
        }
        Ok(outcome)
    }
}

struct Outcome {
    latencies_us: Vec<u32>,
    failed: usize,
    first_error: Option<String>,
}

/// Runs `count` sends spread over all connections and returns the elapsed time and outcomes.
async fn phase(
    senders: &[std::sync::Arc<Sender>],
    first_sequence: u64,
    count: usize,
    inflight: usize,
) -> BenchResult<(Duration, Vec<Outcome>)> {
    let per_connection = count / senders.len();
    let started = Instant::now();
    let tasks: Vec<_> = senders
        .iter()
        .enumerate()
        .map(|(index, sender)| {
            let sender = sender.clone();
            let first = first_sequence + (index * per_connection) as u64;
            tokio::spawn(async move { sender.run(first, per_connection, inflight).await })
        })
        .collect();
    let mut outcomes = Vec::with_capacity(tasks.len());
    for task in tasks {
        outcomes.push(task.await??);
    }
    Ok((started.elapsed(), outcomes))
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    let options = Options::parse()?;
    let body = Bytes::from(vec![b'x'; options.message_size]);
    let run_id = std::process::id();
    let mut senders = Vec::with_capacity(options.connections);
    for index in 0..options.connections {
        let channel = Endpoint::from_shared(options.endpoint.clone())?
            .tcp_nodelay(true)
            .connect()
            .await?;
        senders.push(std::sync::Arc::new(Sender {
            client: MessagingServiceClient::new(channel),
            topic: options.topic.clone(),
            body: body.clone(),
            client_id: format!("grpc-send-benchmark-{run_id}-{index}"),
            timeout: options.timeout,
        }));
    }
    if options.warmup_count > 0 {
        phase(&senders, 0, options.warmup_count, options.inflight).await?;
    }
    let (elapsed, outcomes) = phase(&senders, 1 << 40, options.message_count, options.inflight).await?;
    let failed: usize = outcomes.iter().map(|outcome| outcome.failed).sum();
    let first_error = outcomes.iter().find_map(|outcome| outcome.first_error.clone());
    let mut latencies: Vec<u32> = outcomes.into_iter().flat_map(|outcome| outcome.latencies_us).collect();
    latencies.sort_unstable();
    let percentile = |fraction: f64| -> u32 {
        if latencies.is_empty() {
            return 0;
        }
        let index = ((latencies.len() - 1) as f64 * fraction) as usize;
        latencies[index]
    };
    let summary = serde_json::json!({
        "throughput_messages_per_second": latencies.len() as f64 / elapsed.as_secs_f64(),
        "success_count": latencies.len(),
        "failed": failed,
        "elapsed_ms": u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        "latency_us": {"p50": percentile(0.5), "p99": percentile(0.99)},
        "first_error": first_error.map(|error| error.chars().take(600).collect::<String>()),
    });
    if let Some(path) = &options.output_json {
        std::fs::write(path, summary.to_string())?;
    }
    println!("{summary}");
    if failed > 0 {
        return Err(format!("{failed} sends failed").into());
    }
    Ok(())
}
