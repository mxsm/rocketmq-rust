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

#![recursion_limit = "256"]

use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::SendStatus;
use rocketmq_client_rust::SessionCredentials;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_protocol::common::message::message_decoder::decode_message_id;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

#[derive(Debug, Parser)]
#[command(about = "Create and verify an append-only PutOk failover ledger")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Seed(SeedArgs),
    Verify(VerifyArgs),
}

#[derive(Debug, Clone, Args)]
struct ConnectionArgs {
    #[arg(long)]
    namesrv: String,
    #[arg(long)]
    topic: String,
    #[arg(long, default_value_t = 10_000)]
    timeout_millis: u64,
}

#[derive(Debug, Args)]
struct SeedArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    run_id: String,
    #[arg(long, default_value_t = 10_000)]
    message_count: usize,
    #[arg(long, default_value_t = 256)]
    message_size: usize,
    #[arg(long)]
    ledger: PathBuf,
    #[arg(long)]
    ambiguous_ledger: PathBuf,
}

#[derive(Debug, Args)]
struct VerifyArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    ledger: PathBuf,
    #[arg(long)]
    observations: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PutOkLedgerEntry {
    sequence: usize,
    audit_id: String,
    unique_key: String,
    broker_message_id: String,
    offset_message_id: String,
    broker_name: String,
    queue_id: i32,
    queue_offset: u64,
    commit_log_offset: u64,
    store_size: u64,
    end_offset: u64,
    payload_sha256: String,
    put_ok_at_utc: String,
}

#[derive(Debug, Serialize)]
struct AmbiguousSend {
    sequence: usize,
    audit_id: String,
    unique_key: String,
    observed_at_utc: String,
    outcome: String,
}

#[derive(Debug, Serialize)]
struct RecoveryObservation {
    audit_id: String,
    unique_key: String,
    broker_name: String,
    queue_id: i32,
    queue_offset: u64,
    commit_log_offset: u64,
    store_size: u64,
    end_offset: u64,
    payload_sha256: String,
}

struct QualificationRuntime {
    owner: RuntimeOwner,
    client: Arc<ClientRuntime>,
    telemetry: rocketmq_observability::TelemetryRuntimeGuard,
}

impl QualificationRuntime {
    fn create() -> anyhow::Result<Self> {
        let owner = RuntimeOwner::new(RuntimeConfig {
            thread_name: "rocketmq-failover-qualification".to_string(),
            ..Default::default()
        })
        .context("create qualification runtime")?;
        let telemetry =
            rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
                .context("initialize qualification telemetry")?;
        let client = ClientRuntime::try_new(
            owner.root_context().component("failover-qualification"),
            ClientRuntimeConfig::default(),
            telemetry.handle(),
        )
        .context("create RocketMQ client runtime")?;
        Ok(Self {
            owner,
            client,
            telemetry,
        })
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        let client = self.client.shutdown().await;
        anyhow::ensure!(
            client.is_healthy(),
            "client shutdown was unhealthy: {}",
            client.to_json()
        );
        let tasks = self.owner.shutdown_tasks().await;
        anyhow::ensure!(tasks.is_healthy(), "task shutdown was unhealthy: {}", tasks.to_json());
        let background = self.owner.shutdown_background();
        anyhow::ensure!(
            background.is_healthy(),
            "background shutdown was unhealthy: {}",
            background.to_json()
        );
        let telemetry = self.telemetry.shutdown();
        anyhow::ensure!(
            telemetry.is_healthy(),
            "telemetry shutdown was unhealthy: {}",
            telemetry.to_json()
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Seed(args) => seed(args).await,
        Command::Verify(args) => verify(args).await,
    }
}

async fn seed(args: SeedArgs) -> anyhow::Result<()> {
    anyhow::ensure!(args.message_count > 0, "message-count must be positive");
    anyhow::ensure!(args.message_size >= 64, "message-size must be at least 64 bytes");
    anyhow::ensure!(!args.run_id.trim().is_empty(), "run-id must not be empty");
    prepare_output(&args.ledger)?;
    prepare_output(&args.ambiguous_ledger)?;

    let runtime = QualificationRuntime::create()?;
    let mut producer_builder = DefaultMQProducer::builder(Arc::clone(&runtime.client))
        .producer_group(format!("rpo-seed-{}", args.run_id))
        .name_server_addr(args.connection.namesrv.clone());
    if let Some(hook) = acl_hook() {
        producer_builder = producer_builder.rpc_hook(hook);
    }
    let mut producer = producer_builder.build();
    let consumer = build_query_consumer(Arc::clone(&runtime.client), &args.connection, &args.run_id)?;
    producer.start().await.context("start qualification producer")?;
    consumer.start().await.context("start qualification query client")?;

    let ledger_file = File::create(&args.ledger).context("create PutOk ledger")?;
    let ambiguous_file = File::create(&args.ambiguous_ledger).context("create ambiguous-send ledger")?;
    let mut ledger = BufWriter::new(ledger_file);
    let mut ambiguous = BufWriter::new(ambiguous_file);
    let mut sequence = 0_usize;
    let maximum_attempts = args.message_count.saturating_mul(3);
    let mut put_ok_count = 0_usize;

    while put_ok_count < args.message_count && sequence < maximum_attempts {
        let (audit_id, unique_key, body) = message_identity(&args.run_id, sequence, args.message_size);
        let message = Message::builder()
            .topic(args.connection.topic.as_str())
            .keys(vec![unique_key.clone()])
            .body_slice(&body)
            .build()?;
        match producer
            .send_with_timeout(message, args.connection.timeout_millis)
            .await
        {
            Ok(Some(result)) if result.send_status == SendStatus::SendOk => {
                let offset_message_id = result
                    .offset_msg_id
                    .clone()
                    .context("PutOk result did not contain an offset message ID")?;
                let stored = wait_for_stored_message(
                    &consumer,
                    &args.connection.topic,
                    &offset_message_id,
                    args.connection.timeout_millis,
                )
                .await?;
                let decoded = decode_message_id(&offset_message_id)
                    .map_err(anyhow::Error::msg)
                    .context("decode offset message ID")?;
                let commit_log_offset = u64::try_from(decoded.offset).context("negative CommitLog offset")?;
                let store_size = u64::try_from(stored.store_size()).context("negative store size")?;
                let queue = result
                    .message_queue
                    .context("PutOk result did not contain a message queue")?;
                let entry = PutOkLedgerEntry {
                    sequence,
                    audit_id,
                    unique_key,
                    broker_message_id: result.msg_id.map_or_else(String::new, |value| value.to_string()),
                    offset_message_id,
                    broker_name: queue.broker_name().to_string(),
                    queue_id: queue.queue_id(),
                    queue_offset: result.queue_offset,
                    commit_log_offset,
                    store_size,
                    end_offset: commit_log_offset.saturating_add(store_size),
                    payload_sha256: sha256(&body),
                    put_ok_at_utc: utc_now(),
                };
                append_json_line(&mut ledger, &entry)?;
                ledger.flush().context("flush PutOk ledger entry")?;
                put_ok_count += 1;
            }
            outcome => {
                let outcome = match outcome {
                    Ok(Some(result)) => format!("response:{}", result.send_status),
                    Ok(None) => "unknown:no-send-result".to_string(),
                    Err(error) => format!("unknown:{error}"),
                };
                append_json_line(
                    &mut ambiguous,
                    &AmbiguousSend {
                        sequence,
                        audit_id,
                        unique_key,
                        observed_at_utc: utc_now(),
                        outcome,
                    },
                )?;
                ambiguous.flush().context("flush ambiguous-send entry")?;
            }
        }
        sequence += 1;
    }

    ledger.flush().context("flush PutOk ledger")?;
    ledger.get_ref().sync_all().context("fsync PutOk ledger")?;
    ambiguous.flush().context("flush ambiguous-send ledger")?;
    ambiguous.get_ref().sync_all().context("fsync ambiguous-send ledger")?;
    producer.shutdown().await;
    consumer.shutdown().await;
    runtime.shutdown().await?;
    anyhow::ensure!(
        put_ok_count == args.message_count,
        "only {put_ok_count} of {} required PutOk messages were recorded in {maximum_attempts} attempts",
        args.message_count
    );
    println!("put_ok_count={put_ok_count} attempts={sequence}");
    Ok(())
}

async fn verify(args: VerifyArgs) -> anyhow::Result<()> {
    let expected = read_ledger(&args.ledger)?;
    anyhow::ensure!(!expected.is_empty(), "PutOk ledger is empty");
    prepare_output(&args.observations)?;
    let runtime = QualificationRuntime::create()?;
    let consumer = build_query_consumer(Arc::clone(&runtime.client), &args.connection, "verify")?;
    consumer.start().await.context("start qualification query client")?;
    let file = File::create(&args.observations).context("create recovery observations")?;
    let mut output = BufWriter::new(file);
    let mut observations = 0_usize;

    for entry in expected.values() {
        let result = consumer
            .query_message(&args.connection.topic, &entry.unique_key, 16, 0, u64::MAX)
            .await;
        let Ok(result) = result else {
            continue;
        };
        for message in result.message_list() {
            if !message_has_key(message, &entry.unique_key) {
                continue;
            }
            let commit_log_offset = u64::try_from(message.commit_log_offset()).unwrap_or_default();
            let store_size = u64::try_from(message.store_size()).unwrap_or_default();
            let body = message.body().unwrap_or_default();
            append_json_line(
                &mut output,
                &RecoveryObservation {
                    audit_id: entry.audit_id.clone(),
                    unique_key: entry.unique_key.clone(),
                    broker_name: message.broker_name().to_string(),
                    queue_id: message.queue_id(),
                    queue_offset: u64::try_from(message.queue_offset()).unwrap_or_default(),
                    commit_log_offset,
                    store_size,
                    end_offset: commit_log_offset.saturating_add(store_size),
                    payload_sha256: sha256(body.as_ref()),
                },
            )?;
            observations += 1;
        }
    }
    output.flush().context("flush recovery observations")?;
    output.get_ref().sync_all().context("fsync recovery observations")?;
    consumer.shutdown().await;
    runtime.shutdown().await?;
    println!("expected={} observations={observations}", expected.len());
    Ok(())
}

fn build_query_consumer(
    runtime: Arc<ClientRuntime>,
    connection: &ConnectionArgs,
    suffix: &str,
) -> anyhow::Result<DefaultLitePullConsumer> {
    let mut builder = DefaultLitePullConsumer::builder(runtime)
        .consumer_group(format!("rpo-query-{suffix}"))
        .name_server_addr(connection.namesrv.clone())
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .auto_commit(false);
    if let Some(hook) = acl_hook() {
        builder = builder.rpc_hook(hook);
    }
    builder.build().context("build qualification query client")
}

fn acl_hook() -> Option<Arc<AclClientRPCHook>> {
    let access_key = std::env::var("ROCKETMQ_ACL_ACCESS_KEY").ok()?;
    let secret_key = std::env::var("ROCKETMQ_ACL_SECRET_KEY").ok()?;
    Some(Arc::new(AclClientRPCHook::new(SessionCredentials::with_keys(
        access_key, secret_key,
    ))))
}

async fn wait_for_stored_message(
    consumer: &DefaultLitePullConsumer,
    topic: &str,
    offset_message_id: &str,
    timeout_millis: u64,
) -> anyhow::Result<MessageExt> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis.max(1_000));
    loop {
        match consumer.view_message(topic, offset_message_id).await {
            Ok(message) => return Ok(message),
            Err(error) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(error = %error, "waiting for PutOk message visibility");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error).context("query PutOk message before ledger append"),
        }
    }
}

fn message_identity(run_id: &str, sequence: usize, size: usize) -> (String, String, Vec<u8>) {
    let audit_id = format!("{run_id}-{sequence:012}");
    let unique_key = format!("rpo-{audit_id}");
    let prefix = format!("{audit_id}|");
    let mut body = Vec::with_capacity(size);
    while body.len() < size {
        body.extend_from_slice(prefix.as_bytes());
    }
    body.truncate(size);
    (audit_id, unique_key, body)
}

fn message_has_key(message: &MessageExt, expected: &str) -> bool {
    message
        .message_inner()
        .keys()
        .is_some_and(|keys| keys.iter().any(|key| key == expected))
}

fn read_ledger(path: &Path) -> anyhow::Result<HashMap<String, PutOkLedgerEntry>> {
    let file = File::open(path).context("open PutOk ledger")?;
    let mut result = HashMap::new();
    for line in BufReader::new(file).lines() {
        let entry: PutOkLedgerEntry =
            serde_json::from_str(&line.context("read PutOk ledger line")?).context("decode PutOk ledger line")?;
        anyhow::ensure!(
            result.insert(entry.audit_id.clone(), entry).is_none(),
            "PutOk ledger contains duplicate audit IDs"
        );
    }
    Ok(result)
}

fn prepare_output(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("create evidence output directory")?;
    }
    anyhow::ensure!(!path.exists(), "refusing to overwrite evidence: {}", path.display());
    Ok(())
}

fn append_json_line(writer: &mut BufWriter<File>, value: &impl Serialize) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *writer, value).context("encode evidence entry")?;
    writer.write_all(b"\n").context("write evidence delimiter")
}

fn sha256(value: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(value)))
}

fn utc_now() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{millis}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_identity_is_deterministic_and_bounded() {
        let first = message_identity("run-a", 7, 128);
        let second = message_identity("run-a", 7, 128);
        assert_eq!(first, second);
        assert_eq!(first.2.len(), 128);
        assert!(first.1.contains(&first.0));
    }

    #[test]
    fn ledger_entry_round_trips_without_message_body() {
        let entry = PutOkLedgerEntry {
            sequence: 1,
            audit_id: "audit-1".to_string(),
            unique_key: "key-1".to_string(),
            broker_message_id: "message-1".to_string(),
            offset_message_id: "offset-1".to_string(),
            broker_name: "broker-a".to_string(),
            queue_id: 0,
            queue_offset: 1,
            commit_log_offset: 100,
            store_size: 64,
            end_offset: 164,
            payload_sha256: format!("sha256:{}", "a".repeat(64)),
            put_ok_at_utc: "1".to_string(),
        };
        let json = serde_json::to_string(&entry).expect("encode ledger entry");
        assert!(!json.contains("message body"));
        assert_eq!(entry, serde_json::from_str(&json).expect("decode ledger entry"));
    }
}
