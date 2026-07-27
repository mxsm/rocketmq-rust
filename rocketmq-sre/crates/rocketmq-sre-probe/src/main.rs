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

use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::ConsumeConcurrentlyContext;
use rocketmq_client_rust::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageListenerConcurrently;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_probe::ProbeAclConfig;
use rocketmq_sre_probe::ProbeConfig;
use rocketmq_sre_probe::ProbePlan;
use rocketmq_sre_probe::load_probe_acl_config;
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

const DEFAULT_CLUSTER_ID: &str = "00000000-0000-4000-8000-000000000001";
const DEFAULT_RUN_ID: &str = "00000000-0000-0000-0000-000000000000";
const DEFAULT_NAMESRV_ADDR: &str = "namesrv:9876";
const PROBE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Command {
    Plan,
    Register,
    Send,
    Consume,
}

impl Command {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Register => "register",
            Self::Send => "send",
            Self::Consume => "consume",
        }
    }
}

#[derive(Debug, Error)]
enum ProbeRunError {
    #[error("usage: rocketmq-sre-probe <plan|register|send|consume>")]
    Usage,
    #[error("probe environment `{name}` is invalid")]
    InvalidEnvironment { name: &'static str },
    #[error("probe configuration is invalid")]
    InvalidProbe(#[from] rocketmq_sre_probe::ProbeConfigError),
    #[error("probe ACL configuration is invalid")]
    InvalidProbeAcl(#[from] rocketmq_sre_probe::ProbeAclConfigError),
    #[error("RocketMQ probe operation failed")]
    RocketMq(#[from] rocketmq_error::RocketMQError),
    #[error("probe timed out before the bounded operation completed")]
    Timeout,
    #[error("probe plan could not be encoded")]
    Encoding(#[from] serde_json::Error),
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let command = parse_command()?;
    let (plan, namesrv_addr) = load_plan()?;
    if matches!(command, Command::Plan) {
        println!("{}", serde_json::to_string(&plan)?);
        return Ok(());
    }
    let acl_config = load_probe_acl_config()?;
    let operation_timeout = Duration::from_secs(u64::from(plan.max_duration_seconds));

    let runtime_owner = RuntimeOwner::new(RuntimeConfig {
        thread_name: "rocketmq-sre-probe".to_owned(),
        shutdown_timeout: PROBE_SHUTDOWN_TIMEOUT,
        ..RuntimeConfig::default()
    })?;
    let client_runtime = ClientRuntime::new(
        runtime_owner.root_context().child("probe.client"),
        ClientRuntimeConfig {
            shutdown_timeout: PROBE_SHUTDOWN_TIMEOUT,
        },
    );
    let operation_result = runtime_owner.block_on(async {
        match tokio::time::timeout(
            operation_timeout,
            run_command(command, Arc::clone(&client_runtime), plan, namesrv_addr, acl_config),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(ProbeRunError::Timeout),
        }
    });
    let client_shutdown =
        runtime_owner.block_on(async { tokio::time::timeout(PROBE_SHUTDOWN_TIMEOUT, client_runtime.shutdown()).await });
    let runtime_shutdown = runtime_owner.shutdown_runtime_blocking();

    let mut cleanup_partial = operation_result?;
    cleanup_partial |= match client_shutdown {
        Ok(report) => {
            let unhealthy = !report.is_healthy();
            if unhealthy {
                eprintln!("level=warn stage=client_runtime_shutdown cleanup_partial=true reason=unhealthy");
            }
            unhealthy
        }
        Err(_) => {
            eprintln!("level=warn stage=client_runtime_shutdown cleanup_partial=true reason=timeout");
            true
        }
    };
    let runtime_shutdown = runtime_shutdown?;
    if !runtime_shutdown.is_healthy() {
        eprintln!("level=warn stage=runtime_owner_shutdown cleanup_partial=true reason=unhealthy");
        cleanup_partial = true;
    }
    println!(
        "probe_result command={} cleanup_partial={cleanup_partial}",
        command.as_str()
    );
    Ok(())
}

fn parse_command() -> Result<Command, ProbeRunError> {
    match env::args().nth(1).as_deref() {
        Some("plan") => Ok(Command::Plan),
        Some("register") => Ok(Command::Register),
        Some("send") => Ok(Command::Send),
        Some("consume") => Ok(Command::Consume),
        _ => Err(ProbeRunError::Usage),
    }
}

fn load_plan() -> Result<(ProbePlan, String), ProbeRunError> {
    let cluster_id = env::var("ROCKETMQ_SRE_PROBE_CLUSTER_ID")
        .unwrap_or_else(|_| DEFAULT_CLUSTER_ID.to_owned())
        .parse::<ClusterId>()
        .map_err(|_| ProbeRunError::InvalidEnvironment {
            name: "ROCKETMQ_SRE_PROBE_CLUSTER_ID",
        })?;
    let run_id = env::var("ROCKETMQ_SRE_PROBE_RUN_ID")
        .unwrap_or_else(|_| DEFAULT_RUN_ID.to_owned())
        .parse::<Uuid>()
        .map_err(|_| ProbeRunError::InvalidEnvironment {
            name: "ROCKETMQ_SRE_PROBE_RUN_ID",
        })?;
    let config = ProbeConfig {
        cluster_id,
        max_messages: parse_env("ROCKETMQ_SRE_PROBE_MAX_MESSAGES", 10)?,
        max_payload_bytes: parse_env("ROCKETMQ_SRE_PROBE_PAYLOAD_BYTES", 64)?,
        max_duration_seconds: parse_env("ROCKETMQ_SRE_PROBE_DURATION_SECONDS", 30)?,
    };
    let plan = config.plan(run_id)?;
    let namesrv_addr = env::var("ROCKETMQ_NAMESRV_ADDR").unwrap_or_else(|_| DEFAULT_NAMESRV_ADDR.to_owned());
    if namesrv_addr.trim().is_empty() {
        return Err(ProbeRunError::InvalidEnvironment {
            name: "ROCKETMQ_NAMESRV_ADDR",
        });
    }
    Ok((plan, namesrv_addr))
}

fn parse_env<T>(name: &'static str, default: T) -> Result<T, ProbeRunError>
where
    T: std::str::FromStr,
{
    match env::var(name) {
        Ok(value) => value.parse().map_err(|_| ProbeRunError::InvalidEnvironment { name }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(ProbeRunError::InvalidEnvironment { name }),
    }
}

async fn run_command(
    command: Command,
    client_runtime: Arc<ClientRuntime>,
    plan: ProbePlan,
    namesrv_addr: String,
    acl_config: Option<ProbeAclConfig>,
) -> Result<bool, ProbeRunError> {
    println!("probe_stage command={} stage=operation status=begin", command.as_str());
    match command {
        Command::Plan => Ok(false),
        Command::Register => register(client_runtime, &plan, &namesrv_addr, acl_config.as_ref()).await,
        Command::Send => send(client_runtime, &plan, &namesrv_addr, acl_config.as_ref()).await,
        Command::Consume => consume(client_runtime, &plan, &namesrv_addr, acl_config.as_ref()).await,
    }
}

async fn register(
    client_runtime: Arc<ClientRuntime>,
    plan: &ProbePlan,
    namesrv_addr: &str,
    acl_config: Option<&ProbeAclConfig>,
) -> Result<bool, ProbeRunError> {
    let builder = DefaultMQPushConsumer::builder(Arc::clone(&client_runtime))
        .consumer_group(plan.identity.consumer_group.clone())
        .name_server_addr(namesrv_addr.to_owned());
    let builder = match acl_config {
        Some(config) => builder.rpc_hook(Some(Arc::new(config.rpc_hook()))),
        None => builder,
    };
    let mut consumer = builder.build();
    consumer.subscribe(&plan.identity.topic, "*").await?;
    consumer.register_message_listener_concurrently(CountingListener::default());
    println!("probe_stage command=register stage=consumer_start status=begin");
    consumer.start().await?;
    println!("probe_stage command=register stage=consumer_start status=end");
    let cancellation = client_runtime.service_context().task_group().cancellation_token();
    tokio::time::sleep(Duration::from_secs(1)).await;
    cancellation.cancel();
    let cleanup_partial = tokio::time::timeout(PROBE_SHUTDOWN_TIMEOUT, consumer.shutdown())
        .await
        .is_err();
    if cleanup_partial {
        eprintln!("level=warn stage=consumer_shutdown cleanup_partial=true reason=timeout");
    }
    println!(
        "registered topic={} group={} cleanup_partial={cleanup_partial}",
        plan.identity.topic, plan.identity.consumer_group,
    );
    println!("probe_stage command=register stage=operation status=end");
    Ok(cleanup_partial)
}

async fn send(
    client_runtime: Arc<ClientRuntime>,
    plan: &ProbePlan,
    namesrv_addr: &str,
    acl_config: Option<&ProbeAclConfig>,
) -> Result<bool, ProbeRunError> {
    let builder = DefaultMQProducer::builder(Arc::clone(&client_runtime))
        .producer_group(plan.identity.producer_group.clone())
        .name_server_addr(namesrv_addr.to_owned());
    let builder = match acl_config {
        Some(config) => builder.rpc_hook(Arc::new(config.rpc_hook())),
        None => builder,
    };
    let mut producer = builder.build();
    println!("probe_stage command=send stage=producer_start status=begin");
    producer.start().await?;
    println!("probe_stage command=send stage=producer_start status=end");
    let payload = vec![b'x'; plan.max_payload_bytes as usize];
    println!("probe_stage command=send stage=message_batch status=begin");
    for sequence in 0..plan.max_messages {
        let message = Message::builder()
            .topic(plan.identity.topic.clone())
            .tags("phase00")
            .keys(vec![format!("probe-{sequence}")])
            .body_slice(&payload)
            .build_unchecked();
        producer.send_with_timeout(message, 2_000).await?;
    }
    println!("probe_stage command=send stage=message_batch status=end");
    println!("probe_stage command=send stage=producer_shutdown status=begin");
    let cleanup_partial = tokio::time::timeout(PROBE_SHUTDOWN_TIMEOUT, producer.shutdown())
        .await
        .is_err();
    println!("probe_stage command=send stage=producer_shutdown status=end");
    client_runtime
        .service_context()
        .task_group()
        .cancellation_token()
        .cancel();
    if cleanup_partial {
        eprintln!("level=warn stage=producer_shutdown cleanup_partial=true reason=timeout");
    }
    println!(
        "sent={} topic={} payload_bytes={} cleanup_partial={cleanup_partial}",
        plan.max_messages, plan.identity.topic, plan.max_payload_bytes,
    );
    println!("probe_stage command=send stage=operation status=end");
    Ok(cleanup_partial)
}

async fn consume(
    client_runtime: Arc<ClientRuntime>,
    plan: &ProbePlan,
    namesrv_addr: &str,
    acl_config: Option<&ProbeAclConfig>,
) -> Result<bool, ProbeRunError> {
    let listener = CountingListener::default();
    let observed = Arc::clone(&listener.observed);
    let notification = Arc::clone(&listener.notification);
    let builder = DefaultMQPushConsumer::builder(Arc::clone(&client_runtime))
        .consumer_group(plan.identity.consumer_group.clone())
        .name_server_addr(namesrv_addr.to_owned());
    let builder = match acl_config {
        Some(config) => builder.rpc_hook(Some(Arc::new(config.rpc_hook()))),
        None => builder,
    };
    let mut consumer = builder.build();
    consumer.subscribe(&plan.identity.topic, "*").await?;
    consumer.register_message_listener_concurrently(listener);
    println!("probe_stage command=consume stage=consumer_start status=begin");
    consumer.start().await?;
    println!("probe_stage command=consume stage=consumer_start status=end");
    let expected = usize::from(plan.max_messages);
    let wait = async {
        while observed.load(Ordering::Acquire) < expected {
            notification.notified().await;
        }
    };
    let result = tokio::time::timeout(Duration::from_secs(u64::from(plan.max_duration_seconds)), wait).await;
    client_runtime
        .service_context()
        .task_group()
        .cancellation_token()
        .cancel();
    let cleanup_partial = tokio::time::timeout(PROBE_SHUTDOWN_TIMEOUT, consumer.shutdown())
        .await
        .is_err();
    if cleanup_partial {
        eprintln!("level=warn stage=consumer_shutdown cleanup_partial=true reason=timeout");
    }
    result.map_err(|_| ProbeRunError::Timeout)?;
    println!(
        "consumed={} topic={} group={} cleanup_partial={cleanup_partial}",
        observed.load(Ordering::Acquire),
        plan.identity.topic,
        plan.identity.consumer_group,
    );
    println!("probe_stage command=consume stage=operation status=end");
    Ok(cleanup_partial)
}

#[derive(Clone, Default)]
struct CountingListener {
    observed: Arc<AtomicUsize>,
    notification: Arc<Notify>,
}

impl MessageListenerConcurrently for CountingListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        self.observed.fetch_add(messages.len(), Ordering::AcqRel);
        self.notification.notify_waiters();
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
