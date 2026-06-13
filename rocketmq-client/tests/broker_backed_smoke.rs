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

//! Broker-backed client smoke tests.
//!
//! These tests are skipped unless `ROCKETMQ_NAMESRV_ADDR` is set. They are
//! intended as the stable harness for Java/Rust same-broker compatibility
//! checks. Set `ROCKETMQ_REQUIRE_BROKER_BACKED_SMOKE=true` in
//! production-readiness runs to fail instead of silently skipping missing
//! broker/ACL/TLS/Trace/Recall scenarios. Keep each scenario small and explicit
//! so it can be mirrored by the Java client during parity runs.

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use tokio::sync::mpsc;
use tokio::sync::Notify;

use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::common::session_credentials::SessionCredentials;
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::ConsumeOrderlyContext;
use rocketmq_client_rust::consumer::listener::ConsumeOrderlyStatus;
use rocketmq_client_rust::consumer::mq_consumer::MQConsumer;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_client_rust::utils::message_util::MessageUtil;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

static GROUP_COUNTER: AtomicUsize = AtomicUsize::new(0);
const REQUIRE_BROKER_BACKED_SMOKE: &str = "ROCKETMQ_REQUIRE_BROKER_BACKED_SMOKE";

struct BrokerEnv {
    namesrv_addr: String,
    topic: String,
    producer_group: String,
}

fn broker_env() -> RocketMQResult<Option<BrokerEnv>> {
    let namesrv_addr = match std::env::var("ROCKETMQ_NAMESRV_ADDR") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            skip_or_fail("skipping broker-backed smoke test: ROCKETMQ_NAMESRV_ADDR is not set")?;
            return Ok(None);
        }
    };
    let topic = std::env::var("ROCKETMQ_TEST_TOPIC").unwrap_or_else(|_| "TopicTest".to_string());
    let producer_group = std::env::var("ROCKETMQ_TEST_PRODUCER_GROUP").unwrap_or_else(|_| unique_group("producer"));

    Ok(Some(BrokerEnv {
        namesrv_addr,
        topic,
        producer_group,
    }))
}

fn message(topic: &str, tag: &str, body: &str) -> Message {
    Message::builder()
        .topic(topic)
        .tags(tag)
        .body_slice(body.as_bytes())
        .build_unchecked()
}

fn unique_group(kind: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let sequence = GROUP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rocketmq-rust-smoke-{kind}-{suffix}-{sequence}")
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn strict_skip_error(reason: &str, strict: bool) -> RocketMQResult<()> {
    if strict {
        return Err(RocketMQError::illegal_argument(format!(
            "{reason}; set the required smoke-test environment or disable {REQUIRE_BROKER_BACKED_SMOKE}"
        )));
    }
    Ok(())
}

fn skip_or_fail(reason: impl Into<String>) -> RocketMQResult<()> {
    let reason = reason.into();
    strict_skip_error(&reason, env_flag_enabled(REQUIRE_BROKER_BACKED_SMOKE))?;
    eprintln!("{reason}");
    Ok(())
}

fn optional_smoke_enabled_or_skip(flag_name: &str, scenario: &str) -> RocketMQResult<bool> {
    if env_flag_enabled(flag_name) {
        return Ok(true);
    }
    skip_or_fail(format!("skipping {scenario}: {flag_name} is not enabled"))?;
    Ok(false)
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trace_body_matches(body: &[u8], topic: &str, message_key: &str) -> bool {
    let trace_data = String::from_utf8_lossy(body);
    trace_data.contains("Pub") && trace_data.contains(topic) && trace_data.contains(message_key)
}

async fn smoke_timeout<T, F>(operation: &'static str, timeout: Duration, future: F) -> RocketMQResult<T>
where
    F: Future<Output = RocketMQResult<T>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| RocketMQError::Timeout {
            operation,
            timeout_ms: timeout.as_millis().try_into().unwrap_or(u64::MAX),
        })?
}

async fn ensure_topic_route(namesrv_addr: &str, topic: &str) -> RocketMQResult<()> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("route-bootstrap-producer"))
        .name_server_addr(namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    let create_result = producer
        .create_topic(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC, topic, 4, HashMap::new())
        .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut last_error = create_result.err();
    while tokio::time::Instant::now() < deadline {
        match producer.fetch_publish_message_queues(topic).await {
            Ok(queues) if !queues.is_empty() => {
                producer.shutdown().await;
                return Ok(());
            }
            Ok(_) => {}
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    producer.shutdown().await;
    Err(last_error.unwrap_or_else(|| {
        RocketMQError::illegal_argument(format!(
            "topic route for {topic} was not available before smoke timeout"
        ))
    }))
}

fn acl_rpc_hook_from_env() -> RocketMQResult<Option<Arc<AclClientRPCHook>>> {
    let Some(access_key) = env_non_empty("ROCKETMQ_ACL_ACCESS_KEY") else {
        skip_or_fail("skipping ACL smoke test: ROCKETMQ_ACL_ACCESS_KEY is not set")?;
        return Ok(None);
    };
    let Some(secret_key) = env_non_empty("ROCKETMQ_ACL_SECRET_KEY") else {
        skip_or_fail("skipping ACL smoke test: ROCKETMQ_ACL_SECRET_KEY is not set")?;
        return Ok(None);
    };

    let credentials = match env_non_empty("ROCKETMQ_ACL_SECURITY_TOKEN") {
        Some(security_token) => SessionCredentials::with_token(access_key, secret_key, security_token),
        None => SessionCredentials::with_keys(access_key, secret_key),
    };
    Ok(Some(Arc::new(AclClientRPCHook::new(credentials))))
}

#[test]
fn strict_skip_error_allows_default_skip_mode() {
    strict_skip_error("missing smoke env", false).expect("default skip mode should not fail");
}

#[test]
fn strict_skip_error_fails_when_broker_smoke_is_required() {
    let error = strict_skip_error("missing smoke env", true)
        .expect_err("strict broker-backed smoke mode should fail on missing environment");

    let message = error.to_string();
    assert!(message.contains("missing smoke env"));
    assert!(message.contains(REQUIRE_BROKER_BACKED_SMOKE));
}

struct CommitTransactionListener;

impl TransactionListener for CommitTransactionListener {
    fn execute_local_transaction(
        &self,
        _msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }

    fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_producer_sync_oneway_batch_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let mut producer = DefaultMQProducer::builder()
        .producer_group(env.producer_group)
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;

    let sync_result = producer
        .send_with_timeout(message(&env.topic, "Sync", "rocketmq-rust-sync-smoke"), 5_000)
        .await?;
    assert!(sync_result.is_some(), "sync send should return a SendResult");

    producer
        .send_oneway(message(&env.topic, "Oneway", "rocketmq-rust-oneway-smoke"))
        .await?;

    let batch_result = producer
        .send_batch(vec![
            message(&env.topic, "Batch", "rocketmq-rust-batch-smoke-0"),
            message(&env.topic, "Batch", "rocketmq-rust-batch-smoke-1"),
            message(&env.topic, "Batch", "rocketmq-rust-batch-smoke-2"),
        ])
        .await?;
    assert!(
        batch_result.msg_id.as_ref().is_some_and(|msg_id| !msg_id.is_empty()),
        "batch send should return a message id"
    );

    producer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_producer_mq_admin_offsets_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let topic = env.topic.clone();
    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("admin-producer"))
        .name_server_addr(env.namesrv_addr.clone())
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;

    let queue_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let queues = loop {
        match producer.fetch_publish_message_queues(&topic).await {
            Ok(queues) if !queues.is_empty() => break queues,
            Ok(_) => {
                if tokio::time::Instant::now() >= queue_deadline {
                    return Err(rocketmq_error::RocketMQError::illegal_argument(
                        "MQAdmin smoke topic did not expose publish queues",
                    ));
                }
            }
            Err(error) => {
                if tokio::time::Instant::now() >= queue_deadline {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let queue = queues[0].clone();
    let message_key = format!("rocketmq-rust-mqadmin-key-{}", unique_group("key"));
    let message_body = format!("rocketmq-rust-mqadmin-offset-smoke-{message_key}");

    let send_result = producer
        .send_to_queue_with_timeout(
            Message::builder()
                .topic(topic.as_str())
                .tags("MQAdmin")
                .key(message_key.as_str())
                .body_slice(message_body.as_bytes())
                .build_unchecked(),
            queue.clone(),
            5_000,
        )
        .await?;
    assert!(
        send_result
            .as_ref()
            .and_then(|result| result.msg_id.as_ref())
            .is_some_and(|msg_id| !msg_id.is_empty()),
        "admin offset smoke should seed one message in the selected queue"
    );

    let min_offset = producer.min_offset(&queue).await?;
    let max_offset = producer.max_offset(&queue).await?;
    let search_offset = producer.search_offset(&queue, 0).await?;
    let earliest_store_time = producer.earliest_msg_store_time(&queue).await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(u64::MAX);
    let query_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let query_result = loop {
        match producer
            .query_message(&topic, &message_key, 32, 0, now_millis.saturating_add(60_000))
            .await
        {
            Ok(query_result) => {
                if query_result.message_list().iter().any(|msg| {
                    msg.body()
                        .as_ref()
                        .is_some_and(|body| body.as_ref() == message_body.as_bytes())
                }) || tokio::time::Instant::now() >= query_deadline
                {
                    break query_result;
                }
            }
            Err(error) => {
                if tokio::time::Instant::now() >= query_deadline {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(
        query_result.message_list().iter().any(|msg| {
            msg.body()
                .as_ref()
                .is_some_and(|body| body.as_ref() == message_body.as_bytes())
        }),
        "queryMessage should return the seeded keyed message"
    );

    let view_msg_id = send_result
        .as_ref()
        .and_then(|result| result.offset_msg_id.as_deref().or(result.msg_id.as_deref()))
        .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("send result did not contain a message id"))?;
    let viewed_message = producer.view_message(&topic, view_msg_id).await?;
    let viewed_by_unique_msg_id =
        if let Some(unique_msg_id) = send_result.as_ref().and_then(|result| result.msg_id.as_deref()) {
            let view_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            Some(loop {
                match producer.view_message(&topic, unique_msg_id).await {
                    Ok(message) => {
                        if message
                            .body()
                            .as_ref()
                            .is_some_and(|body| body.as_ref() == message_body.as_bytes())
                            || tokio::time::Instant::now() >= view_deadline
                        {
                            break message;
                        }
                    }
                    Err(error) => {
                        if tokio::time::Instant::now() >= view_deadline {
                            return Err(error);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            })
        } else {
            None
        };

    let mut push_admin = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("admin-push"))
        .name_server_addr(env.namesrv_addr)
        .build();
    push_admin.subscribe(&topic, "*").await?;
    push_admin.register_message_listener_concurrently(
        |_msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess),
    );
    push_admin.start().await?;

    let push_min_offset = MQConsumer::min_offset(&mut push_admin, &queue).await?;
    let push_max_offset = MQConsumer::max_offset(&mut push_admin, &queue).await?;
    let push_query_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let push_query_result = loop {
        match MQConsumer::query_message(
            &mut push_admin,
            &topic,
            &message_key,
            32,
            0,
            now_millis.saturating_add(60_000),
        )
        .await
        {
            Ok(query_result) => {
                if query_result.message_list().iter().any(|msg| {
                    msg.body()
                        .as_ref()
                        .is_some_and(|body| body.as_ref() == message_body.as_bytes())
                }) || tokio::time::Instant::now() >= push_query_deadline
                {
                    break query_result;
                }
            }
            Err(error) => {
                if tokio::time::Instant::now() >= push_query_deadline {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let push_view_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let push_viewed_message = loop {
        match MQConsumer::view_message(&mut push_admin, &topic, view_msg_id).await {
            Ok(message) => {
                if message
                    .body()
                    .as_ref()
                    .is_some_and(|body| body.as_ref() == message_body.as_bytes())
                    || tokio::time::Instant::now() >= push_view_deadline
                {
                    break message;
                }
            }
            Err(error) => {
                if tokio::time::Instant::now() >= push_view_deadline {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    push_admin.shutdown().await;

    producer.shutdown().await;

    assert!(min_offset >= 0, "minOffset should be non-negative");
    assert!(max_offset >= min_offset, "maxOffset should not be below minOffset");
    assert!(
        search_offset >= 0,
        "searchOffset by epoch should return a non-negative broker offset"
    );
    assert!(
        earliest_store_time >= 0,
        "earliestMsgStoreTime should return a non-negative broker timestamp"
    );
    assert_eq!(
        viewed_message.body().as_ref().map(|body| body.as_ref()),
        Some(message_body.as_bytes()),
        "viewMessage should return the seeded message body"
    );
    if let Some(viewed_by_unique_msg_id) = viewed_by_unique_msg_id {
        assert_eq!(
            viewed_by_unique_msg_id.body().as_ref().map(|body| body.as_ref()),
            Some(message_body.as_bytes()),
            "producer viewMessage should fall back to uniq-key query for Java-compatible msgId lookup"
        );
    }
    assert!(push_min_offset >= 0, "push consumer minOffset should be non-negative");
    assert!(
        push_max_offset >= push_min_offset,
        "push consumer maxOffset should not be below minOffset"
    );
    assert!(
        push_query_result.message_list().iter().any(|msg| {
            msg.body()
                .as_ref()
                .is_some_and(|body| body.as_ref() == message_body.as_bytes())
        }),
        "push consumer queryMessage should return the seeded keyed message"
    );
    assert_eq!(
        push_viewed_message.body().as_ref().map(|body| body.as_ref()),
        Some(message_body.as_bytes()),
        "push consumer viewMessage should return the seeded message body"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_producer_create_topic_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    if !optional_smoke_enabled_or_skip("ROCKETMQ_ENABLE_CREATE_TOPIC_SMOKE", "create topic smoke test")? {
        return Ok(());
    }

    let new_topic = format!("rocketmq-rust-smoke-topic-{}", unique_group("topic"));
    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("create-topic-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    producer
        .create_topic(
            TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            new_topic.as_str(),
            1,
            HashMap::new(),
        )
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let queues = producer.fetch_publish_message_queues(&new_topic).await?;
    producer.shutdown().await;

    assert!(
        !queues.is_empty(),
        "created topic should expose publish queues through name server route"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_producer_async_callback_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("async-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;

    let callback_result = Arc::new(Mutex::new(None));
    let callback_result_inner = Arc::clone(&callback_result);
    let callback_notify = Arc::new(Notify::new());
    let callback_notify_inner = Arc::clone(&callback_notify);

    producer
        .send_with_callback_timeout(
            message(&env.topic, "Async", "rocketmq-rust-async-smoke"),
            move |result, error| {
                let outcome = match (result, error) {
                    (Some(send_result), None) => Ok(send_result.msg_id.clone().unwrap_or_default()),
                    (None, Some(error)) => Err(error.to_string()),
                    _ => Err("async callback received neither result nor error".to_string()),
                };
                *callback_result_inner
                    .lock()
                    .expect("async callback result mutex should not be poisoned") = Some(outcome);
                callback_notify_inner.notify_one();
            },
            5_000,
        )
        .await?;

    tokio::time::timeout(std::time::Duration::from_secs(10), callback_notify.notified())
        .await
        .expect("async send callback should be invoked");

    let outcome = callback_result
        .lock()
        .expect("async callback result mutex should not be poisoned")
        .take()
        .expect("async callback should store an outcome");
    let msg_id =
        outcome.map_err(|error| rocketmq_error::RocketMQError::network_request_failed(env.topic.clone(), error))?;
    assert!(!msg_id.is_empty(), "async callback should return a message id");

    producer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_acl_producer_send_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    let Some(acl_hook) = acl_rpc_hook_from_env()? else {
        return Ok(());
    };

    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("acl-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .rpc_hook(acl_hook)
        .build();

    producer.start().await?;
    let send_result = producer
        .send_with_timeout(message(&env.topic, "Acl", "rocketmq-rust-acl-smoke"), 5_000)
        .await?;
    assert!(
        send_result
            .as_ref()
            .and_then(|result| result.msg_id.as_ref())
            .is_some_and(|msg_id| !msg_id.is_empty()),
        "ACL producer send should return a message id"
    );

    producer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_tls_producer_send_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    if !optional_smoke_enabled_or_skip("ROCKETMQ_ENABLE_TLS_SMOKE", "TLS smoke test")? {
        return Ok(());
    }

    let client_config = ClientConfig::builder()
        .namesrv_addr(env.namesrv_addr)
        .enable_tls(true)
        .build()?;
    let mut producer = DefaultMQProducer::builder()
        .client_config(client_config)
        .producer_group(unique_group("tls-producer"))
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    let send_result = producer
        .send_with_timeout(message(&env.topic, "Tls", "rocketmq-rust-tls-smoke"), 5_000)
        .await?;
    assert!(
        send_result
            .as_ref()
            .and_then(|result| result.msg_id.as_ref())
            .is_some_and(|msg_id| !msg_id.is_empty()),
        "TLS producer send should return a message id"
    );

    producer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_trace_producer_send_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    if !optional_smoke_enabled_or_skip("ROCKETMQ_ENABLE_TRACE_SMOKE", "trace smoke test")? {
        return Ok(());
    }

    let trace_topic = env_non_empty("ROCKETMQ_TRACE_TOPIC").unwrap_or_else(|| unique_group("trace-topic"));
    ensure_topic_route(&env.namesrv_addr, &env.topic).await?;
    ensure_topic_route(&env.namesrv_addr, &trace_topic).await?;
    let trace_message_key = format!("rocketmq-rust-trace-key-{}", unique_group("key"));
    let trace_message_body = format!("rocketmq-rust-trace-smoke-{trace_message_key}");

    let trace_consumer = DefaultLitePullConsumer::builder()
        .consumer_group(unique_group("trace-lite-pull"))
        .name_server_addr(env.namesrv_addr.clone())
        .pull_batch_size(8)
        .poll_timeout_millis(500)
        .auto_commit(true)
        .build()?;
    trace_consumer.subscribe_with_expression(&trace_topic, "*").await?;
    smoke_timeout("trace_consumer.start", Duration::from_secs(30), trace_consumer.start()).await?;

    let mut client_config_builder = ClientConfig::builder()
        .namesrv_addr(env.namesrv_addr.clone())
        .enable_trace(true)
        .trace_msg_batch_num(1);
    client_config_builder = client_config_builder.trace_topic(trace_topic.clone());
    let client_config = client_config_builder.build()?;
    let mut producer = DefaultMQProducer::builder()
        .client_config(client_config)
        .producer_group(unique_group("trace-producer"))
        .send_msg_timeout(5_000)
        .build();

    smoke_timeout("trace_producer.start", Duration::from_secs(30), producer.start()).await?;
    let trace_message = Message::builder()
        .topic(env.topic.as_str())
        .tags("Trace")
        .key(trace_message_key.as_str())
        .body_slice(trace_message_body.as_bytes())
        .build_unchecked();
    let send_result = smoke_timeout(
        "trace_producer.send_with_timeout",
        Duration::from_secs(15),
        producer.send_with_timeout(trace_message, 5_000),
    )
    .await?;
    assert!(
        send_result
            .as_ref()
            .and_then(|result| result.msg_id.as_ref())
            .is_some_and(|msg_id| !msg_id.is_empty()),
        "trace-enabled producer send should return a message id"
    );

    smoke_timeout("trace_producer.shutdown", Duration::from_secs(15), async {
        producer.shutdown().await;
        Ok(())
    })
    .await?;

    let trace_found = smoke_timeout("trace_topic.poll", Duration::from_secs(60), async {
        loop {
            let trace_messages = trace_consumer.poll_with_timeout(1_000).await;
            if trace_messages.iter().any(|message| {
                message.body().as_ref().is_some_and(|body| {
                    trace_body_matches(body.as_ref(), env.topic.as_str(), trace_message_key.as_str())
                })
            }) {
                return Ok(true);
            }
        }
    })
    .await?;

    smoke_timeout("trace_consumer.shutdown", Duration::from_secs(15), async {
        trace_consumer.shutdown().await;
        Ok(())
    })
    .await?;

    assert!(
        trace_found,
        "trace smoke should consume a Pub trace record for the sent message from trace topic {trace_topic}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_backed_producer_request_reply_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    ensure_topic_route(&env.namesrv_addr, &env.topic).await?;

    let tag = "RequestReplySmoke";
    let request_body = format!("rocketmq-rust-request-smoke-{}", unique_group("body"));
    let reply_body = format!("rocketmq-rust-reply-smoke-{request_body}");
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<Message>();

    let reply_namesrv_addr = env.namesrv_addr.clone();
    let reply_task = tokio::spawn(async move {
        let mut reply_producer = DefaultMQProducer::builder()
            .producer_group(unique_group("reply-producer"))
            .name_server_addr(reply_namesrv_addr)
            .send_msg_timeout(5_000)
            .build();

        reply_producer.start().await?;
        let Some(reply_message) = reply_rx.recv().await else {
            reply_producer.shutdown().await;
            return Err(rocketmq_error::RocketMQError::illegal_argument(
                "request/reply smoke did not enqueue a reply message",
            ));
        };

        let send_result = reply_producer.send_with_timeout(reply_message, 5_000).await?;
        reply_producer.shutdown().await;

        assert!(
            send_result
                .as_ref()
                .and_then(|result| result.msg_id.as_ref())
                .is_some_and(|msg_id| !msg_id.is_empty()),
            "reply producer should return a message id"
        );

        Ok::<(), rocketmq_error::RocketMQError>(())
    });

    let expected_request_body = request_body.clone();
    let reply_body_for_listener = reply_body.clone();
    let reply_tx_for_listener = reply_tx.clone();
    let mut responder = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("request-responder"))
        .name_server_addr(env.namesrv_addr.clone())
        .consume_thread_min(1)
        .consume_thread_max(2)
        .consume_message_batch_max_size(1)
        .pull_batch_size(4)
        .build();

    responder.subscribe(&env.topic, tag).await?;
    responder.register_message_listener_concurrently(
        move |msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
            for msg in msgs {
                if msg
                    .body()
                    .as_ref()
                    .is_some_and(|body| body.as_ref() == expected_request_body.as_bytes())
                {
                    let reply_message =
                        MessageUtil::create_reply_message(msg.message_inner(), reply_body_for_listener.as_bytes())?;
                    reply_tx_for_listener.send(reply_message).map_err(|error| {
                        rocketmq_error::RocketMQError::illegal_argument(format!(
                            "request/reply smoke failed to enqueue reply message: {error}"
                        ))
                    })?;
                }
            }
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        },
    );
    responder.start().await?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut requester = DefaultMQProducer::builder()
        .producer_group(unique_group("request-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    requester.start().await?;
    let response = requester
        .request(message(&env.topic, tag, &request_body), 15_000)
        .await?;

    requester.shutdown().await;
    responder.shutdown().await;
    drop(reply_tx);

    reply_task.await.map_err(|error| {
        rocketmq_error::RocketMQError::network_request_failed(env.topic.clone(), error.to_string())
    })??;

    assert_eq!(
        response.get_body().map(|body| body.as_ref()),
        Some(reply_body.as_bytes()),
        "request should receive the body sent by the responder"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_producer_recall_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    if !optional_smoke_enabled_or_skip("ROCKETMQ_ENABLE_RECALL_SMOKE", "recall smoke test")? {
        return Ok(());
    }

    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("recall-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;

    let delayed_message = Message::builder()
        .topic(env.topic.as_str())
        .tags("Recall")
        .body_slice(format!("rocketmq-rust-recall-smoke-{}", unique_group("body")).as_bytes())
        .delay_millis(60_000)
        .build_unchecked();
    let send_result = producer
        .send_with_timeout(delayed_message, 5_000)
        .await?
        .expect("recall smoke uses sync send and should return SendResult");
    let recall_handle = send_result
        .recall_handle()
        .ok_or_else(|| {
            rocketmq_error::RocketMQError::illegal_argument(
                "recall smoke broker did not return a recall handle for the delayed message",
            )
        })?
        .to_string();

    let recalled_msg_id = producer.recall_message(env.topic, recall_handle).await?;
    assert!(
        !recalled_msg_id.is_empty(),
        "recall should return the recalled message id"
    );

    producer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_transaction_producer_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let mut producer = TransactionMQProducer::builder()
        .producer_group(unique_group("transaction-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .transaction_listener(CommitTransactionListener)
        .build();

    producer.start().await?;

    let result = producer
        .send_message_in_transaction(
            message(&env.topic, "Transaction", "rocketmq-rust-transaction-smoke"),
            Some("commit"),
        )
        .await?;

    assert_eq!(
        result.local_transaction_state,
        Some(LocalTransactionState::CommitMessage),
        "transaction listener should commit the half message"
    );
    assert!(
        result
            .send_result
            .as_ref()
            .and_then(|send_result| send_result.msg_id.as_ref())
            .is_some_and(|msg_id| !msg_id.is_empty()),
        "transaction send should return a message id"
    );

    producer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_lite_pull_assign_offset_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };
    ensure_topic_route(&env.namesrv_addr, &env.topic).await?;

    let consumer = DefaultLitePullConsumer::builder()
        .consumer_group(unique_group("lite-pull"))
        .name_server_addr(env.namesrv_addr)
        .pull_batch_size(4)
        .poll_timeout_millis(500)
        .auto_commit(false)
        .build()?;

    consumer.set_sub_expression_for_assign(&env.topic, "*").await?;
    consumer.start().await?;
    assert!(consumer.is_running().await, "consumer should be running");
    assert!(!consumer.is_auto_commit().await, "smoke test uses manual offset commit");

    let queues = consumer.fetch_message_queues(&env.topic).await?;
    assert!(!queues.is_empty(), "test topic should have at least one readable queue");
    let queue = queues[0].clone();

    consumer.assign(vec![queue.clone()]).await?;
    let assignment = consumer.assignment().await?;
    assert!(
        assignment.contains(&queue),
        "manual assignment should include the selected queue"
    );

    consumer.pause(vec![queue.clone()]).await;
    assert!(consumer.is_paused(&queue).await, "queue should be paused");
    consumer.resume(vec![queue.clone()]).await;
    assert!(
        !consumer.is_paused(&queue).await,
        "queue should be resumed before polling"
    );

    let timestamp_offset = consumer.offset_for_timestamp(&queue, 0).await?;
    assert!(
        timestamp_offset >= 0,
        "offset_for_timestamp should return a non-negative broker offset"
    );
    consumer.seek(&queue, timestamp_offset).await?;

    let zero_copy_messages = consumer.poll_with_timeout_zero_copy(500).await;
    assert!(
        zero_copy_messages
            .iter()
            .all(|message| message.topic().as_str() == env.topic),
        "zero-copy poll should only return messages for the subscribed topic"
    );

    let owned_messages = consumer.poll_with_timeout(500).await;
    assert!(
        owned_messages
            .iter()
            .all(|message| message.topic().as_str() == env.topic),
        "owned poll should only return messages for the subscribed topic"
    );

    let mut offset_map = HashMap::new();
    offset_map.insert(queue.clone(), timestamp_offset);
    consumer.commit_with_map(offset_map, true).await;
    consumer.commit_with_set(HashSet::from([queue.clone()]), true).await;
    consumer.commit().await;
    let _ = consumer.commit_all().await;
    let _ = consumer.committed(&queue).await;

    consumer.seek_to_begin(&queue).await?;
    consumer.seek_to_end(&queue).await?;
    consumer.unsubscribe(&env.topic).await;
    consumer.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_push_consumer_concurrent_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let tag = "PushSmoke";
    let body = format!("rocketmq-rust-push-smoke-{}", unique_group("body"));
    let received_body = Arc::new(Mutex::new(None));
    let received_body_inner = Arc::clone(&received_body);
    let received_notify = Arc::new(Notify::new());
    let received_notify_inner = Arc::clone(&received_notify);
    let expected_body = body.clone();

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("push"))
        .name_server_addr(env.namesrv_addr.clone())
        .consume_thread_min(1)
        .consume_thread_max(2)
        .consume_message_batch_max_size(1)
        .pull_batch_size(4)
        .build();

    consumer.subscribe(&env.topic, tag).await?;
    consumer.register_message_listener_concurrently(
        move |msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
            for msg in msgs {
                if msg
                    .body()
                    .as_ref()
                    .is_some_and(|body| body.as_ref() == expected_body.as_bytes())
                {
                    *received_body_inner
                        .lock()
                        .expect("push listener result mutex should not be poisoned") = Some(expected_body.clone());
                    received_notify_inner.notify_one();
                }
            }
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        },
    );
    consumer.start().await?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("push-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    let send_result = producer
        .send_with_timeout(message(&env.topic, tag, &body), 5_000)
        .await?;
    assert!(send_result.is_some(), "push smoke producer send should succeed");

    let receive_result = tokio::time::timeout(std::time::Duration::from_secs(30), received_notify.notified()).await;

    producer.shutdown().await;
    consumer.shutdown().await;

    receive_result.expect("push consumer should receive the smoke message");
    assert_eq!(
        received_body
            .lock()
            .expect("push listener result mutex should not be poisoned")
            .as_deref(),
        Some(body.as_str())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_push_consumer_retry_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let topic = unique_group("push-retry-topic");
    ensure_topic_route(&env.namesrv_addr, &topic).await?;

    let tag = "PushRetrySmoke";
    let body = format!("rocketmq-rust-push-retry-smoke-{}", unique_group("body"));
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_inner = Arc::clone(&attempts);
    let retry_notify = Arc::new(Notify::new());
    let retry_notify_inner = Arc::clone(&retry_notify);
    let expected_body = body.clone();

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("push-retry"))
        .name_server_addr(env.namesrv_addr.clone())
        .consume_thread_min(1)
        .consume_thread_max(2)
        .consume_message_batch_max_size(1)
        .pull_batch_size(4)
        .build();

    consumer.subscribe(&topic, tag).await?;
    consumer.register_message_listener_concurrently(
        move |msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
            for msg in msgs {
                if msg
                    .body()
                    .as_ref()
                    .is_some_and(|body| body.as_ref() == expected_body.as_bytes())
                {
                    let attempt = attempts_inner.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt == 1 {
                        return Ok(ConsumeConcurrentlyStatus::ReconsumeLater);
                    }
                    retry_notify_inner.notify_one();
                    return Ok(ConsumeConcurrentlyStatus::ConsumeSuccess);
                }
            }
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        },
    );
    consumer.start().await?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("push-retry-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    let send_result = producer.send_with_timeout(message(&topic, tag, &body), 5_000).await?;
    assert!(send_result.is_some(), "retry smoke producer send should succeed");

    let retry_result = tokio::time::timeout(std::time::Duration::from_secs(90), retry_notify.notified()).await;

    producer.shutdown().await;
    consumer.shutdown().await;

    retry_result.expect("push consumer should receive the retried message");
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "listener should be invoked once for the original delivery and once for retry"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_push_consumer_orderly_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let tag = "PushOrderlySmoke";
    let body_prefix = format!("rocketmq-rust-push-orderly-smoke-{}", unique_group("body"));
    let first_body = format!("{body_prefix}-0");
    let second_body = format!("{body_prefix}-1");
    let expected_bodies = vec![first_body.clone(), second_body.clone()];
    let received_bodies = Arc::new(Mutex::new(Vec::new()));
    let received_bodies_inner = Arc::clone(&received_bodies);
    let received_notify = Arc::new(Notify::new());
    let received_notify_inner = Arc::clone(&received_notify);
    let prefix_for_listener = body_prefix.clone();

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("push-orderly"))
        .name_server_addr(env.namesrv_addr.clone())
        .consume_thread_min(1)
        .consume_thread_max(2)
        .consume_message_batch_max_size(1)
        .pull_batch_size(4)
        .build();

    consumer.subscribe(&env.topic, tag).await?;
    consumer.register_message_listener_orderly(move |msgs: &[&MessageExt], context: &mut ConsumeOrderlyContext| {
        context.set_auto_commit(true);
        for msg in msgs {
            let body = msg.body();
            let Some(body) = body.as_ref() else {
                continue;
            };
            let body = String::from_utf8_lossy(body.as_ref()).to_string();
            if body.starts_with(prefix_for_listener.as_str()) {
                let mut received = received_bodies_inner
                    .lock()
                    .expect("orderly listener result mutex should not be poisoned");
                received.push(body);
                if received.len() >= 2 {
                    received_notify_inner.notify_one();
                }
            }
        }
        Ok(ConsumeOrderlyStatus::Success)
    });
    consumer.start().await?;
    assert!(
        consumer.is_consume_orderly(),
        "orderly listener should switch the consumer mode"
    );

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("push-orderly-producer"))
        .name_server_addr(env.namesrv_addr)
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    let selector = |queues: &[MessageQueue], _msg: &Message, _order_id: &i32| queues.first().cloned();
    let first_send = producer
        .send_with_selector_timeout(message(&env.topic, tag, &first_body), selector, 0, 5_000)
        .await?;
    assert!(first_send.is_some(), "first orderly send should succeed");
    let selector = |queues: &[MessageQueue], _msg: &Message, _order_id: &i32| queues.first().cloned();
    let second_send = producer
        .send_with_selector_timeout(message(&env.topic, tag, &second_body), selector, 0, 5_000)
        .await?;
    assert!(second_send.is_some(), "second orderly send should succeed");

    let receive_result = tokio::time::timeout(std::time::Duration::from_secs(30), received_notify.notified()).await;

    producer.shutdown().await;
    consumer.shutdown().await;

    receive_result.expect("orderly push consumer should receive both smoke messages");
    assert_eq!(
        *received_bodies
            .lock()
            .expect("orderly listener result mutex should not be poisoned"),
        expected_bodies,
        "messages sent to the same queue should be consumed in send order"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_backed_lite_pull_subscribe_poll_smoke() -> RocketMQResult<()> {
    let Some(env) = broker_env()? else {
        return Ok(());
    };

    let tag = "LitePullSubscribeSmoke";
    let topic = unique_group("lite-pull-subscribe-topic");
    ensure_topic_route(&env.namesrv_addr, &topic).await?;
    let body = format!("rocketmq-rust-lite-pull-subscribe-smoke-{}", unique_group("body"));
    let mut producer = DefaultMQProducer::builder()
        .producer_group(unique_group("lite-pull-subscribe-producer"))
        .name_server_addr(env.namesrv_addr.clone())
        .send_msg_timeout(5_000)
        .build();

    producer.start().await?;
    producer
        .send_with_timeout(
            message(&topic, "LitePullSubscribeBootstrap", &unique_group("bootstrap")),
            5_000,
        )
        .await?;

    let consumer = DefaultLitePullConsumer::builder()
        .consumer_group(unique_group("lite-pull-subscribe"))
        .name_server_addr(env.namesrv_addr.clone())
        .pull_batch_size(4)
        .poll_timeout_millis(500)
        .auto_commit(true)
        .build()?;

    consumer.subscribe_with_expression(&topic, tag).await?;
    consumer.start().await?;
    assert!(
        consumer.is_running().await,
        "lite pull subscribe consumer should be running"
    );
    assert!(consumer.is_auto_commit().await, "subscribe smoke uses auto commit");

    let route_queues = consumer.fetch_message_queues(&topic).await?;
    assert!(
        !route_queues.is_empty(),
        "lite pull subscribe smoke topic should have readable queues before assignment"
    );

    let assignment_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut assignment = HashSet::new();
    while tokio::time::Instant::now() < assignment_deadline {
        assignment = consumer.assignment().await?;
        if !assignment.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(
        !assignment.is_empty(),
        "lite pull subscribe consumer should receive assigned queues before polling"
    );
    let send_result = producer.send_with_timeout(message(&topic, tag, &body), 5_000).await?;
    assert!(
        send_result
            .as_ref()
            .and_then(|result| result.msg_id.as_ref())
            .is_some_and(|msg_id| !msg_id.is_empty()),
        "lite pull subscribe smoke producer should return a message id"
    );

    let poll_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut received = false;
    while tokio::time::Instant::now() < poll_deadline {
        let messages = consumer.poll_with_timeout(1_000).await;
        if messages.iter().any(|message| {
            message
                .body()
                .as_ref()
                .is_some_and(|candidate| candidate.as_ref() == body.as_bytes())
        }) {
            received = true;
            break;
        }
    }

    producer.shutdown().await;
    consumer.shutdown().await;

    assert!(received, "lite pull subscribe poll should receive the smoke message");

    Ok(())
}
