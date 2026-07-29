use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_client_rust::factory::transport_health::subscribe_consumer_transport_events;
use rocketmq_client_rust::factory::transport_health::ConsumerTransportOperation;
use rocketmq_client_rust::factory::transport_health::ConsumerTransportOutcome;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

struct NoopListener;

impl MessageListenerConcurrently for NoopListener {
    fn consume_message(
        &self,
        _msgs: &[&MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

fn unique_group(suffix: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    format!("phase3_lifecycle_{suffix}_{nonce}")
}

fn consumer(group: String, nameserver: &str) -> DefaultMQPushConsumer {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(group)
        .name_server_addr(nameserver)
        .build();
    consumer.subscribe("TopicTest", "*").expect("subscribe TopicTest");
    consumer.register_message_listener_concurrently(NoopListener);
    consumer
}

fn consumer_with_fast_heartbeat(group: String, nameserver: &str) -> DefaultMQPushConsumer {
    let mut client_config = rocketmq_client_rust::base::client_config::ClientConfig::default();
    client_config.set_namesrv_addr(nameserver.to_string().into());
    client_config.set_heartbeat_broker_interval(250);
    let mut consumer = DefaultMQPushConsumer::builder()
        .client_config(client_config)
        .consumer_group(group)
        .build();
    consumer.subscribe("TopicTest", "*").expect("subscribe TopicTest");
    consumer.register_message_listener_concurrently(NoopListener);
    consumer
}

async fn wait_for_transport_event(
    receiver: &mut tokio::sync::broadcast::Receiver<
        rocketmq_client_rust::factory::transport_health::ConsumerTransportEvent,
    >,
    group: &str,
    outcome: ConsumerTransportOutcome,
) {
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        loop {
            let event = receiver.recv().await.expect("transport event channel remains open");
            if event.consumer_group == group
                && event.operation == ConsumerTransportOperation::Heartbeat
                && event.outcome == outcome
            {
                return;
            }
        }
    })
    .await
    .expect("expected transport event before timeout");
}

/// Requires a live RocketMQ NameServer. Run with:
/// `ROCKETMQ_PHASE3_NAMESERVER=127.0.0.1:19876 cargo test -p rocketmq-client-rust --test
/// phase3_lifecycle_integration -- --ignored`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live RocketMQ NameServer and Broker"]
async fn concurrent_clustering_consumers_share_startup_and_shutdown_cleanly() {
    let nameserver = std::env::var("ROCKETMQ_PHASE3_NAMESERVER")
        .expect("set ROCKETMQ_PHASE3_NAMESERVER to a live NameServer address");
    let mut first = consumer(unique_group("first"), &nameserver);
    let mut second = consumer(unique_group("second"), &nameserver);

    let (first_result, second_result) = tokio::join!(first.start(), second.start());
    first_result.expect("first clustering consumer starts");
    second_result.expect("second clustering consumer starts");

    first.shutdown().await;
    second.shutdown().await;
}

/// Requires a live NameServer and an external broker stop after the test prints
/// its readiness line. For example:
/// `ROCKETMQ_PHASE3_NAMESERVER=127.0.0.1:9876 ROCKETMQ_PHASE4_EXPECT_BROKER_STOP=1 \
/// cargo test -p rocketmq-client-rust --test phase3_lifecycle_integration \
/// transport_failure_is_emitted_after_broker_stops -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live RocketMQ NameServer and controlled broker stop"]
async fn transport_failure_is_emitted_after_broker_stops() {
    if std::env::var("ROCKETMQ_PHASE4_EXPECT_BROKER_STOP").as_deref() != Ok("1") {
        return;
    }
    let nameserver = std::env::var("ROCKETMQ_PHASE3_NAMESERVER")
        .expect("set ROCKETMQ_PHASE3_NAMESERVER to a live NameServer address");
    let group = unique_group("transport");
    let mut events = subscribe_consumer_transport_events();
    let mut consumer = consumer_with_fast_heartbeat(group.clone(), &nameserver);

    consumer.start().await.expect("consumer starts before broker stop");
    wait_for_transport_event(&mut events, &group, ConsumerTransportOutcome::Success).await;
    println!("phase4_transport_ready group={group}");
    wait_for_transport_event(&mut events, &group, ConsumerTransportOutcome::Failure).await;
    consumer.shutdown().await;
}

/// Requires a live NameServer and a broker admin request issued while the
/// consumer is held active. The runner supplies a stable group through
/// `ROCKETMQ_PHASE4_ADMIN_GROUP` and invokes `mqadmin consumerStatus`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live RocketMQ broker-admin diagnostic fixture"]
async fn active_consumer_survives_admin_diagnostic_request() {
    let nameserver = std::env::var("ROCKETMQ_PHASE3_NAMESERVER")
        .expect("set ROCKETMQ_PHASE3_NAMESERVER to a live NameServer address");
    let group = std::env::var("ROCKETMQ_PHASE4_ADMIN_GROUP")
        .expect("set ROCKETMQ_PHASE4_ADMIN_GROUP to the fixture consumer group");
    let mut events = subscribe_consumer_transport_events();
    let mut consumer = consumer_with_fast_heartbeat(group.clone(), &nameserver);

    consumer.start().await.expect("consumer starts before admin request");
    wait_for_transport_event(&mut events, &group, ConsumerTransportOutcome::Success).await;
    println!("phase4_admin_ready group={group}");
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    consumer.shutdown().await;
}
