use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

struct NoopListener;

impl MessageListenerConcurrently for NoopListener {
    fn consume_message(
        &self,
        _msgs: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
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
