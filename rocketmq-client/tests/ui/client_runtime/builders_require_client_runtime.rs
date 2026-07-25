use rocketmq_client_rust::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;

fn main() {
    let _ = DefaultMQProducer::builder();
    let _ = DefaultMQPushConsumer::builder();
    let _ = DefaultLitePullConsumer::builder();
    let _ = TransactionMQProducer::builder();
    let _ = DefaultMQAdminExt::new();
}
