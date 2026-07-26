use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::TransactionMQProducer;

fn main() {
    let _ = DefaultMQProducer::builder();
    let _ = DefaultMQPushConsumer::builder();
    let _ = DefaultLitePullConsumer::builder();
    let _ = TransactionMQProducer::builder();
    let _ = DefaultMQAdminExt::new();
}
