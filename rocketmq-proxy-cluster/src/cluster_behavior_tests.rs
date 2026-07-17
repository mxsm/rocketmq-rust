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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rocketmq_model::result::PullOutcome;
use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_proxy_core::ConsumerFilterExpression;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ReceiveTarget;
use rocketmq_security_api::SecurityRequestView;
use rocketmq_security_api::Signature;
use rocketmq_security_api::SigningError;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::*;

type EventLog = Arc<Mutex<Vec<&'static str>>>;
type SendScript = Arc<Mutex<VecDeque<Result<Option<SendResult>, RocketMQError>>>>;

struct EmptySigner;

impl OutboundSigner for EmptySigner {
    fn sign(&self, _request: SecurityRequestView<'_>) -> Result<Signature, SigningError> {
        Ok(Signature::new(Vec::new()))
    }
}

struct ProducerStartControl {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    block: Notify,
}

struct ScriptedClientIo {
    events: EventLog,
    routes: Mutex<VecDeque<Result<Option<TopicRouteData>, RocketMQError>>>,
    pulls: Mutex<VecDeque<Result<PullOutcome<MessageExt>, RocketMQError>>>,
    pops: Mutex<VecDeque<Result<PopResult, RocketMQError>>>,
    acks: Mutex<VecDeque<Result<AckResult, RocketMQError>>>,
    broker_lookup_misses: AtomicUsize,
    route_calls: AtomicUsize,
    refresh_calls: AtomicUsize,
    ack_calls: AtomicUsize,
    start_entered: Mutex<Option<oneshot::Sender<()>>>,
    start_block: Option<Arc<Notify>>,
    pull_entered: Mutex<Option<oneshot::Sender<()>>>,
    pull_block: Option<Arc<Notify>>,
    shutdown_block: Option<Arc<Notify>>,
}

impl ScriptedClientIo {
    fn new(events: EventLog) -> Self {
        Self {
            events,
            routes: Mutex::new(VecDeque::new()),
            pulls: Mutex::new(VecDeque::new()),
            pops: Mutex::new(VecDeque::new()),
            acks: Mutex::new(VecDeque::new()),
            broker_lookup_misses: AtomicUsize::new(0),
            route_calls: AtomicUsize::new(0),
            refresh_calls: AtomicUsize::new(0),
            ack_calls: AtomicUsize::new(0),
            start_entered: Mutex::new(None),
            start_block: None,
            pull_entered: Mutex::new(None),
            pull_block: None,
            shutdown_block: None,
        }
    }

    fn blocking_start(events: EventLog) -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        let mut client = Self::new(events);
        client.start_entered = Mutex::new(Some(sender));
        client.start_block = Some(Arc::new(Notify::new()));
        (client, receiver)
    }

    fn blocking_shutdown(events: EventLog) -> Self {
        let mut client = Self::new(events);
        client.shutdown_block = Some(Arc::new(Notify::new()));
        client
    }

    fn blocking_pull(events: EventLog) -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        let mut client = Self::new(events);
        client.pull_entered = Mutex::new(Some(sender));
        client.pull_block = Some(Arc::new(Notify::new()));
        (client, receiver)
    }

    fn record(&self, event: &'static str) {
        self.events.lock().expect("event log lock poisoned").push(event);
    }

    fn push_route(&self, route: TopicRouteData) {
        self.routes
            .lock()
            .expect("route script lock poisoned")
            .push_back(Ok(Some(route)));
    }

    fn push_pull(&self, outcome: PullOutcome<MessageExt>) {
        self.pulls
            .lock()
            .expect("pull script lock poisoned")
            .push_back(Ok(outcome));
    }

    fn push_pop(&self, result: PopResult) {
        self.pops
            .lock()
            .expect("pop script lock poisoned")
            .push_back(Ok(result));
    }

    fn push_ack(&self, result: AckResult) {
        self.acks
            .lock()
            .expect("ack script lock poisoned")
            .push_back(Ok(result));
    }

    fn fail_broker_lookup_times(&self, count: usize) {
        self.broker_lookup_misses.store(count, Ordering::Release);
    }

    fn scripted<T>(queue: &Mutex<VecDeque<Result<T, RocketMQError>>>, operation: &str) -> Result<T, RocketMQError> {
        queue
            .lock()
            .expect("Client script lock poisoned")
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_client_call(operation)))
    }
}

fn unexpected_client_call(operation: &str) -> RocketMQError {
    RocketMQError::IllegalArgument(format!("unexpected scripted Client operation: {operation}"))
}

#[async_trait]
impl ClusterClientIo for ScriptedClientIo {
    async fn start(&self) -> Result<(), RocketMQError> {
        self.record("client.start");
        if let Some(sender) = self
            .start_entered
            .lock()
            .expect("start notification lock poisoned")
            .take()
        {
            let _ = sender.send(());
        }
        if let Some(block) = &self.start_block {
            block.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&self) {
        self.record("client.shutdown");
        if let Some(block) = &self.shutdown_block {
            block.notified().await;
        }
    }

    async fn topic_route(&self, _topic: &str, _timeout_millis: u64) -> Result<Option<TopicRouteData>, RocketMQError> {
        self.record("client.route");
        self.route_calls.fetch_add(1, Ordering::AcqRel);
        Self::scripted(&self.routes, "topic_route")
    }

    async fn lock_batch_mq(
        &self,
        _broker_addr: &str,
        _request: LockBatchRequestBody,
        _timeout_millis: u64,
    ) -> Result<HashSet<MessageQueue>, RocketMQError> {
        Err(unexpected_client_call("lock_batch_mq"))
    }

    async fn unlock_batch_mq(
        &self,
        _broker_addr: &CheetahString,
        _request: UnlockBatchRequestBody,
        _timeout_millis: u64,
    ) -> Result<(), RocketMQError> {
        Err(unexpected_client_call("unlock_batch_mq"))
    }

    async fn query_assignment(
        &self,
        _broker_addr: &CheetahString,
        _topic: CheetahString,
        _consumer_group: CheetahString,
        _client_id: CheetahString,
        _strategy_name: CheetahString,
        _message_model: MessageModel,
        _timeout_millis: u64,
    ) -> Result<Option<Vec<MessageQueueAssignment>>, RocketMQError> {
        Err(unexpected_client_call("query_assignment"))
    }

    async fn pop_message(
        &self,
        _broker_name: &CheetahString,
        _broker_addr: &CheetahString,
        _request: PopMessageRequestHeader,
        _timeout_millis: u64,
    ) -> Result<PopResult, RocketMQError> {
        self.record("client.pop");
        Self::scripted(&self.pops, "pop_message")
    }

    async fn ack_message(
        &self,
        _broker_addr: &CheetahString,
        _request: AckMessageRequestHeader,
        _timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError> {
        self.record("client.ack");
        self.ack_calls.fetch_add(1, Ordering::AcqRel);
        Self::scripted(&self.acks, "ack_message")
    }

    async fn change_invisible_time(
        &self,
        _broker_name: &CheetahString,
        _broker_addr: &CheetahString,
        _request: ChangeInvisibleTimeRequestHeader,
        _timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError> {
        Err(unexpected_client_call("change_invisible_time"))
    }

    async fn end_transaction(
        &self,
        _broker_addr: &CheetahString,
        _request: EndTransactionRequestHeader,
        _remark: CheetahString,
        _timeout_millis: u64,
    ) -> Result<(), RocketMQError> {
        Err(unexpected_client_call("end_transaction"))
    }

    async fn find_subscribe_broker_addr(
        &self,
        _broker_name: &CheetahString,
        _broker_id: u64,
        _only_this_broker: bool,
    ) -> Option<CheetahString> {
        self.record("client.find-broker");
        let mut misses = self.broker_lookup_misses.load(Ordering::Acquire);
        while misses > 0 {
            match self.broker_lookup_misses.compare_exchange_weak(
                misses,
                misses - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return None,
                Err(actual) => misses = actual,
            }
        }
        Some(CheetahString::from("127.0.0.1:10911"))
    }

    async fn refresh_topic_route(&self, _topic: &CheetahString) -> bool {
        self.record("client.refresh-route");
        self.refresh_calls.fetch_add(1, Ordering::AcqRel);
        true
    }

    async fn broker_name_for_queue(&self, queue: &MessageQueue) -> CheetahString {
        CheetahString::from(queue.broker_name())
    }

    async fn pull_outcome_from_broker(
        &self,
        _broker_addr: &str,
        _request: PullMessageRequestHeader,
        _timeout_millis: u64,
    ) -> Result<PullOutcome<MessageExt>, RocketMQError> {
        self.record("client.pull");
        if let Some(sender) = self
            .pull_entered
            .lock()
            .expect("pull notification lock poisoned")
            .take()
        {
            let _ = sender.send(());
        }
        if let Some(block) = &self.pull_block {
            block.notified().await;
        }
        Self::scripted(&self.pulls, "pull_outcome_from_broker")
    }

    async fn consumer_send_message_back(
        &self,
        _broker_addr: &str,
        _broker_name: Option<&str>,
        _message: &MessageExt,
        _consumer_group: &str,
        _delay_level: i32,
        _timeout_millis: u64,
        _max_consume_retry_times: i32,
    ) -> Result<(), RocketMQError> {
        Err(unexpected_client_call("consumer_send_message_back"))
    }

    async fn update_consumer_offset(
        &self,
        _broker_addr: &CheetahString,
        _request: UpdateConsumerOffsetRequestHeader,
        _timeout_millis: u64,
    ) -> Result<(), RocketMQError> {
        Err(unexpected_client_call("update_consumer_offset"))
    }

    async fn query_consumer_offset(
        &self,
        _broker_addr: &str,
        _request: QueryConsumerOffsetRequestHeader,
        _timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        Err(unexpected_client_call("query_consumer_offset"))
    }

    async fn min_offset(
        &self,
        _broker_addr: &str,
        _queue: &MessageQueue,
        _timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        Err(unexpected_client_call("min_offset"))
    }

    async fn max_offset(
        &self,
        _broker_addr: &str,
        _queue: &MessageQueue,
        _timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        Err(unexpected_client_call("max_offset"))
    }

    async fn search_offset(
        &self,
        _broker_addr: &str,
        _queue: &MessageQueue,
        _timestamp: i64,
        _boundary_type: BoundaryType,
        _timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        Err(unexpected_client_call("search_offset"))
    }

    async fn topic_config(
        &self,
        _broker_addr: &CheetahString,
        _topic: CheetahString,
        _timeout_millis: u64,
    ) -> Result<rocketmq_model::topic::TopicConfig, RocketMQError> {
        Err(unexpected_client_call("topic_config"))
    }

    async fn subscription_group_config(
        &self,
        _broker_addr: &CheetahString,
        _group: CheetahString,
        _timeout_millis: u64,
    ) -> Result<SubscriptionGroupConfig, RocketMQError> {
        Err(unexpected_client_call("subscription_group_config"))
    }

    async fn broker_cluster_info(&self, _timeout_millis: u64) -> Result<ClusterInfo, RocketMQError> {
        Err(unexpected_client_call("broker_cluster_info"))
    }

    async fn user(
        &self,
        _broker_addr: CheetahString,
        _username: CheetahString,
        _timeout_millis: u64,
    ) -> Result<Option<UserInfo>, RocketMQError> {
        Err(unexpected_client_call("user"))
    }

    async fn acl(
        &self,
        _broker_addr: CheetahString,
        _subject: CheetahString,
        _timeout_millis: u64,
    ) -> Result<Option<AclInfo>, RocketMQError> {
        Err(unexpected_client_call("acl"))
    }
}

struct ScriptedProducerFactory {
    events: EventLog,
    send_results: SendScript,
    start_control: Option<Arc<ProducerStartControl>>,
}

struct CapturingClientFactory {
    client: Arc<dyn ClusterClientIo>,
    observed: Arc<Mutex<Option<(u64, bool)>>>,
}

impl ClusterClientFactory for CapturingClientFactory {
    fn get_or_create(
        &self,
        domain_id: u64,
        _client_config: RocketmqClientConfig,
        rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Result<Arc<dyn ClusterClientIo>, RocketMQError> {
        *self.observed.lock().expect("Client factory observation lock poisoned") =
            Some((domain_id, rpc_hook.is_some()));
        Ok(self.client.clone())
    }
}

impl ClusterProducerFactory for ScriptedProducerFactory {
    fn create(
        &self,
        _domain_id: u64,
        _config: &ClusterConfig,
        producer_group: &str,
        _timeout_millis: u64,
        _rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Box<dyn ClusterProducerIo> {
        self.events
            .lock()
            .expect("event log lock poisoned")
            .push("producer.create");
        Box::new(ScriptedProducer {
            events: self.events.clone(),
            send_results: self.send_results.clone(),
            group: producer_group.to_owned(),
            topics: Vec::new(),
            start_control: self.start_control.clone(),
        })
    }
}

struct ScriptedProducer {
    events: EventLog,
    send_results: SendScript,
    group: String,
    topics: Vec<CheetahString>,
    start_control: Option<Arc<ProducerStartControl>>,
}

#[async_trait]
impl ClusterProducerIo for ScriptedProducer {
    fn set_topics(&mut self, topics: Vec<CheetahString>) {
        self.topics = topics;
    }

    fn topics(&self) -> Vec<CheetahString> {
        self.topics.clone()
    }

    fn set_send_timeout(&mut self, _timeout_millis: u32) {}

    fn producer_group(&self) -> &str {
        self.group.as_str()
    }

    async fn start(&mut self) -> Result<(), RocketMQError> {
        self.events
            .lock()
            .expect("event log lock poisoned")
            .push("producer.start");
        if let Some(control) = &self.start_control {
            if let Some(sender) = control
                .entered
                .lock()
                .expect("producer start notification lock poisoned")
                .take()
            {
                let _ = sender.send(());
            }
            control.block.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&mut self) {
        self.events
            .lock()
            .expect("event log lock poisoned")
            .push("producer.shutdown");
    }

    async fn recall_message(
        &mut self,
        _topic: CheetahString,
        _recall_handle: CheetahString,
    ) -> Result<String, RocketMQError> {
        Err(unexpected_client_call("producer.recall_message"))
    }

    async fn fetch_publish_message_queues(&mut self, _topic: &str) -> Result<Vec<MessageQueue>, RocketMQError> {
        Err(unexpected_client_call("producer.fetch_publish_message_queues"))
    }

    async fn send(&mut self, _message: Message, _timeout_millis: u64) -> Result<Option<SendResult>, RocketMQError> {
        self.events
            .lock()
            .expect("event log lock poisoned")
            .push("producer.send");
        self.send_results
            .lock()
            .expect("send script lock poisoned")
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_client_call("producer.send")))
    }

    async fn send_to_queue(
        &mut self,
        message: Message,
        _queue: MessageQueue,
        timeout_millis: u64,
    ) -> Result<Option<SendResult>, RocketMQError> {
        self.send(message, timeout_millis).await
    }
}

async fn run_test_worker<T, F, Fut>(
    client: Arc<dyn ClusterClientIo>,
    producer_factory: Arc<dyn ClusterProducerFactory>,
    scenario: F,
) -> T
where
    F: FnOnce(ClusterTaskExecutor, CancellationToken) -> Fut,
    Fut: Future<Output = T>,
{
    run_test_worker_with_config(ClusterConfig::default(), client, producer_factory, scenario).await
}

async fn run_test_worker_with_config<T, F, Fut>(
    config: ClusterConfig,
    client: Arc<dyn ClusterClientIo>,
    producer_factory: Arc<dyn ClusterProducerFactory>,
    scenario: F,
) -> T
where
    F: FnOnce(ClusterTaskExecutor, CancellationToken) -> Fut,
    Fut: Future<Output = T>,
{
    let state = ClusterWorkerState::with_test_runtime(client, producer_factory);
    run_scripted_worker(config, state, scenario).await
}

async fn run_scripted_worker<T, F, Fut>(config: ClusterConfig, state: ClusterWorkerState, scenario: F) -> T
where
    F: FnOnce(ClusterTaskExecutor, CancellationToken) -> Fut,
    Fut: Future<Output = T>,
{
    let (sender, receiver) = mpsc::channel(CLUSTER_COMMAND_CAPACITY);
    let executor = ClusterTaskExecutor {
        sender,
        startup_error: None,
    };
    let cancellation = CancellationToken::new();
    let scenario = scenario(executor, cancellation.clone());
    let worker = run_cluster_worker_with_state(config, state, receiver, cancellation);
    let ((), result) = tokio::join!(worker, scenario);
    result
}

fn found_message(message_id: &str, body: &'static [u8]) -> MessageExt {
    let mut message = MessageExt::default();
    message.set_topic(CheetahString::from("TopicA"));
    message.set_body(bytes::Bytes::from_static(body));
    message.set_msg_id(CheetahString::from(message_id));
    message.set_broker_name(CheetahString::from("broker-a"));
    message.set_queue_id(0);
    message
}

fn filter_expression() -> ConsumerFilterExpression {
    ConsumerFilterExpression {
        expression_type: "TAG".to_owned(),
        expression: "*".to_owned(),
    }
}

fn target() -> MessageQueueTarget {
    MessageQueueTarget {
        topic: ResourceIdentity::new("", "TopicA"),
        queue_id: 0,
        broker_name: Some("broker-a".to_owned()),
        broker_addr: Some("127.0.0.1:10911".to_owned()),
    }
}

#[tokio::test]
async fn worker_maps_route_pull_pop_and_ack_with_retry() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_route(TopicRouteData::default());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        7,
        1,
        11,
        None::<Vec<MessageExt>>,
    ));
    client.push_pop(PopResult {
        pop_status: PopStatus::NoNewMsg,
        pop_time: 10,
        invisible_time: 30_000,
        ..Default::default()
    });
    client.push_ack(AckResult::default());
    client.fail_broker_lookup_times(1);
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        executor
            .query_route(ResourceIdentity::new("", "TopicA"))
            .await
            .expect("first route query");
        executor
            .query_route(ResourceIdentity::new("", "TopicA"))
            .await
            .expect("cached route query");

        let pull = executor
            .pull_message(
                PullMessageRequest {
                    group: ResourceIdentity::new("", "GroupA"),
                    target: target(),
                    offset: 5,
                    batch_size: 16,
                    filter_expression: filter_expression(),
                    long_polling_timeout: Duration::from_millis(50),
                },
                None,
            )
            .await
            .expect("pull command");
        assert!(!pull.status.is_ok());
        assert_eq!((pull.next_offset, pull.min_offset, pull.max_offset), (7, 1, 11));
        assert!(pull.messages.is_empty());

        let receive = executor
            .receive_message(
                ReceiveMessageRequest {
                    group: ResourceIdentity::new("", "GroupA"),
                    target: ReceiveTarget {
                        topic: ResourceIdentity::new("", "TopicA"),
                        queue_id: 0,
                        broker_name: Some("broker-a".to_owned()),
                        broker_addr: Some("127.0.0.1:10911".to_owned()),
                        fifo: false,
                    },
                    filter_expression: filter_expression(),
                    batch_size: 16,
                    invisible_duration: Duration::from_secs(30),
                    auto_renew: false,
                    long_polling_timeout: Duration::from_millis(50),
                    attempt_id: None,
                },
                None,
            )
            .await
            .expect("receive command");
        assert!(!receive.status.is_ok());
        assert!(receive.messages.is_empty());

        let receipt_handle = ExtraInfoUtil::build_extra_info_with_offset(0, 1, 30_000, 0, "TopicA", "broker-a", 0, 7);
        let ack = executor
            .ack_message(
                AckMessageRequest {
                    group: ResourceIdentity::new("", "GroupA"),
                    topic: ResourceIdentity::new("", "TopicA"),
                    entries: vec![AckMessageEntry {
                        message_id: "message-id".to_owned(),
                        receipt_handle,
                        lite_topic: None,
                    }],
                },
                None,
            )
            .await
            .expect("ack command");
        assert_eq!(ack.len(), 1);
        assert!(ack[0].status.is_ok());
        cancellation.cancel();
    })
    .await;

    assert_eq!(client.route_calls.load(Ordering::Acquire), 1);
    assert_eq!(client.refresh_calls.load(Ordering::Acquire), 1);
    assert_eq!(client.ack_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        events.lock().expect("event log lock poisoned").last(),
        Some(&"client.shutdown")
    );
}

#[tokio::test]
async fn worker_preserves_owned_messages_for_found_pull_and_pop_results() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_pull(PullOutcome::new(
        PullStatus::Found,
        8,
        1,
        12,
        vec![found_message("pull-message", b"pull-body")],
    ));
    client.push_pop(PopResult {
        msg_found_list: Some(vec![found_message("pop-message", b"pop-body")]),
        pop_status: PopStatus::Found,
        pop_time: 10,
        invisible_time: 30_000,
        rest_num: 0,
    });
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client, factory, |executor, cancellation| async move {
        let pull = executor
            .pull_message(
                PullMessageRequest {
                    group: ResourceIdentity::new("", "GroupA"),
                    target: target(),
                    offset: 5,
                    batch_size: 16,
                    filter_expression: filter_expression(),
                    long_polling_timeout: Duration::from_millis(50),
                },
                None,
            )
            .await
            .expect("found pull command");
        assert!(pull.status.is_ok());
        assert_eq!(pull.messages.len(), 1);
        assert_eq!(pull.messages[0].topic(), "TopicA");
        assert_eq!(pull.messages[0].body(), Some(b"pull-body".as_slice()));
        assert_eq!(pull.messages[0].msg_id, "pull-message");

        let receive = executor
            .receive_message(
                ReceiveMessageRequest {
                    group: ResourceIdentity::new("", "GroupA"),
                    target: ReceiveTarget {
                        topic: ResourceIdentity::new("", "TopicA"),
                        queue_id: 0,
                        broker_name: Some("broker-a".to_owned()),
                        broker_addr: Some("127.0.0.1:10911".to_owned()),
                        fifo: false,
                    },
                    filter_expression: filter_expression(),
                    batch_size: 16,
                    invisible_duration: Duration::from_secs(30),
                    auto_renew: false,
                    long_polling_timeout: Duration::from_millis(50),
                    attempt_id: None,
                },
                None,
            )
            .await
            .expect("found pop command");
        assert!(receive.status.is_ok());
        assert_eq!(receive.messages.len(), 1);
        assert_eq!(receive.messages[0].message.topic(), "TopicA");
        assert_eq!(receive.messages[0].message.body(), Some(b"pop-body".as_slice()));
        assert_eq!(receive.messages[0].message.msg_id, "pop-message");
        cancellation.cancel();
    })
    .await;
}

#[tokio::test]
async fn worker_passes_the_outbound_signer_hook_to_the_client_transport_factory() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_route(TopicRouteData::default());
    let observed = Arc::new(Mutex::new(None));
    let client_factory = Arc::new(CapturingClientFactory {
        client,
        observed: observed.clone(),
    });
    let producer_factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let domain_id = 71_010;
    let state = ClusterWorkerState::with_test_factories(
        domain_id,
        Some(rpc_hook_from_outbound_signer(Arc::new(EmptySigner))),
        client_factory,
        producer_factory,
    );

    run_scripted_worker(ClusterConfig::default(), state, |executor, cancellation| async move {
        executor
            .query_route(ResourceIdentity::new("", "TopicA"))
            .await
            .expect("signed Client route command");
        cancellation.cancel();
    })
    .await;

    assert_eq!(
        *observed.lock().expect("Client factory observation lock poisoned"),
        Some((domain_id, true))
    );
}

#[tokio::test]
async fn worker_shutdown_is_bounded_by_one_shared_deadline() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::blocking_shutdown(events.clone()));
    client.push_route(TopicRouteData::default());
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let config = ClusterConfig {
        shutdown_timeout_ms: 1,
        ..Default::default()
    };

    tokio::time::timeout(
        Duration::from_millis(250),
        run_test_worker_with_config(config, client, factory, |executor, cancellation| async move {
            executor
                .query_route(ResourceIdentity::new("", "TopicA"))
                .await
                .expect("route command before shutdown");
            cancellation.cancel();
        }),
    )
    .await
    .expect("worker shutdown must honor its configured deadline");

    assert_eq!(
        events.lock().expect("event log lock poisoned").as_slice(),
        ["client.start", "client.route", "client.shutdown"]
    );
}

#[tokio::test]
async fn worker_maps_send_results_and_orders_shutdown() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    let send_results = Arc::new(Mutex::new(VecDeque::from([
        Ok(Some(SendResult::default())),
        Err(RocketMQError::IllegalArgument("scripted send failure".to_owned())),
    ])));
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results,
        start_control: None,
    });
    run_test_worker(client, factory, |executor, cancellation| async move {
        let entries = executor
            .send_message(
                SendMessageRequest {
                    messages: vec![
                        SendMessageEntry {
                            topic: ResourceIdentity::new("", "TopicA"),
                            client_message_id: "client-message-1".to_owned(),
                            message: ProxyMessage::new("TopicA", b"first".to_vec()),
                            queue_id: None,
                        },
                        SendMessageEntry {
                            topic: ResourceIdentity::new("", "TopicA"),
                            client_message_id: "client-message-2".to_owned(),
                            message: ProxyMessage::new("TopicA", b"second".to_vec()),
                            queue_id: None,
                        },
                    ],
                    timeout: Some(Duration::from_millis(100)),
                },
                Some("client-a".to_owned()),
                "request-a".to_owned(),
            )
            .await
            .expect("send command");
        assert_eq!(entries.len(), 2);
        assert!(entries[0].status.is_ok());
        assert!(entries[0].send_result.is_some());
        assert!(!entries[1].status.is_ok());
        assert!(entries[1].send_result.is_none());
        cancellation.cancel();
    })
    .await;

    let events = events.lock().expect("event log lock poisoned");
    let producer_shutdown = events
        .iter()
        .position(|event| *event == "producer.shutdown")
        .expect("producer shutdown event");
    let client_shutdown = events
        .iter()
        .position(|event| *event == "client.shutdown")
        .expect("Client shutdown event");
    assert!(producer_shutdown < client_shutdown, "events: {events:?}");
}

#[tokio::test]
async fn cancellation_during_client_start_still_shuts_down_owned_client() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, start_entered) = ScriptedClientIo::blocking_start(events.clone());
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let error = run_test_worker(client, factory, |executor, cancellation| async move {
        let command = executor.query_route(ResourceIdentity::new("", "TopicA"));
        let cancel = async move {
            start_entered.await.expect("Client start was entered");
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(command, cancel);
        result.expect_err("cancelled startup must not produce a route")
    })
    .await;
    assert!(matches!(error, ProxyError::Transport { .. }));
    assert_eq!(
        events.lock().expect("event log lock poisoned").as_slice(),
        ["client.start", "client.shutdown"]
    );
}

#[tokio::test]
async fn cancellation_during_active_pull_cancels_the_command_and_shuts_down_client() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    let error = run_test_worker(client, factory, |executor, cancellation| async move {
        let command = executor.pull_message(
            PullMessageRequest {
                group: ResourceIdentity::new("", "GroupA"),
                target: target(),
                offset: 5,
                batch_size: 16,
                filter_expression: filter_expression(),
                long_polling_timeout: Duration::from_millis(50),
            },
            None,
        );
        let cancel = async move {
            pull_entered.await.expect("pull operation was entered");
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(command, cancel);
        result.expect_err("cancelled pull must not produce a response")
    })
    .await;

    assert!(matches!(error, ProxyError::Transport { .. }));
    assert_eq!(
        events.lock().expect("event log lock poisoned").as_slice(),
        ["client.start", "client.pull", "client.shutdown"]
    );
}

#[tokio::test]
async fn cancellation_during_producer_start_still_shuts_down_producer_before_client() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    let (sender, start_entered) = oneshot::channel();
    let start_control = Arc::new(ProducerStartControl {
        entered: Mutex::new(Some(sender)),
        block: Notify::new(),
    });
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: Some(start_control),
    });
    let error = run_test_worker(client, factory, |executor, cancellation| async move {
        let command = executor.send_message(
            SendMessageRequest {
                messages: vec![SendMessageEntry {
                    topic: ResourceIdentity::new("", "TopicA"),
                    client_message_id: "client-message".to_owned(),
                    message: ProxyMessage::new("TopicA", b"body".to_vec()),
                    queue_id: None,
                }],
                timeout: Some(Duration::from_millis(100)),
            },
            None,
            "request".to_owned(),
        );
        let cancel = async move {
            start_entered.await.expect("producer start was entered");
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(command, cancel);
        result.expect_err("cancelled producer startup must not send")
    });
    let error = error.await;
    assert!(matches!(error, ProxyError::Transport { .. }));
    let events = events.lock().expect("event log lock poisoned");
    assert_eq!(
        events.as_slice(),
        [
            "client.start",
            "producer.create",
            "producer.start",
            "producer.shutdown",
            "client.shutdown",
        ]
    );
}
