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
    route_panics: AtomicUsize,
    route_calls: AtomicUsize,
    refresh_calls: AtomicUsize,
    ack_calls: AtomicUsize,
    pull_calls: AtomicUsize,
    readiness_calls: AtomicUsize,
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
            route_panics: AtomicUsize::new(0),
            route_calls: AtomicUsize::new(0),
            refresh_calls: AtomicUsize::new(0),
            ack_calls: AtomicUsize::new(0),
            pull_calls: AtomicUsize::new(0),
            readiness_calls: AtomicUsize::new(0),
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

    fn push_ack_error(&self, message: &str) {
        self.acks
            .lock()
            .expect("ack script lock poisoned")
            .push_back(Err(RocketMQError::IllegalArgument(message.to_owned())));
    }

    fn fail_broker_lookup_times(&self, count: usize) {
        self.broker_lookup_misses.store(count, Ordering::Release);
    }

    fn panic_route_times(&self, count: usize) {
        self.route_panics.store(count, Ordering::Release);
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
        if self
            .route_panics
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            panic!("scripted route panic");
        }
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

    async fn batch_ack_message(
        &self,
        _broker_addr: &CheetahString,
        _request: BatchAckMessageRequestBody,
        _timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError> {
        self.record("client.batch-ack");
        self.ack_calls.fetch_add(1, Ordering::AcqRel);
        Self::scripted(&self.acks, "batch_ack_message")
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
        self.pull_calls.fetch_add(1, Ordering::AcqRel);
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
        self.readiness_calls.fetch_add(1, Ordering::AcqRel);
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
        _client_runtime: &ClientRuntime,
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
        _client_runtime: Arc<ClientRuntime>,
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

    fn producer_group(&self) -> CheetahString {
        self.group.clone().into()
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
    run_scripted_worker_with_policy(config, state, ClusterExecutionPolicy::default(), scenario).await
}

async fn run_scripted_worker_with_policy<T, F, Fut>(
    config: ClusterConfig,
    state: ClusterWorkerState,
    policy: ClusterExecutionPolicy,
    scenario: F,
) -> T
where
    F: FnOnce(ClusterTaskExecutor, CancellationToken) -> Fut,
    Fut: Future<Output = T>,
{
    let service = test_service_context("proxy-cluster-scripted");
    let executor_result = ClusterTaskExecutor::new_with_test_state(config, state, &service, policy);
    assert!(executor_result.is_ok(), "scripted cluster execution starts");
    let Some((executor, cancellation)) = executor_result.ok() else {
        std::process::abort();
    };
    let result = scenario(executor, cancellation.clone()).await;
    cancellation.cancel();
    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
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

fn pull_request(group: &str) -> PullMessageRequest {
    PullMessageRequest {
        group: ResourceIdentity::new("", group),
        target: target(),
        offset: 5,
        batch_size: 16,
        filter_expression: filter_expression(),
        long_polling_timeout: Duration::from_secs(1),
    }
}

fn ack_request(group: &str) -> AckMessageRequest {
    AckMessageRequest {
        group: ResourceIdentity::new("", group),
        topic: ResourceIdentity::new("", "TopicA"),
        entries: vec![AckMessageEntry {
            message_id: "message-id".to_owned(),
            receipt_handle: ExtraInfoUtil::build_extra_info_with_offset(0, 1, 30_000, 0, "TopicA", "broker-a", 0, 7),
            lite_topic: None,
        }],
    }
}

fn batch_ack_request(group: &str, count: usize) -> AckMessageRequest {
    AckMessageRequest {
        group: ResourceIdentity::new("", group),
        topic: ResourceIdentity::new("", "TopicA"),
        entries: (0..count)
            .map(|offset| AckMessageEntry {
                message_id: format!("message-{offset}"),
                receipt_handle: ExtraInfoUtil::build_extra_info_with_offset(
                    100,
                    1,
                    30_000,
                    0,
                    "TopicA",
                    "broker-a",
                    0,
                    100 + offset as i64,
                ),
                lite_topic: None,
            })
            .collect(),
    }
}

#[tokio::test]
async fn compatible_ack_entries_use_one_broker_batch() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_ack(AckResult::default());
    let factory = Arc::new(ScriptedProducerFactory {
        events: events.clone(),
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        let results = executor
            .ack_message(batch_ack_request("GroupA", 32), Some(Duration::from_secs(1)))
            .await
            .expect("batch ACK command");
        assert_eq!(results.len(), 32);
        assert!(results.iter().all(|result| result.status.is_ok()));
        assert_eq!(client.ack_calls.load(Ordering::Acquire), 1);
        assert_eq!(
            events
                .lock()
                .expect("event log lock poisoned")
                .iter()
                .filter(|event| **event == "client.batch-ack")
                .count(),
            1
        );
        cancellation.cancel();
    })
    .await;
}

#[tokio::test]
async fn failed_batch_falls_back_once_per_entry() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_ack_error("batch unavailable");
    client.push_ack(AckResult::default());
    client.push_ack(AckResult::default());
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        let results = executor
            .ack_message(batch_ack_request("GroupA", 2), Some(Duration::from_secs(1)))
            .await
            .expect("fallback ACK command");
        assert!(results.iter().all(|result| result.status.is_ok()));
        assert_eq!(client.ack_calls.load(Ordering::Acquire), 3);
        cancellation.cancel();
    })
    .await;
}

#[tokio::test]
async fn malformed_receipt_failure_is_mapped_to_only_that_entry() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_ack(AckResult::default());
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let mut request = batch_ack_request("GroupA", 2);
    request.entries[1].receipt_handle = "malformed".to_owned();

    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        let results = executor
            .ack_message(request, Some(Duration::from_secs(1)))
            .await
            .expect("partially valid ACK command");
        assert_eq!(results.len(), 2);
        assert!(results[0].status.is_ok());
        assert!(!results[1].status.is_ok());
        assert_eq!(results[1].status.code(), v2::Code::InvalidReceiptHandle as i32);
        assert_eq!(client.ack_calls.load(Ordering::Acquire), 1);
        cancellation.cancel();
    })
    .await;
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_millis(250), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deterministic execution condition was not reached");
}

fn shutdown_report_panics(report: &rocketmq_runtime::ShutdownReport) -> usize {
    report.panicked + report.children.iter().map(shutdown_report_panics).sum::<usize>()
}

#[tokio::test]
async fn unrelated_route_progresses_while_pull_is_blocked() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        7,
        1,
        11,
        None::<Vec<MessageExt>>,
    ));
    client.push_route(TopicRouteData::default());
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        let pull = executor.pull_message(
            PullMessageRequest {
                group: ResourceIdentity::new("", "BlockedGroup"),
                target: target(),
                offset: 5,
                batch_size: 16,
                filter_expression: filter_expression(),
                long_polling_timeout: Duration::from_secs(1),
            },
            Some(Duration::from_secs(2)),
        );
        let probe = async {
            let pull_started = pull_entered.await;
            assert!(pull_started.is_ok(), "pull operation was entered");
            let route = executor.query_route(ResourceIdentity::new("", "IndependentTopic"));
            let observe = async {
                let progress = tokio::time::timeout(Duration::from_millis(250), async {
                    while client.route_calls.load(Ordering::Acquire) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
                if let Some(block) = &client.pull_block {
                    block.notify_one();
                }
                progress
            };
            let (route_result, progress) = tokio::join!(route, observe);
            assert!(route_result.is_ok(), "unrelated route query");
            progress
        };
        let (pull_result, route_progress) = tokio::join!(pull, probe);
        assert!(pull_result.is_ok(), "blocked pull result");
        cancellation.cancel();
        assert!(
            route_progress.is_ok(),
            "an unrelated route query must not wait behind a blocked pull"
        );
    })
    .await;
}

#[tokio::test]
async fn same_consumer_key_is_fifo_without_serializing_distinct_keys() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, first_pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        11,
        1,
        21,
        None::<Vec<MessageExt>>,
    ));
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        22,
        1,
        32,
        None::<Vec<MessageExt>>,
    ));
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        let first = executor.pull_message(pull_request("GroupA"), Some(Duration::from_secs(2)));
        let probe = async {
            first_pull_entered.await.expect("first pull entered remote I/O");
            let second = executor.pull_message(pull_request("GroupA"), Some(Duration::from_secs(2)));
            let observe = async {
                wait_until(|| executor.lanes.snapshot().queued_and_active == 2).await;
                assert_eq!(
                    client.pull_calls.load(Ordering::Acquire),
                    1,
                    "the second same-key command must remain queued"
                );
                client.pull_block.as_ref().expect("blocking pull control").notify_one();
                wait_until(|| client.pull_calls.load(Ordering::Acquire) == 2).await;
                client.pull_block.as_ref().expect("blocking pull control").notify_one();
            };
            let (second, ()) = tokio::join!(second, observe);
            second
        };
        let (first, second) = tokio::join!(first, probe);
        assert_eq!(first.expect("first pull").next_offset, 11);
        assert_eq!(second.expect("second pull").next_offset, 22);
        cancellation.cancel();
    })
    .await;
}

#[tokio::test]
async fn distinct_consumer_keys_reach_remote_io_concurrently() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, first_pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        11,
        1,
        21,
        None::<Vec<MessageExt>>,
    ));
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        22,
        1,
        32,
        None::<Vec<MessageExt>>,
    ));
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client.clone(), factory, |executor, cancellation| async move {
        let first = executor.pull_message(pull_request("GroupA"), Some(Duration::from_secs(2)));
        let probe = async {
            first_pull_entered.await.expect("first pull entered remote I/O");
            let second = executor.pull_message(pull_request("GroupB"), Some(Duration::from_secs(2)));
            let observe = async {
                wait_until(|| client.pull_calls.load(Ordering::Acquire) == 2).await;
                assert_eq!(executor.lanes.snapshot().current_inflight, 2);
                client
                    .pull_block
                    .as_ref()
                    .expect("blocking pull control")
                    .notify_waiters();
            };
            let (second, ()) = tokio::join!(second, observe);
            second
        };
        let (first, second) = tokio::join!(first, probe);
        assert!(first.is_ok(), "first distinct-key pull");
        assert!(second.is_ok(), "second distinct-key pull");
        assert_eq!(executor.lanes.snapshot().max_inflight, 2);
        cancellation.cancel();
    })
    .await;
}

#[tokio::test]
async fn long_poll_saturation_preserves_short_data_and_control_capacity() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, first_pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        11,
        1,
        21,
        None::<Vec<MessageExt>>,
    ));
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        22,
        1,
        32,
        None::<Vec<MessageExt>>,
    ));
    client.push_route(TopicRouteData::default());
    client.push_ack(AckResult::default());
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let state = ClusterWorkerState::with_test_runtime(client.clone(), factory);
    let policy = ClusterExecutionPolicy {
        capacity_count: 3,
        capacity_bytes: 16 * 1024,
        max_queue_age: Duration::from_secs(2),
        io_max_inflight: 2,
        control_reserve: 1,
        long_poll_max_inflight: 1,
        lane_idle_timeout: Duration::from_secs(1),
    };

    run_scripted_worker_with_policy(
        ClusterConfig::default(),
        state,
        policy,
        |executor, cancellation| async move {
            let first = executor.pull_message(pull_request("GroupA"), Some(Duration::from_secs(2)));
            let probe = async {
                first_pull_entered.await.expect("first pull entered remote I/O");
                let second = executor.pull_message(pull_request("GroupB"), Some(Duration::from_secs(2)));
                let checks = async {
                    wait_until(|| {
                        let snapshot = executor.lanes.snapshot();
                        snapshot.long_poll_queued_and_active == 2 && snapshot.current_long_poll_inflight == 1
                    })
                    .await;
                    executor
                        .query_route(ResourceIdentity::new("", "DataOverflow"))
                        .await
                        .expect("short-data request must not wait behind long polling");
                    let ack = executor
                        .ack_message(ack_request("GroupA"), Some(Duration::from_secs(1)))
                        .await
                        .expect("control request must not wait behind long polling");
                    assert_eq!(ack.len(), 1);
                    assert!(ack[0].status.is_ok());
                    assert_eq!(client.route_calls.load(Ordering::Acquire), 1);
                    assert_eq!(client.ack_calls.load(Ordering::Acquire), 1);
                    cancellation.cancel();
                };
                let (second, ()) = tokio::join!(second, checks);
                second
            };
            let (first, second) = tokio::join!(first, probe);
            assert!(first.is_err(), "shutdown cancels the active data command");
            assert!(second.is_err(), "shutdown cancels the queued data command");
            wait_until(|| {
                let snapshot = executor.lanes.snapshot();
                snapshot.current_inflight == 0
                    && snapshot.current_long_poll_inflight == 0
                    && snapshot.queued_and_active == 0
                    && snapshot.active_keys == 0
            })
            .await;
            assert_eq!(executor.lanes.snapshot().oldest_queued_age_ms, None);
        },
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn idle_key_lane_is_reclaimed_without_leaking_budget_or_tasks() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.push_route(TopicRouteData::default());
    client.push_route(TopicRouteData::default());
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let state = ClusterWorkerState::with_test_runtime(client, factory);
    let policy = ClusterExecutionPolicy {
        capacity_count: 4,
        capacity_bytes: 16 * 1024,
        max_queue_age: Duration::from_secs(1),
        io_max_inflight: 2,
        control_reserve: 1,
        long_poll_max_inflight: 2,
        lane_idle_timeout: Duration::from_millis(10),
    };

    run_scripted_worker_with_policy(
        ClusterConfig::default(),
        state,
        policy,
        |executor, cancellation| async move {
            executor
                .query_route(ResourceIdentity::new("", "TopicA"))
                .await
                .expect("route command");
            assert_eq!(executor.lanes.snapshot().active_keys, 1);
            tokio::time::advance(Duration::from_millis(10)).await;
            wait_until(|| {
                let snapshot = executor.lanes.snapshot();
                snapshot.active_keys == 0
                    && snapshot.active_lane_tasks == 0
                    && snapshot.current_inflight == 0
                    && snapshot.queued_and_active == 0
            })
            .await;
            executor
                .query_route(ResourceIdentity::new("", "TopicA"))
                .await
                .expect("retired exact key is recreated with a new generation");
            assert_eq!(executor.lanes.snapshot().active_keys, 1);
            tokio::time::advance(Duration::from_millis(10)).await;
            wait_until(|| executor.lanes.snapshot().active_lane_tasks == 0).await;
            tokio::time::resume();
            cancellation.cancel();
        },
    )
    .await;
}

#[tokio::test]
async fn active_long_poll_uses_only_the_long_poll_budget() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        7,
        1,
        11,
        None::<Vec<MessageExt>>,
    ));
    client.push_route(TopicRouteData::default());
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let state = ClusterWorkerState::with_test_runtime(client.clone(), factory);
    let policy = ClusterExecutionPolicy {
        capacity_count: 2,
        capacity_bytes: 4 * 1024,
        max_queue_age: Duration::from_secs(1),
        io_max_inflight: 2,
        control_reserve: 1,
        long_poll_max_inflight: 1,
        lane_idle_timeout: Duration::from_secs(1),
    };

    run_scripted_worker_with_policy(
        ClusterConfig::default(),
        state,
        policy,
        |executor, cancellation| async move {
            let pull = executor.pull_message(
                PullMessageRequest {
                    group: ResourceIdentity::new("", "BlockedGroup"),
                    target: target(),
                    offset: 5,
                    batch_size: 16,
                    filter_expression: filter_expression(),
                    long_polling_timeout: Duration::from_secs(1),
                },
                Some(Duration::from_secs(2)),
            );
            let probe = async {
                let pull_started = pull_entered.await;
                assert!(pull_started.is_ok(), "pull operation was entered");
                let snapshot = executor.lanes.snapshot();
                assert_eq!(snapshot.long_poll_queued_and_active, 1);
                assert_eq!(snapshot.current_long_poll_inflight, 1);
                assert_eq!(executor.lanes.root_budget.snapshot().current_count, 0);
                executor
                    .query_route(ResourceIdentity::new("", "IndependentTopic"))
                    .await
                    .expect("short-data admission remains available during long polling");
                if let Some(block) = &client.pull_block {
                    block.notify_one();
                }
            };
            let (pull_result, ()) = tokio::join!(pull, probe);
            assert!(pull_result.is_ok(), "blocked pull result");
            cancellation.cancel();
            wait_until(|| {
                let snapshot = executor.lanes.snapshot();
                snapshot.current_long_poll_inflight == 0 && snapshot.long_poll_queued_and_active == 0
            })
            .await;
        },
    )
    .await;
}

#[tokio::test]
async fn request_timeout_cancels_remote_io_and_releases_all_permits() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (client, pull_entered) = ScriptedClientIo::blocking_pull(events.clone());
    client.push_pull(PullOutcome::new(
        PullStatus::NoNewMsg,
        7,
        1,
        11,
        None::<Vec<MessageExt>>,
    ));
    let client = Arc::new(client);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });

    run_test_worker(client, factory, |executor, cancellation| async move {
        let request = executor.pull_message(pull_request("TimedOutGroup"), Some(Duration::from_millis(10)));
        let observe = async {
            pull_entered.await.expect("pull entered remote I/O");
        };
        let (result, ()) = tokio::join!(request, observe);
        assert!(matches!(
            result,
            Err(ProxyError::RocketMQ(RocketMQError::Timeout {
                operation: "proxy cluster command",
                timeout_ms: 10,
            }))
        ));
        wait_until(|| {
            let snapshot = executor.lanes.snapshot();
            snapshot.current_inflight == 0
                && snapshot.queued_and_active == 0
                && snapshot.cancelled == 1
                && snapshot.timed_out == 1
        })
        .await;
        cancellation.cancel();
        wait_until(|| executor.lanes.snapshot().active_lane_tasks == 0).await;
    })
    .await;
}

#[tokio::test]
async fn panicked_lane_is_reclaimed_and_closes_without_leaks() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    client.panic_route_times(1);
    let factory = Arc::new(ScriptedProducerFactory {
        events,
        send_results: Arc::new(Mutex::new(VecDeque::new())),
        start_control: None,
    });
    let state = ClusterWorkerState::with_test_runtime(client, factory);
    let service = test_service_context("proxy-cluster-panicked-lane");
    let executor_result = ClusterTaskExecutor::new_with_test_state(
        ClusterConfig::default(),
        state,
        &service,
        ClusterExecutionPolicy::default(),
    );
    assert!(executor_result.is_ok(), "scripted cluster execution starts");
    let Some((executor, cancellation)) = executor_result.ok() else {
        std::process::abort();
    };

    let first = executor
        .query_route(ResourceIdentity::new("", "RecoveredTopic"))
        .await
        .expect_err("first keyed lane panics");
    assert!(matches!(first, ProxyError::Transport { .. }));
    wait_until(|| {
        let snapshot = executor.lanes.snapshot();
        snapshot.active_keys == 0
            && snapshot.active_lane_tasks == 0
            && snapshot.current_inflight == 0
            && snapshot.queued_and_active == 0
    })
    .await;

    let closed = executor
        .query_route(ResourceIdentity::new("", "RecoveredTopic"))
        .await
        .expect_err("a lane panic cancels the owning execution task group");
    assert!(
        matches!(closed, ProxyError::Transport { .. }),
        "unexpected post-panic lane error: {closed:?}"
    );
    cancellation.cancel();
    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.leaked, 0, "{}", report.to_json());
    assert_eq!(report.detached_still_running, 0, "{}", report.to_json());
    assert_eq!(shutdown_report_panics(&report), 1, "{}", report.to_json());
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
        test_client_runtime(),
        domain_id,
        Some(rpc_hook_from_outbound_signer(Arc::new(EmptySigner))),
        client_factory,
        producer_factory,
    );
    let topic = ResourceIdentity::new("", "TopicA");
    let expected_domain_id = domain_id;

    run_scripted_worker(ClusterConfig::default(), state, |executor, cancellation| async move {
        let route_result = executor.query_route(topic).await;
        assert!(route_result.is_ok(), "signed Client route command");
        cancellation.cancel();
    })
    .await;

    let observed = observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(*observed, Some((expected_domain_id, true)));
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
                    timeout: None,
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
                timeout: None,
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
    assert!(
        matches!(error, ProxyError::Transport { .. }),
        "producer startup cancellation must surface as transport shutdown: {error:?}"
    );
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
