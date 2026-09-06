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

#[allow(unused_imports)]
use super::lifecycle::*;
#[allow(unused_imports)]
use super::retry::*;
#[allow(unused_imports)]
use super::send::*;
#[allow(unused_imports)]
use super::transaction::*;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;

use super::*;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::transaction_listener::TransactionListener;
use bytes::Bytes;

struct CountingCompressor {
    calls: AtomicUsize,
}

impl Compressor for CountingCompressor {
    fn compress(&self, src: &[u8], _level: i32) -> rocketmq_error::RocketMQResult<Bytes> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Bytes::copy_from_slice(src))
    }

    fn decompress(&self, src: &[u8]) -> rocketmq_error::RocketMQResult<Bytes> {
        Ok(Bytes::copy_from_slice(src))
    }
}

static COUNTING_COMPRESSOR: CountingCompressor = CountingCompressor {
    calls: AtomicUsize::new(0),
};

#[derive(Default)]
struct ProducerRoutePreparationProcessor {
    responses: std::sync::Mutex<std::collections::VecDeque<RemotingCommand>>,
    primary_responses: std::sync::Mutex<std::collections::VecDeque<RemotingCommand>>,
    topics: std::sync::Mutex<Vec<CheetahString>>,
    primary_sends: AtomicUsize,
}

impl rocketmq_transport::test_support::SessionProcessor for ProducerRoutePreparationProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = rocketmq_error::RocketMQResult<RemotingCommand>> + Send + '_>>
    {
        Box::pin(async move {
            if request.code() != rocketmq_protocol::code::request_code::RequestCode::GetRouteinfoByTopic.to_i32() {
                self.primary_sends.fetch_add(1, Ordering::SeqCst);
                let response = self
                    .primary_responses
                    .lock()
                    .expect("primary responses lock")
                    .pop_front()
                    .ok_or_else(|| RocketMQError::illegal_argument("unexpected primary send"))?;
                return Ok(response.set_opaque(request.opaque()));
            }
            let header = request.decode_command_custom_header::<
                rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader,
            >()?;
            self.topics.lock().expect("route topics lock").push(header.topic);
            let response = self
                .responses
                .lock()
                .expect("route responses lock")
                .pop_front()
                .ok_or_else(|| RocketMQError::illegal_argument("unexpected producer route request"))?;
            Ok(response.set_opaque(request.opaque()))
        })
    }
}

fn producer_default_route_response(write_queues: u32, broker_addr: &CheetahString) -> RemotingCommand {
    use rocketmq_model::common::constant::PermName;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_protocol::protocol::RemotingSerializable;

    let broker_name = CheetahString::from_static_str("broker-a");
    let route = TopicRouteData {
        queue_datas: vec![QueueData::new(
            broker_name.clone(),
            write_queues,
            write_queues,
            PermName::PERM_READ | PermName::PERM_WRITE,
            0,
        )],
        broker_datas: vec![BrokerData::new(
            CheetahString::from_static_str("cluster-a"),
            broker_name,
            [(mix_all::MASTER_ID, broker_addr.clone())].into_iter().collect(),
            None,
        )],
        ..Default::default()
    };
    RemotingCommand::create_success_response_command().set_body(route.encode().expect("encode producer route"))
}

fn test_runtime() -> Arc<ClientRuntime> {
    crate::runtime::test_client_runtime("default-producer-impl-test")
}

fn test_client_instance(client_config: ClientConfig, client_id: &'static str) -> Arc<MQClientInstance> {
    let runtime = test_runtime();
    MQClientInstance::new_arc(
        client_config,
        0,
        client_id,
        None,
        runtime.component(client_id),
        runtime.telemetry_handle().clone(),
        runtime.pool().request_future_holder(),
    )
}

fn running_producer_without_client() -> DefaultMQProducerImpl {
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
    producer.store_state(ProducerState::Running, Ordering::SeqCst);
    producer.set_service_state(ServiceState::Running);
    producer
}

fn running_producer_arc_with_self_inner() -> Arc<DefaultMQProducerImpl> {
    let producer = Arc::new(running_producer_without_client());
    producer
        .initialize_self_reference(&producer)
        .expect("self reference should initialize");
    producer
}

#[test]
fn standard_weak_self_reference_is_idempotent_and_does_not_retain_root() {
    let producer = Arc::new(DefaultMQProducerImpl::new(
        test_runtime(),
        ClientConfig::default(),
        ProducerConfig::default(),
        None,
    ));
    producer
        .initialize_self_reference(&producer)
        .expect("first initialization should succeed");
    producer
        .initialize_self_reference(&producer)
        .expect("same root initialization should be idempotent");
    let weak = Arc::downgrade(&producer);
    assert_eq!(Arc::strong_count(&producer), 1);

    let other = Arc::new(DefaultMQProducerImpl::new(
        test_runtime(),
        ClientConfig::default(),
        ProducerConfig::default(),
        None,
    ));
    assert!(producer.initialize_self_reference(&other).is_err());
    drop(producer);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn resolver_and_detector_back_references_do_not_retain_client() {
    let client = test_client_instance(ClientConfig::default(), "weak-client");
    let weak = Arc::downgrade(&client);
    let resolver = DefaultResolver {
        client_instance: weak.clone(),
    };
    let _detector = DefaultServiceDetector {
        client_instance: weak.clone(),
        topic_publish_info_table: Arc::new(DashMap::new()),
    };
    assert_eq!(Arc::strong_count(&client), 1);
    drop(client);
    assert!(weak.upgrade().is_none());
    assert!(resolver
        .resolve(&CheetahString::from_static_str("broker-a"))
        .await
        .is_none());
}

#[test]
fn task_admission_rejects_new_work_after_shutdown_begins() {
    let producer = running_producer_without_client();
    producer
        .begin_task_shutdown(ProducerState::Running)
        .expect("running producer should begin shutdown");

    let error = producer
        .spawn_tracked_task("rocketmq-client-producer-rejected-test", async {})
        .expect_err("stopping producer must reject new tasks");
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn cancelled_starting_state_is_recovered_before_retry() {
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
    producer.store_state(ProducerState::Starting, Ordering::SeqCst);

    let result = tokio::time::timeout(Duration::from_secs(1), producer.start())
        .await
        .expect("retry must not wait forever in Starting");
    assert!(result.is_err());
    assert_eq!(producer.load_state(Ordering::SeqCst), ProducerState::StartFailed);
}

#[test]
fn start_runtime_publish_reloads_config_under_update_lock() {
    let producer = Arc::new(DefaultMQProducerImpl::new(
        test_runtime(),
        ClientConfig::default(),
        ProducerConfig::default(),
        None,
    ));
    let configured = DefaultMQProducer::builder(test_runtime())
        .producer_group("start-runtime-group")
        .send_msg_timeout(1_234)
        .build();
    let update_guard = producer.config_update.lock();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let producer_for_start = Arc::clone(&producer);
    let start = std::thread::spawn(move || {
        started_tx.send(()).expect("start observer should remain open");
        producer_for_start.prepare_start_runtime()
    });
    started_rx.recv().expect("start preparation should begin");

    let current = producer.runtime_snapshot();
    producer.runtime.store(Arc::new(ProducerRuntimeSnapshot::new(
        current.client_config.clone(),
        configured.producer_config_snapshot().as_ref().clone(),
    )));
    drop(update_guard);

    let captured = start.join().expect("start preparation should finish");
    assert_eq!(captured.producer_config.send_msg_timeout(), 1_234);
    assert!(Arc::ptr_eq(&captured, &producer.runtime_snapshot()));
}

#[tokio::test]
async fn running_is_published_only_after_async_start_initialization() {
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
    producer.store_state(ProducerState::Starting, Ordering::SeqCst);
    producer.set_service_state(ServiceState::StartFailed);
    let initialization_started = Arc::new(tokio::sync::Notify::new());
    let initialization_release = Arc::new(tokio::sync::Notify::new());
    let started = Arc::clone(&initialization_started);
    let release = Arc::clone(&initialization_release);

    let complete = producer.complete_start_after(async move {
        started.notify_one();
        release.notified().await;
    });
    let observe = async {
        initialization_started.notified().await;
        assert_eq!(producer.load_state(Ordering::SeqCst), ProducerState::Starting);
        assert_eq!(producer.service_state(), ServiceState::StartFailed);
        initialization_release.notify_one();
    };
    tokio::join!(complete, observe);

    assert_eq!(producer.load_state(Ordering::SeqCst), ProducerState::Running);
    assert_eq!(producer.service_state(), ServiceState::Running);
}

#[test]
fn request_cause_from_error_uses_typed_error() {
    let error = DefaultMQProducerImpl::request_cause_from_error(&RocketMQError::Shared(Arc::new(
        rocketmq_error::Error::caused_by(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            std::io::Error::other("send failed"),
        ),
    )));

    assert!(matches!(
        error,
        rocketmq_error::RocketMQError::ResponseProcessFailed { .. }
    ));
    assert_eq!(
        error.to_string(),
        "Response request_response_callback failed: transport.connection.failed: Transport connection operation failed"
    );
}

#[test]
fn spawn_producer_task_uses_injected_service_context() {
    let (tx, rx) = std::sync::mpsc::channel();
    let tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();

    spawn_producer_task(
        &crate::runtime::test_service_context("producer-task-test"),
        "rocketmq-client-producer-test",
        &tracker,
        &shutdown_token,
        async move {
            let current_thread = std::thread::current();
            let thread_name = current_thread.name().unwrap_or_default().to_string();
            tx.send(thread_name).expect("test receiver should still be open");
        },
    )
    .expect("producer task should spawn through the injected runtime");

    let thread_name = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("injected producer task should complete");
    assert_eq!(thread_name, "rocketmq-client-unit-test");
}

#[tokio::test]
async fn execute_async_message_send_uses_injected_client_runtime() {
    let (tx, rx) = std::sync::mpsc::channel();
    let producer = running_producer_without_client();

    producer
        .execute_async_message_send(
            async move {
                let current_thread = std::thread::current();
                let thread_name = current_thread.name().unwrap_or_default().to_string();
                tx.send(thread_name).expect("test receiver should still be open");
            },
            None,
            RequestDeadline::from_timeout_millis(1000),
            1,
        )
        .await
        .expect("async send should spawn on the injected runtime");

    let thread_name = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("injected producer task should complete");
    assert_eq!(thread_name, "rocketmq-client-unit-test");
}

#[test]
fn spawn_producer_task_uses_explicit_client_runtime() {
    let (tx, rx) = std::sync::mpsc::channel();
    let tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();

    spawn_producer_task(
        &crate::runtime::test_service_context("producer-explicit-runtime-test"),
        "rocketmq-client-producer-test",
        &tracker,
        &shutdown_token,
        async move {
            let current_thread = std::thread::current();
            let thread_name = current_thread.name().unwrap_or_default().to_string();
            tx.send(thread_name).expect("test receiver should still be open");
        },
    )
    .expect("producer task should spawn on configured runtime");

    let thread_name = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("configured producer task should complete");
    assert_eq!(thread_name, "rocketmq-client-unit-test");
}

#[tokio::test]
async fn tracked_producer_task_cancellation_stops_pending_task() {
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let started_in_task = started.clone();
    let dropped_in_task = dropped.clone();

    spawn_producer_task(
        &crate::runtime::test_service_context("producer-tracked-task-test"),
        "rocketmq-client-producer-tracked-test",
        &tracker,
        &shutdown_token,
        async move {
            let _drop_flag = DropFlag(dropped_in_task);
            started_in_task.store(true, Ordering::Release);
            std::future::pending::<()>().await;
        },
    )
    .expect("producer task should spawn on current Tokio runtime");

    tokio::time::timeout(Duration::from_secs(1), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("producer task should start before cancellation");

    tracker.close();
    assert!(tokio::time::timeout(Duration::from_millis(20), tracker.wait())
        .await
        .is_err());

    shutdown_token.cancel();
    tokio::time::timeout(Duration::from_secs(1), tracker.wait())
        .await
        .expect("producer task should stop after cancellation");
    assert!(dropped.load(Ordering::Acquire));
}

struct PanicTransactionListener;

impl TransactionListener for PanicTransactionListener {
    fn execute_local_transaction(
        &self,
        _msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        std::panic::panic_any(String::from("local transaction boom"));
    }

    fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::Unknown
    }
}

struct NoopTransactionListener;

impl TransactionListener for NoopTransactionListener {
    fn execute_local_transaction(
        &self,
        _msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        LocalTransactionState::Unknown
    }

    fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::Unknown
    }
}

struct ThreadRecordingTransactionListener {
    thread_id: Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>,
}

impl TransactionListener for ThreadRecordingTransactionListener {
    fn execute_local_transaction(
        &self,
        _msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        *self.thread_id.lock().expect("thread id lock should not be poisoned") = Some(std::thread::current().id());
        LocalTransactionState::CommitMessage
    }

    fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::Unknown
    }
}

struct CapturingSendHook {
    exception_message: Arc<std::sync::Mutex<Option<String>>>,
}

impl SendMessageHook for CapturingSendHook {
    fn hook_name(&self) -> &'static str {
        "CapturingSendHook"
    }

    fn send_message_before(&self, _context: &Option<SendMessageContext<'_>>) {}

    fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
        let Some(exception) = context.as_ref().and_then(|context| context.exception.as_ref()) else {
            return;
        };
        *self
            .exception_message
            .lock()
            .expect("exception message lock should not be poisoned") = Some(exception.to_string());
    }
}

#[test]
fn send_message_after_hook_observes_exception_like_java() {
    let producer = running_producer_without_client();
    let exception_message = Arc::new(std::sync::Mutex::new(None));
    let hook: Arc<dyn SendMessageHook> = Arc::new(CapturingSendHook {
        exception_message: exception_message.clone(),
    });
    *producer.send_message_hook_list.write() = vec![hook].into();
    let context = Some(SendMessageContext {
        exception: Some(DefaultMQProducerImpl::context_error(
            "sendKernelImpl exception".to_string(),
        )),
        ..Default::default()
    });

    producer.execute_send_message_hook_after(&context);

    assert_eq!(
        exception_message
            .lock()
            .expect("exception message lock should not be poisoned")
            .as_deref(),
        Some("Response send_message failed: sendKernelImpl exception")
    );
}

#[tokio::test]
async fn start_failure_enters_start_failed_and_shutdown_noops_like_java() {
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);

    let start_result = producer.start().await;
    assert!(start_result.is_err());
    assert_eq!(
        ProducerState::from_u8(producer.state.load(Ordering::SeqCst)),
        ProducerState::StartFailed
    );
    assert_eq!(producer.service_state(), ServiceState::StartFailed);

    tokio::time::timeout(Duration::from_millis(100), producer.shutdown())
        .await
        .expect("shutdown after start failure should not wait forever like Starting")
        .expect("shutdown after Java START_FAILED should be a no-op");
    assert_eq!(
        ProducerState::from_u8(producer.state.load(Ordering::SeqCst)),
        ProducerState::StartFailed
    );
    assert_eq!(producer.service_state(), ServiceState::StartFailed);
}

#[tokio::test]
async fn owned_partial_start_shutdown_cleans_starting_and_failed_states() {
    for state in [ProducerState::Starting, ProducerState::StartFailed] {
        let producer =
            DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
        producer.set_service_state(ServiceState::StartFailed);
        producer.store_state(state, Ordering::SeqCst);

        producer
            .shutdown_after_partial_start_with_factory(false)
            .await
            .expect("owned partial startup must be cleaned up");

        assert_eq!(producer.load_state(Ordering::SeqCst), ProducerState::Stopped);
        assert_eq!(producer.service_state(), ServiceState::ShutdownAlready);
    }
}

#[tokio::test]
async fn producer_state_wait_uses_state_change_notification() {
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
    producer.store_state(ProducerState::Starting, Ordering::SeqCst);

    let wait_for_running = async {
        producer.wait_until_state_changes_from(ProducerState::Starting).await;
        producer.load_state(Ordering::SeqCst)
    };
    let complete_start = async {
        tokio::task::yield_now().await;
        producer.store_state(ProducerState::Running, Ordering::SeqCst);
    };

    let (state, _) = tokio::time::timeout(Duration::from_millis(100), async {
        tokio::join!(wait_for_running, complete_start)
    })
    .await
    .expect("state waiter should be notified without polling");
    assert_eq!(state, ProducerState::Running);
}

#[tokio::test]
async fn producer_selector_paths_without_client_return_error_instead_of_panicking() {
    let producer = running_producer_without_client();
    let mut msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

    let selector_result = producer
        .invoke_message_queue_selector(&mut msg, |_queues, _msg, _arg| None, &(), 3000)
        .await;
    assert!(selector_result.is_err());

    let fetch_result = producer.fetch_publish_message_queues(&"TopicTest".into()).await;
    assert!(fetch_result.is_err());
    assert!(fetch_result
        .err()
        .is_some_and(|error| error.to_string().contains("MQClientInstance is not available")));
}

#[tokio::test]
async fn request_fail_removes_future_and_executes_callback_once_like_java() {
    let producer = running_producer_without_client();
    let request_future_holder = Arc::clone(&producer.request_future_holder);
    let correlation_id = format!("request-fail-{}", current_millis());
    request_future_holder.remove_request(correlation_id.as_str()).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_inner = Arc::clone(&calls);
    let callback: RequestCallbackFn = Arc::new(move |response, error| {
        assert!(response.is_none());
        assert!(error.is_none());
        calls_inner.fetch_add(1, Ordering::SeqCst);
    });
    let future = Arc::new(RequestResponseFuture::new(
        correlation_id.as_str().into(),
        3_000,
        Some(callback),
    ));
    request_future_holder.put_request(correlation_id.clone(), future).await;

    request_future_holder.fail_request(correlation_id.clone());
    request_future_holder.fail_request(correlation_id.clone());
    for _ in 0..100 {
        if calls.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert!(request_future_holder
        .get_request(correlation_id.as_str())
        .await
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn selector_helper_uses_user_topic_and_restores_namespace_like_java() {
    let mut client_config = ClientConfig::default();
    client_config.set_namespace(CheetahString::from_static_str("ns-a"));
    let queues = vec![MessageQueue::from_parts("TopicTest", "broker-a", 0)];
    let mut msg = Message::builder()
        .topic("ns-a%TopicTest")
        .empty_body()
        .build_unchecked();
    let seen_topic = std::sync::Mutex::new(None);

    let selected = DefaultMQProducerImpl::select_message_queue_with_user_message(
        &client_config,
        &queues,
        &mut msg,
        &|queues, msg, _arg| {
            *seen_topic.lock().expect("seen topic lock should not be poisoned") = Some(msg.topic().clone());
            queues.first().cloned()
        },
        &(),
    )
    .expect("selector should return a queue");

    assert_eq!(
        seen_topic
            .into_inner()
            .expect("seen topic lock should not be poisoned")
            .as_deref(),
        Some("TopicTest")
    );
    assert_eq!(msg.topic(), "ns-a%TopicTest");
    assert_eq!(selected.topic(), "ns-a%TopicTest");
}

#[test]
fn default_send_retry_attempts_match_java_communication_mode() {
    let producer = running_producer_without_client();
    let runtime = producer.runtime_snapshot();

    assert_eq!(
        DefaultMQProducerImpl::get_retry_times(&runtime, CommunicationMode::Sync),
        runtime.producer_config.retry_times_when_send_failed() + 1
    );
    assert_eq!(
        DefaultMQProducerImpl::get_retry_times(&runtime, CommunicationMode::Async),
        1
    );
    assert_eq!(
        DefaultMQProducerImpl::get_retry_times(&runtime, CommunicationMode::Oneway),
        1
    );
}

#[tokio::test]
async fn zero_retry_producer_prepares_route_then_executes_one_primary_send() {
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::ShutdownDeadline;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::test_support::SessionTransportServer;
    use rocketmq_transport::test_support::SessionTransportServerConfig;

    let server_runtime = RuntimeContext::from_current("zero-retry-producer-route-test");
    let processor = Arc::new(ProducerRoutePreparationProcessor::default());
    let server = SessionTransportServer::bind(
        server_runtime.service_context("route-server"),
        SessionTransportServerConfig::loopback(),
        Arc::clone(&processor) as Arc<dyn rocketmq_transport::test_support::SessionProcessor>,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .await
    .expect("bind route server");
    let name_server_addr = CheetahString::from_string(server.local_addr().to_string());
    server.start().expect("start route server");
    processor.responses.lock().expect("route responses lock").push_back(
        RemotingCommand::create_response_command_with_code(ResponseCode::TopicNotExist),
    );
    processor
        .responses
        .lock()
        .expect("route responses lock")
        .push_back(producer_default_route_response(8, &name_server_addr));
    let mut primary_response = RemotingCommand::create_success_response_command_with_header(
        rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader::new(
            CheetahString::from_static_str("message-id"),
            0,
            1,
            None,
            None,
            None,
        ),
    );
    primary_response
        .try_make_custom_header_to_net()
        .expect("materialize primary response header");
    primary_response
        .decode_command_custom_header_fast::<
            rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader,
        >()
        .expect("primary response fixture must decode before transport");
    processor
        .primary_responses
        .lock()
        .expect("primary responses lock")
        .push_back(primary_response);

    let client_runtime = crate::runtime::test_client_runtime("zero-retry-producer-route-test");
    let mut client_config = ClientConfig::default();
    client_config.set_vip_channel_enabled(false);
    let instance = MQClientInstance::new_arc(
        client_config,
        0,
        "zero-retry-producer-route-client",
        None,
        client_runtime.component("instance"),
        client_runtime.telemetry_handle().clone(),
        client_runtime.pool().request_future_holder(),
    );
    let api = instance.get_mq_client_api_impl().expect("client API");
    api.update_name_server_address_list_sync(name_server_addr.as_str());
    api.start().await.expect("start route client transport");

    let producer = running_producer_without_client();
    producer.bind_client_instance(&instance).expect("bind producer client");
    let mut configured = DefaultMQProducer::builder(test_runtime())
        .producer_group("zero-retry-route-group")
        .build();
    configured.set_retry_times_when_send_failed(0);
    configured.set_default_topic_queue_nums(3);
    producer.replace_producer_config(configured.producer_config_snapshot().as_ref().clone());
    let runtime = producer.runtime_snapshot();
    assert_eq!(
        DefaultMQProducerImpl::get_retry_times(&runtime, CommunicationMode::Sync),
        1
    );

    let mut message = Message::builder()
        .topic("new-topic")
        .body_slice(b"body")
        .build_unchecked();
    let result = producer
        .send_with_retry(
            &mut message,
            &CheetahString::from_static_str("new-topic"),
            &TopicPublishInfo::new(),
            None,
            SendContext::new(RequestDeadline::from_timeout_millis(2_000), CommunicationMode::Sync),
            &runtime,
        )
        .await
        .expect("zero-retry producer should complete its initial primary send")
        .expect("sync send should return a result");
    assert_eq!(result.send_status, SendStatus::SendOk);
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), 1);
    assert_eq!(
        processor.topics.lock().expect("route topics lock").as_slice(),
        [
            CheetahString::from_static_str("new-topic"),
            CheetahString::from_static_str("TBW102"),
        ]
    );

    let strategy = producer.fault_strategy_snapshot();
    let mut same_broker = TopicPublishInfo::new();
    same_broker.message_queue_list = vec![MessageQueue::from_parts("new-topic", "broker-a", 0)];
    let (selected_addr, selected_broker) =
        crate::implementation::mq_client_api_impl::MQClientAPIImpl::select_async_retry_target(
            &strategy,
            Some(&same_broker),
            Some(&instance),
            &CheetahString::from_static_str("broker-a"),
        )
        .await
        .expect("a selected same-broker queue with a matching address remains valid");
    assert_eq!(selected_broker, "broker-a");
    assert_eq!(selected_addr, name_server_addr);

    let mut missing_alternate = TopicPublishInfo::new();
    missing_alternate.message_queue_list = vec![
        MessageQueue::from_parts("new-topic", "broker-a", 0),
        MessageQueue::from_parts("new-topic", "broker-b", 0),
    ];
    assert!(
        crate::implementation::mq_client_api_impl::MQClientAPIImpl::select_async_retry_target(
            &strategy,
            Some(&missing_alternate),
            Some(&instance),
            &CheetahString::from_static_str("broker-a"),
        )
        .await
        .is_none()
    );
    assert!(
        crate::implementation::mq_client_api_impl::MQClientAPIImpl::select_async_retry_target(
            &strategy,
            None,
            Some(&instance),
            &CheetahString::from_static_str("broker-a"),
        )
        .await
        .is_none()
    );

    let mut configured = DefaultMQProducer::builder(test_runtime())
        .producer_group("zero-retry-route-group")
        .build();
    configured.set_retry_times_when_send_failed(3);
    configured.set_retry_times_when_send_async_failed(3);
    configured.set_default_topic_queue_nums(3);
    producer.replace_producer_config(configured.producer_config_snapshot().as_ref().clone());
    let runtime = producer.runtime_snapshot();
    {
        let mut primary_responses = processor.primary_responses.lock().expect("primary responses lock");
        primary_responses.push_back(RemotingCommand::create_response_command_with_code(ResponseCode::GoAway));
        primary_responses.push_back(RemotingCommand::create_response_command_with_code(ResponseCode::GoAway));
    }

    let sends_before_sync = processor.primary_sends.load(Ordering::SeqCst);
    let mut sync_message = Message::builder()
        .topic("new-topic")
        .body_slice(b"sync-go-away")
        .build_unchecked();
    let sync_error = producer
        .send_with_retry(
            &mut sync_message,
            &CheetahString::from_static_str("new-topic"),
            &same_broker,
            None,
            SendContext::new(RequestDeadline::from_timeout_millis(2_000), CommunicationMode::Sync),
            &runtime,
        )
        .await
        .expect_err("a non-idempotent sync send must not replay GO_AWAY");
    assert!(matches!(
        sync_error,
        RocketMQError::BrokerOperationFailed { code, .. } if code == ResponseCode::GoAway.to_i32()
    ));
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), sends_before_sync + 1);

    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    let callback: ArcSendCallback = Arc::new(move |_result: Option<&SendResult>, error: Option<&RocketMQError>| {
        let code = match error {
            Some(RocketMQError::BrokerOperationFailed { code, .. }) => *code,
            _ => i32::MIN,
        };
        if let Some(sender) = callback_tx.lock().expect("callback sender lock").take() {
            let _ = sender.send(code);
        }
    });
    let sends_before_async = processor.primary_sends.load(Ordering::SeqCst);
    let mut async_message = Message::builder()
        .topic("new-topic")
        .body_slice(b"async-go-away")
        .build_unchecked();
    assert!(producer
        .send_with_retry(
            &mut async_message,
            &CheetahString::from_static_str("new-topic"),
            &same_broker,
            Some(callback),
            SendContext::new(RequestDeadline::from_timeout_millis(2_000), CommunicationMode::Async),
            &runtime,
        )
        .await
        .expect("async send should hand execution to the owned callback task")
        .is_none());
    let callback_code = tokio::time::timeout(Duration::from_secs(2), callback_rx)
        .await
        .expect("async GO_AWAY callback should complete")
        .expect("async GO_AWAY callback sender should remain available");
    assert_eq!(callback_code, ResponseCode::GoAway.to_i32());
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), sends_before_async + 1);

    instance.shutdown().await;
    client_runtime
        .shutdown()
        .await
        .assert_no_task_leak()
        .expect("client runtime tasks drained");
    server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
        .await
        .assert_no_task_leak()
        .expect("route server tasks drained");
    server_runtime
        .shutdown_tasks(Duration::from_secs(5))
        .await
        .assert_no_task_leak()
        .expect("route server runtime tasks drained");
}

#[test]
fn retry_attempt_count_saturates_without_preallocating_configured_maximum() {
    let mut configured = DefaultMQProducer::builder(test_runtime()).build();
    configured.set_retry_times_when_send_failed(u32::MAX);
    let producer = running_producer_without_client();
    producer.replace_producer_config(configured.producer_config_snapshot().as_ref().clone());
    let runtime = producer.runtime_snapshot();

    assert_eq!(
        DefaultMQProducerImpl::get_retry_times(&runtime, CommunicationMode::Sync),
        u32::MAX
    );
    let retry_state = RetryState::new(u32::MAX);
    assert_eq!(retry_state.times_total, u32::MAX);
    assert_eq!(retry_state.brokers_sent.capacity(), 0);
}

#[test]
fn send_deadline_for_attempt_caps_without_extending_the_absolute_deadline() {
    let producer = running_producer_without_client();
    let configured = DefaultMQProducer::builder(test_runtime())
        .producer_group("retry-timeout-group")
        .send_msg_max_timeout_per_request(500)
        .build();
    producer.replace_producer_config(configured.producer_config_snapshot().as_ref().clone());
    let runtime = producer.runtime_snapshot();
    let deadline = RequestDeadline::from_timeout_millis(3_000);

    let first = DefaultMQProducerImpl::send_deadline_for_attempt(&runtime, deadline, 1, 3);
    let second = DefaultMQProducerImpl::send_deadline_for_attempt(&runtime, deadline, 2, 3);
    assert_eq!(first.budget_millis(), 500);
    assert_eq!(second.budget_millis(), 500);
    assert!(first.instant() <= deadline.instant());
    assert!(second.instant() <= deadline.instant());
    assert_eq!(
        DefaultMQProducerImpl::send_deadline_for_attempt(&runtime, deadline, 3, 3),
        deadline
    );
    assert_eq!(
        DefaultMQProducerImpl::send_deadline_for_attempt(&runtime, deadline, 1, 1),
        deadline
    );

    let producer_without_cap = running_producer_without_client();
    let runtime = producer_without_cap.runtime_snapshot();
    assert_eq!(
        DefaultMQProducerImpl::send_deadline_for_attempt(&runtime, deadline, 1, 3),
        deadline
    );
}

#[test]
fn last_retryable_send_result_is_preserved() {
    let first = SendResult::new(
        SendStatus::FlushDiskTimeout,
        Some(CheetahString::from_static_str("first-id")),
        Some("first-offset-id".to_string()),
        Some(MessageQueue::from_parts("TopicTest", "broker-a", 0)),
        11,
    );
    let last = SendResult::new(
        SendStatus::FlushSlaveTimeout,
        Some(CheetahString::from_static_str("last-id")),
        Some("last-offset-id".to_string()),
        Some(MessageQueue::from_parts("TopicTest", "broker-b", 1)),
        22,
    );
    let mut retry_state = RetryState::new(2);

    retry_state.record_send_result(first);
    retry_state.record_send_result(last.clone());

    assert_eq!(retry_state.take_last_send_result(), Some(last));
}

#[test]
fn retry_failure_preserves_shared_network_source_identity_and_redaction() {
    let canonical = Arc::new(rocketmq_error::Error::caused_by(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "private network detail"),
    ));
    let mut retry_state = RetryState::new(1);
    retry_state.set_error(rocketmq_error::RocketMQError::Shared(Arc::clone(&canonical)));
    let error = retry_state.take_failure_error(&CheetahString::from_static_str("TopicTest"), 1);
    let rendered = error.to_string();
    let remoting_code = error.boundary_view().remoting().code.as_i32();
    let rocketmq_error::RocketMQError::Shared(retained) = error else {
        panic!("expected the canonical shared carrier")
    };

    assert!(Arc::ptr_eq(&canonical, &retained));
    assert!(std::error::Error::source(retained.as_ref())
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some());
    assert_eq!(
        retained.descriptor().code(),
        rocketmq_error::TRANSPORT_CONNECTION_FAILED.code()
    );
    assert_eq!(remoting_code, 2);
    assert!(!rendered.contains("private network detail"));
}

#[tokio::test]
async fn sync_retry_actions_preserve_the_target_unless_switch_broker_is_requested() {
    let producer = running_producer_without_client();
    let topic = CheetahString::from_static_str("TopicTest");
    let ctx = SendContext::new(RequestDeadline::from_timeout_millis(3_000), CommunicationMode::Sync);
    let broker_a = MessageQueue::from_parts("TopicTest", "broker-a", 0);
    let broker_b = MessageQueue::from_parts("TopicTest", "broker-b", 0);
    let mut publish_info = TopicPublishInfo {
        message_queue_list: vec![broker_a.clone(), broker_b.clone()],
        ..Default::default()
    };
    let producer_config = ProducerConfig::default();

    for action in [RetryAction::Stop, RetryAction::RefreshLeader] {
        let mut queued_retry_queue = None;
        assert!(!producer
            .execute_producer_retry_action(
                action,
                &ctx,
                &topic,
                &mut publish_info,
                &broker_a,
                &mut queued_retry_queue,
            )
            .await
            .expect("unsupported action should stop without projecting an error"));
        assert!(queued_retry_queue.is_none());
    }

    for action in [RetryAction::RetryNow, RetryAction::RetryAfter(Duration::ZERO)] {
        let mut queued_retry_queue = None;
        assert!(producer
            .execute_producer_retry_action(
                action,
                &ctx,
                &topic,
                &mut publish_info,
                &broker_a,
                &mut queued_retry_queue,
            )
            .await
            .expect("same-target retry action should execute"));
        let selected = producer
            .select_or_refresh_route_for_attempt(
                &mut queued_retry_queue,
                &topic,
                &mut publish_info,
                Some(broker_a.broker_name()),
                true,
                &producer_config,
                ctx.deadline,
            )
            .await
            .expect("the next attempt should consume the queued retry target");
        assert_eq!(selected, broker_a);
        assert!(queued_retry_queue.is_none());
    }

    let mut queued_retry_queue = None;
    assert!(producer
        .execute_producer_retry_action(
            RetryAction::SwitchBroker,
            &ctx,
            &topic,
            &mut publish_info,
            &broker_a,
            &mut queued_retry_queue,
        )
        .await
        .expect("switch-broker should continue without queuing the current target"));
    let selected = producer
        .select_or_refresh_route_for_attempt(
            &mut queued_retry_queue,
            &topic,
            &mut publish_info,
            Some(broker_a.broker_name()),
            true,
            &producer_config,
            ctx.deadline,
        )
        .await
        .expect("switch-broker should select the alternate broker");
    assert_eq!(selected, broker_b);
}

#[test]
fn async_remaining_timeout_requires_positive_budget_like_java() {
    assert_eq!(DefaultMQProducerImpl::remaining_async_timeout(3_000, 2_999), Some(1));
    assert_eq!(DefaultMQProducerImpl::remaining_async_timeout(3_000, 3_000), None);
    assert_eq!(DefaultMQProducerImpl::remaining_async_timeout(3_000, 3_001), None);
}

#[test]
fn request_remaining_timeout_returns_typed_error_instead_of_underflow() {
    assert_eq!(
        DefaultMQProducerImpl::remaining_request_timeout(3_000, 2_999).expect("one ms remains"),
        1
    );

    for elapsed in [3_000, 3_001] {
        let error = DefaultMQProducerImpl::remaining_request_timeout(3_000, elapsed)
            .expect_err("exhausted request budget should be a typed timeout");
        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::Timeout {
                operation: "send request message",
                timeout_ms: 3_000
            }
        ));
    }
}

#[tokio::test]
async fn send_oneway_with_message_queue_does_not_reject_topic_mismatch_before_kernel_like_java() {
    let producer = running_producer_without_client();
    let msg = Message::builder().topic("TopicA").body_slice(b"body").build_unchecked();
    let mq = MessageQueue::from_parts("TopicB", "broker-a", 0);

    let result = producer.send_oneway_with_message_queue(msg, mq).await;

    let error = result.expect_err("kernel path should fail without a client instance");
    assert!(error.to_string().contains("MQClientInstance is not available"));
    assert!(!error.to_string().contains("is not equal with message queue topic"));
}

#[tokio::test]
async fn sync_send_to_queue_topic_mismatch_uses_java_error_message() {
    let producer = running_producer_without_client();
    let msg = Message::builder().topic("TopicA").body_slice(b"body").build_unchecked();
    let mq = MessageQueue::from_parts("TopicB", "broker-a", 0);

    let error = producer
        .sync_send_with_message_queue_timeout(msg, mq, 3_000)
        .await
        .expect_err("topic mismatch should fail before broker lookup");

    assert!(error.to_string().contains("message's topic not equal mq's topic"));
}

#[tokio::test]
async fn async_send_to_queue_validates_message_before_kernel_like_java() {
    let producer = running_producer_arc_with_self_inner();
    let notify = Arc::new(tokio::sync::Notify::new());
    let seen_error = Arc::new(std::sync::Mutex::new(None::<String>));
    let notify_for_callback = notify.clone();
    let seen_for_callback = seen_error.clone();
    let callback: ArcSendCallback = Arc::new(move |_result: Option<&SendResult>, error: Option<&RocketMQError>| {
        if let Some(error) = error {
            *seen_for_callback
                .lock()
                .expect("seen error lock should not be poisoned") = Some(error.to_string());
            notify_for_callback.notify_one();
        }
    });
    let msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();
    let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

    producer
        .async_send_batch_to_queue_with_callback_timeout(msg, mq, Some(callback), 3_000)
        .await
        .expect("async send should schedule validation");
    tokio::time::timeout(Duration::from_secs(1), notify.notified())
        .await
        .expect("validation error should reach callback");

    let error = seen_error
        .lock()
        .expect("seen error lock should not be poisoned")
        .clone()
        .expect("callback should record validation error");
    assert!(error.contains("message body is null") || error.contains("message body length is zero"));
    assert!(!error.contains("MQClientInstance is not available"));
}

#[tokio::test]
async fn async_send_to_queue_topic_mismatch_uses_java_callback_error_message() {
    let producer = running_producer_arc_with_self_inner();
    let notify = Arc::new(tokio::sync::Notify::new());
    let seen_error = Arc::new(std::sync::Mutex::new(None::<String>));
    let notify_for_callback = notify.clone();
    let seen_for_callback = seen_error.clone();
    let callback: ArcSendCallback = Arc::new(move |_result: Option<&SendResult>, error: Option<&RocketMQError>| {
        if let Some(error) = error {
            *seen_for_callback
                .lock()
                .expect("seen error lock should not be poisoned") = Some(error.to_string());
            notify_for_callback.notify_one();
        }
    });
    let msg = Message::builder().topic("TopicA").body_slice(b"body").build_unchecked();
    let mq = MessageQueue::from_parts("TopicB", "broker-a", 0);

    producer
        .async_send_batch_to_queue_with_callback_timeout(msg, mq, Some(callback), 3_000)
        .await
        .expect("async send should schedule mismatch validation");
    tokio::time::timeout(Duration::from_secs(1), notify.notified())
        .await
        .expect("topic mismatch should reach callback");

    let error = seen_error
        .lock()
        .expect("seen error lock should not be poisoned")
        .clone()
        .expect("callback should record topic mismatch");
    assert!(
        error.contains("Topic of the message does not match its target message queue"),
        "unexpected callback error: {error}"
    );
    assert!(!error.contains("MQClientInstance is not available"));
}

#[tokio::test]
async fn async_send_with_callback_reports_kernel_error_to_callback() {
    let producer = running_producer_arc_with_self_inner();
    let notify = Arc::new(tokio::sync::Notify::new());
    let seen_error = Arc::new(std::sync::Mutex::new(None::<String>));
    let notify_for_callback = notify.clone();
    let seen_for_callback = seen_error.clone();
    let callback: ArcSendCallback = Arc::new(move |_result: Option<&SendResult>, error: Option<&RocketMQError>| {
        if let Some(error) = error {
            *seen_for_callback
                .lock()
                .expect("seen error lock should not be poisoned") = Some(error.to_string());
            notify_for_callback.notify_one();
        }
    });
    let msg = Message::builder()
        .topic("TopicTest")
        .body_slice(b"body")
        .build_unchecked();

    producer
        .async_send_with_callback_timeout(msg, Some(callback), 3_000)
        .await
        .expect("async send should schedule kernel send");
    tokio::time::timeout(Duration::from_secs(1), notify.notified())
        .await
        .expect("kernel error should reach callback");

    let error = seen_error
        .lock()
        .expect("seen error lock should not be poisoned")
        .clone()
        .expect("callback should record kernel error");
    assert!(error.contains("MQClientInstance is not available"));
}

#[tokio::test]
async fn invoke_selector_uses_user_message_and_returns_namespaced_queue_like_java() {
    let mut client_config = ClientConfig::default();
    client_config.set_namespace(CheetahString::from_static_str("ns-a"));
    let client_instance = test_client_instance(client_config.clone(), "selector-namespace-client");
    let producer = DefaultMQProducerImpl::new(test_runtime(), client_config.clone(), ProducerConfig::default(), None);
    producer.store_state(ProducerState::Running, Ordering::SeqCst);
    producer.set_service_state(ServiceState::Running);
    producer
        .client_instance
        .set(Arc::downgrade(&client_instance))
        .expect("client reference should initialize");
    producer.topic_publish_info_table.insert(
        CheetahString::from_static_str("ns-a%TopicTest"),
        Arc::new(TopicPublishInfo {
            have_topic_router_info: true,
            message_queue_list: vec![MessageQueue::from_parts("ns-a%TopicTest", "broker-a", 0)],
            ..Default::default()
        }),
    );
    let mut msg = Message::builder()
        .topic("ns-a%TopicTest")
        .body_slice(b"selector")
        .build_unchecked();
    let seen = std::sync::Mutex::new((None::<CheetahString>, None::<CheetahString>));

    let selected = producer
        .invoke_message_queue_selector(
            &mut msg,
            |queues, msg, seen| {
                let mut seen = seen.lock().expect("seen lock should not be poisoned");
                seen.0 = Some(msg.topic().clone());
                seen.1 = queues.first().map(|queue| queue.topic().clone());
                queues.first().cloned()
            },
            &seen,
            3000,
        )
        .await
        .expect("selector should resolve from cached route info");

    let seen = seen.into_inner().expect("seen lock should not be poisoned");
    assert_eq!(seen.0.as_deref(), Some("TopicTest"));
    assert_eq!(seen.1.as_deref(), Some("TopicTest"));
    assert_eq!(msg.topic(), "ns-a%TopicTest");
    assert_eq!(selected.topic(), "ns-a%TopicTest");
}

#[tokio::test]
async fn cached_topic_publish_info_is_returned_as_shared_snapshot() {
    let topic = CheetahString::from_static_str("TopicTest");
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
    let info = Arc::new(TopicPublishInfo {
        have_topic_router_info: true,
        message_queue_list: (0..256)
            .map(|queue_id| MessageQueue::from_parts("TopicTest", "broker-a", queue_id))
            .collect(),
        ..Default::default()
    });

    producer
        .topic_publish_info_table
        .insert(topic.clone(), Arc::clone(&info));

    let cached = producer
        .try_to_find_topic_publish_info(&topic)
        .await
        .expect("cached route should be returned");

    assert!(Arc::ptr_eq(&cached, &info));
    assert_eq!(cached.message_queue_list.len(), 256);
    assert!(producer.select_one_message_queue(&cached, None, false).is_some());
}

#[test]
fn replace_producer_config_refreshes_send_config_snapshot() {
    let producer = DefaultMQProducerImpl::new(test_runtime(), ClientConfig::default(), ProducerConfig::default(), None);
    let configured = DefaultMQProducer::builder(test_runtime())
        .producer_group("snapshot-group")
        .create_topic_key("SnapshotTopic")
        .default_topic_queue_nums(12)
        .compress_msg_body_over_howmuch(8192)
        .compressor(&COUNTING_COMPRESSOR)
        .build();

    producer.replace_producer_config(configured.producer_config_snapshot().as_ref().clone());
    let send_config = producer.runtime_snapshot().send_config.clone();

    assert_eq!(send_config.producer_group, "snapshot-group");
    assert_eq!(send_config.create_topic_key, "SnapshotTopic");
    assert_eq!(send_config.default_topic_queue_nums, 12);
    assert_eq!(send_config.compress_msg_body_over_howmuch, 8192);
    assert!(send_config.compressor.is_some());
}

#[test]
fn latency_updates_publish_one_coherent_runtime_snapshot() {
    let producer = running_producer_without_client();

    producer.set_latency_max(vec![10, 20, 30]);
    producer.set_not_available_duration(vec![0, 100, 200]);

    let runtime = producer.runtime_snapshot();
    assert_eq!(runtime.producer_config.latency_max(), &[10, 20, 30]);
    assert_eq!(runtime.producer_config.not_available_duration(), &[0, 100, 200]);
    assert_eq!(producer.mq_fault_strategy.read().get_latency_max(), &[10, 20, 30]);
    assert_eq!(
        producer.mq_fault_strategy.read().get_not_available_duration(),
        &[0, 100, 200]
    );
}

#[test]
fn compression_below_threshold_does_not_access_compressor() {
    COUNTING_COMPRESSOR.calls.store(0, Ordering::Relaxed);
    let configured = DefaultMQProducer::builder(test_runtime())
        .producer_group("compress-threshold-group")
        .compress_msg_body_over_howmuch(1024)
        .compressor(&COUNTING_COMPRESSOR)
        .build();
    let producer = DefaultMQProducerImpl::new(
        test_runtime(),
        ClientConfig::default(),
        configured.producer_config_snapshot().as_ref().clone(),
        None,
    );
    let mut msg = Message::builder()
        .topic("TopicTest")
        .body(vec![b'a'; 128])
        .build_unchecked();

    let runtime = producer.runtime_snapshot();
    assert!(!producer.try_to_compress_message(&mut msg, &runtime.send_config));
    assert_eq!(COUNTING_COMPRESSOR.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn producer_request_prepare_without_client_returns_error_instead_of_panicking() {
    let producer = running_producer_without_client();
    let mut msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

    let result = producer.prepare_send_request(&mut msg, 3000).await;

    assert!(result.is_err());
    assert!(result
        .err()
        .is_some_and(|error| error.to_string().contains("MQClientInstance is not available")));
}

#[test]
fn backpressure_setters_clamp_and_resize_permits_like_java() {
    let producer = running_producer_without_client();

    producer.set_enable_backpressure_for_async_mode(true);
    producer.set_back_pressure_for_async_send_num(1);
    producer.set_back_pressure_for_async_send_size(128);

    assert!(producer.enable_backpressure_for_async_mode());
    assert_eq!(
        producer.back_pressure_for_async_send_num(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
    );
    assert_eq!(
        producer.back_pressure_for_async_send_size(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
    );
    assert_eq!(
        producer.semaphore_async_send_num.available_permits(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM as usize
    );
    assert_eq!(
        producer.semaphore_async_send_size.available_permits(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as usize
    );
}

#[test]
fn semaphore_async_adjust_updates_backpressure_limits_like_java_callback() {
    let producer = running_producer_without_client();
    producer.set_back_pressure_for_async_send_num(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM);
    producer.set_back_pressure_for_async_send_size(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE);

    producer
        .semaphore_async_adjust(2, 16)
        .expect("positive semaphore adjustment should be accepted");

    assert_eq!(
        producer.back_pressure_for_async_send_num(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM + 2
    );
    assert_eq!(
        producer.back_pressure_for_async_send_size(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE + 16
    );
    assert_eq!(
        producer.semaphore_async_send_num.available_permits(),
        (MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM + 2) as usize
    );
    assert_eq!(
        producer.semaphore_async_send_size.available_permits(),
        (MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE + 16) as usize
    );

    producer.semaphore_processor();
}

#[test]
fn check_listener_alias_returns_transaction_listener() {
    let producer = running_producer_without_client();

    assert!(producer.check_listener().is_none());
    producer.set_transaction_listener(Arc::new(NoopTransactionListener));

    assert!(producer.check_listener().is_some());
}

#[tokio::test]
async fn async_backpressure_permits_are_held_until_task_finishes_like_java() {
    let producer = running_producer_without_client();
    producer.set_enable_backpressure_for_async_mode(true);
    producer.set_back_pressure_for_async_send_num(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM);
    producer.set_back_pressure_for_async_send_size(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE);

    let msg_len = 8;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

    producer
        .execute_async_message_send(
            async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
            },
            None,
            RequestDeadline::from_timeout_millis(1000),
            msg_len,
        )
        .await
        .expect("backpressure execution should spawn task");
    started_rx.await.expect("spawned task should start");

    assert_eq!(
        producer.semaphore_async_send_num.available_permits(),
        (MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM - 1) as usize
    );
    assert_eq!(
        producer.semaphore_async_send_size.available_permits(),
        MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as usize - msg_len
    );

    release_tx.send(()).expect("spawned task should still be waiting");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if producer.semaphore_async_send_num.available_permits() == MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM as usize
                && producer.semaphore_async_send_size.available_permits()
                    == MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as usize
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("permits should be released after async task finishes");
}

#[tokio::test]
async fn transaction_env_initializes_and_destroys_java_lifecycle_state() {
    let producer = running_producer_without_client();

    producer.init_transaction_env(1, 2, 3).unwrap();
    assert!(producer.is_transaction_env_initialized());

    producer.destroy_transaction_env().await;
    assert!(!producer.is_transaction_env_initialized());
}

#[test]
fn transaction_env_rejects_invalid_java_executor_config() {
    let producer = running_producer_without_client();

    let min_over_max = producer.init_transaction_env(2, 1, 3);
    assert!(min_over_max
        .err()
        .is_some_and(|error| error.to_string().contains("min size cannot exceed max size")));

    let zero_pool = producer.init_transaction_env(0, 1, 3);
    assert!(zero_pool
        .err()
        .is_some_and(|error| error.to_string().contains("must be greater than 0")));

    let zero_hold = producer.init_transaction_env(1, 1, 0);
    assert!(zero_hold
        .err()
        .is_some_and(|error| error.to_string().contains("hold max must be greater than 0")));
}

#[tokio::test]
async fn transaction_send_without_impl_listener_fails_before_send_like_java() {
    let producer = running_producer_without_client();
    let msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

    let result = producer.send_message_in_transaction(msg, None).await;

    assert!(result
        .err()
        .is_some_and(|error| error.to_string().contains("tranExecutor is null")));
}

#[test]
fn transaction_send_future_is_send() {
    fn assert_send<T: Send>(_: T) {}

    let producer = running_producer_without_client();
    let msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

    assert_send(producer.send_message_in_transaction(msg, None));
}

#[tokio::test]
async fn transaction_send_delay_millis_fails_before_send_like_java() {
    let producer = running_producer_without_client();
    producer.set_transaction_listener(Arc::new(NoopTransactionListener));
    let msg = Message::builder()
        .topic("TopicTest")
        .body_slice(b"transaction")
        .delay_millis(3000)
        .build_unchecked();

    let result = producer.send_message_in_transaction(msg, None).await;

    assert!(result.err().is_some_and(|error| {
        error
            .to_string()
            .contains("Transactional messages do not support delayed delivery")
    }));
}

#[test]
fn local_transaction_listener_panic_becomes_unknown_like_java() {
    let listener: ArcTransactionListener = Arc::new(PanicTransactionListener);
    let msg = Message::builder()
        .topic("TopicTest")
        .body_slice(b"transaction")
        .build_unchecked();

    let (state, remark) = DefaultMQProducerImpl::execute_local_transaction_branch(&listener, &msg, None);

    assert_eq!(state, LocalTransactionState::Unknown);
    assert!(remark
        .as_ref()
        .is_some_and(|remark| remark.as_str().contains("local transaction boom")));
}

#[tokio::test(flavor = "current_thread")]
async fn initial_transaction_listener_uses_owned_blocking_lane() {
    let producer = running_producer_without_client();
    let caller_thread = std::thread::current().id();
    let listener_thread = Arc::new(std::sync::Mutex::new(None));
    let listener: ArcTransactionListener = Arc::new(ThreadRecordingTransactionListener {
        thread_id: Arc::clone(&listener_thread),
    });
    let msg = Arc::new(
        Message::builder()
            .topic("TopicTest")
            .body_slice(b"transaction")
            .build_unchecked(),
    );

    let (state, remark) = producer.execute_local_transaction_listener(listener, msg, None).await;

    assert_eq!(state, LocalTransactionState::CommitMessage);
    assert!(remark.is_none());
    assert_ne!(
        listener_thread
            .lock()
            .expect("thread id lock should not be poisoned")
            .expect("listener should record its thread"),
        caller_thread
    );
}

#[test]
fn transaction_check_end_header_preserves_java_offsets() {
    let check_header = CheckTransactionStateRequestHeader {
        topic: Some(CheetahString::from_static_str("TopicA")),
        tran_state_table_offset: 123,
        commit_log_offset: 456,
        msg_id: Some(CheetahString::from_static_str("msg-id")),
        transaction_id: Some(CheetahString::from_static_str("tx-id")),
        offset_msg_id: Some(CheetahString::from_static_str("offset-msg-id")),
        rpc_request_header: Some(RpcRequestHeader {
            broker_name: Some(CheetahString::from_static_str("broker-a")),
            ..Default::default()
        }),
    };

    let end_header = DefaultMQProducerImpl::build_end_transaction_header_for_check(
        CheetahString::from_static_str("ProducerA"),
        &check_header,
        CheetahString::from_static_str("unique-msg-id"),
        LocalTransactionState::CommitMessage,
    );

    assert_eq!(end_header.topic, "TopicA");
    assert_eq!(end_header.producer_group, "ProducerA");
    assert_eq!(end_header.tran_state_table_offset, 123);
    assert_eq!(end_header.commit_log_offset, 456);
    assert_eq!(end_header.commit_or_rollback, MessageSysFlag::TRANSACTION_COMMIT_TYPE);
    assert!(end_header.from_transaction_check);
    assert_eq!(end_header.msg_id, "unique-msg-id");
    assert_eq!(end_header.transaction_id.as_deref(), Some("tx-id"));
    assert_eq!(end_header.rpc_request_header.broker_name.as_deref(), Some("broker-a"));
}

#[test]
fn transaction_check_end_header_preserves_negative_java_offsets_without_wrapping() {
    let check_header = CheckTransactionStateRequestHeader {
        topic: Some(CheetahString::from_static_str("TopicA")),
        tran_state_table_offset: -123,
        commit_log_offset: -456,
        ..Default::default()
    };

    let end_header = DefaultMQProducerImpl::build_end_transaction_header_for_check(
        CheetahString::from_static_str("ProducerA"),
        &check_header,
        CheetahString::from_static_str("msg-id"),
        LocalTransactionState::Unknown,
    );

    assert_eq!(end_header.tran_state_table_offset, -123);
    assert_eq!(end_header.commit_log_offset, -456);
}

#[test]
fn end_transaction_send_result_queue_offset_must_fit_java_long() {
    assert_eq!(
        DefaultMQProducerImpl::u64_to_java_long_field("endTransaction", "tranStateTableOffset", i64::MAX as u64)
            .expect("i64 max should fit Java long"),
        i64::MAX
    );

    let error =
        DefaultMQProducerImpl::u64_to_java_long_field("endTransaction", "tranStateTableOffset", i64::MAX as u64 + 1)
            .expect_err("queue offsets larger than Java long must not wrap");

    assert!(error
        .to_string()
        .contains("endTransaction tranStateTableOffset exceeds Java long range"));
}

#[test]
fn transaction_check_end_header_maps_local_transaction_state() {
    let check_header = CheckTransactionStateRequestHeader {
        tran_state_table_offset: 1,
        commit_log_offset: 2,
        ..Default::default()
    };

    let cases = [
        (
            LocalTransactionState::CommitMessage,
            MessageSysFlag::TRANSACTION_COMMIT_TYPE,
        ),
        (
            LocalTransactionState::RollbackMessage,
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
        ),
        (LocalTransactionState::Unknown, MessageSysFlag::TRANSACTION_NOT_TYPE),
    ];

    for (state, expected_flag) in cases {
        let end_header = DefaultMQProducerImpl::build_end_transaction_header_for_check(
            CheetahString::from_static_str("ProducerA"),
            &check_header,
            CheetahString::from_static_str("msg-id"),
            state,
        );

        assert_eq!(end_header.commit_or_rollback, expected_flag);
    }
}
