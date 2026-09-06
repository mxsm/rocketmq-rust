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

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredWaitLimits;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionRegistry;
use rocketmq_transport::api::TransportServer;
use rocketmq_transport::test_support::Connection;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use super::PullMessageProcessor;
use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::client::client_session_info::ClientSessionInfo;
use crate::client::manager::consumer_manager::ConsumerClientRegistration;
use crate::config::broker_config::BrokerConfig;
use crate::long_polling::pull_deferred::PullCriteriaLimits;
use crate::long_polling::pull_deferred::PullDeferredService;
use crate::long_polling::pull_deferred::PullSessionClientLookup;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::pull_message_processor::capability::PullMessageProcessorContext;

#[derive(Clone)]
struct ArcHeldPullProcessor {
    inner: Arc<PullMessageProcessor<BrokerMessageStore>>,
    sessions: Arc<Mutex<Vec<SessionId>>>,
    broadcast_registration: Option<ConsumerClientRegistration>,
}

impl ArcHeldPullProcessor {
    fn new(processor: PullMessageProcessor<BrokerMessageStore>) -> Self {
        Self {
            inner: Arc::new(processor),
            sessions: Arc::new(Mutex::new(Vec::new())),
            broadcast_registration: None,
        }
    }

    fn with_broadcast_registration(mut self, registration: ConsumerClientRegistration) -> Self {
        self.broadcast_registration = Some(registration);
        self
    }
}

impl RequestProcessor for ArcHeldPullProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let session_id = request.session().id();
        self.sessions.lock().push(session_id);
        if let Some(registration) = &self.broadcast_registration {
            registration.register_consumer_session_without_sub(
                &CheetahString::from_static_str("group-a"),
                ClientSessionInfo::new(
                    session_id,
                    CheetahString::from_static_str("wire-client"),
                    None,
                    LanguageCode::RUST,
                    1,
                ),
                ConsumeType::ConsumePassively,
                MessageModel::Broadcasting,
                ConsumeFromWhere::ConsumeFromLastOffset,
                false,
            );
        }
        Box::pin(self.inner.process_shared(request)).await
    }
}

#[derive(Clone)]
struct TraitPullProcessor {
    inner: Arc<tokio::sync::Mutex<PullMessageProcessor<BrokerMessageStore>>>,
}

impl TraitPullProcessor {
    fn new(processor: PullMessageProcessor<BrokerMessageStore>) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(processor)),
        }
    }
}

impl RequestProcessor for TraitPullProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let mut processor = self.inner.lock().await;
        Box::pin(RequestProcessor::process(&mut *processor, request)).await
    }
}

struct RunningServer {
    owner: RuntimeOwner,
    stop: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningServer {
    async fn finish(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let report = self.result.await.expect("Pull server shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let tasks = self.owner.shutdown_tasks().await;
        assert!(tasks.is_healthy(), "{}", tasks.to_json());
        let background = self.owner.shutdown_background();
        assert!(background.is_healthy(), "{}", background.to_json());
    }
}

async fn start_server<P>(processor: P, controller: Arc<AdmissionController>) -> (Connection, RunningServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    start_server_with_registry(processor, controller, None).await
}

async fn start_server_with_registry<P>(
    processor: P,
    controller: Arc<AdmissionController>,
    session_registry: Option<Arc<SessionRegistry>>,
) -> (Connection, RunningServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("pull-message-leaf"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("Pull runtime owner");
    let server_context = owner.root_context().component("pull-message.server");
    let runner_context = owner.root_context().component("pull-message.runner");
    let server = TransportServer::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        }),
        server_context,
        processor,
    )
    .with_admission_controller(controller);
    let server = match session_registry {
        Some(session_registry) => server.with_session_registry(session_registry),
        None => server,
    };
    let (stop_tx, stop_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service("pull-message-server", async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = stop_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("Pull server report");
            let _ = result_tx.send(report);
        })
        .expect("start Pull server");
    let address = startup_rx
        .await
        .expect("Pull startup result")
        .expect("Pull startup address");
    let connection = Connection::new(TcpStream::connect(address).await.expect("connect Pull client"));
    (
        connection,
        RunningServer {
            owner,
            stop: Some(stop_tx),
            result: result_rx,
        },
    )
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test limit must be nonzero")
}

fn deferred_service(controller: &AdmissionController) -> Arc<PullDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(4, 16 * 1024 * 1024))
        .expect("Pull deferred admission");
    Arc::new(PullDeferredService::new(
        admission,
        PullCriteriaLimits::new(nonzero(4), nonzero(4)),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(1),
        nonzero(1),
    ))
}

fn temp_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rocketmq-pull-{label}-{}-{}",
        std::process::id(),
        rocketmq_runtime::common::time_utils::current_millis()
    ))
}

fn available_ha_port() -> usize {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve Pull HA port")
        .local_addr()
        .expect("Pull HA address")
        .port() as usize
}

async fn runtime(label: &str, broadcast: bool) -> (BrokerRuntime, PathBuf) {
    let root = temp_root(label);
    std::fs::create_dir_all(&root).expect("create Pull test root");
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
        enable_broadcast_offset_store: broadcast,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        ha_listen_port: available_ha_port(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    runtime.initialize().await.expect("initialize Pull Broker runtime");
    runtime.seed_pop_topic_and_group_for_test("topic-a", "group-a");
    runtime
        .start_message_store_for_test()
        .await
        .expect("start Pull message store");
    (runtime, root)
}

fn processor(
    context: Arc<PullMessageProcessorContext<BrokerMessageStore>>,
) -> PullMessageProcessor<BrokerMessageStore> {
    PullMessageProcessor::new(
        Arc::new(DefaultPullMessageResultHandler::new(
            Arc::new(Vec::new()),
            Arc::clone(&context),
            None,
        )),
        context,
    )
}

fn pull_request(group: &str, offset: i64, suspend: bool, opaque: i32) -> RemotingCommand {
    let header = PullMessageRequestHeader {
        consumer_group: group.into(),
        topic: CheetahString::from_static_str("topic-a"),
        queue_id: 0,
        queue_offset: offset,
        max_msg_nums: 1,
        sys_flag: PullSysFlag::build_sys_flag(false, suspend, true, false) as i32,
        commit_offset: 0,
        suspend_timeout_millis: 60_000,
        sub_version: 1,
        subscription: Some(CheetahString::from_static_str("*")),
        expression_type: Some(CheetahString::from_static_str(ExpressionType::TAG)),
        ..PullMessageRequestHeader::default()
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::PullMessage, header).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

async fn send_and_receive(connection: &mut Connection, request: RemotingCommand) -> RemotingCommand {
    connection.send_command(request).await.expect("send Pull request");
    connection
        .receive_command()
        .await
        .expect("read Pull connection")
        .expect("Pull inline response")
}

async fn wait_for_deferred(service: &PullDeferredService) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.admission_snapshot().waiting_count() != 1 || service.index_snapshot().live() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Pull request should seal a deferred registration");
}

#[tokio::test]
async fn immediate_reply_is_bound_to_the_original_wire_opaque() {
    const OPAQUE: i32 = 98_471;
    let (mut runtime, root) = runtime("immediate", false).await;
    let shared = TraitPullProcessor::new(processor(runtime.pull_message_context_for_test()));
    let (mut client, server) =
        start_server(shared, Arc::new(AdmissionController::new(AdmissionLimits::default()))).await;

    let response = send_and_receive(&mut client, pull_request("group-a", 0, false, OPAQUE)).await;
    assert_eq!(response.code(), ResponseCode::PullNotFound as i32);
    assert_eq!(response.opaque(), OPAQUE);

    drop(client);
    server.finish().await;
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn arc_held_suspension_requires_injected_service_and_retains_one_sealed_lease() {
    const UNAVAILABLE_OPAQUE: i32 = 98_472;
    const DEFERRED_OPAQUE: i32 = 98_473;
    let (mut runtime, root) = runtime("deferred", false).await;
    let context = runtime.pull_message_context_for_test();

    let (mut unavailable_client, unavailable_server) = start_server(
        ArcHeldPullProcessor::new(processor(Arc::clone(&context))),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .await;
    let unavailable = send_and_receive(
        &mut unavailable_client,
        pull_request("group-a", 0, true, UNAVAILABLE_OPAQUE),
    )
    .await;
    assert_eq!(unavailable.code(), ResponseCode::ServiceNotAvailable as i32);
    assert_eq!(unavailable.opaque(), UNAVAILABLE_OPAQUE);
    drop(unavailable_client);
    unavailable_server.finish().await;

    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = deferred_service(controller.as_ref());
    let deferred_processor = processor(context);
    assert!(deferred_processor
        .install_pull_deferred_service(Arc::clone(&service))
        .is_ok());
    let (mut deferred_client, deferred_server) =
        start_server(ArcHeldPullProcessor::new(deferred_processor), controller).await;
    deferred_client
        .send_command(pull_request("group-a", 0, true, DEFERRED_OPAQUE))
        .await
        .expect("send deferred Pull request");
    wait_for_deferred(&service).await;

    assert_eq!(service.index_snapshot().live(), 1);
    assert_eq!(service.admission_snapshot().waiting_count(), 1);
    assert!(service.admission_snapshot().retained_bytes() > 0);

    let _ = service.shutdown();
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.admission_snapshot().waiting_count() != 0 || service.index_snapshot().live() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Pull deferred owners are released after dispatcher commit");
    assert_eq!(service.index_snapshot().live(), 0);
    assert_eq!(service.admission_snapshot().waiting_count(), 0);
    assert_eq!(service.admission_snapshot().retained_bytes(), 0);
    drop(deferred_client);
    deferred_server.finish().await;
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}

fn stored_message() -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str("topic-a"));
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from_static(b"broadcast-pull"));
    message.set_wait_store_msg_ok(false);
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

#[tokio::test]
async fn broadcast_offset_uses_the_canonical_network_session_identity() {
    const OPAQUE: i32 = 98_474;
    let (mut runtime, root) = runtime("broadcast", true).await;
    let transport_sessions = runtime.session_registry_for_test();
    let context = runtime.pull_message_context_for_test();
    for _ in 0..2 {
        let result = context
            .store()
            .put_message_for_test(stored_message())
            .await
            .expect("append broadcast message");
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    }
    runtime.reput_message_store_once_for_test().await;

    let session_registry = context.consumers().session_registry();
    let session_registration = context.consumers().client_registration();
    let pull_processor = processor(Arc::clone(&context));
    let lookup_port: Arc<dyn PullSessionClientLookup> = Arc::new(session_registry.clone());
    assert!(pull_processor.install_session_client_lookup(lookup_port).is_ok());
    let shared = ArcHeldPullProcessor::new(pull_processor).with_broadcast_registration(session_registration);
    let sessions = Arc::clone(&shared.sessions);
    let (mut client, server) = start_server_with_registry(
        shared,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
        Some(transport_sessions),
    )
    .await;

    let response = send_and_receive(&mut client, pull_request("group-a", 1, false, OPAQUE)).await;
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(response.opaque(), OPAQUE);
    let observed_session = *sessions.lock().first().expect("observed Pull session");
    assert_eq!(
        PullSessionClientLookup::client_id(
            &session_registry,
            observed_session,
            &CheetahString::from_static_str("group-a")
        ),
        Some(CheetahString::from_static_str("wire-client"))
    );
    assert_eq!(
        context.query_broadcast_offset("topic-a", "group-a", 0, "wire-client", -1, true),
        1
    );

    drop(client);
    server.finish().await;
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}
