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

use rocketmq_error::RocketMQError;
use rocketmq_model::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredId;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredWaitLimits;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrdering;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::TransportServer;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::*;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexLimits;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteRetainedEstimate;
use crate::long_polling::pop_lite_deferred::service::PopLiteDeferredService;

const SUCCESS_OPAQUE: i32 = 98_336;
const EMPTY_OPAQUE: i32 = 98_337;
const CLIENT_ID: &str = "client-1";
const GROUP: &str = "group-a";
const PARENT_TOPIC: &str = "parent-topic";
const INVISIBLE_TIME: i64 = 60_000;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test limit is non-zero")
}

fn deferred_service(controller: &AdmissionController, dispatcher: LiteEventDispatcher) -> Arc<PopLiteDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(8, 8 * 1024 * 1024))
        .expect("configure PopLite real-store deferred admission");
    Arc::new(PopLiteDeferredService::new(
        admission,
        PopLiteIndexLimits::new(nonzero(8), nonzero(8), nonzero(4)),
        dispatcher,
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        Duration::from_secs(30),
        nonzero(8),
    ))
}

fn deferred_request(opaque: i32) -> RemotingCommand {
    let header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str(CLIENT_ID),
        consumer_group: CheetahString::from_static_str(GROUP),
        topic: CheetahString::from_static_str(PARENT_TOPIC),
        max_msg_num: 1,
        invisible_time: INVISIBLE_TIME,
        poll_time: 30_000,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

#[derive(Clone)]
struct RegisteringProcessor {
    service: Arc<PopLiteDeferredService>,
    registrations: mpsc::UnboundedSender<DeferredId>,
}

impl RequestProcessor for RegisteringProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.registrations
            .send(registration.deferred_id())
            .map_err(|_| RocketMQError::illegal_argument("PopLite registration observer closed"))?;
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

struct RunningWireServer {
    owner: RuntimeOwner,
    shutdown: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningWireServer {
    async fn finish(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let report = self.result.await.expect("owned PopLite wire server report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let tasks = self.owner.shutdown_tasks().await;
        assert!(tasks.is_healthy(), "{}", tasks.to_json());
        let background = self.owner.shutdown_background();
        assert!(background.is_healthy(), "{}", background.to_json());
    }
}

async fn start_wire_server(
    processor: RegisteringProcessor,
    controller: Arc<AdmissionController>,
) -> (Connection, RunningWireServer) {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("pop-lite-real-store-wire"))
        .expect("PopLite real-store runtime owner");
    let server_context = owner.root_context().component("pop-lite.real-store.server");
    let runner_context: ChildServiceContext = owner.root_context().component("pop-lite.real-store.runner");
    let server = TransportServer::new(
        Arc::new(rocketmq_transport::api::ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..rocketmq_transport::api::ServerConfig::default()
        }),
        server_context,
        processor,
    )
    .with_admission_controller(controller);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service("pop-lite-real-store-wire-server", async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("owned PopLite real-store server shutdown report");
            let _ = result_tx.send(report);
        })
        .expect("spawn PopLite real-store server");
    let address = startup_rx
        .await
        .expect("PopLite real-store startup channel")
        .expect("PopLite real-store server startup");
    let client = Connection::new(
        TcpStream::connect(address)
            .await
            .expect("connect PopLite real-store client"),
    );
    (
        client,
        RunningWireServer {
            owner,
            shutdown: Some(shutdown_tx),
            result: result_rx,
        },
    )
}

fn assert_response_header(response: &mut RemotingCommand, expected_order_count: Option<&str>) {
    response.make_custom_header_to_net();
    let header = response
        .decode_command_custom_header::<PopLiteMessageResponseHeader>()
        .expect("decode PopLite response header");
    assert!(header.pop_time > 0);
    assert_eq!(header.invisible_time, INVISIBLE_TIME);
    assert_eq!(header.revive_qid, POP_ORDER_REVIVE_QUEUE);
    assert_eq!(header.start_offset_info, None);
    assert_eq!(header.msg_offset_info, None);
    assert_eq!(header.order_count_info.as_deref(), expected_order_count);
}

fn assert_terminal_resources(service: &PopLiteDeferredService, dispatcher: &LiteEventDispatcher) {
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.admission.waiting_count(), 0);
    assert_eq!(snapshot.admission.retained_bytes(), 0);
    assert_eq!(snapshot.index.live, 0);
    assert_eq!(snapshot.index.reserved, 0);
    assert_eq!(snapshot.index.candidates, 0);
    assert_eq!(snapshot.index.clients, 0);
    assert_eq!(snapshot.index.oldest_waiter_age, None);
    assert_eq!(snapshot.event_reservations.batches, 0);
    assert_eq!(snapshot.event_reservations.events, 0);
    assert_eq!(snapshot.event_reservations.permits, 0);
    assert_eq!(snapshot.event_reservations.retained_bytes, 0);
    assert_eq!(snapshot.active_client_gates, 0);
    assert_eq!(snapshot.prepared_registrations, 0);
    assert_eq!(snapshot.pending_claims, 0);
    assert_eq!(snapshot.accepted_resumes, 0);
    assert_eq!(snapshot.resume_execution_count, 0);
    assert_eq!(snapshot.resume_execution_bytes, 0);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    assert_eq!(dispatcher.budget_snapshot().current_bytes, 0);
}

#[tokio::test]
async fn pop_lite_deferred_real_store_single_chain_writes_exact_terminal_frame() {
    let mut runtime = new_lite_test_runtime("pop-lite-deferred-real-store-wire").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-deferred-real-store").await;
    let _ = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let pop_lite = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = deferred_service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let (mut client, running) = start_wire_server(
        RegisteringProcessor {
            service: Arc::clone(&service),
            registrations: registration_tx,
        },
        controller,
    )
    .await;

    client
        .send_command(deferred_request(SUCCESS_OPAQUE))
        .await
        .expect("send real-store PopLite deferred request");
    registrations.recv().await.expect("observe real-store registration");
    let registered = service.resource_snapshot();
    assert_eq!(registered.admission.waiting_count(), 1);
    assert!(registered.admission.retained_bytes() > 0);
    assert_eq!(registered.index.live, 1);
    assert_eq!(registered.index.clients, 1);
    assert!(registered.index.oldest_waiter_age.is_some());

    let lmq_name = CheetahString::from_string(to_lmq_name(PARENT_TOPIC, "child-a").expect("child-a lmq"));
    let client_id = CheetahString::from_static_str(CLIENT_ID);
    let group = CheetahString::from_static_str(GROUP);
    assert_eq!(
        dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()])),
        1
    );
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    assert!(dispatcher.budget_snapshot().current_bytes > 0);
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim real-store PopLite event")
        .expect("registered real-store client has a claim");
    let service_during_resume = Arc::clone(&service);
    let pop_lite_during_resume = Arc::clone(&pop_lite);
    service
        .resume_event_claim(
            claim,
            DeferredResumeRetainedSize::new(512),
            move |resume, reason, events| async move {
                assert_eq!(reason, DeferredWakeReason::MessageArrived);
                let active = service_during_resume.resource_snapshot();
                assert_eq!(active.event_reservations.batches, 1);
                assert_eq!(active.event_reservations.events, 1);
                assert_eq!(active.event_reservations.permits, 1);
                assert!(active.event_reservations.retained_bytes > 0);
                assert_eq!(active.active_client_gates, 1);
                assert_eq!(active.resume_execution_count, 1);
                assert!(active.resume_execution_bytes >= 512);
                pop_lite_during_resume.resume_pop_lite(resume, reason, events).await
            },
        )
        .await
        .expect("write canonical real-store PopLite response");

    let mut response = client
        .receive_command()
        .await
        .expect("real-store connection remains open")
        .expect("one real-store PopLite response frame");
    assert_eq!(response.opaque(), SUCCESS_OPAQUE);
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(response.remark().map(CheetahString::as_str), Some("FOUND"));
    assert_response_header(&mut response, Some("0"));
    let mut body = response.body().cloned().expect("real-store PopLite response body");
    let message = MessageDecoder::decode(&mut body, true, false, false, false, false)
        .expect("decode real-store PopLite response body");
    assert_eq!(message.body(), Some(Bytes::from_static(b"lite-deferred-real-store")));
    assert!(body.is_empty(), "response body contains exactly one stored message");
    assert_eq!(
        runtime
            .composition
            .state
            .consumer_offset_manager()
            .query_offset(&group, &lmq_name, 0),
        1
    );
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_terminal_resources(&service, &dispatcher);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "EOF proves exactly one response frame"
    );
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_deferred_real_store_claimed_empty_is_exact_terminal_timeout() {
    let mut runtime = new_lite_test_runtime("pop-lite-deferred-real-store-empty").await;
    seed_lite_query_state(&mut runtime);
    let _ = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let pop_lite = runtime
        .composition
        .state
        .pop_lite_message_processor()
        .expect("PopLite processor should initialize")
        .clone();
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = deferred_service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let (mut client, running) = start_wire_server(
        RegisteringProcessor {
            service: Arc::clone(&service),
            registrations: registration_tx,
        },
        controller,
    )
    .await;

    client
        .send_command(deferred_request(EMPTY_OPAQUE))
        .await
        .expect("send empty-store PopLite deferred request");
    registrations.recv().await.expect("observe empty-store registration");
    let lmq_name = CheetahString::from_string(to_lmq_name(PARENT_TOPIC, "child-b").expect("child-b lmq"));
    let client_id = CheetahString::from_static_str(CLIENT_ID);
    let group = CheetahString::from_static_str(GROUP);
    assert_eq!(
        dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()])),
        1
    );
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim empty-store PopLite event")
        .expect("registered empty-store client has a claim");
    service
        .resume_event_claim(
            claim,
            DeferredResumeRetainedSize::new(128),
            move |resume, reason, events| async move { pop_lite.resume_pop_lite(resume, reason, events).await },
        )
        .await
        .expect("write canonical empty-store PopLite response");

    let mut response = client
        .receive_command()
        .await
        .expect("empty-store connection remains open")
        .expect("one empty-store PopLite response frame");
    assert_eq!(response.opaque(), EMPTY_OPAQUE);
    assert_eq!(response.code(), ResponseCode::PollingTimeout as i32);
    assert_eq!(
        response.remark().map(CheetahString::as_str),
        Some("NO_MESSAGE_IN_QUEUE")
    );
    assert!(response.body().is_none());
    assert_response_header(&mut response, None);
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_terminal_resources(&service, &dispatcher);

    assert_eq!(
        dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([lmq_name.clone()])),
        1
    );
    assert!(
        service
            .claim_event(&client_id)
            .await
            .expect("post-terminal claim check")
            .is_none(),
        "a claimed-empty responder is terminal and is never re-registered"
    );
    assert_eq!(dispatcher.take_pending_events(&client_id), vec![lmq_name]);
    assert_terminal_resources(&service, &dispatcher);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "EOF proves exactly one timeout frame"
    );
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}
