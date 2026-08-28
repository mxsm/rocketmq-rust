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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredId;
use rocketmq_transport::api::v2::DeferredResumeErrorKind;
use rocketmq_transport::api::v2::DeferredResumeRetainedSize;
use rocketmq_transport::api::v2::DeferredTerminalReason;
use rocketmq_transport::api::v2::DeferredWaitLimits;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrdering;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::TransportServerV2;
use rocketmq_transport::test_support::Connection;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::index::PopLiteIndexLimits;
use super::prepare::PopLiteRetainedEstimate;
use super::service::PopLiteDeferredService;
use super::service::PopLiteReplayObservation;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;

const ORIGINAL_OPAQUE: i32 = 9_833;

#[test]
fn pending_replay_observation_distinguishes_singleflight_ownership() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let dispatcher = LiteEventDispatcher::default();
    let service = service(&controller, dispatcher.clone());
    let client_id = CheetahString::from_static_str("singleflight-client");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([CheetahString::from_static_str("%LMQ%$parent-topic$child")]),
    );

    assert_eq!(
        service.observe_pending_replay(&client_id),
        PopLiteReplayObservation::NewlyObserved
    );
    assert_eq!(
        service.observe_pending_replay(&client_id),
        PopLiteReplayObservation::AlreadyObserved
    );
    assert!(service.take_pending_replays(nonzero(1)).is_empty());
    service.finish_event_producer(&client_id);
    assert_eq!(service.take_pending_replays(nonzero(1)), [client_id]);
}

#[test]
fn replay_resource_snapshot_reads_pending_and_active_under_one_lock() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let dispatcher = LiteEventDispatcher::default();
    let service = service(&controller, dispatcher.clone());
    let group = CheetahString::from_static_str("group-a");
    let event = HashSet::from([CheetahString::from_static_str("%LMQ%$parent-topic$child")]);
    let active = CheetahString::from_static_str("active-client");
    let pending = CheetahString::from_static_str("pending-client");
    dispatcher.do_full_dispatch(&active, &group, &event);
    dispatcher.do_full_dispatch(&pending, &group, &event);

    assert_eq!(
        service.observe_pending_replay(&active),
        PopLiteReplayObservation::NewlyObserved
    );
    assert_eq!(
        service.observe_pending_replay(&pending),
        PopLiteReplayObservation::NewlyObserved
    );
    service.finish_event_producer(&pending);

    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.active_event_producers, 1);
    assert_eq!(snapshot.pending_replays, 1);
}

struct CountingBodyOwner {
    body: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for CountingBodyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

impl Drop for CountingBodyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

mod concurrency_tests;
mod deadline_tests;
mod lifecycle_tests;
mod owner_tests;
mod post_take_fault_tests;
mod provenance_tests;
mod resource_wire_tests;
mod terminal_session_tests;

pub(super) fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test limit is non-zero")
}

pub(super) fn service(
    controller: &AdmissionController,
    dispatcher: LiteEventDispatcher,
) -> Arc<PopLiteDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
        .expect("PopLite deferred admission");
    Arc::new(PopLiteDeferredService::new(
        admission,
        PopLiteIndexLimits::new(nonzero(4), nonzero(4), nonzero(2)),
        dispatcher,
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        Duration::from_secs(30),
        nonzero(4),
    ))
}

pub(super) fn request_command() -> RemotingCommand {
    request_command_for("client-a", ORIGINAL_OPAQUE, 60_000)
}

pub(super) fn request_command_for(client_id: &str, opaque: i32, poll_time: i64) -> RemotingCommand {
    let header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_string(client_id.to_owned()),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 30_000,
        poll_time,
        born_time: i64::try_from(current_millis()).expect("wall time fits signed protocol field"),
        attempt_id: None,
        rpc: None,
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

#[derive(Clone)]
struct DeferredTestProcessor {
    service: Arc<PopLiteDeferredService>,
    registrations: mpsc::UnboundedSender<DeferredId>,
}

impl RequestProcessorV2 for DeferredTestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = self
            .service
            .prepare(request, PopLiteRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let id = registration.deferred_id();
        self.registrations
            .send(id)
            .map_err(|_| RocketMQError::illegal_argument("PopLite registration observer closed"))?;
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::v2::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

pub(super) struct RunningServer {
    owner: RuntimeOwner,
    pub(super) action_context: ChildServiceContext,
    shutdown: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningServer {
    pub(super) async fn finish(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let report = self.result.await.expect("owned PopLite server report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let tasks = self.owner.shutdown_tasks().await;
        assert!(tasks.is_healthy(), "{}", tasks.to_json());
        let final_report = self.owner.shutdown_background();
        assert!(final_report.is_healthy(), "{}", final_report.to_json());
    }
}

pub(super) async fn start_server<P>(processor: P, controller: Arc<AdmissionController>) -> (Connection, RunningServer)
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("pop-lite-deferred-acceptance"))
        .expect("PopLite V2 runtime owner");
    let server_context = owner.root_context().component("pop-lite.server");
    let runner_context = owner.root_context().component("pop-lite.runner");
    let action_context = owner.root_context().component("pop-lite.actions");
    let server = TransportServerV2::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        }),
        server_context,
        processor,
    )
    .with_admission_controller(controller);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service("pop-lite-deferred-v2-server", async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("owned PopLite server shutdown report");
            let _ = result_tx.send(report);
        })
        .expect("spawn PopLite V2 server");
    let address = startup_rx
        .await
        .expect("PopLite startup channel")
        .expect("PopLite server startup");
    let client = Connection::new(TcpStream::connect(address).await.expect("connect PopLite client"));
    (
        client,
        RunningServer {
            owner,
            action_context,
            shutdown: Some(shutdown_tx),
            result: result_rx,
        },
    )
}

#[tokio::test]
async fn pop_lite_deferred_v2_event_claim_writes_one_canonical_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let client_id = CheetahString::from_static_str("client-a");
    let event = CheetahString::from_static_str("%LMQ%$parent-topic$child-a");
    assert_eq!(
        dispatcher.do_full_dispatch(
            &client_id,
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([event.clone()]),
        ),
        1
    );
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send deferred PopLite request");
    let _id = registrations.recv().await.expect("observe PopLite registration");
    assert_eq!(
        service.take_pending_replays(nonzero(1)),
        vec![client_id.clone()],
        "post-registration observation replays an arrival-before-register event"
    );
    let registered = service.resource_snapshot();
    assert_eq!(registered.admission.waiting_count(), 1);
    assert!(registered.admission.retained_bytes() > 0);
    assert_eq!(registered.index.live, 1);
    let claim = service
        .claim_event(&client_id)
        .await
        .expect("claim PopLite event")
        .expect("registered client has a claim");
    assert_eq!(service.resource_snapshot().active_client_gates, 1);

    let service_for_resume = Arc::clone(&service);
    let service_for_handler = Arc::clone(&service);
    let resumed_client_id = client_id.clone();
    let body_drops = Arc::new(AtomicUsize::new(0));
    let body_drops_for_handler = Arc::clone(&body_drops);
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("pop-lite-deferred-resume", async move {
            let result = service_for_resume
                .resume_event_claim(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    move |resume, reason, reservation| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        assert_eq!(resume.request().client_id(), &resumed_client_id);
                        let accepted = service_for_handler.resource_snapshot();
                        assert_eq!(accepted.active_client_gates, 1);
                        assert_eq!(accepted.event_reservations.events, 1);
                        assert_eq!(accepted.accepted_resumes, 1);
                        let batch = reservation.commit();
                        assert_eq!(batch.event_names(), vec![event]);
                        batch.complete(&HashSet::new());
                        let head = application_remoting_command_factory().create_success_response_command_with_header(
                            PopLiteMessageResponseHeader {
                                pop_time: current_millis() as i64,
                                invisible_time: resume.request().header().invisible_time,
                                revive_qid: POP_ORDER_REVIVE_QUEUE,
                                start_offset_info: None,
                                msg_offset_info: None,
                                order_count_info: None,
                            },
                        );
                        let body = Bytes::from_owner(CountingBodyOwner {
                            body: b"owner-backed-pop-lite-success".to_vec(),
                            drops: body_drops_for_handler,
                        });
                        ResponsePlan::bytes(head, body)
                            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn PopLite resume");
    receipt_rx
        .await
        .expect("PopLite receipt channel")
        .expect("canonical PopLite write");

    let response = client
        .receive_command()
        .await
        .expect("connection remains open")
        .expect("one PopLite response frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(
        response.body().map(|body| body.as_ref()),
        Some(b"owner-backed-pop-lite-success".as_slice())
    );
    assert_eq!(body_drops.load(Ordering::SeqCst), 1);
    assert!(dispatcher.pending_events(&client_id).is_empty());
    let snapshot = service.resource_snapshot();
    assert_eq!(snapshot.admission.waiting_count(), 0);
    assert_eq!(snapshot.index.live, 0);
    assert_eq!(snapshot.event_reservations.events, 0);
    assert_eq!(snapshot.active_client_gates, 0);
    assert_eq!(snapshot.accepted_resumes, 0);

    running.finish().await;
    assert!(client.receive_command().await.is_none(), "EOF proves exactly one frame");
}

#[tokio::test]
async fn pop_lite_deferred_claim_failure_rolls_back_event_order_and_permits() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let dispatcher = LiteEventDispatcher::default();
    let service = service(controller.as_ref(), dispatcher.clone());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send deferred PopLite request");
    let id = registrations.recv().await.expect("observe PopLite registration");

    let client_id = CheetahString::from_static_str("client-a");
    assert!(
        service
            .claim_event(&client_id)
            .await
            .expect("pending-event miss is not a claim failure")
            .is_none(),
        "pending-event miss must leave the registered waiter claimable"
    );
    assert_eq!(service.resource_snapshot().index.live, 1);
    let first = CheetahString::from_static_str("%LMQ%$parent-topic$child-a");
    let second = CheetahString::from_static_str("%LMQ%$parent-topic$child-b");
    assert_eq!(
        dispatcher.do_full_dispatch(
            &client_id,
            &CheetahString::from_static_str("group-a"),
            &HashSet::from([first.clone(), second.clone()]),
        ),
        2
    );
    let claimed_elsewhere = service
        .registry
        .claim(id, DeferredWakeReason::Timeout)
        .await
        .expect("test owner claims registry entry first");

    match service.claim_event(&client_id).await {
        Err(_) => {}
        Ok(_) => panic!("stale PopLite index claim must fail at the registry"),
    }
    assert_eq!(dispatcher.pending_events(&client_id), vec![first, second]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 2);
    assert_eq!(dispatcher.reservation_snapshot().events, 0);
    assert_eq!(service.resource_snapshot().active_client_gates, 0);
    assert_eq!(
        service.take_pending_replays(nonzero(1)),
        vec![client_id.clone()],
        "registry-claim rollback must schedule the restored event without another arrival"
    );
    drop(claimed_elsewhere);
    assert_eq!(service.resource_snapshot().index.live, 0);

    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 2);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    running.finish().await;
}

#[tokio::test]
async fn pop_lite_deferred_shutdown_cannot_reinsert_a_checked_pending_replay() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let dispatcher = LiteEventDispatcher::default();
    let service = service(&controller, dispatcher.clone());
    let client_id = CheetahString::from_static_str("shutdown-client");
    dispatcher.do_full_dispatch(
        &client_id,
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([CheetahString::from_static_str("%LMQ%$parent-topic$shutdown-child")]),
    );
    let checked = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    service.set_replay_insert_hook(Arc::clone(&checked), Arc::clone(&resume));
    let observing_service = Arc::clone(&service);
    let observing_client = client_id.clone();
    let observer = std::thread::spawn(move || observing_service.observe_pending_event(&observing_client));
    checked.wait();
    service.shutdown();
    resume.wait();
    assert!(!observer.join().expect("pending replay observer thread"));
    assert_eq!(service.resource_snapshot().pending_replays, 0);
    assert_eq!(dispatcher.take_pending_events(&client_id).len(), 1);
}
