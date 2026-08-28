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

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredClaimErrorKind;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredId;
use rocketmq_transport::api::v2::DeferredResumeErrorKind;
use rocketmq_transport::api::v2::DeferredTerminalReason;
use rocketmq_transport::api::v2::DeferredWaitLimits;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrdering;
use rocketmq_transport::api::v2::RequestOrderingKey;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::TransportServerV2;
use rocketmq_transport::test_support::Connection;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;

use super::super::*;
use crate::long_polling::pop_deferred::index::PopArrivalView;
use crate::long_polling::pop_deferred::service::PopDeferredWakeupObserver;

const SENTINEL_CODE: i32 = 98_281;
const ORDERING_KEY: u64 = 9_828;
const TEST_POLL_MILLIS: u64 = 60_000;

mod lifecycle_tests;
mod provenance_tests;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value is non-zero")
}

fn assert_clean_shutdown(owner: &str, report: &ShutdownReport) {
    assert!(report.is_healthy(), "{owner}: {}", report.to_json());
    assert_eq!(report.aborted, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.panicked, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.timed_out, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.leaked, 0, "{owner}: {}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{owner}: {}", report.to_json());
    for child in &report.children {
        assert_clean_shutdown(owner, child);
    }
}

fn service(
    controller: &AdmissionController,
    waiters: usize,
    index_entries: usize,
    entries_per_key: usize,
) -> Arc<PopDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(waiters, 16 * 1024 * 1024))
        .expect("POP deferred admission");
    Arc::new(PopDeferredService::new(
        admission,
        PopCriteriaLimits::new(nonzero(index_entries), nonzero(entries_per_key)),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(index_entries),
    ))
}

fn request_header(topic: &str, group: &str, queue_id: i32, filter_tag: Option<i64>) -> PopMessageRequestHeader {
    PopMessageRequestHeader {
        consumer_group: CheetahString::from_string(group.to_owned()),
        topic: CheetahString::from_string(topic.to_owned()),
        queue_id,
        max_msg_nums: 8,
        invisible_time: 30_000,
        poll_time: TEST_POLL_MILLIS,
        born_time: current_millis(),
        init_mode: 0,
        exp_type: filter_tag.map(|_| CheetahString::from_static_str("TAG")),
        exp: filter_tag.map(|tag| CheetahString::from_string(tag.to_string())),
        order: Some(false),
        attempt_id: None,
        topic_request_header: None,
    }
}

fn preflight_test_data(topic: &str, caller_host: &str) -> PopRequestData {
    let mut header = request_header(topic, "GroupA", 0, None);
    header.born_time = 10_000;
    PopRequestData::from_test_header(header, caller_host.into())
}

fn request_command(topic: &str, group: &str, queue_id: i32, filter_tag: Option<i64>, opaque: i32) -> RemotingCommand {
    let mut command = RemotingCommand::create_request_command(
        RequestCode::PopMessage,
        request_header(topic, group, queue_id, filter_tag),
    )
    .set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

fn expiring_request_command(topic: &str, group: &str, queue_id: i32, opaque: i32) -> RemotingCommand {
    let mut header = request_header(topic, group, queue_id, None);
    header.poll_time = 100;
    let mut command = RemotingCommand::create_request_command(RequestCode::PopMessage, header).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

struct MatchTagFilter(i64);

impl MessageFilter for MatchTagFilter {
    fn is_matched_by_consume_queue(&self, tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        tags_code == Some(self.0)
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        true
    }
}

#[derive(Clone, Copy)]
struct RegistrationObservation {
    id: DeferredId,
    caller: SocketAddr,
}

#[derive(Default)]
struct ProcessorBarrier {
    before_outcome: Notify,
    release_outcome: Notify,
    commit_observed: Notify,
}

#[derive(Clone)]
struct DeferredTestProcessor {
    service: Arc<PopDeferredService>,
    registrations: mpsc::UnboundedSender<RegistrationObservation>,
    barrier: Arc<ProcessorBarrier>,
    hold_before_outcome: bool,
    rollback_registration: bool,
}

impl RequestProcessorV2 for DeferredTestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if request.command().code() == SENTINEL_CODE {
            self.barrier.commit_observed.notify_one();
            return ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
            .map(HandlerOutcome::Reply)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()));
        }

        let header = request
            .command()
            .decode_command_custom_header::<PopMessageRequestHeader>()?;
        let caller = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            RequestOrigin::Embedded { .. } => {
                return Err(RocketMQError::illegal_argument(
                    "POP deferred test requires a trusted network peer",
                ));
            }
            _ => return Err(RocketMQError::illegal_argument("unsupported POP request origin")),
        };
        let filter_tag = header.exp.as_ref().and_then(|value| value.parse::<i64>().ok());
        let filter = filter_tag.map(|tag| Arc::new(MatchTagFilter(tag)) as rocketmq_store::ArcMessageFilter);
        let subscription = filter.as_ref().map(|_| SubscriptionData::default());
        let prepared = self
            .service
            .prepare(request, subscription, filter, PopRetainedEstimate::default())
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let id = registration.deferred_id();
        self.registrations
            .send(RegistrationObservation { id, caller })
            .map_err(|_| RocketMQError::illegal_argument("registration observer closed"))?;
        if self.rollback_registration {
            drop(registration);
            return Err(RocketMQError::illegal_argument("intentional POP registration rollback"));
        }
        if self.hold_before_outcome {
            self.barrier.before_outcome.notify_one();
            self.barrier.release_outcome.notified().await;
        }
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::v2::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Ordered(RequestOrderingKey::new(ORDERING_KEY))
    }
}

struct RunningServer {
    owner: RuntimeOwner,
    action_context: ChildServiceContext,
    shutdown: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningServer {
    fn begin_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    async fn finish(mut self) {
        self.begin_shutdown();
        let report = self.result.await.expect("owned V2 server result channel");
        assert_clean_shutdown("V2 server", &report);
        let task_report = self.owner.shutdown_tasks().await;
        assert_clean_shutdown("V2 runtime", &task_report);
        let final_report = self.owner.shutdown_background();
        assert_clean_shutdown("V2 runtime finalization", &final_report);
    }
}

async fn start_server<P>(processor: P, controller: Arc<AdmissionController>) -> (Connection, SocketAddr, RunningServer)
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-pop-deferred-acceptance"))
        .expect("POP V2 test runtime owner");
    let server_context = owner.root_context().component("pop-deferred.server");
    let runner_context = owner.root_context().component("pop-deferred.runner");
    let action_context = owner.root_context().component("pop-deferred.actions");
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
        .spawn_service("pop-deferred-v2-server", async move {
            let result = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("owned POP V2 server shutdown report");
            let _ = result_tx.send(result);
        })
        .expect("spawn owned POP V2 server");
    let address = startup_rx
        .await
        .expect("POP V2 startup channel")
        .expect("POP V2 server startup");
    let client = Connection::new(TcpStream::connect(address).await.expect("connect POP V2 client"));
    (
        client,
        address,
        RunningServer {
            owner,
            action_context,
            shutdown: Some(shutdown_tx),
            result: result_rx,
        },
    )
}

async fn commit_barrier(client: &mut Connection, barrier: &ProcessorBarrier, opaque: i32) {
    client
        .send_command(
            RemotingCommand::create_remoting_command(SENTINEL_CODE)
                .set_opaque(opaque)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send ordered commit sentinel");
    barrier.commit_observed.notified().await;
}

fn assert_released(service: &PopDeferredService) {
    assert_eq!(service.index_snapshot(), PopIndexSnapshot::default());
    let admission = service.admission_snapshot();
    assert_eq!(admission.waiting_count(), 0);
    assert_eq!(admission.retained_bytes(), 0);
}

#[tokio::test]
async fn prepared_wake_replays_then_observed_resume_writes_one_bound_frame() {
    const ORIGINAL_OPAQUE: i32 = 9_821;
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 4, 4, 4);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: true,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, ORIGINAL_OPAQUE))
        .await
        .expect("send deferred POP request");
    let registered = registrations.recv().await.expect("observe POP registration");
    barrier.before_outcome.notified().await;

    let mut pending_claim = Box::pin(service.claim(registered.id, DeferredWakeReason::MessageArrived));
    tokio::select! {
        biased;
        result = &mut pending_claim => panic!("prepared claim completed before dispatcher commit: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    barrier.release_outcome.notify_one();
    let claim = pending_claim.await.expect("prepared wake replays after commit");
    assert_eq!(claim.reason(), DeferredWakeReason::MessageArrived);
    assert_eq!(service.index_snapshot().live(), 0);
    assert_eq!(service.admission_snapshot().waiting_count(), 1);

    let handler_started = Arc::new(Notify::new());
    let handler_release = Arc::new(Notify::new());
    let rereads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (receipt_tx, receipt_rx) = oneshot::channel();
    let service_for_resume = Arc::clone(&service);
    let started = Arc::clone(&handler_started);
    let release = Arc::clone(&handler_release);
    let reread_count = Arc::clone(&rereads);
    let (observer, mut completion) = PopDeferredWakeupObserver::new();
    running
        .action_context
        .spawn_service("pop-deferred-resume", async move {
            let result = service_for_resume
                .resume_claimed_observed(
                    claim,
                    rocketmq_transport::api::v2::DeferredResumeRetainedSize::new(257),
                    observer,
                    move |resume, reason| async move {
                        started.notify_one();
                        release.notified().await;
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        assert_eq!(resume.request().caller_host().as_str(), registered.caller.to_string());
                        reread_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::PollingTimeout,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn owned POP resume assertion");
    handler_started.notified().await;
    let active_resume = service.resource_snapshot();
    assert_eq!(active_resume.resume_executions, 1);
    assert_eq!(active_resume.resume_execution_bytes, 257);
    assert_eq!(
        service.admission_snapshot().waiting_count(),
        0,
        "wait admission is released before execution admission invokes the handler"
    );
    tokio::select! {
        biased;
        result = &mut completion => panic!("POP wake completion finished before canonical resume/write: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    handler_release.notify_one();
    receipt_rx
        .await
        .expect("resume receipt channel")
        .expect("accepted POP resume drains through canonical writing");
    assert_eq!(
        completion.await.expect("POP wake completion"),
        crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupOutcome::ProcessingCompleted
    );

    let response = client
        .receive_command()
        .await
        .expect("POP V2 connection remains open until the accepted write drains")
        .expect("one POP timeout response frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.code(), ResponseCode::PollingTimeout as i32);
    assert_eq!(rereads.load(std::sync::atomic::Ordering::SeqCst), 1);
    let terminal_resume = service.resource_snapshot();
    assert_eq!(terminal_resume.resume_executions, 0);
    assert_eq!(terminal_resume.resume_execution_bytes, 0);
    assert_released(&service);
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "owned shutdown reaches EOF after exactly one response frame"
    );
}

#[tokio::test]
async fn provisional_oldest_claim_does_not_hide_active_second_waiter() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 4, 4, 4);
    let first_barrier = Arc::new(ProcessorBarrier::default());
    let second_barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let first_processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx.clone(),
        barrier: Arc::clone(&first_barrier),
        hold_before_outcome: true,
        rollback_registration: false,
    };
    let second_processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&second_barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut first_client, _first_address, first_running) =
        start_server(first_processor, Arc::clone(&controller)).await;
    let (mut second_client, _second_address, second_running) =
        start_server(second_processor, Arc::clone(&controller)).await;

    first_client
        .send_command(request_command("TopicA", "GroupA", 0, None, 81))
        .await
        .expect("send provisional oldest waiter");
    let first = registrations.recv().await.expect("provisional registration");
    first_barrier.before_outcome.notified().await;
    second_client
        .send_command(request_command("TopicA", "GroupA", 0, None, 82))
        .await
        .expect("send active second waiter");
    let second = registrations.recv().await.expect("active second registration");
    commit_barrier(&mut second_client, &second_barrier, 83).await;

    let arrival = PopArrival::new("TopicA".into(), "GroupA".into(), 0);
    let mut pending_first = Box::pin(service.claim_message(&arrival, PopSelectionOrder::Oldest));
    tokio::select! {
        biased;
        result = &mut pending_first => panic!("provisional oldest claim completed before commit: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    let second_claim = service
        .claim_message(&arrival, PopSelectionOrder::Oldest)
        .await
        .expect("second arrival remains claimable")
        .expect("active second waiter is not hidden by the provisional candidate");
    assert_eq!(second_claim.deferred_id(), second.id);
    drop(second_claim);

    drop(pending_first);
    first_barrier.release_outcome.notify_one();
    commit_barrier(&mut first_client, &first_barrier, 84).await;
    let first_claim = service
        .claim_message(&arrival, PopSelectionOrder::Oldest)
        .await
        .expect("cancelled provisional candidate reopens")
        .expect("oldest waiter remains claimable after commit");
    assert_eq!(first_claim.deferred_id(), first.id);
    drop(first_claim);
    assert_released(&service);

    first_running.finish().await;
    second_running.finish().await;
}

#[tokio::test]
async fn service_shutdown_drains_accepted_resume_to_parent_cancelled_without_a_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 9_823))
        .await
        .expect("send deferred POP request");
    let registered = registrations.recv().await.expect("observe POP registration");
    commit_barrier(&mut client, &barrier, 9_824).await;
    let claim = service
        .claim(registered.id, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim active POP waiter");

    let handler_started = Arc::new(Notify::new());
    let handler_release = Arc::new(Notify::new());
    let handler_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (receipt_tx, receipt_rx) = oneshot::channel();
    let service_for_resume = Arc::clone(&service);
    let started = Arc::clone(&handler_started);
    let release = Arc::clone(&handler_release);
    let calls = Arc::clone(&handler_calls);
    running
        .action_context
        .spawn_service("pop-deferred-cancelled-resume", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    rocketmq_transport::api::v2::DeferredResumeRetainedSize::default(),
                    move |_resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        started.notify_one();
                        release.notified().await;
                        ResponsePlan::command(RemotingCommand::create_response_command_with_code(
                            ResponseCode::PollingTimeout,
                        ))
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn owned POP resume cancellation assertion");
    handler_started.notified().await;
    assert_eq!(service.admission_snapshot().waiting_count(), 0);
    assert!(matches!(
        service.shutdown(),
        rocketmq_transport::api::v2::DeferredRegistryShutdownOutcome::Completed(_)
    ));
    handler_release.notify_one();
    let error = receipt_rx
        .await
        .expect("resume cancellation result channel")
        .expect_err("service shutdown terminalizes the accepted resume");
    assert_eq!(error.kind(), DeferredResumeErrorKind::Cancelled);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(DeferredTerminalReason::ParentCancelled)
    );
    assert_eq!(error.write_progress(), None);
    assert_eq!(handler_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_released(&service);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "lifecycle cancellation drains the accepted task without writing a response"
    );
}

#[tokio::test]
async fn matching_skips_filter_miss_then_advances_oldest_exact_wildcard_and_retry_waiters() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 8, 8, 8);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;

    client
        .send_command(request_command("TopicA", "GroupA", 0, Some(7), 1))
        .await
        .expect("send filtered exact waiter");
    let filtered = registrations.recv().await.expect("filtered registration");
    client
        .send_command(request_command("TopicA", "GroupA", -1, None, 2))
        .await
        .expect("send wildcard waiter");
    let wildcard = registrations.recv().await.expect("wildcard registration");
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 3))
        .await
        .expect("send second exact waiter");
    let second_exact = registrations.recv().await.expect("second exact registration");
    commit_barrier(&mut client, &barrier, 4).await;

    let miss = PopArrival::new("TopicA".into(), "GroupA".into(), 0).with_filter_metadata(Some(6), 0, None, None);
    let first = service
        .claim_message(&miss, PopSelectionOrder::Oldest)
        .await
        .expect("filter miss is not a claim error")
        .expect("wildcard waiter remains eligible after the oldest filter miss");
    assert_eq!(first.deferred_id(), wildcard.id);
    drop(first);

    let matching = PopArrival::new("TopicA".into(), "GroupA".into(), 0).with_filter_metadata(Some(7), 0, None, None);
    let second = service
        .claim_message(&matching, PopSelectionOrder::Oldest)
        .await
        .expect("matching arrival")
        .expect("oldest filtered exact waiter");
    assert_eq!(second.deferred_id(), filtered.id);
    drop(second);

    let retry_topic = rocketmq_model::common::key_builder::KeyBuilder::build_pop_retry_topic_v2("TopicA", "GroupA");
    let retry = PopArrival::from_retry_topic(retry_topic.into(), "GroupA".into(), 0);
    let third = service
        .claim_message(&retry, PopSelectionOrder::Oldest)
        .await
        .expect("retry arrival")
        .expect("retry topic normalizes to the remaining exact waiter");
    assert_eq!(third.deferred_id(), second_exact.id);
    drop(third);
    assert_released(&service);

    running.finish().await;
}

#[tokio::test]
async fn topic_fanout_and_forced_refresh_bypass_filter_then_cleanup() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 2, 2, 2);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, Some(7), 51))
        .await
        .expect("send forced-refresh waiter");
    let registered = registrations.recv().await.expect("forced-refresh registration");
    commit_barrier(&mut client, &barrier, 52).await;

    assert_eq!(
        service.consumer_groups_for_arrival(&"TopicA".into(), 0),
        [CheetahString::from_static_str("GroupA")]
    );
    let arrival = PopArrival::new("TopicA".into(), "GroupA".into(), 0).with_filter_metadata(Some(99), 0, None, None);
    assert!(service
        .claim_message(&arrival, PopSelectionOrder::Oldest)
        .await
        .expect("normal filter miss")
        .is_none());
    let target = service
        .forced_target_batch(&"TopicA".into(), &"GroupA".into())
        .into_iter()
        .next()
        .expect("bounded lag producer finds the exact queue target");
    let topic = CheetahString::from_static_str("TopicA");
    let consumer_group = CheetahString::from_static_str("GroupA");
    let forced_arrival = PopArrivalView::new(&topic, &consumer_group, target.queue_id()).forced();
    let candidate = service
        .reserve_target_arrival_candidate(&target, forced_arrival, PopSelectionOrder::Oldest)
        .expect("forced target bypasses the filter");
    let forced = service
        .claim_forced_candidate(candidate)
        .await
        .expect("forced refresh claim");
    assert_eq!(forced.deferred_id(), registered.id);
    assert_eq!(forced.reason(), DeferredWakeReason::ForcedRefresh);
    drop(forced);
    assert!(service.consumer_groups_for_arrival(&"TopicA".into(), 0).is_empty());
    assert_released(&service);

    running.finish().await;
}

#[tokio::test]
async fn duplicate_claim_and_session_close_never_execute_or_write() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref(), 4, 4, 4);
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 11))
        .await
        .expect("send deferred waiter");
    let registered = registrations.recv().await.expect("registration");
    commit_barrier(&mut client, &barrier, 12).await;

    let first = service
        .claim(registered.id, DeferredWakeReason::MessageArrived)
        .await
        .expect("first claim");
    let duplicate = service
        .claim(registered.id, DeferredWakeReason::Timeout)
        .await
        .expect_err("a live claim excludes every duplicate reason");
    assert_eq!(duplicate.kind(), DeferredClaimErrorKind::AlreadyClaimed);
    let (observer, completion) = PopDeferredWakeupObserver::new();
    observer.complete_claim_error(&duplicate);
    assert_eq!(
        completion.await.expect("duplicate wake completion"),
        crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupOutcome::AlreadyCompleted
    );
    drop(first);
    assert_released(&service);

    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 13))
        .await
        .expect("send timeout-first waiter");
    let timeout_first = registrations.recv().await.expect("timeout-first registration");
    commit_barrier(&mut client, &barrier, 14).await;
    let (message, timeout) = tokio::join!(
        service.claim(timeout_first.id, DeferredWakeReason::MessageArrived),
        service.claim(timeout_first.id, DeferredWakeReason::Timeout),
    );
    match (message, timeout) {
        (Ok(winner), Err(loser)) => {
            assert_eq!(winner.reason(), DeferredWakeReason::MessageArrived);
            assert_eq!(loser.kind(), DeferredClaimErrorKind::AlreadyClaimed);
            drop(winner);
        }
        (Err(loser), Ok(winner)) => {
            assert_eq!(winner.reason(), DeferredWakeReason::Timeout);
            assert_eq!(loser.kind(), DeferredClaimErrorKind::AlreadyClaimed);
            drop(winner);
        }
        (message, timeout) => panic!("exactly one concurrent reason must win: {message:?}, {timeout:?}"),
    }
    assert_released(&service);

    client
        .send_command(request_command("TopicA", "GroupA", 0, None, 15))
        .await
        .expect("send session-close waiter");
    let closed = registrations.recv().await.expect("session-close registration");
    commit_barrier(&mut client, &barrier, 16).await;
    client.shutdown().await.expect("close the POP client session");
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.admission_snapshot().waiting_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session cleanup releases the waiter");
    assert_released(&service);
    let closed_claim = service
        .claim(closed.id, DeferredWakeReason::MessageArrived)
        .await
        .expect_err("closed session cannot execute POP recovery");
    assert!(matches!(
        closed_claim.kind(),
        DeferredClaimErrorKind::SessionClosed
            | DeferredClaimErrorKind::AlreadyCompleted
            | DeferredClaimErrorKind::NotFound
    ));
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "claimed, dropped, and inactive-session waiters execute no resume handler and emit no frame"
    );
}
