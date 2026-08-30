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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::poll;
use parking_lot::Mutex;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::AuthorizedCommandDispatcher;
use rocketmq_transport::api::EmbeddedDispatchErrorKind;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RejectRequestDecision;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestControlView;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ResponseObservation;
use rocketmq_transport::api::ResponsePlan;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::api::TransportServer;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::EmbeddedRequestHarness;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::Notify;

use super::*;
use crate::config::broker_config::BrokerConfig;

fn fast_failure(max_count: usize) -> BrokerFastFailure {
    BrokerFastFailure::new(Arc::new(BrokerConfig {
        broker_fast_failure_enable: true,
        broker_fast_failure_pending_max_count: max_count,
        broker_fast_failure_pending_max_bytes: 64 * 1024,
        wait_time_mills_in_send_queue: 60_000,
        ..BrokerConfig::default()
    }))
}

fn request(opaque: i32) -> RemotingCommand {
    RemotingCommand::new_request(
        RequestCode::SendMessage,
        bytes::Bytes::from_static(b"fast-failure-request"),
    )
    .set_opaque(opaque)
}

fn metadata(opaque: i32) -> FastFailureRequestMetadata {
    FastFailureRequestMetadata::from_command(&request(opaque))
}

fn assert_queue_resources_released(service: &BrokerFastFailure) {
    let budget = service.pending_budget_snapshot();
    assert_eq!(budget.current_count, 0);
    assert_eq!(budget.current_bytes, 0);
    assert!(service.pending_count_snapshot().iter().all(|(_, count)| *count == 0));
}

#[tokio::test]
async fn queue_cancellation_wins_the_permit_race_and_returns_one_typed_rejection() {
    let service = fast_failure(2);
    let admission = try_admit(&service, FastFailureQueueKind::Send, metadata(71)).expect("admitted request");
    let cancel_response = service
        .command_factory()
        .create_response_command_with_code_remark(ResponseCode::SystemBusy, "cancelled before dispatch")
        .set_opaque(71);
    assert!(service.cancel(FastFailureQueueKind::Send, &admission.task, cancel_response));

    let error = match admission.await_run(FastFailureControl::legacy()).await {
        Err(error) => error,
        Ok(_) => panic!("cancelled admission must not acquire processor ownership"),
    };
    let FastFailureAwaitError::Rejected(rejection) = error else {
        panic!("queue cancellation must not be reported as lifecycle cancellation");
    };
    assert_eq!(rejection.kind(), FastFailureRejectionKind::QueueCancelled);
    let response = rejection.into_legacy_command();
    assert_eq!(response.code(), ResponseCode::SystemBusy as i32);
    assert_eq!(response.opaque(), 71);
    assert_queue_resources_released(&service);
}

#[tokio::test]
async fn dropping_pending_admission_releases_budget_without_a_cleaner_scan() {
    let service = fast_failure(1);
    let admission = try_admit(&service, FastFailureQueueKind::Send, metadata(72)).expect("admitted request");
    assert_eq!(service.pending_budget_snapshot().current_count, 1);

    drop(admission);

    assert_queue_resources_released(&service);
    let replacement = try_admit(&service, FastFailureQueueKind::Send, metadata(73)).expect("budget was released");
    drop(replacement);
    assert_queue_resources_released(&service);
}

#[tokio::test]
async fn lifecycle_stop_after_permit_cas_rolls_back_before_business_ownership() {
    let service = fast_failure(2);
    let checks = AtomicUsize::new(0);
    let admission = try_admit(&service, FastFailureQueueKind::Send, metadata(78)).expect("admitted request");

    let error = match admission
        .await_run(FastFailureControl::cancel_on_second_check(&checks))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("post-CAS lifecycle stop must not escape as a run owner"),
    };

    assert!(matches!(error, FastFailureAwaitError::LifecycleStopped));
    assert_eq!(checks.load(Ordering::SeqCst), 2);
    assert_queue_resources_released(&service);
    let replacement = try_admit(&service, FastFailureQueueKind::Send, metadata(79)).expect("permit was rolled back");
    let run = replacement
        .await_run(FastFailureControl::legacy())
        .await
        .expect("replacement acquires the released permit");
    drop(run);
    assert_queue_resources_released(&service);
}

#[tokio::test]
async fn dropping_run_guards_releases_execution_permits_and_running_ownership() {
    let service = fast_failure(128);
    let mut guards = Vec::new();
    let blocked = loop {
        let admission = try_admit(
            &service,
            FastFailureQueueKind::Send,
            metadata(100 + i32::try_from(guards.len()).expect("bounded guard count")),
        )
        .expect("admitted request");
        let mut wait = Box::pin(admission.await_run(FastFailureControl::legacy()));
        match poll!(&mut wait) {
            std::task::Poll::Ready(Ok(guard)) => guards.push(guard),
            std::task::Poll::Ready(Err(error)) => panic!("unexpected admission failure: {error:?}"),
            std::task::Poll::Pending => break wait,
        }
    };
    assert!(!guards.is_empty());
    assert_eq!(service.pending_budget_snapshot().current_count, 1);

    drop(guards);
    let resumed = blocked.await.expect("dropped run guards release an execution permit");
    drop(resumed);

    assert_queue_resources_released(&service);
}

struct AllowEmbeddedPolicy;

impl RequestPolicy for AllowEmbeddedPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

#[derive(Clone)]
struct ControlCaptureProcessor {
    sender: Arc<Mutex<Option<oneshot::Sender<RequestControlView>>>>,
}

impl RequestProcessor for ControlCaptureProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if let Some(sender) = self.sender.lock().take() {
            let _ = sender.send(request.control().clone());
        }
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_opaque(request.original_identity().original_opaque());
        Ok(HandlerOutcome::Reply(
            ResponsePlan::command(response).expect("control capture response plan"),
        ))
    }
}

struct ControlFixture {
    owner: RuntimeOwner,
    context: rocketmq_runtime::ChildServiceContext,
    harness: EmbeddedRequestHarness<ControlCaptureProcessor>,
    receiver: Option<oneshot::Receiver<RequestControlView>>,
}

impl ControlFixture {
    fn new(name: &'static str) -> Self {
        let owner = RuntimeOwner::new(RuntimeConfig::server_default(name)).expect("control fixture runtime");
        let context = owner.root_context().component(format!("{name}.request"));
        let (sender, receiver) = oneshot::channel();
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            ControlCaptureProcessor {
                sender: Arc::new(Mutex::new(Some(sender))),
            },
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarness::new(
            dispatcher,
            context.task_group().clone(),
            Principal::new("fast-failure-control-test"),
        );
        Self {
            owner,
            context,
            harness,
            receiver: Some(receiver),
        }
    }

    async fn capture(&mut self, deadline: Option<RequestDeadline>, opaque: i32) -> RequestControlView {
        let EmbeddedDispatchOutcome::Reply(plan) = self
            .harness
            .dispatch(deadline, request(opaque))
            .await
            .expect("capture a real request control")
        else {
            panic!("control capture must return an inline reply");
        };
        drop(plan);
        self.receiver
            .take()
            .expect("one control receiver")
            .await
            .expect("processor supplied its control")
    }

    async fn finish(self) {
        drop(self.harness);
        drop(self.context);
        assert!(self.owner.shutdown_tasks().await.is_healthy());
        assert!(self.owner.shutdown_background().is_healthy());
    }
}

#[tokio::test]
async fn real_deadline_stops_pending_admission_without_a_processor_owner() {
    let service = fast_failure(2);
    let mut fixture = ControlFixture::new("fast-failure-deadline");
    let control = fixture
        .capture(Some(RequestDeadline::after(Duration::from_secs(1))), 74)
        .await;
    control.cancelled().await;
    let admission = try_admit(&service, FastFailureQueueKind::Send, metadata(74)).expect("admitted request");

    let error = match admission.await_run(FastFailureControl::from(&control)).await {
        Err(error) => error,
        Ok(_) => panic!("expired control must not acquire processor ownership"),
    };
    assert!(matches!(error, FastFailureAwaitError::LifecycleStopped));
    assert_queue_resources_released(&service);
    fixture.finish().await;
}

#[tokio::test]
async fn real_parent_cancel_stops_pending_admission_without_a_processor_owner() {
    let service = fast_failure(2);
    let mut fixture = ControlFixture::new("fast-failure-parent-cancel");
    let control = fixture.capture(None, 75).await;
    fixture.context.task_group().cancel();
    let admission = try_admit(&service, FastFailureQueueKind::Send, metadata(75)).expect("admitted request");

    let error = match admission.await_run(FastFailureControl::from(&control)).await {
        Err(error) => error,
        Ok(_) => panic!("cancelled parent must not acquire processor ownership"),
    };
    assert!(matches!(error, FastFailureAwaitError::LifecycleStopped));
    assert_queue_resources_released(&service);
    fixture.finish().await;
}

#[derive(Clone)]
struct PendingFastFailureProcessor {
    service: BrokerFastFailure,
    entered: Arc<tokio::sync::Notify>,
}

impl RequestProcessor for PendingFastFailureProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let admission = match try_admit(
            &self.service,
            FastFailureQueueKind::Send,
            FastFailureRequestMetadata::from_command(request.command()),
        ) {
            Ok(admission) => admission,
            Err(rejection) => {
                return Ok(HandlerOutcome::Reply(
                    rejection.into_response_plan().expect("pending rejection plan"),
                ));
            }
        };
        let _run = match admission.await_run(FastFailureControl::from(request.control())).await {
            Ok(run) => run,
            Err(FastFailureAwaitError::Rejected(rejection)) => {
                return Ok(HandlerOutcome::Reply(
                    rejection.into_response_plan().expect("queued rejection plan"),
                ));
            }
            Err(FastFailureAwaitError::LifecycleStopped) => {
                return Err(rocketmq_error::RocketMQError::invariant_violated(
                    "dispatcher lifecycle stopped before fast-failure execution",
                ));
            }
        };
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn canonical_deadline_drops_post_mark_run_owner_and_releases_resources() {
    let service = fast_failure(2);
    let entered = Arc::new(tokio::sync::Notify::new());
    let owner =
        RuntimeOwner::new(RuntimeConfig::server_default("fast-failure-post-mark-deadline")).expect("post-mark runtime");
    let context = owner
        .root_context()
        .component("fast-failure-post-mark-deadline.request");
    let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
        PendingFastFailureProcessor {
            service: service.clone(),
            entered: Arc::clone(&entered),
        },
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(
            Some(Arc::new(AllowEmbeddedPolicy)),
            None,
        )),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    ));
    let harness = EmbeddedRequestHarness::new(
        dispatcher,
        context.task_group().clone(),
        Principal::new("fast-failure-post-mark-test"),
    );
    let error = {
        let entered_wait = entered.notified();
        let dispatch = harness.dispatch(Some(RequestDeadline::after(Duration::from_secs(1))), request(77));
        tokio::pin!(dispatch);
        tokio::select! {
            () = entered_wait => {}
            result = &mut dispatch => panic!("post-mark processor completed before deadline: {result:?}"),
        }
        dispatch
            .await
            .expect_err("canonical deadline cancels the running processor")
    };
    assert_eq!(error.kind(), EmbeddedDispatchErrorKind::DeadlineExceeded);
    assert_queue_resources_released(&service);

    drop(harness);
    drop(context);
    assert!(owner.shutdown_tasks().await.is_healthy());
    assert!(owner.shutdown_background().is_healthy());
}

#[derive(Clone)]
struct CanonicalFastFailureProcessor {
    service: BrokerFastFailure,
    admissions: Arc<AtomicUsize>,
    business_executions: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    write_observed: Arc<Notify>,
}

impl RequestProcessor for CanonicalFastFailureProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.admissions.fetch_add(1, Ordering::SeqCst);
        let metadata = FastFailureRequestMetadata::from_command(request.command());
        let plan = match try_admit(&self.service, FastFailureQueueKind::Send, metadata) {
            Err(rejection) => rejection
                .into_response_plan()
                .expect("typed fast-failure rejection plan"),
            Ok(admission) => match admission.await_run(FastFailureControl::from(request.control())).await {
                Ok(run) => {
                    self.business_executions.fetch_add(1, Ordering::SeqCst);
                    let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.original_identity().original_opaque());
                    let response = run
                        .complete(Some(response))
                        .await
                        .map_err(|_| {
                            rocketmq_error::RocketMQError::invariant_violated("fast-failure completion failed")
                        })?
                        .ok_or_else(|| {
                            rocketmq_error::RocketMQError::invariant_violated(
                                "fast-failure completion lost its response",
                            )
                        })?;
                    ResponsePlan::command(response).expect("completed response plan")
                }
                Err(FastFailureAwaitError::Rejected(rejection)) => {
                    rejection.into_response_plan().expect("queued rejection plan")
                }
                Err(FastFailureAwaitError::LifecycleStopped) => {
                    return Err(rocketmq_error::RocketMQError::invariant_violated(
                        "fast-failure request lifecycle stopped",
                    ));
                }
            },
        };
        Ok(HandlerOutcome::Reply(plan))
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        RejectRequestDecision::Proceed
    }

    fn observe_response(&self, _observation: ResponseObservation) {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.write_observed.notify_one();
    }
}

#[tokio::test]
async fn typed_budget_rejection_is_driven_and_written_once_by_the_canonical_dispatcher() {
    const OPAQUE: i32 = 76;

    let service = fast_failure(1);
    let held = try_admit(&service, FastFailureQueueKind::Send, metadata(70)).expect("fill pending budget");
    let held_usage = service.pending_budget_snapshot();
    let admissions = Arc::new(AtomicUsize::new(0));
    let business_executions = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let write_observed = Arc::new(Notify::new());
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-fast-failure-rejection"))
        .expect("fast-failure test runtime owner");
    let server = TransportServer::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        }),
        owner.root_context().component("fast-failure-server"),
        CanonicalFastFailureProcessor {
            service: service.clone(),
            admissions: Arc::clone(&admissions),
            business_executions: Arc::clone(&business_executions),
            writes: Arc::clone(&writes),
            write_observed: Arc::clone(&write_observed),
        },
    );
    let runner = owner.root_context().component("fast-failure-runner");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner
        .spawn_service("fast-failure-run", async move {
            let result = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await;
            let _ = result_tx.send(result);
        })
        .expect("spawn owned fast-failure server");

    let address = startup_rx
        .await
        .expect("fast-failure startup channel")
        .expect("fast-failure server startup");
    let mut client = Connection::new(TcpStream::connect(address).await.expect("connect fast-failure client"));
    let wire_request = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(OPAQUE);
    let rejected_bytes = estimate_retained_bytes(&wire_request);
    client
        .send_command(wire_request)
        .await
        .expect("send fast-failure request");
    let response = client
        .receive_command()
        .await
        .expect("fast-failure connection remains open")
        .expect("canonical busy response frame");

    assert_eq!(response.code(), ResponseCode::SystemBusy as i32);
    assert_eq!(response.opaque(), OPAQUE);
    assert_eq!(
        response.remark().map(AsRef::<str>::as_ref),
        Some(
            format!(
                "[PENDING_BUDGET]broker busy, retry later, queue: send, exhausted: Count, request bytes: \
                 {rejected_bytes}, pending count: 1, pending bytes: {}",
                held_usage.current_bytes
            )
            .as_str()
        )
    );
    assert!(response.body().is_none());
    tokio::time::timeout(Duration::from_secs(2), write_observed.notified())
        .await
        .expect("canonical write observation deadline");
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(business_executions.load(Ordering::SeqCst), 0);
    assert_eq!(writes.load(Ordering::SeqCst), 1);

    client.shutdown().await.expect("shutdown fast-failure client");
    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(2), result_rx)
        .await
        .expect("fast-failure shutdown deadline")
        .expect("fast-failure result channel")
        .expect("fast-failure shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
    let task_report = owner.shutdown_tasks().await;
    assert!(task_report.is_healthy(), "{}", task_report.to_json());
    let final_report = owner.shutdown_background();
    assert!(final_report.is_healthy(), "{}", final_report.to_json());
    drop(held);
    assert_queue_resources_released(&service);
}
