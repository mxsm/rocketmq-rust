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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredWaitLimits;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::FileTransferMode;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::api::RequestOrdering;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::TransportServer;
use rocketmq_transport::test_support::Connection;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::index::NotificationArrivalView;
use super::index::NotificationCriteriaLimits;
use super::service::NotificationContinuationOutcome;
use super::service::NotificationDeferredPrepareFailure;
use super::service::NotificationDeferredPrepareOutcome;
use super::service::NotificationDeferredRegisterFailure;
use super::service::NotificationDeferredRegisterOutcome;
use super::service::NotificationDeferredService;
use super::service::NotificationRetainedEstimate;
use super::service::PreparedNotificationRegistration;

const ORIGINAL_OPAQUE: i32 = 9_833;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test limit is non-zero")
}

fn service(controller: &AdmissionController) -> Arc<NotificationDeferredService> {
    service_with_scan(controller, 4)
}

fn service_with_scan(controller: &AdmissionController, scan_limit: usize) -> Arc<NotificationDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
        .expect("Notification deferred admission");
    Arc::new(NotificationDeferredService::new(
        admission,
        NotificationCriteriaLimits::new(nonzero(4), 3, 1),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(scan_limit),
        nonzero(4),
        nonzero(2),
        nonzero(1024 * 1024),
    ))
}

fn request_command() -> RemotingCommand {
    request_command_for("GroupA", ORIGINAL_OPAQUE, 60_000)
}

fn request_command_for(group: &str, opaque: i32, poll_time: i64) -> RemotingCommand {
    let born_time = i64::try_from(current_millis()).expect("current wall time fits the signed protocol field");
    let header = NotificationRequestHeader {
        consumer_group: CheetahString::from_string(group.to_owned()),
        topic: CheetahString::from_static_str("TopicA"),
        queue_id: 0,
        poll_time,
        born_time,
        order: false,
        attempt_id: None,
        exp_type: None,
        exp: None,
        is_lite_consumer: false,
        client_id: Some(CheetahString::from_string(opaque.to_string())),
        topic_request_header: None,
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::Notification, header).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

#[derive(Clone, Copy)]
struct Registration {
    peer: SocketAddr,
}

fn prepared_or_test_error(
    result: Result<NotificationDeferredPrepareOutcome, NotificationDeferredPrepareFailure>,
) -> rocketmq_error::RocketMQResult<PreparedNotificationRegistration> {
    match result {
        Ok(NotificationDeferredPrepareOutcome::Prepared(prepared)) => Ok(*prepared),
        Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => {
            Err(RocketMQError::illegal_argument(format!("{:?}", rejection.kind())))
        }
        Err(error) => Err(RocketMQError::illegal_argument(error.to_string())),
    }
}

fn registration_or_test_error(
    result: Result<NotificationDeferredRegisterOutcome, NotificationDeferredRegisterFailure>,
) -> rocketmq_error::RocketMQResult<rocketmq_transport::api::DeferredRegistration> {
    match result {
        Ok(NotificationDeferredRegisterOutcome::Registered(registration)) => Ok(*registration),
        Ok(NotificationDeferredRegisterOutcome::Rejected(rejection)) => {
            Err(RocketMQError::illegal_argument(format!("{:?}", rejection.kind())))
        }
        Err(error) => Err(RocketMQError::illegal_argument(error.to_string())),
    }
}

#[derive(Clone)]
struct DeferredProcessor {
    service: Arc<NotificationDeferredService>,
    registrations: mpsc::UnboundedSender<Registration>,
    filter: Option<ArcMessageFilter>,
}

impl RequestProcessor for DeferredProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = prepared_or_test_error(self.service.prepare(
            request,
            None,
            self.filter.clone(),
            NotificationRetainedEstimate::default(),
        ))?;
        let peer = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            _ => return Err(RocketMQError::illegal_argument("trusted network origin required")),
        };
        let registration = registration_or_test_error(self.service.register(prepared, request))?;
        self.registrations
            .send(Registration { peer })
            .map_err(|_| RocketMQError::illegal_argument("Notification registration observer closed"))?;
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Concurrent
    }
}

struct ToggleFilter(Arc<AtomicBool>);

impl MessageFilter for ToggleFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

struct RunningServer {
    owner: RuntimeOwner,
    server_context: ChildServiceContext,
    action_context: ChildServiceContext,
    shutdown: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningServer {
    fn cancel_server_parent(&self) {
        self.server_context.task_group().cancel();
    }

    fn begin_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    async fn finish(mut self) {
        self.begin_shutdown();
        let report = self.result.await.expect("owned server report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let tasks = self.owner.shutdown_tasks().await;
        assert!(tasks.is_healthy(), "{}", tasks.to_json());
        let final_report = self.owner.shutdown_background();
        assert!(final_report.is_healthy(), "{}", final_report.to_json());
    }
}

async fn start_server<P>(processor: P, controller: Arc<AdmissionController>) -> (Connection, RunningServer)
where
    P: RequestProcessor + Clone + Send + Sync + 'static,
{
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("notification-deferred-acceptance"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("Notification runtime owner");
    let server_context = owner.root_context().component("notification.server");
    let runner_context = owner.root_context().component("notification.runner");
    let action_context = owner.root_context().component("notification.actions");
    let server = TransportServer::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            file_transfer_mode: FileTransferMode::Portable,
            ..ServerConfig::default()
        }),
        server_context.clone(),
        processor,
    )
    .with_admission_controller(controller);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service("notification-deferred-server", async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("owned Notification server shutdown report");
            let _ = result_tx.send(report);
        })
        .expect("spawn Notification server");
    let address = startup_rx
        .await
        .expect("Notification startup channel")
        .expect("Notification server startup");
    let client = Connection::new(TcpStream::connect(address).await.expect("connect Notification client"));
    (
        client,
        RunningServer {
            owner,
            server_context,
            action_context,
            shutdown: Some(shutdown_tx),
            result: result_rx,
        },
    )
}

#[tokio::test]
async fn notification_deferred_tcp_prepare_register_claim_resume_writes_one_frame() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send deferred Notification request");
    let registration = registrations.recv().await.expect("observe Notification registration");

    let topic = CheetahString::from_static_str("TopicA");
    let prepared = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    let batch = service.claim_prepared_arrival(prepared).await;
    let (mut claims, cursor) = batch.into_parts();
    assert!(cursor.is_complete());
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().expect("one claimed Notification waiter");
    let service_for_resume = Arc::clone(&service);
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("notification-deferred-resume", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    move |resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        assert_eq!(resume.request().effective_peer(), registration.peer);
                        let head = application_remoting_command_factory().create_success_response_command_with_header(
                            NotificationResponseHeader {
                                has_msg: false,
                                polling_full: false,
                            },
                        );
                        RemotingResponse::command(head)
                            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn Notification resume");
    assert!(matches!(
        receipt_rx
            .await
            .expect("Notification receipt channel")
            .expect("canonical Notification write"),
        rocketmq_transport::api::DeferredResumeOutcome::Completed(_)
    ));

    let response = client
        .receive_command()
        .await
        .expect("connection remains open")
        .expect("one Notification response frame");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert!(response.remark().is_none());
    assert!(response.body().is_none());
    let header = response
        .decode_command_custom_header::<NotificationResponseHeader>()
        .expect("Notification response header");
    assert!(!header.has_msg);
    assert!(!header.polling_full);
    assert_eq!(service.snapshot().index().live(), 0);
    assert_eq!(service.snapshot().admission().waiting_count(), 0);

    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "EOF proves exactly one response frame"
    );
}

#[tokio::test]
async fn notification_deferred_filter_miss_stays_registered_then_later_match_claims() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let matches = Arc::new(AtomicBool::new(false));
    let filter: ArcMessageFilter = Arc::new(ToggleFilter(Arc::clone(&matches)));
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: Some(filter),
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send filtered Notification request");
    registrations.recv().await.expect("observe filtered registration");

    let topic = CheetahString::from_static_str("TopicA");
    let missed = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    let missed = service.claim_prepared_arrival(missed).await;
    let (missed_claims, missed_cursor) = missed.into_parts();
    assert!(missed_claims.is_empty());
    assert!(missed_cursor.is_complete());
    assert_eq!(service.snapshot().index().live(), 1);
    assert_eq!(service.snapshot().index().candidates(), 0);
    assert_eq!(service.snapshot().admission().waiting_count(), 1);

    matches.store(true, Ordering::Release);
    let matched = service.prepare_arrival_batch(NotificationArrivalView::new(&topic, 0), None);
    let matched = service.claim_prepared_arrival(matched).await;
    let (mut claims, cursor) = matched.into_parts();
    assert!(cursor.is_complete());
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().expect("later matching arrival claims the waiter");
    let service_for_resume = Arc::clone(&service);
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .action_context
        .spawn_service("notification-deferred-filter-resume", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    DeferredResumeRetainedSize::default(),
                    |_resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        let head = application_remoting_command_factory().create_success_response_command_with_header(
                            NotificationResponseHeader {
                                has_msg: true,
                                polling_full: false,
                            },
                        );
                        RemotingResponse::command(head)
                            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn filtered Notification resume");
    assert!(matches!(
        receipt_rx
            .await
            .expect("filtered Notification receipt channel")
            .expect("canonical filtered Notification write"),
        rocketmq_transport::api::DeferredResumeOutcome::Completed(_)
    ));
    let response = client
        .receive_command()
        .await
        .expect("filtered connection remains open")
        .expect("filtered Notification response frame");
    let header = response
        .decode_command_custom_header::<NotificationResponseHeader>()
        .expect("filtered Notification response header");
    assert!(header.has_msg);
    assert!(!header.polling_full);
    assert_eq!(service.snapshot().index().live(), 0);
    assert_eq!(service.snapshot().admission().waiting_count(), 0);

    running.finish().await;
}

#[tokio::test]
async fn notification_deferred_session_close_drains_registry_permit_and_index() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command())
        .await
        .expect("send Notification request before session close");
    registrations.recv().await.expect("observe Notification registration");
    assert_eq!(service.snapshot().admission().waiting_count(), 1);
    assert_eq!(service.snapshot().index().live(), 1);

    client.shutdown().await.expect("close Notification client session");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = service.snapshot();
            if snapshot.admission().waiting_count() == 0 && snapshot.index().live() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session close drains Notification deferred state");
    let DeferredRegistryShutdownOutcome::Completed(stats) = service.shutdown() else {
        panic!("session-close verification should win service shutdown");
    };
    assert_eq!(
        stats.detached_entries(),
        0,
        "session close already removed the registry entry"
    );
    assert_eq!(stats.invariant_failures(), 0);
    let snapshot = service.snapshot();
    assert_eq!(snapshot.admission().waiting_count(), 0);
    assert_eq!(snapshot.admission().retained_bytes(), 0);
    assert_eq!(snapshot.index().live(), 0);
    assert_eq!(snapshot.index().reserved(), 0);
    assert_eq!(snapshot.index().candidates(), 0);
    assert_eq!(snapshot.index().keys(), 0);
    assert_eq!(snapshot.index().oldest_waiter_age_millis(), None);
    assert_eq!(snapshot.prepared(), 0);
    assert_eq!(snapshot.pending_claims(), 0);
    assert_eq!(snapshot.resume_executions(), 0);
    assert_eq!(snapshot.resume_execution_bytes(), 0);
    assert_eq!(snapshot.active_continuations(), 0);
    assert_eq!(snapshot.continuation_bytes(), 0);

    running.finish().await;
}

#[tokio::test]
async fn notification_deferred_timeout_winner_excludes_prepared_arrival_and_duplicate_claims() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    client
        .send_command(request_command_for("GroupA", 4, 250))
        .await
        .expect("send short Notification waiter");
    registrations
        .recv()
        .await
        .expect("observe short Notification registration");

    let topic = CheetahString::from_static_str("TopicA");
    let arrival = NotificationArrivalView::new(&topic, 0);
    let prepared_arrival = service.prepare_arrival_batch(arrival, None);
    assert_eq!(prepared_arrival.candidate_count(), 1);
    let duplicate_arrival = service.prepare_arrival_batch(arrival, None);
    assert_eq!(
        duplicate_arrival.candidate_count(),
        0,
        "candidate ownership excludes duplicate arrival"
    );

    let mut timeout_claims = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let claims = service.sweep_expired().into_claims();
            if !claims.is_empty() {
                break claims;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("protocol timeout becomes due");
    assert_eq!(timeout_claims.len(), 1);
    let timeout_claim = timeout_claims.pop().expect("one timeout claim");
    assert_eq!(timeout_claim.reason(), DeferredWakeReason::Timeout);

    let arrival_after_timeout = service.claim_prepared_arrival(prepared_arrival).await;
    assert!(arrival_after_timeout.into_parts().0.is_empty());
    let duplicate_after_timeout = service.claim_prepared_arrival(duplicate_arrival).await;
    assert!(duplicate_after_timeout.into_parts().0.is_empty());
    drop(timeout_claim);
    assert_eq!(service.snapshot().index().live(), 0);
    assert_eq!(service.snapshot().admission().waiting_count(), 0);

    client.shutdown().await.expect("close timeout Notification client");
    running.finish().await;
}

#[tokio::test]
async fn notification_deferred_newest_first_bounded_batch_continuation_advances_other_key() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service_with_scan(controller.as_ref(), 1);
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = DeferredProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        filter: None,
    };
    let (mut client, running) = start_server(processor, controller).await;
    for command in [
        request_command_for("GroupA", 1, 60_000),
        request_command_for("GroupA", 2, 60_000),
        request_command_for("GroupB", 3, 60_000),
    ] {
        client.send_command(command).await.expect("send Notification waiter");
        registrations
            .recv()
            .await
            .expect("observe Notification waiter registration");
    }
    assert_eq!(service.snapshot().index().live(), 3);
    assert_eq!(service.snapshot().admission().waiting_count(), 3);

    let topic = CheetahString::from_static_str("TopicA");
    let arrival = NotificationArrivalView::new(&topic, 0);
    let first = service.prepare_arrival_batch(arrival, None);
    assert_eq!(first.inspected(), 1, "callback executes one bounded synchronous batch");
    let first = service.claim_prepared_arrival(first).await;
    let (mut first_claims, cursor) = first.into_parts();
    assert_eq!(first_claims.len(), 1);
    assert!(
        !cursor.is_complete(),
        "the second matching key remains for continuation"
    );
    let first_claim = first_claims.pop().expect("newest GroupA waiter claimed");
    assert_eq!(
        first_claim.resume_data().request().header().client_id.as_deref(),
        Some("2"),
        "same-key selection is newest-first"
    );

    let continuation = match service.admit_continuation(arrival, cursor) {
        Ok(NotificationContinuationOutcome::Continued(continuation)) => continuation,
        Ok(NotificationContinuationOutcome::Rejected(rejection)) => {
            panic!("admit bounded continuation: {rejection:?}")
        }
        Err(error) => panic!("admit bounded continuation: {error:?}"),
    };
    let (claims_tx, mut claims_rx) = mpsc::unbounded_channel();
    let handle_claims = Arc::new(move |claims| {
        let claims_tx = claims_tx.clone();
        async move {
            let _ = claims_tx.send(claims);
        }
    });
    service
        .spawn_continuation(running.action_context.task_group(), continuation, handle_claims)
        .expect("spawn lifecycle-owned Notification continuation");
    let mut continued_claims = tokio::time::timeout(Duration::from_secs(2), claims_rx.recv())
        .await
        .expect("bounded continuation completes")
        .expect("continuation claim batch");
    assert_eq!(continued_claims.len(), 1);
    let continued_claim = continued_claims.pop().expect("GroupB waiter claimed by continuation");
    assert_eq!(
        continued_claim.resume_data().request().consumer_group().as_str(),
        "GroupB"
    );

    drop((first_claim, continued_claim));
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.snapshot().active_continuations() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("continuation permit released");
    assert_eq!(
        service.snapshot().index().live(),
        1,
        "older GroupA waiter remains registered"
    );
    assert_eq!(service.snapshot().admission().waiting_count(), 1);

    client
        .shutdown()
        .await
        .expect("close multi-waiter Notification session");
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.snapshot().admission().waiting_count() != 0 || service.snapshot().index().live() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session close drains final older waiter");
    assert_eq!(service.snapshot().continuation_bytes(), 0);
    running.finish().await;
}

#[path = "acceptance_tests/body_owner_audit_tests.rs"]
mod body_owner_audit_tests;
#[path = "acceptance_tests/filter_audit_tests.rs"]
mod filter_audit_tests;
#[path = "acceptance_tests/registration_audit_tests.rs"]
mod registration_audit_tests;
#[path = "acceptance_tests/snapshot_audit_tests.rs"]
mod snapshot_audit_tests;
#[path = "acceptance_tests/terminal_audit_tests.rs"]
mod terminal_audit_tests;
