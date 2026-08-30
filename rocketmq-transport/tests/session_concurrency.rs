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

#![cfg(feature = "test-support")]

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::TaskKind;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::RequestOrdering;
use rocketmq_transport::api::RequestOrderingKey;
use rocketmq_transport::api::ResourceLimit;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::run_connected_session;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::ConnectionHandler;
use rocketmq_transport::test_support::SessionHandle;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::Semaphore;

const ORDERED_FIRST: i32 = 40_001;
const ORDERED_SECOND: i32 = 40_002;
const UNRELATED: i32 = 40_003;
const ORDERING_KEY: RequestOrderingKey = RequestOrderingKey::new(17);

type SessionRunner = tokio::task::JoinHandle<()>;

async fn start_session<H>(
    name: &'static str,
    limits: AdmissionLimits,
    handler: Arc<H>,
) -> (RuntimeContext, ChildServiceContext, Connection, SessionRunner)
where
    H: ConnectionHandler,
{
    let runtime = RuntimeContext::from_current(name);
    let service = runtime.service_context(name);
    let (transport, peer) = tokio::io::duplex(64 * 1024);
    let local_addr: SocketAddr = "127.0.0.1:19101".parse().expect("local address");
    let remote_addr: SocketAddr = "127.0.0.1:19102".parse().expect("remote address");
    let runner = tokio::spawn(run_connected_session(
        Connection::new_with_plaintext_stream(transport),
        local_addr,
        remote_addr,
        service.task_group().clone(),
        Arc::new(AdmissionController::new(limits)),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        None,
        Duration::from_secs(30),
        handler,
    ));
    (runtime, service, Connection::new_with_plaintext_stream(peer), runner)
}

async fn finish_session(
    runtime: RuntimeContext,
    service: ChildServiceContext,
    peer: Connection,
    runner: SessionRunner,
) {
    drop(peer);
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .expect("session runner should observe peer closure")
        .expect("session runner should complete");
    drop(service);
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn request_churn_is_history_independent() {
    const REQUESTS: usize = 100_000;

    let runtime = RuntimeContext::from_current("transport-request-churn");
    let service = runtime.service_context("session-component");
    let owner = service.task_group().clone();
    let baseline_components = owner.component_count();
    let operation = OperationContext::without_deadline(TaskKind::Worker);

    for _ in 0..REQUESTS {
        owner
            .spawn_operation(&operation, "transport-request", async {})
            .expect("request operation should spawn");
    }
    operation.close_admission();
    assert!(operation
        .wait(&owner, Duration::from_secs(10))
        .await
        .expect("operation must remain bound to its session component"));

    assert_eq!(operation.active_task_count(), 0);
    assert_eq!(owner.task_count(), 0);
    assert_eq!(owner.component_count(), baseline_components);
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_close_racing_with_request_registration_leaves_no_task() {
    const ATTEMPTS: usize = 10_000;

    let runtime = RuntimeContext::from_current("transport-session-close-race");
    let service = runtime.service_context("session-component");
    let owner = service.task_group().clone();
    let operation = OperationContext::without_deadline(TaskKind::Worker);
    let operation_for_spawn = operation.clone();
    let owner_for_spawn = owner.clone();
    let (first_registered_tx, first_registered_rx) = tokio::sync::oneshot::channel();

    let spawner = tokio::spawn(async move {
        let mut first_registered_tx = Some(first_registered_tx);
        let mut accepted = 0;
        for _ in 0..ATTEMPTS {
            match owner_for_spawn.spawn_operation(&operation_for_spawn, "racing-request", async {}) {
                Ok(_) => {
                    accepted += 1;
                    if let Some(first_registered_tx) = first_registered_tx.take() {
                        let _ = first_registered_tx.send(());
                    }
                }
                Err(_) => break,
            }
            tokio::task::yield_now().await;
        }
        accepted
    });

    first_registered_rx
        .await
        .expect("at least one request should register before close");
    assert!(operation
        .cancel_and_wait(&owner, Duration::from_secs(5))
        .await
        .expect("session operation must remain bound to its component"));
    assert!(spawner.await.expect("request spawner should join") > 0);
    assert_eq!(operation.active_task_count(), 0);
    assert_eq!(owner.task_count(), 0);

    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

async fn send_success(session: SessionHandle, request: RemotingCommand) {
    let mut connection = session.connection();
    connection
        .send_command(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()),
        )
        .await
        .expect("response should reach the session writer");
}

struct SlowDataHandler {
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
}

struct CapturingHandler {
    connected: StdMutex<Option<tokio::sync::oneshot::Sender<SessionHandle>>>,
}

impl ConnectionHandler for CapturingHandler {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let connected = self
            .connected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Box::pin(async move {
            if let Some(connected) = connected {
                let _ = connected.send(session);
            }
        })
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(send_success(session, request))
    }
}

#[tokio::test]
async fn session_writer_reports_bounded_queue_and_write_diagnostics() {
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let handler = Arc::new(CapturingHandler {
        connected: StdMutex::new(Some(connected_tx)),
    });
    let (runtime, service, mut peer, runner) =
        start_session("session-writer-diagnostics", AdmissionLimits::default(), handler).await;
    let session = connected_rx
        .await
        .expect("handler should receive the session capability");

    let initial = session.writer_snapshot();
    assert!(initial.capacity > 0);
    assert_eq!(initial.capacity, initial.control_capacity + initial.data_capacity);
    assert!(initial.control_capacity > 0);
    assert!(initial.data_capacity > 0);
    assert_eq!(initial.queued_items, 0);
    assert_eq!(initial.queued_bytes, 0);
    assert_eq!(initial.control_queued_items, 0);
    assert_eq!(initial.control_queued_bytes, 0);
    assert_eq!(initial.data_queued_items, 0);
    assert_eq!(initial.data_queued_bytes, 0);

    peer.send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(7))
        .await
        .expect("send diagnostic request");
    let response = peer
        .receive_command()
        .await
        .expect("session should remain open")
        .expect("diagnostic response should decode");
    assert_eq!(response.opaque(), 7);

    let completed = session.writer_snapshot();
    assert_eq!(completed.accepted, 1);
    assert_eq!(completed.completed, 1);
    assert_eq!(completed.failed, 0);
    assert_eq!(completed.queued_items, 0);
    assert_eq!(completed.queued_bytes, 0);
    assert_eq!(completed.control_queued_items, 0);
    assert_eq!(completed.control_queued_bytes, 0);
    assert_eq!(completed.data_queued_items, 0);
    assert_eq!(completed.data_queued_bytes, 0);
    assert_eq!(completed.oldest_queue_age_millis, None);

    finish_session(runtime, service, peer, runner).await;
}

impl ConnectionHandler for SlowDataHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if request.code() == RequestCode::SendMessage.to_i32() {
                entered.notify_one();
                release
                    .acquire()
                    .await
                    .expect("test release semaphore remains open")
                    .forget();
            }
            send_success(session, request).await;
        })
    }
}

#[tokio::test]
async fn reader_dispatches_control_while_a_data_request_is_running() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let handler = Arc::new(SlowDataHandler {
        entered: entered.clone(),
        release: release.clone(),
    });
    let limits = AdmissionLimits {
        processors: ResourceLimit {
            count: 2,
            bytes: 1024 * 1024,
        },
        control_reserve: ResourceLimit {
            count: 1,
            bytes: 1024 * 1024,
        },
        ..AdmissionLimits::default()
    };
    let (runtime, service, mut peer, runner) = start_session("session-control-progress", limits, handler).await;

    peer.send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(1))
        .await
        .expect("send slow data request");
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("data request should enter its processor");

    peer.send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(2))
        .await
        .expect("send control request");
    let control = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
        .await
        .expect("control request should not wait for data")
        .expect("session should remain open")
        .expect("control response should decode");
    assert_eq!(control.opaque(), 2);
    assert_eq!(control.code(), ResponseCode::Success.to_i32());

    release.add_permits(1);
    let data = peer
        .receive_command()
        .await
        .expect("session should remain open")
        .expect("data response should decode");
    assert_eq!(data.opaque(), 1);

    finish_session(runtime, service, peer, runner).await;
}

#[tokio::test]
async fn slow_session_does_not_block_an_independent_session_writer() {
    let first_entered = Arc::new(Notify::new());
    let first_release = Arc::new(Semaphore::new(0));
    let first_handler = Arc::new(SlowDataHandler {
        entered: first_entered.clone(),
        release: first_release.clone(),
    });
    let second_entered = Arc::new(Notify::new());
    let second_release = Arc::new(Semaphore::new(0));
    let second_handler = Arc::new(SlowDataHandler {
        entered: second_entered,
        release: second_release,
    });
    let (first_runtime, first_service, mut first_peer, first_runner) =
        start_session("session-independent-first", AdmissionLimits::default(), first_handler).await;
    let (second_runtime, second_service, mut second_peer, second_runner) =
        start_session("session-independent-second", AdmissionLimits::default(), second_handler).await;

    first_peer
        .send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(1))
        .await
        .expect("send blocked request to first session");
    tokio::time::timeout(Duration::from_secs(1), first_entered.notified())
        .await
        .expect("first session request should enter");

    second_peer
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(2))
        .await
        .expect("send control request to independent session");
    let second_response = tokio::time::timeout(Duration::from_secs(1), second_peer.receive_command())
        .await
        .expect("independent session writer must make progress")
        .expect("second session should remain open")
        .expect("second response should decode");
    assert_eq!(second_response.opaque(), 2);

    first_release.add_permits(1);
    let first_response = first_peer
        .receive_command()
        .await
        .expect("first session should remain open")
        .expect("first response should decode");
    assert_eq!(first_response.opaque(), 1);

    finish_session(first_runtime, first_service, first_peer, first_runner).await;
    finish_session(second_runtime, second_service, second_peer, second_runner).await;
}

struct BoundedDataHandler {
    entered: mpsc::UnboundedSender<i32>,
    release: Arc<Semaphore>,
    active_data: AtomicUsize,
    max_active_data: AtomicUsize,
}

impl ConnectionHandler for BoundedDataHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if request.code() == RequestCode::SendMessage.to_i32() {
                let active = self.active_data.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active_data.fetch_max(active, Ordering::SeqCst);
                entered.send(request.opaque()).expect("test observer remains open");
                release
                    .acquire()
                    .await
                    .expect("test release semaphore remains open")
                    .forget();
                self.active_data.fetch_sub(1, Ordering::SeqCst);
            }
            send_success(session, request).await;
        })
    }
}

#[tokio::test]
async fn processor_limit_rejects_excess_data_without_closing_the_session() {
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let handler = Arc::new(BoundedDataHandler {
        entered: entered_tx,
        release: release.clone(),
        active_data: AtomicUsize::new(0),
        max_active_data: AtomicUsize::new(0),
    });
    let limits = AdmissionLimits {
        processors: ResourceLimit {
            count: 3,
            bytes: 1024 * 1024,
        },
        control_reserve: ResourceLimit {
            count: 1,
            bytes: 1024 * 1024,
        },
        ..AdmissionLimits::default()
    };
    let (runtime, service, mut peer, runner) = start_session("session-processor-bound", limits, handler.clone()).await;

    for opaque in 1..=2 {
        peer.send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(opaque))
            .await
            .expect("send admitted data request");
    }
    let first = entered_rx.recv().await.expect("first request should enter");
    let second = entered_rx.recv().await.expect("second request should enter");
    assert_eq!(HashSet::from([first, second]), HashSet::from([1, 2]));

    peer.send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(3))
        .await
        .expect("send overloaded data request");
    let rejection = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
        .await
        .expect("overload should return a bounded response")
        .expect("overload should not close the session")
        .expect("overload response should decode");
    assert_eq!(rejection.opaque(), 3);
    assert_eq!(rejection.code(), ResponseCode::SystemBusy.to_i32());

    peer.send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(4))
        .await
        .expect("send reserved control request");
    let control = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
        .await
        .expect("control reserve should remain responsive")
        .expect("session should remain open")
        .expect("control response should decode");
    assert_eq!(control.opaque(), 4);
    assert_eq!(control.code(), ResponseCode::Success.to_i32());

    release.add_permits(2);
    let mut completed = HashSet::new();
    for _ in 0..2 {
        let response = peer
            .receive_command()
            .await
            .expect("session should remain open")
            .expect("data response should decode");
        completed.insert(response.opaque());
    }
    assert_eq!(completed, HashSet::from([1, 2]));
    assert_eq!(handler.max_active_data.load(Ordering::SeqCst), 2);

    finish_session(runtime, service, peer, runner).await;
}

struct OrderingHandler {
    entered: mpsc::UnboundedSender<i32>,
    release_first: Arc<Semaphore>,
}

impl ConnectionHandler for OrderingHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        match request.code() {
            ORDERED_FIRST | ORDERED_SECOND => RequestOrdering::Ordered(ORDERING_KEY),
            _ => RequestOrdering::Concurrent,
        }
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let entered = self.entered.clone();
        let release_first = self.release_first.clone();
        Box::pin(async move {
            entered.send(request.code()).expect("test observer remains open");
            if request.code() == ORDERED_FIRST {
                release_first
                    .acquire()
                    .await
                    .expect("test release semaphore remains open")
                    .forget();
            }
            send_success(session, request).await;
        })
    }
}

#[tokio::test]
async fn explicit_ordering_serializes_one_key_without_blocking_unrelated_work() {
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let release_first = Arc::new(Semaphore::new(0));
    let handler = Arc::new(OrderingHandler {
        entered: entered_tx,
        release_first: release_first.clone(),
    });
    let (runtime, service, mut peer, runner) =
        start_session("session-request-ordering", AdmissionLimits::default(), handler).await;

    peer.send_command(RemotingCommand::create_remoting_command(ORDERED_FIRST).set_opaque(1))
        .await
        .expect("send first ordered request");
    assert_eq!(entered_rx.recv().await, Some(ORDERED_FIRST));

    peer.send_command(RemotingCommand::create_remoting_command(ORDERED_SECOND).set_opaque(2))
        .await
        .expect("send second ordered request");
    peer.send_command(RemotingCommand::create_remoting_command(UNRELATED).set_opaque(3))
        .await
        .expect("send unrelated request");

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("unrelated work should enter"),
        Some(UNRELATED)
    );
    let unrelated = peer
        .receive_command()
        .await
        .expect("session should remain open")
        .expect("unrelated response should decode");
    assert_eq!(unrelated.opaque(), 3);

    release_first.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("second ordered request should follow its predecessor"),
        Some(ORDERED_SECOND)
    );
    let first = peer
        .receive_command()
        .await
        .expect("session should remain open")
        .expect("first ordered response should decode");
    let second = peer
        .receive_command()
        .await
        .expect("session should remain open")
        .expect("second ordered response should decode");
    assert_eq!((first.opaque(), second.opaque()), (1, 2));

    finish_session(runtime, service, peer, runner).await;
}

#[tokio::test]
async fn shutdown_drains_accepted_requests_before_flushing_and_closing_writer() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let handler = Arc::new(SlowDataHandler {
        entered: entered.clone(),
        release: release.clone(),
    });
    let (runtime, service, mut peer, runner) =
        start_session("session-graceful-drain", AdmissionLimits::default(), handler).await;

    peer.send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(9))
        .await
        .expect("send request before shutdown");
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("accepted request should enter");

    service.task_group().cancel();
    release.add_permits(1);

    let response = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
        .await
        .expect("accepted request should drain")
        .expect("writer should remain open while draining")
        .expect("drained response should decode");
    assert_eq!(response.opaque(), 9);
    assert!(tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
        .await
        .expect("writer should close after its queue is flushed")
        .is_none());
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .expect("session runner should complete after writer closure")
        .expect("session runner should not panic");

    drop(peer);
    drop(service);
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}
