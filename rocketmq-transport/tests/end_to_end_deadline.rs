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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::NetworkError;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::admission::ResourceLimit;
use rocketmq_transport::client::TransportClient;
use rocketmq_transport::connection::Connection;
use rocketmq_transport::deadline::RequestDeadline;
use rocketmq_transport::security::TransportSecurity;
use rocketmq_transport::server::run_connected_session;
use rocketmq_transport::server::ConnectionHandler;
use rocketmq_transport::server::SessionHandle;
use tokio::io::DuplexStream;
use tokio::sync::oneshot;

struct CaptureSession {
    sender: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
}

impl ConnectionHandler for CaptureSession {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if let Some(sender) = self.sender.lock().expect("capture lock").take() {
                let _ = sender.send(session);
            }
        })
    }

    fn command(
        &self,
        _session: SessionHandle,
        _command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

async fn connected_session(
    capacity: usize,
    name: &'static str,
) -> (SessionHandle, DuplexStream, tokio::task::JoinHandle<()>) {
    let (session, peer, runner, _admission) =
        connected_session_with_limits(capacity, name, AdmissionLimits::default()).await;
    (session, peer, runner)
}

async fn connected_session_with_limits(
    capacity: usize,
    name: &'static str,
    limits: AdmissionLimits,
) -> (
    SessionHandle,
    DuplexStream,
    tokio::task::JoinHandle<()>,
    Arc<AdmissionController>,
) {
    let runtime = RuntimeContext::from_current(name);
    let service = runtime.service_context(name);
    let (transport, peer) = tokio::io::duplex(capacity);
    let (session_tx, session_rx) = oneshot::channel();
    let handler = Arc::new(CaptureSession {
        sender: std::sync::Mutex::new(Some(session_tx)),
    });
    let local_addr: SocketAddr = "127.0.0.1:19011".parse().expect("local address");
    let remote_addr: SocketAddr = "127.0.0.1:19012".parse().expect("remote address");
    let admission = Arc::new(AdmissionController::new(limits));
    let runner = tokio::spawn(run_connected_session(
        Connection::new_with_stream(transport),
        local_addr,
        remote_addr,
        service.task_group().clone(),
        admission.clone(),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        None,
        Duration::from_secs(30),
        handler,
    ));
    let session = session_rx.await.expect("capture connected session");
    (session, peer, runner, admission)
}

#[tokio::test(start_paused = true)]
async fn request_deadline_is_frozen_once_at_entry() {
    let deadline = RequestDeadline::after(Duration::from_millis(100));
    let expires_at = deadline.instant();

    tokio::time::advance(Duration::from_millis(40)).await;

    assert_eq!(deadline.instant(), expires_at);
    assert_eq!(deadline.remaining(), Duration::from_millis(60));

    tokio::time::advance(Duration::from_millis(60)).await;

    assert!(deadline.is_expired());
    assert_eq!(deadline.remaining(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn expired_before_send_has_zero_remote_side_effects() {
    let (session, peer, runner) = connected_session(4096, "deadline-before-send-test").await;
    let side_effects = Arc::new(AtomicUsize::new(0));
    let remote_effects = side_effects.clone();
    let remote = tokio::spawn(async move {
        let mut peer = Connection::new_with_stream(peer);
        if peer.receive_command().await.is_some() {
            remote_effects.fetch_add(1, Ordering::SeqCst);
        }
    });
    let deadline = RequestDeadline::after(Duration::from_millis(10));
    tokio::time::advance(Duration::from_millis(10)).await;

    let error = session
        .connection()
        .send_command_with_deadline(
            RemotingCommand::create_remoting_command(1),
            deadline,
            session.remote_addr().to_string(),
        )
        .await
        .expect_err("expired request must not enter the writer");

    assert!(matches!(
        error,
        RocketMQError::Network(NetworkError::DeadlineExceededBeforeSend { .. })
    ));
    tokio::task::yield_now().await;
    assert_eq!(side_effects.load(Ordering::SeqCst), 0);

    remote.abort();
    session.task_group().cancel();
    runner.await.expect("session runner");
}

#[tokio::test(start_paused = true)]
async fn blocked_socket_write_uses_the_original_deadline() {
    let (session, _peer, runner) = connected_session(64, "deadline-write-timeout-test").await;
    let deadline = RequestDeadline::after(Duration::from_millis(50));
    let mut connection = session.connection();
    let send = tokio::spawn(async move {
        connection
            .send_command_with_deadline(
                RemotingCommand::create_remoting_command(2).set_body(vec![0_u8; 1024 * 1024]),
                deadline,
                "127.0.0.1:19012".to_string(),
            )
            .await
    });
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_millis(50)).await;
    let error = send
        .await
        .expect("send task")
        .expect_err("blocked socket write must time out");

    assert!(matches!(
        error,
        RocketMQError::Network(NetworkError::WriteTimeout { timeout_ms: 50, .. })
    ));

    session.task_group().cancel();
    runner.await.expect("session runner");
}

#[tokio::test(start_paused = true)]
async fn full_outbound_admission_returns_queue_full_without_extending_deadline() {
    let limits = AdmissionLimits {
        queued: ResourceLimit {
            count: 1,
            bytes: 4 * 1024 * 1024,
        },
        ..AdmissionLimits::default()
    };
    let (session, _peer, runner, admission) =
        connected_session_with_limits(64, "deadline-full-queue-test", limits).await;
    let first_deadline = RequestDeadline::after(Duration::from_secs(10));
    let mut first_connection = session.connection();
    let first = tokio::spawn(async move {
        first_connection
            .send_command_with_deadline(
                RemotingCommand::create_remoting_command(9000).set_body(vec![0_u8; 1024 * 1024]),
                first_deadline,
                "127.0.0.1:19012".to_string(),
            )
            .await
    });
    while admission.snapshot().queued.current_count != 1 {
        if first.is_finished() {
            let result = first.await.expect("first send task");
            panic!("first send completed before holding admission: {result:?}");
        }
        tokio::task::yield_now().await;
    }

    let error = session
        .connection()
        .send_command_with_deadline(
            RemotingCommand::create_remoting_command(9001),
            RequestDeadline::after(Duration::from_secs(1)),
            "127.0.0.1:19012".to_string(),
        )
        .await
        .expect_err("full admission must reject immediately");

    assert!(
        matches!(error, RocketMQError::Network(NetworkError::QueueFull { .. })),
        "unexpected error: {error:?}"
    );

    session.task_group().cancel();
    assert!(first.await.expect("first send task").is_err());
    runner.await.expect("session runner");
}

#[tokio::test(start_paused = true)]
async fn queued_request_expiry_is_reported_before_send() {
    let (session, _peer, runner, admission) =
        connected_session_with_limits(64, "deadline-queue-wait-test", AdmissionLimits::default()).await;
    let mut first_connection = session.connection();
    let first = tokio::spawn(async move {
        first_connection
            .send_command_with_deadline(
                RemotingCommand::create_remoting_command(9100).set_body(vec![0_u8; 1024 * 1024]),
                RequestDeadline::after(Duration::from_secs(10)),
                "127.0.0.1:19012".to_string(),
            )
            .await
    });
    while admission.snapshot().queued.current_count != 1 {
        tokio::task::yield_now().await;
    }

    let mut queued_connection = session.connection();
    let queued = tokio::spawn(async move {
        queued_connection
            .send_command_with_deadline(
                RemotingCommand::create_remoting_command(9101),
                RequestDeadline::after(Duration::from_millis(50)),
                "127.0.0.1:19012".to_string(),
            )
            .await
    });
    while admission.snapshot().queued.current_count != 2 {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(Duration::from_millis(50)).await;
    let error = queued
        .await
        .expect("queued send task")
        .expect_err("queued request must expire before socket write");

    assert!(matches!(
        error,
        RocketMQError::Network(NetworkError::DeadlineExceededBeforeSend { .. })
    ));

    session.task_group().cancel();
    assert!(first.await.expect("first send task").is_err());
    runner.await.expect("session runner");
}

#[tokio::test(start_paused = true)]
async fn missing_response_uses_the_same_absolute_response_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (request_seen_tx, request_seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        let _request = connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
        let _ = request_seen_tx.send(());
        let _ = release_rx.await;
    });
    let runtime = RuntimeContext::from_current("deadline-no-response-test");
    let service = runtime.service_context("deadline-no-response-client");
    let client = Arc::new(TransportClient::new(
        service,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    ));
    let invoking = client.clone();
    let invoke = tokio::spawn(async move {
        invoking
            .invoke(
                address,
                RemotingCommand::create_remoting_command(3),
                RequestDeadline::after(Duration::from_millis(100)),
            )
            .await
    });
    request_seen_rx.await.expect("server observed request");

    tokio::time::advance(Duration::from_millis(100)).await;
    let error = match invoke.await.expect("invoke task") {
        Ok(_) => panic!("missing response must time out"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        RocketMQError::Network(NetworkError::ResponseTimeout { timeout_ms: 100, .. })
    ));

    let _ = release_tx.send(());
    server.await.expect("server task");
}
