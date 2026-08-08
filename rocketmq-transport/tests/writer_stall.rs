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

use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::ConnectionState;
use rocketmq_transport::api::v1::TransportSecurity;
use rocketmq_transport::test_support::run_connected_session_with_io_policy;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::ConnectionHandler;
use rocketmq_transport::test_support::SessionHandle;
use rocketmq_transport::test_support::SessionIoPolicy;
use rocketmq_transport::test_support::WriterQueueConfig;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tokio::sync::Notify;

struct PermanentlyStalledTransport {
    write_polled: Arc<Notify>,
}

impl AsyncRead for PermanentlyStalledTransport {
    fn poll_read(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for PermanentlyStalledTransport {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, _buffer: &[u8]) -> Poll<io::Result<usize>> {
        self.write_polled.notify_one();
        Poll::Pending
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.write_polled.notify_one();
        Poll::Pending
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct CaptureSessionHandler {
    sessions: mpsc::Sender<SessionHandle>,
}

impl ConnectionHandler for CaptureSessionHandler {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let sessions = self.sessions.clone();
        Box::pin(async move {
            let _ = sessions.send(session).await;
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

#[tokio::test(start_paused = true)]
async fn hard_write_stall_deadline_poison_closes_and_drains_session_writer() {
    let runtime = RuntimeContext::from_current("writer-stall-deadline");
    let service = runtime.service_context("writer-stall-deadline");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let write_polled = Arc::new(Notify::new());
    let (sessions_tx, mut sessions_rx) = mpsc::channel(1);
    let local_addr: SocketAddr = "127.0.0.1:19301".parse().expect("local address");
    let remote_addr: SocketAddr = "127.0.0.1:19302".parse().expect("remote address");
    let runner = tokio::spawn(run_connected_session_with_io_policy(
        Connection::new_with_plaintext_stream(PermanentlyStalledTransport {
            write_polled: write_polled.clone(),
        }),
        local_addr,
        remote_addr,
        service.task_group().clone(),
        admission.clone(),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        None,
        SessionIoPolicy {
            idle_timeout: Duration::from_secs(30),
            writer_queue: WriterQueueConfig {
                max_write_stall: Duration::from_millis(50),
                ..WriterQueueConfig::default()
            },
        },
        Arc::new(CaptureSessionHandler { sessions: sessions_tx }),
    ));
    let session = sessions_rx.recv().await.expect("session handle");

    let mut first_connection = session.connection();
    let first = tokio::spawn(async move {
        first_connection
            .send_command(RemotingCommand::create_remoting_command(10_101).set_opaque(1))
            .await
    });
    write_polled.notified().await;

    let mut second_connection = session.connection();
    let second = tokio::spawn(async move {
        second_connection
            .send_command(RemotingCommand::create_remoting_command(10_102).set_opaque(2))
            .await
    });
    while session.writer_snapshot().queued_items != 1 {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(Duration::from_millis(49)).await;
    assert!(
        !first.is_finished(),
        "write must remain active before the hard deadline"
    );
    assert!(
        !second.is_finished(),
        "queued write must remain pending before the hard deadline"
    );

    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(first.await.expect("first send task").is_err());
    assert!(second.await.expect("second send task").is_err());

    let snapshot = session.writer_snapshot();
    assert_eq!(snapshot.queued_items, 0);
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.failed, 2);
    assert_eq!(snapshot.deadline_expired, 1);
    assert_eq!(admission.snapshot().queued.current_count, 0);
    assert_eq!(session.connection().state(), ConnectionState::Closed);
    assert!(session
        .connection()
        .send_command(RemotingCommand::create_remoting_command(10_103))
        .await
        .is_err());

    runner.await.expect("session runner");
    drop(service);
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}
