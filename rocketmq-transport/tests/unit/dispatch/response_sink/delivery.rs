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

use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::BlockingLane;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::TaskGroup;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::oneshot;

use super::*;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::ResourceLimit;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::connection::Connection;
use crate::connection::ConnectionState;
use crate::deadline::RequestDeadline;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseBody;
use crate::dispatch::ResponseBodyKind;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionLease;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;
use crate::security::TransportSecurity;
use crate::server::run_connected_session;
use crate::server::ConnectionHandler;
use crate::server::SessionHandle;
use crate::session_view::SessionStateView;

struct ControlHarness {
    runtime: RuntimeContext,
    parent: TaskGroup,
    _state_tx: tokio::sync::watch::Sender<ConnectionState>,
    closed_tx: tokio::sync::watch::Sender<bool>,
}

impl ControlHarness {
    fn new(name: &'static str, deadline: Option<RequestDeadline>) -> (Self, RequestControlView) {
        let runtime = RuntimeContext::from_current(name);
        let parent = runtime.service_context(name).task_group().clone();
        let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), deadline),
            SessionStateView::from_receivers(state_rx, closed_rx),
            &parent,
        );
        (
            Self {
                runtime,
                parent,
                _state_tx: state_tx,
                closed_tx,
            },
            control,
        )
    }

    async fn shutdown(self) {
        let report = self.runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

fn response_head(code: i32, opaque: i32) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(code).set_opaque(opaque)
}

fn bind(response: RemotingResponse, owner: u64, opaque: i32) -> BoundResponse {
    let request = RemotingCommand::create_remoting_command(31).set_opaque(opaque);
    let original = OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &request)
        .expect("test request identity should allocate");
    response.bind(original).expect("ordinary request should bind")
}

struct CountingHeader {
    encodes: Arc<AtomicUsize>,
}

impl CommandCustomHeader for CountingHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        self.encodes.fetch_add(1, Ordering::SeqCst);
        Some(std::collections::HashMap::new())
    }
}

struct CountingLease {
    file: File,
    accesses: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

struct FlushCountingTransport {
    inner: tokio::io::DuplexStream,
    flushes: Arc<AtomicUsize>,
    fail_flush: bool,
}

impl AsyncRead for FlushCountingTransport {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for FlushCountingTransport {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.inner).poll_flush(context) {
            Poll::Ready(Ok(())) if self.fail_flush => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected flush failure",
            ))),
            Poll::Ready(Ok(())) => {
                self.flushes.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }

    fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

struct CaptureSession {
    sender: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
}

impl ConnectionHandler for CaptureSession {
    fn connected(
        &self,
        session: SessionHandle,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if let Some(sender) = self.sender.lock().expect("capture session lock").take() {
                let _ = sender.send(session);
            }
        })
    }

    fn command(
        &self,
        _session: SessionHandle,
        _command: RemotingCommand,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

struct NetworkHarness {
    runtime: RuntimeContext,
    session: SessionHandle,
    peer: Connection,
    runner: tokio::task::JoinHandle<()>,
    flushes: Arc<AtomicUsize>,
}

impl NetworkHarness {
    async fn new(
        name: &'static str,
        limits: FrameLimits,
        admission_limits: AdmissionLimits,
        preflight: Option<crate::write_strategy::WritePreflightBarrier>,
    ) -> Self {
        Self::new_with_flush_behavior(name, limits, admission_limits, preflight, false).await
    }

    async fn new_with_flush_failure(name: &'static str) -> Self {
        Self::new_with_flush_behavior(name, FrameLimits::default(), AdmissionLimits::default(), None, true).await
    }

    async fn new_with_flush_behavior(
        name: &'static str,
        limits: FrameLimits,
        admission_limits: AdmissionLimits,
        preflight: Option<crate::write_strategy::WritePreflightBarrier>,
        fail_flush: bool,
    ) -> Self {
        let runtime = RuntimeContext::from_current(name);
        let service = runtime.service_context(name);
        let (transport, peer) = tokio::io::duplex(1024 * 1024);
        let flushes = Arc::new(AtomicUsize::new(0));
        let transport = FlushCountingTransport {
            inner: transport,
            flushes: Arc::clone(&flushes),
            fail_flush,
        };
        let mut connection = Connection::new_with_plaintext_stream_and_limits(transport, limits).with_file_region_io(
            runtime.blocking(BlockingLane::StorageIo).clone(),
            FileTransferMode::Portable,
        );
        if let Some(preflight) = preflight {
            connection.set_write_preflight_barrier(preflight);
        }
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let local_addr = "127.0.0.1:19021".parse().expect("local address");
        let remote_addr = "127.0.0.1:19022".parse().expect("remote address");
        let runner = tokio::spawn(run_connected_session(
            connection,
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(admission_limits)),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let session = session_rx.await.expect("capture connected session");
        Self {
            runtime,
            session,
            peer: Connection::new_with_plaintext_stream(peer),
            runner,
            flushes,
        }
    }

    fn control(&self, name: &'static str, deadline: Option<RequestDeadline>) -> (RequestControlView, TaskGroup) {
        let parent = self
            .runtime
            .root_group()
            .try_child(name)
            .expect("request control child group");
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), deadline),
            self.session.session_view().state().clone(),
            &parent,
        );
        (control, parent)
    }

    async fn receive(&mut self) -> RemotingCommand {
        self.peer
            .receive_command()
            .await
            .expect("peer should receive a frame")
            .expect("peer should decode a frame")
    }

    async fn shutdown(self) {
        let Self {
            runtime,
            session,
            peer,
            runner,
            ..
        } = self;
        drop(session);
        drop(peer);
        runner.await.expect("connected session runner should stop");
        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    async fn close_and_collect_frames(mut self) -> Vec<RemotingCommand> {
        self.peer.shutdown().await.expect("close peer write half");
        self.runner.await.expect("connected session runner should drain");
        drop(self.session);

        let mut frames = Vec::new();
        while let Some(frame) = self.peer.receive_command().await {
            frames.push(frame.expect("drained response frame should decode"));
        }
        let report = self.runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        frames
    }
}

impl FileRegionLease for CountingLease {
    fn file(&self) -> &File {
        self.accesses.fetch_add(1, Ordering::SeqCst);
        &self.file
    }
}

impl Drop for CountingLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_file_response(
    code: i32,
    opaque: i32,
    body: &[u8],
) -> (RemotingResponse, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let accesses = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut file = tempfile::tempfile().expect("temporary counting lease file");
    file.write_all(body).expect("write counting lease body");
    let lease = Arc::new(CountingLease {
        file,
        accesses: Arc::clone(&accesses),
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease.clone(), 0, body.len() as u64).expect("counting file region");
    let response = RemotingResponse::file_regions(response_head(code, opaque), FileRegionSequence::single(region))
        .expect("counting file remoting response");
    drop(lease);
    (response, accesses, drops)
}

#[path = "delivery/local.rs"]
mod local;

#[path = "delivery/network.rs"]
mod network;
