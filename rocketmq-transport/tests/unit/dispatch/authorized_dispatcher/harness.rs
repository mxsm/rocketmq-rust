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

pub(super) use std::future::Future;
pub(super) use std::net::IpAddr;
pub(super) use std::net::Ipv4Addr;
pub(super) use std::pin::Pin;
pub(super) use std::sync::atomic::AtomicBool;
pub(super) use std::sync::atomic::AtomicU64;
pub(super) use std::sync::atomic::AtomicUsize;
pub(super) use std::sync::atomic::Ordering;
pub(super) use std::sync::Arc;
pub(super) use std::sync::Mutex;
pub(super) use std::task::Context;
pub(super) use std::task::Poll;
pub(super) use std::task::Waker;
pub(super) use std::time::Duration;
pub(super) use std::time::Instant;

pub(super) use bytes::Bytes;
pub(super) use rocketmq_error::RocketMQError;
pub(super) use rocketmq_protocol::code::response_code::ResponseCode;
pub(super) use rocketmq_runtime::RuntimeContext;
pub(super) use rocketmq_runtime::ShutdownDeadline;
pub(super) use rocketmq_security_api::PeerInfo;
pub(super) use tokio::io::AsyncRead;
pub(super) use tokio::io::AsyncWrite;
pub(super) use tokio::io::ReadBuf;

pub(super) use super::super::*;
pub(super) use crate::admission::AdmissionController;
pub(super) use crate::admission::AdmissionLimits;
pub(super) use crate::admission::AdmissionResource;
pub(super) use crate::admission::AdmissionScope;
pub(super) use crate::connection::Connection;
pub(super) use crate::deadline::RequestDeadline;
pub(super) use crate::dispatch::DeferredId;
pub(super) use crate::dispatch::DeferredParts;
pub(super) use crate::dispatch::DeferredRegistry;
pub(super) use crate::dispatch::DeferredRequest;
pub(super) use crate::dispatch::DeferredRetainedSizeParts;
pub(super) use crate::dispatch::HandlerOutcome;
pub(super) use crate::dispatch::ProtocolNoResponseReason;
pub(super) use crate::dispatch::RemotingRequest;
pub(super) use crate::dispatch::ResponseBodyKind;
pub(super) use crate::dispatch::ResponseDisposition;
pub(super) use crate::request_ordering::RequestOrdering;
pub(super) use crate::request_ordering::RequestOrderingKey;
pub(super) use crate::runtime::processor::RejectRequestDecision;
pub(super) use crate::runtime::processor::ResponseWriteObservation;
pub(super) use crate::runtime::processor::ResponseWriteOutcome;
pub(super) use crate::runtime::processor::ResponseWritePath;
pub(super) use crate::security::TransportSecurity;
pub(super) use crate::server::run_connected_session;
pub(super) use crate::server::ConnectionHandler;
pub(super) use tokio::sync::oneshot;

#[derive(Clone, Copy)]
pub(super) enum Behavior {
    Reply,
    WaitReply,
    Reject,
    Error,
    NoReply,
    Deferred,
    UnclaimedDeferred,
    ReplyAfterDeferred,
}

#[derive(Default)]
pub(super) struct ProcessorState {
    pub(super) clones: AtomicUsize,
    pub(super) rejects: AtomicUsize,
    pub(super) processes: AtomicUsize,
    pub(super) request_body_pointer: Mutex<Option<usize>>,
    pub(super) events: Mutex<Vec<&'static str>>,
    pub(super) observations: Mutex<Vec<ResponseWriteObservation>>,
    pub(super) terminal_observations: Mutex<Vec<crate::runtime::processor::ResponseObservation>>,
    pub(super) observed: tokio::sync::Notify,
    pub(super) entered: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
}

pub(super) struct TestProcessor {
    behavior: Behavior,
    state: Arc<ProcessorState>,
}

impl TestProcessor {
    pub(super) fn new(behavior: Behavior) -> (Self, Arc<ProcessorState>) {
        let state = Arc::new(ProcessorState::default());
        (
            Self {
                behavior,
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl Clone for TestProcessor {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            behavior: self.behavior,
            state: Arc::clone(&self.state),
        }
    }
}

impl RequestProcessor for TestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.state.processes.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("event lock").push("process");
        *self.state.request_body_pointer.lock().expect("body pointer lock") =
            request.command().body().map(|body| body.as_ptr() as usize);
        if matches!(self.behavior, Behavior::WaitReply) {
            self.state.entered.notify_one();
            self.state.resume.notified().await;
        }
        match self.behavior {
            Behavior::Reply | Behavior::WaitReply | Behavior::Reject => Ok(HandlerOutcome::Reply(
                RemotingResponse::bytes(
                    RemotingCommand::create_response_command_with_code(71).set_opaque(-777),
                    Bytes::from_static(b"response body"),
                )
                .expect("test remoting response"),
            )),
            Behavior::Error => Err(RocketMQError::illegal_argument("processor failure")),
            Behavior::NoReply => Ok(HandlerOutcome::NoReply(
                request
                    .protocol_no_response(ProtocolNoResponseReason::CallbackHandled)
                    .map_err(|_| RocketMQError::illegal_argument("protocol no-response contract failed"))?,
            )),
            Behavior::Deferred => {
                request
                    .mark_deferred_response_taken()
                    .expect("test request reserves deferred capability");
                Ok(HandlerOutcome::Deferred(
                    crate::dispatch::DeferredRegistration::for_test(request.original_identity().request_id()),
                ))
            }
            Behavior::UnclaimedDeferred => Ok(HandlerOutcome::Deferred(
                crate::dispatch::DeferredRegistration::for_test(request.original_identity().request_id()),
            )),
            Behavior::ReplyAfterDeferred => {
                request
                    .mark_deferred_response_taken()
                    .expect("test request reserves deferred capability");
                Ok(HandlerOutcome::Reply(
                    RemotingResponse::command(RemotingCommand::create_response_command_with_code(75))
                        .expect("reply after deferred plan"),
                ))
            }
        }
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        self.state.rejects.fetch_add(1, Ordering::SeqCst);
        self.state.events.lock().expect("event lock").push("reject");
        match self.behavior {
            Behavior::Reject => RejectRequestDecision::Reject(
                RemotingResponse::bytes(
                    RemotingCommand::create_response_command_with_code(73).set_opaque(-991),
                    Bytes::from_static(b"structured rejection"),
                )
                .expect("test rejection plan"),
            ),
            Behavior::Reply
            | Behavior::WaitReply
            | Behavior::Error
            | Behavior::NoReply
            | Behavior::Deferred
            | Behavior::UnclaimedDeferred
            | Behavior::ReplyAfterDeferred => RejectRequestDecision::Proceed,
        }
    }

    fn request_ordering(&self, ingress: crate::dispatch::IngressRequestView<'_>) -> RequestOrdering {
        self.state.events.lock().expect("event lock").push("ordering");
        assert_eq!(ingress.original_identity().original_opaque(), 811);
        assert_eq!(
            ingress
                .ext_fields()
                .and_then(|fields| fields.get("ingress"))
                .map(cheetah_string::CheetahString::as_str),
            Some("preserved")
        );
        if matches!(self.behavior, Behavior::WaitReply) {
            RequestOrdering::Ordered(RequestOrderingKey::new(17))
        } else {
            RequestOrdering::Concurrent
        }
    }

    fn observe_response(&self, observation: crate::runtime::processor::ResponseObservation) {
        if let Some(write) = observation.write_projection() {
            self.state.events.lock().expect("event lock").push("observe");
            self.state.observations.lock().expect("observation lock").push(write);
        }
        self.state
            .terminal_observations
            .lock()
            .expect("terminal observation lock")
            .push(observation);
        self.state.observed.notify_one();
    }
}

pub(super) struct RecordingHook {
    pub(super) events: Arc<Mutex<Vec<&'static str>>>,
    pub(super) before_body_seen: Arc<AtomicUsize>,
    pub(super) after_request_body_seen: Arc<AtomicUsize>,
    pub(super) after_response_body_seen: Arc<AtomicUsize>,
    pub(super) fail_after: bool,
    pub(super) attach_before_body: bool,
    pub(super) attach_after_body: bool,
    pub(super) mark_after_oneway: bool,
    pub(super) clear_after_response_type: bool,
}

impl crate::runtime::RPCHook for RecordingHook {
    fn do_before_request(
        &self,
        _remote_addr: std::net::SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.events.lock().expect("hook event lock").push("before");
        self.before_body_seen
            .fetch_add(usize::from(request.body().is_some()), Ordering::SeqCst);
        request.set_opaque_mut(-81);
        request.set_code_mut(-82);
        request.add_ext_field("hook-before", "applied");
        if self.attach_before_body {
            request.set_body_mut_ref(Bytes::from_static(b"forbidden hook body"));
        }
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: std::net::SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.events.lock().expect("hook event lock").push("after");
        self.after_request_body_seen
            .fetch_add(usize::from(request.body().is_some()), Ordering::SeqCst);
        self.after_response_body_seen
            .fetch_add(usize::from(response.body().is_some()), Ordering::SeqCst);
        response.set_opaque_mut(-83);
        response.add_ext_field("hook-after", "applied");
        if self.attach_after_body {
            response.set_body_mut_ref(Bytes::from_static(b"forbidden response hook body"));
        }
        if self.clear_after_response_type {
            let flag = response.flag() & !1;
            let head = std::mem::replace(response, RemotingCommand::create_remoting_command(0));
            *response = head.set_flag(flag);
        }
        if self.fail_after {
            Err(RocketMQError::illegal_argument("after hook failure"))
        } else {
            if self.mark_after_oneway {
                response.mark_oneway_rpc_ref();
            }
            Ok(())
        }
    }
}

struct CaptureSession {
    sender: Mutex<Option<oneshot::Sender<SessionHandle>>>,
}

struct FlushFailingStream {
    inner: tokio::io::DuplexStream,
    fail_flush: bool,
    post_start_barrier: Option<Arc<PostStartWriteBarrier>>,
}

pub(super) struct PostStartWriteBarrier {
    reached: tokio::sync::Notify,
    released: AtomicBool,
    writer_waker: Mutex<Option<Waker>>,
}

impl PostStartWriteBarrier {
    fn new() -> Self {
        Self {
            reached: tokio::sync::Notify::new(),
            released: AtomicBool::new(false),
            writer_waker: Mutex::new(None),
        }
    }

    fn poll_ready(&self, context: &Context<'_>) -> Poll<()> {
        if self.released.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.writer_waker.lock().expect("post-start writer waker lock") = Some(context.waker().clone());
        self.reached.notify_one();
        if self.released.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub(super) async fn wait_reached(&self) {
        self.reached.notified().await;
    }

    pub(super) fn release(&self) {
        self.released.store(true, Ordering::Release);
        if let Some(waker) = self.writer_waker.lock().expect("post-start writer waker lock").take() {
            waker.wake();
        }
    }
}

impl AsyncRead for FlushFailingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for FlushFailingStream {
    fn poll_write(mut self: Pin<&mut Self>, context: &mut Context<'_>, buffer: &[u8]) -> Poll<std::io::Result<usize>> {
        if self
            .post_start_barrier
            .as_ref()
            .is_some_and(|barrier| barrier.poll_ready(context).is_pending())
        {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        if self
            .post_start_barrier
            .as_ref()
            .is_some_and(|barrier| barrier.poll_ready(context).is_pending())
        {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(context) {
            Poll::Ready(Ok(())) if self.fail_flush => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected flush failure",
            ))),
            result => result,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl ConnectionHandler for CaptureSession {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

pub(super) struct DispatchHarness {
    pub(super) runtime: RuntimeContext,
    pub(super) session: SessionHandle,
    request_sequence: AtomicU64,
    pub(super) authorized: AuthorizedDispatchSession,
    pub(super) admission_scope: crate::admission::AdmissionScopeHandle,
    pub(super) admission_controller: Arc<AdmissionController>,
    pub(super) peer: Connection,
    pub(super) runner: tokio::task::JoinHandle<()>,
    pub(super) security_profile: rocketmq_security_api::SecurityBootstrapProfile,
}

impl DispatchHarness {
    pub(super) async fn new(name: &'static str) -> Self {
        Self::new_with_options(
            name,
            AdmissionLimits::default(),
            false,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        )
        .await
    }

    pub(super) async fn new_with_limits(name: &'static str, limits: AdmissionLimits) -> Self {
        Self::new_with_options(
            name,
            limits,
            false,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        )
        .await
    }

    pub(super) async fn new_with_flush_failure(name: &'static str) -> Self {
        Self::new_with_options(
            name,
            AdmissionLimits::default(),
            true,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        )
        .await
    }

    pub(super) async fn new_with_security(name: &'static str, security: Arc<TransportSecurity>) -> Self {
        Self::new_with_options(name, AdmissionLimits::default(), false, security).await
    }

    pub(super) async fn new_with_post_start_barrier(name: &'static str) -> (Self, Arc<PostStartWriteBarrier>) {
        let barrier = Arc::new(PostStartWriteBarrier::new());
        let harness = Self::new_with_stream_options(
            name,
            AdmissionLimits::default(),
            false,
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            Some(Arc::clone(&barrier)),
        )
        .await;
        (harness, barrier)
    }

    pub(super) async fn new_with_options(
        name: &'static str,
        limits: AdmissionLimits,
        fail_flush: bool,
        security: Arc<TransportSecurity>,
    ) -> Self {
        Self::new_with_stream_options(name, limits, fail_flush, security, None).await
    }

    async fn new_with_stream_options(
        name: &'static str,
        limits: AdmissionLimits,
        fail_flush: bool,
        security: Arc<TransportSecurity>,
        post_start_barrier: Option<Arc<PostStartWriteBarrier>>,
    ) -> Self {
        let runtime = RuntimeContext::from_current(name);
        let service = runtime.service_context(name);
        let admission = Arc::new(AdmissionController::new(limits));
        let security_profile = security.profile();
        let (transport, peer) = tokio::io::duplex(1024 * 1024);
        let transport = FlushFailingStream {
            inner: transport,
            fail_flush,
            post_start_barrier,
        };
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: Mutex::new(Some(session_tx)),
        });
        let local_addr = "127.0.0.1:19131".parse().expect("local address");
        let remote_addr = "127.0.0.1:19132".parse().expect("remote address");
        let runner = tokio::spawn(run_connected_session(
            Connection::new_with_plaintext_stream(transport).with_file_region_io(
                runtime.blocking(rocketmq_runtime::BlockingLane::StorageIo).clone(),
                crate::file_region::FileTransferMode::Portable,
            ),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::clone(&admission),
            Arc::clone(&security),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let session = session_rx.await.expect("capture canonical session");
        let scope = AdmissionScope::new(IpAddr::V4(Ipv4Addr::LOCALHOST)).with_session(session.session_id());
        let admission_scope = admission.prepare_scope(scope).expect("prepare dispatch scope");
        let boundary = Arc::new(super::super::super::AuthorizedDispatchBoundary::new(
            security,
            Arc::clone(&admission),
        ));
        let authorized = boundary
            .session(service.task_group(), admission_scope.clone())
            .expect("authorized dispatch session");
        Self {
            runtime,
            session,
            request_sequence: AtomicU64::new(1),
            authorized,
            admission_scope,
            admission_controller: admission,
            peer: Connection::new_with_plaintext_stream(peer),
            runner,
            security_profile,
        }
    }

    pub(super) fn request_session(&self, command: &RemotingCommand) -> (SessionHandle, OriginalRequestIdentity) {
        let original = OriginalRequestIdentity::capture(self.session.session_id(), &self.request_sequence, command)
            .expect("capture request identity");
        (
            self.session.clone().with_original_request_identity(Some(original)),
            original,
        )
    }

    pub(super) fn context(&self, deadline: Option<RequestDeadline>) -> RequestContext {
        RequestContext::network_with_security_profile(
            PeerInfo::new(self.session.remote_addr(), false),
            None,
            deadline,
            self.security_profile,
        )
    }

    pub(super) async fn receive(&mut self) -> RemotingCommand {
        tokio::time::timeout(Duration::from_secs(2), self.peer.receive_command())
            .await
            .expect("response timeout")
            .expect("response read")
            .expect("response frame")
    }

    pub(super) async fn assert_no_response(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(25), self.peer.receive_command())
                .await
                .is_err()
        );
    }

    pub(super) async fn assert_no_response_frame(&mut self) {
        match tokio::time::timeout(Duration::from_millis(25), self.peer.receive_command()).await {
            Err(_) | Ok(None) => {}
            Ok(Some(Ok(command))) => panic!("unexpected response command: {}", command.code()),
            Ok(Some(Err(error))) => panic!("unexpected response read failure: {error}"),
        }
    }

    pub(super) async fn drain_requests(&self) {
        self.authorized
            .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await
            .assert_no_task_leak()
            .expect("requests should drain");
    }

    pub(super) async fn shutdown(self) {
        self.drain_requests().await;
        drop(self.session);
        drop(self.peer);
        self.runner.await.expect("session runner should stop");
        let report = self.runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

pub(super) async fn wait_for_observation_count(state: &ProcessorState, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let notified = state.observed.notified();
            if state.observations.lock().expect("observation lock").len() >= expected {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("response observation barrier");
}

pub(super) fn request(one_way: bool) -> RemotingCommand {
    let mut command = RemotingCommand::create_remoting_command(91)
        .set_opaque(811)
        .set_body(Bytes::from_static(b"request body"));
    command.add_ext_field("ingress", "preserved");
    if one_way {
        command.mark_oneway_rpc()
    } else {
        command
    }
}

pub(super) fn hook(fail_after: bool, attach_before_body: bool, events: Arc<Mutex<Vec<&'static str>>>) -> RecordingHook {
    RecordingHook {
        events,
        before_body_seen: Arc::new(AtomicUsize::new(0)),
        after_request_body_seen: Arc::new(AtomicUsize::new(0)),
        after_response_body_seen: Arc::new(AtomicUsize::new(0)),
        fail_after,
        attach_before_body,
        attach_after_body: false,
        mark_after_oneway: false,
        clear_after_response_type: false,
    }
}
