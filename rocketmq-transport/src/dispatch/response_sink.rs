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

#[allow(
    dead_code,
    reason = "RSP-05 defines the private plan delivery seam wired by the later dispatcher stage"
)]
mod plan;

use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use tokio::sync::oneshot;
use tokio_util::codec::Decoder;
use tokio_util::sync::CancellationToken;

use crate::codec::remoting_command_codec::FrameLimits;
use crate::codec::remoting_command_codec::RemotingCommandCodec;
use crate::deadline::RequestDeadline;
use crate::dispatch::DeferredResponseSeed;
use crate::dispatch::ResponseDisposition;
use crate::dispatch::ResponseError;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::ResponseTerminalState;
use crate::server::SessionHandle;
use crate::session_view::SessionStateView;
use rocketmq_runtime::TaskGroup;

use plan::LocalPlanSenderState;
pub(crate) use plan::LocalResponsePlanReceiver;
pub(crate) use plan::NetworkResponsePlanContext;
pub(crate) use plan::ResponseTransportDropHandle;

/// Typed failure produced by a response output capability.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResponseSinkError {
    /// A single-response sink was completed more than once.
    #[error("response sink was already completed")]
    AlreadyCompleted,
    /// The caller stopped waiting before the response was delivered.
    #[error("response receiver was dropped")]
    ReceiverDropped,
    /// The request owner cancelled the response wait.
    #[error("response wait was cancelled")]
    Cancelled,
    /// The immutable request deadline expired.
    #[error("response deadline expired")]
    DeadlineExceeded,
    /// A network writer rejected or failed the response.
    #[error("response transport failed: {0}")]
    Transport(String),
    /// A processor-provided encoded response was malformed.
    #[error("encoded local response failed to decode: {0}")]
    Decode(String),
}

struct LegacyLocalResponseState {
    sender: Option<oneshot::Sender<Result<RemotingCommand, ResponseSinkError>>>,
    encoded: BytesMut,
    terminal_state: Option<ResponseTerminalState>,
}

#[derive(Clone)]
enum LocalResponseMode {
    Legacy(Arc<parking_lot::Mutex<LegacyLocalResponseState>>),
    #[allow(
        dead_code,
        reason = "RSP-05 plan mode is constructed by the private seam wired by the later dispatcher stage"
    )]
    Plan(Arc<LocalPlanSenderState>),
}

#[derive(Clone)]
/// Cloneable single-response capability used by the embedded adapter.
///
/// Construction remains private to [`ResponseSink::local`].
pub struct LocalResponseSink {
    mode: LocalResponseMode,
}

impl LocalResponseSink {
    fn legacy_state(&self) -> Result<&parking_lot::Mutex<LegacyLocalResponseState>, ResponseSinkError> {
        match &self.mode {
            LocalResponseMode::Legacy(state) => Ok(state),
            LocalResponseMode::Plan(_) => Err(ResponseSinkError::AlreadyCompleted),
        }
    }

    fn complete(&self, result: Result<RemotingCommand, ResponseSinkError>) -> Result<(), ResponseSinkError> {
        let mut state = self.legacy_state()?.lock();
        let sender = state.sender.take().ok_or(ResponseSinkError::AlreadyCompleted)?;
        let terminal_state = if result.is_ok() {
            ResponseTerminalState::Completed
        } else {
            ResponseTerminalState::Failed {
                progress: crate::dispatch::WriteProgress::NotStarted,
            }
        };
        match sender.send(result) {
            Ok(()) => {
                state.terminal_state = Some(terminal_state);
                Ok(())
            }
            Err(_) => {
                state.terminal_state = Some(ResponseTerminalState::Closed);
                Err(ResponseSinkError::ReceiverDropped)
            }
        }
    }

    fn complete_legacy_v1_legacy_mode(
        &self,
        command: RemotingCommand,
        receipt: ResponseReceipt,
    ) -> Result<ResponseReceipt, ResponseError> {
        let mut state = self
            .legacy_state()
            .map_err(|_| ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Closed,
            })?
            .lock();
        let Some(sender) = state.sender.take() else {
            return Err(ResponseError::AlreadyCompleted {
                state: state.terminal_state.unwrap_or(ResponseTerminalState::Closed),
            });
        };

        match sender.send(Ok(command)) {
            Ok(()) => {
                state.terminal_state = Some(ResponseTerminalState::Completed);
                Ok(receipt)
            }
            Err(_) => {
                state.terminal_state = Some(ResponseTerminalState::Closed);
                Err(ResponseError::SessionClosed)
            }
        }
    }

    async fn complete_legacy_v1(
        &self,
        command: RemotingCommand,
        receipt: ResponseReceipt,
    ) -> Result<ResponseReceipt, ResponseError> {
        match &self.mode {
            LocalResponseMode::Legacy(_) => self.complete_legacy_v1_legacy_mode(command, receipt),
            LocalResponseMode::Plan(_) => plan::complete_local_legacy(self.clone(), command, receipt).await,
        }
    }

    fn send_bytes(&self, bytes: Bytes) -> Result<(), ResponseSinkError> {
        let mut state = self.legacy_state()?.lock();
        if state.sender.is_none() {
            return Err(ResponseSinkError::AlreadyCompleted);
        }
        state.encoded.extend_from_slice(&bytes);
        let decoded = RemotingCommandCodec::with_limits(FrameLimits::java_compatibility())
            .decode(&mut state.encoded)
            .map_err(|error| ResponseSinkError::Decode(error.to_string()))?;
        let Some(command) = decoded else {
            return Ok(());
        };
        if !state.encoded.is_empty() {
            return Err(ResponseSinkError::Decode(
                "multiple commands were written to a single-response local sink".to_owned(),
            ));
        }
        let sender = state.sender.take().ok_or(ResponseSinkError::AlreadyCompleted)?;
        match sender.send(Ok(command)) {
            Ok(()) => {
                state.terminal_state = Some(ResponseTerminalState::Completed);
                Ok(())
            }
            Err(_) => {
                state.terminal_state = Some(ResponseTerminalState::Closed);
                Err(ResponseSinkError::ReceiverDropped)
            }
        }
    }
}

impl Drop for LocalResponseSink {
    fn drop(&mut self) {
        if let LocalResponseMode::Plan(state) = &self.mode {
            if Arc::strong_count(state) == 1 {
                state.close_last_sender();
            }
        }
    }
}

/// Closed response output variants for network and in-process dispatch.
#[derive(Clone)]
pub enum ResponseSink {
    /// A bounded canonical session writer.
    Network(Arc<SessionHandle>),
    /// A single-use in-process response channel.
    Local(LocalResponseSink),
}

impl ResponseSink {
    /// Creates an in-process sink and its single owner receiver.
    #[must_use]
    pub fn local() -> (Self, LocalResponseReceiver) {
        let (sender, receiver) = oneshot::channel();
        let sink = LocalResponseSink {
            mode: LocalResponseMode::Legacy(Arc::new(parking_lot::Mutex::new(LegacyLocalResponseState {
                sender: Some(sender),
                encoded: BytesMut::new(),
                terminal_state: None,
            }))),
        };
        (Self::Local(sink), LocalResponseReceiver { receiver })
    }

    /// Returns whether this sink performs no socket I/O.
    #[must_use]
    pub const fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    #[allow(
        dead_code,
        reason = "DSP-05 bridge provenance remains dormant until DSP-06 coexistence routing"
    )]
    pub(crate) const fn is_network_transport(&self) -> bool {
        matches!(self, Self::Network(_))
    }

    #[allow(
        dead_code,
        reason = "DSP-05 bridge provenance remains dormant until DSP-06 coexistence routing"
    )]
    pub(crate) fn same_completion_owner(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Network(left), Self::Network(right)) => left.same_canonical_owner(right),
            (Self::Local(left), Self::Local(right)) => match (&left.mode, &right.mode) {
                (LocalResponseMode::Legacy(left), LocalResponseMode::Legacy(right)) => Arc::ptr_eq(left, right),
                (LocalResponseMode::Plan(left), LocalResponseMode::Plan(right)) => Arc::ptr_eq(left, right),
                (LocalResponseMode::Legacy(_), LocalResponseMode::Plan(_))
                | (LocalResponseMode::Plan(_), LocalResponseMode::Legacy(_)) => false,
            },
            (Self::Network(_), Self::Local(_)) | (Self::Local(_), Self::Network(_)) => false,
        }
    }

    /// Proves that this is the local plan capability whose control observes
    /// the supplied embedded lifecycle owners.
    pub(crate) fn is_local_plan_owner(&self, session: &SessionStateView, task_group: &TaskGroup) -> bool {
        matches!(
            self,
            Self::Local(LocalResponseSink {
                mode: LocalResponseMode::Plan(state),
            }) if state.control().same_lifecycle_owner(session, task_group)
        )
    }

    /// Builds the deferred responder seed for a canonical embedded local-plan
    /// owner. The lifecycle proof prevents a sink, session view, and task group
    /// from unrelated dispatches being combined.
    pub(crate) fn local_deferred_seed_with_resume(
        &self,
        telemetry: crate::telemetry::TransportTelemetry,
        session: &crate::session_view::SessionView,
        task_group: &TaskGroup,
        ordering: crate::request_ordering::RequestOrdering,
        class: crate::admission::AdmissionClass,
        executor: crate::session_executor::DeferredResumeExecutor,
    ) -> Option<DeferredResponseSeed> {
        if !matches!(session, crate::session_view::SessionView::Embedded { .. })
            || !self.is_local_plan_owner(session.state(), task_group)
        {
            return None;
        }
        Some(
            DeferredResponseSeed::new(
                self.clone(),
                telemetry,
                session.id(),
                self.local_plan_control()?.clone(),
            )
            .with_resume_context(ordering, class, executor),
        )
    }

    fn local_plan_control(&self) -> Option<&crate::dispatch::RequestControlView> {
        match self {
            Self::Local(LocalResponseSink {
                mode: LocalResponseMode::Plan(state),
            }) => Some(state.control()),
            Self::Network(_)
            | Self::Local(LocalResponseSink {
                mode: LocalResponseMode::Legacy(_),
            }) => None,
        }
    }

    #[allow(
        dead_code,
        reason = "DSP-05 bridge provenance remains dormant until DSP-06 coexistence routing"
    )]
    pub(crate) fn is_network_owner(&self, session: &SessionHandle) -> bool {
        matches!(self, Self::Network(owner) if owner.same_canonical_owner(session))
    }

    /// Proves this is the plan-bound view of a canonical network session. A bare
    /// compatibility sink is not sufficient for the legacy adapter because it
    /// has no shared completion slot.
    pub(crate) fn is_canonical_network_plan_owner(&self, session: &SessionHandle) -> bool {
        matches!(
            self,
            Self::Network(owner)
                if owner.same_canonical_owner(session)
                    && owner
                        .response_plan_context()
                        .is_some_and(|context| context.same_lifecycle_owner(session))
        )
    }

    pub(crate) fn network_deferred_seed_with_resume(
        &self,
        session: &SessionHandle,
        ordering: crate::request_ordering::RequestOrdering,
        class: crate::admission::AdmissionClass,
        executor: crate::session_executor::DeferredResumeExecutor,
    ) -> Option<DeferredResponseSeed> {
        if !self.is_canonical_network_plan_owner(session) {
            return None;
        }
        let Self::Network(owner) = self else {
            return None;
        };
        let context = owner.response_plan_context()?;
        Some(
            DeferredResponseSeed::new(
                self.clone(),
                session.connection().telemetry(),
                session.session_view().id(),
                context.control().clone(),
            )
            .with_resume_context(ordering, class, executor),
        )
    }

    #[cfg(test)]
    pub(crate) fn network_deferred_seed(&self, session: &SessionHandle) -> Option<DeferredResponseSeed> {
        if !self.is_canonical_network_plan_owner(session) {
            return None;
        }
        let Self::Network(owner) = self else {
            return None;
        };
        let context = owner.response_plan_context()?;
        Some(DeferredResponseSeed::new(
            self.clone(),
            session.connection().telemetry(),
            session.session_view().id(),
            context.control().clone(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn deferred_seed_for_test(
        &self,
        telemetry: crate::telemetry::TransportTelemetry,
        session_id: crate::session_view::SessionId,
        control: crate::dispatch::RequestControlView,
    ) -> DeferredResponseSeed {
        DeferredResponseSeed::new(self.clone(), telemetry, session_id, control)
    }

    #[cfg(test)]
    pub(crate) fn terminal_state(&self) -> Option<ResponseTerminalState> {
        match self {
            Self::Network(session) => session.response_plan_context()?.terminal_state(),
            Self::Local(LocalResponseSink {
                mode: LocalResponseMode::Plan(state),
            }) => state.terminal_state(),
            Self::Local(LocalResponseSink {
                mode: LocalResponseMode::Legacy(state),
            }) => state.lock().terminal_state,
        }
    }

    pub(crate) fn reserve_legacy_v1_receipt(&self) -> Result<ResponseReceipt, ResponseError> {
        let disposition = match self {
            Self::Network(_) => ResponseDisposition::TransportWritten,
            Self::Local(_) => ResponseDisposition::InProcessAccepted,
        };
        ResponseReceipt::legacy_v1(disposition)
    }

    pub(crate) async fn complete_legacy_v1_reserved(
        &self,
        command: RemotingCommand,
        receipt: ResponseReceipt,
    ) -> Result<ResponseReceipt, ResponseError> {
        match self {
            Self::Network(session) if session.response_plan_context().is_some() => {
                plan::complete_network_legacy(Arc::clone(session), command, receipt).await
            }
            Self::Network(session) => session.connection().send_response(command).await.map(|()| receipt),
            Self::Local(sink) => sink.complete_legacy_v1(command, receipt).await,
        }
    }

    /// Delivers one materialized response.
    ///
    /// # Errors
    ///
    /// Returns a typed lifecycle, duplicate-response, or writer error.
    pub async fn send(&self, command: RemotingCommand) -> Result<(), ResponseSinkError> {
        match self {
            Self::Network(session) => session
                .connection()
                .send_command(command)
                .await
                .map_err(|error| ResponseSinkError::Transport(error.to_string())),
            Self::Local(sink) => sink.complete(Ok(command)),
        }
    }

    /// Delivers an already encoded response without introducing a local socket.
    ///
    /// The local branch incrementally decodes processor-generated response
    /// segments into one command. It never encodes the inbound request.
    ///
    /// # Errors
    ///
    /// Returns a typed decoding, lifecycle, or writer error.
    pub async fn send_bytes(&self, bytes: Bytes) -> Result<(), ResponseSinkError> {
        match self {
            Self::Network(session) => session
                .connection()
                .send_bytes(bytes)
                .await
                .map_err(|error| ResponseSinkError::Transport(error.to_string())),
            Self::Local(sink) => sink.send_bytes(bytes),
        }
    }

    /// Delivers one pre-encoded response as a validated immutable segment sequence.
    pub async fn send_frame_segments(&self, segments: Vec<Bytes>) -> Result<(), ResponseSinkError> {
        match self {
            Self::Network(session) => session
                .connection()
                .send_frame_segments(segments)
                .await
                .map_err(|error| ResponseSinkError::Transport(error.to_string())),
            Self::Local(sink) => {
                for segment in segments {
                    sink.send_bytes(segment)?;
                }
                Ok(())
            }
        }
    }
}

/// Single owner of an in-process response result.
pub struct LocalResponseReceiver {
    receiver: oneshot::Receiver<Result<RemotingCommand, ResponseSinkError>>,
}

impl LocalResponseReceiver {
    /// Waits under the parent cancellation token and immutable request deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed cancellation, deadline, sender-drop, or response error.
    pub async fn receive(
        mut self,
        cancellation: &CancellationToken,
        deadline: Option<RequestDeadline>,
    ) -> Result<RemotingCommand, ResponseSinkError> {
        let result = if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ResponseSinkError::Cancelled),
                result = deadline.timeout(&mut self.receiver) => {
                    result.map_err(|_| ResponseSinkError::DeadlineExceeded)?
                }
            }
        } else {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ResponseSinkError::Cancelled),
                result = &mut self.receiver => result,
            }
        };
        result.map_err(|_| ResponseSinkError::ReceiverDropped)?
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;
    use tokio::io::DuplexStream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::admission::AdmissionController;
    use crate::admission::AdmissionLimits;
    use crate::connection::Connection;
    use crate::net::channel::ChannelInner;
    use crate::security::TransportSecurity;
    use crate::server::run_connected_session;
    use crate::server::ConnectionHandler;

    struct CaptureSession {
        sender: std::sync::Mutex<Option<oneshot::Sender<SessionHandle>>>,
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

    async fn connected_network_sink(
        name: &'static str,
    ) -> (RuntimeContext, ResponseSink, DuplexStream, tokio::task::JoinHandle<()>) {
        let runtime = RuntimeContext::from_current(name);
        let service = runtime.service_context(name);
        let (transport, peer) = tokio::io::duplex(4096);
        let (session_tx, session_rx) = oneshot::channel();
        let handler = Arc::new(CaptureSession {
            sender: std::sync::Mutex::new(Some(session_tx)),
        });
        let local_addr: SocketAddr = "127.0.0.1:19011".parse().expect("local address");
        let remote_addr: SocketAddr = "127.0.0.1:19012".parse().expect("remote address");
        let runner = tokio::spawn(run_connected_session(
            Connection::new_with_plaintext_stream(transport),
            local_addr,
            remote_addr,
            service.task_group().clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            None,
            Duration::from_secs(30),
            handler,
        ));
        let session = session_rx.await.expect("capture connected session");

        (runtime, ResponseSink::Network(Arc::new(session)), peer, runner)
    }

    fn legacy_v1_receipt(sink: &ResponseSink) -> ResponseReceipt {
        sink.reserve_legacy_v1_receipt()
            .expect("V1 receipt identity should be available")
    }

    #[tokio::test]
    async fn network_sink_reservation_and_local_channel_adapter_write_transport_receipts() {
        let (runtime, sink, peer, runner) = connected_network_sink("response-sink-network-receipt-test").await;

        assert_eq!(
            legacy_v1_receipt(&sink).disposition(),
            ResponseDisposition::TransportWritten
        );

        let channel = ChannelInner::new_local(
            sink,
            runtime
                .service_context("response-sink-network-channel")
                .task_group()
                .clone(),
        );
        let owned_receipt = channel
            .send_response(RemotingCommand::create_remoting_command(1).set_opaque(71))
            .await
            .expect("network response should write one owned frame");
        let mut borrowed = RemotingCommand::create_remoting_command(2)
            .set_opaque(72)
            .set_body(b"borrowed network body".to_vec());
        let borrowed_receipt = channel
            .send_response_ref(&mut borrowed)
            .await
            .expect("network response should write one borrowed frame");

        assert_eq!(owned_receipt.disposition(), ResponseDisposition::TransportWritten);
        assert_eq!(borrowed_receipt.disposition(), ResponseDisposition::TransportWritten);
        assert!(
            borrowed.body().is_none(),
            "borrowed response body should be consumed after handoff"
        );

        let mut peer = Connection::new_with_plaintext_stream(peer);
        let owned = peer
            .receive_command()
            .await
            .expect("network peer should receive owned response")
            .expect("owned response frame should decode");
        let borrowed = peer
            .receive_command()
            .await
            .expect("network peer should receive borrowed response")
            .expect("borrowed response frame should decode");
        assert_eq!(owned.opaque(), 71);
        assert_eq!(borrowed.opaque(), 72);
        assert_eq!(
            borrowed.body().map(bytes::Bytes::as_ref),
            Some(&b"borrowed network body"[..])
        );

        drop(channel);
        drop(peer);
        runner
            .await
            .expect("connected session runner should stop after peer closes");
    }

    #[tokio::test]
    async fn legacy_v1_local_completion_returns_an_in_process_receipt() {
        let (sink, receiver) = ResponseSink::local();
        let command = RemotingCommand::create_remoting_command(1).set_opaque(77);

        let receipt = sink
            .complete_legacy_v1_reserved(command, legacy_v1_receipt(&sink))
            .await
            .expect("local response completion should hand off the command");

        assert_eq!(receipt.disposition(), ResponseDisposition::InProcessAccepted);
        let cancellation = CancellationToken::new();
        let received = receiver
            .receive(&cancellation, None)
            .await
            .expect("local response receiver should receive the command");
        assert_eq!(received.opaque(), 77);
    }

    #[tokio::test]
    async fn legacy_v1_local_completion_reports_receiver_drop_and_prior_terminal_state() {
        let (sink, receiver) = ResponseSink::local();
        drop(receiver);

        let error = sink
            .complete_legacy_v1_reserved(RemotingCommand::create_remoting_command(1), legacy_v1_receipt(&sink))
            .await
            .expect_err("dropped receiver should close the local response session");
        assert!(matches!(error, ResponseError::SessionClosed));

        let duplicate = sink
            .complete_legacy_v1_reserved(RemotingCommand::create_remoting_command(2), legacy_v1_receipt(&sink))
            .await
            .expect_err("closed local response sink should reject a second completion");
        assert!(matches!(
            duplicate,
            ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Closed
            }
        ));
    }

    #[tokio::test]
    async fn legacy_v1_local_completion_reports_completed_for_duplicates() {
        let (sink, _receiver) = ResponseSink::local();

        sink.complete_legacy_v1_reserved(RemotingCommand::create_remoting_command(1), legacy_v1_receipt(&sink))
            .await
            .expect("first completion should succeed");
        let duplicate = sink
            .complete_legacy_v1_reserved(RemotingCommand::create_remoting_command(2), legacy_v1_receipt(&sink))
            .await
            .expect_err("second completion should be rejected");

        assert!(matches!(
            duplicate,
            ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Completed
            }
        ));
    }
}
