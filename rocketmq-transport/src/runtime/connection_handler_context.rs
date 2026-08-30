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

use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::Arc;

use tracing::error;

use crate::connection::ConnectionStateHandle;
use crate::dispatch::LegacySessionCleanupCapability;
pub use crate::dispatch::LegacySessionCleanupEnrollment;
pub use crate::dispatch::LegacySessionCleanupInstallError;
use crate::dispatch::LegacySessionExecutionCapability;
pub use crate::dispatch::LegacySessionExecutionEnrollment;
use crate::dispatch::LegacySessionExecutionSeed;
pub use crate::dispatch::LegacySessionExecutionSubmitError;
use crate::dispatch::ResponseError;
use crate::dispatch::ResponseReceipt;
use crate::net::channel::Channel;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

/// Shared immutable context for request handlers.
///
/// Clones share channel identity and a serialized response-writer capability without
/// exposing mutable connection or channel references.
pub type ConnectionHandlerContext = Arc<ConnectionHandlerContextWrapper>;

/// Request handler context - provides access to the channel for a specific connection.
///
/// `ConnectionHandlerContextWrapper` is the execution context passed to request processors.
/// It encapsulates the channel associated with the incoming request, allowing handlers to:
///
/// - **Send responses**: Via `write()` or `write_ref()`
/// - **Access connection metadata**: Remote address, connection state
/// - **Observe lifecycle state**: Via a cloneable handle without socket mutation
///
/// ## Design Rationale
///
/// - **Thin wrapper**: Delegates most operations to the underlying `Channel`
/// - **Hash/Eq based on channel**: Contexts for the same channel are equal
/// - **Wrapped in Arc**: Shared across async tasks without mutable capability propagation
///
/// ## Naming Note
///
/// The "Wrapper" suffix indicates this is the concrete type wrapped by the
/// `ConnectionHandlerContext` type alias. It's rarely used directly - prefer
/// using the type alias in function signatures.
pub struct ConnectionHandlerContextWrapper {
    // === Core State ===
    /// The channel associated with this request handler context.
    ///
    /// Provides access to:
    /// - Serialized command writes
    /// - Address information (local/remote)
    /// - Channel identity (ID)
    pub(crate) channel: Channel,
    legacy_session_cleanup: Option<LegacySessionCleanupCapability>,
    legacy_session_execution: Option<LegacySessionExecutionCapability>,
}

impl ConnectionHandlerContextWrapper {
    /// Creates a new handler context wrapping the given channel.
    ///
    /// # Arguments
    ///
    /// * `channel` - The channel associated with this handler invocation
    ///
    /// # Returns
    ///
    /// A new context ready for use by request processors
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            legacy_session_cleanup: None,
            legacy_session_execution: None,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn new_with_legacy_session_cleanup(
        channel: Channel,
        session_cleanup: crate::dispatch::DeferredSessionCleanupRegistration,
    ) -> Self {
        Self {
            channel,
            legacy_session_cleanup: Some(LegacySessionCleanupCapability::new(session_cleanup)),
            legacy_session_execution: None,
        }
    }

    pub(crate) fn new_with_legacy_session_execution(channel: Channel, seed: LegacySessionExecutionSeed) -> Self {
        let execution = LegacySessionExecutionCapability::new(seed);
        Self {
            channel,
            legacy_session_cleanup: Some(execution.cleanup_capability()),
            legacy_session_execution: Some(execution),
        }
    }

    /// Atomically installs one identity-keyed session-close callback with a
    /// legacy waiter node.
    ///
    /// The callback is invoked at most once and receives no request or session
    /// data. The returned enrollment is affine: the waiter must retain it until
    /// it leaves the real legacy wait table. Dropping it deregisters the
    /// callback.
    ///
    /// # Errors
    ///
    /// Returns [`LegacySessionCleanupInstallError::Unavailable`] for contexts
    /// that were not created by an admitted network request, or a typed closed,
    /// caller-install, or invariant failure.
    pub fn install_legacy_session_cleanup<T, E>(
        &self,
        cleanup: impl Fn() + Send + Sync + 'static,
        install: impl FnOnce(LegacySessionCleanupEnrollment) -> Result<T, (E, LegacySessionCleanupEnrollment)>,
    ) -> Result<T, LegacySessionCleanupInstallError<E>> {
        let Some(capability) = &self.legacy_session_cleanup else {
            return Err(LegacySessionCleanupInstallError::Unavailable);
        };
        capability.install(cleanup, install)
    }

    /// Atomically installs one identity-keyed session execution owner with a
    /// legacy waiter node.
    ///
    /// The affine enrollment must move from the waiter into its exact wake
    /// claim and then into the submitted handler. Its future runs through the
    /// original canonical session executor and remains cancellable by session
    /// close through handler and writer completion.
    ///
    /// # Errors
    ///
    /// Returns [`LegacySessionCleanupInstallError::Unavailable`] for embedded
    /// or compatibility contexts without a canonical network session owner,
    /// or a typed closed, caller-install, or invariant failure.
    pub fn install_legacy_session_execution<T, E>(
        &self,
        cleanup: impl Fn() + Send + Sync + 'static,
        install: impl FnOnce(LegacySessionExecutionEnrollment) -> Result<T, (E, LegacySessionExecutionEnrollment)>,
    ) -> Result<T, LegacySessionCleanupInstallError<E>> {
        let Some(capability) = &self.legacy_session_execution else {
            return Err(LegacySessionCleanupInstallError::Unavailable);
        };
        capability.install(cleanup, install)
    }

    // === Connection Access ===

    /// Gets an immutable connection lifecycle handle.
    ///
    /// # Returns
    ///
    /// Immutable handle for health checks and close signaling
    ///
    /// # Use Case
    ///
    /// Checking connection health, reading connection ID, etc.
    #[deprecated(
        since = "1.0.0",
        note = "Use `api::v2::RemotingRequest::session()` and `RequestControlView` instead"
    )]
    pub fn connection_ref(&self) -> &ConnectionStateHandle {
        self.channel.connection_ref()
    }

    // === Response Writing ===

    /// Writes a response and reports its local completion disposition.
    ///
    /// A successful receipt confirms only local completion: a network receipt means the canonical
    /// writer wrote and flushed the frame, while an embedded receipt means the response was handed
    /// to its in-process receiver. Neither outcome confirms peer receipt or business completion.
    /// The receipt uses a synthetic process-local V1 identifier; it is not derived from the
    /// protocol opaque value and does not provide request ownership or at-most-once allocation.
    /// V1 contexts have no request deadline capability, so this method does not synthesize a
    /// `DeadlineExceeded` failure.
    ///
    /// # Errors
    ///
    /// Returns a typed failure that preserves the write stage and, for encoding or transport
    /// failures, the source error for programmatic inspection.
    pub async fn try_write_response(&self, cmd: RemotingCommand) -> Result<ResponseReceipt, ResponseError> {
        self.channel.send_response(cmd).await
    }

    /// Writes a borrowed response and reports its local completion disposition.
    ///
    /// A successful receipt confirms only local completion: a network receipt means the canonical
    /// writer wrote and flushed the frame, while an embedded receipt means the response was handed
    /// to its in-process receiver. Neither outcome confirms peer receipt or business completion.
    /// The receipt uses a synthetic process-local V1 identifier; it is not derived from the
    /// protocol opaque value and does not provide request ownership or at-most-once allocation.
    /// The command body may be consumed after encoding and before a later error is returned.
    /// V1 contexts have no request deadline capability, so this method does not synthesize a
    /// `DeadlineExceeded` failure.
    ///
    /// # Errors
    ///
    /// Returns a typed failure that preserves the write stage and, for encoding or transport
    /// failures, the source error for programmatic inspection.
    pub async fn try_write_response_ref(&self, cmd: &mut RemotingCommand) -> Result<ResponseReceipt, ResponseError> {
        self.channel.send_response_ref(cmd).await
    }

    /// Sends a response command back to the client (consumes command).
    ///
    /// This compatibility facade logs typed local completion failures but does not propagate them.
    ///
    /// # Arguments
    ///
    /// * `cmd` - The response command to send (consumed)
    ///
    /// # Behavior
    ///
    /// - **Success**: Command completed at the local writer or embedded handoff boundary
    /// - **Error**: One redacted structured failure is logged and the method returns normally
    ///
    /// # Example
    ///
    /// ```ignore
    /// async fn handle_request(ctx: &ConnectionHandlerContext, request: RemotingCommand) {
    ///     let response = RemotingCommand::create_success_response_command()
    ///         .set_opaque(request.opaque());
    ///     ctx.write(response).await;
    /// }
    /// ```
    #[deprecated(
        since = "1.0.0",
        note = "Return `HandlerOutcome::Reply(ResponsePlan)` or use `DeferredResponder::respond` instead"
    )]
    pub async fn write_response(&self, cmd: RemotingCommand) {
        match self.try_write_response(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!(
                    kind = error.kind().as_str(),
                    progress = error.write_progress().map_or("none", |progress| progress.as_str()),
                    retryable = error.retryable(),
                    "response write failed"
                );
            }
        }
    }

    /// Sends a response command back to the client (borrows command).
    ///
    /// Similar to `write_response`, but borrows the command instead of consuming it.
    /// Use when the caller needs to retain ownership of the command after any body consumption.
    ///
    /// # Arguments
    ///
    /// * `cmd` - Mutable reference to the response command to send
    ///
    /// # Behavior
    ///
    /// - **Success**: Command completed at the local writer or embedded handoff boundary
    /// - **Error**: One redacted structured failure is logged and the method returns normally
    ///
    /// # Note
    ///
    /// The command's body may be consumed during sending (`take_body()`).
    #[deprecated(
        since = "1.0.0",
        note = "Return `HandlerOutcome::Reply(ResponsePlan)` or use `DeferredResponder::respond` instead"
    )]
    pub async fn write_response_ref(&self, cmd: &mut RemotingCommand) {
        match self.try_write_response_ref(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!(
                    kind = error.kind().as_str(),
                    progress = error.write_progress().map_or("none", |progress| progress.as_str()),
                    retryable = error.retryable(),
                    "response write failed"
                );
            }
        }
    }

    /// Legacy alias for `write_response()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Return a V2 [`crate::api::v2::ResponsePlan`] through
    /// [`crate::api::v2::HandlerOutcome::Reply`], or claim a
    /// [`crate::api::v2::DeferredResponder`] for deferred completion.
    #[deprecated(
        since = "1.0.0",
        note = "Return `HandlerOutcome::Reply(ResponsePlan)` or use `DeferredResponder::respond` instead"
    )]
    pub async fn write(&self, cmd: RemotingCommand) {
        match self.try_write_response(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!(
                    kind = error.kind().as_str(),
                    progress = error.write_progress().map_or("none", |progress| progress.as_str()),
                    retryable = error.retryable(),
                    "response write failed"
                );
            }
        }
    }

    /// Legacy alias for `write_response_ref()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Return a V2 [`crate::api::v2::ResponsePlan`] through
    /// [`crate::api::v2::HandlerOutcome::Reply`], or claim a
    /// [`crate::api::v2::DeferredResponder`] for deferred completion.
    #[deprecated(
        since = "1.0.0",
        note = "Return `HandlerOutcome::Reply(ResponsePlan)` or use `DeferredResponder::respond` instead"
    )]
    pub async fn write_ref(&self, cmd: &mut RemotingCommand) {
        match self.try_write_response_ref(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!(
                    kind = error.kind().as_str(),
                    progress = error.write_progress().map_or("none", |progress| progress.as_str()),
                    retryable = error.retryable(),
                    "response write failed"
                );
            }
        }
    }

    // === Channel Access ===

    /// Gets an immutable reference to the channel.
    ///
    /// # Returns
    ///
    /// Immutable reference to the `Channel`
    ///
    /// # Use Case
    ///
    /// Accessing channel metadata (ID, addresses, etc.)
    #[deprecated(
        since = "1.0.0",
        note = "Use `api::v2::RemotingRequest::session()` for read-only facts and composition-owned session capabilities for push or close"
    )]
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub(crate) const fn legacy_channel(&self) -> &Channel {
        &self.channel
    }

    // === Convenience Accessors ===

    /// Gets the remote peer's socket address.
    ///
    /// # Returns
    ///
    /// Socket address of the remote peer
    ///
    /// # Use Case
    ///
    /// Logging, authorization checks, rate limiting by IP
    pub fn remote_address(&self) -> SocketAddr {
        self.channel.remote_address()
    }
}

impl PartialEq for ConnectionHandlerContextWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.channel == other.channel
    }
}

impl Eq for ConnectionHandlerContextWrapper {}

impl Hash for ConnectionHandlerContextWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.channel.hash(state);
    }
}

impl AsRef<ConnectionHandlerContextWrapper> for ConnectionHandlerContextWrapper {
    fn as_ref(&self) -> &ConnectionHandlerContextWrapper {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::base::pending_request_table::PendingRequestTable;
    use crate::connection::Connection;
    use crate::dispatch::LocalResponseReceiver;
    use crate::dispatch::ResponseDisposition;
    use crate::dispatch::ResponseSink;
    use crate::net::channel::ChannelInner;
    use crate::session_view::SessionId;

    fn embedded_context(
        runtime: &rocketmq_runtime::RuntimeContext,
        name: &'static str,
    ) -> (ConnectionHandlerContext, LocalResponseReceiver) {
        let (sink, receiver) = ResponseSink::local();
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let channel = Channel::new(
            Arc::new(ChannelInner::new_local(
                sink,
                runtime.service_context(name).task_group().clone(),
            )),
            address,
            address,
        );
        (Arc::new(ConnectionHandlerContextWrapper::new(channel)), receiver)
    }

    #[tokio::test]
    async fn cloned_contexts_share_one_serialized_channel_writer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client_stream = TcpStream::connect(address).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();
        let local_address = server_stream.local_addr().unwrap();
        let remote_address = server_stream.peer_addr().unwrap();
        let response_table = PendingRequestTable::new();
        let parent = rocketmq_runtime::RuntimeContext::from_current("connection-handler-context-test")
            .service_context("connection-handler-service")
            .task_group()
            .clone();
        let inner = Arc::new(
            ChannelInner::try_new_with_pending_requests(Connection::new(server_stream), response_table, parent)
                .expect("build test channel inner"),
        );
        let channel = Channel::new(inner, local_address, remote_address);
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let context_clone = context.clone();

        assert!(Arc::ptr_eq(&context, &context_clone));
        let first = RemotingCommand::create_remoting_command(1).set_opaque(101);
        let second = RemotingCommand::create_remoting_command(2).set_opaque(101);
        let (first_receipt, second_receipt) = tokio::join!(
            context.try_write_response(first),
            context_clone.try_write_response(second)
        );
        let first_receipt = first_receipt.expect("first response should be written locally");
        let second_receipt = second_receipt.expect("second response should be written locally");
        assert_eq!(first_receipt.disposition(), ResponseDisposition::TransportWritten);
        assert_eq!(second_receipt.disposition(), ResponseDisposition::TransportWritten);
        assert_ne!(first_receipt.request_id(), second_receipt.request_id());

        let mut peer = Connection::new(client_stream);
        let first = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
            .await
            .expect("first complete frame should arrive")
            .expect("peer should remain connected")
            .expect("first frame should decode");
        let second = tokio::time::timeout(Duration::from_secs(1), peer.receive_command())
            .await
            .expect("second complete frame should arrive")
            .expect("peer should remain connected")
            .expect("second frame should decode");
        let mut opaque_ids = [first.opaque(), second.opaque()];
        opaque_ids.sort_unstable();
        assert_eq!(opaque_ids, [101, 101]);

        channel.connection_ref().close();
        assert!(!channel.connection_ref().is_healthy());
        let report = channel.close_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn embedded_context_returns_an_in_process_receipt() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("embedded-context-receipt-test");
        let (context, receiver) = embedded_context(&runtime, "embedded-context-receipt");

        let receipt = context
            .try_write_response(RemotingCommand::create_remoting_command(1).set_opaque(37))
            .await
            .expect("embedded response handoff should succeed");
        assert_eq!(receipt.disposition(), ResponseDisposition::InProcessAccepted);
        let cancellation = CancellationToken::new();
        let response = receiver
            .receive(&cancellation, None)
            .await
            .expect("embedded receiver should receive the response");
        assert_eq!(response.opaque(), 37);
    }

    #[tokio::test]
    #[allow(
        deprecated,
        reason = "the compatibility test intentionally verifies the deprecated swallow-error facade"
    )]
    async fn borrowed_response_write_consumes_the_body_before_a_late_error() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("borrowed-context-receipt-test");
        let (context, receiver) = embedded_context(&runtime, "borrowed-context-success");
        let mut response = RemotingCommand::create_remoting_command(1).set_body(b"body".to_vec());

        let receipt = context
            .try_write_response_ref(&mut response)
            .await
            .expect("embedded borrowed response handoff should succeed");
        assert_eq!(receipt.disposition(), ResponseDisposition::InProcessAccepted);
        assert!(response.body().is_none());
        let cancellation = CancellationToken::new();
        assert_eq!(
            receiver
                .receive(&cancellation, None)
                .await
                .expect("embedded receiver should receive the response")
                .body()
                .map(|body| body.as_ref()),
            Some(b"body".as_slice())
        );

        let (closed_context, closed_receiver) = embedded_context(&runtime, "borrowed-context-closed");
        drop(closed_receiver);
        let mut late_failure = RemotingCommand::create_remoting_command(2).set_body(b"late".to_vec());
        assert!(matches!(
            closed_context.try_write_response_ref(&mut late_failure).await,
            Err(ResponseError::SessionClosed)
        ));
        assert!(late_failure.body().is_none());

        let mut compatibility = RemotingCommand::create_remoting_command(3).set_body(b"compatibility".to_vec());
        closed_context.write_response_ref(&mut compatibility).await;
        assert!(compatibility.body().is_none());
        closed_context
            .write_response(RemotingCommand::create_remoting_command(4))
            .await;
    }

    #[tokio::test]
    async fn legacy_cleanup_is_affine_and_isolated_between_two_sessions() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("legacy-context-cleanup-test");
        let (base, _receiver) = embedded_context(&runtime, "legacy-context-cleanup-service");
        let first_owner = crate::dispatch::DeferredSessionCleanupOwner::new(SessionId::from_session_owner(701));
        let second_owner = crate::dispatch::DeferredSessionCleanupOwner::new(SessionId::from_session_owner(702));
        let first_context = ConnectionHandlerContextWrapper::new_with_legacy_session_cleanup(
            base.channel.clone(),
            first_owner.registration(),
        );
        let second_context = ConnectionHandlerContextWrapper::new_with_legacy_session_cleanup(
            base.channel.clone(),
            second_owner.registration(),
        );
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let mut first_enrollment = None;
        let mut second_enrollment = None;

        first_context
            .install_legacy_session_cleanup(
                {
                    let calls = Arc::clone(&first_calls);
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                    }
                },
                |enrollment| {
                    first_enrollment = Some(enrollment);
                    Ok::<_, ((), LegacySessionCleanupEnrollment)>(())
                },
            )
            .expect("first cleanup enrollment");
        second_context
            .install_legacy_session_cleanup(
                {
                    let calls = Arc::clone(&second_calls);
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                    }
                },
                |enrollment| {
                    second_enrollment = Some(enrollment);
                    Ok::<_, ((), LegacySessionCleanupEnrollment)>(())
                },
            )
            .expect("second cleanup enrollment");

        assert_eq!(
            first_owner.close(),
            crate::dispatch::DeferredSessionCleanupCloseOutcome::Completed
        );
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            first_owner.close(),
            crate::dispatch::DeferredSessionCleanupCloseOutcome::AlreadyClosed
        );
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);

        drop(first_enrollment.take());
        assert_eq!(first_owner.target_counts(), (0, 0));
        assert_eq!(second_owner.target_counts(), (1, 1));
        assert_eq!(
            second_owner.close(),
            crate::dispatch::DeferredSessionCleanupCloseOutcome::Completed
        );
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        drop(second_enrollment.take());
        assert_eq!(second_owner.target_counts(), (0, 0));
    }
}
