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

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::error;

use crate::connection::ConnectionStateHandle;
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
#[derive(Hash, Eq, PartialEq)]
pub struct ConnectionHandlerContextWrapper {
    // === Core State ===
    /// The channel associated with this request handler context.
    ///
    /// Provides access to:
    /// - Serialized command writes
    /// - Address information (local/remote)
    /// - Channel identity (ID)
    pub(crate) channel: Channel,
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
        Self { channel }
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
    /// Use `write_response()` for clearer semantics.
    #[deprecated(since = "0.6.0", note = "Use `write_response()` instead")]
    pub async fn write(&self, cmd: RemotingCommand) {
        self.write_response(cmd).await;
    }

    /// Legacy alias for `write_response_ref()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `write_response_ref()` for clearer semantics.
    #[deprecated(since = "0.6.0", note = "Use `write_response_ref()` instead")]
    pub async fn write_ref(&self, cmd: &mut RemotingCommand) {
        self.write_response_ref(cmd).await;
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
    pub fn channel(&self) -> &Channel {
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

impl AsRef<ConnectionHandlerContextWrapper> for ConnectionHandlerContextWrapper {
    fn as_ref(&self) -> &ConnectionHandlerContextWrapper {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
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

        context.connection_ref().close();
        assert!(!context_clone.connection_ref().is_healthy());
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
}
