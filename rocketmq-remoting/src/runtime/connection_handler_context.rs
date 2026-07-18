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
use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;

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

    /// Sends a response command back to the client (consumes command).
    ///
    /// This is the primary method for responding to requests. Errors are
    /// logged but not propagated to allow handlers to continue processing.
    ///
    /// # Arguments
    ///
    /// * `cmd` - The response command to send (consumed)
    ///
    /// # Behavior
    ///
    /// - **Success**: Command encoded and sent
    /// - **Error**: Logged at ERROR level, method returns normally
    ///
    /// # Example
    ///
    /// ```ignore
    /// async fn handle_request(ctx: &ConnectionHandlerContext, request: RemotingCommand) {
    ///     let response = RemotingCommand::create_response_command()
    ///         .set_opaque(request.opaque());
    ///     ctx.write(response).await;
    /// }
    /// ```
    pub async fn write_response(&self, cmd: RemotingCommand) {
        match self.channel.send_command(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!("failed to send response: {}", error);
            }
        }
    }

    /// Sends a response command back to the client (borrows command).
    ///
    /// Similar to `write_response`, but borrows the command instead of consuming it.
    /// Use when the caller needs to retain ownership of the command.
    ///
    /// # Arguments
    ///
    /// * `cmd` - Mutable reference to the response command to send
    ///
    /// # Behavior
    ///
    /// - **Success**: Command encoded and sent
    /// - **Error**: Logged at ERROR level, method returns normally
    ///
    /// # Note
    ///
    /// The command's body may be consumed during sending (`take_body()`).
    pub async fn write_response_ref(&self, cmd: &mut RemotingCommand) {
        match self.channel.send_command_ref(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!("failed to send response: {}", error);
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
    use std::collections::HashMap;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    use super::*;
    use crate::base::response_future::ResponseFuture;
    use crate::connection::Connection;
    use crate::net::channel::ChannelInner;

    #[tokio::test]
    async fn cloned_contexts_share_one_serialized_channel_writer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client_stream = TcpStream::connect(address).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();
        let local_address = server_stream.local_addr().unwrap();
        let remote_address = server_stream.peer_addr().unwrap();
        let response_table = Arc::new(parking_lot::Mutex::new(HashMap::<i32, ResponseFuture>::new()));
        let inner = Arc::new(ChannelInner::new(Connection::new(server_stream), response_table));
        let channel = Channel::new(inner, local_address, remote_address);
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let context_clone = context.clone();

        assert!(Arc::ptr_eq(&context, &context_clone));
        let first = RemotingCommand::create_remoting_command(1).set_opaque(101);
        let second = RemotingCommand::create_remoting_command(2).set_opaque(202);
        tokio::join!(context.write_response(first), context_clone.write_response(second));

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
        assert_eq!(opaque_ids, [101, 202]);

        context.connection_ref().close();
        assert!(!context_clone.connection_ref().is_healthy());
        let report = channel.close_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}
