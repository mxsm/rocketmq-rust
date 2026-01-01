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

use rocketmq_rust::ArcMut;
use tracing::error;

use crate::connection::Connection;
use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;

/// Shared mutable context for request handlers.
///
/// This type alias wraps `ConnectionHandlerContextWrapper` in an `ArcMut` to allow
/// efficient sharing and mutation across async tasks. Handlers receive this context
/// and use it to access the channel and send responses.
pub type ConnectionHandlerContext = ArcMut<ConnectionHandlerContextWrapper>;

/// Request handler context - provides access to the channel for a specific connection.
///
/// `ConnectionHandlerContextWrapper` is the execution context passed to request processors.
/// It encapsulates the channel associated with the incoming request, allowing handlers to:
///
/// - **Send responses**: Via `write()` or `write_ref()`
/// - **Access connection metadata**: Remote address, connection state
/// - **Perform advanced operations**: Direct connection access if needed
///
/// ## Design Rationale
///
/// - **Thin wrapper**: Delegates most operations to the underlying `Channel`
/// - **Hash/Eq based on channel**: Contexts for the same channel are equal
/// - **Wrapped in ArcMut**: Shared across async tasks, enables interior mutability
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
    /// - Underlying connection for I/O
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

    /// Gets an immutable reference to the underlying connection.
    ///
    /// # Returns
    ///
    /// Immutable reference to the `Connection` for inspection
    ///
    /// # Use Case
    ///
    /// Checking connection health, reading connection ID, etc.
    pub fn connection_ref(&self) -> &Connection {
        self.channel.connection_ref()
    }

    /// Gets a mutable reference to the underlying connection.
    ///
    /// # Returns
    ///
    /// Mutable reference to the `Connection` for advanced I/O
    ///
    /// # Use Case
    ///
    /// Direct send/receive operations bypassing channel abstractions
    pub fn connection_mut(&mut self) -> &mut Connection {
        self.channel.connection_mut()
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
    /// async fn handle_request(ctx: &mut ConnectionHandlerContext, request: RemotingCommand) {
    ///     let response = RemotingCommand::create_response_command()
    ///         .set_opaque(request.opaque());
    ///     ctx.write(response).await;
    /// }
    /// ```
    pub async fn write_response(&mut self, cmd: RemotingCommand) {
        match self.channel.connection_mut().send_command(cmd).await {
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
    pub async fn write_response_ref(&mut self, cmd: &mut RemotingCommand) {
        match self.channel.connection_mut().send_command_ref(cmd).await {
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
    pub async fn write(&mut self, cmd: RemotingCommand) {
        self.write_response(cmd).await;
    }

    /// Legacy alias for `write_response_ref()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `write_response_ref()` for clearer semantics.
    #[deprecated(since = "0.6.0", note = "Use `write_response_ref()` instead")]
    pub async fn write_ref(&mut self, cmd: &mut RemotingCommand) {
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

    /// Gets a mutable reference to the channel.
    ///
    /// # Returns
    ///
    /// Mutable reference to the `Channel`
    ///
    /// # Use Case
    ///
    /// Advanced channel operations (modify addresses, access inner state)
    pub fn channel_mut(&mut self) -> &mut Channel {
        &mut self.channel
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

impl AsMut<ConnectionHandlerContextWrapper> for ConnectionHandlerContextWrapper {
    fn as_mut(&mut self) -> &mut ConnectionHandlerContextWrapper {
        self
    }
}
