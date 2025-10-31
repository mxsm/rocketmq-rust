/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::hash::Hash;
use std::hash::Hasher;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::codec::remoting_command_codec::CompositeCodec;
use crate::protocol::remoting_command::RemotingCommand;

pub type ConnectionId = CheetahString;

/// Bidirectional TCP connection for RocketMQ protocol communication.
///
/// `Connection` handles low-level frame encoding/decoding and provides high-level
/// APIs for sending/receiving `RemotingCommand` messages. It maintains connection
/// state, manages I/O buffers, and tracks the connection lifecycle.
///
/// ## Lifecycle
///
/// 1. **Created**: New connection from `TcpStream`
/// 2. **Active**: Processing requests/responses
/// 3. **Degraded**: `ok = false` after I/O error
/// 4. **Closed**: Stream ends or explicit shutdown
///
/// ## Threading
///
/// - Safe for concurrent sends (internal buffering)
/// - Receives must be sequential (single reader)
pub struct Connection {
    // === I/O Transport ===
    /// Outbound message sink (sends encoded frames to peer)
    /// 
    /// Renamed from `writer` for clarity: handles outbound data flow
    outbound_sink: SplitSink<Framed<TcpStream, CompositeCodec>, Bytes>,
    
    /// Inbound message stream (receives decoded frames from peer)
    /// 
    /// Renamed from `reader` for clarity: handles inbound data flow
    inbound_stream: SplitStream<Framed<TcpStream, CompositeCodec>>,

    // === State Management ===
    /// Connection health status
    /// 
    /// - `true`: Connection is healthy and operational
    /// - `false`: Connection degraded due to I/O error, should be closed
    pub(crate) ok: bool,

    // === Buffers ===
    /// Reusable encoding buffer to avoid repeated allocations
    /// 
    /// Used for staging `RemotingCommand` serialization before sending.
    /// Cleared and reused for each send operation.
    encode_buffer: BytesMut,

    // === Identification ===
    /// Unique identifier for this connection instance
    /// 
    /// Generated via UUID, stable across the connection lifetime
    connection_id: ConnectionId,
}

impl Hash for Connection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.connection_id.hash(state);
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.connection_id == other.connection_id
    }
}

impl Eq for Connection {}

impl Connection {
    /// Creates a new `Connection` instance.
    ///
    /// # Arguments
    ///
    /// * `tcp_stream` - The `TcpStream` associated with the connection.
    ///
    /// # Returns
    ///
    /// A new `Connection` instance.
    pub fn new(tcp_stream: TcpStream) -> Connection {
        const CAPACITY: usize = 1024 * 1024; // 1 MB
        const BUFFER_SIZE: usize = 8 * 1024; // 8 KB
        let framed = Framed::with_capacity(tcp_stream, CompositeCodec::new(), CAPACITY);
        let (outbound_sink, inbound_stream) = framed.split();
        Self {
            outbound_sink,
            inbound_stream,
            ok: true,
            encode_buffer: BytesMut::with_capacity(BUFFER_SIZE),
            connection_id: CheetahString::from_string(Uuid::new_v4().to_string()),
        }
    }

    /// Gets a reference to the inbound stream for receiving messages
    /// 
    /// # Returns
    /// 
    /// Immutable reference to the inbound message stream
    #[inline]
    pub fn inbound_stream(&self) -> &SplitStream<Framed<TcpStream, CompositeCodec>> {
        &self.inbound_stream
    }

    /// Gets a reference to the outbound sink for sending messages
    /// 
    /// # Returns
    /// 
    /// Immutable reference to the outbound message sink
    #[inline]
    pub fn outbound_sink(&self) -> &SplitSink<Framed<TcpStream, CompositeCodec>, Bytes> {
        &self.outbound_sink
    }

    /// Receives the next `RemotingCommand` from the peer.
    ///
    /// Blocks until a complete frame is available or the stream ends.
    /// 
    /// # Returns
    ///
    /// - `Some(Ok(command))`: Successfully received and decoded a command
    /// - `Some(Err(e))`: Decoding error occurred
    /// - `None`: Stream ended (peer closed connection)
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// while let Some(result) = connection.receive_command().await {
    ///     match result {
    ///         Ok(cmd) => handle_command(cmd),
    ///         Err(e) => eprintln!("Decode error: {}", e),
    ///     }
    /// }
    /// // Connection closed
    /// ```
    pub async fn receive_command(
        &mut self,
    ) -> Option<rocketmq_error::RocketMQResult<RemotingCommand>> {
        self.inbound_stream.next().await
    }

    /// Sends a `RemotingCommand` to the peer (consumes command).
    ///
    /// Encodes the command into the internal buffer, then flushes to the network.
    /// This method takes ownership of the command for optimization.
    /// 
    /// # Arguments
    ///
    /// * `command` - The command to send (consumed)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Command successfully sent
    /// - `Err(e)`: Network I/O error occurred
    /// 
    /// # Lifecycle
    /// 
    /// 1. Encode command header + body into reusable buffer
    /// 2. Use zero-copy `split_to()` to extract buffer contents as `Bytes`
    /// 3. Send extracted bytes via outbound sink
    /// 4. Buffer is now empty and ready for next command (no clear() needed)
    /// 
    /// # Performance Optimization
    /// 
    /// - Uses `split_to(len)` instead of `split()` for better performance
    /// - `split_to()` returns all data and leaves buffer empty, eliminating need for clear()
    /// - `freeze()` converts BytesMut to Bytes with zero-copy (just refcount increment)
    pub async fn send_command(
        &mut self,
        mut command: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        // Encode command into buffer (buffer might have capacity from previous use)
        command.fast_header_encode(&mut self.encode_buffer);
        if let Some(body_inner) = command.take_body() {
            self.encode_buffer.put(body_inner);
        }
        
        // Zero-copy extraction: split_to(len) returns all data, leaves buffer empty
        // This is more efficient than split() + clear() pattern
        let len = self.encode_buffer.len();
        let bytes = self.encode_buffer.split_to(len).freeze();
        
        self.outbound_sink.send(bytes).await?;
        Ok(())
    }

    /// Sends a `RemotingCommand` to the peer (borrows command).
    ///
    /// Similar to `send_command`, but borrows the command mutably instead of
    /// consuming it. Use when the caller needs to retain ownership.
    /// 
    /// # Arguments
    ///
    /// * `command` - Mutable reference to the command to send
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Command successfully sent
    /// - `Err(e)`: Network I/O error occurred
    /// 
    /// # Note
    /// 
    /// This method may consume the command's body (`take_body()`), modifying
    /// the original command.
    pub async fn send_command_ref(
        &mut self,
        command: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        // Encode command into buffer
        command.fast_header_encode(&mut self.encode_buffer);
        if let Some(body_inner) = command.take_body() {
            self.encode_buffer.put(body_inner);
        }
        
        // Zero-copy extraction using split_to() pattern
        let len = self.encode_buffer.len();
        let bytes = self.encode_buffer.split_to(len).freeze();
        
        self.outbound_sink.send(bytes).await?;
        Ok(())
    }

    /// Sends raw `Bytes` directly to the peer (zero-copy).
    ///
    /// Bypasses command encoding and sends pre-serialized bytes directly.
    /// Use for forwarding or when bytes are already encoded.
    /// 
    /// # Arguments
    ///
    /// * `bytes` - The bytes to send (reference-counted, zero-copy)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Bytes successfully sent
    /// - `Err(e)`: Network I/O error occurred
    /// 
    /// # Performance
    /// 
    /// This is the most efficient send method as it avoids intermediate buffering
    /// and serialization overhead.
    pub async fn send_bytes(&mut self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.outbound_sink.send(bytes).await?;
        Ok(())
    }

    /// Sends a static byte slice to the peer (zero-copy).
    ///
    /// Converts a `&'static [u8]` to `Bytes` and sends. Use for compile-time
    /// known data (e.g., protocol constants).
    /// 
    /// # Arguments
    ///
    /// * `slice` - Static byte slice with `'static` lifetime
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Slice successfully sent
    /// - `Err(e)`: Network I/O error occurred
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// const PING: &[u8] = b"PING\r\n";
    /// connection.send_slice(PING).await?;
    /// ```
    pub async fn send_slice(&mut self, slice: &'static [u8]) -> rocketmq_error::RocketMQResult<()> {
        let bytes = slice.into();
        self.outbound_sink.send(bytes).await?;
        Ok(())
    }

    /// Gets the unique identifier for this connection.
    /// 
    /// # Returns
    /// 
    /// Reference to the connection ID (UUID-based string)
    #[inline]
    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    /// Checks if the connection is in a healthy state.
    /// 
    /// # Returns
    /// 
    /// - `true`: Connection is operational
    /// - `false`: Connection has encountered an error and should be closed
    /// 
    /// # Note
    /// 
    /// This flag is set to `false` when an I/O error occurs. The connection
    /// should be discarded and a new one established.
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.ok
    }
    
    /// Legacy alias for `is_healthy()` - kept for backward compatibility.
    /// 
    /// # Deprecated
    /// 
    /// Use `is_healthy()` instead for clearer semantics.
    #[inline]
    #[deprecated(since = "0.1.0", note = "Use `is_healthy()` instead")]
    pub fn connection_is_ok(&self) -> bool {
        self.ok
    }
}
