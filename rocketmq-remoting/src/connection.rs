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

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal framed, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
pub struct Connection {
    /// The `Framed` instance used for reading from and writing to the TCP stream.
    /// It leverages the `RemotingCommandCodec` for encoding and decoding frames.
    //pub(crate) framed: Framed<TcpStream, RemotingCommandCodec>,
    writer: SplitSink<Framed<TcpStream, CompositeCodec>, Bytes>,
    reader: SplitStream<Framed<TcpStream, CompositeCodec>>,

    /// A boolean flag indicating the current state of the connection.
    /// `true` means the connection is in a good state, while `false` indicates
    /// there are issues with the connection.
    pub(crate) ok: bool,

    buf: BytesMut,

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
        let (writer, reader) = framed.split();
        Self {
            writer,
            reader,
            ok: true,
            buf: BytesMut::with_capacity(BUFFER_SIZE),
            connection_id: CheetahString::from_string(Uuid::new_v4().to_string()),
        }
    }

    #[inline]
    pub fn reader(&self) -> &SplitStream<Framed<TcpStream, CompositeCodec>> {
        &self.reader
    }

    #[inline]
    pub fn writer(&self) -> &SplitSink<Framed<TcpStream, CompositeCodec>, Bytes> {
        &self.writer
    }

    /// Receives a `RemotingCommand` from the connection.
    ///
    /// # Returns
    ///
    /// A result containing the received command or an error.
    pub async fn receive_command(
        &mut self,
    ) -> Option<rocketmq_error::RocketMQResult<RemotingCommand>> {
        self.reader.next().await
    }

    /// Sends a `RemotingCommand` over the connection.
    ///
    /// # Arguments
    ///
    /// * `command` - The `RemotingCommand` to send.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub async fn send_command(
        &mut self,
        mut command: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.buf.clear();
        command.fast_header_encode(&mut self.buf);
        if let Some(body_inner) = command.take_body() {
            self.buf.put(body_inner);
        }
        self.writer.send(self.buf.split().freeze()).await?;
        Ok(())
    }

    /// Sends a `RemotingCommand` over the connection.
    ///
    /// # Arguments
    ///
    /// * `command` - The `RemotingCommand` to send.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    pub async fn send_command_ref(
        &mut self,
        command: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.buf.clear();
        command.fast_header_encode(&mut self.buf);
        if let Some(body_inner) = command.take_body() {
            self.buf.put(body_inner);
        }
        self.writer.send(self.buf.split().freeze()).await?;
        Ok(())
    }

    /// Sends a `Bytes` object over the connection.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The `Bytes` object to send.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success (`Ok(())`) or failure (`Err(RemotingError)`).
    ///
    /// # Errors
    ///
    /// This function returns a `RemotingError` if the underlying writer fails to send the data.
    pub async fn send_bytes(&mut self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.writer.send(bytes).await?;
        Ok(())
    }

    /// Sends a static byte slice (`&'static [u8]`) over the connection.
    ///
    /// # Arguments
    ///
    /// * `slice` - A static byte slice to send.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success (`Ok(())`) or failure (`Err(RemotingError)`).
    ///
    /// # Errors
    ///
    /// This function returns a `RemotingError` if the underlying writer fails to send the data.
    ///
    /// # Notes
    ///
    /// The static lifetime of the slice ensures that the data is valid for the entire duration
    /// of the program, making it suitable for scenarios where the data does not need to be
    /// dynamically allocated or modified.
    pub async fn send_slice(&mut self, slice: &'static [u8]) -> rocketmq_error::RocketMQResult<()> {
        let bytes = slice.into();
        self.writer.send(bytes).await?;
        Ok(())
    }

    /// Returns a reference to the connection ID.
    #[inline]
    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    #[inline]
    pub fn connection_is_ok(&self) -> bool {
        self.ok
    }
}
