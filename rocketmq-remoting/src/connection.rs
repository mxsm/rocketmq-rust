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
use bytes::BytesMut;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::codec::remoting_command_codec::CompositeCodec;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting_error::RemotingError;

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
    writer: SplitSink<Framed<TcpStream, CompositeCodec>, BytesMut>,
    reader: SplitStream<Framed<TcpStream, CompositeCodec>>,

    /// A boolean flag indicating the current state of the connection.
    /// `true` means the connection is in a good state, while `false` indicates
    /// there are issues with the connection.
    pub(crate) ok: bool,

    buf: BytesMut,
}

impl Hash for Connection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the boolean flag
        self.ok.hash(state);

        // Use the addr: *const _ess of writer and reader to hash them (they serve as a unique
        // identifier for these components)
        let writer_addr: *const SplitSink<Framed<TcpStream, CompositeCodec>, BytesMut> =
            &self.writer as *const SplitSink<Framed<TcpStream, CompositeCodec>, BytesMut>;
        let reader_addr: *const SplitStream<Framed<TcpStream, CompositeCodec>> =
            &self.reader as *const SplitStream<Framed<TcpStream, CompositeCodec>>;

        writer_addr.hash(state);
        reader_addr.hash(state);
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        // Compare the boolean flag
        self.ok == other.ok

        // Compare the addr: *const _ess of writer and reader
            && (std::ptr::eq(&self.writer, &other.writer))
            && (std::ptr::eq(&self.reader, &other.reader))
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
        let framed = Framed::with_capacity(tcp_stream, CompositeCodec::new(), 1024 * 4);
        let (writer, reader) = framed.split();
        Self {
            writer,
            reader,
            ok: true,
            buf: BytesMut::with_capacity(4096),
        }
    }

    #[inline]
    pub fn reader(&self) -> &SplitStream<Framed<TcpStream, CompositeCodec>> {
        &self.reader
    }

    #[inline]
    pub fn writer(&self) -> &SplitSink<Framed<TcpStream, CompositeCodec>, BytesMut> {
        &self.writer
    }

    /// Receives a `RemotingCommand` from the connection.
    ///
    /// # Returns
    ///
    /// A result containing the received command or an error.
    pub async fn receive_command(&mut self) -> Option<Result<RemotingCommand, RemotingError>> {
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
    ) -> Result<(), RemotingError> {
        self.buf.clear();
        command.fast_header_encode(&mut self.buf);
        if let Some(body_inner) = command.take_body() {
            self.buf.put(body_inner);
        }
        self.writer.send(self.buf.clone()).await?;
        Ok(())
    }
}
