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

use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::codec::remoting_command_codec::RemotingCommandCodec;

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
    pub(crate) framed: Framed<TcpStream, RemotingCommandCodec>,

    /// A boolean flag indicating the current state of the connection.
    /// `true` means the connection is in a good state, while `false` indicates
    /// there are issues with the connection.
    pub(crate) ok: bool,
}

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
        Self {
            framed: Framed::with_capacity(tcp_stream, RemotingCommandCodec::new(), 1024 * 4),
            ok: true,
        }
    }
}

impl Connection {
    pub fn framed(&self) -> &Framed<TcpStream, RemotingCommandCodec> {
        &self.framed
    }
}
