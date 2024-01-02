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

use std::{collections::HashMap, net::SocketAddr};

use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::{
    code::request_code::RequestCode, codec::remoting_command_codec::RemotingCommandCodec,
    protocol::remoting_command::RemotingCommand,
};

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<RemotingCommand>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<RemotingCommand>;

struct Shared<RP> {
    peers: HashMap<SocketAddr, Tx>,
    processor_table: HashMap<RequestCode, (RP, rocketmq_common::ThreadPool)>,
}

struct Connection {
    rx: Rx,
    framed: Framed<TcpStream, RemotingCommandCodec>,
}

struct ServerBootstrap {}
