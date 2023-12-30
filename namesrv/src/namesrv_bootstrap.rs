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

use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};

use futures::SinkExt;
use rocketmq_remoting::{
    codec::remoting_command_codec::RemotingCommandCodec,
    protocol::remoting_command::RemotingCommand,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::processor::broker_request_processor::BrokerRequestProcessor;

pub async fn boot() -> anyhow::Result<()> {
    info!("Starting rocketmq name server (Rust)");

    let state = Arc::new(Mutex::new(Shared::new()));

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let namesrv_listener = TcpListener::bind(&addr).await?;
    info!("Rocketmq name server(Rust) running on {}", addr);
    loop {
        //wait for a connection
        let (tcp_stream, addr) = namesrv_listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            info!("name server accepted connection, client ip:{}", addr);
            if let Err(e) = process_connection(state, tcp_stream, addr).await {
                tracing::error!("an error occurred; error = {:?}", e)
            }
        });
    }
}

async fn process_connection(
    state: Arc<Mutex<Shared>>,
    stream: TcpStream,
    addr: SocketAddr,
) -> anyhow::Result<(), anyhow::Error> {
    let framed = Framed::new(stream, RemotingCommandCodec::new());
    let mut conn = Connection::new(state.clone(), framed).await?;

    loop {
        tokio::select! {
            result = conn.framed.next() => match result {
                // A message was received from the current user, we should
                // broadcast this message to the other users.
                Some(Ok(msg)) => {
                    let mut state1 = state.lock().await;
                    state1.process(addr, msg).await;
                }
                // An error occurred.
                Some(Err(e)) => {
                    tracing::error!(
                        "an error occurred while processing messages for error = {:?}",e);
                }
                // The stream has been exhausted.
                None => break,
            },
            // A message was received from a peer. Send it to the current user.
            Some(msg) = conn.rx.recv() => {
                conn.framed.send(msg).await?;
            }
        }
    }

    {
        //remove disconnected client connection
        let mut state = state.lock().await;
        state.peers.remove(&addr);
        warn!("remove client [IP={}] connection name server.", addr);
    }
    Ok(())
}

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<RemotingCommand>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<RemotingCommand>;

struct Shared {
    peers: HashMap<SocketAddr, Tx>,
    broker_request_processor: BrokerRequestProcessor,
}

impl Shared {
    fn new() -> Self {
        Shared {
            peers: HashMap::new(),
            broker_request_processor: BrokerRequestProcessor::new(),
        }
    }

    async fn process(&mut self, client_addr: SocketAddr, cmd: RemotingCommand) {
        if let Some(tx) = self.peers.get_mut(&client_addr) {
            let result = self.broker_request_processor.process_request(cmd);
            let _ = tx.send(result);
        }
    }
}

struct Connection {
    rx: Rx,
    framed: Framed<TcpStream, RemotingCommandCodec>,
}

impl Connection {
    async fn new(
        state: Arc<Mutex<Shared>>,
        framed: Framed<TcpStream, RemotingCommandCodec>,
    ) -> anyhow::Result<Connection> {
        let client_addr = framed.get_ref().peer_addr()?;
        let (tx, rx) = mpsc::unbounded_channel();
        state.lock().await.peers.insert(client_addr, tx);
        Ok(Connection { rx, framed })
    }
}
