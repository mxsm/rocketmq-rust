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

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use futures::SinkExt;
use rocketmq_common::TokioExecutorService;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::{
    codec::remoting_command_codec::RemotingCommandCodec,
    protocol::remoting_command::RemotingCommand,
    runtime::{
        processor::RequestProcessor, remoting_service::RemotingService,
        server::remoting_server::RemotingServer, RPCHook,
    },
};

type ProcessorTable = HashMap<i32, Arc<Box<dyn RequestProcessor + Send + 'static>>>;
type DefaultProcessor = Box<dyn RequestProcessor + Send + Sync + 'static>;

pub struct TokioRemotingServer {
    ip: String,
    port: u32,
    processor_table: ProcessorTable,
    default_request_processor: Arc<tokio::sync::RwLock<DefaultProcessor>>,
    remoting_executor: TokioExecutorService,
}
impl TokioRemotingServer {
    pub fn new(
        port: u32,
        ip: impl Into<String>,
        remoting_executor: TokioExecutorService,
        default_request_processor: DefaultProcessor,
    ) -> TokioRemotingServer {
        let address = ip.into();
        TokioRemotingServer {
            ip: address,
            port,
            processor_table: Default::default(),
            default_request_processor: Arc::new(tokio::sync::RwLock::new(
                default_request_processor,
            )),
            remoting_executor,
        }
    }

    async fn boot(&self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.ip, self.port);
        let namesrv_listener = TcpListener::bind(&addr).await?;
        let state = Arc::new(tokio::sync::RwLock::new(Shared::new(
            self.default_request_processor.clone(),
        )));
        loop {
            let state = Arc::clone(&state);
            //wait for a connection
            let (tcp_stream, addr) = namesrv_listener.accept().await?;
            self.remoting_executor.spawn(async move {
                info!("name server accepted connection, client ip:{}", addr);
                if let Err(e) = process_connection(state, tcp_stream, addr).await {
                    tracing::error!("an error occurred; error = {:?}", e)
                }
            });
        }
    }
}

impl RemotingService for TokioRemotingServer {
    fn start(&self) {
        /*let mut local_pool = futures::executor::LocalPool::new();
        let t = local_pool.spawner().spawn_local(self.boot());*/
        let _ = futures::executor::block_on(self.boot());
    }

    fn shutdown(&self) {
        todo!()
    }

    fn register_rpc_hook(&self, _rpc_hook: Box<dyn RPCHook>) {
        todo!()
    }

    fn clear_rpc_hook(&self) {
        todo!()
    }
}

impl RemotingServer for TokioRemotingServer {
    fn register_processor(
        &mut self,
        request_code: i32,
        processor: impl RequestProcessor + Send + Sync + 'static,
        _executor: Arc<TokioExecutorService>,
    ) {
        self.processor_table
            .insert(request_code, Arc::new(Box::new(processor)));
    }

    fn register_default_processor(
        &mut self,
        processor: impl RequestProcessor + Send + Sync + 'static,
        _executor: Arc<TokioExecutorService>,
    ) {
        self.default_request_processor = Arc::new(tokio::sync::RwLock::new(Box::new(processor)));
    }
}

async fn process_connection(
    state: Arc<tokio::sync::RwLock<Shared>>,
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
                    //let mut state1 = state.lock().await;
                    info!("{}",serde_json::to_string(&msg).unwrap());
                    let mut state = state.write().await;
                    state.process(addr, msg).await;
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
        let mut state = state.write().await;
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
    //broker_request_processor: Box<dyn RequestProcessor + Send + 'static>,
    default_request_processor: Arc<tokio::sync::RwLock<DefaultProcessor>>,
}

impl Shared {
    fn new(default_request_processor: Arc<tokio::sync::RwLock<DefaultProcessor>>) -> Self {
        Shared {
            peers: HashMap::new(),
            default_request_processor,
        }
    }

    async fn process(&mut self, client_addr: SocketAddr, cmd: RemotingCommand) {
        if let Some(tx) = self.peers.get_mut(&client_addr) {
            let opaque = cmd.opaque();
            let result = self
                .default_request_processor
                .write()
                .await
                .process_request(cmd);
            //Broker handling compatible with the Java platform.
            let _ = tx.send(result.set_opaque(opaque));
        }
    }
}

struct Connection {
    rx: Rx,
    framed: Framed<TcpStream, RemotingCommandCodec>,
}

impl Connection {
    async fn new(
        state: Arc<tokio::sync::RwLock<Shared>>,
        framed: Framed<TcpStream, RemotingCommandCodec>,
    ) -> anyhow::Result<Connection> {
        let client_addr = framed.get_ref().peer_addr()?;
        let (tx, rx) = mpsc::unbounded_channel();
        state.write().await.peers.insert(client_addr, tx);
        Ok(Connection { rx, framed })
    }
}
