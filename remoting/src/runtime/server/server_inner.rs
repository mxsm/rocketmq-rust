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
 use rocketmq_common::{FuturesExecutorService, TokioExecutorService};
 use tokio::{
     net::{TcpListener, TcpStream},
     sync::mpsc,
 };
 use tokio_stream::StreamExt;
 use tokio_util::codec::Framed;
 use tracing::{info, warn};
 
 use crate::{
     codec::remoting_command_codec::RemotingCommandCodec,
     protocol::remoting_command::RemotingCommand, runtime::processor::RequestProcessor,
 };
 
 /// Shorthand for the transmit half of the message channel.
 type Tx = mpsc::UnboundedSender<RemotingCommand>;
 
 /// Shorthand for the receive half of the message channel.
 type Rx = mpsc::UnboundedReceiver<RemotingCommand>;
 
 struct Shared<RP> {
     peers: HashMap<SocketAddr, Tx>,
     processor_table: HashMap<i32, (RP, Arc<TokioExecutorService>)>,
 }
 
 struct Connection {
     rx: Rx,
     framed: Framed<TcpStream, RemotingCommandCodec>,
 }
 
 impl Connection {
     async fn new(
         state: Arc<
             tokio::sync::Mutex<Shared<Box<dyn RequestProcessor + std::marker::Send + 'static>>>,
         >,
         framed: Framed<TcpStream, RemotingCommandCodec>,
     ) -> anyhow::Result<Connection> {
         let client_addr = framed.get_ref().peer_addr()?;
         let (tx, rx) = mpsc::unbounded_channel();
         state.lock().await.peers.insert(client_addr, tx);
         Ok(Connection { rx, framed })
     }
 }
 
 pub(crate) struct ServerBootstrap {
     conn_handle_executor: FuturesExecutorService,
     state_conn:
         Arc<tokio::sync::Mutex<Shared<Box<dyn RequestProcessor + std::marker::Send + 'static>>>>,
     address: String,
     bind_port: u32,
 }
 
 impl ServerBootstrap {
     pub fn address(&self) -> &str {
         &self.address
     }
     pub fn bind_port(&self) -> u32 {
         self.bind_port
     }
 }
 
 impl ServerBootstrap {
     pub fn new(address: impl Into<String>, bind_port: u32) -> ServerBootstrap {
         Self {
             conn_handle_executor: rocketmq_common::FuturesExecutorServiceBuilder::new()
                 .create()
                 .unwrap(),
             state_conn: Arc::new(tokio::sync::Mutex::new(Shared {
                 peers: HashMap::new(),
                 processor_table: HashMap::new(),
             })),
             address: address.into(),
             bind_port,
         }
     }
 
     pub async fn start(&self) -> anyhow::Result<()> {
         let listener = TcpListener::bind(&format!("{}:{}", self.address, self.bind_port)).await?;
         loop {
             //wait for a connection
             let (tcp_stream, addr) = listener.accept().await?;
             let arc = self.state_conn.clone();
             self.conn_handle_executor.spawn(async move {
                 info!("name server accepted connection, client ip:{}", addr);
                 if let Err(e) = process_connection(arc, tcp_stream, addr).await {
                     tracing::error!("an error occurred; error = {:?}", e)
                 }
             });
         }
     }
 }
 
 async fn process_connection(
     state: Arc<tokio::sync::Mutex<Shared<Box<dyn RequestProcessor + std::marker::Send + 'static>>>>,
     stream: TcpStream,
     addr: SocketAddr,
 ) -> anyhow::Result<(), anyhow::Error> {
     let framed = Framed::new(stream, RemotingCommandCodec::new());
     tokio::spawn(async move { process_request(state, framed, addr) });
     Ok(())
 }
 
 async fn process_request(
     state: Arc<tokio::sync::Mutex<Shared<Box<dyn RequestProcessor + std::marker::Send + 'static>>>>,
     framed: Framed<TcpStream, RemotingCommandCodec>,
     addr: SocketAddr,
 ) -> anyhow::Result<(), anyhow::Error> {
     let mut conn = Connection::new(state.clone(), framed).await?;
     loop {
         tokio::select! {
             result = conn.framed.next() => match result {
                 // A message was received from the current user, we should
                 // broadcast this message to the other users.
                 Some(Ok(msg)) => {
                     let mut state_conn = state.lock().await;
                     let (processor, executor) = state_conn.processor_table.get_mut(&msg.code()).unwrap();
                     let  _ = processor.process_request(msg);
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
 