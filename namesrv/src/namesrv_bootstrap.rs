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

 use std::{collections::HashMap, env, net::SocketAddr, sync::Arc, time::Duration};

 use config::Config;
 use futures::SinkExt;
 use rocketmq_common::{common::namesrv::namesrv_config::NamesrvConfig, TokioExecutorService};
 use rocketmq_remoting::{
     codec::remoting_command_codec::RemotingCommandCodec,
     protocol::remoting_command::RemotingCommand,
     runtime::{
         remoting_service::RemotingService,
         server::{remoting_server::RemotingServer, tokio_remoting_server::TokioRemotingServer},
     },
 };
 use tokio::{
     net::{TcpListener, TcpStream},
     sync::{mpsc, Mutex},
 };
 use tokio_stream::StreamExt;
 use tokio_util::codec::Framed;
 use tracing::{info, warn};
 
 use crate::processor::{
     broker_request_processor::BrokerRequestProcessor,
     default_request_processor::DefaultRequestProcessor,
 };
 
 pub async fn boot() -> anyhow::Result<()> {
     info!("Starting rocketmq name server (Rust)");
     //let _ = parse_command_and_config_file();
     let mut server = TokioRemotingServer::new(9876, "127.0.0.1");
     let config = NamesrvConfig::new();
     let services = ExecutorServices::new(&config);
     server.register_default_processor(
         DefaultRequestProcessor::new(),
         services.default_executor.clone(),
     );
     server.start();
     /*    let state = Arc::new(Mutex::new(Shared::new()));
 
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
     }*/
     Ok(())
 }
 
 fn parse_command_and_config_file() -> anyhow::Result<(), anyhow::Error> {
     let namesrv_config = Config::builder()
         .add_source(config::File::with_name(
             format!(
                 "namesrv{}resource{}namesrv.toml",
                 std::path::MAIN_SEPARATOR,
                 std::path::MAIN_SEPARATOR
             )
             .as_str(),
         ))
         .build()
         .unwrap();
     let result = namesrv_config.try_deserialize::<NamesrvConfig>().unwrap();
     info!("namesrv config: {:?}", result);
     Ok(())
 }
 
 struct ExecutorServices {
     default_executor: Arc<TokioExecutorService>,
     client_request_executor: Arc<TokioExecutorService>,
 }
 
 impl ExecutorServices {
     pub fn new(namesrv_config: &NamesrvConfig) -> Self {
         Self {
             default_executor: Arc::new(TokioExecutorService::new_with_config(
                 namesrv_config.default_thread_pool_nums as usize,
                 "RemotingExecutorThread_",
                 Duration::from_secs(60),
                 namesrv_config.default_thread_pool_queue_capacity as usize,
             )),
             client_request_executor: Arc::new(TokioExecutorService::new_with_config(
                 namesrv_config.client_request_thread_pool_nums as usize,
                 "ClientRequestExecutorThread_",
                 Duration::from_secs(60),
                 namesrv_config.client_request_thread_pool_queue_capacity as usize,
             )),
         }
     }
 }
 
 //----------------------------------------------------------
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
             let opaque = cmd.opaque();
             let result = self.broker_request_processor.process_request(cmd);
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
         state: Arc<Mutex<Shared>>,
         framed: Framed<TcpStream, RemotingCommandCodec>,
     ) -> anyhow::Result<Connection> {
         let client_addr = framed.get_ref().peer_addr()?;
         let (tx, rx) = mpsc::unbounded_channel();
         state.lock().await.peers.insert(client_addr, tx);
         Ok(Connection { rx, framed })
     }
 }
 