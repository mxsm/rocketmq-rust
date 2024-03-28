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

 use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

 use futures::SinkExt;
 use tokio::{
     net::{TcpListener, TcpStream},
     sync::{broadcast, mpsc, Semaphore},
     time,
 };
 use tokio_stream::StreamExt;
 use tokio_util::codec::Framed;
 use tracing::{error, info, warn};
 
 use crate::{
     codec::remoting_command_codec::RemotingCommandCodec,
     protocol::remoting_command::RemotingCommand,
     runtime::{server::ConnectionHandlerContext, ArcDefaultRequestProcessor, ArcProcessorTable},
 };
 
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
     pub(crate) framed: Framed<TcpStream, RemotingCommandCodec>,
     pub(crate) remote_addr: SocketAddr,
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
     pub fn new(tcp_stream: TcpStream, remote_addr: SocketAddr) -> Connection {
         Self {
             framed: Framed::new(tcp_stream, RemotingCommandCodec::new()),
             remote_addr,
         }
     }
 }
 
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
 
 /// Default limit the max number of connections.
 const DEFAULT_MAX_CONNECTIONS: usize = 1000;
 
 /// Shorthand for the transmit half of the message channel.
 type Tx = mpsc::UnboundedSender<RemotingCommand>;
 
 /// Shorthand for the receive half of the message channel.
 type Rx = mpsc::UnboundedReceiver<RemotingCommand>;
 

 /// Server listener state. Created in the `run` call. It includes a `run` method
 /// which performs the TCP listening and initialization of per-connection state.
 pub(crate) struct ConnectionListener {
    /// The TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,

    notify_shutdown: broadcast::Sender<()>,

    shutdown_complete_tx: mpsc::Sender<()>,

    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,

    default_request_processor: ArcDefaultRequestProcessor,

    processor_table: ArcProcessorTable,
}

 pub struct ConnectionHandler {
     default_request_processor: ArcDefaultRequestProcessor,
     processor_table: ArcProcessorTable,
     connection: Connection,
     shutdown: Shutdown,
     _shutdown_complete: mpsc::Sender<()>,
     conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
 }

 #[derive(Debug)]
 pub(crate) struct Shutdown {
     /// `true` if the shutdown signal has been received
     is_shutdown: bool,
 
     /// The receive half of the channel used to listen for shutdown.
     notify: broadcast::Receiver<()>,
 }

 
 impl Drop for ConnectionHandler {
     fn drop(&mut self) {
         if let Some(ref sender) = self.conn_disconnect_notify {
             let socket_addr = self.connection.remote_addr;
             warn!(
                 "connection[{}] disconnected, Send notify message.",
                 socket_addr
             );
             let _ = sender.send(socket_addr);
         }
     }
 }
 
 impl ConnectionHandler {
     async fn handle(&mut self) -> anyhow::Result<()> {
         let _remote_addr = self.connection.remote_addr;
         while !self.shutdown.is_shutdown {
             let frame = tokio::select! {
                 res = self.connection.framed.next() => res,
                 _ = self.shutdown.recv() =>{
                     //If a shutdown signal is received, return from `handle`.
                     return Ok(());
                 }
             };
 
             let cmd = match frame {
                 Some(frame) => match frame {
                     Ok(cmd) => cmd,
                     Err(err) => {
                         error!("read frame failed: {}", err);
                         return Ok(());
                     }
                 },
                 None => {
                     //If the frame is None, it means the connection is closed.
                     return Ok(());
                 }
             };
             let ctx = ConnectionHandlerContext::new(&self.connection);
             let opaque = cmd.opaque();
             let response = match self.processor_table.get(&cmd.code()) {
                 None => self.default_request_processor.process_request(ctx, cmd),
                 Some(pr) => pr.process_request(ctx, cmd),
             };
             tokio::select! {
                 result = self.connection.framed.send(response.set_opaque(opaque)) => match result{
                     Ok(_) =>{},
                     Err(err) => {
                         error!("send response failed: {}", err);
                         return Ok(())
                     },
                 },
             }
         }
         Ok(())
     }
 }
 

 
 impl ConnectionListener {
     async fn run(&mut self) -> anyhow::Result<()> {
         info!("Prepare accepting connection");
         loop {
             let permit = self
                 .limit_connections
                 .clone()
                 .acquire_owned()
                 .await
                 .unwrap();
 
             // Accept a new socket. This will attempt to perform error handling.
             // The `accept` method internally attempts to recover errors, so an
             // error here is non-recoverable.
             let (socket, remote_addr) = self.accept().await?;
             info!("Accepted connection, client ip:{}", remote_addr);
             //create per connection handler state
             let mut handler = ConnectionHandler {
                 default_request_processor: self.default_request_processor.clone(),
                 processor_table: self.processor_table.clone(),
                 connection: Connection::new(socket, remote_addr),
                 shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                 _shutdown_complete: self.shutdown_complete_tx.clone(),
                 conn_disconnect_notify: self.conn_disconnect_notify.clone(),
             };
 
             tokio::spawn(async move {
                 if let Err(err) = handler.handle().await {
                     error!(cause = ?err, "connection error");
                 }
                 warn!(
                     "The client[IP={}] disconnected from the server.",
                     remote_addr
                 );
                 /*  if let Some(ref sender) = handler.conn_disconnect_notify {
                     let _ = sender.send(remote_addr);
                 }*/
                 drop(permit);
                 drop(handler);
             });
         }
     }
 
     async fn accept(&mut self) -> anyhow::Result<(TcpStream, SocketAddr)> {
         let mut backoff = 1;
 
         // Try to accept a few times
         loop {
             // Perform the accept operation. If a socket is successfully
             // accepted, return it. Otherwise, save the error.
             match self.listener.accept().await {
                 Ok((socket, remote_addr)) => return Ok((socket, remote_addr)),
                 Err(err) => {
                     if backoff > 64 {
                         // Accept has failed too many times. Return the error.
                         return Err(err.into());
                     }
                 }
             }
 
             // Pause execution until the back off period elapses.
             time::sleep(Duration::from_secs(backoff)).await;
 
             // Double the back off
             backoff *= 2;
         }
     }
 }
 

 
 impl Shutdown {
     /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
     pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
         Shutdown {
             is_shutdown: false,
             notify,
         }
     }
 
     /// Returns `true` if the shutdown signal has been received.
     pub(crate) fn is_shutdown(&self) -> bool {
         self.is_shutdown
     }
 
     /// Receive the shutdown notice, waiting if necessary.
     pub(crate) async fn recv(&mut self) {
         // If the shutdown signal has already been received, then return
         // immediately.
         if self.is_shutdown {
             return;
         }
 
         // Cannot receive a "lag error" as only one value is ever sent.
         let _ = self.notify.recv().await;
 
         // Remember that the signal has been received.
         self.is_shutdown = true;
     }
 }
 