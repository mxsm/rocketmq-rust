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

 use std::sync::Arc;

 use tokio::{net::TcpListener, sync::{broadcast, mpsc}};
 
 use crate::{net::connection::ConnectionListener, server::config::ServerConfig};
 
 pub struct Server {
     config: Arc<ServerConfig>,
 }
 
 impl Server {
     pub fn new(config: Arc<ServerConfig>) -> Self {
         Server { config }
     }
 
     pub fn config(&self) -> Arc<ServerConfig> {
         self.config.clone()
     }
 
     pub fn config_ref(&self) -> &ServerConfig {
         &self.config
     }
 }
 
 impl Server {
     pub async fn start(&self) {
         let listener = TcpListener::bind(&format!(
             "{}:{}",
             self.config.bind_address, self.config.listen_port
         ))
         .await?;
         let (notify_shutdown, _) = broadcast::channel(1);
         let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
         // Initialize the connection listener state
         let mut listener = ConnectionListener {
             listener,
             notify_shutdown,
             default_request_processor,
             shutdown_complete_tx,
             conn_disconnect_notify,
             processor_table,
             limit_connections: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
         };
 
         tokio::select! {
             res = listener.run() => {
                 // If an error is received here, accepting connections from the TCP
                 // listener failed multiple times and the server is giving up and
                 // shutting down.
                 //
                 // Errors encountered when handling individual connections do not
                 // bubble up to this point.
                 if let Err(err) = res {
                     error!(cause = %err, "failed to accept");
                 }
             }
             _ = shutdown => {
                 info!("shutdown now");
             }
         }
 
         let ConnectionListener {
             shutdown_complete_tx,
             notify_shutdown,
             ..
         } = listener;
         drop(notify_shutdown);
         drop(shutdown_complete_tx);
 
         let _ = shutdown_complete_rx.recv().await;
     }
 }
 