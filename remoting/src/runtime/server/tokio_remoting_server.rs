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

 use tracing::info;

 use crate::runtime::{
     remoting_service::RemotingService,
     server::{remoting_server::RemotingServer, server_inner::ServerBootstrap},
     RPCHook,
 };
 
 pub struct TokioRemotingServer {
     boot_strap: ServerBootstrap,
     ip: String,
     port: u32,
 }
 impl TokioRemotingServer {
     pub fn new(port: u32, ip: impl Into<String>) -> TokioRemotingServer {
         let address = ip.into();
         let boot_strap = ServerBootstrap::new(address.clone(), port);
         TokioRemotingServer {
             boot_strap,
             ip: address,
             port,
         }
     }
 }
 
 impl RemotingService for TokioRemotingServer {
     fn start(&self) {
         info!("Start name server(Rust) at {}:{}", self.ip, self.port);
         let _ = futures::executor::block_on(self.boot_strap.start());
     }
 
     fn shutdown(&self) {
         todo!()
     }
 
     fn register_rpc_hook(&self, rpc_hook: Box<dyn RPCHook>) {
         todo!()
     }
 
     fn clear_rpc_hook(&self) {
         todo!()
     }
 }
 
 impl RemotingServer for TokioRemotingServer {}
 