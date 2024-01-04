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

 use std::{collections::HashMap, sync::Arc};

 use rocketmq_common::TokioExecutorService;
 use tracing::info;
 
 use crate::runtime::{
     processor::RequestProcessor,
     remoting_service::RemotingService,
     server::{remoting_server::RemotingServer, server_inner::ServerBootstrap},
     RPCHook,
 };
 
 pub struct TokioRemotingServer {
     boot_strap: ServerBootstrap,
     ip: String,
     port: u32,
     processor_table: HashMap<
         i32,
         (
             Box<dyn RequestProcessor + Send + 'static>,
             Arc<TokioExecutorService>,
         ),
     >,
     default_request_processor: Option<(
         Box<dyn RequestProcessor + Send + 'static>,
         Arc<TokioExecutorService>,
     )>,
 }
 impl TokioRemotingServer {
     pub fn new(
         port: u32,
         ip: impl Into<String>,
         remoting_executor: TokioExecutorService,
     ) -> TokioRemotingServer {
         let address = ip.into();
         let boot_strap = ServerBootstrap::new(address.clone(), port, remoting_executor);
         TokioRemotingServer {
             boot_strap,
             ip: address,
             port,
             processor_table: Default::default(),
             default_request_processor: None,
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
 
 impl RemotingServer for TokioRemotingServer {
     fn register_processor(
         &mut self,
         request_code: i32,
         processor: impl RequestProcessor + Send + 'static,
         executor: Arc<TokioExecutorService>,
     ) {
         self.processor_table
             .insert(request_code, (Box::new(processor), executor));
     }
 
     fn register_default_processor(
         &mut self,
         processor: impl RequestProcessor + Send + 'static,
         executor: Arc<TokioExecutorService>,
     ) {
         self.default_request_processor = Some((Box::new(processor), executor));
     }
 }
 