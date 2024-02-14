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
#![allow(unused_variables)]

use std::net::SocketAddr;
use std::{error::Error, sync::Arc};

use tokio::net::TcpListener;
use tokio::sync::broadcast;

use rocketmq_common::TokioExecutorService;

use crate::runtime::ServerInner;
use crate::{
    protocol::remoting_command::RemotingCommand,
    remoting::{InvokeCallback, RemotingService},
    runtime::{processor::RequestProcessor, RPCHook},
    server::{config::BrokerServerConfig, RemotingServer},
};

pub struct RocketmqDefaultServer {
    pub(crate) broker_server_config: BrokerServerConfig,
    pub(crate) server_inner: ServerInner,
}

impl RocketmqDefaultServer {
    pub fn new(broker_server_config: BrokerServerConfig) -> Self {
        Self {
            broker_server_config,
            server_inner: ServerInner::new(),
        }
    }
}

impl RemotingService for RocketmqDefaultServer {
    async fn start(&mut self) {
        let listener = TcpListener::bind(&format!(
            "{}:{}",
            self.broker_server_config.bind_address.as_str(),
            self.broker_server_config.listen_port
        ))
        .await
        .unwrap();
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        /*run(
            listener,
            tokio::signal::ctrl_c(),
            self.server_inner
                .default_request_processor_pair
                .as_ref()
                .unwrap()
                .clone(),
            self.server_inner.processor_table.as_ref().unwrap().clone(),
            Some(notify_conn_disconnect),
        )
        .await;*/
    }

    fn shutdown(&mut self) {
        todo!()
    }

    fn register_rpc_hook(&mut self, hook: impl RPCHook) {
        todo!()
    }

    fn clear_rpc_hook(&mut self) {
        todo!()
    }
}

impl RemotingServer for RocketmqDefaultServer {
    fn register_processor(
        &mut self,
        request_code: impl Into<i32>,
        processor: Arc<dyn RequestProcessor + Send + Sync + 'static>,
        executor: Arc<TokioExecutorService>,
    ) {
        /*self.server_inner
        .processor_table
        .insert(request_code.into(), Pair::new(processor, executor));*/
    }

    fn register_default_processor(
        &mut self,
        processor: impl RequestProcessor + Send + Sync + 'static,
        executor: TokioExecutorService,
    ) {
        /*self.server_inner.default_request_processor_pair =
        Some(Pair::new(Box::new(processor), executor));*/
    }

    fn local_listen_port(&mut self) -> i32 {
        todo!()
    }

    fn get_processor_pair(
        &mut self,
        request_code: i32,
    ) -> (Arc<dyn RequestProcessor>, Arc<TokioExecutorService>) {
        todo!()
    }

    fn get_default_processor_pair(
        &mut self,
    ) -> (Arc<dyn RequestProcessor>, Arc<TokioExecutorService>) {
        todo!()
    }

    fn remove_remoting_server(&mut self, port: i32) {
        todo!()
    }

    fn invoke_sync(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, Box<dyn Error>> {
        todo!()
    }

    fn invoke_async(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: Box<dyn InvokeCallback>,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn invoke_oneway(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}
