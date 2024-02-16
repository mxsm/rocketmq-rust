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

use std::{error::Error, net::SocketAddr, sync::Arc};

use futures::executor::block_on;
use rocketmq_common::TokioExecutorService;
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle};

use crate::{
    protocol::remoting_command::RemotingCommand,
    remoting::{InvokeCallback, RemotingService},
    runtime::{processor::RequestProcessor, server::run, RPCHook, ServerInner},
    server::{config::BrokerServerConfig, RemotingServer},
};

pub struct RocketmqDefaultServer {
    pub(crate) broker_server_config: BrokerServerConfig,
    pub(crate) server_inner: ServerInner,
    pub future: Option<JoinHandle<()>>,
}

impl RocketmqDefaultServer {
    pub fn new(broker_server_config: BrokerServerConfig) -> Self {
        Self {
            broker_server_config,
            server_inner: ServerInner::new(),
            future: None,
        }
    }
}

impl RemotingService for RocketmqDefaultServer {
    fn start(&mut self) -> impl std::future::Future<Output = ()> + Send {
        let address = self.broker_server_config.bind_address.as_str();
        let port = self.broker_server_config.listen_port;
        let listener = block_on(async move {
            TcpListener::bind(&format!("{}:{}", address, port))
                .await
                .unwrap()
        });
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        let default_request_processor = self
            .server_inner
            .default_request_processor_pair
            .as_ref()
            .unwrap()
            .clone();
        let processor_table = self.server_inner.processor_table.as_ref().unwrap().clone();

        run(
            listener,
            tokio::signal::ctrl_c(),
            default_request_processor,
            processor_table,
            Some(notify_conn_disconnect),
        )
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
    ) {
        /*self.server_inner
        .processor_table
        .insert(request_code.into(), Pair::new(processor, executor));*/
    }

    fn register_default_processor(
        &mut self,
        processor: impl RequestProcessor + Send + Sync + 'static,
    ) {
        self.server_inner.default_request_processor_pair =
            Some(Arc::new(tokio::sync::RwLock::new(Box::new(processor))));
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
