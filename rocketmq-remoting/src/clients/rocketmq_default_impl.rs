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
use std::{collections::HashMap, error::Error, sync::Arc};

use rocketmq_common::TokioExecutorService;
use tokio::sync::Mutex;

use crate::{
    clients::{Client, RemotingClient},
    protocol::remoting_command::RemotingCommand,
    remoting::{InvokeCallback, RemotingService},
    runtime::{
        config::client_config::TokioClientConfig, processor::RequestProcessor, RPCHook,
        ServiceBridge,
    },
};

pub struct RocketmqDefaultClient {
    service_bridge: ServiceBridge,
    tokio_client_config: TokioClientConfig,
    //cache connection
    connection_tables: HashMap<String /* ip:port */, Arc<Mutex<Client>>>,
    lock: std::sync::RwLock<()>,
}

impl RocketmqDefaultClient {
    pub fn new(tokio_client_config: TokioClientConfig) -> Self {
        Self {
            service_bridge: ServiceBridge::new(),
            tokio_client_config,
            connection_tables: Default::default(),
            lock: Default::default(),
        }
    }
}

impl RocketmqDefaultClient {
    fn get_and_create_client(&mut self, addr: String) -> Arc<Mutex<Client>> {
        let lc = self.lock.write().unwrap();

        if self.connection_tables.contains_key(&addr) {
            return self.connection_tables.get(&addr).cloned().unwrap();
        }

        let addr_inner = addr.clone();
        let client =
            futures::executor::block_on(async move { Client::connect(addr_inner).await.unwrap() });

        self.connection_tables
            .insert(addr.clone(), Arc::new(Mutex::new(client)));
        drop(lc);
        self.connection_tables.get(&addr).cloned().unwrap()
    }
}

#[allow(unused_variables)]
impl RemotingService for RocketmqDefaultClient {
    async fn start(&mut self) {
        todo!()
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

#[allow(unused_variables)]
impl RemotingClient for RocketmqDefaultClient {
    fn update_name_server_address_list(&mut self, addrs: Vec<String>) {
        todo!()
    }

    fn get_name_server_address_list(&self) -> Vec<String> {
        todo!()
    }

    fn get_available_name_srv_list(&self) -> Vec<String> {
        todo!()
    }

    fn invoke_sync(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, Box<dyn Error>> {
        let client = self.get_and_create_client(addr.clone());
        Ok(self
            .service_bridge
            .invoke_sync(client, request, timeout_millis)
            .unwrap())
    }

    async fn invoke_async(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: impl InvokeCallback,
    ) -> Result<(), Box<dyn Error>> {
        let client = self.get_and_create_client(addr.clone());
        self.service_bridge
            .invoke_async(client, request, timeout_millis, invoke_callback)
            .await;
        Ok(())
    }

    fn invoke_oneway(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn register_processor(
        &mut self,
        request_code: i32,
        processor: impl RequestProcessor + Send + Sync + 'static,
        executor: Arc<TokioExecutorService>,
    ) {
        todo!()
    }

    fn set_callback_executor(&mut self, executor: Arc<TokioExecutorService>) {
        todo!()
    }

    fn is_address_reachable(&mut self, addr: String) {
        todo!()
    }

    fn close_clients(&mut self, addrs: Vec<String>) {
        todo!()
    }
}
