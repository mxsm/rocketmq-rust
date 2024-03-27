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
use tracing::info;

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
    connection_tables:
        tokio::sync::Mutex<HashMap<String /* ip:port */, Arc<tokio::sync::Mutex<Client>>>>,
    namesrv_addr_list: Arc<tokio::sync::Mutex<Vec<String>>>,
    namesrv_addr_choosed: Arc<tokio::sync::Mutex<Option<String>>>,
}

impl RocketmqDefaultClient {
    pub fn new(tokio_client_config: TokioClientConfig) -> Self {
        Self {
            service_bridge: ServiceBridge::new(),
            tokio_client_config,
            connection_tables: Default::default(),
            namesrv_addr_list: Arc::new(Default::default()),
            namesrv_addr_choosed: Arc::new(Default::default()),
        }
    }
}

impl RocketmqDefaultClient {
    async fn get_and_create_client(&mut self, addr: String) -> Arc<tokio::sync::Mutex<Client>> {
        let mut mutex_guard = self.connection_tables.lock().await;
        if mutex_guard.contains_key(&addr) {
            return mutex_guard.get(&addr).unwrap().clone();
        }

        let addr_inner = addr.clone();
        let client = Client::connect(addr_inner).await.unwrap();
        mutex_guard.insert(addr.clone(), Arc::new(tokio::sync::Mutex::new(client)));
        mutex_guard.get(&addr).unwrap().clone()
    }
}

#[allow(unused_variables)]
impl RemotingService for RocketmqDefaultClient {
    async fn start(&self) {
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
    async fn update_name_server_address_list(&mut self, addrs: Vec<String>) {
        let mut old = self.namesrv_addr_list.lock().await;
        let mut update = false;

        if !addrs.is_empty() {
            if old.is_empty() || addrs.len() != old.len() {
                update = true;
            } else {
                for addr in &addrs {
                    if !old.contains(addr) {
                        update = true;
                        break;
                    }
                }
            }

            if update {
                // Shuffle the addresses
                // Shuffle logic is not implemented here as it is not available in standard library
                // You can implement it using various algorithms like Fisher-Yates shuffle

                info!(
                    "name server address updated. NEW : {:?} , OLD: {:?}",
                    addrs, old
                );
                old.clone_from(&addrs);

                // should close the channel if choosed addr is not exist.
                if let Some(namesrv_addr) = self.namesrv_addr_choosed.lock().await.as_ref() {
                    if !addrs.contains(namesrv_addr) {
                        let mut remove_vec = Vec::new();
                        let mut result = self.connection_tables.lock().await;
                        for (addr, client) in result.iter() {
                            if addr.contains(namesrv_addr) {
                                remove_vec.push(addr.clone());
                            }
                        }
                        for addr in &remove_vec {
                            result.remove(addr);
                        }
                    }
                }
            }
        }
    }

    fn get_name_server_address_list(&self) -> Vec<String> {
        todo!()
    }

    fn get_available_name_srv_list(&self) -> Vec<String> {
        vec!["127.0.0.1:9876".to_string()]
    }

    async fn invoke_sync(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RemotingCommand {
        let client = self.get_and_create_client(addr.clone()).await;
        let client_ref = &mut *client.lock().await;
        ServiceBridge::invoke_sync(client_ref, request, timeout_millis)
            .await
            .unwrap()
    }

    async fn invoke_async(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: impl InvokeCallback,
    ) -> Result<(), Box<dyn Error>> {
        let client = self.get_and_create_client(addr.clone()).await;
        let client_ref = &mut *client.lock().await;
        ServiceBridge::invoke_async(client_ref, request, timeout_millis, invoke_callback).await;
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
