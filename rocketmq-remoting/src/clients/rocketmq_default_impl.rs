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
use std::{collections::HashMap, sync::Arc, time::Duration};

use rocketmq_common::TokioExecutorService;
use tokio::{runtime::Handle, time};
use tracing::info;

use crate::{
    clients::{Client, RemotingClient},
    error::RemotingError,
    protocol::remoting_command::RemotingCommand,
    remoting::RemotingService,
    runtime::{config::client_config::TokioClientConfig, processor::RequestProcessor, RPCHook},
};

#[derive(Clone)]
pub struct RocketmqDefaultClient {
    tokio_client_config: Arc<TokioClientConfig>,
    //cache connection
    connection_tables:
        Arc<parking_lot::Mutex<HashMap<String /* ip:port */, Arc<tokio::sync::Mutex<Client>>>>>,
    namesrv_addr_list: Arc<parking_lot::Mutex<Vec<String>>>,
    namesrv_addr_choosed: Arc<parking_lot::Mutex<Option<String>>>,
}

impl RocketmqDefaultClient {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>) -> Self {
        Self {
            tokio_client_config,
            connection_tables: Arc::new(parking_lot::Mutex::new(Default::default())),
            namesrv_addr_list: Arc::new(Default::default()),
            namesrv_addr_choosed: Arc::new(Default::default()),
        }
    }
}

impl RocketmqDefaultClient {
    fn get_and_create_client(&self, addr: String) -> Arc<tokio::sync::Mutex<Client>> {
        let mut mutex_guard = self.connection_tables.lock();
        if mutex_guard.contains_key(&addr) {
            return mutex_guard.get(&addr).unwrap().clone();
        }

        let addr_inner = addr.clone();
        let handle = Handle::current();
        let client = std::thread::spawn(move || {
            handle.block_on(async move { Client::connect(addr_inner).await.unwrap() })
        })
        .join()
        .unwrap();
        // let client = Client::connect(addr_inner).await.unwrap();
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
    fn update_name_server_address_list(&self, addrs: Vec<String>) {
        let mut old = self.namesrv_addr_list.lock();
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
                if let Some(namesrv_addr) = self.namesrv_addr_choosed.lock().as_ref() {
                    if !addrs.contains(namesrv_addr) {
                        let mut remove_vec = Vec::new();
                        let mut result = self.connection_tables.lock();
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
        /*let cloned = self.namesrv_addr_list.clone();
        Handle::current().block_on(async move { cloned.lock().await.clone() })*/
        self.namesrv_addr_list.lock().clone()
    }

    fn get_available_name_srv_list(&self) -> Vec<String> {
        vec!["127.0.0.1:9876".to_string()]
    }

    fn invoke_sync(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RemotingCommand {
        let client = self.get_and_create_client(addr.clone());
        let handle = Handle::current();
        std::thread::spawn(move || {
            handle.block_on(async move { client.lock().await.send_read(request).await.unwrap() })
        })
        .join()
        .unwrap()

        /*if let Ok(result) = timeout(Duration::from_millis(timeout_millis), async {
            client.lock().await.send_read(request).await.unwrap()
        }) {
            result
        } else {
            RemotingCommand::create_response_command()
        }*/
    }

    async fn invoke_async(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, RemotingError> {
        let client = self.get_and_create_client(addr.clone());
        match time::timeout(Duration::from_millis(timeout_millis), async move {
            client.lock().await.send_read(request).await
        })
        .await
        {
            Ok(result) => match result {
                Ok(response) => Ok(response),
                Err(err) => Err(RemotingError::RemoteException(err.to_string())),
            },
            Err(err) => Err(RemotingError::RemoteException(err.to_string())),
        }
    }

    async fn invoke_oneway(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<(), RemotingError> {
        let client = self.get_and_create_client(addr.clone());
        /*        let _ = time::timeout(Duration::from_millis(timeout_millis), async move {
            client.lock().await.send(request).await.unwrap()
        })
        .await;*/

        tokio::spawn(async move {
            match time::timeout(Duration::from_millis(timeout_millis), async move {
                client.lock().await.send(request).await.unwrap()
            })
            .await
            {
                Ok(_) => Ok(()),
                Err(err) => Err(RemotingError::RemoteException(err.to_string())),
            }
        });
        Ok(())
    }

    fn register_processor(
        &mut self,
        request_code: i32,
        processor: impl RequestProcessor + Sync + 'static,
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
