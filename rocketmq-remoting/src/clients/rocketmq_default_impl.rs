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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::clients::Client;
use crate::clients::RemotingClient;
use crate::error::RemotingError;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::RemotingService;
use crate::runtime::config::client_config::TokioClientConfig;
use crate::runtime::RPCHook;

#[derive(Clone)]
pub struct RocketmqDefaultClient {
    tokio_client_config: Arc<TokioClientConfig>,
    //cache connection
    connection_tables:
        Arc<parking_lot::Mutex<HashMap<String /* ip:port */, Arc<tokio::sync::Mutex<Client>>>>>,
    namesrv_addr_list: Arc<parking_lot::RwLock<Vec<String>>>,
    namesrv_addr_choosed: Arc<parking_lot::Mutex<Option<String>>>,
    available_namesrv_addr_set: Arc<parking_lot::RwLock<HashSet<String>>>,
}
impl RocketmqDefaultClient {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>) -> Self {
        Self {
            tokio_client_config,
            connection_tables: Arc::new(parking_lot::Mutex::new(Default::default())),
            namesrv_addr_list: Arc::new(Default::default()),
            namesrv_addr_choosed: Arc::new(Default::default()),
            available_namesrv_addr_set: Arc::new(Default::default()),
        }
    }
}

impl RocketmqDefaultClient {
    fn get_and_create_client(&self, addr: String) -> Option<Arc<tokio::sync::Mutex<Client>>> {
        let mut connection_tables = self.connection_tables.lock();
        match connection_tables.get(&addr) {
            None => {
                let addr_inner = addr.clone();
                let handle = Handle::current();

                match std::thread::spawn(move || {
                    handle.block_on(async move { Client::connect(addr_inner).await })
                })
                .join()
                {
                    Ok(client_inner) => match client_inner {
                        Ok(client_r) => {
                            let client = Arc::new(tokio::sync::Mutex::new(client_r));
                            connection_tables.insert(addr, client.clone());
                            Some(client)
                        }
                        Err(_) => {
                            error!("getAndCreateClient connect to {} failed", addr);
                            None
                        }
                    },
                    Err(_) => {
                        error!("getAndCreateClient connect to {} failed", addr);
                        None
                    }
                }
            }
            Some(conn) => Some(conn.clone()),
        }
    }

    fn scan_available_name_srv(&self) {
        if self.namesrv_addr_list.read().is_empty() {
            debug!("scanAvailableNameSrv addresses of name server is null!");
            return;
        }
        for address in self.available_namesrv_addr_set.read().iter() {
            if !self.namesrv_addr_list.read().contains(address) {
                warn!("scanAvailableNameSrv remove invalid address {}", address);
                self.available_namesrv_addr_set.write().remove(address);
            }
        }
        for namesrv_addr in self.namesrv_addr_list.read().iter() {
            let client = self.get_and_create_client(namesrv_addr.clone());
            match client {
                None => {
                    self.available_namesrv_addr_set.write().remove(namesrv_addr);
                }
                Some(_) => {
                    self.available_namesrv_addr_set
                        .write()
                        .insert(namesrv_addr.clone());
                }
            }
        }
    }
}

#[allow(unused_variables)]
impl RemotingService for RocketmqDefaultClient {
    async fn start(&self) {
        let client = self.clone();
        let handle = task::spawn(async move {
            loop {
                client.scan_available_name_srv();
                time::sleep(Duration::from_millis(1)).await;
            }
        });
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
        let mut old = self.namesrv_addr_list.write();
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
        self.namesrv_addr_list.read().clone()
    }

    fn get_available_name_srv_list(&self) -> Vec<String> {
        self.available_namesrv_addr_set
            .read()
            .clone()
            .into_iter()
            .collect()
    }

    async fn invoke_async(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, RemotingError> {
        let client = self.get_and_create_client(addr.clone()).unwrap();
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

    async fn invoke_oneway(&self, addr: String, request: RemotingCommand, timeout_millis: u64) {
        let client = self.get_and_create_client(addr.clone()).unwrap();
        tokio::spawn(async move {
            match time::timeout(Duration::from_millis(timeout_millis), async move {
                client.lock().await.send(request).await
            })
            .await
            {
                Ok(_) => Ok(()),
                Err(err) => Err(RemotingError::RemoteException(err.to_string())),
            }
        });
    }

    fn is_address_reachable(&mut self, addr: String) {
        todo!()
    }

    fn close_clients(&mut self, addrs: Vec<String>) {
        todo!()
    }
}
