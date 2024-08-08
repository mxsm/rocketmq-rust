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
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_runtime::RocketMQRuntime;
use tokio::sync::Mutex;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::clients::Client;
use crate::clients::RemotingClient;
use crate::error::Error;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::RemotingService;
use crate::runtime::config::client_config::TokioClientConfig;
use crate::runtime::RPCHook;
use crate::Result;

const LOCK_TIMEOUT_MILLIS: u64 = 3000;

#[derive(Clone)]
pub struct RocketmqDefaultClient {
    tokio_client_config: Arc<TokioClientConfig>,
    //cache connection
    connection_tables: Arc<Mutex<HashMap<String /* ip:port */, ArcRefCellWrapper<Client>>>>,
    namesrv_addr_list: ArcRefCellWrapper<Vec<String>>,
    namesrv_addr_choosed: ArcRefCellWrapper<Option<String>>,
    available_namesrv_addr_set: ArcRefCellWrapper<HashSet<String>>,
    namesrv_index: Arc<AtomicI32>,
    client_runtime: Arc<RocketMQRuntime>,
}
impl RocketmqDefaultClient {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>) -> Self {
        Self {
            tokio_client_config,
            connection_tables: Arc::new(Mutex::new(Default::default())),
            namesrv_addr_list: ArcRefCellWrapper::new(Default::default()),
            namesrv_addr_choosed: ArcRefCellWrapper::new(Default::default()),
            available_namesrv_addr_set: ArcRefCellWrapper::new(Default::default()),
            namesrv_index: Arc::new(AtomicI32::new(init_value_index())),
            client_runtime: Arc::new(RocketMQRuntime::new_multi(10, "client-thread")),
        }
    }
}

impl RocketmqDefaultClient {
    async fn get_and_create_nameserver_client(&self) -> Option<ArcRefCellWrapper<Client>> {
        let mut addr = self.namesrv_addr_choosed.as_ref().clone();
        if let Some(ref addr) = addr {
            let guard = self.connection_tables.lock().await;
            let ct = guard.get(addr);
            if let Some(ct) = ct {
                let conn_status = ct.connection().ok;
                if conn_status {
                    return Some(ct.clone());
                }
            }
        }
        let connection_tables = self.connection_tables.try_lock();
        if let Ok(connection_tables) = connection_tables {
            addr.clone_from(self.namesrv_addr_choosed.as_ref());
            if let Some(addr) = addr.as_ref() {
                let ct = connection_tables.get(addr);
                if let Some(ct) = ct {
                    let conn_status = ct.connection().ok;
                    if conn_status {
                        return Some(ct.clone());
                    }
                }
            }
            let addr_list = self.namesrv_addr_list.as_ref();
            if !addr_list.is_empty() {
                let index = self
                    .namesrv_index
                    .fetch_and(1, std::sync::atomic::Ordering::Release)
                    .abs();
                let index = index as usize % addr_list.len();
                let new_addr = addr_list[index].clone();
                info!(
                    "new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}",
                    new_addr, new_addr, index
                );
                self.namesrv_addr_choosed
                    .mut_from_ref()
                    .replace(new_addr.clone());
                drop(connection_tables);
                return self
                    .create_client(
                        new_addr.as_str(),
                        //&mut connection_tables,
                        Duration::from_millis(
                            self.tokio_client_config.connect_timeout_millis as u64,
                        ),
                    )
                    .await;
            }
        }
        None
    }

    async fn get_and_create_client(&self, addr: Option<&str>) -> Option<ArcRefCellWrapper<Client>> {
        match addr {
            None => self.get_and_create_nameserver_client().await,
            Some(addr) => {
                if addr.is_empty() {
                    return self.get_and_create_nameserver_client().await;
                }
                let client = self.connection_tables.lock().await.get(addr).cloned();
                if client.is_some() && client.as_ref().unwrap().connection().ok {
                    return client;
                }
                self.create_client(
                    addr,
                    Duration::from_millis(self.tokio_client_config.connect_timeout_millis as u64),
                )
                .await
            }
        }
    }

    async fn create_client(
        &self,
        addr: &str,
        duration: Duration,
    ) -> Option<ArcRefCellWrapper<Client>> {
        let binding = self.connection_tables.lock().await;
        let cw = binding.get(addr);
        if let Some(cw) = cw {
            if cw.connection().ok {
                return Some(cw.clone());
            }
        }
        drop(binding);

        let connection_tables_lock = self.connection_tables.try_lock();
        if let Ok(mut connection_tables) = connection_tables_lock {
            let cw = connection_tables.get(addr);
            if let Some(cw) = cw {
                if cw.connection().ok {
                    return Some(cw.clone());
                }
            } else {
                let _ = connection_tables.remove(addr);
            }
            drop(connection_tables);
            let addr_inner = addr.to_string();

            match self
                .client_runtime
                .get_handle()
                .spawn(async move {
                    time::timeout(duration, async { Client::connect(addr_inner).await }).await?
                })
                .await
            {
                Ok(client_inner) => match client_inner {
                    Ok(client_r) => {
                        let client = ArcRefCellWrapper::new(client_r);
                        self.connection_tables
                            .lock()
                            .await
                            .insert(addr.to_string(), client.clone());
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
        } else {
            None
        }
    }

    async fn scan_available_name_srv(&self) {
        if self.namesrv_addr_list.as_ref().is_empty() {
            debug!("scanAvailableNameSrv addresses of name server is null!");
            return;
        }
        for address in self.available_namesrv_addr_set.as_ref().iter() {
            if !self.namesrv_addr_list.as_ref().contains(address) {
                warn!("scanAvailableNameSrv remove invalid address {}", address);
                self.available_namesrv_addr_set
                    .mut_from_ref()
                    .remove(address);
            }
        }
        for namesrv_addr in self.namesrv_addr_list.as_ref().iter() {
            let client = self
                .get_and_create_client(Some(namesrv_addr.as_str()))
                .await;
            match client {
                None => {
                    self.available_namesrv_addr_set
                        .mut_from_ref()
                        .remove(namesrv_addr);
                }
                Some(_) => {
                    self.available_namesrv_addr_set
                        .mut_from_ref()
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
        //invoke scan available name sever now
        client.scan_available_name_srv().await;
        /*let handle = task::spawn(async move {
            loop {
                time::sleep(Duration::from_millis(1)).await;
                client.scan_available_name_srv().await;
            }
        });*/
        self.client_runtime.get_handle().spawn(async move {
            loop {
                time::sleep(Duration::from_millis(1)).await;
                client.scan_available_name_srv().await;
            }
        });
    }

    fn shutdown(&mut self) {
        todo!()
    }

    fn register_rpc_hook(&mut self, hook: Arc<Box<dyn RPCHook>>) {
        todo!()
    }

    fn clear_rpc_hook(&mut self) {
        todo!()
    }
}

#[allow(unused_variables)]
impl RemotingClient for RocketmqDefaultClient {
    async fn update_name_server_address_list(&self, addrs: Vec<String>) {
        let old = self.namesrv_addr_list.mut_from_ref();
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
                if let Some(namesrv_addr) = self.namesrv_addr_choosed.as_ref() {
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
        self.namesrv_addr_list.as_ref().clone()
    }

    fn get_available_name_srv_list(&self) -> Vec<String> {
        self.available_namesrv_addr_set
            .as_ref()
            .clone()
            .into_iter()
            .collect()
    }

    async fn invoke_async(
        &self,
        addr: Option<String>,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand> {
        let client = self.get_and_create_client(addr.as_deref()).await;
        match client {
            None => Err(Error::RemoteException("get client failed".to_string())),
            Some(mut client) => {
                match self
                    .client_runtime
                    .get_handle()
                    .spawn(async move {
                        time::timeout(Duration::from_millis(timeout_millis), async move {
                            client.send_read(request).await
                        })
                        .await
                    })
                    .await
                {
                    Ok(result) => match result {
                        Ok(response) => match response {
                            Ok(value) => Ok(value),
                            Err(e) => Err(Error::RemoteException(e.to_string())),
                        },
                        Err(err) => Err(Error::RemoteException(err.to_string())),
                    },
                    Err(err) => Err(Error::RemoteException(err.to_string())),
                }
            }
        }
    }

    async fn invoke_oneway(&self, addr: String, request: RemotingCommand, timeout_millis: u64) {
        let client = self.get_and_create_client(Some(addr.as_str())).await;
        match client {
            None => {
                error!("get client failed");
            }
            Some(mut client) => {
                self.client_runtime.get_handle().spawn(async move {
                    match time::timeout(Duration::from_millis(timeout_millis), async move {
                        client.send(request).await
                    })
                    .await
                    {
                        Ok(_) => Ok(()),
                        Err(err) => Err(Error::RemoteException(err.to_string())),
                    }
                });
            }
        }
    }

    fn is_address_reachable(&mut self, addr: String) {
        todo!()
    }

    fn close_clients(&mut self, addrs: Vec<String>) {
        todo!()
    }
}

fn init_value_index() -> i32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(0..999)
}
