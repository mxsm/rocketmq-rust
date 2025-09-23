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

use cheetah_string::CheetahString;
use rand::Rng;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::clients::Client;
use crate::clients::RemotingClient;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::RemotingService;
use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use crate::runtime::config::client_config::TokioClientConfig;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;

const LOCK_TIMEOUT_MILLIS: u64 = 3000;

pub type ArcSyncClient = Arc<Mutex<Client>>;

pub struct RocketmqDefaultClient<PR = DefaultRemotingRequestProcessor> {
    tokio_client_config: Arc<TokioClientConfig>,
    //cache connection
    connection_tables: Arc<Mutex<HashMap<CheetahString /* ip:port */, Client>>>,
    namesrv_addr_list: ArcMut<Vec<CheetahString>>,
    namesrv_addr_choosed: ArcMut<Option<CheetahString>>,
    available_namesrv_addr_set: ArcMut<HashSet<CheetahString>>,
    namesrv_index: Arc<AtomicI32>,
    client_runtime: Option<RocketMQRuntime>,
    processor: PR,
    tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
}
impl<PR: RequestProcessor + Sync + Clone + 'static> RocketmqDefaultClient<PR> {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>, processor: PR) -> Self {
        Self::new_with_cl(tokio_client_config, processor, None)
    }

    pub fn new_with_cl(
        tokio_client_config: Arc<TokioClientConfig>,
        processor: PR,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> Self {
        Self {
            tokio_client_config,
            connection_tables: Arc::new(Mutex::new(Default::default())),
            namesrv_addr_list: ArcMut::new(Default::default()),
            namesrv_addr_choosed: ArcMut::new(Default::default()),
            available_namesrv_addr_set: ArcMut::new(Default::default()),
            namesrv_index: Arc::new(AtomicI32::new(init_value_index())),
            client_runtime: Some(RocketMQRuntime::new_multi(10, "client-thread")),
            processor,
            tx,
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> RocketmqDefaultClient<PR> {
    async fn get_and_create_nameserver_client(&self) -> Option<Client> {
        let mut addr = self.namesrv_addr_choosed.as_ref().clone();
        if let Some(ref addr) = addr {
            let guard = self.connection_tables.lock().await;
            let ct = guard.get(addr);
            if let Some(ct) = ct {
                let conn_status = ct.connection().ok;
                //let conn_status = ct.lock().await.connection().ok;
                if conn_status {
                    return Some(ct.clone());
                }
            }
        }
        let connection_tables = self.connection_tables.lock().await;

        addr.clone_from(self.namesrv_addr_choosed.as_ref());
        if let Some(addr) = addr.as_ref() {
            let ct = connection_tables.get(addr);
            if let Some(ct) = ct {
                let conn_status = ct.connection().ok;
                //let conn_status = ct.lock().await.connection().ok;
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
            let new_addr = &addr_list[index];
            info!(
                "new name remoting_server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}",
                new_addr, new_addr, index
            );
            self.namesrv_addr_choosed
                .mut_from_ref()
                .replace(new_addr.clone());
            drop(connection_tables);
            return self
                .create_client(
                    new_addr,
                    //&mut connection_tables,
                    Duration::from_millis(self.tokio_client_config.connect_timeout_millis as u64),
                )
                .await;
        }
        None
    }

    async fn get_and_create_client(&self, addr: Option<&CheetahString>) -> Option<Client> {
        match addr {
            None => self.get_and_create_nameserver_client().await,
            Some(addr) => {
                if addr.is_empty() {
                    return self.get_and_create_nameserver_client().await;
                }
                let client = self.connection_tables.lock().await.get(addr).cloned();
                // if client.is_some() && client.as_ref()?.lock().await.connection().ok {
                if client.is_some() && client.as_ref()?.connection().ok {
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

    async fn create_client(&self, addr: &CheetahString, duration: Duration) -> Option<Client> {
        let mut connection_tables = self.connection_tables.lock().await;
        let cw = connection_tables.get(addr);
        if let Some(cw) = cw {
            // if cw.lock().await.connection().ok {
            if cw.connection().ok {
                return Some(cw.clone());
            }
        }

        let cw = connection_tables.get(addr.as_str());
        if let Some(cw) = cw {
            if cw.connection().ok {
                // if cw.lock().await.connection().ok {
                return Some(cw.clone());
            }
        } else {
            let _ = connection_tables.remove(addr.as_str());
        }

        let addr_inner = addr.to_string();

        match time::timeout(duration, async {
            Client::connect(addr_inner, self.processor.clone(), self.tx.as_ref()).await
        })
        .await
        {
            Ok(client_inner) => match client_inner {
                Ok(client_r) => {
                    //let client = Arc::new(Mutex::new(client_r));
                    let client = client_r;
                    connection_tables.insert(addr.clone(), client.clone());
                    Some(client)
                }
                Err(e) => {
                    error!("getAndCreateClient connect to {} failed,{e}", addr);
                    None
                }
            },
            Err(e) => {
                error!("getAndCreateClient connect to {} failed,{e}", addr);
                None
            }
        }
    }

    async fn scan_available_name_srv(&self) {
        if self.namesrv_addr_list.as_ref().is_empty() {
            debug!("scanAvailableNameSrv addresses of name remoting_server is null!");
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
            let client = self.get_and_create_client(Some(namesrv_addr)).await;
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
impl<PR: RequestProcessor + Sync + Clone + 'static> RemotingService for RocketmqDefaultClient<PR> {
    async fn start(&self, this: WeakArcMut<Self>) {
        if let Some(client) = this.upgrade() {
            let connect_timeout_millis = self.tokio_client_config.connect_timeout_millis as u64;
            self.client_runtime
                .as_ref()
                .unwrap()
                .get_handle()
                .spawn(async move {
                    loop {
                        client.scan_available_name_srv().await;
                        time::sleep(Duration::from_millis(connect_timeout_millis)).await;
                    }
                });
        }
    }

    fn shutdown(&mut self) {
        if let Some(rt) = self.client_runtime.take() {
            rt.shutdown();
        }
        let connection_tables = self.connection_tables.clone();
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async move {
                connection_tables.lock().await.clear();
            });
        });
        self.namesrv_addr_list.clear();
        self.available_namesrv_addr_set.clear();

        info!(">>>>>>>>>>>>>>>RemotingClient shutdown success<<<<<<<<<<<<<<<<<");
    }

    fn register_rpc_hook(&mut self, hook: Arc<Box<dyn RPCHook>>) {
        todo!()
    }

    fn clear_rpc_hook(&mut self) {
        todo!()
    }
}

#[allow(unused_variables)]
impl<PR: RequestProcessor + Sync + Clone + 'static> RemotingClient for RocketmqDefaultClient<PR> {
    async fn update_name_server_address_list(&self, addrs: Vec<CheetahString>) {
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
                    "name remoting_server address updated. NEW : {:?} , OLD: {:?}",
                    addrs, old
                );
                /* let mut rng = thread_rng();
                addrs.shuffle(&mut rng);*/
                self.namesrv_addr_list.mut_from_ref().extend(addrs.clone());

                // should close the channel if choosed addr is not exist.
                if let Some(namesrv_addr) = self.namesrv_addr_choosed.as_ref() {
                    if !addrs.contains(namesrv_addr) {
                        let mut remove_vec = Vec::new();
                        let mut result = self.connection_tables.lock().await;
                        for (addr, client) in result.iter() {
                            if addr == namesrv_addr {
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

    fn get_name_server_address_list(&self) -> &[CheetahString] {
        self.namesrv_addr_list.as_ref()
    }

    fn get_available_name_srv_list(&self) -> Vec<CheetahString> {
        self.available_namesrv_addr_set
            .as_ref()
            .clone()
            .into_iter()
            .collect()
    }

    async fn invoke_async(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let client = self.get_and_create_client(addr).await;
        match client {
            None => Err(rocketmq_error::RocketmqError::RemoteError(
                "get client failed".to_string(),
            )),
            Some(mut client) => {
                match self
                    .client_runtime
                    .as_ref()
                    .unwrap()
                    .get_handle()
                    .spawn(async move {
                        time::timeout(Duration::from_millis(timeout_millis), async move {
                            client.send_read(request, timeout_millis).await
                        })
                        .await
                    })
                    .await
                {
                    Ok(result) => match result {
                        Ok(response) => match response {
                            Ok(value) => Ok(value),
                            Err(e) => {
                                Err(rocketmq_error::RocketmqError::RemoteError(e.to_string()))
                            }
                        },
                        Err(err) => {
                            Err(rocketmq_error::RocketmqError::RemoteError(err.to_string()))
                        }
                    },
                    Err(err) => Err(rocketmq_error::RocketmqError::RemoteError(err.to_string())),
                }
            }
        }
    }

    async fn invoke_oneway(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) {
        let client = self.get_and_create_client(Some(addr)).await;
        match client {
            None => {
                error!("get client failed");
            }
            Some(mut client) => {
                self.client_runtime
                    .as_ref()
                    .unwrap()
                    .get_handle()
                    .spawn(async move {
                        match time::timeout(Duration::from_millis(timeout_millis), async move {
                            let mut request = request;
                            request.mark_oneway_rpc_ref();
                            client.send(request).await
                        })
                        .await
                        {
                            Ok(_) => Ok(()),
                            Err(err) => {
                                Err(rocketmq_error::RocketmqError::RemoteError(err.to_string()))
                            }
                        }
                    });
            }
        }
    }

    fn is_address_reachable(&mut self, addr: &CheetahString) {
        todo!()
    }

    fn close_clients(&mut self, addrs: Vec<String>) {
        todo!()
    }

    fn register_processor(&mut self, processor: impl RequestProcessor + Sync) {
        todo!()
    }
}

fn init_value_index() -> i32 {
    let mut rng = rand::rng();
    rng.random_range(0..999)
}
