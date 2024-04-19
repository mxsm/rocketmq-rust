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
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_remoting::{
    code::request_code::RequestCode,
    runtime::{
        processor::RequestProcessor, server::RocketMQServer, BoxedRequestProcessor,
        RequestProcessorTable,
    },
    server::config::ServerConfig,
};
use rocketmq_runtime::RocketMQRuntime;
use tokio::{select, sync::broadcast};

use crate::{
    processor::{
        default_request_processor::DefaultRequestProcessor, ClientRequestProcessor,
        NameServerRequestProcessor,
    },
    KVConfigManager, RouteInfoManager,
};

pub struct NameServerBootstrap {
    name_server_runtime: NameServerRuntime,
}

pub struct Builder {
    name_server_config: Option<NamesrvConfig>,
    server_config: Option<ServerConfig>,
}

struct NameServerRuntime {
    name_server_config: Arc<NamesrvConfig>,
    server_config: Arc<ServerConfig>,
    route_info_manager: Arc<parking_lot::RwLock<RouteInfoManager>>,
    kvconfig_manager: Arc<parking_lot::RwLock<KVConfigManager>>,
    name_server_runtime: Option<RocketMQRuntime>,
}

impl NameServerBootstrap {
    pub async fn boot(mut self) {
        select! {
            _ = self.name_server_runtime.start() =>{

            }
        }
    }
}

impl NameServerRuntime {
    pub async fn start(&mut self) {
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        let receiver = notify_conn_disconnect.subscribe();
        let request_processor = self.init_processors(receiver);
        let server = RocketMQServer::new(self.server_config.clone(), request_processor);
        server.run().await;
    }

    fn init_processors(
        &self,
        receiver: broadcast::Receiver<SocketAddr>,
    ) -> Arc<NameServerRequestProcessor> {
        RouteInfoManager::start(self.route_info_manager.clone(), receiver);

        let client_request_processor = ClientRequestProcessor::new(
            self.route_info_manager.clone(),
            self.name_server_config.clone(),
            self.kvconfig_manager.clone(),
        );
        let default_request_processor = DefaultRequestProcessor::new_with(
            self.route_info_manager.clone(),
            self.kvconfig_manager.clone(),
        );

        let route_info_manager_arc = self.route_info_manager.clone();
        self.name_server_runtime
            .as_ref()
            .unwrap()
            .schedule_at_fixed_rate(
                move || {
                    route_info_manager_arc.write().scan_not_active_broker();
                },
                Some(Duration::from_secs(5)),
                Duration::from_secs(5),
            );
        Arc::new(NameServerRequestProcessor {
            client_request_processor: Arc::new(client_request_processor),
            default_request_processor: Arc::new(default_request_processor),
        })
    }
}

impl Drop for NameServerRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.name_server_runtime.take() {
            runtime.shutdown();
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            name_server_config: None,
            server_config: None,
        }
    }

    pub fn set_name_server_config(mut self, name_server_config: NamesrvConfig) -> Self {
        self.name_server_config = Some(name_server_config);
        self
    }

    pub fn set_server_config(mut self, server_config: ServerConfig) -> Self {
        self.server_config = Some(server_config);
        self
    }

    pub fn build(self) -> NameServerBootstrap {
        let name_server_config = Arc::new(self.name_server_config.unwrap());
        let runtime = RocketMQRuntime::new_multi(10, "namesrv-thread");
        NameServerBootstrap {
            name_server_runtime: NameServerRuntime {
                name_server_config: name_server_config.clone(),
                server_config: Arc::new(self.server_config.unwrap()),
                route_info_manager: Arc::new(parking_lot::RwLock::new(
                    RouteInfoManager::new_with_config(name_server_config.clone()),
                )),
                kvconfig_manager: Arc::new(parking_lot::RwLock::new(KVConfigManager::new(
                    name_server_config,
                ))),
                name_server_runtime: Some(runtime),
            },
        }
    }
}
