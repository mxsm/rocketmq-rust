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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::utils::network_util::NetworkUtil;
use rocketmq_remoting::clients::rocketmq_default_impl::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::wait_for_signal;
use rocketmq_rust::ArcMut;
use tokio::sync::broadcast;
use tracing::info;

use crate::processor::ClientRequestProcessor;
use crate::processor::NameServerRequestProcessor;
use crate::KVConfigManager;
use crate::RouteInfoManager;

pub struct NameServerBootstrap {
    name_server_runtime: NameServerRuntime,
}

pub struct Builder {
    name_server_config: Option<NamesrvConfig>,
    server_config: Option<ServerConfig>,
}

struct NameServerRuntime {
    name_server_config: ArcMut<NamesrvConfig>,
    tokio_client_config: Arc<TokioClientConfig>,
    server_config: Arc<ServerConfig>,
    route_info_manager: RouteInfoManager,
    kvconfig_manager: KVConfigManager,
    name_server_runtime: Option<RocketMQRuntime>,
    remoting_client: ArcMut<RocketmqDefaultClient>,
}

impl NameServerBootstrap {
    pub async fn boot(mut self) {
        /*select! {
            _ = self.name_server_runtime.start() =>{

            }
        }*/
        tokio::join!(self.name_server_runtime.start(), wait_for_signal());
    }
}

impl NameServerRuntime {
    pub async fn start(&mut self) {
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        let receiver = notify_conn_disconnect.subscribe();
        let request_processor = self.init_processors(receiver);
        let server = RocketMQServer::new(self.server_config.clone());
        tokio::spawn(async move {
            server.run(request_processor).await;
        });
        let namesrv = CheetahString::from_string(format!(
            "{}:{}",
            NetworkUtil::get_local_address().unwrap(),
            self.server_config.listen_port
        ));
        let weak_arc_mut = ArcMut::downgrade(&self.remoting_client);
        self.remoting_client
            .update_name_server_address_list(vec![namesrv])
            .await;
        self.remoting_client.start(weak_arc_mut).await;
        info!("Rocketmq NameServer(Rust) started");
    }

    fn init_processors(
        &self,
        receiver: broadcast::Receiver<SocketAddr>,
    ) -> NameServerRequestProcessor {
        RouteInfoManager::start(self.route_info_manager.clone(), receiver);

        let client_request_processor = ClientRequestProcessor::new(
            self.route_info_manager.clone(),
            self.name_server_config.clone(),
            self.kvconfig_manager.clone(),
        );
        let default_request_processor =
            crate::processor::default_request_processor::DefaultRequestProcessor::new(
                self.route_info_manager.clone(),
                self.kvconfig_manager.clone(),
            );

        let mut route_info_manager_arc = self.route_info_manager.clone();
        self.name_server_runtime
            .as_ref()
            .unwrap()
            .schedule_at_fixed_rate_mut(
                move || {
                    route_info_manager_arc.scan_not_active_broker();
                },
                Some(Duration::from_secs(5)),
                Duration::from_secs(5),
            );
        NameServerRequestProcessor {
            client_request_processor: ArcMut::new(client_request_processor),
            default_request_processor: ArcMut::new(default_request_processor),
        }
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
        let name_server_config = ArcMut::new(self.name_server_config.unwrap_or_default());
        let runtime = RocketMQRuntime::new_multi(10, "namesrv-thread");
        let tokio_client_config = Arc::new(TokioClientConfig::default());
        let remoting_client = ArcMut::new(RocketmqDefaultClient::new(
            tokio_client_config.clone(),
            DefaultRemotingRequestProcessor,
        ));

        NameServerBootstrap {
            name_server_runtime: NameServerRuntime {
                name_server_config: name_server_config.clone(),
                tokio_client_config,
                server_config: Arc::new(self.server_config.unwrap()),
                route_info_manager: RouteInfoManager::new(
                    name_server_config.clone(),
                    remoting_client.clone(),
                ),
                kvconfig_manager: KVConfigManager::new(name_server_config),
                name_server_runtime: Some(runtime),
                remoting_client,
            },
        }
    }
}

pub(crate) struct NameServerRuntimeInner {
    name_server_config: NamesrvConfig,
    tokio_client_config: TokioClientConfig,
    server_config: ServerConfig,
    route_info_manager: RouteInfoManager,
    kvconfig_manager: KVConfigManager,
    remoting_client: RocketmqDefaultClient,
}

impl NameServerRuntimeInner {
    #[inline]
    pub fn name_server_config_mut(&mut self) -> &mut NamesrvConfig {
        &mut self.name_server_config
    }

    #[inline]
    pub fn tokio_client_config_mut(&mut self) -> &mut TokioClientConfig {
        &mut self.tokio_client_config
    }

    #[inline]
    pub fn server_config_mut(&mut self) -> &mut ServerConfig {
        &mut self.server_config
    }

    #[inline]
    pub fn route_info_manager_mut(&mut self) -> &mut RouteInfoManager {
        &mut self.route_info_manager
    }

    #[inline]
    pub fn kvconfig_manager_mut(&mut self) -> &mut KVConfigManager {
        &mut self.kvconfig_manager
    }

    #[inline]
    pub fn remoting_client_mut(&mut self) -> &mut RocketmqDefaultClient {
        &mut self.remoting_client
    }

    #[inline]
    pub fn name_server_config(&self) -> &NamesrvConfig {
        &self.name_server_config
    }

    #[inline]
    pub fn tokio_client_config(&self) -> &TokioClientConfig {
        &self.tokio_client_config
    }

    #[inline]
    pub fn server_config(&self) -> &ServerConfig {
        &self.server_config
    }

    #[inline]
    pub fn route_info_manager(&self) -> &RouteInfoManager {
        &self.route_info_manager
    }

    #[inline]
    pub fn kvconfig_manager(&self) -> &KVConfigManager {
        &self.kvconfig_manager
    }

    #[inline]
    pub fn remoting_client(&self) -> &RocketmqDefaultClient {
        &self.remoting_client
    }

    #[inline]
    pub fn set_name_server_config(&mut self, name_server_config: NamesrvConfig) {
        self.name_server_config = name_server_config;
    }

    #[inline]
    pub fn set_tokio_client_config(&mut self, tokio_client_config: TokioClientConfig) {
        self.tokio_client_config = tokio_client_config;
    }

    #[inline]
    pub fn set_server_config(&mut self, server_config: ServerConfig) {
        self.server_config = server_config;
    }

    #[inline]
    pub fn set_route_info_manager(&mut self, route_info_manager: RouteInfoManager) {
        self.route_info_manager = route_info_manager;
    }

    #[inline]
    pub fn set_kvconfig_manager(&mut self, kvconfig_manager: KVConfigManager) {
        self.kvconfig_manager = kvconfig_manager;
    }

    #[inline]
    pub fn set_remoting_client(&mut self, remoting_client: RocketmqDefaultClient) {
        self.remoting_client = remoting_client;
    }
}
