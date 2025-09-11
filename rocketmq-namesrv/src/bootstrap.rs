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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::clients::rocketmq_default_impl::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::wait_for_signal;
use rocketmq_rust::ArcMut;
use tokio::sync::broadcast;
use tracing::info;
use tracing::warn;

use crate::processor::ClientRequestProcessor;
use crate::processor::NameServerRequestProcessor;
use crate::processor::NameServerRequestProcessorWrapper;
use crate::route_info::broker_housekeeping_service::BrokerHousekeepingService;
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
    //name_server_runtime: Option<RocketMQRuntime>,
    inner: ArcMut<NameServerRuntimeInner>,
    scheduled_task_manager: ScheduledTaskManager,
    // receiver for shutdown signal
    shutdown_rx: Option<tokio::sync::broadcast::Receiver<()>>,
}

impl NameServerBootstrap {
    pub async fn boot(mut self) -> RocketMQResult<()> {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.name_server_runtime.shutdown_rx = Some(shutdown_rx);
        self.name_server_runtime.initialize().await?;
        tokio::join!(
            self.name_server_runtime.start(),
            wait_for_signal_inner(shutdown_tx)
        );
        Ok(())
    }
}

#[inline]
async fn wait_for_signal_inner(shutdown_tx: broadcast::Sender<()>) {
    tokio::select! {
        _ = wait_for_signal() => {
            info!("Received signal, initiating shutdown...");
        }
    }
    // Send shutdown signal to all tasks
    let _ = shutdown_tx.send(());
}

impl NameServerRuntime {
    pub async fn initialize(&mut self) -> RocketMQResult<()> {
        self.load_config().await?;
        self.initiate_network_components();
        self.register_processor();
        self.start_schedule_service();
        self.initiate_ssl_context();
        self.initiate_rpc_hooks();
        Ok(())
    }

    async fn load_config(&mut self) -> RocketMQResult<()> {
        if let Some(kv_config_manager) = self.inner.kvconfig_manager.as_mut() {
            kv_config_manager.load()?;
        }
        Ok(())
    }
    fn initiate_network_components(&mut self) {
        //nothing to do
    }

    fn register_processor(&mut self) {
        //nothing to do
    }
    fn start_schedule_service(&self) {
        let scan_not_active_broker_interval = self
            .inner
            .name_server_config
            .scan_not_active_broker_interval;
        let mut name_server_runtime_inner = self.inner.clone();
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(5),
            Duration::from_millis(scan_not_active_broker_interval),
            async move |_ctx| {
                if let Some(route_info_manager) =
                    name_server_runtime_inner.route_info_manager.as_mut()
                {
                    route_info_manager.scan_not_active_broker();
                }
                Ok(())
            },
        );
    }
    fn initiate_ssl_context(&mut self) {
        warn!("SSL is not supported yet");
    }
    fn initiate_rpc_hooks(&mut self) {
        warn!("RPC hooks are not supported yet");
    }

    pub async fn start(&mut self) {
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        let receiver = notify_conn_disconnect.subscribe();
        let request_processor = self.init_processors(receiver);
        let server = RocketMQServer::new(Arc::new(self.inner.server_config.clone()));
        let channel_event_listener = self
            .inner
            .broker_housekeeping_service
            .take()
            .map(|item| item as Arc<dyn ChannelEventListener>);
        tokio::spawn(async move {
            server.run(request_processor, channel_event_listener).await;
        });
        let namesrv = CheetahString::from_string(format!(
            "{}:{}",
            NetworkUtil::get_local_address().unwrap(),
            self.inner.server_config.listen_port
        ));
        let weak_arc_mut = ArcMut::downgrade(&self.inner.remoting_client);
        self.inner
            .remoting_client
            .update_name_server_address_list(vec![namesrv])
            .await;
        self.inner.remoting_client.start(weak_arc_mut).await;
        info!("Rocketmq NameServer(Rust) started");

        tokio::select! {
            _ = self.shutdown_rx.as_mut().unwrap().recv() => {
                info!("Shutdown received, initiating graceful shutdown...");
                self.shutdown();
            }
        }
    }

    #[inline]
    fn shutdown(&mut self) {
        self.scheduled_task_manager.cancel_all();
        self.inner
            .route_info_manager_mut()
            .un_register_service
            .shutdown();
        /*if let Some(runtime) = self.name_server_runtime.take() {
            runtime.shutdown();
        }*/
        info!("Rocketmq NameServer(Rust) gracefully shutdown completed");
    }

    #[inline]
    fn init_processors(
        &self,
        receiver: broadcast::Receiver<SocketAddr>,
    ) -> NameServerRequestProcessor {
        self.inner.route_info_manager().start(receiver);

        let client_request_processor = ClientRequestProcessor::new(self.inner.clone());
        let default_request_processor =
            crate::processor::default_request_processor::DefaultRequestProcessor::new(
                self.inner.clone(),
            );
        let mut name_server_request_processor = NameServerRequestProcessor::new();
        name_server_request_processor.register_processor(
            RequestCode::GetRouteinfoByTopic,
            NameServerRequestProcessorWrapper::ClientRequestProcessor(ArcMut::new(
                client_request_processor,
            )),
        );
        name_server_request_processor.register_default_processor(
            NameServerRequestProcessorWrapper::DefaultRequestProcessor(ArcMut::new(
                default_request_processor,
            )),
        );
        name_server_request_processor
    }
}

impl Drop for NameServerRuntime {
    #[inline]
    fn drop(&mut self) {
        /*if let Some(runtime) = self.name_server_runtime.take() {
            runtime.shutdown();
        }*/
    }
}

impl Default for Builder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    #[inline]
    pub fn new() -> Self {
        Builder {
            name_server_config: None,
            server_config: None,
        }
    }

    #[inline]
    pub fn set_name_server_config(mut self, name_server_config: NamesrvConfig) -> Self {
        self.name_server_config = Some(name_server_config);
        self
    }

    #[inline]
    pub fn set_server_config(mut self, server_config: ServerConfig) -> Self {
        self.server_config = Some(server_config);
        self
    }

    #[inline]
    pub fn build(self) -> NameServerBootstrap {
        let name_server_config = self.name_server_config.unwrap_or_default();
        //let runtime = RocketMQRuntime::new_multi(10, "namesrv-thread");
        let tokio_client_config = TokioClientConfig::default();
        let remoting_client = ArcMut::new(RocketmqDefaultClient::new(
            Arc::new(tokio_client_config.clone()),
            DefaultRemotingRequestProcessor,
        ));
        let server_config = self.server_config.unwrap_or_default();
        let mut inner = ArcMut::new(NameServerRuntimeInner {
            name_server_config,
            tokio_client_config,
            server_config,
            route_info_manager: None,
            kvconfig_manager: None,
            remoting_client,
            broker_housekeeping_service: None,
        });

        let route_info_manager = RouteInfoManager::new(inner.clone());
        let kv_config_manager = KVConfigManager::new(inner.clone());

        inner.kvconfig_manager = Some(kv_config_manager);
        inner.route_info_manager = Some(route_info_manager);
        inner.broker_housekeeping_service =
            Some(Arc::new(BrokerHousekeepingService::new(inner.clone())));

        NameServerBootstrap {
            name_server_runtime: NameServerRuntime {
                //name_server_runtime: Some(runtime),
                inner,
                scheduled_task_manager: ScheduledTaskManager::new(),
                shutdown_rx: None,
            },
        }
    }
}

pub(crate) struct NameServerRuntimeInner {
    name_server_config: NamesrvConfig,
    tokio_client_config: TokioClientConfig,
    server_config: ServerConfig,
    route_info_manager: Option<RouteInfoManager>,
    kvconfig_manager: Option<KVConfigManager>,
    remoting_client: ArcMut<RocketmqDefaultClient>,
    broker_housekeeping_service: Option<Arc<BrokerHousekeepingService>>,
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
        self.route_info_manager
            .as_mut()
            .expect("route_info_manager is None")
    }

    #[inline]
    pub fn kvconfig_manager_mut(&mut self) -> &mut KVConfigManager {
        self.kvconfig_manager
            .as_mut()
            .expect("kvconfig_manager is None")
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
        self.route_info_manager
            .as_ref()
            .expect("route_info_manager is None")
    }

    #[inline]
    pub fn kvconfig_manager(&self) -> &KVConfigManager {
        self.kvconfig_manager
            .as_ref()
            .expect("kvconfig_manager is None")
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
        self.route_info_manager = Some(route_info_manager);
    }

    #[inline]
    pub fn set_kvconfig_manager(&mut self, kvconfig_manager: KVConfigManager) {
        self.kvconfig_manager = Some(kvconfig_manager);
    }

    #[inline]
    pub fn set_remoting_client(&mut self, remoting_client: ArcMut<RocketmqDefaultClient>) {
        self.remoting_client = remoting_client;
    }
}
