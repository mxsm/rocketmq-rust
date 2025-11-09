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
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::RocketMQServer;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::wait_for_signal;
use rocketmq_rust::ArcMut;
use tokio::sync::broadcast;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::processor::ClientRequestProcessor;
use crate::processor::NameServerRequestProcessor;
use crate::processor::NameServerRequestProcessorWrapper;
use crate::route::route_info_manager::RouteInfoManager;
use crate::route::route_info_manager_v2::RouteInfoManagerV2;
use crate::route::route_info_manager_wrapper::RouteInfoManagerWrapper;
use crate::route::zone_route_rpc_hook::ZoneRouteRPCHook;
use crate::route_info::broker_housekeeping_service::BrokerHousekeepingService;
use crate::KVConfigManager;

pub struct NameServerBootstrap {
    name_server_runtime: NameServerRuntime,
}

/// Builder for creating NameServerBootstrap with custom configuration
pub struct Builder {
    name_server_config: Option<NamesrvConfig>,
    server_config: Option<ServerConfig>,
}

/// Core runtime managing NameServer lifecycle and operations
struct NameServerRuntime {
    inner: ArcMut<NameServerRuntimeInner>,
    scheduled_task_manager: ScheduledTaskManager,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    server_inner: Option<RocketMQServer<NameServerRequestProcessor>>,
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
        self.initialize_network_components();
        self.register_processor();
        self.start_schedule_service();
        self.initialize_ssl_context();
        self.initialize_rpc_hooks();
        Ok(())
    }

    async fn load_config(&mut self) -> RocketMQResult<()> {
        if let Some(kv_config_manager) = self.inner.kvconfig_manager.as_mut() {
            kv_config_manager.load()?;
        }
        Ok(())
    }

    /// Initialize network components for handling client requests
    fn initialize_network_components(&mut self) {
        let server = RocketMQServer::new(Arc::new(self.inner.server_config.clone()));
        self.server_inner = Some(server);
    }

    /// Register request processors for handling different types of requests
    fn register_processor(&mut self) {
        // Processor registration is handled in init_processors()
    }

    /// Start scheduled tasks for broker health monitoring
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

    /// Initialize SSL context (not implemented yet)
    fn initialize_ssl_context(&mut self) {
        warn!("SSL is not supported yet");
    }

    /// Initialize RPC hooks for request pre-processing
    fn initialize_rpc_hooks(&mut self) {
        if let Some(server) = self.server_inner.as_mut() {
            server.register_rpc_hook(Arc::new(ZoneRouteRPCHook));
        }
    }

    pub async fn start(&mut self) {
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        let receiver = notify_conn_disconnect.subscribe();
        let request_processor = self.init_processors(receiver);

        // Take server instance for async execution
        let mut server = self
            .server_inner
            .take()
            .expect("Server not initialized - call initialize() first");

        let channel_event_listener = self
            .inner
            .broker_housekeeping_service
            .take()
            .map(|item| item as Arc<dyn ChannelEventListener>);

        tokio::spawn(async move {
            server.run(request_processor, channel_event_listener).await;
        });

        // Setup remoting client with name server address
        let local_address = NetworkUtil::get_local_address().unwrap_or_else(|| {
            warn!("Failed to get local address, using 127.0.0.1");
            "127.0.0.1".to_string()
        });

        let namesrv = CheetahString::from_string(format!(
            "{}:{}",
            local_address, self.inner.server_config.listen_port
        ));

        let weak_arc_mut = ArcMut::downgrade(&self.inner.remoting_client);
        self.inner
            .remoting_client
            .update_name_server_address_list(vec![namesrv])
            .await;
        self.inner.remoting_client.start(weak_arc_mut).await;

        info!("RocketMQ NameServer (Rust) started successfully");

        // Wait for shutdown signal
        tokio::select! {
            result = self.shutdown_rx.as_mut()
                .expect("Shutdown channel not initialized")
                .recv() => {
                match result {
                    Ok(_) => info!("Shutdown signal received, initiating graceful shutdown..."),
                    Err(err) => error!("Error receiving shutdown signal: {}", err),
                }
                self.shutdown();
            }
        }
    }

    /// Gracefully shutdown the NameServer
    #[inline]
    fn shutdown(&mut self) {
        info!("Shutting down scheduled tasks...");
        self.scheduled_task_manager.cancel_all();

        info!("Shutting down route info manager...");
        self.inner
            .route_info_manager_mut()
            .shutdown_unregister_service();

        info!("RocketMQ NameServer (Rust) gracefully shutdown completed");
    }

    /// Initialize and register request processors
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

        // Register topic route query processor
        name_server_request_processor.register_processor(
            RequestCode::GetRouteinfoByTopic,
            NameServerRequestProcessorWrapper::ClientRequestProcessor(ArcMut::new(
                client_request_processor,
            )),
        );

        // Register default processor for other requests
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
        // Cleanup is handled in shutdown()
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

    /// Build the NameServerBootstrap with the configured settings
    ///
    /// Creates all necessary components:
    /// - RouteInfoManager (V1 or V2 based on configuration)
    /// - KVConfigManager for key-value storage
    /// - RemotingClient for broker communication
    /// - BrokerHousekeepingService for broker lifecycle management
    #[inline]
    pub fn build(self) -> NameServerBootstrap {
        let name_server_config = self.name_server_config.unwrap_or_default();
        let tokio_client_config = TokioClientConfig::default();
        let remoting_client = ArcMut::new(RocketmqDefaultClient::new(
            Arc::new(tokio_client_config.clone()),
            DefaultRemotingRequestProcessor,
        ));
        let server_config = self.server_config.unwrap_or_default();

        // Check configuration flag for RouteInfoManager version
        let use_v2 = name_server_config.use_route_info_manager_v2;

        let mut inner = ArcMut::new(NameServerRuntimeInner {
            name_server_config,
            tokio_client_config,
            server_config,
            route_info_manager: None,
            kvconfig_manager: None,
            remoting_client,
            broker_housekeeping_service: None,
        });

        // Select RouteInfoManager implementation based on configuration
        // V2 is the default (production-ready as of v0.7.0)
        let route_info_manager = if use_v2 {
            info!(
                "Using RouteInfoManager V2 (DashMap-based, 5-50x faster for concurrent operations)"
            );
            RouteInfoManagerWrapper::V2(RouteInfoManagerV2::new(inner.clone()))
        } else {
            warn!(
                "Using RouteInfoManager V1 (legacy RwLock-based implementation). Consider \
                 migrating to V2 for better performance."
            );
            RouteInfoManagerWrapper::V1(RouteInfoManager::new(inner.clone()))
        };

        let kv_config_manager = KVConfigManager::new(inner.clone());

        // Initialize components
        inner.kvconfig_manager = Some(kv_config_manager);
        inner.route_info_manager = Some(route_info_manager);
        inner.broker_housekeeping_service =
            Some(Arc::new(BrokerHousekeepingService::new(inner.clone())));

        NameServerBootstrap {
            name_server_runtime: NameServerRuntime {
                inner,
                scheduled_task_manager: ScheduledTaskManager::new(),
                shutdown_rx: None,
                server_inner: None,
            },
        }
    }
}

/// Internal runtime state shared across components
pub(crate) struct NameServerRuntimeInner {
    name_server_config: NamesrvConfig,
    tokio_client_config: TokioClientConfig,
    server_config: ServerConfig,
    route_info_manager: Option<RouteInfoManagerWrapper>,
    kvconfig_manager: Option<KVConfigManager>,
    remoting_client: ArcMut<RocketmqDefaultClient>,
    broker_housekeeping_service: Option<Arc<BrokerHousekeepingService>>,
}

impl NameServerRuntimeInner {
    // Mutable accessors

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
    pub fn route_info_manager_mut(&mut self) -> &mut RouteInfoManagerWrapper {
        self.route_info_manager
            .as_mut()
            .expect("RouteInfoManager not initialized - this is a bug in bootstrap logic")
    }

    #[inline]
    pub fn kvconfig_manager_mut(&mut self) -> &mut KVConfigManager {
        self.kvconfig_manager
            .as_mut()
            .expect("KVConfigManager not initialized - this is a bug in bootstrap logic")
    }

    #[inline]
    pub fn remoting_client_mut(&mut self) -> &mut RocketmqDefaultClient {
        &mut self.remoting_client
    }

    // Immutable accessors

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
    pub fn route_info_manager(&self) -> &RouteInfoManagerWrapper {
        self.route_info_manager
            .as_ref()
            .expect("RouteInfoManager not initialized - this is a bug in bootstrap logic")
    }

    #[inline]
    pub fn kvconfig_manager(&self) -> &KVConfigManager {
        self.kvconfig_manager
            .as_ref()
            .expect("KVConfigManager not initialized - this is a bug in bootstrap logic")
    }

    #[inline]
    pub fn remoting_client(&self) -> &RocketmqDefaultClient {
        &self.remoting_client
    }

    // Setters

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
    pub fn set_route_info_manager(&mut self, route_info_manager: RouteInfoManagerWrapper) {
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
