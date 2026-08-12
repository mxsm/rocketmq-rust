// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
impl NameServerUpdateCallback for MQClientAPIImpl {
    fn on_name_server_address_changed(&self, namesrv_address: Option<String>) -> String {
        namesrv_address.unwrap_or_default()
    }
}

impl MQClientAPIImpl {
    pub fn new(
        tokio_client_config: Arc<TransportClientConfig>,
        client_remoting_processor: ClientRemotingProcessor,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        client_config: Arc<ClientConfig>,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        service_context: ChildServiceContext,
        telemetry_handle: rocketmq_observability::TelemetryHandle,
    ) -> Self {
        Self::init_remoting_version();

        let mut remoting_config = (*tokio_client_config).clone();
        remoting_config.tls = client_config.tls_config.clone();
        remoting_config.tls.enable = client_config.use_tls;
        #[cfg(any(feature = "observability", feature = "observability-metrics"))]
        let transport_telemetry = TransportTelemetry::from_handle(&telemetry_handle);
        #[cfg(not(any(feature = "observability", feature = "observability-metrics")))]
        let transport_telemetry = {
            let _ = telemetry_handle;
            TransportTelemetry::noop()
        };
        let mut builder = RemotingClient::builder(
            Arc::new(remoting_config),
            client_remoting_processor,
            service_context.component("transport"),
        )
        .telemetry(transport_telemetry);
        if let Some(tx) = tx {
            builder = builder.connection_events(tx);
        }
        let default_client = builder.build().expect("clamped transport client budgets must be valid");
        if let Some(hook) = rpc_hook {
            default_client.register_rpc_hook(hook);
        }

        MQClientAPIImpl {
            service_context,
            remoting_client: Arc::new(default_client),
            top_addressing: RwLock::new(Arc::new(DefaultTopAddressing::new(
                mix_all::get_ws_addr().into(),
                client_config.unit_name.clone(),
            ))),
            //client_remoting_processor,
            name_srv_addr: RwLock::new(None),
            client_config,
            background_tasks: TaskTracker::new(),
            background_shutdown: CancellationToken::new(),
        }
    }

    pub async fn start(&self) {
        if let Err(error) = self.remoting_client.start().await {
            tracing::error!(?error, "failed to start transport client");
        }
    }

    pub fn shutdown(&self) {
        self.background_shutdown.cancel();
        self.background_tasks.close();
        self.remoting_client.shutdown();
    }

    pub async fn shutdown_background_tasks(&self, timeout: Duration) -> bool {
        self.background_shutdown.cancel();
        self.background_tasks.close();
        tokio::time::timeout(timeout, self.background_tasks.wait())
            .await
            .is_ok()
    }

    pub fn get_remoting_client(&self) -> Arc<RemotingClient<ClientRemotingProcessor>> {
        self.remoting_client.clone()
    }

    #[inline]
    pub(crate) fn is_use_tls(&self) -> bool {
        self.remoting_client.is_use_tls()
    }

    pub async fn fetch_name_server_addr(&self) -> Option<String> {
        let top_addressing = self.top_addressing.read().await.clone();
        let addrs = top_addressing.fetch_ns_addr().await;

        if let Some(addrs) = addrs.as_ref().filter(|addr| !addr.is_empty()) {
            if update_cached_name_server_addr(&self.name_srv_addr, addrs, |addrs| {
                self.update_name_server_address_list_sync(addrs);
            })
            .await
            {
                return Some(addrs.clone());
            }
        }
        self.name_srv_addr.read().await.clone()
    }

    pub async fn on_name_server_address_change(&self, namesrv_address: Option<String>) -> String {
        if let Some(addrs) = namesrv_address.as_ref().filter(|addr| !addr.is_empty()) {
            if update_cached_name_server_addr(&self.name_srv_addr, addrs, |addrs| {
                self.update_name_server_address_list_sync(addrs);
            })
            .await
            {
                return addrs.clone();
            }
        }
        self.name_srv_addr.read().await.clone().unwrap_or_default()
    }

    pub async fn update_name_server_address_list(&self, addrs: &str) {
        self.update_name_server_address_list_sync(addrs);
    }

    pub(crate) fn update_name_server_address_list_sync(&self, addrs: &str) {
        let addr_vec = addrs
            .split(";")
            .map(CheetahString::from_slice)
            .collect::<Vec<CheetahString>>();
        self.update_name_server_targets_sync(addr_vec);
    }

    pub(crate) fn update_name_server_targets_sync(&self, addr_vec: Vec<CheetahString>) {
        self.remoting_client.update_name_server_address_list_sync(addr_vec);
    }

    pub(crate) fn update_name_server_connect_targets_sync(&self, targets: Vec<ConnectTarget>, drain_timeout: Duration) {
        self.remoting_client
            .update_name_server_connect_targets_sync(targets, drain_timeout);
    }

    pub(crate) async fn configure_top_addressing(&self, domain: &str, subgroup: &str) {
        let addressing = Arc::new(DefaultTopAddressing::from_domain_and_subgroup(
            domain,
            subgroup,
            self.client_config.unit_name.clone(),
        ));
        *self.top_addressing.write().await = addressing;
    }

    pub async fn invoke(
        &self,
        broker_addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<RemotingCommand> {
        self.remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await
    }

    pub async fn invoke_oneway(
        &self,
        broker_addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.remoting_client
            .invoke_request_oneway(broker_addr, request, timeout_millis)
            .await
    }
}
