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

use std::future;
use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;

use crate::auth::ProxyAuthRuntime;
use crate::config::ProxyConfig;
use crate::config::ProxyMode;
use crate::error::ProxyResult;
use crate::grpc::server;
use crate::grpc::ProxyGrpcService;
use crate::local::local_components_from_config;
use crate::local::LocalRemotingBackend;
use crate::observability::ProxyHookChain;
use crate::observability::ProxyMetrics;
use crate::processor::DefaultMessagingProcessor;
use crate::processor::MessagingProcessor;
use crate::remoting;
use crate::remoting::ProxyRemotingBackend;
use crate::service::ClusterServiceManager;
use crate::service::ServiceManager;
use crate::session::ClientSessionRegistry;

pub struct ProxyRuntimeBuilder {
    config: ProxyConfig,
    service_manager: Option<Arc<dyn ServiceManager>>,
    session_registry: Option<ClientSessionRegistry>,
    auth_runtime: Option<ProxyAuthRuntime>,
    hooks: Option<ProxyHookChain>,
    metrics: Option<ProxyMetrics>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
}

impl ProxyRuntimeBuilder {
    pub fn with_service_manager(mut self, service_manager: Arc<dyn ServiceManager>) -> Self {
        self.service_manager = Some(service_manager);
        self
    }

    pub fn with_session_registry(mut self, session_registry: ClientSessionRegistry) -> Self {
        self.session_registry = Some(session_registry);
        self
    }

    pub fn with_auth_runtime(mut self, auth_runtime: ProxyAuthRuntime) -> Self {
        self.auth_runtime = Some(auth_runtime);
        self
    }

    pub fn with_hooks(mut self, hooks: ProxyHookChain) -> Self {
        self.hooks = Some(hooks);
        self
    }

    pub fn with_metrics(mut self, metrics: ProxyMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_remoting_backend(mut self, remoting_backend: Arc<dyn ProxyRemotingBackend>) -> Self {
        self.remoting_backend = Some(remoting_backend);
        self
    }

    pub fn build(self) -> ProxyRuntime<DefaultMessagingProcessor> {
        let local_mode_supported = true;
        let (service_manager, remoting_backend) = match self.service_manager {
            Some(service_manager) => (service_manager, self.remoting_backend),
            None => default_service_manager_and_backend(&self.config),
        };
        let session_registry = self.session_registry.unwrap_or_default();
        let processor = Arc::new(DefaultMessagingProcessor::new(service_manager));
        ProxyRuntime::from_processor_with_local_mode_support(
            self.config,
            processor,
            session_registry,
            local_mode_supported,
            self.auth_runtime,
            self.hooks.unwrap_or_default(),
            self.metrics.unwrap_or_default(),
            remoting_backend,
        )
    }
}

pub struct ProxyRuntime<P = DefaultMessagingProcessor> {
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    grpc_service: ProxyGrpcService<P>,
    local_mode_supported: bool,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
}

impl ProxyRuntime<DefaultMessagingProcessor> {
    pub fn builder(config: ProxyConfig) -> ProxyRuntimeBuilder {
        ProxyRuntimeBuilder {
            config,
            service_manager: None,
            session_registry: None,
            auth_runtime: None,
            hooks: None,
            metrics: None,
            remoting_backend: None,
        }
    }

    pub fn new(config: ProxyConfig) -> Self {
        Self::builder(config).build()
    }
}

impl<P> ProxyRuntime<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn from_processor(config: ProxyConfig, processor: Arc<P>, session_registry: ClientSessionRegistry) -> Self {
        Self::from_processor_with_local_mode_support(
            config,
            processor,
            session_registry,
            true,
            None,
            ProxyHookChain::default(),
            ProxyMetrics::default(),
            None,
        )
    }

    fn from_processor_with_local_mode_support(
        config: ProxyConfig,
        processor: Arc<P>,
        session_registry: ClientSessionRegistry,
        local_mode_supported: bool,
        auth_runtime: Option<ProxyAuthRuntime>,
        hooks: ProxyHookChain,
        metrics: ProxyMetrics,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    ) -> Self {
        let config = Arc::new(config);
        let processor_ref = Arc::clone(&processor);
        let sessions = session_registry.clone();
        let grpc_service = ProxyGrpcService::new(Arc::clone(&config), processor, session_registry)
            .with_hooks(hooks)
            .with_metrics(metrics);
        Self {
            config,
            processor: processor_ref,
            sessions,
            grpc_service,
            local_mode_supported,
            auth_runtime,
            remoting_backend,
        }
    }

    pub fn config(&self) -> &ProxyConfig {
        self.config.as_ref()
    }

    pub async fn serve(self) -> ProxyResult<()> {
        self.serve_with_shutdown(future::pending::<()>()).await
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> ProxyResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let ProxyRuntime {
            config,
            processor,
            sessions,
            grpc_service,
            local_mode_supported,
            auth_runtime,
            remoting_backend,
        } = self;
        if matches!(config.mode, ProxyMode::Local) && !local_mode_supported {
            return Err(crate::error::ProxyError::not_implemented(
                "Local mode requires a broker-backed service manager and is not available in the default proxy runtime",
            ));
        }
        let auth_runtime = match auth_runtime {
            Some(auth_runtime) => Some(auth_runtime),
            None => ProxyAuthRuntime::from_proxy_config(&config.auth).await?,
        };
        let grpc_service = grpc_service.with_auth_runtime(auth_runtime.clone());
        if !config.remoting.enabled {
            return server::serve(config, grpc_service, shutdown).await;
        }

        let shared_shutdown = shutdown.boxed().shared();
        let grpc_shutdown = {
            let shared_shutdown = shared_shutdown.clone();
            async move {
                shared_shutdown.await;
            }
        };
        let remoting_shutdown = async move {
            shared_shutdown.await;
        };
        let grpc_future = server::serve(config.clone(), grpc_service, grpc_shutdown);
        let remoting_future = remoting::serve(
            config,
            processor,
            sessions,
            auth_runtime,
            remoting_backend,
            remoting_shutdown,
        );
        let (grpc_result, remoting_result) = tokio::join!(grpc_future, remoting_future);
        grpc_result?;
        remoting_result
    }
}

fn default_service_manager_and_backend(
    config: &ProxyConfig,
) -> (Arc<dyn ServiceManager>, Option<Arc<dyn ProxyRemotingBackend>>) {
    match config.mode {
        ProxyMode::Cluster => (
            Arc::new(ClusterServiceManager::from_cluster_config(config.cluster.clone())),
            None,
        ),
        ProxyMode::Local => {
            let (manager, client) = local_components_from_config(
                config.local.clone(),
                config.cluster.query_assignment_strategy_name.clone(),
            );
            (Arc::new(manager), Some(Arc::new(LocalRemotingBackend::new(client))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProxyRuntime;
    use super::ProxyRuntimeBuilder;
    use crate::config::ProxyConfig;
    use crate::config::ProxyMode;

    #[tokio::test]
    async fn default_local_mode_builds_broker_backed_runtime() {
        let runtime = ProxyRuntimeBuilder::build(ProxyRuntime::builder(ProxyConfig {
            mode: ProxyMode::Local,
            ..ProxyConfig::default()
        }));

        assert!(matches!(runtime.config().mode, ProxyMode::Local));
    }
}
