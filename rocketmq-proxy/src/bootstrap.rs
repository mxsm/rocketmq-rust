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

use crate::auth::ProxyAuthRuntime;
use crate::config::ProxyConfig;
use crate::config::ProxyMode;
use crate::error::ProxyResult;
use crate::grpc::server;
use crate::grpc::ProxyGrpcService;
use crate::observability::ProxyHookChain;
use crate::observability::ProxyMetrics;
use crate::processor::DefaultMessagingProcessor;
use crate::processor::MessagingProcessor;
use crate::service::ClusterServiceManager;
use crate::service::LocalServiceManager;
use crate::service::ServiceManager;
use crate::session::ClientSessionRegistry;

pub struct ProxyRuntimeBuilder {
    config: ProxyConfig,
    service_manager: Option<Arc<dyn ServiceManager>>,
    session_registry: Option<ClientSessionRegistry>,
    auth_runtime: Option<ProxyAuthRuntime>,
    hooks: Option<ProxyHookChain>,
    metrics: Option<ProxyMetrics>,
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

    pub fn build(self) -> ProxyRuntime<DefaultMessagingProcessor> {
        let local_mode_supported = true;
        let service_manager = self
            .service_manager
            .unwrap_or_else(|| default_service_manager(&self.config));
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
        )
    }
}

pub struct ProxyRuntime<P = DefaultMessagingProcessor> {
    config: Arc<ProxyConfig>,
    grpc_service: ProxyGrpcService<P>,
    local_mode_supported: bool,
    auth_runtime: Option<ProxyAuthRuntime>,
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
    ) -> Self {
        let config = Arc::new(config);
        let grpc_service = ProxyGrpcService::new(Arc::clone(&config), processor, session_registry)
            .with_hooks(hooks)
            .with_metrics(metrics);
        Self {
            config,
            grpc_service,
            local_mode_supported,
            auth_runtime,
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
            grpc_service,
            local_mode_supported,
            auth_runtime,
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
        server::serve(config, grpc_service.with_auth_runtime(auth_runtime), shutdown).await
    }
}

fn default_service_manager(config: &ProxyConfig) -> Arc<dyn ServiceManager> {
    match config.mode {
        ProxyMode::Cluster => Arc::new(ClusterServiceManager::from_cluster_config(config.cluster.clone())),
        ProxyMode::Local => Arc::new(LocalServiceManager::from_local_config(
            config.local.clone(),
            config.cluster.query_assignment_strategy_name.clone(),
        )),
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
