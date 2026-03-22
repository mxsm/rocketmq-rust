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

use crate::config::ProxyConfig;
use crate::config::ProxyMode;
use crate::error::ProxyResult;
use crate::grpc::server;
use crate::grpc::ProxyGrpcService;
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

    pub fn build(self) -> ProxyRuntime<DefaultMessagingProcessor> {
        let local_mode_supported = !(matches!(self.config.mode, ProxyMode::Local) && self.service_manager.is_none());
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
        )
    }
}

pub struct ProxyRuntime<P = DefaultMessagingProcessor> {
    config: Arc<ProxyConfig>,
    grpc_service: ProxyGrpcService<P>,
    local_mode_supported: bool,
}

impl ProxyRuntime<DefaultMessagingProcessor> {
    pub fn builder(config: ProxyConfig) -> ProxyRuntimeBuilder {
        ProxyRuntimeBuilder {
            config,
            service_manager: None,
            session_registry: None,
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
        Self::from_processor_with_local_mode_support(config, processor, session_registry, true)
    }

    fn from_processor_with_local_mode_support(
        config: ProxyConfig,
        processor: Arc<P>,
        session_registry: ClientSessionRegistry,
        local_mode_supported: bool,
    ) -> Self {
        let config = Arc::new(config);
        let grpc_service = ProxyGrpcService::new(Arc::clone(&config), processor, session_registry);
        Self {
            config,
            grpc_service,
            local_mode_supported,
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
        if matches!(self.config.mode, ProxyMode::Local) && !self.local_mode_supported {
            return Err(crate::error::ProxyError::not_implemented(
                "Local mode requires a broker-backed service manager and is not available in the default proxy runtime",
            ));
        }
        server::serve(self.config, self.grpc_service, shutdown).await
    }
}

fn default_service_manager(config: &ProxyConfig) -> Arc<dyn ServiceManager> {
    match config.mode {
        ProxyMode::Cluster => Arc::new(ClusterServiceManager::from_cluster_config(config.cluster.clone())),
        ProxyMode::Local => Arc::new(LocalServiceManager::default()),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::GrpcConfig;

    use super::ProxyRuntime;
    use super::ProxyRuntimeBuilder;
    use crate::config::ProxyConfig;
    use crate::config::ProxyMode;
    use crate::error::ProxyError;

    #[tokio::test]
    async fn default_local_mode_fails_fast_before_server_start() {
        let runtime = ProxyRuntimeBuilder::build(ProxyRuntime::builder(ProxyConfig {
            mode: ProxyMode::Local,
            grpc: GrpcConfig {
                listen_addr: "127.0.0.1:0".to_owned(),
                ..GrpcConfig::default()
            },
            ..ProxyConfig::default()
        }));

        let error = runtime
            .serve_with_shutdown(async {})
            .await
            .expect_err("default local mode must fail fast");

        assert!(matches!(error, ProxyError::NotImplemented { .. }));
    }
}
