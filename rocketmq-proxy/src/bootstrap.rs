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

use futures::FutureExt;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ShutdownReason;
use std::future;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::auth::build_cluster_acl_signer;
use crate::auth::ProxyAuthRuntime;
use crate::cluster::ClusterClient;
use crate::cluster::ClusterRemotingBackend;
use crate::cluster::RocketmqClusterClient;
use crate::config::ProxyConfig;
use crate::config::ProxyMode;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::server;
use crate::grpc::ProxyGrpcService;
use crate::local::local_components_from_config_with_service_context;
use crate::local::LocalRemotingBackend;
use crate::observability::ProxyHookChain;
use crate::observability::ProxyMetrics;
use crate::processor::DefaultMessagingProcessor;
use crate::processor::MessagingProcessor;
use crate::remoting;
use crate::remoting::ProxyRemotingBackend;
use crate::service::ClusterServiceManager;
use crate::service::MetadataService;
use crate::service::ServiceManager;
use crate::session::ClientSessionRegistry;

#[derive(Clone)]
struct LifecycleReadiness {
    lifecycle: ServiceLifecycle,
    remaining_listeners: Arc<AtomicUsize>,
}

impl LifecycleReadiness {
    fn new(lifecycle: ServiceLifecycle, listener_count: usize) -> Self {
        Self {
            lifecycle,
            remaining_listeners: Arc::new(AtomicUsize::new(listener_count)),
        }
    }

    fn listener_bound(&self) -> ProxyResult<()> {
        let previous = self
            .remaining_listeners
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .map_err(|_| ProxyError::Transport {
                message: "Proxy listener readiness was published more than once".to_string(),
            })?;
        if previous == 1 {
            self.lifecycle.mark_ready().map_err(|error| ProxyError::Transport {
                message: format!("failed to publish Proxy readiness: {error}"),
            })?;
        }
        Ok(())
    }
}

fn publish_listener_ready(readiness: Option<LifecycleReadiness>) -> ProxyResult<()> {
    match readiness {
        Some(readiness) => readiness.listener_bound(),
        None => Ok(()),
    }
}

async fn verify_cluster_route_and_security(
    mode: ProxyMode,
    metadata_service: Option<&Arc<dyn MetadataService>>,
) -> ProxyResult<()> {
    if !matches!(mode, ProxyMode::Cluster) {
        return Ok(());
    }

    let metadata_service = metadata_service.ok_or_else(|| ProxyError::Transport {
        message: "Proxy Cluster readiness requires a metadata service".to_string(),
    })?;
    metadata_service.readiness_check().await?;
    Ok(())
}

fn require_healthy_grpc_shutdown(report: server::ProxyGrpcServerShutdownReport) -> ProxyResult<()> {
    if report.is_healthy() {
        Ok(())
    } else {
        Err(ProxyError::Transport {
            message: "Proxy gRPC shutdown report is unhealthy".to_string(),
        })
    }
}

fn require_healthy_remoting_shutdown(report: Option<rocketmq_runtime::ShutdownReport>) -> ProxyResult<()> {
    match report {
        Some(report) if report.is_healthy() => Ok(()),
        Some(report) => Err(ProxyError::Transport {
            message: format!("Proxy remoting shutdown report is unhealthy: {}", report.to_json()),
        }),
        None => Err(ProxyError::Transport {
            message: "Proxy remoting server stopped without a shutdown report".to_string(),
        }),
    }
}

pub struct ProxyRuntimeBuilder {
    config: ProxyConfig,
    service_manager: Option<Arc<dyn ServiceManager>>,
    session_registry: Option<ClientSessionRegistry>,
    auth_runtime: Option<ProxyAuthRuntime>,
    hooks: Option<ProxyHookChain>,
    metrics: Option<ProxyMetrics>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    service_context: Option<ChildServiceContext>,
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

    pub fn with_service_context(mut self, service_context: ChildServiceContext) -> Self {
        self.service_context = Some(service_context);
        self
    }

    pub fn build(self) -> ProxyResult<ProxyRuntime<DefaultMessagingProcessor>> {
        let service_context = self.service_context.clone().ok_or_else(|| ProxyError::Transport {
            message: "Proxy runtime requires an injected ChildServiceContext".to_string(),
        })?;
        Ok(self.build_inner(service_context))
    }

    fn build_inner(self, service_context: ChildServiceContext) -> ProxyRuntime<DefaultMessagingProcessor> {
        let local_mode_supported = true;
        let (service_manager, remoting_backend) = match self.service_manager {
            Some(service_manager) => (service_manager, self.remoting_backend),
            None => default_service_manager_and_backend(&self.config, &service_context),
        };
        let auth_metadata_service = Some(service_manager.metadata_service());
        let session_registry = self.session_registry.unwrap_or_default();
        let processor = Arc::new(DefaultMessagingProcessor::new(service_manager));
        ProxyRuntime::from_processor_with_local_mode_support(
            self.config,
            processor,
            session_registry,
            local_mode_supported,
            self.auth_runtime,
            auth_metadata_service,
            self.hooks.unwrap_or_default(),
            self.metrics.unwrap_or_default(),
            remoting_backend,
            service_context,
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
    auth_metadata_service: Option<Arc<dyn MetadataService>>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    service_context: ChildServiceContext,
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
            service_context: None,
        }
    }

    pub fn new(config: ProxyConfig, service_context: ChildServiceContext) -> ProxyResult<Self> {
        Self::builder(config).with_service_context(service_context).build()
    }
}

impl<P> ProxyRuntime<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn from_processor_with_context(
        config: ProxyConfig,
        processor: Arc<P>,
        session_registry: ClientSessionRegistry,
        service_context: ChildServiceContext,
    ) -> Self {
        Self::from_processor_with_local_mode_support(
            config,
            processor,
            session_registry,
            true,
            None,
            None,
            ProxyHookChain::default(),
            ProxyMetrics::default(),
            None,
            service_context,
        )
    }

    fn from_processor_with_local_mode_support(
        config: ProxyConfig,
        processor: Arc<P>,
        session_registry: ClientSessionRegistry,
        local_mode_supported: bool,
        auth_runtime: Option<ProxyAuthRuntime>,
        auth_metadata_service: Option<Arc<dyn MetadataService>>,
        hooks: ProxyHookChain,
        metrics: ProxyMetrics,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        service_context: ChildServiceContext,
    ) -> Self {
        #[cfg(feature = "observability")]
        crate::observability::init_observability_metrics(&config);

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
            auth_metadata_service,
            remoting_backend,
            service_context,
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
        self.serve_with_shutdown_and_lifecycle(shutdown, None).await
    }

    /// Serves until pre-stop or an operating-system signal requests the shared lifecycle deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed Proxy error when startup, readiness publication, serving, or shutdown fails.
    pub async fn serve_with_lifecycle(self, lifecycle: ServiceLifecycle) -> ProxyResult<()> {
        let shutdown_lifecycle = lifecycle.clone();
        self.serve_with_shutdown_and_lifecycle(
            async move {
                if let Err(error) = shutdown_lifecycle.wait_for_shutdown_signal().await {
                    tracing::warn!(error = %error, "Proxy signal observation failed");
                    shutdown_lifecycle.mark_failed();
                    shutdown_lifecycle.request_shutdown(ShutdownReason::Internal);
                }
            },
            Some(lifecycle),
        )
        .await
    }

    async fn serve_with_shutdown_and_lifecycle<F>(
        self,
        shutdown: F,
        lifecycle: Option<ServiceLifecycle>,
    ) -> ProxyResult<()>
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
            auth_metadata_service,
            remoting_backend,
            service_context,
        } = self;
        async move {
            if matches!(config.mode, ProxyMode::Local) && !local_mode_supported {
                return Err(crate::error::ProxyError::not_implemented(
                    "Local mode requires a broker-backed service manager and is not available in the default proxy \
                     runtime",
                ));
            }
            verify_cluster_route_and_security(config.mode, auth_metadata_service.as_ref()).await?;
            let auth_runtime = match auth_runtime {
                Some(auth_runtime) => Some(auth_runtime),
                None => {
                    ProxyAuthRuntime::from_proxy_config_with_metadata_service(
                        &config.auth,
                        auth_metadata_service,
                        &service_context.child("rocketmq-proxy.auth"),
                    )
                    .await?
                }
            };
            let auth_runtime_for_shutdown = auth_runtime.clone();
            let grpc_service = grpc_service.with_auth_runtime(auth_runtime.clone());
            let readiness = lifecycle
                .map(|lifecycle| LifecycleReadiness::new(lifecycle, if config.remoting.enabled { 2 } else { 1 }));
            if !config.remoting.enabled {
                let grpc_ready = readiness;
                let result = server::serve_with_report_with_task_group_and_ready(
                    config,
                    grpc_service,
                    shutdown,
                    service_context.task_group().clone(),
                    move || publish_listener_ready(grpc_ready),
                )
                .await
                .and_then(require_healthy_grpc_shutdown);
                let shutdown_result = match auth_runtime_for_shutdown {
                    Some(auth_runtime) => Some(auth_runtime.shutdown().await),
                    None => None,
                };
                result?;
                if let Some(shutdown_result) = shutdown_result {
                    shutdown_result?;
                }
                return Ok(());
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
            let grpc_parent_task_group = service_context.task_group().clone();
            let remoting_service_context = service_context.child("rocketmq-proxy.remoting");
            let grpc_config = config.clone();
            let remoting_config = config;
            let grpc_ready = readiness.clone();
            let remoting_ready = readiness;
            let grpc_future = async move {
                server::serve_with_report_with_task_group_and_ready(
                    grpc_config,
                    grpc_service,
                    grpc_shutdown,
                    grpc_parent_task_group,
                    move || publish_listener_ready(grpc_ready),
                )
                .await
                .and_then(require_healthy_grpc_shutdown)
            };
            let remoting_future = async move {
                remoting::serve_with_service_context_and_ready(
                    remoting_service_context,
                    remoting_config,
                    processor,
                    sessions,
                    auth_runtime,
                    remoting_backend,
                    remoting_shutdown,
                    move || publish_listener_ready(remoting_ready),
                )
                .await
                .and_then(require_healthy_remoting_shutdown)
            };
            let listener_result = tokio::try_join!(grpc_future, remoting_future);
            let shutdown_result = match auth_runtime_for_shutdown {
                Some(auth_runtime) => Some(auth_runtime.shutdown().await),
                None => None,
            };
            listener_result?;
            if let Some(shutdown_result) = shutdown_result {
                shutdown_result?;
            }
            Ok(())
        }
        .await
    }
}

fn default_service_manager_and_backend(
    config: &ProxyConfig,
    service_context: &ChildServiceContext,
) -> (Arc<dyn ServiceManager>, Option<Arc<dyn ProxyRemotingBackend>>) {
    match config.mode {
        ProxyMode::Cluster => {
            let mut cluster_config = config.cluster.clone();
            if !config.auth.cluster_name.trim().is_empty() {
                cluster_config.broker_cluster_name = config.auth.cluster_name.clone();
            }
            let signer = build_cluster_acl_signer(config).map(|signer| signer.into_outbound_signer());
            let cluster_context = service_context.child("rocketmq-proxy.cluster");
            let client = Arc::new(RocketmqClusterClient::with_service_context(
                cluster_config,
                signer,
                &cluster_context,
            ));
            let service_client: Arc<dyn ClusterClient> = client.clone();
            (
                Arc::new(ClusterServiceManager::from_cluster_client(service_client)),
                Some(Arc::new(ClusterRemotingBackend::new(client))),
            )
        }
        ProxyMode::Local => {
            let local_context = service_context.child("rocketmq-proxy.local");
            let (manager, client) = local_components_from_config_with_service_context(
                config.local.clone(),
                config.cluster.query_assignment_strategy_name.clone(),
                &local_context,
            );
            (Arc::new(manager), Some(Arc::new(LocalRemotingBackend::new(client))))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::ServiceLifecycle;
    use rocketmq_runtime::ServiceLifecycleConfig;
    use rocketmq_runtime::ServiceLifecycleState;

    use super::verify_cluster_route_and_security;
    use super::LifecycleReadiness;
    use super::ProxyRuntime;
    use super::ProxyRuntimeBuilder;
    use crate::config::ProxyConfig;
    use crate::config::ProxyMode;
    use crate::service::DefaultMetadataService;
    use crate::service::MetadataService;

    fn lifecycle() -> ServiceLifecycle {
        ServiceLifecycle::new(ServiceLifecycleConfig {
            service_name: Arc::from("proxy-readiness-test"),
            probe_bind_addr: None,
            shutdown_timeout: Duration::from_secs(45),
            liveness_stale_after: Duration::from_secs(30),
        })
    }

    #[test]
    fn lifecycle_readiness_waits_for_every_required_listener() {
        let lifecycle = lifecycle();
        let readiness = LifecycleReadiness::new(lifecycle.clone(), 2);

        readiness.listener_bound().expect("first listener binds");
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Starting);
        readiness.listener_bound().expect("second listener binds");
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Ready);
        assert!(readiness.listener_bound().is_err());
    }

    #[tokio::test]
    async fn cluster_readiness_requires_a_healthy_metadata_path() {
        assert!(
            verify_cluster_route_and_security(ProxyMode::Cluster, None)
                .await
                .is_err(),
            "Cluster mode must fail closed without a route and security metadata path"
        );

        let metadata: Arc<dyn MetadataService> = Arc::new(DefaultMetadataService);
        verify_cluster_route_and_security(ProxyMode::Cluster, Some(&metadata))
            .await
            .expect("healthy metadata path should satisfy the readiness preflight");
        verify_cluster_route_and_security(ProxyMode::Local, None)
            .await
            .expect("Local mode does not require a Cluster metadata preflight");
    }

    #[tokio::test]
    async fn default_local_mode_builds_broker_backed_runtime() {
        let runtime_context = RuntimeContext::from_current("proxy-local-runtime-test");
        let runtime = ProxyRuntimeBuilder::build(
            ProxyRuntime::builder(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            })
            .with_service_context(runtime_context.service_context("proxy-local-runtime")),
        )
        .expect("an injected child context should build the Local proxy runtime");

        assert!(matches!(runtime.config().mode, ProxyMode::Local));
    }

    #[tokio::test]
    async fn default_cluster_runtime_uses_injected_child_context() {
        let runtime_context = RuntimeContext::from_current("proxy-cluster-runtime-test");
        let runtime = ProxyRuntime::new(
            ProxyConfig::default(),
            runtime_context.service_context("proxy-cluster-runtime"),
        )
        .expect("an injected child context should build the Cluster proxy runtime");

        assert_eq!(runtime.service_context.name(), "proxy-cluster-runtime");

        drop(runtime);
        let report = runtime_context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn build_rejects_missing_child_context() {
        let error = ProxyRuntime::builder(ProxyConfig::default())
            .build()
            .err()
            .expect("library constructors must not discover or create a runtime");

        assert!(error.to_string().contains("requires an injected ChildServiceContext"));
    }
}
