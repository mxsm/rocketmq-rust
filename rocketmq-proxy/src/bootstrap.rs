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
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReason;
use rocketmq_runtime::ShutdownReport;
use std::future;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "cluster-mode")]
use crate::auth::build_cluster_acl_signer;
use crate::auth::ProxyAuthRuntime;
#[cfg(feature = "cluster-mode")]
use crate::cluster::ClusterClient;
#[cfg(feature = "cluster-mode")]
use crate::cluster::ClusterRemotingBackend;
#[cfg(feature = "cluster-mode")]
use crate::cluster::RocketmqClusterClient;
use crate::config::ProxyConfig;
use crate::config::ProxyMode;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::server;
use crate::grpc::ProxyGrpcService;
#[cfg(feature = "local-mode")]
use crate::local::local_components_from_config_with_service_context;
#[cfg(feature = "local-mode")]
use crate::local::LocalRemotingBackend;
use crate::observability::ProxyHookChain;
use crate::observability::ProxyMetrics;
use crate::processor::DefaultMessagingProcessor;
use crate::processor::MessagingProcessor;
use crate::remoting;
use crate::remoting::ProxyRemotingBackend;
#[cfg(feature = "cluster-mode")]
use crate::service::ClusterServiceManager;
use crate::service::MetadataService;
use crate::service::ServiceManager;
use crate::session::ClientSessionRegistry;

#[derive(Clone)]
struct LifecycleReadiness {
    lifecycle: ServiceLifecycle,
    remaining_listeners: Arc<AtomicUsize>,
}

struct DefaultBackend {
    service_manager: Arc<dyn ServiceManager>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    context: Option<ChildServiceContext>,
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
    telemetry: rocketmq_observability::TelemetryHandle,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    service_context: ChildServiceContext,
}

fn require_healthy_component_shutdown(component: &'static str, report: ShutdownReport) -> ProxyResult<()> {
    if report.is_healthy() {
        Ok(())
    } else {
        Err(ProxyError::Transport {
            message: format!("{component} shutdown report is unhealthy: {}", report.to_json()),
        })
    }
}

impl ProxyRuntimeBuilder {
    fn new(
        config: ProxyConfig,
        service_context: ChildServiceContext,
        telemetry: rocketmq_observability::TelemetryHandle,
    ) -> Self {
        Self {
            config,
            service_manager: None,
            session_registry: None,
            auth_runtime: None,
            hooks: None,
            metrics: None,
            telemetry,
            remoting_backend: None,
            service_context,
        }
    }

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

    pub fn build(self) -> ProxyResult<ProxyRuntime<DefaultMessagingProcessor>> {
        self.build_inner()
    }

    fn build_inner(self) -> ProxyResult<ProxyRuntime<DefaultMessagingProcessor>> {
        let grpc_guards = ProxyGrpcService::<DefaultMessagingProcessor>::try_execution_guards(&self.config)?;
        let service_context = self.service_context.clone();
        let telemetry = self.telemetry.clone();
        let local_mode_supported = true;
        let metrics = self
            .metrics
            .unwrap_or_else(|| ProxyMetrics::from_telemetry(&telemetry, &self.config));
        #[cfg(any(feature = "observability", feature = "otel-traces"))]
        let transport_telemetry = rocketmq_transport::TransportTelemetry::from_handle(&telemetry);
        #[cfg(not(any(feature = "observability", feature = "otel-traces")))]
        let transport_telemetry = rocketmq_transport::TransportTelemetry::noop();
        let backend = match self.service_manager {
            Some(service_manager) => DefaultBackend {
                service_manager,
                remoting_backend: self.remoting_backend,
                context: None,
            },
            None => default_service_manager_and_backend(&self.config, &service_context, telemetry)?,
        };
        let auth_metadata_service = Some(backend.service_manager.metadata_service());
        let session_registry = self.session_registry.unwrap_or_default();
        let processor = Arc::new(DefaultMessagingProcessor::new(backend.service_manager));
        Ok(ProxyRuntime::from_processor_with_local_mode_support_and_guards(
            self.config,
            processor,
            session_registry,
            local_mode_supported,
            self.auth_runtime,
            auth_metadata_service,
            self.hooks.unwrap_or_default(),
            metrics,
            transport_telemetry,
            grpc_guards,
            backend.remoting_backend,
            backend.context,
            service_context,
        ))
    }
}

pub struct ProxyRuntime<P = DefaultMessagingProcessor> {
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    grpc_service: ProxyGrpcService<P>,
    drain: rocketmq_proxy_core::ProxyDrainController,
    local_mode_supported: bool,
    auth_runtime: Option<ProxyAuthRuntime>,
    auth_metadata_service: Option<Arc<dyn MetadataService>>,
    transport_telemetry: rocketmq_transport::TransportTelemetry,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    backend_context: Option<ChildServiceContext>,
    service_context: ChildServiceContext,
}

impl ProxyRuntime<DefaultMessagingProcessor> {
    pub fn builder(
        config: ProxyConfig,
        service_context: ChildServiceContext,
        telemetry: rocketmq_observability::TelemetryHandle,
    ) -> ProxyRuntimeBuilder {
        ProxyRuntimeBuilder::new(config, service_context, telemetry)
    }

    pub fn new(
        config: ProxyConfig,
        service_context: ChildServiceContext,
        telemetry: rocketmq_observability::TelemetryHandle,
    ) -> ProxyResult<Self> {
        Self::builder(config, service_context, telemetry).build()
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
        telemetry: rocketmq_observability::TelemetryHandle,
    ) -> ProxyResult<Self> {
        let metrics = ProxyMetrics::from_telemetry(&telemetry, &config);
        #[cfg(any(feature = "observability", feature = "otel-traces"))]
        let transport_telemetry = rocketmq_transport::TransportTelemetry::from_handle(&telemetry);
        #[cfg(not(any(feature = "observability", feature = "otel-traces")))]
        let transport_telemetry = rocketmq_transport::TransportTelemetry::noop();
        Self::from_processor_with_local_mode_support(
            config,
            processor,
            session_registry,
            true,
            None,
            None,
            ProxyHookChain::default(),
            metrics,
            transport_telemetry,
            None,
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
        transport_telemetry: rocketmq_transport::TransportTelemetry,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        backend_context: Option<ChildServiceContext>,
        service_context: ChildServiceContext,
    ) -> ProxyResult<Self> {
        let grpc_guards = ProxyGrpcService::<P>::try_execution_guards(&config)?;
        Ok(Self::from_processor_with_local_mode_support_and_guards(
            config,
            processor,
            session_registry,
            local_mode_supported,
            auth_runtime,
            auth_metadata_service,
            hooks,
            metrics,
            transport_telemetry,
            grpc_guards,
            remoting_backend,
            backend_context,
            service_context,
        ))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "assembles the validated Proxy runtime from independently owned production components"
    )]
    fn from_processor_with_local_mode_support_and_guards(
        config: ProxyConfig,
        processor: Arc<P>,
        session_registry: ClientSessionRegistry,
        local_mode_supported: bool,
        auth_runtime: Option<ProxyAuthRuntime>,
        auth_metadata_service: Option<Arc<dyn MetadataService>>,
        hooks: ProxyHookChain,
        metrics: ProxyMetrics,
        transport_telemetry: rocketmq_transport::TransportTelemetry,
        grpc_guards: rocketmq_proxy_core::ingress::grpc::service::ExecutionGuards,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        backend_context: Option<ChildServiceContext>,
        service_context: ChildServiceContext,
    ) -> Self {
        let config = Arc::new(config);
        let processor_ref = Arc::clone(&processor);
        let sessions = session_registry.clone();
        let drain = rocketmq_proxy_core::ProxyDrainController::default();
        let grpc_service =
            ProxyGrpcService::from_execution_guards(Arc::clone(&config), processor, session_registry, grpc_guards)
                .with_drain_controller(drain.clone())
                .with_hooks(hooks)
                .with_metrics(metrics);
        Self {
            config,
            processor: processor_ref,
            sessions,
            grpc_service,
            drain,
            local_mode_supported,
            auth_runtime,
            auth_metadata_service,
            transport_telemetry,
            remoting_backend,
            backend_context,
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
        self.serve_with_shutdown_and_lifecycle(
            async move {
                shutdown.await;
                ShutdownDeadline::after(Duration::from_secs(10))
            },
            None,
        )
        .await
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
                match shutdown_lifecycle.wait_for_shutdown_signal().await {
                    Ok(request) => request.deadline,
                    Err(error) => {
                        tracing::warn!(error = %error, "Proxy signal observation failed");
                        shutdown_lifecycle.mark_failed();
                        shutdown_lifecycle.request_shutdown(ShutdownReason::Internal).deadline
                    }
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
        F: Future<Output = ShutdownDeadline> + Send + 'static,
    {
        let ProxyRuntime {
            config,
            processor,
            sessions,
            grpc_service,
            drain,
            local_mode_supported,
            auth_runtime,
            auth_metadata_service,
            transport_telemetry,
            remoting_backend,
            backend_context,
            service_context,
        } = self;
        let auth_context = service_context.child("auth");
        let mut auth_runtime = auth_runtime;
        let lifecycle_for_shutdown = lifecycle.clone();
        let shared_shutdown = shutdown.boxed().shared();
        let listener_result = async {
            if matches!(config.mode, ProxyMode::Local) && !local_mode_supported {
                return Err(crate::error::ProxyError::not_implemented(
                    "Local mode requires a broker-backed service manager and is not available in the default proxy \
                     runtime",
                ));
            }
            verify_cluster_route_and_security(config.mode, auth_metadata_service.as_ref()).await?;
            if let Some(lifecycle) = lifecycle.as_ref() {
                drain
                    .attach_lifecycle(lifecycle.clone())
                    .map_err(|error| ProxyError::Transport {
                        message: format!("failed to attach Proxy drain lifecycle: {error}"),
                    })?;
            }
            let auth_context = service_context.child("auth");
            let auth_runtime = match auth_runtime {
                Some(auth_runtime) => Some(auth_runtime),
                None => {
                    ProxyAuthRuntime::from_proxy_config_with_metadata_service(
                        &config.auth,
                        auth_metadata_service,
                        &auth_context,
                    )
                    .await?
                }
            };
            let auth_runtime_for_shutdown = auth_runtime.clone();
            let grpc_service = grpc_service.with_auth_runtime(auth_runtime.clone());
            let readiness = lifecycle
                .map(|lifecycle| LifecycleReadiness::new(lifecycle, if config.remoting.enabled { 2 } else { 1 }));
            let serve_result = if !config.remoting.enabled {
                let grpc_ready = readiness;
                let grpc_context = service_context.child("grpc-ingress");
                server::serve_with_report_with_task_group_and_ready(
                    config,
                    grpc_service,
                    shared_shutdown.clone(),
                    grpc_context.task_group().clone(),
                    move || publish_listener_ready(grpc_ready),
                )
                .await
                .and_then(require_healthy_grpc_shutdown)
            } else {
                let grpc_shutdown = shared_shutdown.clone();
                let remoting_shutdown = {
                    let shared_shutdown = shared_shutdown.clone();
                    async move {
                        let _ = shared_shutdown.await;
                    }
                };
                let grpc_parent_task_group = service_context.child("grpc-ingress").task_group().clone();
                let remoting_service_context = service_context.child("remoting-ingress");
                let grpc_config = config.clone();
                let remoting_config = config;
                let remoting_auth_runtime = auth_runtime.clone();
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
                    remoting::serve_with_service_context_and_ready_and_drain(
                        remoting_service_context,
                        transport_telemetry,
                        remoting_config,
                        processor,
                        sessions,
                        remoting_auth_runtime,
                        remoting_backend,
                        drain,
                        remoting_shutdown,
                        move || publish_listener_ready(remoting_ready),
                    )
                    .await
                    .and_then(require_healthy_remoting_shutdown)
                };
                tokio::try_join!(grpc_future, remoting_future).map(|_| ())
            };
            auth_runtime = auth_runtime_for_shutdown;
            serve_result
        }
        .await;
        let deadline = resolve_shutdown_deadline(&shared_shutdown, lifecycle_for_shutdown.as_ref());
        finalize_proxy_run(
            listener_result,
            backend_context.as_ref(),
            auth_runtime.as_ref(),
            &auth_context,
            &service_context,
            deadline,
        )
        .await
    }
}

fn resolve_shutdown_deadline(
    shutdown: &futures::future::Shared<futures::future::BoxFuture<'static, ShutdownDeadline>>,
    lifecycle: Option<&ServiceLifecycle>,
) -> ShutdownDeadline {
    shutdown.clone().now_or_never().unwrap_or_else(|| {
        lifecycle.map_or_else(
            || ShutdownDeadline::after(Duration::from_secs(10)),
            |lifecycle| lifecycle.request_shutdown(ShutdownReason::Internal).deadline,
        )
    })
}

async fn finalize_proxy_run(
    primary_result: ProxyResult<()>,
    backend_context: Option<&ChildServiceContext>,
    auth_runtime: Option<&ProxyAuthRuntime>,
    auth_context: &ChildServiceContext,
    service_context: &ChildServiceContext,
    deadline: ShutdownDeadline,
) -> ProxyResult<()> {
    let cleanup_result =
        shutdown_proxy_components(backend_context, auth_runtime, auth_context, service_context, deadline).await;
    match (primary_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => {
            tracing::error!(
                primary_error = %primary,
                cleanup_error = %cleanup,
                "Proxy failed and its transactional cleanup was also unhealthy"
            );
            Err(ProxyError::Transport {
                message: format!("Proxy startup or serving failed: {primary}; transactional cleanup failed: {cleanup}"),
            })
        }
    }
}

async fn shutdown_proxy_components(
    backend_context: Option<&ChildServiceContext>,
    auth_runtime: Option<&ProxyAuthRuntime>,
    auth_context: &ChildServiceContext,
    service_context: &ChildServiceContext,
    deadline: ShutdownDeadline,
) -> ProxyResult<()> {
    let mut failures = Vec::new();

    if let Some(backend_context) = backend_context {
        let report = backend_context.task_group().shutdown_until(deadline).await;
        if let Err(error) = require_healthy_component_shutdown("Proxy backend", report) {
            failures.push(error.to_string());
        }
    }

    if let Some(auth_runtime) = auth_runtime {
        match tokio::time::timeout(deadline.remaining(), auth_runtime.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("Proxy authentication runtime shutdown failed: {error}")),
            Err(_) => failures.push("Proxy authentication runtime shutdown exceeded the shared deadline".to_owned()),
        }
    }

    let auth_report = auth_context.task_group().shutdown_until(deadline).await;
    if let Err(error) = require_healthy_component_shutdown("Proxy authentication", auth_report) {
        failures.push(error.to_string());
    }

    let report = service_context.task_group().shutdown_until(deadline).await;
    if let Err(error) = require_healthy_component_shutdown("Proxy service", report) {
        failures.push(error.to_string());
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(ProxyError::Transport {
            message: format!("Proxy component shutdown failures: {}", failures.join("; ")),
        })
    }
}

fn default_service_manager_and_backend(
    config: &ProxyConfig,
    service_context: &ChildServiceContext,
    telemetry_handle: rocketmq_observability::TelemetryHandle,
) -> ProxyResult<DefaultBackend> {
    match config.mode {
        #[cfg(feature = "cluster-mode")]
        ProxyMode::Cluster => {
            let mut cluster_config = config.cluster.clone();
            if !config.auth.cluster_name.trim().is_empty() {
                cluster_config.broker_cluster_name = config.auth.cluster_name.clone();
            }
            let signer = build_cluster_acl_signer(config).map(|signer| signer.into_outbound_signer());
            let cluster_context = service_context.child("cluster-backend");
            let client = Arc::new(RocketmqClusterClient::new(
                cluster_config,
                signer,
                &cluster_context,
                telemetry_handle,
            )?);
            let service_client: Arc<dyn ClusterClient> = client.clone();
            Ok(DefaultBackend {
                service_manager: Arc::new(ClusterServiceManager::from_cluster_client(service_client)),
                remoting_backend: Some(Arc::new(ClusterRemotingBackend::new(client))),
                context: Some(cluster_context),
            })
        }
        #[cfg(not(feature = "cluster-mode"))]
        ProxyMode::Cluster => Err(ProxyError::not_implemented(
            "Cluster mode is unavailable because the 'cluster-mode' feature is disabled",
        )),
        #[cfg(feature = "local-mode")]
        ProxyMode::Local => {
            let local_context = service_context.child("local-backend");
            let (manager, client) = local_components_from_config_with_service_context(
                config.local.clone(),
                config.local.query_assignment_strategy_name.clone(),
                &local_context,
                telemetry_handle,
            )?;
            Ok(DefaultBackend {
                service_manager: Arc::new(manager),
                remoting_backend: Some(Arc::new(LocalRemotingBackend::new(client))),
                context: Some(local_context),
            })
        }
        #[cfg(not(feature = "local-mode"))]
        ProxyMode::Local => Err(ProxyError::not_implemented(
            "Local mode is unavailable because the 'local-mode' feature is disabled",
        )),
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

    use super::finalize_proxy_run;
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

    #[test]
    fn lifecycle_readiness_fails_after_shutdown_has_started() {
        let lifecycle = lifecycle();
        let readiness = LifecycleReadiness::new(lifecycle.clone(), 1);
        lifecycle.request_shutdown(rocketmq_runtime::ShutdownReason::Internal);

        let error = readiness
            .listener_bound()
            .expect_err("readiness must fail closed once draining starts");

        assert!(error.to_string().contains("failed to publish Proxy readiness"));
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Draining);
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
    async fn invalid_grpc_budget_fails_before_default_backend_tasks_start() {
        let runtime_context = RuntimeContext::from_current("proxy-invalid-budget-test");
        let service_context = runtime_context.service_context("proxy-invalid-budget");
        let mut config = ProxyConfig {
            mode: ProxyMode::Local,
            ..ProxyConfig::default()
        };
        config.runtime.route_permits = 0;

        let result = ProxyRuntime::builder(
            config,
            service_context.clone(),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .build();

        assert!(result.is_err(), "an invalid gRPC budget must fail startup");
        assert_eq!(
            service_context.task_group().child_count(),
            0,
            "default backend workers must not start before gRPC budget validation"
        );
        let report = runtime_context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn early_startup_error_cleans_started_backend_task_group() {
        let runtime_context = RuntimeContext::from_current("proxy-startup-rollback-test");
        let service_context = runtime_context.service_context("proxy-startup-rollback");
        let backend_context = service_context.child("backend");
        let auth_context = service_context.child("auth");
        let cancellation = backend_context.task_group().cancellation_token();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        backend_context
            .spawn_service("proxy.test.backend-worker", async move {
                let _ = started_tx.send(());
                cancellation.cancelled().await;
            })
            .expect("test backend worker should start");
        started_rx.await.expect("test backend worker should be running");

        let result = finalize_proxy_run(
            Err(crate::error::ProxyError::not_implemented(
                "test startup failure after backend creation",
            )),
            Some(&backend_context),
            None,
            &auth_context,
            &service_context,
            rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(1)),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(crate::error::ProxyError::NotImplemented {
                    feature: "test startup failure after backend creation"
                })
            ),
            "successful rollback must preserve the primary startup error"
        );
        assert_eq!(
            backend_context.task_group().task_count(),
            0,
            "transactional rollback must leave no backend task behind"
        );
        assert_eq!(
            service_context.task_group().task_count(),
            0,
            "transactional rollback must leave no Proxy service task behind"
        );
    }

    #[tokio::test]
    async fn default_local_mode_builds_broker_backed_runtime() {
        let runtime_context = RuntimeContext::from_current("proxy-local-runtime-test");
        let runtime = ProxyRuntimeBuilder::build(ProxyRuntime::builder(
            ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            },
            runtime_context.service_context("proxy-local-runtime"),
            rocketmq_observability::TelemetryHandle::noop(),
        ))
        .expect("an injected child context should build the Local proxy runtime");

        assert!(matches!(runtime.config().mode, ProxyMode::Local));
    }

    #[tokio::test]
    async fn default_cluster_runtime_uses_injected_child_context() {
        let runtime_context = RuntimeContext::from_current("proxy-cluster-runtime-test");
        let runtime = ProxyRuntime::new(
            ProxyConfig::default(),
            runtime_context.service_context("proxy-cluster-runtime"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .expect("an injected child context should build the Cluster proxy runtime");

        assert_eq!(runtime.service_context.name(), "proxy-cluster-runtime");

        drop(runtime);
        let report = runtime_context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}
