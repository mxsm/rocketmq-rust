// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::Arc;

use crate::adapter::admin_session::AdminCoreSessionFactory;
use crate::adapter::query_facade::QueryFacade;
use crate::config::McpConfig;
use crate::config::TransportKind;
use crate::guard::audit::AuditDrainReport;
use crate::guard::Guard;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;

static LEGACY_TELEMETRY_GUARD: std::sync::OnceLock<
    std::sync::Mutex<Option<rocketmq_observability::TelemetryRuntimeGuard>>,
> = std::sync::OnceLock::new();

#[derive(Debug, Clone)]
pub struct McpShutdownReport {
    pub audit: AuditDrainReport,
    pub runtime: Option<rocketmq_runtime::ShutdownReport>,
    pub telemetry: Option<rocketmq_observability::TelemetryShutdownReport>,
}

/// Opaque proof that MCP telemetry and every final process listener passed the
/// pre-bind security boundary.
///
/// The fields are private so callers cannot replace the resolved telemetry,
/// process identity, or security outcome after validation.
pub struct ValidatedMcpBootstrap {
    config: McpConfig,
    telemetry_resolution: rocketmq_observability::TelemetryResolution,
    security_outcome: rocketmq_security_api::SecurityBootstrapOutcome,
}

impl std::fmt::Debug for ValidatedMcpBootstrap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValidatedMcpBootstrap")
            .field("security_outcome", &self.security_outcome)
            .field(
                "telemetry_enabled",
                &self.telemetry_resolution.bootstrap.observability.enabled,
            )
            .field(
                "prometheus_listener_present",
                &self.telemetry_resolution.prometheus_listener_addr.is_some(),
            )
            .finish()
    }
}

impl ValidatedMcpBootstrap {
    /// Returns the non-sensitive security outcome for startup logging.
    pub const fn security_outcome(&self) -> rocketmq_security_api::SecurityBootstrapOutcome {
        self.security_outcome
    }
}

impl McpShutdownReport {
    pub fn is_healthy(&self) -> bool {
        self.audit.is_healthy()
            && self
                .runtime
                .as_ref()
                .is_none_or(rocketmq_runtime::ShutdownReport::is_healthy)
            && self
                .telemetry
                .as_ref()
                .is_none_or(rocketmq_observability::TelemetryShutdownReport::is_healthy)
    }

    pub fn log_if_unhealthy(&self) {
        self.audit.log_if_unhealthy();
        if let Some(runtime) = &self.runtime {
            runtime.log_if_unhealthy();
        }
        if let Some(telemetry) = &self.telemetry {
            if !telemetry.is_healthy() {
                tracing::error!(report = %telemetry.to_json(), "MCP telemetry shutdown was unhealthy");
            }
        }
    }
}

#[derive(Clone)]
pub struct McpApp {
    config: McpConfig,
    guard: Guard,
    metrics: rocketmq_observability::metrics::mcp::McpMetricsRecorder,
    query: Arc<QueryFacade<AdminCoreSessionFactory>>,
    client_runtime: Arc<ClientRuntime>,
    service_context: rocketmq_runtime::ChildServiceContext,
    telemetry: Arc<std::sync::Mutex<Option<rocketmq_observability::TelemetryRuntimeGuard>>>,
}

impl std::fmt::Debug for McpApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpApp")
            .field("config", &self.config)
            .field("guard", &self.guard)
            .field("query", &self.query)
            .field("service_context", &self.service_context)
            .field(
                "telemetry_installed",
                &self
                    .telemetry
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .is_some(),
            )
            .finish()
    }
}

impl McpApp {
    pub fn new(
        config: McpConfig,
        service_context: rocketmq_runtime::ChildServiceContext,
        telemetry_handle: rocketmq_observability::TelemetryHandle,
    ) -> Result<Self, crate::error::McpError> {
        let guard = Guard::new(config.security.clone(), config.audit.clone(), &config.clusters)
            .map_err(|error| crate::error::McpError::InvalidConfig(error.to_string()))?;
        let metrics = rocketmq_observability::metrics::mcp::McpMetricsRecorder::from_handle(&telemetry_handle);
        let client_runtime = ClientRuntime::try_new(
            service_context.component("rocketmq-mcp-client"),
            ClientRuntimeConfig::default(),
            telemetry_handle,
        )
        .map_err(|error| crate::error::McpError::infrastructure("initialize MCP client runtime", error))?;
        let query = Arc::new(QueryFacade::new(config.clone(), client_runtime.clone()));
        Ok(Self {
            config,
            guard,
            metrics,
            query,
            client_runtime,
            service_context,
            telemetry: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
    pub(crate) fn with_test_session_factory(
        mut self,
        factory: crate::adapter::admin_session::ProtocolTestSessionFactory,
    ) -> Self {
        let factory = AdminCoreSessionFactory::new(self.client_runtime.clone()).with_test_session_factory(factory);
        self.query = Arc::new(QueryFacade::with_factory(self.config.clone(), factory));
        self
    }

    /// Initializes telemetry and background work only after the composition root has
    /// resolved process-wide security bootstrap before listener bind.
    ///
    /// # Errors
    ///
    /// Returns a typed MCP error when telemetry, guards, adapters, or background
    /// services cannot be initialized.
    pub async fn bootstrap_typed(
        config: McpConfig,
        process_telemetry: rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
        security_bootstrap: rocketmq_security_api::SecurityBootstrapOutcome,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Self, crate::error::McpError> {
        let telemetry_resolution = resolve_mcp_telemetry(&config)?;
        ensure_process_telemetry_matches(&process_telemetry, &telemetry_resolution.process)?;
        let handoff = prepare_mcp_bootstrap_from_validated_outcome(config, telemetry_resolution, security_bootstrap)?;
        Self::bootstrap_validated_typed(handoff, service_context).await
    }

    /// Initializes MCP from an opaque handoff created only after shared
    /// telemetry resolution and pre-bind security validation.
    ///
    /// # Errors
    ///
    /// Returns a typed MCP error when telemetry, guards, adapters, or
    /// background services cannot be initialized.
    pub async fn bootstrap_validated_typed(
        handoff: ValidatedMcpBootstrap,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Self, crate::error::McpError> {
        let ValidatedMcpBootstrap {
            config,
            telemetry_resolution,
            security_outcome: _,
        } = handoff;
        let rocketmq_observability::TelemetryResolution { bootstrap, process, .. } = telemetry_resolution;
        let telemetry = init_resolved_tracing_typed(&config, bootstrap, &process, &service_context).await?;
        rocketmq_observability::metrics::runtime::record_lifecycle(
            rocketmq_runtime::RuntimeComponent::Mcp,
            rocketmq_observability::metrics::runtime::RuntimeLifecycleState::Starting,
            rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::Startup,
        );
        let app = match Self::new(config, service_context.clone(), telemetry.handle()) {
            Ok(app) => app,
            Err(error) => {
                let report = telemetry
                    .shutdown_with_service_context(&service_context, std::time::Duration::from_secs(10))
                    .await;
                if !report.is_healthy() {
                    tracing::error!(
                        report = %report.to_json(),
                        "MCP telemetry rollback after composition failure was unhealthy"
                    );
                }
                rocketmq_observability::metrics::runtime::record_lifecycle(
                    rocketmq_runtime::RuntimeComponent::Mcp,
                    rocketmq_observability::metrics::runtime::RuntimeLifecycleState::Failed,
                    rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::Internal,
                );
                return Err(error);
            }
        };
        *app.telemetry.lock().unwrap_or_else(|error| error.into_inner()) = Some(telemetry);
        if let Err(error) = app.start_background_services() {
            let deadline = rocketmq_runtime::ShutdownDeadline::after(std::time::Duration::from_secs(10));
            app.shutdown_with_deadline(deadline).await.log_if_unhealthy();
            return Err(error);
        }
        Ok(app)
    }

    #[deprecated(since = "1.0.0", note = "use McpApp::bootstrap_typed")]
    pub async fn bootstrap(
        config: McpConfig,
        process_telemetry: rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
        validated_security: rocketmq_security_api::ValidatedSecurityBootstrap,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> anyhow::Result<Self> {
        Self::bootstrap_typed(
            config,
            process_telemetry,
            rocketmq_security_api::SecurityBootstrapOutcome::Validated(validated_security),
            service_context,
        )
        .await
        .map_err(anyhow::Error::new)
    }

    pub fn config(&self) -> &McpConfig {
        &self.config
    }

    pub fn guard(&self) -> &Guard {
        &self.guard
    }

    pub(crate) fn metrics(&self) -> &rocketmq_observability::metrics::mcp::McpMetricsRecorder {
        &self.metrics
    }

    pub(crate) fn query(&self) -> &Arc<QueryFacade<AdminCoreSessionFactory>> {
        &self.query
    }

    /// Starts the process lifecycle boundary under the application's owned runtime context.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error when the runtime context is unavailable or the
    /// lifecycle health boundary cannot be started.
    pub async fn start_lifecycle(
        &self,
        lifecycle: &rocketmq_runtime::ServiceLifecycle,
    ) -> Result<(), crate::error::McpError> {
        let service_context = self.service_context.component("rocketmq-mcp-lifecycle");
        lifecycle.start(&service_context).await.map_err(|error| {
            crate::error::McpError::InvalidConfig(format!("failed to start MCP lifecycle boundary: {error}"))
        })
    }

    #[cfg(feature = "streamable-http")]
    pub(crate) fn service_context(
        &self,
        name: &'static str,
    ) -> Result<rocketmq_runtime::ChildServiceContext, crate::error::McpError> {
        Ok(self.service_context.component(name))
    }

    pub(crate) fn trace_cache_metrics(&self) {
        let metrics = self.query.cache_metrics();
        let audit = self.guard.audit_metrics();
        tracing::trace!(
            cache_hits = metrics.hits,
            cache_misses = metrics.misses,
            cache_bypasses = metrics.bypasses,
            cache_evictions = metrics.evictions,
            cache_invalidations = metrics.invalidations,
            cache_coalesced_waiters = metrics.coalesced_waiters,
            audit_queued = audit.queued,
            audit_accepted = audit.accepted,
            audit_written = audit.written,
            audit_dropped = audit.dropped,
            audit_oversized = audit.oversized,
            audit_count_capacity_drops = audit.count_capacity_drops,
            audit_byte_capacity_drops = audit.byte_capacity_drops,
            audit_closed_drops = audit.closed_drops,
            audit_sink_failures = audit.sink_failures,
            audit_flush_failures = audit.flush_failures,
            audit_pending_records = audit.pending_records,
            audit_pending_bytes = audit.pending_bytes,
            "rocketmq-mcp cache metrics"
        );
    }

    pub(crate) fn runtime_diagnostics_view(&self) -> rocketmq_runtime::RuntimeDiagnosticsViewV1 {
        let view = self
            .service_context
            .diagnostics_view_v1(rocketmq_runtime::RuntimeComponent::Mcp);
        rocketmq_observability::metrics::runtime::record_snapshot(&view);
        view
    }

    pub(crate) fn observability_status_view(&self) -> rocketmq_observability::ObservabilityStatusViewV1 {
        self.telemetry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(rocketmq_observability::TelemetryRuntimeGuard::status_handle)
            .unwrap_or_default()
            .view()
    }

    /// Clears all cached RocketMQ query results and returns the number of removed entries.
    pub async fn invalidate_cache(&self) -> usize {
        self.query.invalidate_cache().await
    }

    pub fn transport(&self) -> TransportKind {
        self.config.server.transport
    }

    pub async fn shutdown(&self) {
        let report = self
            .shutdown_with_deadline(rocketmq_runtime::ShutdownDeadline::after(
                std::time::Duration::from_secs(10),
            ))
            .await;
        report.log_if_unhealthy();
    }

    /// Closes audit admission, drains accepted records, and then shuts down all owned runtime work.
    ///
    /// The same absolute `deadline` bounds both phases, so audit draining cannot reset the runtime
    /// shutdown budget.
    pub async fn shutdown_with_deadline(&self, deadline: rocketmq_runtime::ShutdownDeadline) -> McpShutdownReport {
        rocketmq_observability::metrics::runtime::record_lifecycle(
            rocketmq_runtime::RuntimeComponent::Mcp,
            rocketmq_observability::metrics::runtime::RuntimeLifecycleState::Stopping,
            rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::ShutdownRequest,
        );
        let audit = self.guard.audit_log().close_and_drain(deadline).await;
        let client_report = self.client_runtime.shutdown_until(deadline).await;
        let client_healthy = client_report.is_healthy();
        client_report.log_if_unhealthy();
        let runtime = Some(self.service_context.task_group().shutdown_until(deadline).await);
        let runtime_healthy = runtime
            .as_ref()
            .is_none_or(rocketmq_runtime::ShutdownReport::is_healthy);
        let lifecycle_healthy = audit.is_healthy() && client_healthy && runtime_healthy && !deadline.is_expired();
        rocketmq_observability::metrics::runtime::record_lifecycle(
            rocketmq_runtime::RuntimeComponent::Mcp,
            if lifecycle_healthy {
                rocketmq_observability::metrics::runtime::RuntimeLifecycleState::Stopped
            } else {
                rocketmq_observability::metrics::runtime::RuntimeLifecycleState::Failed
            },
            if deadline.is_expired() {
                rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::Timeout
            } else if lifecycle_healthy {
                rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::ShutdownComplete
            } else {
                rocketmq_observability::metrics::runtime::RuntimeLifecycleReason::Internal
            },
        );
        let telemetry_guard = self.telemetry.lock().unwrap_or_else(|error| error.into_inner()).take();
        let telemetry = match telemetry_guard {
            Some(guard) => Some(
                guard
                    .shutdown_with_service_context(&self.service_context, deadline.remaining())
                    .await,
            ),
            None => None,
        };
        McpShutdownReport {
            audit,
            runtime,
            telemetry,
        }
    }

    fn start_background_services(&self) -> Result<(), crate::error::McpError> {
        let audit_service = self.service_context.component("rocketmq-mcp-audit");
        self.guard
            .audit_log()
            .start(&self.config.audit, &audit_service)
            .map_err(|error| crate::error::McpError::InvalidConfig(error.to_string()))?;
        Ok(())
    }
}

pub async fn init_tracing_typed(
    config: &McpConfig,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
    service_context: &rocketmq_runtime::ChildServiceContext,
) -> Result<rocketmq_observability::TelemetryRuntimeGuard, crate::error::McpError> {
    let rocketmq_observability::TelemetryResolution { bootstrap, process, .. } = resolve_mcp_telemetry(config)?;
    ensure_process_telemetry_matches(process_telemetry, &process)?;
    init_resolved_tracing_typed(config, bootstrap, &process, service_context).await
}

async fn init_resolved_tracing_typed(
    config: &McpConfig,
    bootstrap: rocketmq_observability::TelemetryBootstrapConfig,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
    service_context: &rocketmq_runtime::ChildServiceContext,
) -> Result<rocketmq_observability::TelemetryRuntimeGuard, crate::error::McpError> {
    let environment_filter = rocketmq_observability::read_rust_log()
        .map_err(|source| crate::error::McpError::infrastructure("read MCP RUST_LOG", source))?;
    let resolved_filter = rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
        environment: environment_filter.as_deref(),
        config: config.logging.filter.as_deref(),
        legacy_config: config.server.log_level.as_deref(),
        ..rocketmq_observability::LogFilterInputs::default()
    })
    .map_err(|source| crate::error::McpError::infrastructure("resolve MCP tracing filter", source))?;
    let guard = rocketmq_observability::install_global_with_filter_and_service_context(
        &bootstrap,
        resolved_filter.clone(),
        service_context,
    )
    .await
    .map_err(|source| crate::error::McpError::infrastructure("install MCP telemetry", source))?;
    if let Err(registration_error) = register_mcp_release_identity(&guard, process_telemetry) {
        let cleanup_error = guard
            .shutdown_with_service_context(service_context, std::time::Duration::from_secs(10))
            .await
            .into_result()
            .err()
            .map(|error| error.to_string());
        return Err(match cleanup_error {
            Some(cleanup_error) => crate::error::McpError::InvalidConfig(format!(
                "{registration_error}; MCP telemetry cleanup after release identity failure also failed: \
                 {cleanup_error}"
            )),
            None => registration_error,
        });
    }
    tracing::info!(
        service = "rocketmq-mcp",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        subscriber_installed = guard.subscriber_install_status().installed,
        reload_enabled = bootstrap.logging.reload.enabled,
        "MCP telemetry bootstrap initialized"
    );
    if config.logging.filter.is_none() && config.server.log_level.is_some() {
        tracing::warn!("server.log_level is deprecated; use logging.filter instead");
    }
    Ok(guard)
}

fn build_mcp_telemetry_bootstrap_config(config: &McpConfig) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-mcp".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "mcp".to_string();
    bootstrap.observability.node_id = config.server.name.clone();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    bootstrap.logging.reload = config.logging.reload;
    bootstrap
}

fn mcp_telemetry_environment_spec() -> rocketmq_observability::TelemetryEnvironmentSpec {
    rocketmq_observability::TelemetryEnvironmentSpec {
        trace_sample_ratio_env: Some("ROCKETMQ_MCP_TRACE_SAMPLE_RATIO"),
    }
}

fn ensure_process_telemetry_matches(
    supplied: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
    resolved: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
) -> Result<(), crate::error::McpError> {
    if supplied != resolved {
        return Err(crate::error::McpError::InvalidConfig(
            "MCP process telemetry input must match the unified telemetry resolution".to_string(),
        ));
    }
    Ok(())
}

fn validate_mcp_telemetry_resolution(
    config: &McpConfig,
    resolution: &rocketmq_observability::TelemetryResolution,
) -> Result<(), crate::error::McpError> {
    let observability = &resolution.bootstrap.observability;
    if observability.service_name != "rocketmq-mcp"
        || observability.service_namespace != "rocketmq"
        || observability.node_type != "mcp"
        || observability.node_id != config.server.name
        || observability.subscriber_install_policy != rocketmq_observability::SubscriberInstallPolicy::Required
        || resolution.process.release_identity().service() != "rocketmq-mcp"
        || observability.metrics.enabled != resolution.process.metrics_enabled()
        || observability.metrics.exporter != resolution.process.metrics_exporter()
        || observability.prometheus.host != resolution.process.prometheus_host()
        || observability.prometheus.port != resolution.process.prometheus_port()
        || observability.prometheus.path != resolution.process.prometheus_path()
        || observability.enabled
            != (observability.metrics.enabled || observability.traces.enabled || observability.logs.enabled)
    {
        return Err(crate::error::McpError::InvalidConfig(
            "resolved MCP telemetry identity must match the service-owned identity".to_string(),
        ));
    }
    let expected_listener = (observability.metrics.enabled
        && observability.metrics.exporter == rocketmq_observability::MetricsExporter::Prometheus)
        .then(|| resolution.process.prometheus_listener_addr())
        .flatten();
    if resolution.prometheus_listener_addr != expected_listener {
        return Err(crate::error::McpError::InvalidConfig(
            "resolved MCP telemetry listener must match the validated process configuration".to_string(),
        ));
    }
    Ok(())
}

/// Resolves MCP telemetry and validates every final HTTP, Prometheus, and probe
/// listener before returning an opaque startup handoff.
///
/// # Errors
///
/// Returns a redacted invalid-configuration error when telemetry resolution,
/// listener parsing, or security validation fails.
pub fn prepare_mcp_bootstrap(
    config: McpConfig,
    security_bootstrap: &rocketmq_security_api::SecurityBootstrap,
    probe_bind_addr: Option<std::net::SocketAddr>,
) -> Result<ValidatedMcpBootstrap, crate::error::McpError> {
    let telemetry_resolution = resolve_mcp_telemetry(&config)?;
    prepare_mcp_bootstrap_from_resolution(config, security_bootstrap, probe_bind_addr, telemetry_resolution)
}

fn prepare_mcp_bootstrap_from_resolution(
    config: McpConfig,
    security_bootstrap: &rocketmq_security_api::SecurityBootstrap,
    probe_bind_addr: Option<std::net::SocketAddr>,
    telemetry_resolution: rocketmq_observability::TelemetryResolution,
) -> Result<ValidatedMcpBootstrap, crate::error::McpError> {
    validate_mcp_telemetry_resolution(&config, &telemetry_resolution)?;
    let security_outcome = validate_mcp_security(
        security_bootstrap,
        config.server.transport,
        &config.server.http.bind,
        telemetry_resolution.prometheus_listener_addr,
        probe_bind_addr,
    )?;
    Ok(ValidatedMcpBootstrap {
        config,
        telemetry_resolution,
        security_outcome,
    })
}

fn prepare_mcp_bootstrap_from_validated_outcome(
    config: McpConfig,
    telemetry_resolution: rocketmq_observability::TelemetryResolution,
    security_outcome: rocketmq_security_api::SecurityBootstrapOutcome,
) -> Result<ValidatedMcpBootstrap, crate::error::McpError> {
    validate_mcp_telemetry_resolution(&config, &telemetry_resolution)?;
    Ok(ValidatedMcpBootstrap {
        config,
        telemetry_resolution,
        security_outcome,
    })
}

/// Validates the final MCP listener set against the resolved security profile.
///
/// # Errors
///
/// Returns a redacted invalid-configuration error when an HTTP listener is
/// malformed or any listener violates the configured security profile.
pub fn validate_mcp_security(
    security_bootstrap: &rocketmq_security_api::SecurityBootstrap,
    transport: TransportKind,
    http_bind: &str,
    prometheus_bind_addr: Option<std::net::SocketAddr>,
    probe_bind_addr: Option<std::net::SocketAddr>,
) -> Result<rocketmq_security_api::SecurityBootstrapOutcome, crate::error::McpError> {
    if !security_bootstrap.is_enabled() {
        return security_bootstrap.validate(&[]).map_err(|error| {
            crate::error::McpError::InvalidConfig(format!(
                "MCP security bootstrap failed before listener bind: {error}"
            ))
        });
    }
    let mut listeners = Vec::with_capacity(3);
    if transport == TransportKind::StreamableHttp {
        listeners.push(http_bind.parse::<std::net::SocketAddr>().map_err(|_| {
            crate::error::McpError::InvalidConfig("server.http.bind must be a socket address".to_string())
        })?);
    }
    if let Some(prometheus_bind_addr) = prometheus_bind_addr {
        listeners.push(prometheus_bind_addr);
    }
    if let Some(probe_bind_addr) = probe_bind_addr {
        listeners.push(probe_bind_addr);
    }
    security_bootstrap.validate(&listeners).map_err(|error| {
        crate::error::McpError::InvalidConfig(format!("MCP security bootstrap failed before listener bind: {error}"))
    })
}

/// Resolves MCP telemetry from service defaults, file overrides, and present
/// process environment variables.
///
/// # Errors
///
/// Returns a redacted invalid-configuration error when a present environment
/// value or the merged telemetry configuration is invalid.
pub fn resolve_mcp_telemetry(
    config: &McpConfig,
) -> Result<rocketmq_observability::TelemetryResolution, crate::error::McpError> {
    rocketmq_observability::resolve_telemetry_from_env(
        "rocketmq-mcp",
        build_mcp_telemetry_bootstrap_config(config),
        &config.observability,
        mcp_telemetry_environment_spec(),
    )
    .map_err(|error| crate::error::McpError::InvalidConfig(format!("invalid MCP telemetry configuration: {error}")))
}

#[cfg(test)]
fn resolve_mcp_telemetry_values(
    config: &McpConfig,
    environment: &rocketmq_observability::TelemetryEnvironmentValues,
) -> Result<rocketmq_observability::TelemetryResolution, crate::error::McpError> {
    rocketmq_observability::resolve_telemetry_values(
        "rocketmq-mcp",
        build_mcp_telemetry_bootstrap_config(config),
        &config.observability,
        environment,
        mcp_telemetry_environment_spec(),
    )
    .map_err(|error| crate::error::McpError::InvalidConfig(format!("invalid MCP telemetry configuration: {error}")))
}

#[cfg(test)]
fn prepare_mcp_bootstrap_values(
    config: McpConfig,
    security_bootstrap: &rocketmq_security_api::SecurityBootstrap,
    probe_bind_addr: Option<std::net::SocketAddr>,
    environment: &rocketmq_observability::TelemetryEnvironmentValues,
) -> Result<ValidatedMcpBootstrap, crate::error::McpError> {
    let telemetry_resolution = resolve_mcp_telemetry_values(&config, environment)?;
    prepare_mcp_bootstrap_from_resolution(config, security_bootstrap, probe_bind_addr, telemetry_resolution)
}

fn register_mcp_release_identity(
    telemetry_guard: &rocketmq_observability::TelemetryRuntimeGuard,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
) -> Result<(), crate::error::McpError> {
    if !process_telemetry.metrics_enabled() {
        return Ok(());
    }

    #[cfg(feature = "observability")]
    {
        let telemetry = telemetry_guard.handle();
        telemetry
            .register_release_identity(process_telemetry.release_identity().clone())
            .map_err(|source| {
                crate::error::McpError::infrastructure("register MCP release identity before readiness", source)
            })?;
        if !telemetry.release_identity_registered() {
            return Err(crate::error::McpError::InvalidConfig(
                "MCP release identity was not registered before readiness".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(not(feature = "observability"))]
    {
        let _ = telemetry_guard;
        Err(crate::error::McpError::InvalidConfig(
            "MCP metrics require the `observability` Cargo feature".to_string(),
        ))
    }
}

#[deprecated(since = "1.0.0", note = "use init_tracing_typed")]
pub fn init_tracing(config: &McpConfig) -> anyhow::Result<()> {
    let environment_filter = rocketmq_observability::read_rust_log()?;
    let resolved_filter =
        rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
            environment: environment_filter.as_deref(),
            config: config.logging.filter.as_deref(),
            legacy_config: config.server.log_level.as_deref(),
            ..rocketmq_observability::LogFilterInputs::default()
        })?;
    let rocketmq_observability::TelemetryResolution {
        bootstrap,
        process: process_telemetry,
        ..
    } = resolve_mcp_telemetry(config).map_err(anyhow::Error::new)?;
    let guard = rocketmq_observability::install_global_with_filter(&bootstrap, resolved_filter)?;
    register_mcp_release_identity(&guard, &process_telemetry).map_err(anyhow::Error::new)?;
    *LEGACY_TELEMETRY_GUARD
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(guard);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prepare_mcp_bootstrap_from_resolution;
    use super::prepare_mcp_bootstrap_values;
    use super::resolve_mcp_telemetry_values;

    fn example_config() -> crate::config::McpConfig {
        crate::config::McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .expect("example MCP config should load")
    }

    #[test]
    fn normal_shutdown_uses_service_context_for_telemetry_cleanup() {
        let source = include_str!("app.rs");
        let shutdown_start = source
            .find("pub async fn shutdown_with_deadline")
            .expect("MCP app should expose bounded shutdown");
        let shutdown_end = source[shutdown_start..]
            .find("fn start_background_services")
            .map(|offset| shutdown_start + offset)
            .expect("MCP app shutdown should precede background service startup");
        let shutdown = &source[shutdown_start..shutdown_end];

        assert!(
            shutdown.contains("shutdown_with_service_context(&self.service_context, deadline.remaining())"),
            "MCP telemetry cleanup must use the same service context that owns Prometheus work"
        );
        assert!(
            !shutdown.contains("shutdown_with_timeout(deadline.remaining())"),
            "normal shutdown must not bypass the telemetry service context"
        );
    }

    #[test]
    fn file_observability_is_preserved_without_environment_values() {
        let mut config = example_config();
        config.observability.traces.exporter = Some(rocketmq_observability::TraceExporter::OtlpGrpc);
        config.observability.traces.sample_ratio = Some(0.4);
        config.observability.otlp.endpoint = Some("http://file-collector:4317".to_string());
        config.observability.otlp.protocol = Some(rocketmq_observability::OtlpProtocol::Grpc);

        let resolution =
            resolve_mcp_telemetry_values(&config, &rocketmq_observability::TelemetryEnvironmentValues::default())
                .expect("file-only telemetry should resolve");

        assert!(resolution.bootstrap.observability.traces.enabled);
        assert_eq!(resolution.bootstrap.observability.traces.sample_ratio, 0.4);
        assert_eq!(
            resolution.bootstrap.observability.otlp.endpoint,
            "http://file-collector:4317"
        );
    }

    #[test]
    fn standard_otlp_environment_overrides_file_and_enables_all_signals() {
        let mut config = example_config();
        config.observability.traces.exporter = Some(rocketmq_observability::TraceExporter::OtlpGrpc);
        config.observability.traces.sample_ratio = Some(0.4);
        config.observability.otlp.endpoint = Some("http://file-collector:4317".to_string());
        config.observability.otlp.protocol = Some(rocketmq_observability::OtlpProtocol::Grpc);
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            otlp_endpoint: Some("http://environment-collector:4317".into()),
            otlp_protocol: Some("grpc".into()),
            ..Default::default()
        };

        let resolution =
            resolve_mcp_telemetry_values(&config, &environment).expect("standard OTLP environment should resolve");

        let observability = resolution.bootstrap.observability;
        assert!(observability.metrics.enabled);
        assert!(observability.traces.enabled);
        assert!(observability.logs.enabled);
        assert_eq!(observability.otlp.endpoint, "http://environment-collector:4317");
        assert_eq!(observability.traces.sample_ratio, 0.4);

        prepare_mcp_bootstrap_values(
            config,
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            None,
            &environment,
        )
        .expect("standard OTLP environment must produce a consistent validated handoff");
    }

    #[test]
    fn invalid_standard_otlp_environment_is_redacted() {
        let config = example_config();
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            otlp_endpoint: Some("http://secret-endpoint-sentinel:4317".into()),
            otlp_protocol: Some("secret-protocol-sentinel".into()),
            ..Default::default()
        };

        let error = resolve_mcp_telemetry_values(&config, &environment)
            .expect_err("unsupported protocol should fail")
            .to_string();

        assert!(error.contains("OTEL_EXPORTER_OTLP_PROTOCOL"));
        assert!(!error.contains("secret-endpoint-sentinel"));
        assert!(!error.contains("secret-protocol-sentinel"));
    }

    #[test]
    fn mcp_trace_sample_ratio_environment_overrides_file_and_redacts_invalid_values() {
        let mut config = example_config();
        config.observability.traces.sample_ratio = Some(0.4);
        for (environment_value, expected) in [("0", 0.0), ("  1.0  ", 1.0)] {
            let environment = rocketmq_observability::TelemetryEnvironmentValues {
                trace_sample_ratio: Some(environment_value.into()),
                ..Default::default()
            };
            let resolution = resolve_mcp_telemetry_values(&config, &environment)
                .expect("MCP boundary trace sample ratio should resolve");
            assert_eq!(resolution.bootstrap.observability.traces.sample_ratio, expected);
        }

        for invalid_value in ["-0.1", "1.1", "NaN", "inf"] {
            let invalid_environment = rocketmq_observability::TelemetryEnvironmentValues {
                trace_sample_ratio: Some(invalid_value.into()),
                ..Default::default()
            };
            let error = resolve_mcp_telemetry_values(&config, &invalid_environment)
                .expect_err("invalid MCP trace sample ratio should fail")
                .to_string();
            assert!(error.contains("ROCKETMQ_MCP_TRACE_SAMPLE_RATIO"));
            assert!(!error.contains(invalid_value));
        }

        for invalid_value in ["", "   ", "secret-sample-ratio-sentinel"] {
            let invalid_environment = rocketmq_observability::TelemetryEnvironmentValues {
                trace_sample_ratio: Some(invalid_value.into()),
                ..Default::default()
            };
            let error = resolve_mcp_telemetry_values(&config, &invalid_environment)
                .expect_err("empty or non-numeric MCP trace sample ratio should fail")
                .to_string();
            assert_eq!(
                error,
                "invalid configuration: invalid MCP telemetry configuration: invalid observability config: \
                 ROCKETMQ_MCP_TRACE_SAMPLE_RATIO must be a floating-point number"
            );
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn non_utf8_mcp_trace_sample_ratio_is_redacted() {
        #[cfg(unix)]
        let invalid_value = {
            use std::os::unix::ffi::OsStringExt;
            std::ffi::OsString::from_vec(vec![0xff])
        };
        #[cfg(windows)]
        let invalid_value = {
            use std::os::windows::ffi::OsStringExt;
            std::ffi::OsString::from_wide(&[0xd800])
        };
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            trace_sample_ratio: Some(invalid_value),
            ..Default::default()
        };

        let error = resolve_mcp_telemetry_values(&example_config(), &environment)
            .expect_err("non-UTF-8 MCP trace sample ratio should fail")
            .to_string();

        assert!(error.contains("ROCKETMQ_MCP_TRACE_SAMPLE_RATIO"));
    }

    #[test]
    fn validated_handoff_rejects_tampered_resolution_parts() {
        let config = example_config();
        let environment = rocketmq_observability::TelemetryEnvironmentValues::default();

        let mut listener_tampered = resolve_mcp_telemetry_values(&config, &environment).unwrap();
        listener_tampered.prometheus_listener_addr = Some("127.0.0.1:5557".parse().unwrap());
        assert!(prepare_mcp_bootstrap_from_resolution(
            config.clone(),
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            None,
            listener_tampered,
        )
        .is_err());

        let mut process_tampered = resolve_mcp_telemetry_values(&config, &environment).unwrap();
        process_tampered.process =
            rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig::try_from_values(
                "rocketmq-mcp",
                None,
                None,
                Some("true"),
                Some("log"),
                None,
                None,
            )
            .unwrap();
        assert!(prepare_mcp_bootstrap_from_resolution(
            config.clone(),
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            None,
            process_tampered,
        )
        .is_err());

        let mut bootstrap_tampered = resolve_mcp_telemetry_values(&config, &environment).unwrap();
        bootstrap_tampered.bootstrap.observability.enabled = true;
        assert!(prepare_mcp_bootstrap_from_resolution(
            config.clone(),
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            None,
            bootstrap_tampered,
        )
        .is_err());

        let mut identity_tampered = resolve_mcp_telemetry_values(&config, &environment).unwrap();
        identity_tampered.bootstrap.observability.service_name = "not-rocketmq-mcp".to_string();
        let error = prepare_mcp_bootstrap_from_resolution(
            config,
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            None,
            identity_tampered,
        )
        .expect_err("tampered service identity must not produce a validated handoff")
        .to_string();
        assert!(!error.contains("not-rocketmq-mcp"));
    }

    #[test]
    fn validated_handoff_requires_final_listener_security_validation() {
        let mut config = example_config();
        config.observability.metrics.exporter = Some(rocketmq_observability::MetricsExporter::Prometheus);
        config.observability.prometheus.host = Some("0.0.0.0".to_string());
        config.observability.prometheus.port = Some(5557);
        let security =
            rocketmq_security_api::SecurityBootstrap::Enabled(rocketmq_security_api::SecurityBootstrapConfig::new(
                rocketmq_security_api::SecurityBootstrapProfile::DevelopmentInsecureLoopback,
            ));

        let error = prepare_mcp_bootstrap_values(
            config,
            &security,
            None,
            &rocketmq_observability::TelemetryEnvironmentValues::default(),
        )
        .expect_err("public file listener must prevent validated handoff construction")
        .to_string();

        assert!(error.contains("loopback"));
        assert!(!error.contains("0.0.0.0"));
        assert!(!error.contains("5557"));
    }

    #[test]
    fn app_debug_redacts_observability_values() {
        const ENDPOINT_SENTINEL: &str = "secret-app-endpoint-sentinel";
        const HEADER_SENTINEL: &str = "secret-app-header-sentinel";
        const RESOURCE_SENTINEL: &str = "secret-app-resource-sentinel";
        let mut config = example_config();
        config.observability.otlp.endpoint = Some(ENDPOINT_SENTINEL.to_string());
        config.observability.otlp.headers = Some(std::collections::HashMap::from([(
            "authorization".to_string(),
            HEADER_SENTINEL.to_string(),
        )]));
        config.observability.resource_attributes = Some(std::collections::HashMap::from([(
            "deployment.secret".to_string(),
            RESOURCE_SENTINEL.to_string(),
        )]));
        let owner = rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default(
            "mcp-app-debug-redaction-test",
        ))
        .unwrap();
        let app = super::McpApp::new(
            config,
            owner.root_context().component("mcp-app"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .unwrap();

        let debug = format!("{app:?}");

        assert!(debug.contains("observability"));
        for sentinel in [ENDPOINT_SENTINEL, HEADER_SENTINEL, RESOURCE_SENTINEL] {
            assert!(!debug.contains(sentinel));
        }
    }
}
