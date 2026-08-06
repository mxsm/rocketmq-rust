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

const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_PROTOCOL_ENV: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const TRACE_SAMPLE_RATIO_ENV: &str = "ROCKETMQ_MCP_TRACE_SAMPLE_RATIO";

#[derive(Debug, Clone, PartialEq, Eq)]
struct OtlpEnvironment {
    endpoint: String,
}

#[derive(Debug, Clone)]
pub struct McpShutdownReport {
    pub audit: AuditDrainReport,
    pub runtime: Option<rocketmq_runtime::ShutdownReport>,
    pub telemetry: Option<rocketmq_observability::TelemetryShutdownReport>,
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
        let query = Arc::new(QueryFacade::new(config.clone(), client_runtime.clone()).with_visibility_class("local"));
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
        _security_bootstrap: rocketmq_security_api::SecurityBootstrapOutcome,
        service_context: rocketmq_runtime::ChildServiceContext,
    ) -> Result<Self, crate::error::McpError> {
        let telemetry = init_tracing_typed(&config, &process_telemetry, &service_context).await?;
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
        let telemetry = self
            .telemetry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .map(|guard| guard.shutdown_with_timeout(deadline.remaining()));
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
    let environment_filter = rocketmq_observability::read_rust_log()
        .map_err(|source| crate::error::McpError::infrastructure("read MCP RUST_LOG", source))?;
    let resolved_filter = rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
        environment: environment_filter.as_deref(),
        config: config.logging.filter.as_deref(),
        legacy_config: config.server.log_level.as_deref(),
        ..rocketmq_observability::LogFilterInputs::default()
    })
    .map_err(|source| crate::error::McpError::infrastructure("resolve MCP tracing filter", source))?;
    let mut bootstrap = build_mcp_telemetry_bootstrap_config(config, process_telemetry);
    configure_otlp_from_environment(&mut bootstrap)?;
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

fn build_mcp_telemetry_bootstrap_config(
    config: &McpConfig,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-mcp".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "mcp".to_string();
    bootstrap.observability.node_id = config.server.name.clone();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    process_telemetry.apply_to(&mut bootstrap.observability);
    bootstrap.logging.reload = config.logging.reload;
    bootstrap
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

fn configure_otlp_from_environment(
    bootstrap: &mut rocketmq_observability::TelemetryBootstrapConfig,
) -> Result<(), crate::error::McpError> {
    let endpoint = read_utf8_environment(OTLP_ENDPOINT_ENV)?;
    let protocol = read_utf8_environment(OTLP_PROTOCOL_ENV)?;
    let trace_sample_ratio = read_utf8_environment(TRACE_SAMPLE_RATIO_ENV)?;
    if let Some(sample_ratio) = resolve_trace_sample_ratio(trace_sample_ratio.as_deref())? {
        bootstrap.observability.traces.sample_ratio = sample_ratio;
    }
    let Some(otlp) = resolve_otlp_environment(endpoint.as_deref(), protocol.as_deref())? else {
        return Ok(());
    };

    bootstrap.observability.enabled = true;
    bootstrap.observability.metrics.enabled = true;
    bootstrap.observability.metrics.exporter = rocketmq_observability::MetricsExporter::OtlpGrpc;
    bootstrap.observability.traces.enabled = true;
    bootstrap.observability.traces.exporter = rocketmq_observability::TraceExporter::OtlpGrpc;
    bootstrap.observability.logs.enabled = true;
    bootstrap.observability.logs.exporter = rocketmq_observability::LogsExporter::OtlpGrpc;
    bootstrap.observability.otlp.endpoint = otlp.endpoint;
    bootstrap.observability.otlp.protocol = rocketmq_observability::OtlpProtocol::Grpc;
    Ok(())
}

fn read_utf8_environment(name: &'static str) -> Result<Option<String>, crate::error::McpError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(crate::error::McpError::InvalidConfig(format!(
            "{name} must contain valid UTF-8"
        ))),
    }
}

fn resolve_otlp_environment(
    endpoint: Option<&str>,
    protocol: Option<&str>,
) -> Result<Option<OtlpEnvironment>, crate::error::McpError> {
    let Some(endpoint) = endpoint.map(str::trim).filter(|endpoint| !endpoint.is_empty()) else {
        return Ok(None);
    };
    if protocol.map(str::trim) != Some("grpc") {
        return Err(crate::error::McpError::InvalidConfig(format!(
            "{OTLP_PROTOCOL_ENV} must be `grpc` when {OTLP_ENDPOINT_ENV} is configured"
        )));
    }
    Ok(Some(OtlpEnvironment {
        endpoint: endpoint.to_string(),
    }))
}

fn resolve_trace_sample_ratio(value: Option<&str>) -> Result<Option<f64>, crate::error::McpError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let sample_ratio = value.trim().parse::<f64>().map_err(|_| {
        crate::error::McpError::InvalidConfig(format!(
            "{TRACE_SAMPLE_RATIO_ENV} must be a finite number between 0.0 and 1.0"
        ))
    })?;
    if !sample_ratio.is_finite() || !(0.0..=1.0).contains(&sample_ratio) {
        return Err(crate::error::McpError::InvalidConfig(format!(
            "{TRACE_SAMPLE_RATIO_ENV} must be a finite number between 0.0 and 1.0"
        )));
    }
    Ok(Some(sample_ratio))
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
    let process_telemetry =
        rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig::from_process_env("rocketmq-mcp")?;
    let mut bootstrap = build_mcp_telemetry_bootstrap_config(config, &process_telemetry);
    configure_otlp_from_environment(&mut bootstrap).map_err(anyhow::Error::new)?;
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
    use super::resolve_otlp_environment;
    use super::resolve_trace_sample_ratio;

    #[test]
    fn otlp_environment_is_disabled_without_a_non_empty_endpoint() {
        assert_eq!(resolve_otlp_environment(None, None).unwrap(), None);
        assert_eq!(
            resolve_otlp_environment(Some("  "), Some("http/protobuf")).unwrap(),
            None
        );
    }

    #[test]
    fn otlp_environment_accepts_only_explicit_grpc() {
        let resolved = resolve_otlp_environment(Some(" http://otel-collector:4317 "), Some("grpc"))
            .unwrap()
            .expect("OTLP should be enabled");

        assert_eq!(resolved.endpoint, "http://otel-collector:4317");
        assert!(resolve_otlp_environment(Some("http://otel-collector:4317"), None).is_err());
        assert!(resolve_otlp_environment(Some("http://otel-collector:4317"), Some("http/protobuf")).is_err());
    }

    #[test]
    fn trace_sample_ratio_preserves_the_production_default_without_an_override() {
        assert_eq!(resolve_trace_sample_ratio(None).unwrap(), None);
    }

    #[test]
    fn trace_sample_ratio_accepts_only_finite_values_in_the_unit_interval() {
        assert_eq!(resolve_trace_sample_ratio(Some(" 1.0 ")).unwrap(), Some(1.0));
        assert_eq!(resolve_trace_sample_ratio(Some("0")).unwrap(), Some(0.0));
        assert_eq!(resolve_trace_sample_ratio(Some("0.25")).unwrap(), Some(0.25));

        for invalid in ["", "-0.1", "1.1", "NaN", "inf", "secret-sentinel"] {
            let error = resolve_trace_sample_ratio(Some(invalid)).unwrap_err().to_string();
            assert!(error.contains("ROCKETMQ_MCP_TRACE_SAMPLE_RATIO"));
            if !invalid.is_empty() {
                assert!(!error.contains(invalid));
            }
        }
    }
}
