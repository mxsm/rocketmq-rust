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
use crate::config::Args;
use crate::config::McpConfig;
use crate::config::TransportKind;
use crate::guard::audit::AuditDrainReport;
use crate::guard::Guard;

static LEGACY_TELEMETRY_GUARD: std::sync::OnceLock<
    std::sync::Mutex<Option<rocketmq_observability::TelemetryRuntimeGuard>>,
> = std::sync::OnceLock::new();

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
    query: Arc<QueryFacade<AdminCoreSessionFactory>>,
    runtime_context: Option<rocketmq_runtime::RuntimeContext>,
    telemetry: Arc<std::sync::Mutex<Option<rocketmq_observability::TelemetryRuntimeGuard>>>,
}

impl std::fmt::Debug for McpApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpApp")
            .field("config", &self.config)
            .field("guard", &self.guard)
            .field("query", &self.query)
            .field("runtime_context", &self.runtime_context)
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
    pub fn new(config: McpConfig) -> Result<Self, crate::error::McpError> {
        let guard = Guard::new(config.security.clone(), config.audit.clone(), &config.clusters)
            .map_err(|error| crate::error::McpError::InvalidConfig(error.to_string()))?;
        let query = Arc::new(QueryFacade::new(config.clone()).with_visibility_class("local"));
        Ok(Self {
            config,
            guard,
            query,
            runtime_context: None,
            telemetry: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    pub async fn bootstrap_typed(args: Args) -> Result<Self, crate::error::McpError> {
        let config = McpConfig::load_with_overrides(&args)?;
        let telemetry = init_tracing_typed(&config)?;
        let mut app = Self::new(config)?;
        *app.telemetry.lock().unwrap_or_else(|error| error.into_inner()) = Some(telemetry);
        app.start_background_services()?;
        Ok(app)
    }

    #[deprecated(since = "1.0.0", note = "use McpApp::bootstrap_typed")]
    pub async fn bootstrap(args: Args) -> anyhow::Result<Self> {
        Self::bootstrap_typed(args).await.map_err(anyhow::Error::new)
    }

    pub fn config(&self) -> &McpConfig {
        &self.config
    }

    pub fn guard(&self) -> &Guard {
        &self.guard
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
        let service_context = self
            .runtime_context
            .as_ref()
            .map(|runtime| runtime.service_context("rocketmq-mcp-lifecycle"))
            .ok_or_else(|| {
                crate::error::McpError::InvalidConfig(
                    "MCP lifecycle requires an initialized runtime context".to_string(),
                )
            })?;
        lifecycle.start(&service_context).await.map_err(|error| {
            crate::error::McpError::InvalidConfig(format!("failed to start MCP lifecycle boundary: {error}"))
        })
    }

    #[cfg(feature = "streamable-http")]
    pub(crate) fn service_context(
        &self,
        name: &'static str,
    ) -> Result<rocketmq_runtime::ServiceContext, crate::error::McpError> {
        self.runtime_context
            .as_ref()
            .map(|runtime| runtime.service_context(name))
            .ok_or_else(|| {
                crate::error::McpError::InvalidConfig(
                    "MCP transport requires an initialized runtime context".to_string(),
                )
            })
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
        let audit = self.guard.audit_log().close_and_drain(deadline).await;
        let runtime = if let Some(runtime_context) = &self.runtime_context {
            Some(runtime_context.shutdown_tasks_until(deadline).await)
        } else {
            None
        };
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

    fn start_background_services(&mut self) -> Result<(), crate::error::McpError> {
        let runtime_context = rocketmq_runtime::RuntimeContext::try_from_current("rocketmq-mcp").map_err(|error| {
            crate::error::McpError::InvalidConfig(format!("runtime initialization failed: {error}"))
        })?;
        let audit_service = runtime_context.service_context("rocketmq-mcp-audit");
        self.guard
            .audit_log()
            .start(&self.config.audit, &audit_service)
            .map_err(|error| crate::error::McpError::InvalidConfig(error.to_string()))?;
        self.runtime_context = Some(runtime_context);
        Ok(())
    }
}

pub fn init_tracing_typed(
    config: &McpConfig,
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
    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-mcp".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "mcp".to_string();
    bootstrap.observability.node_id = config.server.name.clone();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    bootstrap.logging.reload = config.logging.reload;
    let guard = rocketmq_observability::install_global_with_filter(&bootstrap, resolved_filter.clone())
        .map_err(|source| crate::error::McpError::infrastructure("install MCP telemetry", source))?;
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

#[deprecated(since = "1.0.0", note = "use init_tracing_typed")]
pub fn init_tracing(config: &McpConfig) -> anyhow::Result<()> {
    let guard = init_tracing_typed(config).map_err(anyhow::Error::new)?;
    *LEGACY_TELEMETRY_GUARD
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(guard);
    Ok(())
}
