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
use crate::guard::Guard;

#[derive(Debug, Clone)]
pub struct McpApp {
    config: McpConfig,
    guard: Guard,
    query: Arc<QueryFacade<AdminCoreSessionFactory>>,
    runtime_context: Option<rocketmq_runtime::RuntimeContext>,
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
        })
    }

    pub async fn bootstrap(args: Args) -> anyhow::Result<Self> {
        let config = McpConfig::load_with_overrides(&args)?;
        init_tracing(&config)?;
        let mut app = Self::new(config)?;
        app.start_background_services()?;
        Ok(app)
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
            audit_dropped = audit.dropped,
            audit_sink_failures = audit.sink_failures,
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
        if let Some(runtime_context) = &self.runtime_context {
            let report = runtime_context.shutdown_tasks(std::time::Duration::from_secs(10)).await;
            report.log_if_unhealthy();
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

pub fn init_tracing(config: &McpConfig) -> anyhow::Result<()> {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_new(&config.server.log_level)?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);

    Ok(())
}
