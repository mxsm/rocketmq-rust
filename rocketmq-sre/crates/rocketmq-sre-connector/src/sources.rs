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

mod admin_query;
mod alertmanager;
mod auth_security_diagnostics;
mod broker_store_diagnostics;
mod canonical;
mod change_timeline;
mod common;
mod deployment_state;
mod diagnostic_projection;
mod inventory;
mod kubernetes;
mod kubernetes_events;
mod loki;
mod mcp;
mod projection;
mod prometheus;
mod proxy_diagnostics;
mod remoting_diagnostics;
mod required_signals;
mod runtime_diagnostics;
mod tempo;
mod topology;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ConnectorCapabilityState;
use rocketmq_sre_contracts::ConnectorSourceCapability;
use rocketmq_sre_contracts::ConnectorSourceStatus;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::current_evidence_schema;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Mutex;

pub(crate) use self::admin_query::AdminQuerySource;
use self::alertmanager::AlertmanagerSource;
use self::canonical::CanonicalQuery;
use self::canonical::CanonicalResourceRoute;
pub(crate) use self::common::CancelSignal;
pub(crate) use self::common::SourceOutput;
pub(crate) use self::common::bounded_future;
pub(crate) use self::common::bounded_response;
use self::common::max_duration;
pub(crate) use self::common::sanitize_and_bound;
pub(crate) use self::inventory::InventoryUpload;
use self::kubernetes::KubernetesSource;
use self::loki::LokiSource;
pub(crate) use self::mcp::McpSource;
use self::prometheus::PrometheusSource;
use self::required_signals::RequiredSignalsSource;
use self::runtime_diagnostics::RuntimeDiagnosticsSource;
use self::tempo::TempoSource;
use self::topology::TopologySource;
use crate::ConnectorConfig;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::EvidenceOperation;
use crate::mcp::McpGateway;
use crate::read_gateway::ConnectorReadGateway;
use crate::read_gateway::ReadAdapterKind;
use crate::read_gateway::ReadAuditTarget;
use crate::read_gateway::ReadContext;
use crate::read_gateway::ReadSession;

const CACHE_MAX_ENTRIES: usize = 1024;
const CONSUMER_LAG_HISTORY_MIN_INTERVAL: Duration = Duration::from_secs(1);
const CONSUMER_LAG_HISTORY_MAX_INTERVAL: Duration = Duration::from_secs(300);
const SOURCE_IDS: [&str; 10] = [
    "rocketmq-mcp",
    "admin-query",
    "alertmanager",
    "prometheus",
    "loki",
    "tempo",
    "kubernetes",
    "runtime",
    "required-signals",
    "topology",
];

#[derive(Clone)]
struct CacheEntry {
    expires_at: Instant,
    output: SourceOutput,
}

#[derive(Clone, Debug)]
struct ConsumerLagSample {
    observed_at: Instant,
    total_lag: f64,
    consume_rate_per_sec: f64,
}

#[derive(Clone, Debug)]
struct SourceRuntimeState {
    status: ConnectorSourceStatus,
    last_success_at: Option<DateTime<Utc>>,
    freshness_seconds: Option<u64>,
}

/// Read-only evidence registry with one canonical bounding, caching and
/// missing-evidence path, including the fixed component Required Signals
/// composition.
pub(crate) struct SourceManager<G> {
    config: Arc<ConnectorConfig>,
    read_gateway: ConnectorReadGateway<G>,
    alertmanager: AlertmanagerSource,
    prometheus: PrometheusSource,
    loki: LokiSource,
    tempo: TempoSource,
    kubernetes: KubernetesSource,
    runtime: RuntimeDiagnosticsSource,
    cache: Mutex<BTreeMap<String, CacheEntry>>,
    consumer_lag_history: Mutex<BTreeMap<String, ConsumerLagSample>>,
    state: Mutex<BTreeMap<&'static str, SourceRuntimeState>>,
}

impl<G> SourceManager<G>
where
    G: McpGateway,
{
    pub(crate) fn new(config: Arc<ConnectorConfig>, gateway: Arc<G>) -> Result<Self, ConnectorError> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(config.source_limits.max_concurrency)
            .user_agent(concat!("rocketmq-sre-connector/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| ConnectorError::configuration("evidence source HTTP client cannot be built"))?;
        let read_gateway = ConnectorReadGateway::new(&config, gateway);
        let alertmanager = AlertmanagerSource::new(
            http.clone(),
            config.alertmanager_url.clone(),
            config.pseudonymization_key(),
        );
        let prometheus = PrometheusSource::new(
            http.clone(),
            config.prometheus_url.clone(),
            config.source_limits.label_allowlist.clone(),
            config.source_limits.max_time_range,
        );
        let loki = LokiSource::new(
            http.clone(),
            config.loki_url.clone(),
            config.source_limits.label_allowlist.clone(),
        );
        let tempo = TempoSource::new(
            http.clone(),
            config.tempo_url.clone(),
            config.source_limits.label_allowlist.clone(),
        );
        let kubernetes = KubernetesSource::new(config.kubernetes_source.clone(), config.pseudonymization_key())?;
        let runtime = RuntimeDiagnosticsSource::new(http, &config);
        let mut state = BTreeMap::new();
        state.insert("rocketmq-mcp", initial_state(true));
        state.insert("admin-query", initial_state(read_gateway.admin_configured()));
        state.insert("alertmanager", initial_state(alertmanager.configured()));
        state.insert("prometheus", initial_state(prometheus.configured()));
        state.insert("loki", initial_state(loki.configured()));
        state.insert("tempo", initial_state(tempo.configured()));
        state.insert("kubernetes", initial_state(kubernetes.configured()));
        state.insert("runtime", initial_state(true));
        state.insert("required-signals", initial_state(true));
        state.insert("topology", initial_state(true));
        Ok(Self {
            read_gateway,
            alertmanager,
            prometheus,
            loki,
            tempo,
            kubernetes,
            runtime,
            cache: Mutex::new(BTreeMap::new()),
            consumer_lag_history: Mutex::new(BTreeMap::new()),
            state: Mutex::new(state),
            config,
        })
    }

    pub(crate) async fn initialize(&self, context: ChildServiceContext) {
        self.kubernetes.initialize(context.metadata_io().clone());
        if let Err(error) = self.read_gateway.initialize(context).await {
            self.record_failure("admin-query", error.code).await;
            tracing::warn!(
                code = error.code.as_str(),
                "read-only Admin evidence source is degraded"
            );
        }
    }

    pub(crate) async fn capabilities(&self) -> ConnectorCapabilityState {
        let state = self.state.lock().await;
        let limits = &self.config.source_limits;
        ConnectorCapabilityState {
            mutation_supported: false,
            sources: SOURCE_IDS
                .iter()
                .map(|source| {
                    let runtime = state.get(source).cloned().unwrap_or_else(|| initial_state(false));
                    ConnectorSourceCapability {
                        source: (*source).to_owned(),
                        schema_major: 1,
                        status: runtime.status,
                        max_rows: limits.max_rows as u32,
                        max_bytes: limits.max_bytes as u64,
                        max_time_range_seconds: limits.max_time_range.as_secs(),
                        last_success_at: runtime.last_success_at,
                        freshness_seconds: runtime.freshness_seconds,
                    }
                })
                .collect(),
        }
    }

    pub(crate) async fn query(
        &self,
        mut query: EvidenceQuery,
        external_cluster: &str,
        subject: &str,
        operation: Option<&EvidenceOperation>,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<EvidenceSnapshot, ConnectorError> {
        self.validate_bounds(&query, deadline)?;
        let context = ReadContext {
            tenant_id: query.tenant_id,
            cluster_id: query.cluster_id,
            external_cluster,
            subject,
            correlation_id: query.correlation_id,
            time_range_start: query.time_range.start,
            time_range_end: query.time_range.end,
            deadline,
            cancel,
        };
        let source = normalize_source(&query.source)?;
        let session = self.read_gateway.admit(&context, gateway_audit_target(source)).await?;
        let cache_key = cache_key(&query, external_cluster)?;
        if let Some(output) = self.cached(&cache_key).await {
            pseudonymize_evidence_resource(&mut query.resource, self.config.pseudonymization_key());
            return capture(query, output);
        }

        let started_at = Instant::now();
        // Source dispatch combines several large async implementations. Keep that state on the heap so
        // callers do not embed the complete dispatch future in their thread stack.
        let result = Box::pin(self.query_uncached(source, &query, operation, &session)).await;
        let mut output = match result {
            Ok(output) => {
                self.record_success(source, output.freshness_seconds).await;
                output
            }
            Err(error) if error.code == ConnectorErrorCode::SourceUnavailable => {
                self.record_failure(source, error.code).await;
                SourceOutput::missing(source)
            }
            Err(error) => {
                self.record_failure(source, error.code).await;
                return Err(error.with_correlation_id(query.correlation_id));
            }
        };
        if output.exposure == EvidenceExposure::Unknown {
            output.exposure = exposure_for_source(source);
        }
        let (content, bounded) = sanitize_and_bound(
            output.content,
            self.config.source_limits.max_rows,
            self.config.source_limits.max_bytes,
            self.config.pseudonymization_key(),
        )
        .map_err(|error| error.with_correlation_id(query.correlation_id))?;
        output.content = content;
        if bounded {
            output.partial = true;
            if output.coverage == CoverageStatus::Available {
                output.coverage = CoverageStatus::Partial;
            }
            output.warnings.push("source_output_bounded".to_owned());
        }
        output.warnings.sort();
        output.warnings.dedup();
        output.warnings.truncate(8);
        tracing::debug!(
            source,
            latency_ms = started_at.elapsed().as_millis() as u64,
            partial = output.partial,
            "bounded evidence source query completed"
        );
        validate_query_completion(&context)?;
        self.insert_cache(cache_key, output.clone()).await;
        pseudonymize_evidence_resource(&mut query.resource, self.config.pseudonymization_key());
        capture(query, output)
    }

    pub(crate) async fn inventory(
        &self,
        cluster_id: ClusterId,
        external_cluster: &str,
        subject: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<InventoryUpload, ConnectorError> {
        let observed_at = Utc::now();
        let context = ReadContext {
            tenant_id: self.config.tenant_id,
            cluster_id,
            external_cluster,
            subject,
            correlation_id: rocketmq_sre_contracts::CorrelationId::new(),
            time_range_start: observed_at,
            time_range_end: observed_at,
            deadline,
            cancel,
        };
        let session = self
            .read_gateway
            .admit(&context, Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "inventory")))
            .await?;
        inventory::collect(
            &self.read_gateway,
            &self.kubernetes,
            cluster_id,
            self.config.source_limits.max_rows,
            self.config.source_limits.max_bytes,
            self.config.pseudonymization_key(),
            &session,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "query dispatch keeps every security bound explicit"
    )]
    async fn query_uncached(
        &self,
        source: &'static str,
        query: &EvidenceQuery,
        operation: Option<&EvidenceOperation>,
        session: &ReadSession<'_, '_>,
    ) -> Result<SourceOutput, ConnectorError> {
        if let Some(route) = canonical::resolve(source, &query.resource) {
            return self.query_canonical(source, route, query, session).await;
        }
        self.query_legacy_uncached(source, query, operation, session).await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical query dispatch keeps every security bound explicit"
    )]
    async fn query_canonical(
        &self,
        source: &'static str,
        route: CanonicalResourceRoute,
        query: &EvidenceQuery,
        session: &ReadSession<'_, '_>,
    ) -> Result<SourceOutput, ConnectorError> {
        let context = session.context();
        let CanonicalResourceRoute::Query {
            query: canonical_query,
            projection,
        } = route
        else {
            return Ok(SourceOutput::not_production_verified(
                source,
                route.reason_code().unwrap_or("canonical_read_source_unavailable"),
            ));
        };

        let output = match canonical_query {
            CanonicalQuery::Mcp(operation) => self.read_gateway.mcp_query(session, &operation).await,
            CanonicalQuery::Admin(resource) => self.read_gateway.admin_query(session, &resource).await,
            CanonicalQuery::Prometheus { resource, matchers } => {
                self.prometheus
                    .query_with_matchers(
                        context.external_cluster,
                        &resource,
                        &matchers,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            CanonicalQuery::Kubernetes(resource) => {
                self.kubernetes
                    .query(
                        context.external_cluster,
                        &resource,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            CanonicalQuery::Runtime(resource) => self.runtime.query(&self.read_gateway, session, &resource).await,
        }
        .inspect_err(|error| {
            tracing::warn!(
                source,
                projection = ?projection,
                stage = "source_query",
                code = error.code.as_str(),
                retryable = error.retryable,
                "canonical read-only evidence query stage failed"
            );
        })?;
        let source_was_partial = output.partial;
        let mut projected = projection::apply(output, projection).inspect_err(|error| {
            tracing::warn!(
                source,
                projection = ?projection,
                stage = "projection",
                code = error.code.as_str(),
                retryable = error.retryable,
                "canonical read-only evidence query stage failed"
            );
        })?;
        if projection == canonical::CanonicalProjection::ConsumerLag {
            self.enrich_consumer_lag_rates(
                context.external_cluster,
                &query.resource,
                &mut projected,
                source_was_partial,
            )
            .await?;
        }
        Ok(projected)
    }

    async fn enrich_consumer_lag_rates(
        &self,
        external_cluster: &str,
        resource: &str,
        output: &mut SourceOutput,
        source_was_partial: bool,
    ) -> Result<(), ConnectorError> {
        let observed_at = Instant::now();
        let sample = consumer_lag_sample(&output.content, observed_at)?;
        let key = format!("{external_cluster}\u{0}{resource}");
        let previous = {
            let mut history = self.consumer_lag_history.lock().await;
            let previous = history.insert(key, sample.clone());
            if history.len() > CACHE_MAX_ENTRIES
                && let Some(oldest) = history
                    .iter()
                    .min_by_key(|(_, sample)| sample.observed_at)
                    .map(|(key, _)| key.clone())
            {
                history.remove(&oldest);
            }
            previous
        };
        if let Some(previous) = previous {
            augment_consumer_lag_rates(output, &previous, &sample, source_was_partial);
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "legacy query dispatch keeps every security bound explicit"
    )]
    async fn query_legacy_uncached(
        &self,
        source: &'static str,
        query: &EvidenceQuery,
        operation: Option<&EvidenceOperation>,
        session: &ReadSession<'_, '_>,
    ) -> Result<SourceOutput, ConnectorError> {
        let context = session.context();
        match source {
            "rocketmq-mcp" => {
                let derived;
                let operation = match operation {
                    Some(operation) => operation,
                    None => {
                        derived = mcp_operation(&query.resource)?;
                        &derived
                    }
                };
                self.read_gateway.mcp_query(session, operation).await
            }
            "admin-query" => {
                if let Ok(operation) = mcp_operation(&query.resource) {
                    match self.read_gateway.mcp_query(session, &operation).await {
                        Ok(output) => return Ok(output),
                        Err(error) if error.code == ConnectorErrorCode::SourceUnavailable => {}
                        Err(error) => return Err(error),
                    }
                }
                self.read_gateway.admin_query(session, &query.resource).await
            }
            "alertmanager" => {
                self.alertmanager
                    .query(
                        context.external_cluster,
                        &query.resource,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            "prometheus" => match query.resource.as_str() {
                "proxy/diagnostics" | "proxy-diagnostics" => {
                    proxy_diagnostics::query(
                        &self.prometheus,
                        context.external_cluster,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
                }
                "remoting/diagnostics" | "remoting-diagnostics" => {
                    remoting_diagnostics::query(
                        &self.prometheus,
                        context.external_cluster,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
                }
                _ => {
                    self.prometheus
                        .query(
                            context.external_cluster,
                            &query.resource,
                            query.time_range.start,
                            query.time_range.end,
                            self.config.source_limits.max_rows,
                            self.config.source_limits.max_bytes,
                            context.deadline,
                            context.cancel,
                        )
                        .await
                }
            },
            "loki" => {
                self.loki
                    .query(
                        context.external_cluster,
                        &query.resource,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            "tempo" => {
                self.tempo
                    .query(
                        context.external_cluster,
                        &query.resource,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            "kubernetes" => {
                self.kubernetes
                    .query(
                        context.external_cluster,
                        &query.resource,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            "runtime" => self.runtime.query(&self.read_gateway, session, &query.resource).await,
            "required-signals" => {
                RequiredSignalsSource::query(
                    &self.prometheus,
                    &self.loki,
                    &self.tempo,
                    &self.read_gateway,
                    &self.runtime,
                    session,
                    &query.resource,
                    query.time_range.start,
                    query.time_range.end,
                    self.config.source_limits.max_rows,
                    self.config.source_limits.max_bytes,
                )
                .await
            }
            "topology" => TopologySource::query(&self.read_gateway, session, &query.resource).await,
            _ => Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "unknown evidence source",
            )),
        }
    }

    fn validate_bounds(&self, query: &EvidenceQuery, deadline: DateTime<Utc>) -> Result<(), ConnectorError> {
        if query.time_range.start > query.time_range.end {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "evidence time range starts after it ends",
            ));
        }
        if max_duration(query.time_range.start, query.time_range.end) > self.config.source_limits.max_time_range {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "evidence time range exceeds the configured source bound",
            ));
        }
        if deadline <= Utc::now()
            || deadline
                .signed_duration_since(Utc::now())
                .to_std()
                .is_ok_and(|duration| duration > self.config.source_limits.max_deadline)
        {
            return Err(ConnectorError::new(
                ConnectorErrorCode::DeadlineExceeded,
                true,
                "evidence deadline is elapsed or exceeds the configured bound",
            ));
        }
        Ok(())
    }

    async fn cached(&self, key: &str) -> Option<SourceOutput> {
        let now = Instant::now();
        let mut cache = self.cache.lock().await;
        cache.retain(|_, entry| entry.expires_at > now);
        cache.get(key).map(|entry| entry.output.clone())
    }

    async fn insert_cache(&self, key: String, output: SourceOutput) {
        let mut cache = self.cache.lock().await;
        if cache.len() >= CACHE_MAX_ENTRIES
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(
            key,
            CacheEntry {
                expires_at: Instant::now() + self.config.source_limits.cache_ttl,
                output,
            },
        );
    }

    async fn record_success(&self, source: &'static str, freshness_seconds: u64) {
        self.state.lock().await.insert(
            source,
            SourceRuntimeState {
                status: ConnectorSourceStatus::Queryable,
                last_success_at: Some(Utc::now()),
                freshness_seconds: Some(freshness_seconds),
            },
        );
    }

    async fn record_failure(&self, source: &'static str, _code: ConnectorErrorCode) {
        let mut state = self.state.lock().await;
        let existing = state.get(source).cloned().unwrap_or_else(|| initial_state(false));
        state.insert(
            source,
            SourceRuntimeState {
                status: runtime_failure_status(&existing),
                last_success_at: existing.last_success_at,
                freshness_seconds: existing.freshness_seconds,
            },
        );
    }

    pub(crate) async fn shutdown(&self) {
        self.read_gateway.shutdown().await;
    }
}

fn validate_query_completion(context: &ReadContext<'_>) -> Result<(), ConnectorError> {
    if context.cancel.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorCode::QueryCancelled,
            false,
            "evidence query was cancelled before cache publication",
        )
        .with_correlation_id(context.correlation_id));
    }
    if Utc::now() >= context.deadline {
        return Err(ConnectorError::new(
            ConnectorErrorCode::DeadlineExceeded,
            true,
            "evidence deadline elapsed before cache publication",
        )
        .with_correlation_id(context.correlation_id));
    }
    Ok(())
}

fn consumer_lag_sample(content: &Value, observed_at: Instant) -> Result<ConsumerLagSample, ConnectorError> {
    let content = content.as_object().ok_or_else(consumer_lag_history_schema_mismatch)?;
    let total_lag = content
        .get("total_lag")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(consumer_lag_history_schema_mismatch)?;
    let consume_rate_per_sec = content
        .get("consume_rate_per_sec")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(consumer_lag_history_schema_mismatch)?;
    Ok(ConsumerLagSample {
        observed_at,
        total_lag,
        consume_rate_per_sec,
    })
}

fn consumer_lag_history_schema_mismatch() -> ConnectorError {
    ConnectorError::capability(
        ConnectorErrorCode::CapabilityMismatch,
        "consumer lag projection cannot supply a bounded rate-history sample",
    )
}

fn augment_consumer_lag_rates(
    output: &mut SourceOutput,
    previous: &ConsumerLagSample,
    current: &ConsumerLagSample,
    source_was_partial: bool,
) {
    let Some(elapsed) = current.observed_at.checked_duration_since(previous.observed_at) else {
        return;
    };
    if !(CONSUMER_LAG_HISTORY_MIN_INTERVAL..=CONSUMER_LAG_HISTORY_MAX_INTERVAL).contains(&elapsed) {
        return;
    }
    let elapsed_seconds = elapsed.as_secs_f64();
    let lag_delta_per_sec = (current.total_lag - previous.total_lag) / elapsed_seconds;
    let lag_slope_per_min = lag_delta_per_sec * 60.0;
    let estimated_produce_rate = current.consume_rate_per_sec + lag_delta_per_sec;
    if !lag_slope_per_min.is_finite() || !estimated_produce_rate.is_finite() || estimated_produce_rate < -0.001 {
        output
            .warnings
            .push("consumer_lag_rate_history_inconsistent".to_owned());
        return;
    }
    let Some(content) = output.content.as_object_mut() else {
        return;
    };
    content.insert("lag_slope_per_min".to_owned(), Value::from(lag_slope_per_min));
    content.insert(
        "produce_rate_per_sec".to_owned(),
        Value::from(estimated_produce_rate.max(0.0)),
    );
    output
        .warnings
        .retain(|warning| warning != "consumer_lag_rate_history_unavailable");
    let required_fields_available = [
        "total_lag",
        "lag_slope_per_min",
        "queue_skew_ratio",
        "consume_rate_per_sec",
        "produce_rate_per_sec",
    ]
    .iter()
    .all(|field| content.get(*field).is_some_and(Value::is_number));
    if !source_was_partial && output.warnings.is_empty() && required_fields_available {
        output.partial = false;
        output.coverage = CoverageStatus::Available;
    }
}

fn runtime_failure_status(existing: &SourceRuntimeState) -> ConnectorSourceStatus {
    match existing.status {
        // A configured or previously healthy source remains dispatchable while
        // degraded. Treating one bounded query failure as globally missing
        // would prevent the next read from recovering the source.
        ConnectorSourceStatus::Queryable | ConnectorSourceStatus::Degraded => ConnectorSourceStatus::Degraded,
        ConnectorSourceStatus::Missing | ConnectorSourceStatus::Unsupported if existing.last_success_at.is_some() => {
            ConnectorSourceStatus::Degraded
        }
        status => status,
    }
}

fn initial_state(configured: bool) -> SourceRuntimeState {
    SourceRuntimeState {
        status: if configured {
            ConnectorSourceStatus::Queryable
        } else {
            ConnectorSourceStatus::Missing
        },
        last_success_at: None,
        freshness_seconds: None,
    }
}

fn normalize_source(source: &str) -> Result<&'static str, ConnectorError> {
    match source {
        "rocketmq-mcp" | "mcp" | "rocketmq_mcp" => Ok("rocketmq-mcp"),
        "admin-query" | "admin_query" | "rocketmq-admin-read" => Ok("admin-query"),
        "alertmanager" | "alerts" => Ok("alertmanager"),
        "prometheus" => Ok("prometheus"),
        "loki" => Ok("loki"),
        "tempo" => Ok("tempo"),
        "kubernetes" | "k8s" => Ok("kubernetes"),
        "runtime" | "runtime-diagnostics" => Ok("runtime"),
        "required-signals" | "required_signals" | "component-signals" => Ok("required-signals"),
        "topology" => Ok("topology"),
        _ => Err(ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "evidence source is not registered",
        )),
    }
}

fn gateway_audit_target(source: &'static str) -> Option<ReadAuditTarget> {
    match source {
        "rocketmq-mcp" => Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "logical_query")),
        "admin-query" => Some(ReadAuditTarget::new(ReadAdapterKind::Admin, "logical_query")),
        "runtime" => Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "runtime_diagnostics")),
        "required-signals" => Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "required_signals")),
        "topology" => Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "topology")),
        _ => None,
    }
}

fn mcp_operation(resource: &str) -> Result<EvidenceOperation, ConnectorError> {
    if matches!(
        resource,
        "cluster/overview" | "topology" | "topology/cluster" | "admin/brokers"
    ) {
        return Ok(EvidenceOperation::ClusterOverview);
    }
    if matches!(resource, "topics" | "admin/topics") {
        return Ok(EvidenceOperation::TopicList {
            filter: None,
            limit: Some(200),
            cursor: None,
        });
    }
    if matches!(resource, "consumer-groups" | "admin/consumer-groups") {
        return Ok(EvidenceOperation::ConsumerGroupList {
            filter: None,
            limit: Some(200),
            cursor: None,
        });
    }
    if let Some(topic) = resource.strip_prefix("topics/") {
        return Ok(EvidenceOperation::TopicDescribe {
            topic: topic.to_owned(),
            limit: Some(200),
            cursor: None,
        });
    }
    if let Some(broker_name) = resource.strip_prefix("brokers/") {
        return Ok(EvidenceOperation::BrokerDescribe {
            broker_name: broker_name.to_owned(),
        });
    }
    if let Some(value) = resource.strip_prefix("consumer-groups/") {
        let parts = value.split('/').collect::<Vec<_>>();
        if let [consumer_group, "lag", topic] = parts.as_slice() {
            return Ok(EvidenceOperation::ConsumerLag {
                topic: (*topic).to_owned(),
                consumer_group: (*consumer_group).to_owned(),
                limit: Some(200),
                cursor: None,
            });
        }
    }
    if let Some((consumer_group, topic)) = resource
        .strip_prefix("admin/consumer-lag/")
        .and_then(|value| value.split_once('/'))
    {
        return Ok(EvidenceOperation::ConsumerLag {
            topic: topic.to_owned(),
            consumer_group: consumer_group.to_owned(),
            limit: Some(200),
            cursor: None,
        });
    }
    Err(ConnectorError::new(
        ConnectorErrorCode::InvalidEvidenceQuery,
        false,
        "resource is not represented by a read-only MCP operation",
    ))
}

#[derive(Serialize)]
struct CacheMaterial<'a> {
    source: &'a str,
    resource: &'a str,
    external_cluster: &'a str,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn cache_key(query: &EvidenceQuery, external_cluster: &str) -> Result<String, ConnectorError> {
    let material = CacheMaterial {
        source: &query.source,
        resource: &query.resource,
        external_cluster,
        cluster_id: query.cluster_id,
        start: query.time_range.start,
        end: query.time_range.end,
    };
    let canonical = serde_jcs::to_vec(&material)
        .map_err(|_| ConnectorError::source("evidence query cache key cannot be canonicalized"))?;
    Ok(format!(
        "sha256:{}",
        rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(canonical))
    ))
}

fn pseudonymize_evidence_resource(resource: &mut String, pseudonym_key: &[u8]) {
    let Some(value) = resource.strip_prefix("message-metadata/") else {
        return;
    };
    let (prefix, identifier) = value.rsplit_once('/').unwrap_or(("", value));
    let identifier = self::common::pseudonymize_identifier(identifier, pseudonym_key);
    *resource = if prefix.is_empty() {
        format!("message-metadata/{identifier}")
    } else {
        format!("message-metadata/{prefix}/{identifier}")
    };
}

fn capture(query: EvidenceQuery, output: SourceOutput) -> Result<EvidenceSnapshot, ConnectorError> {
    let correlation_id = query.correlation_id;
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        output.observed_at,
        EvidenceContent::Inline(output.content),
    )
    .map_err(|_| {
        ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "canonical evidence capture failed",
        )
        .with_correlation_id(correlation_id)
    })?;
    snapshot.freshness_seconds = output.freshness_seconds;
    snapshot.partial = output.partial;
    snapshot.warnings = output.warnings;
    snapshot.sensitivity = output.sensitivity;
    snapshot.coverage = output.coverage;
    snapshot.exposure = output.exposure;
    Ok(snapshot)
}

fn exposure_for_source(source: &str) -> EvidenceExposure {
    match source {
        "rocketmq-mcp" => EvidenceExposure::McpTool,
        "admin-query" => EvidenceExposure::AdminRpc,
        "alertmanager" => EvidenceExposure::AlertmanagerApi,
        "prometheus" => EvidenceExposure::PrometheusApi,
        "loki" => EvidenceExposure::LokiApi,
        "tempo" => EvidenceExposure::TempoApi,
        "kubernetes" => EvidenceExposure::KubernetesApi,
        "runtime" => EvidenceExposure::RuntimeDiagnostics,
        "required-signals" => EvidenceExposure::RequiredSignals,
        "topology" => EvidenceExposure::McpTool,
        _ => EvidenceExposure::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::TimeZone;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::CoverageStatus;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::TimeRange;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::VerifiedCapability;
    use crate::WireEvidenceEnvelope;
    use crate::config::ConnectorAuth;
    use crate::config::SecretValue;

    #[test]
    fn runtime_failure_degrades_configured_source_but_preserves_missing_configuration() {
        let configured = initial_state(true);
        assert_eq!(runtime_failure_status(&configured), ConnectorSourceStatus::Degraded);

        let unconfigured = initial_state(false);
        assert_eq!(runtime_failure_status(&unconfigured), ConnectorSourceStatus::Missing);

        let previously_healthy = SourceRuntimeState {
            status: ConnectorSourceStatus::Missing,
            last_success_at: Some(Utc::now()),
            freshness_seconds: Some(0),
        };
        assert_eq!(
            runtime_failure_status(&previously_healthy),
            ConnectorSourceStatus::Degraded
        );
    }

    #[test]
    fn consumer_lag_history_supplies_measured_rates_without_neutral_defaults() {
        let observed_at = Instant::now();
        let previous = ConsumerLagSample {
            observed_at,
            total_lag: 10.0,
            consume_rate_per_sec: 1.0,
        };
        let current = ConsumerLagSample {
            observed_at: observed_at + Duration::from_secs(2),
            total_lag: 14.0,
            consume_rate_per_sec: 1.0,
        };
        let mut output = SourceOutput::available(
            json!({
                "total_lag": 14,
                "queue_skew_ratio": 1.0,
                "consume_rate_per_sec": 1.0
            }),
            Utc::now(),
        );
        output.partial = true;
        output.coverage = CoverageStatus::Partial;
        output.warnings = vec!["consumer_lag_rate_history_unavailable".to_owned()];

        augment_consumer_lag_rates(&mut output, &previous, &current, false);

        assert_eq!(output.content["lag_slope_per_min"], 120.0);
        assert_eq!(output.content["produce_rate_per_sec"], 3.0);
        assert!(!output.partial);
        assert_eq!(output.coverage, CoverageStatus::Available);
        assert!(output.warnings.is_empty());
    }

    #[test]
    fn consumer_lag_history_keeps_single_or_too_recent_samples_partial() {
        let observed_at = Instant::now();
        let previous = ConsumerLagSample {
            observed_at,
            total_lag: 10.0,
            consume_rate_per_sec: 0.0,
        };
        let current = ConsumerLagSample {
            observed_at: observed_at + Duration::from_millis(500),
            total_lag: 10.0,
            consume_rate_per_sec: 0.0,
        };
        let mut output = SourceOutput::available(
            json!({
                "total_lag": 10,
                "queue_skew_ratio": 1.0,
                "consume_rate_per_sec": 0.0
            }),
            Utc::now(),
        );
        output.partial = true;
        output.coverage = CoverageStatus::Partial;
        output.warnings = vec!["consumer_lag_rate_history_unavailable".to_owned()];

        augment_consumer_lag_rates(&mut output, &previous, &current, false);

        assert!(output.content.get("lag_slope_per_min").is_none());
        assert!(output.content.get("produce_rate_per_sec").is_none());
        assert!(output.partial);
        assert_eq!(output.coverage, CoverageStatus::Partial);
        assert_eq!(output.warnings, ["consumer_lag_rate_history_unavailable"]);
    }

    struct NoQueryGateway;

    impl McpGateway for NoQueryGateway {
        async fn handshake(&self) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
            Ok(BTreeMap::new())
        }

        async fn query(
            &self,
            _cluster: &str,
            _operation: &EvidenceOperation,
        ) -> Result<WireEvidenceEnvelope, ConnectorError> {
            panic!("message metadata must never issue an MCP query")
        }

        async fn close(&self) {}
    }

    #[test]
    fn cache_key_excludes_request_identifiers() {
        let at = Utc.with_ymd_and_hms(2026, 7, 27, 8, 0, 0).single().expect("time");
        let cluster_id = ClusterId::new();
        let mut first = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: TenantId::new(),
            cluster_id,
            source: "prometheus".to_owned(),
            resource: "metrics/rocketmq_broker_up".to_owned(),
            time_range: TimeRange::new(at, at).expect("range"),
        };
        let first_key = cache_key(&first, "local").expect("key");
        first.query_id = QueryId::new();
        first.correlation_id = CorrelationId::new();
        first.tenant_id = TenantId::new();
        assert_eq!(cache_key(&first, "local").expect("key"), first_key);
    }

    #[test]
    fn evidence_resource_retains_topic_but_pseudonymizes_message_id() {
        let mut resource = "message-metadata/orders/raw-message-id".to_owned();
        pseudonymize_evidence_resource(&mut resource, b"tenant-key");

        assert!(resource.starts_with("message-metadata/orders/sha256:"));
        assert!(!resource.contains("raw-message-id"));
        let first = resource.clone();
        pseudonymize_evidence_resource(&mut resource, b"tenant-key");
        assert_eq!(resource, first);
    }

    #[test]
    fn mcp_parser_accepts_only_fixed_read_operations() {
        assert!(matches!(
            mcp_operation("consumer-groups/billing/lag/orders"),
            Ok(EvidenceOperation::ConsumerLag { .. })
        ));
        assert!(mcp_operation("messages/raw-body").is_err());
    }

    #[tokio::test]
    async fn manager_pseudonymizes_message_metadata_resource_when_identifiers_are_incomplete() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let config = Arc::new(ConnectorConfig {
            bind_addr: "127.0.0.1:8091".parse().expect("socket"),
            mcp_url: Url::parse("http://127.0.0.1:8089/mcp").expect("URL"),
            mcp_ca_path: None,
            mcp_ca_pem: Vec::new(),
            auth: ConnectorAuth::DevelopmentToken {
                token_env: "TEST_MCP_TOKEN".to_owned(),
                token: SecretValue::new("mcp-token".to_owned()),
            },
            tenant_id,
            cluster_allowlist: BTreeSet::from(["local".to_owned()]),
            cluster_ids: BTreeMap::from([("local".to_owned(), cluster_id)]),
            request_timeout: Duration::from_secs(1),
            handshake_interval: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            max_concurrency: 1,
            max_response_bytes: 4096,
            expected_tool_surface_digest: None,
            prometheus_url: None,
            alertmanager_url: None,
            loki_url: None,
            tempo_url: None,
            admin_source: None,
            kubernetes_source: None,
            runtime_diagnostics_source: None,
            source_limits: crate::config::test_source_limits(1, 4096),
            internal_token_env: "TEST_INTERNAL_TOKEN".to_owned(),
            internal_token: SecretValue::new("internal-token".to_owned()),
            control_plane: None,
        });
        let manager = SourceManager::new(config, Arc::new(NoQueryGateway)).expect("manager");
        let at = Utc.with_ymd_and_hms(2026, 7, 27, 8, 0, 0).single().expect("time");
        let resource = "message-metadata/id-hash-a";
        let snapshot = manager
            .query(
                EvidenceQuery {
                    query_id: QueryId::new(),
                    correlation_id: CorrelationId::new(),
                    tenant_id,
                    cluster_id,
                    source: "admin-query".to_owned(),
                    resource: resource.to_owned(),
                    time_range: TimeRange::new(at, at).expect("range"),
                },
                "local",
                "test-subject",
                None,
                Utc::now() + chrono::Duration::seconds(2),
                &CancelSignal::default(),
            )
            .await
            .expect("fail-closed evidence");

        assert!(snapshot.resource.starts_with("message-metadata/sha256:"));
        assert!(!snapshot.resource.contains("id-hash-a"));
        assert_eq!(snapshot.coverage, CoverageStatus::NotProductionVerified);
        assert!(snapshot.partial);
        let EvidenceContent::Inline(content) = &snapshot.content else {
            panic!("connector source evidence must remain inline");
        };
        assert_eq!(
            content.get("status").and_then(serde_json::Value::as_str),
            Some("not_production_verified")
        );
    }
}
