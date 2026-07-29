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
use std::collections::VecDeque;
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
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use self::admin_query::AdminQuerySource;
use self::alertmanager::AlertmanagerSource;
use self::canonical::CanonicalQuery;
use self::canonical::CanonicalResourceRoute;
pub(crate) use self::common::CancelSignal;
use self::common::SourceOutput;
pub(crate) use self::common::bounded_response;
use self::common::max_duration;
use self::common::sanitize_and_bound;
pub(crate) use self::inventory::InventoryUpload;
use self::kubernetes::KubernetesSource;
use self::loki::LokiSource;
use self::mcp::McpSource;
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

const CACHE_MAX_ENTRIES: usize = 1024;
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
struct SourceRuntimeState {
    status: ConnectorSourceStatus,
    last_success_at: Option<DateTime<Utc>>,
    freshness_seconds: Option<u64>,
}

struct QueryAdmission {
    concurrency: Semaphore,
    recent: Mutex<VecDeque<Instant>>,
    max_per_minute: usize,
}

impl QueryAdmission {
    fn new(max_concurrency: usize, max_per_minute: usize) -> Self {
        Self {
            concurrency: Semaphore::new(max_concurrency),
            recent: Mutex::new(VecDeque::with_capacity(max_per_minute.min(1024))),
            max_per_minute,
        }
    }

    async fn rate_limit(&self) -> Result<(), ConnectorError> {
        let now = Instant::now();
        let mut recent = self.recent.lock().await;
        while recent
            .front()
            .is_some_and(|instant| now.duration_since(*instant) >= Duration::from_secs(60))
        {
            recent.pop_front();
        }
        if recent.len() >= self.max_per_minute {
            return Err(ConnectorError::new(
                ConnectorErrorCode::RateLimited,
                true,
                "connector evidence rate budget is exhausted",
            ));
        }
        recent.push_back(now);
        Ok(())
    }
}

/// Read-only evidence registry with one canonical bounding, caching and
/// missing-evidence path, including the fixed component Required Signals
/// composition.
pub(crate) struct SourceManager<G> {
    config: Arc<ConnectorConfig>,
    mcp: McpSource<G>,
    admin: AdminQuerySource,
    alertmanager: AlertmanagerSource,
    prometheus: PrometheusSource,
    loki: LokiSource,
    tempo: TempoSource,
    kubernetes: KubernetesSource,
    admission: QueryAdmission,
    cache: Mutex<BTreeMap<String, CacheEntry>>,
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
        let admin = AdminQuerySource::new(config.admin_source.clone());
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
            http,
            config.tempo_url.clone(),
            config.source_limits.label_allowlist.clone(),
        );
        let kubernetes = KubernetesSource::new(config.kubernetes_source.clone(), config.pseudonymization_key())?;
        let mut state = BTreeMap::new();
        state.insert("rocketmq-mcp", initial_state(true));
        state.insert("admin-query", initial_state(admin.configured()));
        state.insert("alertmanager", initial_state(alertmanager.configured()));
        state.insert("prometheus", initial_state(prometheus.configured()));
        state.insert("loki", initial_state(loki.configured()));
        state.insert("tempo", initial_state(tempo.configured()));
        state.insert("kubernetes", initial_state(kubernetes.configured()));
        state.insert("runtime", initial_state(true));
        state.insert("required-signals", initial_state(true));
        state.insert("topology", initial_state(true));
        Ok(Self {
            admission: QueryAdmission::new(
                config.source_limits.max_concurrency,
                config.source_limits.max_requests_per_minute,
            ),
            mcp: McpSource::new(gateway),
            admin,
            alertmanager,
            prometheus,
            loki,
            tempo,
            kubernetes,
            cache: Mutex::new(BTreeMap::new()),
            state: Mutex::new(state),
            config,
        })
    }

    pub(crate) async fn initialize(&self, context: ChildServiceContext) {
        self.kubernetes.initialize(context.metadata_io().clone());
        if let Err(error) = self.admin.start(context.child("admin-query")).await {
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
        query: EvidenceQuery,
        external_cluster: &str,
        operation: Option<&EvidenceOperation>,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<EvidenceSnapshot, ConnectorError> {
        self.validate_bounds(&query, deadline)?;
        let source = normalize_source(&query.source)?;
        let cache_key = cache_key(&query, external_cluster)?;
        if let Some(output) = self.cached(&cache_key).await {
            return capture(query, output);
        }

        self.admission.rate_limit().await?;
        let _permit = common::bounded_future(deadline, cancel, async {
            self.admission
                .concurrency
                .acquire()
                .await
                .map_err(|_| ConnectorError::source("connector concurrency limiter is closed"))
        })
        .await?;
        let started_at = Instant::now();
        let result = self
            .query_uncached(source, &query, external_cluster, operation, deadline, cancel)
            .await;
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
        self.insert_cache(cache_key, output.clone()).await;
        capture(query, output)
    }

    pub(crate) async fn inventory(
        &self,
        cluster_id: ClusterId,
        external_cluster: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<InventoryUpload, ConnectorError> {
        self.admission.rate_limit().await?;
        let _permit = common::bounded_future(deadline, cancel, async {
            self.admission
                .concurrency
                .acquire()
                .await
                .map_err(|_| ConnectorError::source("connector concurrency limiter is closed"))
        })
        .await?;
        inventory::collect(
            &self.mcp,
            &self.admin,
            &self.kubernetes,
            cluster_id,
            external_cluster,
            self.config.source_limits.max_rows,
            self.config.source_limits.max_bytes,
            self.config.pseudonymization_key(),
            deadline,
            cancel,
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
        external_cluster: &str,
        operation: Option<&EvidenceOperation>,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        if let Some(route) = canonical::resolve(source, &query.resource) {
            return self
                .query_canonical(source, route, external_cluster, query, deadline, cancel)
                .await;
        }
        self.query_legacy_uncached(source, query, external_cluster, operation, deadline, cancel)
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical query dispatch keeps every security bound explicit"
    )]
    async fn query_canonical(
        &self,
        source: &'static str,
        route: CanonicalResourceRoute,
        external_cluster: &str,
        query: &EvidenceQuery,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
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
            CanonicalQuery::Mcp(operation) => self.mcp.query(external_cluster, &operation, deadline, cancel).await,
            CanonicalQuery::Admin(resource) => self.admin.query(external_cluster, &resource, deadline, cancel).await,
            CanonicalQuery::Prometheus { resource, matchers } => {
                self.prometheus
                    .query_with_matchers(
                        external_cluster,
                        &resource,
                        &matchers,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
            }
            CanonicalQuery::Kubernetes(resource) => {
                self.kubernetes
                    .query(
                        external_cluster,
                        &resource,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
            }
            CanonicalQuery::Runtime(resource) => {
                RuntimeDiagnosticsSource::query(&self.mcp, &resource, deadline, cancel).await
            }
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
        projection::apply(output, projection).inspect_err(|error| {
            tracing::warn!(
                source,
                projection = ?projection,
                stage = "projection",
                code = error.code.as_str(),
                retryable = error.retryable,
                "canonical read-only evidence query stage failed"
            );
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "legacy query dispatch keeps every security bound explicit"
    )]
    async fn query_legacy_uncached(
        &self,
        source: &'static str,
        query: &EvidenceQuery,
        external_cluster: &str,
        operation: Option<&EvidenceOperation>,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
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
                self.mcp.query(external_cluster, operation, deadline, cancel).await
            }
            "admin-query" => {
                if let Ok(operation) = mcp_operation(&query.resource) {
                    match self.mcp.query(external_cluster, &operation, deadline, cancel).await {
                        Ok(output) => return Ok(output),
                        Err(error) if error.code == ConnectorErrorCode::SourceUnavailable => {}
                        Err(error) => return Err(error),
                    }
                }
                self.admin
                    .query(external_cluster, &query.resource, deadline, cancel)
                    .await
            }
            "alertmanager" => {
                self.alertmanager
                    .query(
                        external_cluster,
                        &query.resource,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
            }
            "prometheus" => match query.resource.as_str() {
                "proxy/diagnostics" | "proxy-diagnostics" => {
                    proxy_diagnostics::query(
                        &self.prometheus,
                        external_cluster,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
                }
                "remoting/diagnostics" | "remoting-diagnostics" => {
                    remoting_diagnostics::query(
                        &self.prometheus,
                        external_cluster,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
                }
                _ => {
                    self.prometheus
                        .query(
                            external_cluster,
                            &query.resource,
                            query.time_range.start,
                            query.time_range.end,
                            self.config.source_limits.max_rows,
                            self.config.source_limits.max_bytes,
                            deadline,
                            cancel,
                        )
                        .await
                }
            },
            "loki" => {
                self.loki
                    .query(
                        external_cluster,
                        &query.resource,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
            }
            "tempo" => {
                self.tempo
                    .query(
                        &query.resource,
                        query.time_range.start,
                        query.time_range.end,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
            }
            "kubernetes" => {
                self.kubernetes
                    .query(
                        external_cluster,
                        &query.resource,
                        self.config.source_limits.max_rows,
                        self.config.source_limits.max_bytes,
                        deadline,
                        cancel,
                    )
                    .await
            }
            "runtime" => RuntimeDiagnosticsSource::query(&self.mcp, &query.resource, deadline, cancel).await,
            "required-signals" => {
                RequiredSignalsSource::query(
                    &self.prometheus,
                    &self.loki,
                    &self.tempo,
                    &self.mcp,
                    external_cluster,
                    &query.resource,
                    query.time_range.start,
                    query.time_range.end,
                    self.config.source_limits.max_rows,
                    self.config.source_limits.max_bytes,
                    deadline,
                    cancel,
                )
                .await
            }
            "topology" => {
                TopologySource::query(
                    &self.mcp,
                    &self.admin,
                    external_cluster,
                    &query.resource,
                    deadline,
                    cancel,
                )
                .await
            }
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
        self.admin.shutdown().await;
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
    Ok(format!("sha256:{:x}", Sha256::digest(canonical)))
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
    fn mcp_parser_accepts_only_fixed_read_operations() {
        assert!(matches!(
            mcp_operation("consumer-groups/billing/lag/orders"),
            Ok(EvidenceOperation::ConsumerLag { .. })
        ));
        assert!(mcp_operation("messages/raw-body").is_err());
    }

    #[tokio::test]
    async fn manager_preserves_canonical_resource_for_unverified_body_free_metadata() {
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
                None,
                Utc::now() + chrono::Duration::seconds(2),
                &CancelSignal::default(),
            )
            .await
            .expect("fail-closed evidence");

        assert_eq!(snapshot.resource, resource);
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
