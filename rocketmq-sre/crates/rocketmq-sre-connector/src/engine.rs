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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_sre_contracts::ConnectorCapabilityState;
use rocketmq_sre_contracts::ConnectorSourceCapability;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use serde::Deserialize;
use serde::Serialize;
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;

use crate::CapabilityManifest;
use crate::ConnectorConfig;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::EvidenceOperation;
use crate::MCP_BUSINESS_SCHEMA;
use crate::MCP_PROTOCOL_VERSION;
use crate::VerifiedCapability;
use crate::mcp::McpGateway;
use crate::sources::CancelSignal;
use crate::sources::InventoryUpload;
use crate::sources::SourceManager;

/// Request accepted by the protected evidence endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvidenceQueryRequest {
    pub query: EvidenceQuery,
    pub mcp_cluster: String,
    pub operation: EvidenceOperation,
}

/// Sanitized connector compatibility view.
#[derive(Clone, Debug, Serialize)]
pub struct ConnectorCapabilitiesView {
    pub schema_version: &'static str,
    pub ready: bool,
    pub mcp_protocol_version: &'static str,
    pub mcp_business_schema: &'static str,
    pub mutation_supported: bool,
    pub observed_at: Option<DateTime<Utc>>,
    pub clusters: BTreeMap<String, CapabilityManifest>,
    pub sources: Vec<ConnectorSourceCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<&'static str>,
}

#[derive(Clone, Debug, Default)]
struct HandshakeState {
    ready: bool,
    observed_at: Option<DateTime<Utc>>,
    capabilities: BTreeMap<String, VerifiedCapability>,
    last_error_code: Option<ConnectorErrorCode>,
}

/// Coordinates verified MCP handshakes and canonical evidence conversion.
pub(crate) struct ConnectorEngine<G> {
    config: Arc<ConnectorConfig>,
    gateway: Arc<G>,
    sources: SourceManager<G>,
    handshake: RwLock<HandshakeState>,
    channel_ready: AtomicBool,
}

impl<G> ConnectorEngine<G>
where
    G: McpGateway,
{
    pub(crate) fn new(config: Arc<ConnectorConfig>, gateway: Arc<G>) -> Result<Self, ConnectorError> {
        let sources = SourceManager::new(config.clone(), gateway.clone())?;
        let channel_ready = config.control_plane.is_none();
        Ok(Self {
            config,
            gateway,
            sources,
            handshake: RwLock::new(HandshakeState::default()),
            channel_ready: AtomicBool::new(channel_ready),
        })
    }

    pub(crate) async fn initialize_sources(&self, context: ChildServiceContext) {
        self.sources.initialize(context).await;
    }

    pub(crate) fn authorize(&self, authorization_header: Option<&str>) -> Result<(), ConnectorError> {
        let provided = authorization_header
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorCode::UnauthorizedScope,
                    false,
                    "internal Bearer token is missing",
                )
            })?;
        let expected = self.config.internal_token();
        let matches = provided.len() == expected.len() && bool::from(provided.as_bytes().ct_eq(expected.as_bytes()));
        if !matches {
            return Err(ConnectorError::new(
                ConnectorErrorCode::UnauthorizedScope,
                false,
                "internal Bearer token is invalid",
            ));
        }
        Ok(())
    }

    pub(crate) fn reverse_channel_enabled(&self) -> bool {
        self.config.control_plane.is_some()
    }

    pub(crate) async fn reconcile(&self) -> Result<(), ConnectorError> {
        match self.gateway.handshake().await {
            Ok(capabilities)
                if capabilities.len() == self.config.cluster_allowlist.len()
                    && capabilities
                        .keys()
                        .all(|cluster| self.config.cluster_allowlist.contains(cluster)) =>
            {
                for cluster in capabilities.keys() {
                    if let Err(error) = self.gateway.ensure_cluster_active(cluster).await {
                        self.record_collection_block(error.code).await;
                        return Err(error);
                    }
                }
                *self.handshake.write().await = HandshakeState {
                    ready: true,
                    observed_at: Some(Utc::now()),
                    capabilities,
                    last_error_code: None,
                };
                Ok(())
            }
            Ok(_) => {
                let error = ConnectorError::capability(
                    ConnectorErrorCode::CapabilityMismatch,
                    "handshake did not return every allowed cluster",
                );
                self.record_error(error.code).await;
                Err(error)
            }
            Err(error) => {
                self.record_error(error.code).await;
                Err(error)
            }
        }
    }

    async fn record_error(&self, code: ConnectorErrorCode) {
        let mut state = self.handshake.write().await;
        state.ready = false;
        state.last_error_code = Some(code);
    }

    async fn record_collection_block(&self, code: ConnectorErrorCode) {
        let mut state = self.handshake.write().await;
        state.ready = false;
        state.capabilities.clear();
        state.last_error_code = Some(code);
    }

    pub(crate) async fn is_ready(&self) -> bool {
        self.handshake.read().await.ready && self.channel_ready.load(Ordering::Acquire)
    }

    async fn is_mcp_ready(&self) -> bool {
        self.handshake.read().await.ready
    }

    pub(crate) fn set_channel_ready(&self, ready: bool) {
        self.channel_ready.store(ready, Ordering::Release);
    }

    pub(crate) async fn capabilities(&self) -> ConnectorCapabilitiesView {
        let state = self.handshake.read().await;
        let source_capability = self.sources.capabilities().await;
        ConnectorCapabilitiesView {
            schema_version: "rocketmq-sre.connector-capabilities.v1",
            ready: state.ready && self.channel_ready.load(Ordering::Acquire),
            mcp_protocol_version: MCP_PROTOCOL_VERSION,
            mcp_business_schema: MCP_BUSINESS_SCHEMA,
            mutation_supported: false,
            observed_at: state.observed_at,
            clusters: state
                .capabilities
                .iter()
                .map(|(cluster, capability)| (cluster.clone(), capability.manifest.clone()))
                .collect(),
            sources: source_capability.sources,
            last_error_code: state.last_error_code.map(ConnectorErrorCode::as_str),
        }
    }

    pub(crate) async fn sources_capability(&self) -> ConnectorCapabilityState {
        self.sources.capabilities().await
    }

    pub(crate) async fn evidence(&self, request: EvidenceQueryRequest) -> Result<EvidenceSnapshot, ConnectorError> {
        self.validate_request(&request)?;
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.config.request_timeout).unwrap_or_else(|_| chrono::Duration::seconds(15));
        self.collect(
            request.query,
            &request.mcp_cluster,
            Some(&request.operation),
            deadline,
            &CancelSignal::default(),
        )
        .await
    }

    pub(crate) async fn collect_contract_query(
        &self,
        query: EvidenceQuery,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<EvidenceSnapshot, ConnectorError> {
        let external_cluster = self
            .config
            .cluster_ids
            .iter()
            .find_map(|(cluster, id)| (*id == query.cluster_id).then_some(cluster.clone()))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorCode::ClusterNotAllowed,
                    false,
                    "query cluster identifier has no configured external cluster",
                )
                .with_correlation_id(query.correlation_id)
            })?;
        self.validate_query_boundary(&query, &external_cluster)?;
        self.collect(query, &external_cluster, None, deadline, cancel).await
    }

    pub(crate) async fn inventory(
        &self,
        cluster_id: rocketmq_sre_contracts::ClusterId,
    ) -> Result<InventoryUpload, ConnectorError> {
        let external_cluster = self
            .config
            .cluster_ids
            .iter()
            .find_map(|(cluster, id)| (*id == cluster_id).then_some(cluster.as_str()))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorCode::ClusterNotAllowed,
                    false,
                    "inventory cluster identifier has no configured external cluster",
                )
            })?;
        if !self.is_mcp_ready().await {
            self.reconcile().await?;
        }
        if let Err(error) = self.gateway.ensure_cluster_active(external_cluster).await {
            self.record_collection_block(error.code).await;
            return Err(error);
        }
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.config.request_timeout).unwrap_or_else(|_| chrono::Duration::seconds(15));
        self.sources
            .inventory(cluster_id, external_cluster, deadline, &CancelSignal::default())
            .await
    }

    async fn collect(
        &self,
        query: EvidenceQuery,
        external_cluster: &str,
        operation: Option<&EvidenceOperation>,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<EvidenceSnapshot, ConnectorError> {
        let requires_mcp = matches!(
            query.source.as_str(),
            "rocketmq-mcp" | "mcp" | "rocketmq_mcp" | "runtime" | "runtime-diagnostics" | "topology"
        );
        if requires_mcp && !self.is_mcp_ready().await {
            self.reconcile()
                .await
                .map_err(|error| error.with_correlation_id(query.correlation_id))?;
        }
        if let Err(error) = self.gateway.ensure_cluster_active(external_cluster).await {
            self.record_collection_block(error.code).await;
            return Err(error.with_correlation_id(query.correlation_id));
        }
        self.sources
            .query(query, external_cluster, operation, deadline, cancel)
            .await
    }

    fn validate_request(&self, request: &EvidenceQueryRequest) -> Result<(), ConnectorError> {
        let correlation_id = request.query.correlation_id;
        if request.query.tenant_id != self.config.tenant_id {
            return Err(ConnectorError::new(
                ConnectorErrorCode::TenantMismatch,
                false,
                "query tenant differs from configured tenant",
            )
            .with_correlation_id(correlation_id));
        }
        let configured_cluster_id = self
            .config
            .cluster_ids
            .get(&request.mcp_cluster)
            .copied()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorCode::ClusterNotAllowed,
                    false,
                    "external MCP cluster has no internal cluster mapping",
                )
                .with_correlation_id(correlation_id)
            })?;
        if configured_cluster_id != request.query.cluster_id {
            return Err(ConnectorError::new(
                ConnectorErrorCode::ClusterNotAllowed,
                false,
                "query cluster identifier differs from configured cluster",
            )
            .with_correlation_id(correlation_id));
        }
        if !self.config.cluster_allowlist.contains(&request.mcp_cluster) {
            return Err(ConnectorError::new(
                ConnectorErrorCode::ClusterNotAllowed,
                false,
                "external MCP cluster is outside the allowlist",
            )
            .with_correlation_id(correlation_id));
        }
        if matches!(request.query.source.as_str(), "rocketmq-mcp" | "mcp" | "rocketmq_mcp") {
            request
                .operation
                .validate()
                .map_err(|error| error.with_correlation_id(correlation_id))?;
        }
        if matches!(request.query.source.as_str(), "rocketmq-mcp" | "mcp" | "rocketmq_mcp")
            && request.query.resource != request.operation.resource()
        {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "evidence resource does not match the selected operation",
            )
            .with_correlation_id(correlation_id));
        }
        if request.query.time_range.start > request.query.time_range.end {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "evidence time range starts after it ends",
            )
            .with_correlation_id(correlation_id));
        }
        Ok(())
    }

    fn validate_query_boundary(&self, query: &EvidenceQuery, external_cluster: &str) -> Result<(), ConnectorError> {
        let correlation_id = query.correlation_id;
        if query.tenant_id != self.config.tenant_id {
            return Err(ConnectorError::new(
                ConnectorErrorCode::TenantMismatch,
                false,
                "query tenant differs from configured tenant",
            )
            .with_correlation_id(correlation_id));
        }
        if self.config.cluster_ids.get(external_cluster) != Some(&query.cluster_id)
            || !self.config.cluster_allowlist.contains(external_cluster)
        {
            return Err(ConnectorError::new(
                ConnectorErrorCode::ClusterNotAllowed,
                false,
                "query cluster differs from the configured connector boundary",
            )
            .with_correlation_id(correlation_id));
        }
        Ok(())
    }

    pub(crate) async fn close(&self) {
        self.sources.shutdown().await;
        self.gateway.close().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use chrono::TimeZone;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::CoverageStatus;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::TimeRange;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::WireEvidenceEnvelope;
    use crate::config::ConnectorAuth;
    use crate::config::SecretValue;

    struct FakeGateway {
        capability: VerifiedCapability,
        wire: WireEvidenceEnvelope,
        active: AtomicBool,
        query_count: AtomicUsize,
    }

    impl McpGateway for FakeGateway {
        async fn handshake(&self) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
            Ok(BTreeMap::from([("local".to_owned(), self.capability.clone())]))
        }

        async fn query(
            &self,
            _cluster: &str,
            _operation: &EvidenceOperation,
        ) -> Result<WireEvidenceEnvelope, ConnectorError> {
            self.query_count.fetch_add(1, Ordering::SeqCst);
            Ok(self.wire.clone())
        }

        async fn ensure_cluster_active(&self, _cluster: &str) -> Result<(), ConnectorError> {
            if self.active.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(ConnectorError::new(
                    ConnectorErrorCode::ClusterNotAllowed,
                    false,
                    "cluster is offboarded",
                ))
            }
        }

        async fn close(&self) {}
    }

    fn config(tenant_id: rocketmq_sre_contracts::TenantId, cluster_id: ClusterId) -> Arc<ConnectorConfig> {
        Arc::new(ConnectorConfig {
            bind_addr: "127.0.0.1:8091".parse().expect("socket"),
            mcp_url: Url::parse("http://127.0.0.1:8089/mcp").expect("URL"),
            mcp_ca_path: Some(PathBuf::from("ca.pem")),
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
        })
    }

    fn gateway() -> Arc<FakeGateway> {
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 26, 8, 0, 0).single().expect("time");
        Arc::new(FakeGateway {
            capability: VerifiedCapability {
                manifest: CapabilityManifest {
                    mcp_protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
                    business_schema_version: MCP_BUSINESS_SCHEMA.to_owned(),
                    server_version: "1.0.0".to_owned(),
                    cluster: "local".to_owned(),
                    tools: Vec::new(),
                    resources: Vec::new(),
                    tool_surface_digest: format!("sha256:{}", "0".repeat(64)),
                    mutation_supported: false,
                },
                observed_at,
            },
            wire: crate::WireEvidenceEnvelope {
                schema_version: MCP_BUSINESS_SCHEMA.to_owned(),
                request_id: "request-1".to_owned(),
                cluster: "local".to_owned(),
                observed_at,
                freshness_ms: 1_500,
                cache_status: "miss".to_owned(),
                partial: true,
                warnings: vec!["bounded".to_owned()],
                data: json!({"total_lag": 42}),
            },
            active: AtomicBool::new(true),
            query_count: AtomicUsize::new(0),
        })
    }

    #[tokio::test]
    async fn mock_gateway_becomes_ready_and_emits_canonical_evidence() {
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway();
        let engine = ConnectorEngine::new(config(tenant_id, cluster_id), gateway.clone()).expect("engine");
        let at = Utc.with_ymd_and_hms(2026, 7, 26, 7, 55, 0).single().expect("time");
        let request = EvidenceQueryRequest {
            query: EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id,
                cluster_id,
                source: "rocketmq-mcp".to_owned(),
                resource: "consumer-groups/billing/lag/orders".to_owned(),
                time_range: TimeRange::new(at, at).expect("range"),
            },
            mcp_cluster: "local".to_owned(),
            operation: EvidenceOperation::ConsumerLag {
                topic: "orders".to_owned(),
                consumer_group: "billing".to_owned(),
                limit: Some(50),
                cursor: None,
            },
        };

        let evidence = engine.evidence(request.clone()).await.expect("evidence");
        let cached = engine.evidence(request).await.expect("cached evidence");
        assert!(engine.is_ready().await);
        assert_eq!(evidence.freshness_seconds, 2);
        assert!(evidence.partial);
        assert_eq!(evidence.coverage, CoverageStatus::Partial);
        evidence.verify_content_hash().expect("content hash");
        cached.verify_content_hash().expect("cached content hash");
        assert_eq!(gateway.query_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn tenant_and_internal_token_mismatches_fail_closed() {
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = ClusterId::new();
        let engine = ConnectorEngine::new(config(tenant_id, cluster_id), gateway()).expect("engine");
        assert!(engine.authorize(Some("Bearer internal-token")).is_ok());
        assert_eq!(
            engine
                .authorize(Some("Bearer different-token"))
                .expect_err("wrong token")
                .code,
            ConnectorErrorCode::UnauthorizedScope
        );

        let at = Utc::now();
        let request = EvidenceQueryRequest {
            query: EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: rocketmq_sre_contracts::TenantId::new(),
                cluster_id,
                source: "rocketmq-mcp".to_owned(),
                resource: "cluster/overview".to_owned(),
                time_range: TimeRange::new(at, at).expect("range"),
            },
            mcp_cluster: "local".to_owned(),
            operation: EvidenceOperation::ClusterOverview,
        };
        assert_eq!(
            engine.evidence(request).await.expect_err("tenant mismatch").code,
            ConnectorErrorCode::TenantMismatch
        );
    }

    #[tokio::test]
    async fn offboarded_cluster_clears_readiness_and_blocks_new_collection() {
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway();
        let engine = ConnectorEngine::new(config(tenant_id, cluster_id), gateway.clone()).expect("engine");
        let at = Utc::now();
        let request = EvidenceQueryRequest {
            query: EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id,
                cluster_id,
                source: "rocketmq-mcp".to_owned(),
                resource: "cluster/overview".to_owned(),
                time_range: TimeRange::new(at, at).expect("range"),
            },
            mcp_cluster: "local".to_owned(),
            operation: EvidenceOperation::ClusterOverview,
        };

        engine
            .evidence(request.clone())
            .await
            .expect("active cluster should collect");
        assert_eq!(gateway.query_count.load(Ordering::SeqCst), 1);

        gateway.active.store(false, Ordering::SeqCst);
        let error = engine
            .evidence(request)
            .await
            .expect_err("offboarded cluster must not collect");
        assert_eq!(error.code, ConnectorErrorCode::ClusterNotAllowed);
        assert_eq!(gateway.query_count.load(Ordering::SeqCst), 1);
        assert!(!engine.is_ready().await);
        assert!(engine.capabilities().await.clusters.is_empty());
    }

    #[tokio::test]
    async fn unavailable_optional_source_returns_explicit_missing_evidence() {
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = ClusterId::new();
        let engine = ConnectorEngine::new(config(tenant_id, cluster_id), gateway()).expect("engine");
        let at = Utc::now();
        let request = EvidenceQueryRequest {
            query: EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id,
                cluster_id,
                source: "prometheus".to_owned(),
                resource: "metrics/rocketmq_broker_up".to_owned(),
                time_range: TimeRange::new(at, at).expect("range"),
            },
            mcp_cluster: "local".to_owned(),
            operation: EvidenceOperation::ClusterOverview,
        };

        let evidence = engine.evidence(request).await.expect("missing evidence");
        assert!(evidence.partial);
        assert_eq!(evidence.coverage, CoverageStatus::Missing);
        assert_eq!(
            evidence.content,
            rocketmq_sre_contracts::EvidenceContent::Inline(serde_json::json!({
                "status": "missing",
                "source": "prometheus",
                "error_code": "source_unavailable"
            }))
        );
    }
}
