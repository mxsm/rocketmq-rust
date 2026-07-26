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
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rmcp::Peer;
use rmcp::RoleClient;
use rmcp::ServiceError;
use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::model::ClientCapabilities;
use rmcp::model::ClientInfo;
use rmcp::model::Implementation;
use rmcp::model::ProtocolVersion;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ResourceContents;
use rmcp::model::Tool;
use rmcp::service::ClientInitializeError;
use rmcp::service::RunningService;
use rmcp::transport::DynamicTransportError;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::streamable_http_client::StreamableHttpError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use crate::CapabilityManifest;
use crate::ConnectorConfig;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::EvidenceOperation;
use crate::MCP_PROTOCOL_VERSION;
use crate::VerifiedCapability;
use crate::WireEvidenceEnvelope;
use crate::auth::TokenProvider;
use crate::verify_manifest;
use crate::wire::validate_wire_envelope;

type ClientService = RunningService<RoleClient, ClientInfo>;

const SYSTEM_RESOURCE_SCHEMA: &str = "rocketmq-mcp.system-resource.v1";
const RUNTIME_RESOURCE_URI: &str = "rocketmq://system/runtime/v1";
const OBSERVABILITY_RESOURCE_URI: &str = "rocketmq://system/observability/v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DataSourceAvailability {
    MissingInstrumentation,
    Existing,
    InProcessOnly,
    Queryable,
}

#[derive(Clone, Debug, Serialize)]
struct ConnectorDataSource {
    id: &'static str,
    availability: DataSourceAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    freshness_ms: Option<u64>,
    detail: String,
}

#[derive(Deserialize)]
struct SystemResourceEnvelope {
    schema_version: String,
    resource: String,
    source: String,
    partial: bool,
    warnings: Vec<String>,
    kind: String,
    data: Value,
}

#[derive(Deserialize)]
struct ControlPlaneHandshakeAcknowledgement {
    cluster: ControlPlaneHandshakeState,
    reason: Option<String>,
}

#[derive(Deserialize)]
struct ControlPlaneHandshakeState {
    state: String,
}

pub(crate) trait McpGateway: Send + Sync + 'static {
    fn handshake(&self) -> impl Future<Output = Result<BTreeMap<String, VerifiedCapability>, ConnectorError>> + Send;

    fn query(
        &self,
        cluster: &str,
        operation: &EvidenceOperation,
    ) -> impl Future<Output = Result<WireEvidenceEnvelope, ConnectorError>> + Send;

    fn ensure_cluster_active(&self, _cluster: &str) -> impl Future<Output = Result<(), ConnectorError>> + Send {
        async { Ok(()) }
    }

    fn close(&self) -> impl Future<Output = ()> + Send;
}

struct RmcpSession {
    service: ClientService,
    tools: BTreeMap<String, Tool>,
    generation: u64,
}

#[derive(Clone)]
struct SessionSnapshot {
    peer: Peer<RoleClient>,
    output_schema: rmcp::model::JsonObject,
    generation: u64,
}

/// Production black-box MCP client. All data crosses the public Streamable
/// HTTP protocol; no MCP server DTO or implementation crate is linked.
pub(crate) struct RmcpGateway {
    config: Arc<ConnectorConfig>,
    http: reqwest::Client,
    tokens: TokenProvider,
    session: Mutex<Option<RmcpSession>>,
    verified_digests: Mutex<BTreeMap<String, String>>,
    last_verified: Mutex<BTreeMap<String, VerifiedCapability>>,
    next_generation: AtomicU64,
    concurrency: Semaphore,
}

impl RmcpGateway {
    /// Builds TLS-verifying HTTP clients from the preloaded CA material.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the CA material or HTTP client
    /// cannot be constructed.
    pub(crate) fn new(config: Arc<ConnectorConfig>) -> Result<Self, ConnectorError> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(config.max_concurrency)
            .user_agent(concat!("rocketmq-sre-connector/", env!("CARGO_PKG_VERSION")));
        if !config.mcp_ca_pem.is_empty() {
            let certificates = reqwest::Certificate::from_pem_bundle(&config.mcp_ca_pem)
                .map_err(|_| ConnectorError::configuration("MCP CA file is not a valid PEM certificate bundle"))?;
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let http = builder
            .build()
            .map_err(|error| ConnectorError::configuration(format!("TLS HTTP client cannot be built: {error}")))?;
        let tokens = TokenProvider::new(config.auth.clone(), http.clone(), config.request_timeout);
        Ok(Self {
            concurrency: Semaphore::new(config.max_concurrency),
            config,
            http,
            tokens,
            session: Mutex::new(None),
            verified_digests: Mutex::new(BTreeMap::new()),
            last_verified: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(1),
        })
    }

    async fn connect(&self, force_refresh: bool) -> Result<ClientService, ConnectorError> {
        let token = self.tokens.token(force_refresh).await?;
        let transport_config = StreamableHttpClientTransportConfig::with_uri(self.config.mcp_url.as_str())
            .auth_header(token)
            .reinit_on_expired_session(true);
        let transport = StreamableHttpClientTransport::with_client(self.http.clone(), transport_config);
        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("rocketmq-sre-connector", env!("CARGO_PKG_VERSION")),
        )
        .with_protocol_version(ProtocolVersion::V_2025_11_25);

        match tokio::time::timeout(self.config.request_timeout, client_info.clone().serve(transport)).await {
            Ok(Ok(service)) => Ok(service),
            Ok(Err(error)) if !force_refresh && is_unauthorized_initialize(&error) => {
                self.tokens.invalidate().await;
                let token = self.tokens.token(true).await?;
                let transport_config = StreamableHttpClientTransportConfig::with_uri(self.config.mcp_url.as_str())
                    .auth_header(token)
                    .reinit_on_expired_session(true);
                let transport = StreamableHttpClientTransport::with_client(self.http.clone(), transport_config);
                tokio::time::timeout(self.config.request_timeout, client_info.serve(transport))
                    .await
                    .map_err(|_| ConnectorError::source("MCP initialize timed out after token refresh"))?
                    .map_err(map_initialize_error)
            }
            Ok(Err(error)) => Err(map_initialize_error(error)),
            Err(_) => Err(ConnectorError::source("MCP initialize timed out")),
        }
    }

    async fn replace_session(
        &self,
        force_refresh: bool,
    ) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
        let mut guard = self.session.lock().await;
        if let Some(mut existing) = guard.take() {
            let _ = existing.service.close_with_timeout(self.config.shutdown_timeout).await;
        }
        let mut service = self.connect(force_refresh).await?;
        let mut discovered = self.discover(&service).await;
        if !force_refresh
            && discovered
                .as_ref()
                .is_err_and(|error| error.code == ConnectorErrorCode::UnauthorizedScope)
        {
            let _ = service.close_with_timeout(self.config.shutdown_timeout).await;
            self.tokens.invalidate().await;
            service = self.connect(true).await?;
            discovered = self.discover(&service).await;
        }
        let (tools, capabilities) = match discovered {
            Ok(discovered) => discovered,
            Err(error) => {
                let _ = service.close_with_timeout(self.config.shutdown_timeout).await;
                return Err(error);
            }
        };
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        *guard = Some(RmcpSession {
            service,
            tools,
            generation,
        });
        Ok(capabilities)
    }

    async fn discover(
        &self,
        service: &ClientService,
    ) -> Result<(BTreeMap<String, Tool>, BTreeMap<String, VerifiedCapability>), ConnectorError> {
        let peer_info = service.peer_info().ok_or_else(|| {
            ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "MCP initialize did not provide server information",
            )
        })?;
        if peer_info.protocol_version.as_str() != MCP_PROTOCOL_VERSION {
            return Err(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                format!(
                    "negotiated protocol `{}` does not equal `{MCP_PROTOCOL_VERSION}`",
                    peer_info.protocol_version
                ),
            ));
        }

        let tools = timeout_service(
            self.config.request_timeout,
            service.peer().list_all_tools(),
            "tools/list",
        )
        .await?;
        let resources = timeout_service(
            self.config.request_timeout,
            service.peer().list_all_resources(),
            "resources/list",
        )
        .await?
        .into_iter()
        .map(|resource| resource.uri)
        .collect::<BTreeSet<_>>();
        let tools_by_name = tools
            .iter()
            .cloned()
            .map(|tool| (tool.name.to_string(), tool))
            .collect::<BTreeMap<_, _>>();
        let data_sources = self.collect_data_sources(service, &resources).await;
        let sources_compatible = required_sources_are_queryable(&data_sources, &self.config);

        let mut capabilities = BTreeMap::new();
        for cluster in &self.config.cluster_allowlist {
            let uri = format!("rocketmq://clusters/{cluster}/capabilities");
            let resource = timeout_service(
                self.config.request_timeout,
                service
                    .peer()
                    .read_resource(ReadResourceRequestParams::new(uri.clone())),
                "resources/read capabilities",
            )
            .await?;
            let text = extract_text_resource(resource.contents, &uri)?;
            if text.len() > self.config.max_response_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::OutputTooLarge,
                    false,
                    "capability resource exceeds the configured response bound",
                ));
            }
            let manifest: CapabilityManifest = serde_json::from_str(&text).map_err(|_| {
                ConnectorError::capability(
                    ConnectorErrorCode::CapabilityMismatch,
                    "capability resource is not valid JSON",
                )
            })?;
            let verified = match verify_manifest(
                manifest.clone(),
                cluster,
                &tools,
                &resources,
                self.config.expected_tool_surface_digest.as_deref(),
            ) {
                Ok(verified) => verified,
                Err(error) => {
                    let incompatible = VerifiedCapability {
                        manifest,
                        observed_at: chrono::Utc::now(),
                    };
                    if let Err(report_error) = self
                        .report_to_control_plane(&incompatible, false, Some(error.code.as_str()), &data_sources)
                        .await
                    {
                        tracing::warn!(
                            code = report_error.code.as_str(),
                            "failed to report an incompatible MCP handshake"
                        );
                    }
                    return Err(error);
                }
            };
            let surface_drift = {
                let digests = self.verified_digests.lock().await;
                digests
                    .get(cluster)
                    .is_some_and(|previous| previous != &verified.manifest.tool_surface_digest)
            };
            if surface_drift {
                if let Err(report_error) = self
                    .report_to_control_plane(
                        &verified,
                        false,
                        Some(ConnectorErrorCode::SchemaDigestMismatch.as_str()),
                        &data_sources,
                    )
                    .await
                {
                    tracing::warn!(code = report_error.code.as_str(), "failed to report tool surface drift");
                }
                return Err(ConnectorError::capability(
                    ConnectorErrorCode::SchemaDigestMismatch,
                    "verified tool surface changed after onboarding",
                ));
            }
            self.report_to_control_plane(
                &verified,
                sources_compatible,
                (!sources_compatible).then_some("source_unavailable"),
                &data_sources,
            )
            .await?;
            self.verified_digests
                .lock()
                .await
                .entry(cluster.clone())
                .or_insert_with(|| verified.manifest.tool_surface_digest.clone());
            capabilities.insert(cluster.clone(), verified);
        }

        Ok((tools_by_name, capabilities))
    }

    async fn collect_data_sources(
        &self,
        service: &ClientService,
        resources: &BTreeSet<String>,
    ) -> Vec<ConnectorDataSource> {
        let mut sources = vec![ConnectorDataSource {
            id: "rocketmq_mcp",
            availability: DataSourceAvailability::Queryable,
            freshness_ms: Some(0),
            detail: "verified read-only MCP tool surface".to_owned(),
        }];
        sources.push(
            self.system_resource_source(
                service,
                resources,
                RUNTIME_RESOURCE_URI,
                "runtime",
                "rocketmq.runtime-diagnostics.v1",
                "mcp_runtime",
            )
            .await,
        );
        sources.push(
            self.system_resource_source(
                service,
                resources,
                OBSERVABILITY_RESOURCE_URI,
                "observability",
                "rocketmq.observability-status.v1",
                "mcp_observability",
            )
            .await,
        );
        sources.push(
            self.backend_source(
                "prometheus",
                self.config.prometheus_url.as_ref(),
                "/api/v1/query?query=up",
                true,
            )
            .await,
        );
        sources.push(
            self.backend_source("loki", self.config.loki_url.as_ref(), "/ready", false)
                .await,
        );
        sources.push(
            self.backend_source("tempo", self.config.tempo_url.as_ref(), "/ready", false)
                .await,
        );
        sources.extend([
            ConnectorDataSource {
                id: "kubernetes",
                availability: DataSourceAvailability::MissingInstrumentation,
                freshness_ms: None,
                detail: "remote Kubernetes source is scheduled for Phase 01".to_owned(),
            },
            ConnectorDataSource {
                id: "rocketmq_runtime",
                availability: DataSourceAvailability::InProcessOnly,
                freshness_ms: None,
                detail: "service runtime adapters are local-only in Phase 00".to_owned(),
            },
            ConnectorDataSource {
                id: "store_diagnostics",
                availability: DataSourceAvailability::Existing,
                freshness_ms: None,
                detail: "store snapshots exist but have no remote read adapter in Phase 00".to_owned(),
            },
        ]);
        sources
    }

    async fn system_resource_source(
        &self,
        service: &ClientService,
        resources: &BTreeSet<String>,
        uri: &'static str,
        kind: &'static str,
        data_schema: &'static str,
        id: &'static str,
    ) -> ConnectorDataSource {
        if !resources.contains(uri) {
            return unavailable_source(id, "required MCP System Resource is not advertised");
        }
        let result = timeout_service(
            self.config.request_timeout,
            service
                .peer()
                .read_resource(ReadResourceRequestParams::new(uri.to_owned())),
            "resources/read system diagnostics",
        )
        .await
        .and_then(|result| extract_text_resource(result.contents, uri))
        .and_then(|text| validate_system_resource(&text, uri, kind, data_schema, self.config.max_response_bytes));
        match result {
            Ok(freshness_ms) => ConnectorDataSource {
                id,
                availability: DataSourceAvailability::Queryable,
                freshness_ms: Some(freshness_ms),
                detail: format!("authenticated bounded `{data_schema}` resource"),
            },
            Err(error) => unavailable_source(id, error.code.as_str()),
        }
    }

    async fn backend_source(
        &self,
        id: &'static str,
        base_url: Option<&url::Url>,
        path: &str,
        expect_prometheus_envelope: bool,
    ) -> ConnectorDataSource {
        let Some(base_url) = base_url else {
            return unavailable_source(id, "endpoint is not configured");
        };
        let endpoint = match base_url.join(path) {
            Ok(endpoint) => endpoint,
            Err(_) => return unavailable_source(id, "configured endpoint cannot be resolved"),
        };
        let request = self.http.get(endpoint).send();
        let result = match tokio::time::timeout(self.config.request_timeout, request).await {
            Ok(Ok(response)) if response.status().is_success() => {
                if expect_prometheus_envelope {
                    read_bounded_response(response, self.config.max_response_bytes)
                        .await
                        .and_then(|body| {
                            serde_json::from_slice::<Value>(&body)
                                .map_err(|_| ConnectorError::source("Prometheus query response is not valid JSON"))
                        })
                        .and_then(|value| {
                            (value.get("status").and_then(Value::as_str) == Some("success"))
                                .then_some(())
                                .ok_or_else(|| ConnectorError::source("Prometheus query did not succeed"))
                        })
                } else {
                    Ok(())
                }
            }
            Ok(Ok(_)) => Err(ConnectorError::source("observability backend health query failed")),
            Ok(Err(_)) => Err(ConnectorError::source("observability backend is unavailable")),
            Err(_) => Err(ConnectorError::source("observability backend health query timed out")),
        };
        match result {
            Ok(()) => ConnectorDataSource {
                id,
                availability: DataSourceAvailability::Queryable,
                freshness_ms: Some(0),
                detail: "health/query endpoint verified".to_owned(),
            },
            Err(error) => unavailable_source(id, error.code.as_str()),
        }
    }

    async fn ensure_session(&self) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_mut() {
            match self.discover(&session.service).await {
                Ok((tools, capabilities)) => {
                    session.tools = tools;
                    return Ok(capabilities);
                }
                Err(error) if error.code == ConnectorErrorCode::UnauthorizedScope => {
                    // A periodic discovery request may observe rotation before
                    // a Tool call does. Use the same single forced refresh
                    // path so the reconciler cannot strand the cluster in a
                    // stale degraded state.
                    drop(guard);
                    self.tokens.invalidate().await;
                    return self.replace_session(true).await;
                }
                Err(error) => return Err(error),
            }
        }
        drop(guard);
        self.replace_session(false).await
    }

    async fn snapshot(&self, tool_name: &str) -> Result<SessionSnapshot, ConnectorError> {
        let guard = self.session.lock().await;
        let session = guard
            .as_ref()
            .ok_or_else(|| ConnectorError::source("MCP session has not completed handshaking"))?;
        let tool = session.tools.get(tool_name).ok_or_else(|| {
            ConnectorError::capability(
                ConnectorErrorCode::MissingRequiredFeature,
                format!("verified tool `{tool_name}` is unavailable"),
            )
        })?;
        let output_schema = tool
            .output_schema
            .as_ref()
            .map(|schema| schema.as_ref().clone())
            .ok_or_else(|| {
                ConnectorError::capability(
                    ConnectorErrorCode::SchemaDigestMismatch,
                    format!("verified tool `{tool_name}` has no output schema"),
                )
            })?;
        Ok(SessionSnapshot {
            peer: session.service.peer().clone(),
            output_schema,
            generation: session.generation,
        })
    }

    async fn query_once(
        &self,
        snapshot: &SessionSnapshot,
        cluster: &str,
        operation: &EvidenceOperation,
    ) -> Result<WireEvidenceEnvelope, QueryFailure> {
        let arguments = operation.arguments(cluster).map_err(QueryFailure::Other)?;
        let result = match tokio::time::timeout(
            self.config.request_timeout,
            snapshot
                .peer
                .call_tool(CallToolRequestParams::new(operation.tool_name()).with_arguments(arguments)),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(error)) if is_unauthorized_service(&error) => {
                return Err(QueryFailure::Unauthorized);
            }
            Ok(Err(error)) => {
                return Err(QueryFailure::Other(map_service_error("tools/call", error)));
            }
            Err(_) => {
                return Err(QueryFailure::Other(ConnectorError::source("MCP tools/call timed out")));
            }
        };
        if result.is_error == Some(true) {
            return Err(QueryFailure::Other(ConnectorError::source(
                "MCP tool returned a sanitized error result",
            )));
        }
        let value = result.structured_content.ok_or_else(|| {
            QueryFailure::Other(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "MCP tool did not return structured content",
            ))
        })?;
        let encoded_size = serde_json::to_vec(&value)
            .map_err(|_| {
                QueryFailure::Other(ConnectorError::capability(
                    ConnectorErrorCode::CapabilityMismatch,
                    "MCP structured result cannot be encoded",
                ))
            })?
            .len();
        if encoded_size > self.config.max_response_bytes {
            return Err(QueryFailure::Other(ConnectorError::new(
                ConnectorErrorCode::OutputTooLarge,
                false,
                "MCP structured result exceeds the configured response bound",
            )));
        }
        validate_wire_envelope(&snapshot.output_schema, value, cluster).map_err(QueryFailure::Other)
    }

    async fn reconnect_after_unauthorized(&self, failed_generation: u64) -> Result<(), ConnectorError> {
        let current_generation = self.session.lock().await.as_ref().map(|session| session.generation);
        if current_generation.is_some_and(|value| value != failed_generation) {
            return Ok(());
        }
        self.tokens.invalidate().await;
        self.replace_session(true).await?;
        Ok(())
    }

    async fn report_to_control_plane(
        &self,
        capability: &VerifiedCapability,
        compatible: bool,
        incompatibility_code: Option<&str>,
        data_sources: &[ConnectorDataSource],
    ) -> Result<(), ConnectorError> {
        let Some(control_plane) = &self.config.control_plane else {
            return Ok(());
        };
        let report_digest = capability_report_digest(capability, data_sources);
        #[derive(Serialize)]
        struct Capability<'a> {
            digest: &'a str,
            protocol_version: &'a str,
            schema_version: &'a str,
            mutation_supported: bool,
            manifest: &'a CapabilityManifest,
            data_sources: &'a [ConnectorDataSource],
            observed_at: chrono::DateTime<chrono::Utc>,
        }
        #[derive(Serialize)]
        struct Report<'a> {
            connector_subject: &'a str,
            connector_issuer: &'a str,
            capability: Capability<'a>,
            compatible: bool,
            incompatibility_code: Option<&'a str>,
        }
        let report = Report {
            connector_subject: &control_plane.connector_subject,
            connector_issuer: &control_plane.connector_issuer,
            capability: Capability {
                digest: &report_digest,
                protocol_version: &capability.manifest.mcp_protocol_version,
                schema_version: &capability.manifest.business_schema_version,
                mutation_supported: capability.manifest.mutation_supported,
                manifest: &capability.manifest,
                data_sources,
                observed_at: capability.observed_at,
            },
            compatible,
            incompatibility_code,
        };
        let endpoint = control_plane
            .base_url
            .join(&format!("/v1/clusters/{}/handshake", control_plane.cluster_id))
            .map_err(|_| ConnectorError::configuration("control-plane handshake URL cannot be constructed"))?;
        let response = self
            .http
            .post(endpoint)
            .bearer_auth(self.config.internal_token())
            .json(&report)
            .send()
            .await
            .map_err(|error| ConnectorError::source(format!("control-plane handshake request failed: {error}")))?;
        match response.status() {
            status if status.is_success() => {
                let body = read_bounded_response(response, self.config.max_response_bytes).await?;
                validate_control_plane_handshake_acknowledgement(&body)?;
            }
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::UnauthorizedScope,
                    false,
                    "control plane rejected the connector identity",
                ));
            }
            reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::CONFLICT => {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::ClusterNotAllowed,
                    false,
                    "control plane rejected the cluster handshake",
                ));
            }
            _ => {
                return Err(ConnectorError::source("control-plane handshake request failed"));
            }
        }
        Ok(())
    }

    async fn ensure_control_plane_cluster_active(&self, cluster: &str) -> Result<(), ConnectorError> {
        let Some(control_plane) = &self.config.control_plane else {
            return Ok(());
        };
        if self.config.cluster_ids.get(cluster) != Some(&control_plane.cluster_id) {
            return Err(ConnectorError::new(
                ConnectorErrorCode::ClusterNotAllowed,
                false,
                "external MCP cluster does not match the control-plane cluster",
            ));
        }

        let endpoint = control_plane
            .base_url
            .join(&format!("/v1/clusters/{}", control_plane.cluster_id))
            .map_err(|_| ConnectorError::configuration("control-plane cluster URL cannot be constructed"))?;
        let response = self
            .http
            .get(endpoint)
            .bearer_auth(self.config.internal_token())
            .send()
            .await
            .map_err(|_| ConnectorError::source("control-plane cluster status is unavailable"))?;
        match response.status() {
            status if status.is_success() => {}
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::UnauthorizedScope,
                    false,
                    "control plane rejected the connector identity",
                ));
            }
            reqwest::StatusCode::NOT_FOUND => {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::ClusterNotAllowed,
                    false,
                    "control-plane cluster does not exist",
                ));
            }
            _ => {
                return Err(ConnectorError::source("control-plane cluster status request failed"));
            }
        }

        let body = read_bounded_response(response, self.config.max_response_bytes.min(64 * 1024)).await?;
        let state: ControlPlaneClusterState = serde_json::from_slice(&body)
            .map_err(|_| ConnectorError::source("control-plane cluster status is invalid"))?;
        if state.id != control_plane.cluster_id
            || state.tenant_id != self.config.tenant_id.to_string()
            || state.external_cluster_key != cluster
            || state.effective_access_profile != "read_only"
        {
            return Err(ConnectorError::new(
                ConnectorErrorCode::ClusterNotAllowed,
                false,
                "control-plane cluster boundary does not match the connector",
            ));
        }
        match state.state.as_str() {
            "ready_read_only" => Ok(()),
            "offboarded" | "rejected" => Err(ConnectorError::new(
                ConnectorErrorCode::ClusterNotAllowed,
                false,
                "control-plane cluster no longer permits evidence collection",
            )),
            "pending" | "handshaking" | "read_only_degraded" => Err(ConnectorError::source(
                "control-plane cluster is not ready for evidence collection",
            )),
            _ => Err(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "control-plane returned an unknown cluster state",
            )),
        }
    }

    async fn report_cached_failure(&self, code: ConnectorErrorCode) {
        if !matches!(
            code,
            ConnectorErrorCode::UnauthorizedScope | ConnectorErrorCode::SourceUnavailable
        ) {
            return;
        }
        let cached = self.last_verified.lock().await.clone();
        for capability in cached.values() {
            if let Err(error) = self
                .report_to_control_plane(capability, false, Some(code.as_str()), &[])
                .await
            {
                tracing::warn!(
                    code = error.code.as_str(),
                    "failed to report a cached MCP handshake failure"
                );
            }
        }
    }
}

impl McpGateway for RmcpGateway {
    async fn handshake(&self) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
        match self.ensure_session().await {
            Ok(capabilities) => {
                *self.last_verified.lock().await = capabilities.clone();
                Ok(capabilities)
            }
            Err(error) => {
                self.report_cached_failure(error.code).await;
                Err(error)
            }
        }
    }

    async fn query(
        &self,
        cluster: &str,
        operation: &EvidenceOperation,
    ) -> Result<WireEvidenceEnvelope, ConnectorError> {
        let _permit = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| ConnectorError::source("connector concurrency limiter is closed"))?;
        if self.session.lock().await.is_none() {
            self.handshake().await?;
        }
        let first = self.snapshot(operation.tool_name()).await?;
        match self.query_once(&first, cluster, operation).await {
            Ok(result) => Ok(result),
            Err(QueryFailure::Other(error)) => Err(error),
            Err(QueryFailure::Unauthorized) => {
                // Exactly one token refresh/reconnect and one replay are
                // permitted, and only for these fixed idempotent read tools.
                self.reconnect_after_unauthorized(first.generation).await?;
                let second = self.snapshot(operation.tool_name()).await?;
                match self.query_once(&second, cluster, operation).await {
                    Ok(result) => Ok(result),
                    Err(QueryFailure::Unauthorized) => Err(ConnectorError::new(
                        ConnectorErrorCode::UnauthorizedScope,
                        false,
                        "MCP rejected the refreshed connector token",
                    )),
                    Err(QueryFailure::Other(error)) => Err(error),
                }
            }
        }
    }

    async fn ensure_cluster_active(&self, cluster: &str) -> Result<(), ConnectorError> {
        self.ensure_control_plane_cluster_active(cluster).await
    }

    async fn close(&self) {
        if let Some(mut session) = self.session.lock().await.take() {
            let _ = session.service.close_with_timeout(self.config.shutdown_timeout).await;
        }
    }
}

#[derive(Deserialize)]
struct ControlPlaneClusterState {
    id: rocketmq_sre_contracts::ClusterId,
    tenant_id: String,
    external_cluster_key: String,
    state: String,
    effective_access_profile: String,
}

enum QueryFailure {
    Unauthorized,
    Other(ConnectorError),
}

fn extract_text_resource(contents: Vec<ResourceContents>, expected_uri: &str) -> Result<String, ConnectorError> {
    let mut matches = contents.into_iter().filter_map(|content| match content {
        ResourceContents::TextResourceContents { uri, text, .. } if uri == expected_uri => Some(text),
        _ => None,
    });
    let text = matches.next().ok_or_else(|| {
        ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "capability resource did not contain the requested text content",
        )
    })?;
    if matches.next().is_some() {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "capability resource contained duplicate text content",
        ));
    }
    Ok(text)
}

fn validate_system_resource(
    text: &str,
    expected_uri: &str,
    expected_kind: &str,
    expected_data_schema: &str,
    max_bytes: usize,
) -> Result<u64, ConnectorError> {
    if text.len() > max_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorCode::OutputTooLarge,
            false,
            "MCP System Resource exceeds the configured response bound",
        ));
    }
    let envelope: SystemResourceEnvelope = serde_json::from_str(text).map_err(|_| {
        ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "MCP System Resource is not valid JSON",
        )
    })?;
    if envelope.schema_version != SYSTEM_RESOURCE_SCHEMA
        || envelope.resource != expected_uri
        || envelope.source != "mcp_process"
        || envelope.kind != expected_kind
        || !envelope.warnings.iter().all(|warning| warning.len() <= 512)
    {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "MCP System Resource envelope does not match the verified contract",
        ));
    }
    if envelope.data.get("schema_version").and_then(Value::as_str) != Some(expected_data_schema) {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::UnsupportedSchemaMajor,
            "MCP System Resource data schema is unsupported",
        ));
    }
    let observed_at = envelope
        .data
        .get("observed_at")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<chrono::DateTime<chrono::Utc>>().ok())
        .ok_or_else(|| {
            ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "MCP System Resource has no valid observation time",
            )
        })?;
    let freshness = chrono::Utc::now()
        .signed_duration_since(observed_at)
        .num_milliseconds()
        .max(0) as u64;
    if envelope.partial {
        tracing::debug!(
            resource = expected_kind,
            "MCP System Resource returned bounded partial data"
        );
    }
    Ok(freshness)
}

fn unavailable_source(id: &'static str, detail: &str) -> ConnectorDataSource {
    ConnectorDataSource {
        id,
        availability: DataSourceAvailability::MissingInstrumentation,
        freshness_ms: None,
        detail: detail.to_owned(),
    }
}

fn required_sources_are_queryable(sources: &[ConnectorDataSource], config: &ConnectorConfig) -> bool {
    sources.iter().all(|source| {
        let required = matches!(source.id, "rocketmq_mcp" | "mcp_runtime" | "mcp_observability")
            || (source.id == "prometheus" && config.prometheus_url.is_some())
            || (source.id == "loki" && config.loki_url.is_some())
            || (source.id == "tempo" && config.tempo_url.is_some());
        !required || source.availability == DataSourceAvailability::Queryable
    })
}

fn capability_report_digest(capability: &VerifiedCapability, sources: &[ConnectorDataSource]) -> String {
    #[derive(Serialize)]
    struct SourceDigestMaterial<'a> {
        id: &'static str,
        availability: DataSourceAvailability,
        detail: &'a str,
    }
    let source_material = sources
        .iter()
        .map(|source| SourceDigestMaterial {
            id: source.id,
            availability: source.availability,
            detail: &source.detail,
        })
        .collect::<Vec<_>>();
    crate::capability::digest_value(serde_json::json!({
        "tool_surface_digest": capability.manifest.tool_surface_digest,
        "protocol_version": capability.manifest.mcp_protocol_version,
        "business_schema_version": capability.manifest.business_schema_version,
        "mutation_supported": capability.manifest.mutation_supported,
        "data_sources": source_material,
    }))
}

fn validate_control_plane_handshake_acknowledgement(body: &[u8]) -> Result<(), ConnectorError> {
    let acknowledgement: ControlPlaneHandshakeAcknowledgement = serde_json::from_slice(body).map_err(|_| {
        ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "control-plane handshake acknowledgement is invalid",
        )
    })?;
    match acknowledgement.cluster.state.as_str() {
        "ready_read_only" => Ok(()),
        "read_only_degraded" if acknowledgement.reason.as_deref() == Some("schema_digest_mismatch") => {
            Err(ConnectorError::capability(
                ConnectorErrorCode::SchemaDigestMismatch,
                "control plane rejected a changed tool surface",
            ))
        }
        "read_only_degraded" => Ok(()),
        _ => Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "control-plane handshake did not grant read-only access",
        )),
    }
}

async fn read_bounded_response(mut response: reqwest::Response, max_bytes: usize) -> Result<Vec<u8>, ConnectorError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(ConnectorError::new(
            ConnectorErrorCode::OutputTooLarge,
            false,
            "HTTP response exceeds the configured output bound",
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ConnectorError::source("HTTP response body is unavailable"))?
    {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorCode::OutputTooLarge,
                false,
                "HTTP response exceeds the configured output bound",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn timeout_service<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = Result<T, ServiceError>>,
    operation: &'static str,
) -> Result<T, ConnectorError> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| ConnectorError::source(format!("MCP {operation} timed out")))?
        .map_err(|error| map_service_error(operation, error))
}

fn map_initialize_error(error: ClientInitializeError) -> ConnectorError {
    if is_unauthorized_initialize(&error) {
        ConnectorError::new(
            ConnectorErrorCode::UnauthorizedScope,
            false,
            "MCP rejected the connector token during initialize",
        )
    } else {
        ConnectorError::source(format!("MCP initialize failed: {error}"))
    }
}

fn map_service_error(operation: &'static str, error: ServiceError) -> ConnectorError {
    if is_unauthorized_service(&error) {
        ConnectorError::new(
            ConnectorErrorCode::UnauthorizedScope,
            false,
            format!("MCP rejected authorization during {operation}"),
        )
    } else {
        ConnectorError::source(format!("MCP {operation} failed: {error}"))
    }
}

fn is_unauthorized_initialize(error: &ClientInitializeError) -> bool {
    match error {
        ClientInitializeError::TransportError { error, .. } => is_unauthorized_transport(error),
        _ => false,
    }
}

fn is_unauthorized_service(error: &ServiceError) -> bool {
    match error {
        ServiceError::TransportSend(error) => is_unauthorized_transport(error),
        _ => false,
    }
}

fn is_unauthorized_transport(error: &DynamicTransportError) -> bool {
    error
        .error
        .downcast_ref::<StreamableHttpError<reqwest::Error>>()
        .is_some_and(|error| {
            matches!(
                error,
                StreamableHttpError::AuthRequired(_) | StreamableHttpError::InsufficientScope(_)
            )
        })
}

#[cfg(test)]
mod tests {
    use rmcp::transport::streamable_http_client::AuthRequiredError;

    use super::*;

    #[test]
    fn extracts_only_the_exact_capability_resource() {
        let uri = "rocketmq://clusters/local/capabilities";
        let result = extract_text_resource(vec![ResourceContents::text("{\"ok\":true}", uri)], uri)
            .expect("resource should extract");
        assert_eq!(result, "{\"ok\":true}");

        assert!(
            extract_text_resource(
                vec![ResourceContents::text("{}", "rocketmq://clusters/other/capabilities")],
                uri,
            )
            .is_err()
        );
    }

    #[test]
    fn recognizes_transport_unauthorized_without_matching_error_text() {
        let error = DynamicTransportError::from_parts(
            "fixture",
            std::any::TypeId::of::<()>(),
            Box::new(StreamableHttpError::<reqwest::Error>::AuthRequired(
                AuthRequiredError::new("Bearer".to_owned()),
            )),
        );
        assert!(is_unauthorized_transport(&error));
    }

    #[test]
    fn validates_bounded_versioned_system_resources() {
        let observed_at = chrono::Utc::now();
        let payload = serde_json::json!({
            "schema_version": SYSTEM_RESOURCE_SCHEMA,
            "resource": RUNTIME_RESOURCE_URI,
            "source": "mcp_process",
            "partial": false,
            "warnings": [],
            "kind": "runtime",
            "data": {
                "schema_version": "rocketmq.runtime-diagnostics.v1",
                "observed_at": observed_at,
                "component": "mcp"
            }
        });
        let encoded = serde_json::to_string(&payload).expect("system fixture");
        assert!(
            validate_system_resource(
                &encoded,
                RUNTIME_RESOURCE_URI,
                "runtime",
                "rocketmq.runtime-diagnostics.v1",
                4096,
            )
            .is_ok()
        );

        let mut incompatible = payload;
        incompatible["data"]["schema_version"] = Value::String("rocketmq.runtime-diagnostics.v2".to_owned());
        let encoded = serde_json::to_string(&incompatible).expect("system fixture");
        assert_eq!(
            validate_system_resource(
                &encoded,
                RUNTIME_RESOURCE_URI,
                "runtime",
                "rocketmq.runtime-diagnostics.v1",
                4096,
            )
            .expect_err("unknown major must fail closed")
            .code,
            ConnectorErrorCode::UnsupportedSchemaMajor
        );
    }

    #[test]
    fn capability_report_digest_tracks_source_state_not_freshness() {
        let capability = VerifiedCapability {
            manifest: CapabilityManifest {
                mcp_protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
                business_schema_version: crate::MCP_BUSINESS_SCHEMA.to_owned(),
                server_version: "test".to_owned(),
                cluster: "local".to_owned(),
                tools: Vec::new(),
                resources: Vec::new(),
                tool_surface_digest: format!("sha256:{}", "0".repeat(64)),
                mutation_supported: false,
            },
            observed_at: chrono::Utc::now(),
        };
        let mut sources = vec![ConnectorDataSource {
            id: "mcp_runtime",
            availability: DataSourceAvailability::Queryable,
            freshness_ms: Some(1),
            detail: "runtime v1".to_owned(),
        }];
        let first = capability_report_digest(&capability, &sources);
        sources[0].freshness_ms = Some(100);
        assert_eq!(capability_report_digest(&capability, &sources), first);
        sources[0].availability = DataSourceAvailability::MissingInstrumentation;
        assert_ne!(capability_report_digest(&capability, &sources), first);
    }

    #[test]
    fn persisted_control_plane_surface_drift_fails_closed() {
        let acknowledgement = serde_json::json!({
            "cluster": {"state": "read_only_degraded"},
            "reason": "schema_digest_mismatch"
        });

        let error = validate_control_plane_handshake_acknowledgement(
            &serde_json::to_vec(&acknowledgement).expect("acknowledgement fixture"),
        )
        .expect_err("persisted surface drift must fail closed after connector restart");

        assert_eq!(error.code, ConnectorErrorCode::SchemaDigestMismatch);
    }

    #[test]
    fn source_only_degradation_remains_a_valid_read_only_handshake() {
        let acknowledgement = serde_json::json!({
            "cluster": {"state": "read_only_degraded"},
            "reason": "source_unavailable"
        });

        assert!(
            validate_control_plane_handshake_acknowledgement(
                &serde_json::to_vec(&acknowledgement).expect("acknowledgement fixture"),
            )
            .is_ok()
        );
    }
}
