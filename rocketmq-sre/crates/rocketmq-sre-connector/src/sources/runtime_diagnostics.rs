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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_observability::ObservabilityStatusViewV1;
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeDiagnosticsViewV1;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::common;
use super::common::CancelSignal;
use super::common::SourceOutput;
use crate::ConnectorConfig;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::config::RuntimeDiagnosticsSourceConfig;
use crate::config::SecretValue;
use crate::mcp::McpGateway;
use crate::read_gateway::ConnectorReadGateway;
use crate::read_gateway::ReadSession;

const RUNTIME_RESOURCE_URI: &str = "rocketmq://system/runtime/v1";
const OBSERVABILITY_RESOURCE_URI: &str = "rocketmq://system/observability/v1";
const SYSTEM_RESOURCE_SCHEMA: &str = "rocketmq-mcp.system-resource.v1";
const COMPONENT_SOURCE: &str = "rocketmq_process";
const COMPONENTS_RESOURCE: &str = "runtime/components";

#[derive(Deserialize)]
struct SystemResourceEnvelope<T> {
    schema_version: String,
    resource: String,
    source: String,
    partial: bool,
    #[serde(default)]
    warnings: Vec<String>,
    kind: String,
    data: T,
}

#[derive(Deserialize)]
struct ComponentRuntimeEnvelope {
    schema_version: String,
    source: String,
    data: RuntimeDiagnosticsViewV1,
}

pub(crate) struct RuntimeDiagnosticsSource {
    http: reqwest::Client,
    config: Option<RuntimeDiagnosticsSourceConfig>,
    token: SecretValue,
    max_response_bytes: usize,
}

impl RuntimeDiagnosticsSource {
    pub(crate) fn new(http: reqwest::Client, config: &ConnectorConfig) -> Self {
        Self {
            http,
            config: config.runtime_diagnostics_source.clone(),
            token: config.internal_token.clone(),
            max_response_bytes: config.max_response_bytes.min(64 * 1024),
        }
    }

    pub(crate) async fn query<G>(
        &self,
        read_gateway: &ConnectorReadGateway<G>,
        session: &ReadSession<'_, '_>,
        resource: &str,
    ) -> Result<SourceOutput, ConnectorError>
    where
        G: McpGateway,
    {
        let context = session.context();
        match resource {
            "runtime" | "runtime/diagnostics" | RUNTIME_RESOURCE_URI => {
                let wire = read_gateway.mcp_system_resource(session, RUNTIME_RESOURCE_URI).await?;
                project_runtime(wire.content)
            }
            "runtime/observability" | "observability" | OBSERVABILITY_RESOURCE_URI => {
                let wire = read_gateway
                    .mcp_system_resource(session, OBSERVABILITY_RESOURCE_URI)
                    .await?;
                project_observability(wire.content)
            }
            COMPONENTS_RESOURCE | "components" => self.query_components(context.deadline, context.cancel).await,
            _ if resource.starts_with("runtime/") => {
                let component = resource.trim_start_matches("runtime/");
                let view = self
                    .fetch_component(component, context.deadline, context.cancel)
                    .await?;
                Ok(project_component_runtime(view))
            }
            _ => Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "runtime source supports only fixed protected diagnostics contracts",
            )),
        }
    }

    async fn query_components(
        &self,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let config = self.config.as_ref().ok_or_else(component_source_unavailable)?;
        let mut diagnostics = Vec::with_capacity(config.endpoints.len());
        let mut missing = Vec::new();
        for component in ["broker", "name_server", "controller", "proxy"] {
            if !config.endpoints.contains_key(component) {
                missing.push(component);
                continue;
            }
            match self.fetch_component(component, deadline, cancel).await {
                Ok(view) => diagnostics.push(view),
                Err(error) if error.code == ConnectorErrorCode::SourceUnavailable => missing.push(component),
                Err(error) => return Err(error),
            }
        }
        if diagnostics.is_empty() {
            return Err(component_source_unavailable());
        }
        let observed_at = diagnostics
            .iter()
            .map(|view| view.observed_at)
            .max()
            .unwrap_or_else(Utc::now);
        let mut output = SourceOutput::available(
            json!({
                "schema_version": "rocketmq.sre-runtime-components-evidence.v1",
                "exposure": "protected_component_endpoint",
                "diagnostics": diagnostics,
                "missing_components": missing
            }),
            observed_at,
        )
        .with_exposure(EvidenceExposure::RuntimeDiagnostics);
        if !missing.is_empty() {
            output.partial = true;
            output.coverage = CoverageStatus::Partial;
            output.warnings.push("runtime_component_source_partial".to_owned());
        }
        Ok(output)
    }

    async fn fetch_component(
        &self,
        component: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<RuntimeDiagnosticsViewV1, ConnectorError> {
        let url = self
            .config
            .as_ref()
            .and_then(|config| config.endpoints.get(component))
            .ok_or_else(component_source_unavailable)?
            .clone();
        let http = self.http.clone();
        let token = self.token.clone();
        let max_response_bytes = self.max_response_bytes;
        let response = common::bounded_future(deadline, cancel, async move {
            let mut response = http
                .get(url)
                .bearer_auth(token.expose())
                .header(
                    "X-RocketMQ-SRE-Scope",
                    rocketmq_observability::RUNTIME_DIAGNOSTICS_SCOPE,
                )
                .send()
                .await
                .map_err(|_| component_source_unavailable())?;
            match response.status() {
                reqwest::StatusCode::OK => {}
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::UnauthorizedScope,
                        false,
                        "runtime diagnostics credential or scope was rejected",
                    ));
                }
                reqwest::StatusCode::PAYLOAD_TOO_LARGE => {
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::OutputTooLarge,
                        false,
                        "runtime diagnostics response exceeded the configured bound",
                    ));
                }
                _ => return Err(component_source_unavailable()),
            }
            if response
                .content_length()
                .is_some_and(|length| length > max_response_bytes as u64)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::OutputTooLarge,
                    false,
                    "runtime diagnostics response exceeded the configured bound",
                ));
            }
            let response_capacity = response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or_default();
            let mut body = Vec::with_capacity(response_capacity);
            while let Some(chunk) = response.chunk().await.map_err(|_| component_source_unavailable())? {
                let next_length = body.len().checked_add(chunk.len()).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorCode::OutputTooLarge,
                        false,
                        "runtime diagnostics response exceeded the configured bound",
                    )
                })?;
                if next_length > max_response_bytes {
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::OutputTooLarge,
                        false,
                        "runtime diagnostics response exceeded the configured bound",
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            serde_json::from_slice::<Value>(&body).map_err(|_| schema_mismatch("component runtime"))
        })
        .await?;
        project_component_view(response, component)
    }
}

fn project_component_view(raw: Value, expected_component: &str) -> Result<RuntimeDiagnosticsViewV1, ConnectorError> {
    let envelope: ComponentRuntimeEnvelope =
        serde_json::from_value(raw).map_err(|_| schema_mismatch("component runtime"))?;
    if envelope.schema_version != rocketmq_observability::RUNTIME_DIAGNOSTICS_ENDPOINT_SCHEMA
        || envelope.source != COMPONENT_SOURCE
        || envelope.data.schema_version != RuntimeDiagnosticsViewV1::SCHEMA_VERSION
        || component_name(envelope.data.component) != expected_component
    {
        return Err(schema_mismatch("component runtime"));
    }
    Ok(envelope.data)
}

fn project_component_runtime(view: RuntimeDiagnosticsViewV1) -> SourceOutput {
    let observed_at = view.observed_at;
    SourceOutput::available(
        json!({
            "schema_version": "rocketmq.sre-runtime-evidence.v1",
            "exposure": "protected_component_endpoint",
            "diagnostics": view
        }),
        observed_at,
    )
    .with_exposure(EvidenceExposure::RuntimeDiagnostics)
}

fn component_name(component: RuntimeComponent) -> &'static str {
    match component {
        RuntimeComponent::Broker => "broker",
        RuntimeComponent::NameServer => "name_server",
        RuntimeComponent::Controller => "controller",
        RuntimeComponent::Proxy => "proxy",
        RuntimeComponent::Mcp => "mcp",
        RuntimeComponent::SreControlPlane => "sre_control_plane",
        RuntimeComponent::SreConnector => "sre_connector",
        RuntimeComponent::Other => "other",
    }
}

fn component_source_unavailable() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorCode::SourceUnavailable,
        true,
        "protected runtime diagnostics source is unavailable",
    )
}

fn project_runtime(raw: Value) -> Result<SourceOutput, ConnectorError> {
    let envelope: SystemResourceEnvelope<RuntimeDiagnosticsViewV1> =
        serde_json::from_value(raw).map_err(|_| schema_mismatch("runtime"))?;
    validate_envelope(&envelope, RUNTIME_RESOURCE_URI, "runtime")?;
    if envelope.data.schema_version != RuntimeDiagnosticsViewV1::SCHEMA_VERSION {
        return Err(schema_mismatch("runtime"));
    }
    let observed_at = envelope.data.observed_at;
    let mut output = SourceOutput::available(
        json!({
            "schema_version": "rocketmq.sre-runtime-evidence.v1",
            "exposure": "mcp_system_resource",
            "diagnostics": envelope.data
        }),
        observed_at,
    )
    .with_exposure(EvidenceExposure::RuntimeDiagnostics);
    apply_envelope_status(&mut output, envelope.partial, envelope.warnings);
    Ok(output)
}

fn project_observability(raw: Value) -> Result<SourceOutput, ConnectorError> {
    let envelope: SystemResourceEnvelope<ObservabilityStatusViewV1> =
        serde_json::from_value(raw).map_err(|_| schema_mismatch("observability"))?;
    validate_envelope(&envelope, OBSERVABILITY_RESOURCE_URI, "observability")?;
    if envelope.data.schema_version != ObservabilityStatusViewV1::SCHEMA_VERSION {
        return Err(schema_mismatch("observability"));
    }
    let observed_at = envelope.data.observed_at;
    let mut output = SourceOutput::available(
        json!({
            "schema_version": "rocketmq.sre-observability-evidence.v1",
            "exposure": "mcp_system_resource",
            "status": envelope.data
        }),
        observed_at,
    )
    .with_exposure(EvidenceExposure::RuntimeDiagnostics);
    apply_envelope_status(&mut output, envelope.partial, envelope.warnings);
    Ok(output)
}

fn validate_envelope<T>(
    envelope: &SystemResourceEnvelope<T>,
    expected_resource: &str,
    expected_kind: &str,
) -> Result<(), ConnectorError> {
    if envelope.schema_version != SYSTEM_RESOURCE_SCHEMA
        || envelope.resource != expected_resource
        || envelope.source != "mcp_process"
        || envelope.kind != expected_kind
    {
        return Err(schema_mismatch(expected_kind));
    }
    Ok(())
}

fn apply_envelope_status(output: &mut SourceOutput, partial: bool, warnings: Vec<String>) {
    if partial {
        output.partial = true;
        output.coverage = CoverageStatus::Partial;
    }
    if !warnings.is_empty() {
        output.warnings.push("runtime_diagnostics_source_warning".to_owned());
    }
}

fn schema_mismatch(kind: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorCode::UnsupportedSchemaMajor,
        false,
        format!("MCP {kind} diagnostics contract is incompatible"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_projection_requires_versioned_wrapper_and_preserves_capacity() {
        let raw = json!({
            "schema_version": SYSTEM_RESOURCE_SCHEMA,
            "resource": RUNTIME_RESOURCE_URI,
            "source": "mcp_process",
            "partial": false,
            "warnings": [],
            "kind": "runtime",
            "data": {
                "schema_version": RuntimeDiagnosticsViewV1::SCHEMA_VERSION,
                "observed_at": "2026-07-27T00:00:00Z",
                "component": "mcp",
                "lifecycle_state": "open",
                "task_group_count": 2,
                "task_count": 3,
                "task_kinds": [],
                "blocking_lanes": [{
                    "lane": "metadata_io",
                    "max_concurrency": 4,
                    "max_queue_depth": 16,
                    "queued": 2,
                    "running": 1,
                    "timed_out_still_running": 0,
                    "blocking_still_running": 0,
                    "task_kinds": []
                }],
                "truncated": false
            }
        });
        let output = project_runtime(raw).expect("runtime projection");
        assert_eq!(output.content["diagnostics"]["blocking_lanes"][0]["max_concurrency"], 4);
        assert_eq!(output.exposure, EvidenceExposure::RuntimeDiagnostics);
    }

    #[test]
    fn runtime_projection_rejects_unversioned_or_wrong_resource_payloads() {
        assert!(project_runtime(json!({"data": {}})).is_err());
    }

    #[test]
    fn component_projection_requires_expected_component_and_sanitized_schema() {
        let raw = json!({
            "schema_version": rocketmq_observability::RUNTIME_DIAGNOSTICS_ENDPOINT_SCHEMA,
            "source": COMPONENT_SOURCE,
            "data": {
                "schema_version": RuntimeDiagnosticsViewV1::SCHEMA_VERSION,
                "observed_at": "2026-07-31T00:00:00Z",
                "component": "broker",
                "lifecycle_state": "open",
                "task_group_count": 2,
                "task_count": 3,
                "task_kinds": [],
                "blocking_lanes": [],
                "truncated": false
            }
        });
        let view = project_component_view(raw.clone(), "broker").expect("component view");
        assert_eq!(view.component, RuntimeComponent::Broker);
        assert!(project_component_view(raw, "proxy").is_err());
    }
}
