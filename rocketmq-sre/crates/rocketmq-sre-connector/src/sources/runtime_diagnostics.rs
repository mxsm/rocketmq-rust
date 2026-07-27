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
use rocketmq_runtime::RuntimeDiagnosticsViewV1;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::mcp::McpSource;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::mcp::McpGateway;

const RUNTIME_RESOURCE_URI: &str = "rocketmq://system/runtime/v1";
const OBSERVABILITY_RESOURCE_URI: &str = "rocketmq://system/observability/v1";
const SYSTEM_RESOURCE_SCHEMA: &str = "rocketmq-mcp.system-resource.v1";

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

pub(crate) struct RuntimeDiagnosticsSource;

impl RuntimeDiagnosticsSource {
    pub(crate) async fn query<G>(
        mcp: &McpSource<G>,
        resource: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError>
    where
        G: McpGateway,
    {
        match resource {
            "runtime" | "runtime/diagnostics" | RUNTIME_RESOURCE_URI => {
                let wire = mcp.system_resource(RUNTIME_RESOURCE_URI, deadline, cancel).await?;
                project_runtime(wire.content)
            }
            "runtime/observability" | "observability" | OBSERVABILITY_RESOURCE_URI => {
                let wire = mcp
                    .system_resource(OBSERVABILITY_RESOURCE_URI, deadline, cancel)
                    .await?;
                project_observability(wire.content)
            }
            _ => Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "runtime source supports only the Phase 00 diagnostics contracts",
            )),
        }
    }
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
}
