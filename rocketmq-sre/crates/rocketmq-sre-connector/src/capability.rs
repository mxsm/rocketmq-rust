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

use chrono::DateTime;
use chrono::Utc;
use rmcp::model::Tool;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::ConnectorError;
use crate::ConnectorErrorCode;

pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
pub const MCP_BUSINESS_SCHEMA: &str = "rocketmq-mcp.v2";

const REQUIRED_READ_TOOLS: [&str; 5] = [
    "rocketmq_get_cluster_overview",
    "rocketmq_list_topics",
    "rocketmq_describe_topic",
    "rocketmq_describe_broker",
    "rocketmq_get_consumer_lag",
];
const ALLOWED_SYSTEM_RESOURCES: [&str; 2] = ["rocketmq://system/runtime/v1", "rocketmq://system/observability/v1"];

/// Connector-owned representation of the public MCP capability resource.
///
/// This deliberately mirrors only the wire contract. It does not import any
/// Rust type from the RocketMQ MCP server.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CapabilityManifest {
    pub mcp_protocol_version: String,
    pub business_schema_version: String,
    pub server_version: String,
    pub cluster: String,
    pub tools: Vec<CapabilityTool>,
    pub resources: Vec<String>,
    pub tool_surface_digest: String,
    pub mutation_supported: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CapabilityTool {
    pub name: String,
    pub risk_level: String,
    pub schema_digest: String,
    pub read_only: bool,
    pub destructive: bool,
    pub task_support: String,
    pub mutates_cluster: bool,
}

/// A manifest accepted after protocol, schema, surface, and digest checks.
#[derive(Clone, Debug, Serialize)]
pub struct VerifiedCapability {
    pub manifest: CapabilityManifest,
    pub observed_at: DateTime<Utc>,
}

/// Verifies that a capability resource and the live MCP discovery surface are
/// the same, read-only contract.
///
/// # Errors
///
/// Returns a fail-closed capability error for protocol/schema mismatch,
/// mutation exposure, tool drift, resource drift, or digest drift.
pub fn verify_manifest(
    mut manifest: CapabilityManifest,
    expected_cluster: &str,
    live_tools: &[Tool],
    live_resource_uris: &BTreeSet<String>,
    pinned_surface_digest: Option<&str>,
) -> Result<VerifiedCapability, ConnectorError> {
    if manifest.mcp_protocol_version != MCP_PROTOCOL_VERSION {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            format!(
                "MCP protocol `{}` does not equal `{MCP_PROTOCOL_VERSION}`",
                manifest.mcp_protocol_version
            ),
        ));
    }
    if manifest.business_schema_version != MCP_BUSINESS_SCHEMA {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::UnsupportedSchemaMajor,
            format!(
                "business schema `{}` does not equal `{MCP_BUSINESS_SCHEMA}`",
                manifest.business_schema_version
            ),
        ));
    }
    if manifest.cluster != expected_cluster {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::ClusterNotAllowed,
            format!(
                "capability cluster `{}` does not equal requested cluster `{expected_cluster}`",
                manifest.cluster
            ),
        ));
    }
    if manifest.mutation_supported {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "MCP advertises mutation support",
        ));
    }
    if !is_sha256_digest(&manifest.tool_surface_digest) {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::SchemaDigestMismatch,
            "tool surface digest is malformed",
        ));
    }
    if let Some(expected) = pinned_surface_digest
        && manifest.tool_surface_digest != expected
    {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::SchemaDigestMismatch,
            "tool surface digest differs from the configured pin",
        ));
    }

    manifest.tools.sort_by(|left, right| left.name.cmp(&right.name));
    let manifest_names = manifest
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    if manifest_names.len() != manifest.tools.len() {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "capability manifest contains duplicate tool names",
        ));
    }
    for required in REQUIRED_READ_TOOLS {
        if !manifest_names.contains(required) {
            return Err(ConnectorError::capability(
                ConnectorErrorCode::MissingRequiredFeature,
                format!("required read tool `{required}` is unavailable"),
            ));
        }
    }
    if manifest.tools.iter().any(|tool| {
        !tool.read_only
            || tool.destructive
            || tool.mutates_cluster
            || tool.task_support != "forbidden"
            || !matches!(tool.risk_level.as_str(), "ReadOnly" | "Diagnose" | "Plan")
            || !is_sha256_digest(&tool.schema_digest)
    }) {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "one or more tools are not bounded read-only, task-forbidden tools",
        ));
    }

    let live_by_name = live_tools
        .iter()
        .map(|tool| (tool.name.as_ref(), tool))
        .collect::<BTreeMap<_, _>>();
    let live_names = live_by_name.keys().copied().collect::<BTreeSet<_>>();
    if live_names != manifest_names {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "tools/list differs from the capability manifest",
        ));
    }
    for manifest_tool in &manifest.tools {
        let live_tool = live_by_name.get(manifest_tool.name.as_str()).ok_or_else(|| {
            ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "a manifest tool is absent from tools/list",
            )
        })?;
        let annotations = live_tool.annotations.as_ref().ok_or_else(|| {
            ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                format!("tool `{}` does not publish read-only annotations", manifest_tool.name),
            )
        })?;
        if annotations.read_only_hint != Some(true) || annotations.destructive_hint == Some(true) {
            return Err(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                format!("tool `{}` live annotations are not read-only", manifest_tool.name),
            ));
        }
        let schema_digest = digest_value(Value::Object(Map::from_iter([
            (
                "input".to_owned(),
                Value::Object(live_tool.input_schema.as_ref().clone()),
            ),
            (
                "output".to_owned(),
                live_tool
                    .output_schema
                    .as_ref()
                    .map(|schema| Value::Object(schema.as_ref().clone()))
                    .unwrap_or(Value::Null),
            ),
        ])));
        if schema_digest != manifest_tool.schema_digest {
            return Err(ConnectorError::capability(
                ConnectorErrorCode::SchemaDigestMismatch,
                format!("tool `{}` schema digest differs from tools/list", manifest_tool.name),
            ));
        }
    }
    if digest_value(serde_json::to_value(&manifest.tools).map_err(|error| {
        ConnectorError::capability(
            ConnectorErrorCode::SchemaDigestMismatch,
            format!("tool surface cannot be canonicalized: {error}"),
        )
    })?) != manifest.tool_surface_digest
    {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::SchemaDigestMismatch,
            "tool surface digest does not match the manifest tools",
        ));
    }

    let manifest_resources = manifest.resources.iter().cloned().collect::<BTreeSet<_>>();
    if manifest_resources.len() != manifest.resources.len() {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "capability manifest contains duplicate resources",
        ));
    }
    let cluster_prefix = format!("rocketmq://clusters/{expected_cluster}/");
    let live_relevant_resources = live_resource_uris
        .iter()
        .filter(|uri| uri.starts_with(&cluster_prefix) || ALLOWED_SYSTEM_RESOURCES.contains(&uri.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    if live_relevant_resources != manifest_resources {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "resources/list differs from the cluster capability manifest",
        ));
    }
    if manifest_resources
        .iter()
        .any(|uri| !uri.starts_with(&cluster_prefix) && !ALLOWED_SYSTEM_RESOURCES.contains(&uri.as_str()))
    {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::ClusterNotAllowed,
            "manifest includes a resource outside the requested cluster",
        ));
    }

    Ok(VerifiedCapability {
        manifest,
        observed_at: Utc::now(),
    })
}

#[must_use]
pub(crate) fn digest_value(value: Value) -> String {
    let canonical = canonical_value(value);
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(encoded))
}

fn canonical_value(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonical_value(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_value).collect()),
        other => other,
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ToolAnnotations;
    use serde_json::json;

    use super::*;

    fn tool(name: &'static str) -> Tool {
        Tool::new(
            name,
            "read-only fixture",
            Arc::new(
                serde_json::from_value(json!({
                    "type": "object",
                    "properties": {"cluster": {"type": "string"}},
                    "required": ["cluster"]
                }))
                .expect("object schema"),
            ),
        )
        .with_raw_output_schema(Arc::new(
            serde_json::from_value(json!({"type": "object"})).expect("object schema"),
        ))
        .with_annotations(ToolAnnotations::from_raw(
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(false),
        ))
    }

    fn fixture() -> (CapabilityManifest, Vec<Tool>, BTreeSet<String>) {
        let tools = REQUIRED_READ_TOOLS.into_iter().map(tool).collect::<Vec<_>>();
        let mut manifest_tools = tools
            .iter()
            .map(|tool| CapabilityTool {
                name: tool.name.to_string(),
                risk_level: "ReadOnly".to_owned(),
                schema_digest: digest_value(json!({
                    "input": Value::Object(tool.input_schema.as_ref().clone()),
                    "output": Value::Object(
                        tool.output_schema
                            .as_ref()
                            .expect("output schema")
                            .as_ref()
                            .clone()
                    )
                })),
                read_only: true,
                destructive: false,
                task_support: "forbidden".to_owned(),
                mutates_cluster: false,
            })
            .collect::<Vec<_>>();
        manifest_tools.sort_by(|left, right| left.name.cmp(&right.name));
        let resources = vec![
            "rocketmq://clusters/local/capabilities".to_owned(),
            "rocketmq://clusters/local/overview".to_owned(),
            "rocketmq://system/runtime/v1".to_owned(),
            "rocketmq://system/observability/v1".to_owned(),
        ];
        let live_resources = resources.iter().cloned().collect();
        let manifest = CapabilityManifest {
            mcp_protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
            business_schema_version: MCP_BUSINESS_SCHEMA.to_owned(),
            server_version: "1.0.0".to_owned(),
            cluster: "local".to_owned(),
            tool_surface_digest: digest_value(serde_json::to_value(&manifest_tools).expect("manifest tools serialize")),
            tools: manifest_tools,
            resources,
            mutation_supported: false,
        };
        (manifest, tools, live_resources)
    }

    #[test]
    fn accepts_matching_read_only_surface() {
        let (manifest, tools, resources) = fixture();
        let verified = verify_manifest(manifest, "local", &tools, &resources, None).expect("surface should verify");
        assert!(!verified.manifest.mutation_supported);
    }

    #[test]
    fn rejects_mutation_and_schema_drift() {
        let (mut mutation, tools, resources) = fixture();
        mutation.mutation_supported = true;
        assert_eq!(
            verify_manifest(mutation, "local", &tools, &resources, None)
                .expect_err("mutation must fail")
                .code,
            ConnectorErrorCode::CapabilityMismatch
        );

        let (mut tasks, tools, resources) = fixture();
        tasks.tools[0].task_support = "optional".to_owned();
        tasks.tool_surface_digest = digest_value(serde_json::to_value(&tasks.tools).expect("tools serialize"));
        assert_eq!(
            verify_manifest(tasks, "local", &tools, &resources, None)
                .expect_err("task support must fail")
                .code,
            ConnectorErrorCode::CapabilityMismatch
        );

        let (mut drift, tools, resources) = fixture();
        drift.tools[0].schema_digest = format!("sha256:{}", "0".repeat(64));
        drift.tool_surface_digest = digest_value(serde_json::to_value(&drift.tools).expect("tools serialize"));
        assert_eq!(
            verify_manifest(drift, "local", &tools, &resources, None)
                .expect_err("schema drift must fail")
                .code,
            ConnectorErrorCode::SchemaDigestMismatch
        );
    }
}
