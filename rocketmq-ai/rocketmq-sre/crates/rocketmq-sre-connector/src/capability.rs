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
use crate::ConnectorFailure;

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

/// Closed reason why an MCP capability surface was not accepted.
///
/// These deterministic protocol and surface checks do not implement
/// [`std::error::Error`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCapabilityRejection {
    ProtocolMismatch,
    UnsupportedBusinessSchema,
    ClusterMismatch,
    MutationSupported,
    MalformedSurfaceDigest,
    PinnedSurfaceDigestMismatch,
    DuplicateTool,
    MissingRequiredTool,
    UnsafeTool,
    ToolSurfaceMismatch,
    MissingToolAnnotations,
    UnsafeToolAnnotations,
    ToolSchemaDigestMismatch,
    ToolSurfaceDigestMismatch,
    DuplicateResource,
    ResourceSurfaceMismatch,
    ResourceClusterMismatch,
    IncompleteClusterSurface,
    SurfaceChanged,
}

impl ConnectorCapabilityRejection {
    /// Returns the stable connector classification for reporting and metrics.
    #[must_use]
    pub const fn failure(self) -> ConnectorFailure {
        match self {
            Self::UnsupportedBusinessSchema => ConnectorFailure::UnsupportedSchemaMajor,
            Self::MissingRequiredTool => ConnectorFailure::MissingRequiredFeature,
            Self::MalformedSurfaceDigest
            | Self::PinnedSurfaceDigestMismatch
            | Self::ToolSchemaDigestMismatch
            | Self::ToolSurfaceDigestMismatch
            | Self::SurfaceChanged => ConnectorFailure::SchemaDigestMismatch,
            Self::ClusterMismatch | Self::ResourceClusterMismatch => ConnectorFailure::ClusterNotAllowed,
            Self::ProtocolMismatch
            | Self::MutationSupported
            | Self::DuplicateTool
            | Self::UnsafeTool
            | Self::ToolSurfaceMismatch
            | Self::MissingToolAnnotations
            | Self::UnsafeToolAnnotations
            | Self::DuplicateResource
            | Self::ResourceSurfaceMismatch
            | Self::IncompleteClusterSurface => ConnectorFailure::CapabilityMismatch,
        }
    }

    /// Returns the fixed machine-facing rejection code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.failure().as_str()
    }
}

/// Capability verification result with deterministic rejection separated from
/// operational [`ConnectorError`] failures.
#[derive(Clone, Debug, PartialEq)]
pub enum ConnectorCapabilityOutcome<T> {
    Verified(T),
    Rejected(ConnectorCapabilityRejection),
}

/// Verifies that a capability resource and the live MCP discovery surface are
/// the same, read-only contract.
///
/// # Errors
///
/// Returns an operational error only if the surface cannot be encoded for
/// verification. Protocol/schema mismatch, mutation exposure, tool drift,
/// resource drift, and digest drift are closed rejections.
pub fn verify_manifest(
    mut manifest: CapabilityManifest,
    expected_cluster: &str,
    live_tools: &[Tool],
    live_resource_uris: &BTreeSet<String>,
    pinned_surface_digest: Option<&str>,
) -> Result<ConnectorCapabilityOutcome<VerifiedCapability>, ConnectorError> {
    if manifest.mcp_protocol_version != MCP_PROTOCOL_VERSION {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::ProtocolMismatch,
        ));
    }
    if manifest.business_schema_version != MCP_BUSINESS_SCHEMA {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::UnsupportedBusinessSchema,
        ));
    }
    if manifest.cluster != expected_cluster {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::ClusterMismatch,
        ));
    }
    if manifest.mutation_supported {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::MutationSupported,
        ));
    }
    if !is_sha256_digest(&manifest.tool_surface_digest) {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::MalformedSurfaceDigest,
        ));
    }
    if let Some(expected) = pinned_surface_digest
        && manifest.tool_surface_digest != expected
    {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::PinnedSurfaceDigestMismatch,
        ));
    }

    manifest.tools.sort_by(|left, right| left.name.cmp(&right.name));
    let manifest_names = manifest
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    if manifest_names.len() != manifest.tools.len() {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::DuplicateTool,
        ));
    }
    for required in REQUIRED_READ_TOOLS {
        if !manifest_names.contains(required) {
            return Ok(ConnectorCapabilityOutcome::Rejected(
                ConnectorCapabilityRejection::MissingRequiredTool,
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
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::UnsafeTool,
        ));
    }

    let live_by_name = live_tools
        .iter()
        .map(|tool| (tool.name.as_ref(), tool))
        .collect::<BTreeMap<_, _>>();
    let live_names = live_by_name.keys().copied().collect::<BTreeSet<_>>();
    if live_names.len() != live_tools.len() || live_names != manifest_names {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::ToolSurfaceMismatch,
        ));
    }
    for manifest_tool in &manifest.tools {
        let Some(live_tool) = live_by_name.get(manifest_tool.name.as_str()) else {
            return Ok(ConnectorCapabilityOutcome::Rejected(
                ConnectorCapabilityRejection::ToolSurfaceMismatch,
            ));
        };
        let Some(annotations) = live_tool.annotations.as_ref() else {
            return Ok(ConnectorCapabilityOutcome::Rejected(
                ConnectorCapabilityRejection::MissingToolAnnotations,
            ));
        };
        if annotations.read_only_hint != Some(true) || annotations.destructive_hint == Some(true) {
            return Ok(ConnectorCapabilityOutcome::Rejected(
                ConnectorCapabilityRejection::UnsafeToolAnnotations,
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
            return Ok(ConnectorCapabilityOutcome::Rejected(
                ConnectorCapabilityRejection::ToolSchemaDigestMismatch,
            ));
        }
    }
    if digest_value(
        serde_json::to_value(&manifest.tools)
            .map_err(|source| ConnectorError::from_source(ConnectorFailure::SchemaDigestMismatch, false, source))?,
    ) != manifest.tool_surface_digest
    {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::ToolSurfaceDigestMismatch,
        ));
    }

    let manifest_resources = manifest.resources.iter().cloned().collect::<BTreeSet<_>>();
    if manifest_resources.len() != manifest.resources.len() {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::DuplicateResource,
        ));
    }
    let cluster_prefix = format!("rocketmq://clusters/{expected_cluster}/");
    if manifest_resources
        .iter()
        .any(|uri| !uri.starts_with(&cluster_prefix) && !ALLOWED_SYSTEM_RESOURCES.contains(&uri.as_str()))
    {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::ResourceClusterMismatch,
        ));
    }
    let live_relevant_resources = live_resource_uris
        .iter()
        .filter(|uri| uri.starts_with(&cluster_prefix) || ALLOWED_SYSTEM_RESOURCES.contains(&uri.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    if live_relevant_resources != manifest_resources {
        return Ok(ConnectorCapabilityOutcome::Rejected(
            ConnectorCapabilityRejection::ResourceSurfaceMismatch,
        ));
    }
    Ok(ConnectorCapabilityOutcome::Verified(VerifiedCapability {
        manifest,
        observed_at: Utc::now(),
    }))
}

#[must_use]
pub(crate) fn digest_value(value: Value) -> String {
    let canonical = canonical_value(value);
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    format!(
        "sha256:{}",
        rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(encoded))
    )
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

    fn assert_rejection(
        manifest: CapabilityManifest,
        tools: &[Tool],
        resources: &BTreeSet<String>,
        pin: Option<&str>,
        expected: ConnectorCapabilityRejection,
    ) {
        let outcome = verify_manifest(manifest, "local", tools, resources, pin).expect("verification must complete");
        assert!(matches!(outcome, ConnectorCapabilityOutcome::Rejected(actual) if actual == expected));
    }

    #[test]
    fn accepts_matching_read_only_surface() {
        let (manifest, tools, resources) = fixture();
        let ConnectorCapabilityOutcome::Verified(verified) =
            verify_manifest(manifest, "local", &tools, &resources, None).expect("surface should verify")
        else {
            panic!("matching surface must verify");
        };
        assert!(!verified.manifest.mutation_supported);
    }

    #[test]
    fn rejects_mutation_and_schema_drift() {
        let (mut mutation, tools, resources) = fixture();
        mutation.mutation_supported = true;
        assert!(matches!(
            verify_manifest(mutation, "local", &tools, &resources, None).expect("verification must complete"),
            ConnectorCapabilityOutcome::Rejected(ConnectorCapabilityRejection::MutationSupported)
        ));

        let (mut tasks, tools, resources) = fixture();
        tasks.tools[0].task_support = "optional".to_owned();
        tasks.tool_surface_digest = digest_value(serde_json::to_value(&tasks.tools).expect("tools serialize"));
        assert!(matches!(
            verify_manifest(tasks, "local", &tools, &resources, None).expect("verification must complete"),
            ConnectorCapabilityOutcome::Rejected(ConnectorCapabilityRejection::UnsafeTool)
        ));

        let (mut drift, tools, resources) = fixture();
        drift.tools[0].schema_digest = format!("sha256:{}", "0".repeat(64));
        drift.tool_surface_digest = digest_value(serde_json::to_value(&drift.tools).expect("tools serialize"));
        assert!(matches!(
            verify_manifest(drift, "local", &tools, &resources, None).expect("verification must complete"),
            ConnectorCapabilityOutcome::Rejected(ConnectorCapabilityRejection::ToolSchemaDigestMismatch)
        ));
    }

    #[test]
    fn protocol_schema_cluster_digest_and_resource_drift_are_closed() {
        let (mut protocol, tools, resources) = fixture();
        protocol.mcp_protocol_version = "unsupported".to_owned();
        assert_rejection(
            protocol,
            &tools,
            &resources,
            None,
            ConnectorCapabilityRejection::ProtocolMismatch,
        );

        let (mut schema, tools, resources) = fixture();
        schema.business_schema_version = "rocketmq-mcp.v99".to_owned();
        assert_rejection(
            schema,
            &tools,
            &resources,
            None,
            ConnectorCapabilityRejection::UnsupportedBusinessSchema,
        );

        let (mut cluster, tools, resources) = fixture();
        cluster.cluster = "other".to_owned();
        assert_rejection(
            cluster,
            &tools,
            &resources,
            None,
            ConnectorCapabilityRejection::ClusterMismatch,
        );

        let (mut digest, tools, resources) = fixture();
        digest.tool_surface_digest = "not-a-digest".to_owned();
        assert_rejection(
            digest,
            &tools,
            &resources,
            None,
            ConnectorCapabilityRejection::MalformedSurfaceDigest,
        );

        let (manifest, tools, resources) = fixture();
        let other_pin = format!("sha256:{}", "f".repeat(64));
        assert_rejection(
            manifest,
            &tools,
            &resources,
            Some(&other_pin),
            ConnectorCapabilityRejection::PinnedSurfaceDigestMismatch,
        );

        let (mut resource, tools, mut resources) = fixture();
        resource.resources.push("rocketmq://clusters/other/overview".to_owned());
        resources.insert("rocketmq://clusters/other/overview".to_owned());
        assert_rejection(
            resource,
            &tools,
            &resources,
            None,
            ConnectorCapabilityRejection::ResourceClusterMismatch,
        );

        let (mut resource_surface, tools, resources) = fixture();
        resource_surface.resources.pop();
        assert_rejection(
            resource_surface,
            &tools,
            &resources,
            None,
            ConnectorCapabilityRejection::ResourceSurfaceMismatch,
        );
    }
}
