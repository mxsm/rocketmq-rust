// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use rmcp::model::ReadResourceResult;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::model::contract::SCHEMA_VERSION;
use crate::resources::ResourceDocument;
use crate::tools::catalog::ToolDescriptor;
use crate::tools::catalog::ToolId;

/// The MCP protocol revision this server implements.
///
/// A build with the HTTP transport takes it from `rocketmq-mcp-auth`, the crate it shares with the
/// control server, so that both servers answer the same revision. A build without that transport
/// does not depend on that crate and states the revision itself. The capability snapshot holds
/// both to the same value.
#[cfg(feature = "streamable-http")]
pub const MCP_PROTOCOL_VERSION: &str = rocketmq_mcp_auth::MCP_PROTOCOL_VERSION;
/// The MCP protocol revision this server implements.
///
/// A build with the HTTP transport takes it from the crate it shares with the control server.
/// The capability snapshot holds both to the same value.
#[cfg(not(feature = "streamable-http"))]
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityManifest {
    pub mcp_protocol_version: &'static str,
    pub business_schema_version: &'static str,
    pub server_version: &'static str,
    pub cluster: String,
    pub tools: Vec<CapabilityTool>,
    pub resources: Vec<String>,
    pub tool_surface_digest: String,
    pub mutation_supported: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityTool {
    pub name: String,
    pub risk_level: String,
    pub schema_digest: String,
    pub read_only: bool,
    pub destructive: bool,
    pub task_support: &'static str,
    pub mutates_cluster: bool,
}

pub fn manifest_for(
    cluster: &str,
    descriptors: impl IntoIterator<Item = ToolDescriptor>,
    include_system_resources: bool,
) -> CapabilityManifest {
    let mut tools = descriptors
        .into_iter()
        .map(|descriptor| CapabilityTool {
            name: descriptor.name.to_string(),
            risk_level: descriptor.risk_level.to_string(),
            schema_digest: schema_digest(descriptor.id),
            read_only: descriptor.annotations.read_only,
            destructive: descriptor.annotations.destructive,
            task_support: "forbidden",
            mutates_cluster: false,
        })
        .collect::<Vec<_>>();
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    let tool_surface_digest = digest_value(serde_json::to_value(&tools).unwrap_or(Value::Null));
    let mut resources = vec![
        format!("rocketmq://clusters/{cluster}/capabilities"),
        format!("rocketmq://clusters/{cluster}/overview"),
        format!("rocketmq://clusters/{cluster}/topics"),
        format!("rocketmq://clusters/{cluster}/brokers"),
        format!("rocketmq://clusters/{cluster}/consumer-groups"),
    ];
    if include_system_resources {
        resources.extend([
            "rocketmq://system/runtime/v1".to_string(),
            "rocketmq://system/observability/v1".to_string(),
        ]);
    }
    CapabilityManifest {
        mcp_protocol_version: MCP_PROTOCOL_VERSION,
        business_schema_version: SCHEMA_VERSION,
        server_version: env!("CARGO_PKG_VERSION"),
        cluster: cluster.to_string(),
        resources,
        tools,
        tool_surface_digest,
        mutation_supported: false,
    }
}

/// Returns the digest of the input and the output schema of a Tool.
///
/// The schemas of a Tool never change while the server runs, so every digest is computed once.
fn schema_digest(tool: ToolId) -> String {
    static DIGESTS: LazyLock<BTreeMap<&'static str, String>> = LazyLock::new(|| {
        ToolId::ALL
            .iter()
            .map(|tool| (tool.descriptor().name, compute_schema_digest(*tool)))
            .collect()
    });
    match DIGESTS.get(tool.descriptor().name) {
        Some(digest) => digest.clone(),
        None => compute_schema_digest(tool),
    }
}

fn compute_schema_digest(tool: ToolId) -> String {
    let definition = tool.definition();
    digest_value(serde_json::json!({
        "input": definition.input_schema,
        "output": definition.output_schema,
    }))
}

/// Writes a manifest as indented JSON, without the shared output processing.
///
/// # Errors
///
/// Fails when the manifest cannot be written as JSON.
pub fn read_result(uri: &str, manifest: CapabilityManifest) -> Result<ReadResourceResult, rmcp::ErrorData> {
    document(uri, manifest)?.into_read_result()
}

/// Returns a manifest as the value the Guard processes and then publishes.
///
/// # Errors
///
/// Fails when the manifest cannot be represented as JSON.
pub fn document(uri: &str, manifest: CapabilityManifest) -> Result<ResourceDocument, rmcp::ErrorData> {
    let value =
        serde_json::to_value(&manifest).map_err(|source| crate::McpError::from_source(source).into_error_data())?;
    Ok(ResourceDocument {
        uri: uri.to_string(),
        value,
    })
}

fn digest_value(value: Value) -> String {
    let canonical = canonical_value(value);
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    let digest = Sha256::digest(encoded);
    format!(
        "sha256:{}",
        digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_read_only_and_digest_is_stable() {
        let manifest = manifest_for("local-dev", ToolId::ALL.iter().map(|tool| tool.descriptor()), true);
        let second = manifest_for("local-dev", ToolId::ALL.iter().map(|tool| tool.descriptor()), true);
        assert!(!manifest.mutation_supported);
        assert_eq!(manifest.tool_surface_digest, second.tool_surface_digest);
        assert!(manifest
            .tools
            .iter()
            .all(|tool| tool.read_only && !tool.destructive && !tool.mutates_cluster));
    }

    #[test]
    fn a_kept_schema_digest_is_the_digest_of_the_published_schemas() {
        for tool in ToolId::ALL {
            assert_eq!(
                schema_digest(*tool),
                compute_schema_digest(*tool),
                "{}",
                tool.descriptor().name
            );
        }
        let manifest = manifest_for("local-dev", ToolId::ALL.iter().map(|tool| tool.descriptor()), false);
        for entry in &manifest.tools {
            let tool = ToolId::resolve(&entry.name).expect("a catalog Tool");
            assert_eq!(entry.schema_digest, compute_schema_digest(tool), "{}", entry.name);
        }
    }
}
