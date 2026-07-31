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

use rmcp::model::ReadResourceResult;
use rmcp::model::ResourceContents;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::model::contract::SCHEMA_VERSION;
use crate::resources::uri::JSON_MIME_TYPE;
use crate::tools::catalog::ToolDescriptor;

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
        .map(|descriptor| {
            let definition = descriptor.id.definition();
            let schema = serde_json::json!({
                "input": definition.input_schema,
                "output": definition.output_schema,
            });
            CapabilityTool {
                name: descriptor.name.to_string(),
                risk_level: descriptor.risk_level.to_string(),
                schema_digest: digest_value(schema),
                read_only: descriptor.annotations.read_only,
                destructive: descriptor.annotations.destructive,
                task_support: "forbidden",
                mutates_cluster: false,
            }
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

pub fn read_result(uri: &str, manifest: CapabilityManifest) -> Result<ReadResourceResult, rmcp::ErrorData> {
    let text = serde_json::to_string_pretty(&manifest).map_err(|error| {
        rmcp::ErrorData::internal_error(format!("failed to encode capability manifest: {error}"), None)
    })?;
    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type(JSON_MIME_TYPE)
    ]))
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
    use crate::tools::catalog::ToolId;

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
}
