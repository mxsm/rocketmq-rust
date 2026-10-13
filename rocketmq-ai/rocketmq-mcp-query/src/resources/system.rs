// Copyright 2026 The RocketMQ Rust Authors
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

use rmcp::model::ReadResourceResult;
use rmcp::ErrorData;
use serde::Serialize;

use crate::resources::ResourceDocument;

/// Writes a system Resource as indented JSON, without the shared output processing.
///
/// # Errors
///
/// Fails when `value` cannot be represented as JSON.
pub fn read_result(uri: &str, kind: &str, value: impl Serialize) -> Result<ReadResourceResult, ErrorData> {
    document(uri, kind, value)?.into_read_result().map_err(encoding_failed)
}

/// Returns a system Resource as the value the Guard processes and then publishes.
///
/// # Errors
///
/// Fails when `value` cannot be represented as JSON.
pub fn document(uri: &str, kind: &str, value: impl Serialize) -> Result<ResourceDocument, ErrorData> {
    let data = serde_json::to_value(value)
        .map_err(|source| encoding_failed(crate::McpError::from_source(source).into_error_data()))?;
    Ok(ResourceDocument {
        uri: uri.to_string(),
        value: serde_json::json!({
            "schema_version": "rocketmq-mcp.system-resource.v1",
            "resource": uri,
            "source": "mcp_process",
            "partial": false,
            "warnings": [],
            "kind": kind,
            "data": data,
        }),
    })
}

fn encoding_failed(source: ErrorData) -> ErrorData {
    ErrorData::internal_error(
        "failed to encode system resource",
        Some(serde_json::json!({
            "code": "system_resource_encoding_failed",
            "retryable": false,
            "reason": source.message,
        })),
    )
}
