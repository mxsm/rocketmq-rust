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
use rmcp::model::ResourceContents;
use rmcp::ErrorData;
use serde::Serialize;

use crate::resources::uri::JSON_MIME_TYPE;

pub fn read_result(uri: &str, kind: &str, value: impl Serialize) -> Result<ReadResourceResult, ErrorData> {
    let payload = serde_json::json!({
        "schema_version": "rocketmq-mcp.system-resource.v1",
        "resource": uri,
        "source": "mcp_process",
        "partial": false,
        "warnings": [],
        "kind": kind,
        "data": value,
    });
    let text = serde_json::to_string_pretty(&payload).map_err(|error| {
        ErrorData::internal_error(
            "failed to encode system resource",
            Some(serde_json::json!({
                "code": "system_resource_encoding_failed",
                "retryable": false,
                "reason": error.to_string(),
            })),
        )
    })?;
    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type(JSON_MIME_TYPE)
    ]))
}
