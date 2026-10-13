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

//! Static MCP resources exposed by the RocketMQ MCP server.

pub mod capability;
pub(crate) mod cursor;
pub mod reader;
pub mod registry;
pub mod system;
pub mod uri;

/// The JSON content of one Resource before the shared output processing.
///
/// A read hands this to the Guard as a value. The Guard adds the correlation identifiers,
/// sanitizes and bounds it, and only then writes it as text, once.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceDocument {
    /// The canonical URI the content is published under.
    pub uri: String,
    /// The content. Anything but an object is published without correlation identifiers.
    pub value: serde_json::Value,
}

impl ResourceDocument {
    /// Writes the content as indented JSON, without the shared output processing.
    ///
    /// # Errors
    ///
    /// Fails when the value cannot be written as JSON.
    pub fn into_read_result(self) -> Result<rmcp::model::ReadResourceResult, rmcp::ErrorData> {
        let text = serde_json::to_string_pretty(&self.value)
            .map_err(|source| crate::McpError::from_source(source).into_error_data())?;
        Ok(rmcp::model::ReadResourceResult::new(vec![
            rmcp::model::ResourceContents::text(text, self.uri).with_mime_type(uri::JSON_MIME_TYPE),
        ]))
    }
}
