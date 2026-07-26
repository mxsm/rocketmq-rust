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

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::SchemaVersion;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// Provider-neutral conversation role.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    System,
    User,
    Assistant,
    Tool,
}

/// Provider-neutral model message.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub content: String,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
}

/// Read-only tool contract exposed to a model.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    /// Phase 00 requires every model-visible tool to remain false.
    pub mutates_cluster: bool,
}

/// Requested model response representation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseFormat {
    Text,
    JsonObject,
    JsonSchema { name: String, schema: Value },
}

/// Canonical request before provider-specific translation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalModelRequest {
    pub schema: SchemaVersion,
    pub correlation_id: CorrelationId,
    pub model: String,
    pub messages: Vec<ModelMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ModelTool>,
    pub response_format: ResponseFormat,
    pub temperature_milli: Option<u16>,
    pub max_output_tokens: Option<u32>,
}

/// Provider-neutral tool call selected by a model.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// Why a provider stopped generation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Unknown,
}

/// Canonical response after provider-specific translation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalModelResponse {
    pub schema: SchemaVersion,
    pub provider: String,
    pub model: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ModelToolCall>,
    pub finish_reason: FinishReason,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
}
