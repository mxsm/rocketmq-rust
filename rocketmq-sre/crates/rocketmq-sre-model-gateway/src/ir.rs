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

/// A provider-neutral image reference.
///
/// Data URLs are intentionally represented as references. The gateway's
/// evidence preparation layer remains responsible for bounding and redacting
/// image data before it reaches this contract.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelImageSource {
    Url { url: String },
    DataReference { reference: String },
}

/// One typed content part in a canonical message.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelContentPart {
    Text {
        text: String,
    },
    Json {
        value: Value,
    },
    Image {
        media_type: String,
        source: ModelImageSource,
    },
    ToolCall {
        call: ModelToolCall,
    },
    ToolResult {
        tool_call_id: String,
        content: Value,
        is_error: bool,
    },
    Reasoning {
        text: String,
    },
}

/// Provider-neutral model message.
///
/// `content` is retained as a compatibility convenience for text-only Phase 00
/// callers. New integrations should use typed `parts`; adapters merge the text
/// field and typed text parts deterministically.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: ModelRole,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<ModelContentPart>,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
    /// Provider-neutral reasoning channel. Profiles decide whether the
    /// provider can receive or return this field.
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ModelToolCall>,
}

impl ModelMessage {
    /// Creates a text-only message.
    #[must_use]
    pub fn text(role: ModelRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            parts: Vec::new(),
            name: None,
            tool_call_id: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
        }
    }

    /// Adds provider-neutral reasoning content.
    #[must_use]
    pub fn with_reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.reasoning_content = Some(reasoning.into());
        self
    }

    /// Adds a previous assistant tool call.
    #[must_use]
    pub fn with_tool_call(mut self, tool_call: ModelToolCall) -> Self {
        self.tool_calls.push(tool_call);
        self
    }
}

/// Read-only tool contract exposed to a model.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    /// AI SRE model-visible tools must remain false in the read-only baseline.
    pub mutates_cluster: bool,
    #[serde(default)]
    pub strict: bool,
}

impl ModelTool {
    /// Creates a read-only model tool.
    #[must_use]
    pub fn read_only(name: impl Into<String>, description: impl Into<String>, input_schema: Value) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            mutates_cluster: false,
            strict: false,
        }
    }

    /// Enables native strict schema enforcement for a read-only tool.
    #[must_use]
    pub const fn with_strict(mut self) -> Self {
        self.strict = true;
        self
    }
}

/// Provider-neutral tool selection policy.
#[derive(Clone, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
    Specific {
        name: String,
    },
}

/// Requested model response representation.
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseFormat {
    #[default]
    Text,
    JsonObject,
    JsonSchema {
        name: String,
        schema: Value,
        #[serde(default)]
        strict: bool,
    },
}

/// Explicit provider-profile-gated extensions.
#[derive(Clone, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelRequestExtensions {
    /// Kimi/Moonshot's MFJS mode. This is rejected unless the selected profile
    /// explicitly declares MFJS support.
    #[serde(default)]
    pub kimi_mfjs: bool,
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
    #[serde(default)]
    pub tool_choice: ToolChoice,
    #[serde(default)]
    pub response_format: ResponseFormat,
    pub temperature_milli: Option<u16>,
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub reasoning: bool,
    #[serde(default)]
    pub extensions: ModelRequestExtensions,
}

impl CanonicalModelRequest {
    /// Creates a text-generation request with safe defaults.
    #[must_use]
    pub fn new(correlation_id: CorrelationId, model: impl Into<String>, messages: Vec<ModelMessage>) -> Self {
        Self {
            schema: SchemaVersion::new("rocketmq-sre.model-request", 1, 0),
            correlation_id,
            model: model.into(),
            messages,
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            response_format: ResponseFormat::Text,
            temperature_milli: None,
            max_output_tokens: None,
            stream: false,
            reasoning: false,
            extensions: ModelRequestExtensions::default(),
        }
    }
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
    Safety,
    Cancelled,
    Error,
    Unknown,
}

/// Provider-neutral token usage.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub total_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
    pub cached_input_tokens: Option<u32>,
}

/// Canonical response after provider-specific translation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalModelResponse {
    pub schema: SchemaVersion,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<ModelContentPart>,
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ModelToolCall>,
    pub finish_reason: FinishReason,
    /// Compatibility fields retained for Phase 00 consumers.
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    #[serde(default)]
    pub usage: ModelUsage,
    pub provider_request_id: Option<String>,
}

impl CanonicalModelResponse {
    /// Creates a canonical text response.
    #[must_use]
    pub fn text(
        provider: impl Into<String>,
        model: impl Into<String>,
        content: impl Into<String>,
        finish_reason: FinishReason,
    ) -> Self {
        Self {
            schema: SchemaVersion::new("rocketmq-sre.model-response", 1, 0),
            provider: provider.into(),
            model: model.into(),
            content: content.into(),
            parts: Vec::new(),
            reasoning_content: None,
            tool_calls: Vec::new(),
            finish_reason,
            input_tokens: None,
            output_tokens: None,
            usage: ModelUsage::default(),
            provider_request_id: None,
        }
    }
}

/// Canonical embedding request.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalEmbeddingRequest {
    pub schema: SchemaVersion,
    pub correlation_id: CorrelationId,
    pub model: String,
    pub inputs: Vec<String>,
    pub dimensions: Option<u32>,
}

/// Canonical embedding response.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalEmbeddingResponse {
    pub schema: SchemaVersion,
    pub provider: String,
    pub model: String,
    pub embeddings: Vec<Vec<f32>>,
    pub usage: ModelUsage,
}

/// One reranking candidate.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RerankDocument {
    pub id: String,
    pub text: String,
}

/// Canonical reranking request.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalRerankRequest {
    pub schema: SchemaVersion,
    pub correlation_id: CorrelationId,
    pub model: String,
    pub query: String,
    pub documents: Vec<RerankDocument>,
    pub top_n: Option<u32>,
}

/// One normalized reranking result.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RerankResult {
    pub document_id: String,
    pub index: u32,
    pub relevance_score: f64,
}

/// Canonical reranking response.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CanonicalRerankResponse {
    pub schema: SchemaVersion,
    pub provider: String,
    pub model: String,
    pub results: Vec<RerankResult>,
}

/// One bounded streaming event.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelStreamEvent {
    Start {
        provider_request_id: Option<String>,
    },
    TextDelta {
        delta: String,
    },
    ReasoningDelta {
        delta: String,
    },
    ToolCallDelta {
        index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments_delta: String,
    },
    Usage {
        usage: ModelUsage,
    },
    Finish {
        reason: FinishReason,
    },
    Error {
        code: String,
        retryable: bool,
    },
}
