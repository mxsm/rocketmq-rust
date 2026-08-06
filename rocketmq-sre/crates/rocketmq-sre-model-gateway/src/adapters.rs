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

use std::sync::Arc;

use rocketmq_sre_contracts::SchemaVersion;
use serde_json::Value;
use serde_json::json;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::error::map_provider_status;
use crate::ir::CanonicalEmbeddingRequest;
use crate::ir::CanonicalEmbeddingResponse;
use crate::ir::CanonicalModelRequest;
use crate::ir::CanonicalModelResponse;
use crate::ir::CanonicalRerankRequest;
use crate::ir::CanonicalRerankResponse;
use crate::ir::FinishReason;
use crate::ir::ModelContentPart;
use crate::ir::ModelMessage;
use crate::ir::ModelRole;
use crate::ir::ModelToolCall;
use crate::ir::ModelUsage;
use crate::ir::RerankResult;
use crate::ir::ResponseFormat;
use crate::ir::ToolChoice;
use crate::profile::ProviderCapabilities;
use crate::profile::ProviderDialect;
use crate::profile::ProviderFamily;
use crate::profile::ProviderHealth;
use crate::profile::ProviderProfile;
use crate::provider::ChatModelProvider;
use crate::provider::EmbeddingProvider;
use crate::provider::InvocationContext;
use crate::provider::RerankProvider;
use crate::secret::SecretProvider;
use crate::secret::SecretReferenceKind;
use crate::stream::BoundedModelStream;
use crate::transport::ModelTransport;
use crate::transport::TransportRequest;
use crate::transport::TransportResponse;

struct ProtocolAdapter {
    profile: ProviderProfile,
    transport: Arc<dyn ModelTransport>,
    secrets: Arc<dyn SecretProvider>,
}

impl ProtocolAdapter {
    fn new(
        profile: ProviderProfile,
        transport: Arc<dyn ModelTransport>,
        secrets: Arc<dyn SecretProvider>,
    ) -> Result<Self, ProviderError> {
        profile.validate()?;
        if profile
            .credential_ref
            .as_ref()
            .is_some_and(|reference| matches!(reference.kind(), SecretReferenceKind::Adapter))
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "built-in provider credentials must be gateway-owned",
            ));
        }
        Ok(Self {
            profile,
            transport,
            secrets,
        })
    }

    fn transport_request(
        &self,
        context: &InvocationContext,
        path: String,
        body: Value,
    ) -> Result<TransportRequest, ProviderError> {
        let credential = self
            .profile
            .credential_ref
            .as_ref()
            .map(|reference| self.secrets.resolve(reference))
            .transpose()?;
        Ok(TransportRequest {
            correlation_id: context.correlation_id,
            dialect: self.profile.dialect,
            endpoint: self.profile.endpoint.clone(),
            path,
            body,
            credential,
            deadline_unix_ms: context.deadline_unix_ms,
            max_response_bytes: context.max_response_bytes,
        })
    }

    fn invoke_chat(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderError> {
        let credential = self
            .profile
            .credential_ref
            .as_ref()
            .map(|reference| self.secrets.resolve(reference))
            .transpose()?;
        let transport_request = build_chat_transport_request(&self.profile, context, request, credential)?;
        let response = self.transport.invoke(transport_request)?;
        parse_chat_transport_response(&self.profile, response, context.max_response_bytes)
    }

    fn invoke_chat_stream(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<BoundedModelStream, ProviderError> {
        context.ensure_active()?;
        let mut stream_request = request.clone();
        stream_request.stream = true;
        self.profile.capabilities.ensure_request_supported(&stream_request)?;
        let (path, body) = match self.profile.provider_family {
            ProviderFamily::OpenAiCompatible => (
                openai_path(&self.profile),
                openai_request(&self.profile, &stream_request),
            ),
            ProviderFamily::Anthropic => (
                "/messages".to_owned(),
                anthropic_request(&self.profile, &stream_request),
            ),
            ProviderFamily::Gemini => (
                format!(
                    "/models/{}:streamGenerateContent",
                    encode_path_segment(&self.profile.model)
                ),
                gemini_request(&stream_request),
            ),
            ProviderFamily::Bedrock => (
                format!("/model/{}/converse-stream", encode_path_segment(&self.profile.model)),
                bedrock_request(&stream_request),
            ),
            ProviderFamily::ProviderSpi => {
                return Err(ProviderError::capability_unsupported(
                    "provider SPI requires ProviderSpiClient",
                ));
            }
        };
        let request = self.transport_request(context, path, body)?;
        self.transport
            .invoke_stream(request, context.stream_bounds, context.cancellation.clone())
    }

    fn invoke_embedding(
        &self,
        context: &InvocationContext,
        request: &CanonicalEmbeddingRequest,
    ) -> Result<CanonicalEmbeddingResponse, ProviderError> {
        context.ensure_active()?;
        if !self
            .profile
            .capabilities
            .supported
            .contains(&crate::profile::ProviderCapability::Embeddings)
        {
            return Err(ProviderError::capability_unsupported(
                "provider profile does not support embeddings",
            ));
        }
        let body = json!({
            "model": request.model,
            "input": request.inputs,
            "dimensions": request.dimensions,
        });
        let transport_request = self.transport_request(context, "/embeddings".to_owned(), body)?;
        let response = self.transport.invoke(transport_request)?;
        ensure_response_bound(&response, context.max_response_bytes)?;
        if !(200..300).contains(&response.status) {
            return Err(map_provider_status(response.status));
        }
        let embeddings = response
            .body
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(protocol_error)?
            .iter()
            .map(|item| {
                item.get("embedding")
                    .and_then(Value::as_array)
                    .ok_or_else(protocol_error)?
                    .iter()
                    .map(|number| number.as_f64().map(|value| value as f32).ok_or_else(protocol_error))
                    .collect()
            })
            .collect::<Result<Vec<Vec<f32>>, ProviderError>>()?;
        Ok(CanonicalEmbeddingResponse {
            schema: SchemaVersion::new("rocketmq-sre.embedding-response", 1, 0),
            provider: self.profile.id.clone(),
            model: self.profile.model.clone(),
            embeddings,
            usage: parse_openai_usage(&response.body),
        })
    }

    fn invoke_rerank(
        &self,
        context: &InvocationContext,
        request: &CanonicalRerankRequest,
    ) -> Result<CanonicalRerankResponse, ProviderError> {
        context.ensure_active()?;
        if !self
            .profile
            .capabilities
            .supported
            .contains(&crate::profile::ProviderCapability::Rerank)
        {
            return Err(ProviderError::capability_unsupported(
                "provider profile does not support reranking",
            ));
        }
        let body = json!({
            "model": request.model,
            "query": request.query,
            "documents": request.documents,
            "top_n": request.top_n,
        });
        let transport_request = self.transport_request(context, "/rerank".to_owned(), body)?;
        let response = self.transport.invoke(transport_request)?;
        ensure_response_bound(&response, context.max_response_bytes)?;
        if !(200..300).contains(&response.status) {
            return Err(map_provider_status(response.status));
        }
        let results = response
            .body
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(protocol_error)?
            .iter()
            .enumerate()
            .map(|(fallback_index, item)| {
                let index = item
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or(fallback_index as u64) as u32;
                let document_id = request
                    .documents
                    .get(index as usize)
                    .map_or_else(|| index.to_string(), |document| document.id.clone());
                let relevance_score = item
                    .get("relevance_score")
                    .or_else(|| item.get("score"))
                    .and_then(Value::as_f64)
                    .ok_or_else(protocol_error)?;
                Ok(RerankResult {
                    document_id,
                    index,
                    relevance_score,
                })
            })
            .collect::<Result<Vec<_>, ProviderError>>()?;
        Ok(CanonicalRerankResponse {
            schema: SchemaVersion::new("rocketmq-sre.rerank-response", 1, 0),
            provider: self.profile.id.clone(),
            model: self.profile.model.clone(),
            results,
        })
    }
}

macro_rules! define_chat_adapter {
    ($name:ident, $family:pat) => {
        /// Provider protocol adapter backed by an injected network transport.
        pub struct $name {
            inner: ProtocolAdapter,
        }

        impl $name {
            /// Creates the adapter after validating its provider family.
            ///
            /// # Errors
            ///
            /// Returns a profile error when the selected family is incompatible
            /// or the profile violates gateway invariants.
            pub fn new(
                profile: ProviderProfile,
                transport: Arc<dyn ModelTransport>,
                secrets: Arc<dyn SecretProvider>,
            ) -> Result<Self, ProviderError> {
                if !matches!(profile.provider_family, $family) {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProfileInvalid,
                        "provider profile family does not match adapter",
                    ));
                }
                Ok(Self {
                    inner: ProtocolAdapter::new(profile, transport, secrets)?,
                })
            }
        }

        impl ChatModelProvider for $name {
            fn profile_id(&self) -> &str {
                &self.inner.profile.id
            }

            fn capabilities(&self) -> ProviderCapabilities {
                self.inner.profile.capabilities.clone()
            }

            fn health(&self) -> ProviderHealth {
                self.inner.profile.health
            }

            fn invoke(
                &self,
                context: &InvocationContext,
                request: &CanonicalModelRequest,
            ) -> Result<CanonicalModelResponse, ProviderError> {
                self.inner.invoke_chat(context, request)
            }

            fn invoke_stream(
                &self,
                context: &InvocationContext,
                request: &CanonicalModelRequest,
            ) -> Result<BoundedModelStream, ProviderError> {
                self.inner.invoke_chat_stream(context, request)
            }
        }
    };
}

define_chat_adapter!(OpenAiCompatibleAdapter, ProviderFamily::OpenAiCompatible);
define_chat_adapter!(AnthropicMessagesAdapter, ProviderFamily::Anthropic);
define_chat_adapter!(GeminiNativeAdapter, ProviderFamily::Gemini);
define_chat_adapter!(BedrockConverseAdapter, ProviderFamily::Bedrock);

impl EmbeddingProvider for OpenAiCompatibleAdapter {
    fn profile_id(&self) -> &str {
        &self.inner.profile.id
    }

    fn embed(
        &self,
        context: &InvocationContext,
        request: &CanonicalEmbeddingRequest,
    ) -> Result<CanonicalEmbeddingResponse, ProviderError> {
        self.inner.invoke_embedding(context, request)
    }
}

impl RerankProvider for OpenAiCompatibleAdapter {
    fn profile_id(&self) -> &str {
        &self.inner.profile.id
    }

    fn rerank(
        &self,
        context: &InvocationContext,
        request: &CanonicalRerankRequest,
    ) -> Result<CanonicalRerankResponse, ProviderError> {
        self.inner.invoke_rerank(context, request)
    }
}

/// Constructs the correct built-in adapter for a profile.
///
/// # Errors
///
/// Returns a profile error for a proprietary SPI profile or an invalid
/// profile. Proprietary profiles must use [`crate::spi::ProviderSpiClient`].
pub fn adapter_for_profile(
    profile: ProviderProfile,
    transport: Arc<dyn ModelTransport>,
    secrets: Arc<dyn SecretProvider>,
) -> Result<Arc<dyn ChatModelProvider>, ProviderError> {
    match profile.provider_family {
        ProviderFamily::OpenAiCompatible => Ok(Arc::new(OpenAiCompatibleAdapter::new(profile, transport, secrets)?)),
        ProviderFamily::Anthropic => Ok(Arc::new(AnthropicMessagesAdapter::new(profile, transport, secrets)?)),
        ProviderFamily::Gemini => Ok(Arc::new(GeminiNativeAdapter::new(profile, transport, secrets)?)),
        ProviderFamily::Bedrock => Ok(Arc::new(BedrockConverseAdapter::new(profile, transport, secrets)?)),
        ProviderFamily::ProviderSpi => Err(ProviderError::new(
            ProviderErrorCode::ProfileInvalid,
            "provider SPI profiles require ProviderSpiClient",
        )),
    }
}

pub(crate) fn build_chat_transport_request(
    profile: &ProviderProfile,
    context: &InvocationContext,
    request: &CanonicalModelRequest,
    credential: Option<crate::secret::SecretMaterial>,
) -> Result<TransportRequest, ProviderError> {
    context.ensure_active()?;
    profile.capabilities.ensure_request_supported(request)?;
    let (path, body) = match profile.provider_family {
        ProviderFamily::OpenAiCompatible => (openai_path(profile), openai_request(profile, request)),
        ProviderFamily::Anthropic => ("/messages".to_owned(), anthropic_request(profile, request)),
        ProviderFamily::Gemini => (
            format!("/models/{}:generateContent", encode_path_segment(&profile.model)),
            gemini_request(request),
        ),
        ProviderFamily::Bedrock => (
            format!("/model/{}/converse", encode_path_segment(&profile.model)),
            bedrock_request(request),
        ),
        ProviderFamily::ProviderSpi => {
            return Err(ProviderError::capability_unsupported(
                "provider SPI requires ProviderSpiClient",
            ));
        }
    };
    Ok(TransportRequest {
        correlation_id: context.correlation_id,
        dialect: profile.dialect,
        endpoint: profile.endpoint.clone(),
        path,
        body,
        credential,
        deadline_unix_ms: context.deadline_unix_ms,
        max_response_bytes: context.max_response_bytes,
    })
}

pub(crate) fn parse_chat_transport_response(
    profile: &ProviderProfile,
    response: TransportResponse,
    max_response_bytes: usize,
) -> Result<CanonicalModelResponse, ProviderError> {
    ensure_response_bound(&response, max_response_bytes)?;
    if !(200..300).contains(&response.status) {
        return Err(map_provider_status(response.status));
    }
    match profile.provider_family {
        ProviderFamily::OpenAiCompatible if profile.dialect == ProviderDialect::DeepSeekResponses => {
            parse_deepseek_responses_response(profile, response.body)
        }
        ProviderFamily::OpenAiCompatible => parse_openai_response(profile, response.body),
        ProviderFamily::Anthropic => parse_anthropic_response(profile, response.body),
        ProviderFamily::Gemini => parse_gemini_response(profile, response.body),
        ProviderFamily::Bedrock => parse_bedrock_response(profile, response.body),
        ProviderFamily::ProviderSpi => Err(ProviderError::capability_unsupported(
            "provider SPI requires ProviderSpiClient",
        )),
    }
}

fn openai_path(profile: &ProviderProfile) -> String {
    if profile.dialect == ProviderDialect::DeepSeekResponses {
        "/responses".to_owned()
    } else if profile.dialect == ProviderDialect::AzureOpenAi {
        format!(
            "/openai/deployments/{}/chat/completions?api-version=2024-10-21",
            encode_path_segment(&profile.model)
        )
    } else {
        "/chat/completions".to_owned()
    }
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn role(role: ModelRole) -> &'static str {
    match role {
        ModelRole::System => "system",
        ModelRole::User => "user",
        ModelRole::Assistant => "assistant",
        ModelRole::Tool => "tool",
    }
}

fn openai_message(profile: &ProviderProfile, message: &ModelMessage) -> Value {
    let mut value = json!({
        "role": role(message.role),
        "content": message.content,
    });
    if let Some(name) = &message.name {
        value["name"] = Value::String(name.clone());
    }
    if let Some(tool_call_id) = &message.tool_call_id {
        value["tool_call_id"] = Value::String(tool_call_id.clone());
    }
    if profile.preserve_reasoning_content
        && let Some(reasoning) = &message.reasoning_content
    {
        value["reasoning_content"] = Value::String(reasoning.clone());
    }
    if !message.tool_calls.is_empty() {
        value["tool_calls"] = Value::Array(
            message
                .tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": call.arguments.to_string()
                        }
                    })
                })
                .collect(),
        );
    }
    if !message.parts.is_empty() {
        let mut content = Vec::new();
        if !message.content.is_empty() {
            content.push(json!({"type":"text","text":message.content}));
        }
        for part in &message.parts {
            match part {
                ModelContentPart::Text { text } => {
                    content.push(json!({"type":"text","text":text}));
                }
                ModelContentPart::Image { media_type, source } => {
                    content.push(json!({
                        "type":"image_url",
                        "image_url":{"url":source_reference(source),"media_type":media_type}
                    }));
                }
                ModelContentPart::Json { value } => {
                    content.push(json!({"type":"text","text":value.to_string()}));
                }
                ModelContentPart::ToolResult {
                    tool_call_id,
                    content: result,
                    is_error,
                } => {
                    content.push(json!({
                        "type":"tool_result",
                        "tool_call_id":tool_call_id,
                        "content":result,
                        "is_error":is_error
                    }));
                }
                ModelContentPart::ToolCall { call } => {
                    content.push(json!({
                        "type":"tool_call",
                        "id":call.id,
                        "name":call.name,
                        "arguments":call.arguments
                    }));
                }
                ModelContentPart::Reasoning { text } => {
                    if profile.preserve_reasoning_content {
                        value["reasoning_content"] = Value::String(text.clone());
                    }
                }
            }
        }
        value["content"] = Value::Array(content);
    }
    value
}

fn source_reference(source: &crate::ir::ModelImageSource) -> &str {
    match source {
        crate::ir::ModelImageSource::Url { url } => url,
        crate::ir::ModelImageSource::DataReference { reference } => reference,
    }
}

fn openai_request(profile: &ProviderProfile, request: &CanonicalModelRequest) -> Value {
    if profile.dialect == ProviderDialect::DeepSeekResponses {
        return deepseek_responses_request(request);
    }
    let mut body = json!({
        "model": request.model,
        "messages": request.messages.iter().map(|message| openai_message(profile, message)).collect::<Vec<_>>(),
        "temperature": request.temperature_milli.map(|value| f64::from(value) / 1_000.0),
        "max_tokens": request.max_output_tokens,
        "stream": request.stream,
    });
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    json!({
                        "type":"function",
                        "function":{
                            "name":tool.name,
                            "description":tool.description,
                            "parameters":tool.input_schema,
                            "strict":tool.strict
                        }
                    })
                })
                .collect(),
        );
        body["tool_choice"] = openai_tool_choice(&request.tool_choice);
    }
    match &request.response_format {
        ResponseFormat::Text => {}
        ResponseFormat::JsonObject => {
            body["response_format"] = json!({"type":"json_object"});
        }
        ResponseFormat::JsonSchema { name, schema, strict } => {
            body["response_format"] = json!({
                "type":"json_schema",
                "json_schema":{"name":name,"schema":schema,"strict":strict}
            });
        }
    }
    if request.extensions.kimi_mfjs && profile.kimi_mfjs_enabled {
        body["moonshot_json_mode"] = Value::String("mfjs".to_owned());
    }
    body
}

fn deepseek_responses_request(request: &CanonicalModelRequest) -> Value {
    let instructions = request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::System)
        .map(responses_message_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let mut input = Vec::new();
    for message in request
        .messages
        .iter()
        .filter(|message| message.role != ModelRole::System)
    {
        if message.role == ModelRole::Tool {
            if let Some(call_id) = &message.tool_call_id {
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": responses_message_text(message),
                }));
            }
            continue;
        }
        if let Some(reasoning) = &message.reasoning_content {
            input.push(json!({
                "type": "reasoning",
                "content": [{"type": "reasoning_text", "text": reasoning}],
            }));
        }
        let content = responses_message_text(message);
        if !content.is_empty() {
            input.push(json!({
                "type": "message",
                "role": role(message.role),
                "content": content,
            }));
        }
        input.extend(message.tool_calls.iter().map(|call| {
            json!({
                "type": "function_call",
                "call_id": call.id,
                "name": call.name,
                "arguments": call.arguments.to_string(),
            })
        }));
        for part in &message.parts {
            match part {
                ModelContentPart::ToolCall { call } => input.push(json!({
                    "type": "function_call",
                    "call_id": call.id,
                    "name": call.name,
                    "arguments": call.arguments.to_string(),
                })),
                ModelContentPart::ToolResult {
                    tool_call_id, content, ..
                } => input.push(json!({
                    "type": "function_call_output",
                    "call_id": tool_call_id,
                    "output": content.to_string(),
                })),
                ModelContentPart::Text { .. }
                | ModelContentPart::Json { .. }
                | ModelContentPart::Image { .. }
                | ModelContentPart::Reasoning { .. } => {}
            }
        }
    }
    let mut body = json!({
        "model": request.model,
        "input": input,
        "stream": request.stream,
    });
    if !instructions.is_empty() {
        body["instructions"] = Value::String(instructions);
    }
    if let Some(temperature) = request.temperature_milli {
        body["temperature"] = json!(f64::from(temperature) / 1_000.0);
    }
    if let Some(max_output_tokens) = request.max_output_tokens {
        body["max_output_tokens"] = json!(max_output_tokens);
    }
    if request.reasoning {
        body["reasoning"] = json!({"effort": "medium"});
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    json!({
                        "type": "function",
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                        "strict": tool.strict,
                    })
                })
                .collect(),
        );
        body["tool_choice"] = responses_tool_choice(&request.tool_choice);
    }
    match &request.response_format {
        ResponseFormat::Text => {}
        ResponseFormat::JsonObject => {
            body["text"] = json!({"format": {"type": "json_object"}});
        }
        ResponseFormat::JsonSchema { name, schema, strict } => {
            body["text"] = json!({
                "format": {
                    "type": "json_schema",
                    "name": name,
                    "schema": schema,
                    "strict": strict,
                }
            });
        }
    }
    body
}

fn responses_message_text(message: &ModelMessage) -> String {
    let mut content = Vec::new();
    if !message.content.is_empty() {
        content.push(message.content.clone());
    }
    for part in &message.parts {
        match part {
            ModelContentPart::Text { text } | ModelContentPart::Reasoning { text } => content.push(text.clone()),
            ModelContentPart::Json { value } => content.push(value.to_string()),
            ModelContentPart::Image { .. }
            | ModelContentPart::ToolCall { .. }
            | ModelContentPart::ToolResult { .. } => {}
        }
    }
    content.join("\n")
}

fn responses_tool_choice(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => Value::String("auto".to_owned()),
        ToolChoice::None => Value::String("none".to_owned()),
        ToolChoice::Required => Value::String("required".to_owned()),
        ToolChoice::Specific { name } => json!({"type": "function", "name": name}),
    }
}

fn openai_tool_choice(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => Value::String("auto".to_owned()),
        ToolChoice::None => Value::String("none".to_owned()),
        ToolChoice::Required => Value::String("required".to_owned()),
        ToolChoice::Specific { name } => {
            json!({"type":"function","function":{"name":name}})
        }
    }
}

fn anthropic_request(profile: &ProviderProfile, request: &CanonicalModelRequest) -> Value {
    let messages: Vec<Value> = request
        .messages
        .iter()
        .filter(|message| message.role != ModelRole::System)
        .map(|message| {
            let mut parts = Vec::new();
            if profile.preserve_reasoning_content
                && let Some(reasoning) = &message.reasoning_content
            {
                parts.push(json!({"type":"thinking","thinking":reasoning}));
            }
            if !message.content.is_empty() {
                parts.push(json!({"type":"text","text":message.content}));
            }
            for call in &message.tool_calls {
                parts.push(json!({
                    "type":"tool_use",
                    "id":call.id,
                    "name":call.name,
                    "input":call.arguments
                }));
            }
            json!({
                "role": if message.role == ModelRole::Assistant {"assistant"} else {"user"},
                "content": parts,
            })
        })
        .collect();
    let mut body = json!({
        "model": request.model,
        "messages": messages,
        "max_tokens": request.max_output_tokens.unwrap_or(1_024),
        "temperature": request.temperature_milli.map(|value| f64::from(value) / 1_000.0),
        "stream":request.stream,
    });
    let system: Vec<_> = request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::System)
        .map(|message| message.content.clone())
        .collect();
    if !system.is_empty() {
        body["system"] = Value::String(system.join("\n"));
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    json!({
                        "name":tool.name,
                        "description":tool.description,
                        "input_schema":tool.input_schema,
                        "strict":tool.strict
                    })
                })
                .collect(),
        );
        body["tool_choice"] = match &request.tool_choice {
            ToolChoice::Auto => json!({"type":"auto"}),
            ToolChoice::None => json!({"type":"none"}),
            ToolChoice::Required => json!({"type":"any"}),
            ToolChoice::Specific { name } => json!({"type":"tool","name":name}),
        };
    }
    match &request.response_format {
        ResponseFormat::Text => {}
        ResponseFormat::JsonObject => {
            body["output_format"] = json!({"type":"json_object"});
        }
        ResponseFormat::JsonSchema { name, schema, strict } => {
            body["output_format"] = json!({"type":"json_schema","name":name,"schema":schema,"strict":strict});
        }
    }
    body
}

fn gemini_request(request: &CanonicalModelRequest) -> Value {
    let contents: Vec<Value> = request
        .messages
        .iter()
        .filter(|message| message.role != ModelRole::System)
        .map(|message| {
            let mut parts = vec![json!({"text":message.content})];
            for call in &message.tool_calls {
                parts.push(json!({"functionCall":{"name":call.name,"args":call.arguments}}));
            }
            json!({
                "role": if message.role == ModelRole::Assistant {"model"} else {"user"},
                "parts":parts
            })
        })
        .collect();
    let mut body = json!({
        "contents":contents,
        "generationConfig":{
            "temperature":request.temperature_milli.map(|value| f64::from(value) / 1_000.0),
            "maxOutputTokens":request.max_output_tokens
        }
    });
    let system: Vec<_> = request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::System)
        .map(|message| json!({"text":message.content}))
        .collect();
    if !system.is_empty() {
        body["systemInstruction"] = json!({"parts":system});
    }
    if !request.tools.is_empty() {
        body["tools"] = json!([{
            "functionDeclarations":request.tools.iter().map(|tool|json!({
                "name":tool.name,
                "description":tool.description,
                "parameters":tool.input_schema
            })).collect::<Vec<_>>()
        }]);
    }
    match &request.response_format {
        ResponseFormat::Text => {}
        ResponseFormat::JsonObject => {
            body["generationConfig"]["responseMimeType"] = Value::String("application/json".to_owned());
        }
        ResponseFormat::JsonSchema { schema, .. } => {
            body["generationConfig"]["responseMimeType"] = Value::String("application/json".to_owned());
            body["generationConfig"]["responseJsonSchema"] = schema.clone();
        }
    }
    body
}

fn bedrock_request(request: &CanonicalModelRequest) -> Value {
    let messages: Vec<Value> = request
        .messages
        .iter()
        .filter(|message| message.role != ModelRole::System)
        .map(|message| {
            let mut content = vec![json!({"text":message.content})];
            for call in &message.tool_calls {
                content.push(json!({"toolUse":{
                    "toolUseId":call.id,
                    "name":call.name,
                    "input":call.arguments
                }}));
            }
            json!({"role":role(message.role),"content":content})
        })
        .collect();
    let mut body = json!({
        "messages":messages,
        "inferenceConfig":{
            "maxTokens":request.max_output_tokens,
            "temperature":request.temperature_milli.map(|value| f64::from(value) / 1_000.0)
        }
    });
    let system: Vec<_> = request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::System)
        .map(|message| json!({"text":message.content}))
        .collect();
    if !system.is_empty() {
        body["system"] = Value::Array(system);
    }
    if !request.tools.is_empty() {
        body["toolConfig"] = json!({
            "tools":request.tools.iter().map(|tool|json!({"toolSpec":{
                "name":tool.name,
                "description":tool.description,
                "inputSchema":{"json":tool.input_schema}
            }})).collect::<Vec<_>>(),
            "toolChoice":match &request.tool_choice {
                ToolChoice::Auto => json!({"auto":{}}),
                ToolChoice::None => Value::Null,
                ToolChoice::Required => json!({"any":{}}),
                ToolChoice::Specific{name} => json!({"tool":{"name":name}}),
            }
        });
    }
    match &request.response_format {
        ResponseFormat::Text => {}
        ResponseFormat::JsonObject => {
            body["outputConfig"] = json!({"format":{"type":"json_object"}});
        }
        ResponseFormat::JsonSchema { name, schema, strict } => {
            body["outputConfig"] = json!({"format":{"type":"json_schema","name":name,"schema":schema,"strict":strict}});
        }
    }
    body
}

fn ensure_response_bound(response: &TransportResponse, max_bytes: usize) -> Result<(), ProviderError> {
    let bytes = serde_json::to_vec(&response.body).map_err(|_| protocol_error())?;
    if bytes.len() > max_bytes {
        Err(ProviderError::new(
            ProviderErrorCode::OutputTooLarge,
            "provider response exceeded configured bounds",
        ))
    } else {
        Ok(())
    }
}

fn parse_openai_response(profile: &ProviderProfile, body: Value) -> Result<CanonicalModelResponse, ProviderError> {
    let choice = body
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(protocol_error)?;
    let message = choice.get("message").ok_or_else(protocol_error)?;
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let tool_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .map(|call| {
                    let function = call.get("function").ok_or_else(protocol_error)?;
                    let arguments = function
                        .get("arguments")
                        .and_then(Value::as_str)
                        .map(|arguments| {
                            serde_json::from_str(arguments).unwrap_or_else(|_| Value::String(arguments.to_owned()))
                        })
                        .unwrap_or(Value::Null);
                    Ok(ModelToolCall {
                        id: string_field(call, "id")?,
                        name: string_field(function, "name")?,
                        arguments,
                    })
                })
                .collect::<Result<Vec<_>, ProviderError>>()
        })
        .transpose()?
        .unwrap_or_default();
    let usage = parse_openai_usage(&body);
    Ok(CanonicalModelResponse {
        schema: SchemaVersion::new("rocketmq-sre.model-response", 1, 0),
        provider: profile.id.clone(),
        model: body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(&profile.model)
            .to_owned(),
        content,
        parts: Vec::new(),
        reasoning_content: profile
            .preserve_reasoning_content
            .then(|| {
                message
                    .get("reasoning_content")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .flatten(),
        tool_calls,
        finish_reason: openai_finish_reason(choice.get("finish_reason").and_then(Value::as_str)),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        usage,
        provider_request_id: body.get("id").and_then(Value::as_str).map(ToOwned::to_owned),
    })
}

fn parse_deepseek_responses_response(
    profile: &ProviderProfile,
    body: Value,
) -> Result<CanonicalModelResponse, ProviderError> {
    let status = body.get("status").and_then(Value::as_str).ok_or_else(protocol_error)?;
    if status == "failed" {
        return Err(ProviderError::service_unavailable(
            "DeepSeek Responses API reported a failed response",
        ));
    }
    if !matches!(status, "completed" | "incomplete") {
        return Err(protocol_error());
    }
    let output = body
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(protocol_error)?;
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut tool_calls = Vec::new();
    let mut refusal = false;
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                let parts = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(protocol_error)?;
                for part in parts {
                    match part.get("type").and_then(Value::as_str) {
                        Some("output_text") => {
                            if let Some(text) = part.get("text").and_then(Value::as_str) {
                                content.push_str(text);
                            }
                        }
                        Some("refusal") => refusal = true,
                        _ => {}
                    }
                }
            }
            Some("reasoning") if profile.preserve_reasoning_content => {
                if let Some(parts) = item.get("content").and_then(Value::as_array) {
                    for part in parts {
                        if let Some(text) = part.get("text").and_then(Value::as_str) {
                            reasoning.push_str(text);
                        }
                    }
                }
            }
            Some("function_call") => {
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .map(|value| serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned())))
                    .unwrap_or(Value::Null);
                tool_calls.push(ModelToolCall {
                    id: string_field(item, "call_id")?,
                    name: string_field(item, "name")?,
                    arguments,
                });
            }
            _ => {}
        }
    }
    let finish_reason = if refusal {
        FinishReason::Safety
    } else if status == "incomplete" {
        FinishReason::Length
    } else if tool_calls.is_empty() {
        FinishReason::Stop
    } else {
        FinishReason::ToolCalls
    };
    let usage = parse_responses_usage(&body);
    Ok(CanonicalModelResponse {
        schema: SchemaVersion::new("rocketmq-sre.model-response", 1, 0),
        provider: profile.id.clone(),
        model: body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(&profile.model)
            .to_owned(),
        content,
        parts: Vec::new(),
        reasoning_content: (!reasoning.is_empty()).then_some(reasoning),
        tool_calls,
        finish_reason,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        usage,
        provider_request_id: body.get("id").and_then(Value::as_str).map(ToOwned::to_owned),
    })
}

pub(crate) fn parse_responses_usage(body: &Value) -> ModelUsage {
    let usage = body.get("usage").unwrap_or(&Value::Null);
    ModelUsage {
        input_tokens: u32_field(usage, "input_tokens"),
        output_tokens: u32_field(usage, "output_tokens"),
        total_tokens: u32_field(usage, "total_tokens"),
        reasoning_tokens: usage
            .get("output_tokens_details")
            .and_then(|details| u32_field(details, "reasoning_tokens")),
        cached_input_tokens: usage
            .get("input_tokens_details")
            .and_then(|details| u32_field(details, "cached_tokens")),
    }
}

fn parse_openai_usage(body: &Value) -> ModelUsage {
    let usage = body.get("usage").unwrap_or(&Value::Null);
    ModelUsage {
        input_tokens: u32_field(usage, "prompt_tokens"),
        output_tokens: u32_field(usage, "completion_tokens"),
        total_tokens: u32_field(usage, "total_tokens"),
        reasoning_tokens: usage
            .get("completion_tokens_details")
            .and_then(|details| u32_field(details, "reasoning_tokens")),
        cached_input_tokens: usage
            .get("prompt_tokens_details")
            .and_then(|details| u32_field(details, "cached_tokens")),
    }
}

fn parse_anthropic_response(profile: &ProviderProfile, body: Value) -> Result<CanonicalModelResponse, ProviderError> {
    let parts = body
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(protocol_error)?;
    let mut content = String::new();
    let mut reasoning = None;
    let mut tool_calls = Vec::new();
    for part in parts {
        match part.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    content.push_str(text);
                }
            }
            Some("thinking") => {
                reasoning = part.get("thinking").and_then(Value::as_str).map(ToOwned::to_owned);
            }
            Some("tool_use") => {
                tool_calls.push(ModelToolCall {
                    id: string_field(part, "id")?,
                    name: string_field(part, "name")?,
                    arguments: part.get("input").cloned().unwrap_or(Value::Null),
                });
            }
            _ => {}
        }
    }
    let usage_value = body.get("usage").unwrap_or(&Value::Null);
    let usage = ModelUsage {
        input_tokens: u32_field(usage_value, "input_tokens"),
        output_tokens: u32_field(usage_value, "output_tokens"),
        total_tokens: None,
        reasoning_tokens: None,
        cached_input_tokens: u32_field(usage_value, "cache_read_input_tokens"),
    };
    Ok(CanonicalModelResponse {
        schema: SchemaVersion::new("rocketmq-sre.model-response", 1, 0),
        provider: profile.id.clone(),
        model: body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(&profile.model)
            .to_owned(),
        content,
        parts: Vec::new(),
        reasoning_content: profile.preserve_reasoning_content.then_some(reasoning).flatten(),
        tool_calls,
        finish_reason: anthropic_finish_reason(body.get("stop_reason").and_then(Value::as_str)),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        usage,
        provider_request_id: body.get("id").and_then(Value::as_str).map(ToOwned::to_owned),
    })
}

fn parse_gemini_response(profile: &ProviderProfile, body: Value) -> Result<CanonicalModelResponse, ProviderError> {
    let candidate = body
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first())
        .ok_or_else(protocol_error)?;
    let parts = candidate
        .get("content")
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
        .ok_or_else(protocol_error)?;
    let mut content = String::new();
    let mut tool_calls = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            content.push_str(text);
        }
        if let Some(call) = part.get("functionCall") {
            tool_calls.push(ModelToolCall {
                id: format!("gemini-call-{index}"),
                name: string_field(call, "name")?,
                arguments: call.get("args").cloned().unwrap_or(Value::Null),
            });
        }
    }
    let usage_value = body.get("usageMetadata").unwrap_or(&Value::Null);
    let usage = ModelUsage {
        input_tokens: u32_field(usage_value, "promptTokenCount"),
        output_tokens: u32_field(usage_value, "candidatesTokenCount"),
        total_tokens: u32_field(usage_value, "totalTokenCount"),
        reasoning_tokens: u32_field(usage_value, "thoughtsTokenCount"),
        cached_input_tokens: u32_field(usage_value, "cachedContentTokenCount"),
    };
    Ok(CanonicalModelResponse {
        schema: SchemaVersion::new("rocketmq-sre.model-response", 1, 0),
        provider: profile.id.clone(),
        model: body
            .get("modelVersion")
            .and_then(Value::as_str)
            .unwrap_or(&profile.model)
            .to_owned(),
        content,
        parts: Vec::new(),
        reasoning_content: None,
        tool_calls,
        finish_reason: gemini_finish_reason(candidate.get("finishReason").and_then(Value::as_str)),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        usage,
        provider_request_id: body.get("responseId").and_then(Value::as_str).map(ToOwned::to_owned),
    })
}

fn parse_bedrock_response(profile: &ProviderProfile, body: Value) -> Result<CanonicalModelResponse, ProviderError> {
    let parts = body
        .get("output")
        .and_then(|output| output.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
        .ok_or_else(protocol_error)?;
    let mut content = String::new();
    let mut reasoning = None;
    let mut tool_calls = Vec::new();
    for part in parts {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            content.push_str(text);
        }
        if let Some(reasoning_content) = part
            .get("reasoningContent")
            .and_then(|value| value.get("reasoningText"))
            .and_then(|value| value.get("text"))
            .and_then(Value::as_str)
        {
            reasoning = Some(reasoning_content.to_owned());
        }
        if let Some(call) = part.get("toolUse") {
            tool_calls.push(ModelToolCall {
                id: string_field(call, "toolUseId")?,
                name: string_field(call, "name")?,
                arguments: call.get("input").cloned().unwrap_or(Value::Null),
            });
        }
    }
    let usage_value = body.get("usage").unwrap_or(&Value::Null);
    let usage = ModelUsage {
        input_tokens: u32_field(usage_value, "inputTokens"),
        output_tokens: u32_field(usage_value, "outputTokens"),
        total_tokens: u32_field(usage_value, "totalTokens"),
        reasoning_tokens: None,
        cached_input_tokens: u32_field(usage_value, "cacheReadInputTokens"),
    };
    Ok(CanonicalModelResponse {
        schema: SchemaVersion::new("rocketmq-sre.model-response", 1, 0),
        provider: profile.id.clone(),
        model: profile.model.clone(),
        content,
        parts: Vec::new(),
        reasoning_content: reasoning,
        tool_calls,
        finish_reason: bedrock_finish_reason(body.get("stopReason").and_then(Value::as_str)),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        usage,
        provider_request_id: body
            .get("ResponseMetadata")
            .and_then(|metadata| metadata.get("RequestId"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn openai_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("stop") => FinishReason::Stop,
        Some("length") => FinishReason::Length,
        Some("tool_calls" | "function_call") => FinishReason::ToolCalls,
        Some("content_filter") => FinishReason::ContentFilter,
        _ => FinishReason::Unknown,
    }
}

fn anthropic_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("end_turn" | "stop_sequence") => FinishReason::Stop,
        Some("max_tokens") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolCalls,
        Some("refusal") => FinishReason::Safety,
        _ => FinishReason::Unknown,
    }
}

fn gemini_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("STOP") => FinishReason::Stop,
        Some("MAX_TOKENS") => FinishReason::Length,
        Some("SAFETY" | "BLOCKLIST" | "PROHIBITED_CONTENT") => FinishReason::Safety,
        _ => FinishReason::Unknown,
    }
}

fn bedrock_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("end_turn" | "stop_sequence") => FinishReason::Stop,
        Some("max_tokens") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolCalls,
        Some("content_filtered" | "guardrail_intervened") => FinishReason::Safety,
        _ => FinishReason::Unknown,
    }
}

fn string_field(value: &Value, field: &str) -> Result<String, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(protocol_error)
}

fn u32_field(value: &Value, field: &str) -> Option<u32> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn protocol_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::ProtocolError,
        "provider response did not match its declared protocol",
    )
}
