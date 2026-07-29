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

//! Network-isolated OpenAI-compatible provider fixture for Phase 01 live
//! acceptance. It has no credentials, outbound client, or RocketMQ access. In
//! addition to a bounded diagnosis tied to supplied Evidence IDs, it can emit
//! the exact synthetic read-only tool call used by the Provider smoke check;
//! it never executes that tool or accepts an arbitrary tool surface.

use std::net::SocketAddr;

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_sre_contracts::EvidenceId;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use tracing_subscriber::EnvFilter;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8094";
const MAX_REQUEST_BYTES: usize = 256 * 1024;
const MAX_MESSAGES: usize = 8;
const MAX_PROMPT_CHARS: usize = 128 * 1024;
const PROVIDER_SMOKE_EVIDENCE_ID: &str = "provider-smoke-evidence";
const PROVIDER_SMOKE_RESOURCE: &str = "provider-smoke-resource";
const PROVIDER_SMOKE_TOOL: &str = "read_smoke_evidence";

#[derive(Debug, thiserror::Error)]
enum MockProviderError {
    #[error("invalid Phase 01 model mock configuration")]
    Configuration,
    #[error("Phase 01 model mock listener failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Deserialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    tools: Option<Value>,
    #[serde(default)]
    tool_choice: Option<Value>,
    #[serde(default)]
    response_format: Option<Value>,
}

#[derive(Deserialize)]
struct ChatMessage {
    role: String,
    content: Value,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("phase01_model_mock=info")),
        )
        .json()
        .try_init()?;

    let bind_addr = std::env::var("ROCKETMQ_SRE_MODEL_MOCK_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_owned())
        .parse()
        .map_err(|_| MockProviderError::Configuration)?;
    let runtime_owner = RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-sre-phase01-model-mock"))?;
    let service_result = runtime_owner.block_on(run(bind_addr));
    let shutdown_result = runtime_owner.shutdown_runtime_blocking();
    service_result?;
    shutdown_result?;
    Ok(())
}

async fn run(bind_addr: SocketAddr) -> Result<(), MockProviderError> {
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/v1/chat/completions", post(chat_completion))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES));
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(
        %bind_addr,
        outbound_network = false,
        mutation_supported = false,
        "Phase 01 model mock is ready"
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            if let Err(error) = wait_for_signal_result().await {
                tracing::warn!(
                    error = %error,
                    "Phase 01 model mock shutdown signal watcher failed"
                );
            }
        })
        .await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "ready",
        "outbound_network": false,
        "mutation_supported": false
    }))
}

async fn chat_completion(Json(request): Json<ChatCompletionRequest>) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    build_completion(&request).map(Json).map_err(|code| {
        (
            code,
            Json(json!({
                "error": {
                    "code": "invalid_phase01_fixture_request",
                    "message": "request did not satisfy the bounded read-only fixture contract"
                }
            })),
        )
    })
}

fn build_completion(request: &ChatCompletionRequest) -> Result<Value, StatusCode> {
    if request.model.trim().is_empty()
        || request.model.chars().count() > 200
        || request.messages.is_empty()
        || request.messages.len() > MAX_MESSAGES
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let prompt = request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "user")
        .and_then(|message| message.content.as_str())
        .filter(|content| content.chars().count() <= MAX_PROMPT_CHARS)
        .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;

    if provider_connectivity_probe(request, prompt) {
        return completion(request, Value::String("OK".to_owned()), None, "stop");
    }
    if provider_structured_probe(request, prompt) {
        return completion(
            request,
            Value::String(
                json!({
                    "status": "ok",
                    "evidence_id": PROVIDER_SMOKE_EVIDENCE_ID
                })
                .to_string(),
            ),
            None,
            "stop",
        );
    }
    if provider_tool_probe(request, prompt) {
        return completion(
            request,
            Value::Null,
            Some(json!([{
                "id": "call-phase01-provider-smoke",
                "type": "function",
                "function": {
                    "name": PROVIDER_SMOKE_TOOL,
                    "arguments": json!({"resource": PROVIDER_SMOKE_RESOURCE}).to_string()
                }
            }])),
            "tool_calls",
        );
    }
    if request.tools.as_ref().is_some_and(non_empty_json)
        || request
            .tool_choice
            .as_ref()
            .is_some_and(|choice| choice != "none" && !choice.is_null())
        || request.response_format.is_none()
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    let prompt: Value = serde_json::from_str(prompt).map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    let evidence_ids = allowed_evidence_ids(&prompt);
    let evidence_id = evidence_ids.first().ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;
    let diagnosis = json!({
        "summary": "The cited read-only evidence confirms positive consumer lag.",
        "assessment": "The consumer is not keeping pace with the bounded synthetic workload. No cluster mutation was requested.",
        "confidence_percent": 91,
        "cited_evidence_ids": [evidence_id],
        "recommended_read_only_queries": [
            "Re-check consumer lag and broker runtime through RocketMQ MCP."
        ],
        "rationale": "The conclusion is limited to the supplied canonical Evidence citation."
    });
    let content = serde_json::to_string(&diagnosis).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    completion(request, Value::String(content), None, "stop")
}

fn provider_connectivity_probe(request: &ChatCompletionRequest, prompt: &str) -> bool {
    prompt == "Provider health probe. Respond with the single word OK."
        && !request.tools.as_ref().is_some_and(non_empty_json)
        && request.response_format.is_none()
}

fn provider_structured_probe(request: &ChatCompletionRequest, prompt: &str) -> bool {
    prompt
        == format!(
            "Return the required JSON object and cite evidence_id {PROVIDER_SMOKE_EVIDENCE_ID}. Do not include other \
             fields."
        )
        && !request.tools.as_ref().is_some_and(non_empty_json)
        && request.response_format.as_ref().is_some_and(|format| {
            format.get("type").and_then(Value::as_str) == Some("json_schema")
                && format
                    .get("json_schema")
                    .and_then(|schema| schema.get("name"))
                    .and_then(Value::as_str)
                    == Some("provider_smoke")
        })
}

fn provider_tool_probe(request: &ChatCompletionRequest, prompt: &str) -> bool {
    if prompt != format!("Call {PROVIDER_SMOKE_TOOL} for resource {PROVIDER_SMOKE_RESOURCE}.") {
        return false;
    }
    let Some(tool) = request
        .tools
        .as_ref()
        .and_then(Value::as_array)
        .filter(|tools| tools.len() == 1)
        .and_then(|tools| tools.first())
    else {
        return false;
    };
    let function = tool.get("function");
    let resource = function
        .and_then(|value| value.get("parameters"))
        .and_then(|value| value.get("properties"))
        .and_then(|value| value.get("resource"))
        .and_then(|value| value.get("const"))
        .and_then(Value::as_str);
    let selected = request
        .tool_choice
        .as_ref()
        .and_then(|value| value.get("function"))
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str);
    tool.get("type").and_then(Value::as_str) == Some("function")
        && function.and_then(|value| value.get("name")).and_then(Value::as_str) == Some(PROVIDER_SMOKE_TOOL)
        && resource == Some(PROVIDER_SMOKE_RESOURCE)
        && selected == Some(PROVIDER_SMOKE_TOOL)
        && request.response_format.is_none()
}

fn completion(
    request: &ChatCompletionRequest,
    content: Value,
    tool_calls: Option<Value>,
    finish_reason: &str,
) -> Result<Value, StatusCode> {
    let prompt_bytes = request
        .messages
        .iter()
        .map(|message| message.content.to_string().len())
        .sum();
    let completion_bytes = content.to_string().len() + tool_calls.as_ref().map_or(0, |value| value.to_string().len());
    let prompt_tokens = token_estimate(prompt_bytes);
    let completion_tokens = token_estimate(completion_bytes);
    let mut message = json!({
        "role": "assistant",
        "content": content
    });
    if let Some(tool_calls) = tool_calls {
        message["tool_calls"] = tool_calls;
    }
    Ok(json!({
        "id": "chatcmpl-phase01-read-only-fixture",
        "object": "chat.completion",
        "model": request.model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens.saturating_add(completion_tokens)
        }
    }))
}

fn allowed_evidence_ids(prompt: &Value) -> Vec<String> {
    let candidates = prompt
        .get("evidence")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("evidence_id"))
                .collect::<Vec<_>>()
        })
        .or_else(|| {
            prompt
                .get("allowed_evidence_ids")
                .and_then(Value::as_array)
                .map(|items| items.iter().collect())
        })
        .unwrap_or_default();
    candidates
        .into_iter()
        .filter_map(Value::as_str)
        .filter_map(|value| value.parse::<EvidenceId>().ok())
        .map(|value| value.to_string())
        .take(32)
        .collect()
}

fn non_empty_json(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Array(items) => !items.is_empty(),
        Value::Object(items) => !items.is_empty(),
        _ => true,
    }
}

fn token_estimate(bytes: usize) -> u32 {
    u32::try_from(bytes.saturating_add(3) / 4).unwrap_or(u32::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVIDENCE_ID: &str = "00000000-0000-4000-8000-000000000042";

    #[test]
    fn fixture_cites_only_supplied_evidence_and_has_no_tool_surface() {
        let request = ChatCompletionRequest {
            model: "phase01-mock".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: Value::String(
                    json!({
                        "schema_version": "rocketmq-sre.model-diagnosis-input.v1",
                        "evidence": [{"evidence_id": EVIDENCE_ID}]
                    })
                    .to_string(),
                ),
            }],
            tools: None,
            tool_choice: Some(Value::String("none".to_owned())),
            response_format: Some(json!({"type": "json_schema"})),
        };

        let response = build_completion(&request).expect("bounded completion");
        let content = response["choices"][0]["message"]["content"]
            .as_str()
            .expect("completion content");
        let diagnosis: Value = serde_json::from_str(content).expect("structured diagnosis");
        assert_eq!(diagnosis["cited_evidence_ids"], json!([EVIDENCE_ID]));
        assert_eq!(diagnosis["recommended_read_only_queries"].as_array().unwrap().len(), 1);
        assert!(response.get("tools").is_none());
    }

    #[test]
    fn fixture_rejects_unknown_or_missing_evidence() {
        for prompt in [
            json!({"evidence": []}),
            json!({"evidence": [{"evidence_id": "not-a-uuid"}]}),
        ] {
            let request = ChatCompletionRequest {
                model: "phase01-mock".to_owned(),
                messages: vec![ChatMessage {
                    role: "user".to_owned(),
                    content: Value::String(prompt.to_string()),
                }],
                tools: None,
                tool_choice: None,
                response_format: Some(json!({"type": "json_schema"})),
            };
            assert_eq!(
                build_completion(&request).expect_err("invalid evidence"),
                StatusCode::UNPROCESSABLE_ENTITY
            );
        }
    }

    #[test]
    fn fixture_rejects_any_model_tool_surface() {
        let request = ChatCompletionRequest {
            model: "phase01-mock".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: Value::String(json!({"evidence": [{"evidence_id": EVIDENCE_ID}]}).to_string()),
            }],
            tools: Some(json!([{"type": "function"}])),
            tool_choice: None,
            response_format: Some(json!({"type": "json_schema"})),
        };

        assert_eq!(
            build_completion(&request).expect_err("tool surface"),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn fixture_supports_only_the_exact_bounded_provider_smoke_contract() {
        let connectivity = ChatCompletionRequest {
            model: "phase01-mock".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: Value::String("Provider health probe. Respond with the single word OK.".to_owned()),
            }],
            tools: None,
            tool_choice: None,
            response_format: None,
        };
        let response = build_completion(&connectivity).expect("connectivity smoke");
        assert_eq!(response["choices"][0]["message"]["content"], "OK");

        let structured = ChatCompletionRequest {
            model: "phase01-mock".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: Value::String(format!(
                    "Return the required JSON object and cite evidence_id {PROVIDER_SMOKE_EVIDENCE_ID}. Do not \
                     include other fields."
                )),
            }],
            tools: None,
            tool_choice: None,
            response_format: Some(json!({
                "type": "json_schema",
                "json_schema": {"name": "provider_smoke"}
            })),
        };
        let response = build_completion(&structured).expect("structured smoke");
        let content = response["choices"][0]["message"]["content"]
            .as_str()
            .expect("structured content");
        assert_eq!(
            serde_json::from_str::<Value>(content).expect("structured JSON"),
            json!({"status": "ok", "evidence_id": PROVIDER_SMOKE_EVIDENCE_ID})
        );

        let tool = ChatCompletionRequest {
            model: "phase01-mock".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: Value::String(format!(
                    "Call {PROVIDER_SMOKE_TOOL} for resource {PROVIDER_SMOKE_RESOURCE}."
                )),
            }],
            tools: Some(json!([{
                "type": "function",
                "function": {
                    "name": PROVIDER_SMOKE_TOOL,
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "resource": {"type": "string", "const": PROVIDER_SMOKE_RESOURCE}
                        },
                        "required": ["resource"],
                        "additionalProperties": false
                    }
                }
            }])),
            tool_choice: Some(json!({
                "type": "function",
                "function": {"name": PROVIDER_SMOKE_TOOL}
            })),
            response_format: None,
        };
        let response = build_completion(&tool).expect("read-only tool smoke");
        assert_eq!(response["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            response["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            PROVIDER_SMOKE_TOOL
        );

        let mut arbitrary = tool;
        arbitrary.tools = Some(json!([{
            "type": "function",
            "function": {
                "name": "apply_cluster_change",
                "parameters": {"type": "object"}
            }
        }]));
        assert_eq!(
            build_completion(&arbitrary).expect_err("arbitrary tool must fail closed"),
            StatusCode::BAD_REQUEST
        );
    }
}
