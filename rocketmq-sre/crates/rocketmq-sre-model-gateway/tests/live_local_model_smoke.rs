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
use std::time::Duration;

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_model_gateway::AsyncBuiltinProviderClient;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::HttpModelTransport;
use rocketmq_sre_model_gateway::HttpTransportConfig;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ProviderHealth;
use rocketmq_sre_model_gateway::ToolChoice;
use rocketmq_sre_model_gateway::builtin_provider_profiles;
use rocketmq_sre_model_gateway::current_unix_ms;
use serde_json::json;
use url::Url;

const ENDPOINT_ENV: &str = "ROCKETMQ_SRE_LOCAL_MODEL_QUALIFICATION_ENDPOINT";
const MODEL_ENV: &str = "ROCKETMQ_SRE_LOCAL_MODEL_QUALIFICATION_MODEL";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CONTENT_BYTES: usize = 4 * 1024;
const REQUEST_TIMEOUT_SECONDS: u64 = 120;

/// Credential-free live smoke for a qualification-owned loopback Ollama
/// endpoint. The test is ignored by default and never prints the prompt,
/// response, endpoint, or any credential material.
#[tokio::test]
#[ignore = "requires an explicitly configured qualification-owned loopback Ollama endpoint"]
async fn live_ollama_openai_compatible_endpoint_when_explicitly_configured() {
    let endpoint = std::env::var(ENDPOINT_ENV).expect("local qualification endpoint must be configured");
    let parsed = Url::parse(&endpoint).expect("local qualification endpoint must be a URL");
    assert_eq!(
        parsed.scheme(),
        "http",
        "local qualification endpoint must use loopback HTTP"
    );
    assert_eq!(
        parsed.host_str(),
        Some("127.0.0.1"),
        "local qualification endpoint must use 127.0.0.1"
    );
    assert!(
        parsed.port().is_some(),
        "local qualification endpoint must use an explicit random port"
    );
    assert_eq!(
        parsed.path(),
        "/v1",
        "local qualification endpoint must expose the OpenAI-compatible /v1 root"
    );
    assert!(parsed.query().is_none() && parsed.fragment().is_none());

    let model = std::env::var(MODEL_ENV).expect("local qualification model must be configured");
    assert_eq!(
        model, "qwen2.5:0.5b",
        "qualification model must remain bounded and pinned"
    );
    let mut profile = builtin_provider_profiles()
        .into_iter()
        .find(|candidate| candidate.id == "ollama")
        .expect("built-in Ollama profile");
    profile.endpoint = endpoint;
    profile.model = model.clone();
    profile.model_revision = "qualification-qwen2.5-0.5b".to_owned();
    profile.endpoint_instance = "qualification-loopback".to_owned();
    profile.health = ProviderHealth::Healthy;
    profile.credential_ref = None;
    profile.validate().expect("qualification Ollama profile");

    let transport = Arc::new(
        HttpModelTransport::new(
            HttpTransportConfig::default()
                .with_timeouts(Duration::from_secs(5), Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
                .with_body_limits(32 * 1024, MAX_RESPONSE_BYTES),
        )
        .expect("local model HTTP transport"),
    );
    let client = AsyncBuiltinProviderClient::new(profile, transport).expect("local model client");
    let correlation_id = CorrelationId::new();
    let mut request = CanonicalModelRequest::new(
        correlation_id,
        model,
        vec![ModelMessage::text(
            ModelRole::User,
            "Return the single word OK. Do not include analysis or sensitive data.",
        )],
    );
    request.tool_choice = ToolChoice::None;
    request.max_output_tokens = Some(32);
    request.temperature_milli = Some(0);
    let mut context = InvocationContext::new(correlation_id);
    context.deadline_unix_ms = Some(current_unix_ms().saturating_add(REQUEST_TIMEOUT_SECONDS * 1_000));
    context.max_response_bytes = MAX_RESPONSE_BYTES;

    let response = client
        .invoke(&context, &request, None)
        .await
        .expect("qualification-owned local model call");
    let response_bytes = response.content.len();
    assert!(response_bytes > 0, "local model returned empty content");
    assert!(
        response_bytes <= MAX_CONTENT_BYTES,
        "local model content exceeded the qualification bound"
    );
    assert!(
        response.tool_calls.is_empty(),
        "local model qualification must not select tools"
    );
    assert_eq!(response.provider, "ollama");
    assert!(!response.model.trim().is_empty());

    let marker = json!({
        "provider": response.provider,
        "model_family": "local",
        "response_non_empty": true,
        "response_bytes": response_bytes,
        "tool_calls": response.tool_calls.len(),
        "credential_present": false,
        "input_tokens": response.usage.input_tokens.or(response.input_tokens),
        "output_tokens": response.usage.output_tokens.or(response.output_tokens)
    });
    println!("LOCAL_MODEL_QUALIFICATION_OK {marker}");
}
