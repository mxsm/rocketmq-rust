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

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use rocketmq_sre_model_gateway::ChatModelProvider;
use rocketmq_sre_model_gateway::CredentialOwner;
use rocketmq_sre_model_gateway::DataClass;
use rocketmq_sre_model_gateway::FinishReason;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::InvocationMetadata;
use rocketmq_sre_model_gateway::InvocationPurpose;
use rocketmq_sre_model_gateway::ModelInvocationOutcome;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ModelTool;
use rocketmq_sre_model_gateway::ModelTransport;
use rocketmq_sre_model_gateway::ProviderCapabilities;
use rocketmq_sre_model_gateway::ProviderCapability;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::ProviderHealth;
use rocketmq_sre_model_gateway::ProviderProfile;
use rocketmq_sre_model_gateway::ProviderProfileManifest;
use rocketmq_sre_model_gateway::ProviderRegistry;
use rocketmq_sre_model_gateway::ProviderRouter;
use rocketmq_sre_model_gateway::ProviderSpi;
use rocketmq_sre_model_gateway::ProviderSpiClient;
use rocketmq_sre_model_gateway::ResponseFormat;
use rocketmq_sre_model_gateway::RoutingPolicy;
use rocketmq_sre_model_gateway::RoutingRequirements;
use rocketmq_sre_model_gateway::SecretMaterial;
use rocketmq_sre_model_gateway::SecretProvider;
use rocketmq_sre_model_gateway::SecretReference;
use rocketmq_sre_model_gateway::SpiCancelRequest;
use rocketmq_sre_model_gateway::SpiClientConfig;
use rocketmq_sre_model_gateway::SpiHandshakeRequest;
use rocketmq_sre_model_gateway::SpiHandshakeResponse;
use rocketmq_sre_model_gateway::SpiHealth;
use rocketmq_sre_model_gateway::SpiInvokeRequest;
use rocketmq_sre_model_gateway::SpiStreamRequest;
use rocketmq_sre_model_gateway::ToolChoice;
use rocketmq_sre_model_gateway::TransportRequest;
use rocketmq_sre_model_gateway::TransportResponse;
use rocketmq_sre_model_gateway::adapter_for_profile;
use rocketmq_sre_model_gateway::builtin_provider_profiles;
use serde_json::Value;

#[derive(Default)]
struct TestSecrets;

impl SecretProvider for TestSecrets {
    fn resolve(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError> {
        Ok(SecretMaterial::new(
            format!("credential-for-{:?}", reference.kind()),
            "version:test-v1",
            None,
        ))
    }

    fn refresh(&self, reference: &SecretReference) -> Result<SecretMaterial, ProviderError> {
        self.resolve(reference)
    }
}

struct MockTransport {
    responses: Mutex<VecDeque<Result<TransportResponse, ProviderError>>>,
    requests: Mutex<Vec<TransportRequest>>,
}

impl MockTransport {
    fn returning(responses: Vec<Result<TransportResponse, ProviderError>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn last_body(&self) -> serde_json::Value {
        self.requests
            .lock()
            .expect("request lock")
            .last()
            .expect("transport request")
            .body
            .clone()
    }

    fn last_request(&self) -> TransportRequest {
        self.requests
            .lock()
            .expect("request lock")
            .last()
            .expect("transport request")
            .clone()
    }
}

impl ModelTransport for MockTransport {
    fn invoke(&self, request: TransportRequest) -> Result<TransportResponse, ProviderError> {
        self.requests.lock().expect("request lock").push(request);
        self.responses
            .lock()
            .expect("response lock")
            .pop_front()
            .expect("scripted transport response")
    }
}

fn fixture(file: &str, case: &str) -> TransportResponse {
    let raw = match file {
        "openai-compatible.contract.json" => {
            include_str!("fixtures/providers/openai-compatible.contract.json")
        }
        "anthropic-messages.contract.json" => {
            include_str!("fixtures/providers/anthropic-messages.contract.json")
        }
        "gemini-native.contract.json" => {
            include_str!("fixtures/providers/gemini-native.contract.json")
        }
        "bedrock-converse.contract.json" => {
            include_str!("fixtures/providers/bedrock-converse.contract.json")
        }
        "deepseek-responses.contract.json" => {
            include_str!("fixtures/providers/deepseek-responses.contract.json")
        }
        unexpected => panic!("unknown fixture: {unexpected}"),
    };
    let value: serde_json::Value = serde_json::from_str(raw).expect("fixture JSON");
    TransportResponse {
        status: value[case]["status"].as_u64().expect("fixture status") as u16,
        body: value[case]["body"].clone(),
    }
}

fn profile(id: &str) -> ProviderProfile {
    builtin_provider_profiles()
        .into_iter()
        .find(|candidate| candidate.id == id)
        .unwrap_or_else(|| panic!("missing built-in profile: {id}"))
}

fn text_request() -> CanonicalModelRequest {
    CanonicalModelRequest::new(
        CorrelationId::new(),
        "test-model",
        vec![ModelMessage::text(ModelRole::User, "is the broker healthy?")],
    )
}

#[test]
fn every_protocol_maps_text_json_tool_and_retryable_errors() {
    let protocols = [
        ("openai", "openai-compatible.contract.json"),
        ("anthropic", "anthropic-messages.contract.json"),
        ("google-gemini", "gemini-native.contract.json"),
        ("aws-bedrock", "bedrock-converse.contract.json"),
        ("deepseek-responses", "deepseek-responses.contract.json"),
    ];

    for (profile_id, fixture_file) in protocols {
        let transport = Arc::new(MockTransport::returning(vec![
            Ok(fixture(fixture_file, "text")),
            Ok(fixture(fixture_file, "json")),
            Ok(fixture(fixture_file, "tool")),
            Ok(fixture(fixture_file, "error")),
        ]));
        let provider = adapter_for_profile(profile(profile_id), transport, Arc::new(TestSecrets)).expect("adapter");
        let context = InvocationContext::new(CorrelationId::new());

        let text = provider.invoke(&context, &text_request()).expect("text");
        assert_eq!(text.content, "broker is healthy", "{profile_id}");
        assert_eq!(text.finish_reason, FinishReason::Stop, "{profile_id}");

        let mut json_request = text_request();
        json_request.response_format = ResponseFormat::JsonObject;
        let json = provider.invoke(&context, &json_request).expect("JSON");
        assert_eq!(json.content, r#"{"status":"healthy"}"#, "{profile_id}");

        let mut tool_request = text_request();
        tool_request.tools.push(ModelTool::read_only(
            "query_consumer_lag",
            "Read current lag",
            serde_json::json!({"type":"object"}),
        ));
        let tool = provider.invoke(&context, &tool_request).expect("tool");
        assert_eq!(tool.tool_calls[0].name, "query_consumer_lag", "{profile_id}");

        let error = provider.invoke(&context, &text_request()).expect_err("429 must fail");
        assert_eq!(error.code, ProviderErrorCode::RateLimited, "{profile_id}");
        assert!(error.retryable);
        assert!(error.fallback_allowed());
    }
}

#[test]
fn deepseek_responses_maps_instructions_structured_output_tools_and_usage() {
    let transport = Arc::new(MockTransport::returning(vec![Ok(fixture(
        "deepseek-responses.contract.json",
        "text",
    ))]));
    let provider = adapter_for_profile(profile("deepseek-responses"), transport.clone(), Arc::new(TestSecrets))
        .expect("DeepSeek Responses adapter");
    let mut request = CanonicalModelRequest::new(
        CorrelationId::new(),
        "deepseek-v4-flash",
        vec![
            ModelMessage::text(ModelRole::System, "Return only a bounded read-only diagnosis."),
            ModelMessage::text(ModelRole::User, "Assess the supplied evidence."),
        ],
    );
    request.max_output_tokens = Some(256);
    request.response_format = ResponseFormat::JsonSchema {
        name: "rocketmq_sre_diagnosis".to_owned(),
        schema: serde_json::json!({
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
            "additionalProperties": false
        }),
        strict: true,
    };
    request.tools.push(ModelTool::read_only(
        "query_consumer_lag",
        "Read current lag",
        serde_json::json!({"type":"object"}),
    ));
    request.tool_choice = ToolChoice::Auto;

    let response = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &request)
        .expect("DeepSeek Responses result");
    let sent = transport.last_request();

    assert_eq!(sent.path, "/responses");
    assert_eq!(sent.body["model"], "deepseek-v4-flash");
    assert_eq!(sent.body["instructions"], "Return only a bounded read-only diagnosis.");
    assert_eq!(sent.body["input"][0]["type"], "message");
    assert_eq!(sent.body["max_output_tokens"], 256);
    assert_eq!(sent.body["text"]["format"]["type"], "json_schema");
    assert_eq!(sent.body["text"]["format"]["name"], "rocketmq_sre_diagnosis");
    assert_eq!(sent.body["tools"][0]["name"], "query_consumer_lag");
    assert!(sent.body["tools"][0].get("strict").is_none());
    assert!(sent.body["tools"][0].get("function").is_none());
    assert_eq!(sent.body["tool_choice"], serde_json::json!("auto"));
    assert!(sent.body.get("messages").is_none());

    let mut strict_request = request;
    strict_request.tools[0] = strict_request.tools[0].clone().with_strict();
    let error = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &strict_request)
        .expect_err("standard DeepSeek Responses endpoint does not advertise strict tools");
    assert_eq!(error.code, ProviderErrorCode::CapabilityUnsupported);

    let mut specific_request = strict_request;
    specific_request.tools[0].strict = false;
    specific_request.tool_choice = ToolChoice::Specific {
        name: "query_consumer_lag".to_owned(),
    };
    let error = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &specific_request)
        .expect_err("qualified DeepSeek Responses endpoint does not advertise forced tool selection");
    assert_eq!(error.code, ProviderErrorCode::CapabilityUnsupported);
    assert!(sent.body.get("response_format").is_none());
    assert_eq!(response.model, "deepseek-v4-flash");
    assert_eq!(response.usage.input_tokens, Some(11));
    assert_eq!(response.usage.output_tokens, Some(8));
    assert_eq!(response.usage.reasoning_tokens, Some(4));
    assert_eq!(response.usage.cached_input_tokens, Some(3));
    assert!(response.reasoning_content.is_none());
}

#[test]
fn deepseek_preserves_reasoning_after_tool_calls() {
    let mut deepseek_response = fixture("openai-compatible.contract.json", "text");
    deepseek_response.body["choices"][0]["message"]["reasoning_content"] =
        Value::String("verified route freshness".to_owned());
    let transport = Arc::new(MockTransport::returning(vec![Ok(deepseek_response)]));
    let provider = adapter_for_profile(profile("deepseek"), transport.clone(), Arc::new(TestSecrets)).expect("adapter");
    let mut request = text_request();
    request.messages.insert(
        0,
        ModelMessage::text(ModelRole::Assistant, "")
            .with_reasoning("checked queue distribution")
            .with_tool_call(rocketmq_sre_model_gateway::ModelToolCall {
                id: "prior-call".to_owned(),
                name: "query_consumer_lag".to_owned(),
                arguments: serde_json::json!({"group":"synthetic-group"}),
            }),
    );

    let response = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &request)
        .expect("deepseek response");

    assert_eq!(
        transport.last_body()["messages"][0]["reasoning_content"],
        "checked queue distribution"
    );
    assert_eq!(response.reasoning_content.as_deref(), Some("verified route freshness"));

    let mut anthropic_response = fixture("anthropic-messages.contract.json", "text");
    anthropic_response.body["content"] = serde_json::json!([
        {"type":"thinking","thinking":"verified broker runtime"},
        {"type":"text","text":"broker is healthy"}
    ]);
    let anthropic_transport = Arc::new(MockTransport::returning(vec![Ok(anthropic_response)]));
    let anthropic_provider = adapter_for_profile(
        profile("deepseek-anthropic"),
        anthropic_transport.clone(),
        Arc::new(TestSecrets),
    )
    .expect("DeepSeek Anthropic adapter");
    let anthropic_result = anthropic_provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &request)
        .expect("DeepSeek Anthropic response");
    assert_eq!(
        anthropic_transport.last_body()["messages"][0]["content"][0]["thinking"],
        "checked queue distribution"
    );
    assert_eq!(
        anthropic_result.reasoning_content.as_deref(),
        Some("verified broker runtime")
    );
}

#[test]
fn zhipu_rejects_required_tool_choice_instead_of_simulating_it() {
    let transport = Arc::new(MockTransport::returning(Vec::new()));
    let provider = adapter_for_profile(profile("zhipu-glm"), transport, Arc::new(TestSecrets)).expect("adapter");
    let mut request = text_request();
    request.tools.push(ModelTool::read_only(
        "query_consumer_lag",
        "Read current lag",
        serde_json::json!({"type":"object"}),
    ));
    request.tool_choice = ToolChoice::Required;

    let error = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &request)
        .expect_err("unsupported tool choice must fail closed");

    assert_eq!(error.code, ProviderErrorCode::CapabilityUnsupported);
    assert!(!error.retryable);
    assert!(!error.fallback_allowed());

    let mut strict_request = text_request();
    strict_request.tools.push(
        ModelTool::read_only(
            "query_consumer_lag",
            "Read current lag",
            serde_json::json!({"type":"object"}),
        )
        .with_strict(),
    );
    let strict_error = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &strict_request)
        .expect_err("strict tools are a separate capability");
    assert_eq!(strict_error.code, ProviderErrorCode::CapabilityUnsupported);
}

#[test]
fn kimi_mfjs_requires_an_explicit_profile_flag() {
    let mut request = text_request();
    request.response_format = ResponseFormat::JsonObject;
    request.extensions.kimi_mfjs = true;

    let disabled = adapter_for_profile(
        profile("kimi-moonshot"),
        Arc::new(MockTransport::returning(Vec::new())),
        Arc::new(TestSecrets),
    )
    .expect("adapter");
    assert_eq!(
        disabled
            .invoke(&InvocationContext::new(CorrelationId::new()), &request)
            .expect_err("MFJS must be profile-gated")
            .code,
        ProviderErrorCode::CapabilityUnsupported
    );

    let transport = Arc::new(MockTransport::returning(vec![Ok(fixture(
        "openai-compatible.contract.json",
        "json",
    ))]));
    let enabled =
        adapter_for_profile(profile("kimi-moonshot-mfjs"), transport.clone(), Arc::new(TestSecrets)).expect("adapter");
    enabled
        .invoke(&InvocationContext::new(CorrelationId::new()), &request)
        .expect("MFJS-enabled profile");
    assert_eq!(transport.last_body()["moonshot_json_mode"], "mfjs");
}

#[test]
fn built_in_adapters_reject_mutating_tool_contracts_before_transport() {
    let provider = adapter_for_profile(
        profile("openai"),
        Arc::new(MockTransport::returning(Vec::new())),
        Arc::new(TestSecrets),
    )
    .expect("adapter");
    let mut request = text_request();
    request.tools.push(ModelTool {
        name: "delete_topic".to_owned(),
        description: "must never be model-visible".to_owned(),
        input_schema: serde_json::json!({"type":"object"}),
        mutates_cluster: true,
        strict: true,
    });

    let error = provider
        .invoke(&InvocationContext::new(CorrelationId::new()), &request)
        .expect_err("mutation surface must fail closed");

    assert_eq!(error.code, ProviderErrorCode::PolicyDenied);
    assert!(!error.retryable);
}

#[test]
fn router_uses_only_limited_fallback_and_records_actual_identity() {
    let primary_profile = profile("openai");
    let fallback_profile = profile("deepseek");
    let primary = Arc::new(ScriptedProvider::new(
        primary_profile.clone(),
        Err(ProviderError::timeout("primary timed out")),
    ));
    let fallback = Arc::new(ScriptedProvider::new(
        fallback_profile.clone(),
        Ok(success_response("deepseek", "deepseek-chat")),
    ));
    let mut registry = ProviderRegistry::new();
    registry
        .register(primary_profile.clone(), primary)
        .expect("register primary");
    registry
        .register(fallback_profile.clone(), fallback)
        .expect("register fallback");
    let router = ProviderRouter::new(registry, RoutingPolicy { max_fallbacks: 1 });
    let requirements = RoutingRequirements::new(DataClass::Internal).requiring(ProviderCapability::Chat);
    let metadata = InvocationMetadata {
        purpose: InvocationPurpose::Diagnosis,
        requested_profile_id: Some(primary_profile.id.clone()),
        prompt_version: "diagnosis.v1".to_owned(),
        output_schema_version: "hypothesis.v1".to_owned(),
        mark_primary: true,
        ..InvocationMetadata::default()
    };

    let outcome = router
        .invoke(&text_request(), &requirements, &metadata)
        .expect("fallback invocation");
    let ModelInvocationOutcome::Completed(result) = outcome else {
        panic!("expected completed model invocation");
    };

    assert_eq!(result.record.requested_profile_id.as_deref(), Some("openai"));
    assert_eq!(result.record.actual_profile_id, "deepseek");
    assert_eq!(result.record.actual_model_family, "deepseek");
    assert_eq!(result.record.actual_model_revision, fallback_profile.model_revision);
    assert_eq!(result.record.endpoint_instance, fallback_profile.endpoint_instance);
    assert_eq!(result.record.fallback_chain.len(), 1);
    assert_eq!(
        result.diagnosis_selection.primary_model_invocation_id,
        Some(result.record.invocation_id.clone())
    );
    assert!(!result.diagnosis_selection.execution_eligible);
}

#[test]
fn router_does_not_fallback_on_policy_denial() {
    let primary_profile = profile("openai");
    let fallback_profile = profile("deepseek");
    let primary = Arc::new(ScriptedProvider::new(
        primary_profile.clone(),
        Err(ProviderError::policy_denied("policy denied")),
    ));
    let fallback = Arc::new(ScriptedProvider::new(
        fallback_profile.clone(),
        Ok(success_response("deepseek", "deepseek-chat")),
    ));
    let mut registry = ProviderRegistry::new();
    registry.register(primary_profile, primary).expect("register primary");
    registry
        .register(fallback_profile, fallback)
        .expect("register fallback");
    let router = ProviderRouter::new(registry, RoutingPolicy { max_fallbacks: 1 });
    let metadata = InvocationMetadata {
        requested_profile_id: Some("openai".to_owned()),
        ..InvocationMetadata::default()
    };

    let error = router
        .invoke(
            &text_request(),
            &RoutingRequirements::new(DataClass::Internal),
            &metadata,
        )
        .expect_err("policy denial must stop routing");

    assert_eq!(error.code, ProviderErrorCode::PolicyDenied);
}

#[test]
fn router_validates_json_schema_locally_and_does_not_fallback_on_invalid_output() {
    let primary_profile = profile("openai");
    let fallback_profile = profile("deepseek");
    let primary = Arc::new(ScriptedProvider::new(
        primary_profile.clone(),
        Ok(success_response("openai", "gpt-configured")),
    ));
    let fallback = Arc::new(ScriptedProvider::new(
        fallback_profile.clone(),
        Ok(success_response("deepseek", "deepseek-chat")),
    ));
    let mut registry = ProviderRegistry::new();
    registry.register(primary_profile, primary).expect("register primary");
    registry
        .register(fallback_profile, fallback)
        .expect("register fallback");
    let router = ProviderRouter::new(registry, RoutingPolicy { max_fallbacks: 1 });
    let mut request = text_request();
    request.response_format = ResponseFormat::JsonSchema {
        name: "diagnosis".to_owned(),
        schema: serde_json::json!({
            "type":"object",
            "required":["status"],
            "properties":{"status":{"type":"string"}},
            "additionalProperties":false
        }),
        strict: true,
    };
    let metadata = InvocationMetadata {
        requested_profile_id: Some("openai".to_owned()),
        ..InvocationMetadata::default()
    };

    let error = router
        .invoke(&request, &RoutingRequirements::new(DataClass::Internal), &metadata)
        .expect_err("invalid structured output must fail locally");

    assert_eq!(error.code, ProviderErrorCode::SchemaValidationFailed);
    assert!(!error.fallback_allowed());
}

#[test]
fn router_returns_rules_only_when_no_profile_is_eligible() {
    let router = ProviderRouter::new(ProviderRegistry::new(), RoutingPolicy { max_fallbacks: 1 });
    let outcome = router
        .invoke(
            &text_request(),
            &RoutingRequirements::new(DataClass::Restricted),
            &InvocationMetadata::default(),
        )
        .expect("rules-only is an explicit successful degradation");
    let ModelInvocationOutcome::RulesOnly(result) = outcome else {
        panic!("expected rules-only");
    };

    assert!(result.primary_model_invocation_id.is_none());
    assert!(!result.execution_eligible);
}

#[test]
fn router_filters_residency_region_and_budget_before_requested_priority() {
    let cloud_profile = profile("openai");
    let local_profile = profile("vllm");
    let cloud = Arc::new(ScriptedProvider::new(
        cloud_profile.clone(),
        Ok(success_response("openai", "gpt-configured")),
    ));
    let local = Arc::new(ScriptedProvider::new(
        local_profile.clone(),
        Ok(success_response("vllm", "served-model")),
    ));
    let mut registry = ProviderRegistry::new();
    registry.register(cloud_profile, cloud).expect("register cloud");
    registry.register(local_profile, local).expect("register local");
    let router = ProviderRouter::new(registry, RoutingPolicy { max_fallbacks: 1 });
    let mut requirements = RoutingRequirements::new(DataClass::Restricted).requiring(ProviderCapability::Chat);
    requirements.region = Some("private".to_owned());
    requirements.max_cost_microusd_per_1k_tokens = Some(0);
    let metadata = InvocationMetadata {
        requested_profile_id: Some("openai".to_owned()),
        ..InvocationMetadata::default()
    };

    let outcome = router
        .invoke(&text_request(), &requirements, &metadata)
        .expect("local route");
    let ModelInvocationOutcome::Completed(result) = outcome else {
        panic!("expected local provider");
    };

    assert_eq!(result.record.actual_profile_id, "vllm");
    assert_eq!(result.record.actual_model_family, "local");
}

#[test]
fn profile_manifest_registers_only_existing_readable_contract_fixtures() {
    let raw = include_str!("fixtures/providers/provider-profile-manifest.v1.yaml");
    let manifest = ProviderProfileManifest::parse(raw).expect("manifest");
    let builtins: BTreeSet<_> = builtin_provider_profiles()
        .into_iter()
        .map(|profile| profile.id)
        .collect();
    let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/providers");

    assert_eq!(manifest.schema_version, "rocketmq-sre.provider-profile-manifest.v1");
    assert!(manifest.profiles.len() >= 15);
    for entry in manifest.profiles {
        assert!(builtins.contains(&entry.id), "missing profile {}", entry.id);
        assert!(!entry.capabilities.is_empty(), "missing capabilities for {}", entry.id);
        assert!(
            fixture_root.join(&entry.fixture).is_file(),
            "missing fixture {}",
            entry.fixture
        );
    }
}

struct ScriptedProvider {
    profile: ProviderProfile,
    result: Mutex<Result<CanonicalModelResponse, ProviderError>>,
}

impl ScriptedProvider {
    fn new(profile: ProviderProfile, result: Result<CanonicalModelResponse, ProviderError>) -> Self {
        Self {
            profile,
            result: Mutex::new(result),
        }
    }
}

impl ChatModelProvider for ScriptedProvider {
    fn profile_id(&self) -> &str {
        &self.profile.id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.profile.capabilities.clone()
    }

    fn health(&self) -> ProviderHealth {
        self.profile.health
    }

    fn invoke(
        &self,
        _context: &InvocationContext,
        _request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderError> {
        self.result.lock().expect("script lock").clone()
    }
}

fn success_response(provider: &str, model: &str) -> CanonicalModelResponse {
    CanonicalModelResponse::text(provider, model, "healthy", FinishReason::Stop)
}

#[derive(Default)]
struct MockSpi {
    cancelled: Mutex<bool>,
}

impl ProviderSpi for MockSpi {
    fn handshake(&self, request: &SpiHandshakeRequest) -> Result<SpiHandshakeResponse, ProviderError> {
        Ok(SpiHandshakeResponse {
            wire_version: request.wire_version.clone(),
            adapter_identity: "spiffe://sre/provider/example".to_owned(),
            credential_owner: CredentialOwner::Adapter,
            capabilities: ProviderCapabilities::chat_default(),
            credential_version_fingerprint: Some("version:adapter-v1".to_owned()),
        })
    }

    fn invoke(&self, _request: &SpiInvokeRequest) -> Result<CanonicalModelResponse, ProviderError> {
        Ok(success_response("spi", "example-model"))
    }

    fn invoke_stream(
        &self,
        request: &SpiStreamRequest,
    ) -> Result<rocketmq_sre_model_gateway::BoundedModelStream, ProviderError> {
        let cancellation = rocketmq_sre_model_gateway::CancellationToken::default();
        let (sink, stream) = rocketmq_sre_model_gateway::BoundedModelStream::channel(request.bounds, cancellation)?;
        sink.try_send(rocketmq_sre_model_gateway::ModelStreamEvent::Finish {
            reason: FinishReason::Stop,
        })?;
        Ok(stream)
    }

    fn cancel(&self, _request: &SpiCancelRequest) -> Result<(), ProviderError> {
        *self.cancelled.lock().expect("cancel lock") = true;
        Ok(())
    }

    fn health(&self) -> Result<SpiHealth, ProviderError> {
        Ok(SpiHealth {
            status: ProviderHealth::Healthy,
            credential_version_fingerprint: Some("version:adapter-v1".to_owned()),
        })
    }
}

#[test]
fn provider_spi_enforces_version_mtls_health_and_cancel_contracts() {
    let adapter = Arc::new(MockSpi::default());
    let client = ProviderSpiClient::connect(
        adapter.clone(),
        SpiClientConfig::mutual_tls("spiffe://sre/gateway", "spiffe://sre/provider/example"),
    )
    .expect("SPI handshake");

    assert_eq!(client.credential_owner(), CredentialOwner::Adapter);
    assert_eq!(client.health().expect("health").status, ProviderHealth::Healthy);
    let stream_request = text_request();
    let stream = client
        .invoke_stream(&InvocationContext::new(stream_request.correlation_id), &stream_request)
        .expect("SPI stream");
    assert!(matches!(
        stream.recv_timeout(Duration::from_millis(10)).expect("stream finish"),
        rocketmq_sre_model_gateway::ModelStreamEvent::Finish {
            reason: FinishReason::Stop
        }
    ));
    client.cancel("invocation-1", CorrelationId::new()).expect("cancel");
    assert!(*adapter.cancelled.lock().expect("cancel lock"));

    let wrong_identity = ProviderSpiClient::connect(
        Arc::new(MockSpi::default()),
        SpiClientConfig::mutual_tls("spiffe://sre/gateway", "spiffe://sre/provider/not-the-adapter"),
    )
    .expect_err("adapter identity mismatch must fail closed");
    assert_eq!(wrong_identity.code, ProviderErrorCode::MutualTlsFailed);

    let wrong_version = ProviderSpiClient::connect(
        Arc::new(VersionMismatchSpi),
        SpiClientConfig::mutual_tls("spiffe://sre/gateway", "spiffe://sre/provider/example"),
    )
    .expect_err("wire version mismatch must fail closed");
    assert_eq!(wrong_version.code, ProviderErrorCode::UnsupportedWireVersion);

    let mut bounded_config = SpiClientConfig::mutual_tls("spiffe://sre/gateway", "spiffe://sre/provider/example");
    bounded_config.max_payload_bytes = 1;
    let bounded_client =
        ProviderSpiClient::connect(Arc::new(MockSpi::default()), bounded_config).expect("bounded client handshake");
    assert_eq!(
        bounded_client
            .invoke(&InvocationContext::new(CorrelationId::new()), &text_request())
            .expect_err("SPI payload bound")
            .code,
        ProviderErrorCode::OutputTooLarge
    );

    let expired_request = text_request();
    let mut expired_context = InvocationContext::new(expired_request.correlation_id);
    expired_context.deadline_unix_ms = Some(0);
    assert_eq!(
        client
            .invoke(&expired_context, &expired_request)
            .expect_err("expired SPI deadline")
            .code,
        ProviderErrorCode::Timeout
    );
}

struct VersionMismatchSpi;

impl ProviderSpi for VersionMismatchSpi {
    fn handshake(&self, _request: &SpiHandshakeRequest) -> Result<SpiHandshakeResponse, ProviderError> {
        Ok(SpiHandshakeResponse {
            wire_version: "rocketmq-sre.provider-spi.v999".to_owned(),
            adapter_identity: "spiffe://sre/provider/example".to_owned(),
            credential_owner: CredentialOwner::Adapter,
            capabilities: ProviderCapabilities::chat_default(),
            credential_version_fingerprint: None,
        })
    }

    fn invoke(&self, _request: &SpiInvokeRequest) -> Result<CanonicalModelResponse, ProviderError> {
        Err(ProviderError::service_unavailable("not connected"))
    }

    fn invoke_stream(
        &self,
        _request: &SpiStreamRequest,
    ) -> Result<rocketmq_sre_model_gateway::BoundedModelStream, ProviderError> {
        Err(ProviderError::service_unavailable("not connected"))
    }

    fn cancel(&self, _request: &SpiCancelRequest) -> Result<(), ProviderError> {
        Err(ProviderError::service_unavailable("not connected"))
    }

    fn health(&self) -> Result<SpiHealth, ProviderError> {
        Err(ProviderError::service_unavailable("not connected"))
    }
}
