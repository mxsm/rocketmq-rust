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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::LOCATION;
use axum::response::IntoResponse;
use axum::response::Response;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_model_gateway::AsyncBuiltinProviderClient;
use rocketmq_sre_model_gateway::AsyncModelTransport;
use rocketmq_sre_model_gateway::FinishReason;
use rocketmq_sre_model_gateway::HttpModelTransport;
use rocketmq_sre_model_gateway::HttpTlsConfig;
use rocketmq_sre_model_gateway::HttpTransportConfig;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ModelStreamEvent;
use rocketmq_sre_model_gateway::ModelTool;
use rocketmq_sre_model_gateway::ProviderDialect;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::ResponseFormat;
use rocketmq_sre_model_gateway::SecretMaterial;
use rocketmq_sre_model_gateway::StreamBounds;
use rocketmq_sre_model_gateway::TlsClientIdentity;
use rocketmq_sre_model_gateway::ToolChoice;
use rocketmq_sre_model_gateway::TransportRequest;
use rocketmq_sre_model_gateway::builtin_provider_profiles;
use serde_json::Value;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Default)]
struct MockState {
    redirect_target_hits: AtomicUsize,
}

struct MockServer {
    endpoint: String,
    state: Arc<MockState>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl MockServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock provider");
        let address = listener.local_addr().expect("mock provider address");
        let state = Arc::new(MockState::default());
        let app = Router::new().fallback(mock_handler).with_state(state.clone());
        let (shutdown, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = receiver.await;
                })
                .await
        });
        Self {
            endpoint: format!("http://{address}"),
            state,
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await.expect("mock server task").expect("mock server");
    }
}

async fn mock_handler(State(state): State<Arc<MockState>>, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    match uri.path() {
        "/auth/openai" => auth_response(
            headers
                .get("authorization")
                .is_some_and(|value| value.as_bytes() == b"Bearer test-secret"),
        ),
        "/auth/deepseek-responses" => auth_response(
            headers
                .get("authorization")
                .is_some_and(|value| value.as_bytes() == b"Bearer test-secret"),
        ),
        "/auth/azure" => auth_response(
            headers
                .get("api-key")
                .is_some_and(|value| value.as_bytes() == b"test-secret"),
        ),
        "/auth/anthropic" => auth_response(
            headers
                .get("x-api-key")
                .is_some_and(|value| value.as_bytes() == b"test-secret")
                && headers
                    .get("anthropic-version")
                    .is_some_and(|value| value.as_bytes() == b"2023-06-01"),
        ),
        "/auth/gemini" => auth_response(
            headers
                .get("x-goog-api-key")
                .is_some_and(|value| value.as_bytes() == b"test-secret"),
        ),
        "/auth/bedrock" => auth_response(bedrock_authorized(&headers)),
        "/chat/completions" => openai_response(&headers, &body),
        "/responses" => deepseek_responses_response(&headers, &body).await,
        "/messages" => {
            if headers
                .get("x-api-key")
                .is_none_or(|value| value.as_bytes() != b"test-secret")
            {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            Json(json!({
                "id": "msg-local-mock",
                "model": "test-model",
                "content": [{"type": "text", "text": "broker is healthy"}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 4, "output_tokens": 3}
            }))
            .into_response()
        }
        "/models/test-model:generateContent" => {
            if headers
                .get("x-goog-api-key")
                .is_none_or(|value| value.as_bytes() != b"test-secret")
            {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            Json(json!({
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "broker is healthy"}]},
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": 4,
                    "candidatesTokenCount": 3,
                    "totalTokenCount": 7
                },
                "modelVersion": "test-model"
            }))
            .into_response()
        }
        "/model/test-model/converse" => {
            if !bedrock_authorized(&headers) {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            Json(json!({
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [{"text": "broker is healthy"}]
                    }
                },
                "stopReason": "end_turn",
                "usage": {"inputTokens": 4, "outputTokens": 3, "totalTokens": 7}
            }))
            .into_response()
        }
        "/slow" => {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Json(json!({"status": "late"})).into_response()
        }
        "/large" => Json(json!({"payload": "x".repeat(4_096)})).into_response(),
        "/invalid-json" => (StatusCode::OK, "not-json").into_response(),
        "/redirect" => (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, "/redirect-target")]).into_response(),
        "/redirect-target" => {
            state.redirect_target_hits.fetch_add(1, Ordering::Relaxed);
            Json(json!({"followed": true})).into_response()
        }
        _ => (StatusCode::NOT_FOUND, Json(json!({"error": "not_found"}))).into_response(),
    }
}

async fn deepseek_responses_response(headers: &HeaderMap, bytes: &[u8]) -> Response {
    if headers
        .get("authorization")
        .is_none_or(|value| value.as_bytes() != b"Bearer test-secret")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let request: Value = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let serialized = request.to_string();
    if serialized.contains("STATUS_401") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if serialized.contains("STATUS_429") {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    if serialized.contains("STATUS_503") {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if serialized.contains("TIMEOUT") {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if request["stream"] == true {
        let body = if serialized.contains("NO_TERMINAL") {
            concat!(
                "data: {\"event\":\"response.created\",\"sequence_number\":0,",
                "\"response\":{\"id\":\"resp-local\",\"status\":\"in_progress\"}}\n\n",
                "data: {\"event\":\"response.output_text.delta\",\"sequence_number\":1,",
                "\"delta\":\"partial\"}\n\n",
            )
        } else {
            concat!(
                "data: {\"event\":\"response.created\",\"sequence_number\":0,",
                "\"response\":{\"id\":\"resp-local\",\"status\":\"in_progress\"}}\n\n",
                "data: {\"event\":\"response.output_text.delta\",\"sequence_number\":1,",
                "\"delta\":\"broker \"}\n\n",
                "data: {\"event\":\"response.output_text.delta\",\"sequence_number\":2,",
                "\"delta\":\"healthy\"}\n\n",
                "data: {\"event\":\"response.completed\",\"sequence_number\":3,",
                "\"response\":{\"id\":\"resp-local\",\"status\":\"completed\",",
                "\"usage\":{\"input_tokens\":4,\"output_tokens\":3,\"total_tokens\":7}}}\n\n",
            )
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/event-stream")
            .body(Body::from(body))
            .expect("SSE response");
    }
    if request["tools"].as_array().is_some_and(|tools| !tools.is_empty()) {
        return Json(json!({
            "id": "resp-tool-local",
            "model": "test-model",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call-1",
                "name": "query_consumer_lag",
                "arguments": "{\"consumer_group\":\"synthetic-group\"}"
            }],
            "usage": {"input_tokens": 4, "output_tokens": 3, "total_tokens": 7}
        }))
        .into_response();
    }
    Json(json!({
        "id": "resp-local",
        "model": "test-model",
        "status": "completed",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "broker is healthy"}]
        }],
        "usage": {"input_tokens": 4, "output_tokens": 3, "total_tokens": 7}
    }))
    .into_response()
}

fn auth_response(authorized: bool) -> Response {
    Json(json!({"authorized": authorized})).into_response()
}

fn bedrock_authorized(headers: &HeaderMap) -> bool {
    headers.get("authorization").is_some_and(|value| {
        value
            .to_str()
            .is_ok_and(|value| value.starts_with("AWS4-HMAC-SHA256 Credential=AKID/"))
    }) && headers.contains_key("x-amz-date")
        && headers.contains_key("x-amz-content-sha256")
        && headers
            .get("x-amz-security-token")
            .is_some_and(|value| value.as_bytes() == b"session-token")
}

fn openai_response(headers: &HeaderMap, bytes: &[u8]) -> Response {
    if headers
        .get("authorization")
        .is_none_or(|value| value.as_bytes() != b"Bearer test-secret")
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": {"type": "authentication_error"}})),
        )
            .into_response();
    }
    let request: Value = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid_json"}))).into_response(),
    };
    let prompt = request["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .and_then(|message| message["content"].as_str())
        .unwrap_or_default();
    if prompt == "RATE_LIMIT" {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": {"type": "rate_limit"}})),
        )
            .into_response();
    }
    let message = if request["tools"].as_array().is_some_and(|tools| !tools.is_empty()) {
        json!({
            "role": "assistant",
            "content": null,
            "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {
                    "name": "query_consumer_lag",
                    "arguments": "{\"group\":\"synthetic-group\"}"
                }
            }]
        })
    } else if request.get("response_format").is_some() {
        json!({"role": "assistant", "content": "{\"status\":\"healthy\"}"})
    } else {
        json!({"role": "assistant", "content": "broker is healthy"})
    };
    Json(json!({
        "id": "chatcmpl-local-mock",
        "model": "test-model",
        "choices": [{
            "message": message,
            "finish_reason": if message.get("tool_calls").is_some() { "tool_calls" } else { "stop" }
        }],
        "usage": {
            "prompt_tokens": 4,
            "completion_tokens": 3,
            "total_tokens": 7
        }
    }))
    .into_response()
}

fn transport(config: HttpTransportConfig) -> Arc<HttpModelTransport> {
    Arc::new(HttpModelTransport::new(config).expect("HTTP transport"))
}

fn secret(value: impl Into<String>) -> SecretMaterial {
    SecretMaterial::new(value, "test-version", None)
}

fn request(
    endpoint: &str,
    path: &str,
    dialect: ProviderDialect,
    credential: Option<SecretMaterial>,
) -> TransportRequest {
    TransportRequest {
        correlation_id: CorrelationId::new(),
        dialect,
        endpoint: endpoint.to_owned(),
        path: path.to_owned(),
        body: json!({"ping": true}),
        credential,
        deadline_unix_ms: None,
        max_response_bytes: 1024 * 1024,
    }
}

#[tokio::test]
async fn async_provider_client_maps_text_json_tools_and_status_errors() {
    let server = MockServer::start().await;
    let mut profile = builtin_provider_profiles()
        .into_iter()
        .find(|candidate| candidate.id == "openai")
        .expect("OpenAI profile");
    profile.endpoint.clone_from(&server.endpoint);
    profile.model = "test-model".to_owned();
    let client =
        AsyncBuiltinProviderClient::new(profile, transport(HttpTransportConfig::default())).expect("async provider");
    let context = InvocationContext::new(CorrelationId::new());

    let text_request = rocketmq_sre_model_gateway::CanonicalModelRequest::new(
        CorrelationId::new(),
        "test-model",
        vec![ModelMessage::text(ModelRole::User, "health")],
    );
    let text = client
        .invoke(&context, &text_request, Some(secret("test-secret")))
        .await
        .expect("text response");
    assert_eq!(text.content, "broker is healthy");
    assert_eq!(text.finish_reason, FinishReason::Stop);
    assert_eq!(text.usage.total_tokens, Some(7));

    let mut json_request = text_request.clone();
    json_request.response_format = ResponseFormat::JsonObject;
    let json = client
        .invoke(&context, &json_request, Some(secret("test-secret")))
        .await
        .expect("JSON response");
    assert_eq!(json.content, r#"{"status":"healthy"}"#);

    let mut tool_request = text_request.clone();
    tool_request.tools.push(ModelTool::read_only(
        "query_consumer_lag",
        "Read current lag",
        json!({"type": "object"}),
    ));
    let tool = client
        .invoke(&context, &tool_request, Some(secret("test-secret")))
        .await
        .expect("tool response");
    assert_eq!(tool.tool_calls[0].name, "query_consumer_lag");

    let rate_request = rocketmq_sre_model_gateway::CanonicalModelRequest::new(
        CorrelationId::new(),
        "test-model",
        vec![ModelMessage::text(ModelRole::User, "RATE_LIMIT")],
    );
    let error = client
        .invoke(&context, &rate_request, Some(secret("test-secret")))
        .await
        .expect_err("rate limit");
    assert_eq!(error.code, ProviderErrorCode::RateLimited);
    assert!(error.retryable);

    server.stop().await;
}

#[tokio::test]
async fn async_http_client_parses_every_builtin_protocol_family() {
    let server = MockServer::start().await;
    let transport: Arc<dyn AsyncModelTransport> = transport(HttpTransportConfig::default());
    let cases = [
        ("openai", "test-secret"),
        ("anthropic", "test-secret"),
        ("google-gemini", "test-secret"),
        (
            "aws-bedrock",
            r#"{"access_key_id":"AKID","secret_access_key":"secret-key","session_token":"session-token","region":"us-east-1"}"#,
        ),
    ];

    for (profile_id, credential) in cases {
        let mut profile = builtin_provider_profiles()
            .into_iter()
            .find(|candidate| candidate.id == profile_id)
            .unwrap_or_else(|| panic!("missing profile: {profile_id}"));
        profile.endpoint.clone_from(&server.endpoint);
        profile.model = "test-model".to_owned();
        let client = AsyncBuiltinProviderClient::new(profile, transport.clone()).expect("async provider");
        let correlation_id = CorrelationId::new();
        let request = rocketmq_sre_model_gateway::CanonicalModelRequest::new(
            correlation_id,
            "test-model",
            vec![ModelMessage::text(ModelRole::User, "health")],
        );
        let response = client
            .invoke(
                &InvocationContext::new(correlation_id),
                &request,
                Some(secret(credential)),
            )
            .await
            .unwrap_or_else(|error| panic!("{profile_id}: {error}"));
        assert_eq!(response.content, "broker is healthy", "{profile_id}");
        assert_eq!(response.finish_reason, FinishReason::Stop, "{profile_id}");
    }

    server.stop().await;
}

#[tokio::test]
async fn provider_specific_auth_headers_are_applied_without_exposing_secrets() {
    let server = MockServer::start().await;
    let transport = transport(HttpTransportConfig::default());
    let cases = [
        ("/auth/openai", ProviderDialect::OpenAi, "test-secret"),
        (
            "/auth/deepseek-responses",
            ProviderDialect::DeepSeekResponses,
            "test-secret",
        ),
        ("/auth/azure", ProviderDialect::AzureOpenAi, "test-secret"),
        ("/auth/anthropic", ProviderDialect::Anthropic, "test-secret"),
        ("/auth/gemini", ProviderDialect::Gemini, "test-secret"),
        (
            "/auth/bedrock",
            ProviderDialect::Bedrock,
            r#"{"access_key_id":"AKID","secret_access_key":"secret-key","session_token":"session-token","region":"us-east-1"}"#,
        ),
    ];
    for (path, dialect, credential) in cases {
        let response = transport
            .invoke(request(&server.endpoint, path, dialect, Some(secret(credential))))
            .await
            .unwrap_or_else(|error| panic!("{dialect:?}: {error}"));
        assert_eq!(response.body["authorized"], true, "{dialect:?}");
    }

    let material = secret("super-secret-value");
    assert!(!format!("{material:?}").contains("super-secret-value"));
    server.stop().await;
}

#[tokio::test]
async fn transport_rejects_insecure_non_loopback_redirects_and_expired_deadlines() {
    let server = MockServer::start().await;
    let transport = transport(HttpTransportConfig::default());

    let insecure = transport
        .invoke(request(
            "http://example.com",
            "/chat",
            ProviderDialect::OpenAi,
            Some(secret("test-secret")),
        ))
        .await
        .expect_err("non-loopback HTTP");
    assert_eq!(insecure.code, ProviderErrorCode::PolicyDenied);

    let mut expired = request(
        &server.endpoint,
        "/auth/openai",
        ProviderDialect::OpenAi,
        Some(secret("test-secret")),
    );
    expired.deadline_unix_ms = Some(0);
    let expired = transport.invoke(expired).await.expect_err("expired deadline");
    assert_eq!(expired.code, ProviderErrorCode::Timeout);

    let redirect = transport
        .invoke(request(
            &server.endpoint,
            "/redirect",
            ProviderDialect::OpenAi,
            Some(secret("test-secret")),
        ))
        .await
        .expect_err("redirect");
    assert_eq!(redirect.code, ProviderErrorCode::PolicyDenied);
    assert_eq!(redirect.provider_status, Some(307));
    assert_eq!(server.state.redirect_target_hits.load(Ordering::Relaxed), 0);

    server.stop().await;
}

#[tokio::test]
async fn transport_enforces_timeout_response_and_request_bounds() {
    let server = MockServer::start().await;
    let transport = transport(
        HttpTransportConfig::default()
            .with_timeouts(Duration::from_secs(1), Duration::from_millis(40))
            .with_body_limits(128, 128),
    );

    let timeout = transport
        .invoke(request(
            &server.endpoint,
            "/slow",
            ProviderDialect::OpenAi,
            Some(secret("test-secret")),
        ))
        .await
        .expect_err("timeout");
    assert_eq!(timeout.code, ProviderErrorCode::Timeout);

    let too_large = transport
        .invoke(request(
            &server.endpoint,
            "/large",
            ProviderDialect::OpenAi,
            Some(secret("test-secret")),
        ))
        .await
        .expect_err("response bound");
    assert_eq!(too_large.code, ProviderErrorCode::OutputTooLarge);

    let mut request_too_large = request(
        &server.endpoint,
        "/auth/openai",
        ProviderDialect::OpenAi,
        Some(secret("test-secret")),
    );
    request_too_large.body = json!({"payload": "x".repeat(256)});
    let request_too_large = transport.invoke(request_too_large).await.expect_err("request bound");
    assert_eq!(request_too_large.code, ProviderErrorCode::OutputTooLarge);

    server.stop().await;
}

#[tokio::test]
async fn invalid_json_and_expired_credentials_fail_closed() {
    let server = MockServer::start().await;
    let transport = transport(HttpTransportConfig::default());

    let invalid_json = transport
        .invoke(request(
            &server.endpoint,
            "/invalid-json",
            ProviderDialect::OpenAi,
            Some(secret("test-secret")),
        ))
        .await
        .expect_err("invalid JSON");
    assert_eq!(invalid_json.code, ProviderErrorCode::ProtocolError);

    let expired_secret = SecretMaterial::new("test-secret", "expired", Some(0));
    let expired_secret = transport
        .invoke(request(
            &server.endpoint,
            "/auth/openai",
            ProviderDialect::OpenAi,
            Some(expired_secret),
        ))
        .await
        .expect_err("expired secret");
    assert_eq!(expired_secret.code, ProviderErrorCode::SecretUnavailable);

    server.stop().await;
}

fn deepseek_client(endpoint: &str, timeout: Duration) -> AsyncBuiltinProviderClient {
    let mut profile = builtin_provider_profiles()
        .into_iter()
        .find(|candidate| candidate.id == "deepseek-responses")
        .expect("DeepSeek Responses profile");
    profile.endpoint = endpoint.to_owned();
    profile.model = "test-model".to_owned();
    AsyncBuiltinProviderClient::new(
        profile,
        transport(HttpTransportConfig::default().with_timeouts(Duration::from_secs(1), timeout)),
    )
    .expect("async DeepSeek provider")
}

fn deepseek_request(prompt: &str) -> rocketmq_sre_model_gateway::CanonicalModelRequest {
    rocketmq_sre_model_gateway::CanonicalModelRequest::new(
        CorrelationId::new(),
        "test-model",
        vec![ModelMessage::text(ModelRole::User, prompt)],
    )
}

#[tokio::test]
async fn deepseek_http_stream_maps_semantic_events_and_enforces_termination() {
    let server = MockServer::start().await;
    let client = deepseek_client(&server.endpoint, Duration::from_secs(2));
    let request = deepseek_request("stream health");
    let context = InvocationContext::new(request.correlation_id);
    let mut stream = client
        .invoke_stream(&context, &request, Some(secret("test-secret")))
        .await
        .expect("DeepSeek SSE stream");
    let mut events = Vec::new();
    while let Some(event) = stream.recv().await.expect("bounded SSE event") {
        events.push(event);
    }
    assert!(matches!(events.first(), Some(ModelStreamEvent::Start { .. })));
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::TextDelta { delta } => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>(),
        "broker healthy"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Usage { .. }))
    );
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::Finish {
            reason: FinishReason::Stop
        })
    ));

    let request = deepseek_request("NO_TERMINAL");
    let context = InvocationContext::new(request.correlation_id);
    let mut stream = client
        .invoke_stream(&context, &request, Some(secret("test-secret")))
        .await
        .expect("unterminated DeepSeek SSE stream");
    assert!(matches!(
        stream.recv().await.expect("start"),
        Some(ModelStreamEvent::Start { .. })
    ));
    assert!(matches!(
        stream.recv().await.expect("partial delta"),
        Some(ModelStreamEvent::TextDelta { .. })
    ));
    assert_eq!(
        stream.recv().await.expect_err("premature EOF must fail closed").code,
        ProviderErrorCode::ProtocolError
    );
    server.stop().await;
}

#[tokio::test]
async fn deepseek_http_stream_honors_cancellation_and_event_bounds() {
    let server = MockServer::start().await;
    let client = deepseek_client(&server.endpoint, Duration::from_secs(2));
    let request = deepseek_request("stream health");
    let context = InvocationContext::new(request.correlation_id);
    let mut stream = client
        .invoke_stream(&context, &request, Some(secret("test-secret")))
        .await
        .expect("DeepSeek SSE stream");
    assert!(matches!(
        stream.recv().await.expect("start"),
        Some(ModelStreamEvent::Start { .. })
    ));
    stream.cancel();
    assert_eq!(
        stream.recv().await.expect_err("cancelled stream").code,
        ProviderErrorCode::Cancelled
    );

    let mut bounded_context = InvocationContext::new(request.correlation_id);
    bounded_context.stream_bounds = StreamBounds {
        channel_capacity: 1,
        max_events: 1,
        max_bytes: 4 * 1024,
    };
    let mut stream = client
        .invoke_stream(&bounded_context, &request, Some(secret("test-secret")))
        .await
        .expect("bounded DeepSeek SSE stream");
    assert!(matches!(
        stream.recv().await.expect("first event"),
        Some(ModelStreamEvent::Start { .. })
    ));
    assert_eq!(
        stream.recv().await.expect_err("event bound").code,
        ProviderErrorCode::OutputTooLarge
    );
    server.stop().await;
}

#[tokio::test]
async fn deepseek_http_tool_selection_and_error_matrix_are_stable() {
    let server = MockServer::start().await;
    let client = deepseek_client(&server.endpoint, Duration::from_secs(2));
    let mut tool_request = deepseek_request("Call the declared read-only lag query exactly once");
    tool_request.tools.push(ModelTool::read_only(
        "query_consumer_lag",
        "Read current lag",
        json!({
            "type": "object",
            "properties": {"consumer_group": {"type": "string"}},
            "required": ["consumer_group"],
            "additionalProperties": false
        }),
    ));
    tool_request.tool_choice = ToolChoice::Auto;
    let response = client
        .invoke(
            &InvocationContext::new(tool_request.correlation_id),
            &tool_request,
            Some(secret("test-secret")),
        )
        .await
        .expect("read-only tool selection");
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].name, "query_consumer_lag");
    assert_eq!(response.tool_calls[0].arguments["consumer_group"], "synthetic-group");

    for (prompt, expected) in [
        ("STATUS_401", ProviderErrorCode::AuthenticationFailed),
        ("STATUS_429", ProviderErrorCode::RateLimited),
        ("STATUS_503", ProviderErrorCode::ServiceUnavailable),
    ] {
        let request = deepseek_request(prompt);
        let error = client
            .invoke_stream(
                &InvocationContext::new(request.correlation_id),
                &request,
                Some(secret("test-secret")),
            )
            .await
            .expect_err("provider status must fail");
        assert_eq!(error.code, expected, "{prompt}");
    }

    let timeout_client = deepseek_client(&server.endpoint, Duration::from_millis(40));
    let request = deepseek_request("TIMEOUT");
    let error = timeout_client
        .invoke_stream(
            &InvocationContext::new(request.correlation_id),
            &request,
            Some(secret("test-secret")),
        )
        .await
        .expect_err("stream handshake timeout");
    assert_eq!(error.code, ProviderErrorCode::Timeout);
    server.stop().await;
}

#[test]
fn tls_configuration_is_validated_and_redacted() {
    let invalid_root = HttpTransportConfig::default().with_tls(
        HttpTlsConfig::default()
            .with_root_certificate_pem(b"not a certificate".to_vec())
            .with_only_custom_roots(true),
    );
    let error = HttpModelTransport::new(invalid_root).expect_err("invalid root");
    assert_eq!(error.code, ProviderErrorCode::ProfileInvalid);

    let identity =
        TlsClientIdentity::from_pem(b"certificate-secret", b"private-key-secret").expect("non-empty identity");
    let identity_debug = format!("{identity:?}");
    assert!(!identity_debug.contains("certificate-secret"));
    assert!(!identity_debug.contains("private-key-secret"));
    let invalid_identity =
        HttpTransportConfig::default().with_tls(HttpTlsConfig::default().with_client_identity(identity));
    let error = HttpModelTransport::new(invalid_identity).expect_err("invalid identity");
    assert_eq!(error.code, ProviderErrorCode::ProfileInvalid);
}
