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

use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use reqwest::header::HeaderMap;
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::super::resource_metadata_url;
use super::super::serve_authenticated;
use super::E2eContext;
use super::E2eError;
use super::E2eResult;
use crate::audit::AuditTrail;
use crate::audit::JsonlAuditSink;
use crate::auth::AuthError;
use crate::auth::AuthState;
use crate::auth::JwksSource;
use crate::config::ControlConfig;

const PUBLIC_HOST: &str = "control.example.test";
const PUBLIC_ORIGIN: &str = "https://control.example.test";
const ISSUER: &str = "https://issuer.example.test";
const AUDIENCE: &str = "rocketmq-mcp-control";
const SESSION_HEADER: &str = "mcp-session-id";
const PROTOCOL_VERSION: &str = "2025-11-25";
const EXPECTED_TOOL_NAMES: [&str; 5] = [
    "rocketmq_patch_broker_config",
    "rocketmq_reset_consumer_offset",
    "rocketmq_set_consumer_request_mode",
    "rocketmq_upsert_consumer_group",
    "rocketmq_upsert_topic",
];
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const OAUTH_PRIVATE_KEY: &[u8] = include_bytes!("../../../tests/fixtures/oauth-private-key.pem");

pub(super) struct GeneratedTlsMaterial {
    pub certificate_path: String,
    pub private_key_path: String,
    pub private_key_markers: Vec<String>,
}

#[derive(Clone)]
pub(super) struct StaticJwks;

impl JwksSource for StaticJwks {
    async fn fetch(&self) -> Result<Vec<u8>, AuthError> {
        serde_json::to_vec(&serde_json::json!({"keys": [{
            "kty": "RSA",
            "kid": "e2e-key",
            "alg": "RS256",
            "use": "sig",
            "key_ops": ["verify"],
            "n": RSA_N,
            "e": "AQAB"
        }]}))
        .map_err(|_| AuthError::Unavailable)
    }
}

pub(super) struct ControlInstance {
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<(), crate::error::ControlError>>>,
    runtime: rocketmq_runtime::RuntimeContext,
}

impl ControlInstance {
    pub async fn start(config_path: &Path) -> E2eResult<(Self, McpClient)> {
        let config = ControlConfig::load(config_path).e2e("load E2E control configuration")?;
        let port = config
            .server
            .bind
            .parse::<SocketAddr>()
            .e2e("parse E2E control bind")?
            .port();
        let certificate = tokio::fs::read(&config.server.tls.cert_path)
            .await
            .e2e("read E2E control certificate")?;
        let audit_sink = JsonlAuditSink::open(&config.audit.path, config.audit.capacity, config.audit.max_record_bytes)
            .await
            .e2e("open E2E audit sink")?;
        let audit = AuditTrail::resume(Arc::new(audit_sink))
            .await
            .e2e("recover E2E audit trail")?;
        let auth = AuthState::from_source(&config.oauth, resource_metadata_url(&config), StaticJwks)
            .await
            .e2e("initialize local static RS256 verifier")?;
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-real-cluster-e2e");
        let context = runtime.service_context("control-server");
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task =
            tokio::spawn(
                async move { serve_authenticated(config, context, audit, shutdown.cancelled_owned(), auth).await },
            );
        let mut instance = Self {
            cancellation,
            task: Some(task),
            runtime,
        };
        match McpClient::connect(port, &certificate).await {
            Ok(client) => Ok((instance, client)),
            Err(error) => {
                let cleanup = instance.stop().await;
                match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(E2eError::new(format!(
                        "{error}; control startup cleanup failed: {cleanup}"
                    ))),
                }
            }
        }
    }

    pub async fn stop(&mut self) -> E2eResult<()> {
        self.cancellation.cancel();
        let task_result = match self.task.take() {
            Some(mut task) => match tokio::time::timeout(SERVER_STOP_TIMEOUT, &mut task).await {
                Ok(joined) => joined
                    .e2e("join control server task")?
                    .e2e("control server returned an error"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    Err(E2eError::new("control server stop deadline expired"))
                }
            },
            None => Ok(()),
        };
        let report = self.runtime.shutdown_tasks(SERVER_STOP_TIMEOUT).await;
        let runtime_result = report.assert_no_task_leak().map_err(E2eError::new);
        task_result.and(runtime_result)
    }
}

impl Drop for ControlInstance {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = self.runtime.shutdown_tasks_now();
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    aud: &'a str,
    sub: &'a str,
    exp: u64,
    nbf: u64,
    scope: &'a str,
    rocketmq_operations: Vec<&'a str>,
    rocketmq_clusters: Vec<&'a str>,
}

pub(super) struct McpClient {
    client: reqwest::Client,
    token: String,
    session: Option<String>,
    endpoint: String,
    raw_responses: Vec<String>,
}

impl McpClient {
    async fn connect(port: u16, certificate_pem: &[u8]) -> E2eResult<Self> {
        let certificate = reqwest::Certificate::from_pem(certificate_pem).e2e("parse E2E root certificate")?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(REQUEST_TIMEOUT)
            .add_root_certificate(certificate)
            .resolve(PUBLIC_HOST, SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
            .build()
            .e2e("build E2E TLS client")?;
        let token = token()?;
        let endpoint = format!("{PUBLIC_ORIGIN}/mcp");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let response = send_json(
                &client,
                &token,
                &endpoint,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "real-cluster-e2e", "version": "1.0"}
                    }
                }),
            )
            .await;
            if let Ok((_status, headers, body)) = response {
                let session = session_id_from_initialize(&headers)?;
                let value: Value = serde_json::from_str(&body).e2e("decode initialize response")?;
                validate_json_rpc_response(&value, 1)?;
                super::ensure(
                    value["result"]["protocolVersion"] == PROTOCOL_VERSION,
                    "initialize response did not negotiate the requested protocol version",
                )?;
                let mut mcp = Self {
                    client,
                    token,
                    session,
                    endpoint,
                    raw_responses: vec![body],
                };
                mcp.initialized().await?;
                mcp.assert_five_tools().await?;
                return Ok(mcp);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(E2eError::new(
                    "control TLS endpoint did not become ready before its deadline",
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn initialized(&mut self) -> E2eResult<()> {
        let (status, _headers, body) = send_json(
            &self.client,
            &self.token,
            &self.endpoint,
            self.session.as_deref(),
            serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await?;
        super::ensure(
            status == StatusCode::ACCEPTED,
            "initialized notification did not return HTTP 202",
        )?;
        self.raw_responses.push(body);
        Ok(())
    }

    async fn assert_five_tools(&mut self) -> E2eResult<()> {
        let response = self.request(2, "tools/list", serde_json::json!({})).await?;
        let tools = response["result"]["tools"]
            .as_array()
            .ok_or_else(|| E2eError::new("tools/list response omitted its array"))?;
        let mut names = tools
            .iter()
            .map(|tool| {
                tool["name"]
                    .as_str()
                    .ok_or_else(|| E2eError::new("tools/list entry omitted its name"))
            })
            .collect::<E2eResult<Vec<_>>>()?;
        names.sort_unstable();
        super::ensure(
            names.as_slice() == EXPECTED_TOOL_NAMES,
            "tools/list did not expose exactly the five reviewed mutation tools",
        )
    }

    pub async fn call(&mut self, id: u64, tool: &str, arguments: Value) -> E2eResult<Value> {
        let response = self
            .request(
                id,
                "tools/call",
                serde_json::json!({"name": tool, "arguments": arguments}),
            )
            .await?;
        if response.get("error").is_some() {
            return Err(E2eError::new("MCP tools/call returned a JSON-RPC error"));
        }
        let text = tool_response_text(&response, false)?;
        serde_json::from_str(text).e2e("decode typed mutation response")
    }

    pub async fn call_expect_error(&mut self, id: u64, tool: &str, arguments: Value) -> E2eResult<Value> {
        let response = self
            .request(
                id,
                "tools/call",
                serde_json::json!({"name": tool, "arguments": arguments}),
            )
            .await?;
        super::ensure(
            response.get("error").is_none(),
            "MCP tools/call failure used a JSON-RPC error instead of a tool result",
        )?;
        let text = tool_response_text(&response, true)?;
        serde_json::from_str(text).e2e("decode stable MCP tool error envelope")
    }

    async fn request(&mut self, id: u64, method: &str, params: Value) -> E2eResult<Value> {
        let (_status, _headers, body) = send_json(
            &self.client,
            &self.token,
            &self.endpoint,
            self.session.as_deref(),
            serde_json::json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )
        .await?;
        let value = serde_json::from_str(&body).e2e("decode MCP JSON response")?;
        validate_json_rpc_response(&value, id)?;
        self.raw_responses.push(body);
        Ok(value)
    }

    pub fn assert_public_surfaces_redacted(&self, operator: &str, reason: &str, extra: &[String]) -> E2eResult<()> {
        let oauth_private_key = String::from_utf8_lossy(OAUTH_PRIVATE_KEY);
        let mut sensitive_markers = vec![operator, reason, self.token.as_str(), RSA_N];
        sensitive_markers.extend(private_key_markers(oauth_private_key.as_ref()));
        sensitive_markers.extend(extra.iter().map(String::as_str));
        for response in &self.raw_responses {
            for sensitive in &sensitive_markers {
                if sensitive.is_empty() {
                    continue;
                }
                super::ensure(
                    !response_contains_sensitive(response, sensitive),
                    "a public MCP response exposed private operator, reason, token, key, endpoint, or raw backend data",
                )?;
            }
        }
        Ok(())
    }
}

fn private_key_markers(private_key: &str) -> Vec<&str> {
    let mut markers = Vec::with_capacity(private_key.lines().count() + 1);
    if !private_key.is_empty() {
        markers.push(private_key);
    }
    markers.extend(private_key.lines().map(str::trim).filter(|line| !line.is_empty()));
    markers
}

fn response_contains_sensitive(response: &str, sensitive: &str) -> bool {
    if response.contains(sensitive) {
        return true;
    }
    let escaped = json_escaped_string_contents(sensitive);
    if !escaped.is_empty() && response.contains(&escaped) {
        return true;
    }
    serde_json::from_str::<Value>(response)
        .is_ok_and(|value| json_value_contains_sensitive(&value, sensitive, &escaped, 0))
}

fn json_value_contains_sensitive(value: &Value, sensitive: &str, escaped: &str, depth: usize) -> bool {
    match value {
        Value::String(text) => {
            text.contains(sensitive)
                || (!escaped.is_empty() && text.contains(escaped))
                || (depth < 2
                    && serde_json::from_str::<Value>(text)
                        .is_ok_and(|nested| json_value_contains_sensitive(&nested, sensitive, escaped, depth + 1)))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| json_value_contains_sensitive(value, sensitive, escaped, depth)),
        Value::Object(values) => values
            .values()
            .any(|value| json_value_contains_sensitive(value, sensitive, escaped, depth)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn json_escaped_string_contents(value: &str) -> String {
    serde_json::to_string(value)
        .ok()
        .and_then(|encoded| {
            encoded
                .strip_prefix('"')
                .and_then(|text| text.strip_suffix('"'))
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

fn session_id_from_initialize(headers: &HeaderMap) -> E2eResult<Option<String>> {
    let Some(value) = headers.get(SESSION_HEADER) else {
        return Ok(None);
    };
    let bytes = value.as_bytes();
    super::ensure(
        !bytes.is_empty() && bytes.iter().all(|byte| (0x21..=0x7e).contains(byte)),
        "initialize response returned an invalid MCP session id",
    )?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map(Some)
        .map_err(|_| E2eError::new("initialize response returned an invalid MCP session id"))
}

fn validate_json_rpc_response(response: &Value, request_id: u64) -> E2eResult<()> {
    super::ensure(
        response["jsonrpc"] == "2.0",
        "MCP response omitted the JSON-RPC 2.0 marker",
    )?;
    super::ensure(
        response["id"].as_u64() == Some(request_id),
        "MCP response id did not match its request",
    )?;
    super::ensure(
        response.get("result").is_some() ^ response.get("error").is_some(),
        "MCP response did not contain exactly one of result or error",
    )
}

fn tool_response_text(response: &Value, expected_error: bool) -> E2eResult<&str> {
    let result = response["result"]
        .as_object()
        .ok_or_else(|| E2eError::new("MCP tools/call response omitted its result object"))?;
    let is_error = match result.get("isError") {
        None => false,
        Some(Value::Bool(is_error)) => *is_error,
        Some(_) => return Err(E2eError::new("MCP tools/call result used an invalid isError value")),
    };
    super::ensure(
        is_error == expected_error,
        "MCP tools/call result error semantics did not match the invocation outcome",
    )?;
    let content = result
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| E2eError::new("MCP tools/call result omitted its content array"))?;
    super::ensure(
        content.len() == 1 && content[0]["type"] == "text",
        "MCP tools/call result did not contain exactly one text content item",
    )?;
    content[0]["text"]
        .as_str()
        .ok_or_else(|| E2eError::new("MCP tools/call text content omitted its string value"))
}

async fn send_json(
    client: &reqwest::Client,
    token: &str,
    endpoint: &str,
    session: Option<&str>,
    body: Value,
) -> E2eResult<(StatusCode, HeaderMap, String)> {
    let response = build_json_request(client, token, endpoint, session, body)
        .send()
        .await
        .e2e("send TLS Streamable HTTP request")?;
    let status = response.status();
    let headers = response.headers().clone();
    let text = response.text().await.e2e("read TLS Streamable HTTP response")?;
    if !status.is_success() {
        return Err(E2eError::new(format!(
            "TLS Streamable HTTP request returned status {status}"
        )));
    }
    Ok((status, headers, text))
}

fn build_json_request(
    client: &reqwest::Client,
    token: &str,
    endpoint: &str,
    session: Option<&str>,
    body: Value,
) -> reqwest::RequestBuilder {
    let mut request = client
        .post(endpoint)
        .header("authorization", format!("Bearer {token}"))
        .header("origin", PUBLIC_ORIGIN)
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .json(&body);
    if let Some(session) = session {
        request = request.header(SESSION_HEADER, session);
    }
    request
}

fn token() -> E2eResult<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .e2e("read token clock")?
        .as_secs();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("e2e-key".to_owned());
    jsonwebtoken::encode(
        &header,
        &Claims {
            iss: ISSUER,
            aud: AUDIENCE,
            sub: "e2e.operator@example.test",
            exp: now + 3600,
            nbf: now.saturating_sub(1),
            scope: "rocketmq:write",
            rocketmq_operations: vec![
                "topic_upsert",
                "consumer_group_upsert",
                "consumer_offset_reset",
                "broker_config_patch",
                "consumer_request_mode",
            ],
            rocketmq_clusters: vec!["E2eCluster"],
        },
        &EncodingKey::from_rsa_pem(OAUTH_PRIVATE_KEY).e2e("load test-only RS256 key")?,
    )
    .e2e("sign test-only RS256 token")
}

pub(super) fn write_tls_material(root: &Path) -> E2eResult<GeneratedTlsMaterial> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![PUBLIC_HOST.to_owned()]).e2e("generate E2E TLS identity")?;
    let cert_path = root.join("control-cert.pem");
    let key_path = root.join("control-key.pem");
    let private_key = signing_key.serialize_pem();
    let private_key_markers = private_key_markers(&private_key)
        .into_iter()
        .map(str::to_owned)
        .collect();
    std::fs::write(&cert_path, cert.pem()).e2e("write E2E TLS certificate")?;
    std::fs::write(&key_path, private_key).e2e("write E2E TLS private key")?;
    Ok(GeneratedTlsMaterial {
        certificate_path: path_for_toml(&cert_path),
        private_key_path: path_for_toml(&key_path),
        private_key_markers,
    })
}

pub(super) fn path_for_toml(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub(super) const fn operator() -> &'static str {
    "e2e.operator@example.test"
}

pub(super) const fn reason() -> &'static str {
    "verify isolated E2E #10004"
}

pub(super) const fn public_host() -> &'static str {
    PUBLIC_HOST
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;

    use super::build_json_request;
    use super::private_key_markers;
    use super::response_contains_sensitive;
    use super::session_id_from_initialize;
    use super::tool_response_text;
    use super::validate_json_rpc_response;
    use super::write_tls_material;
    use super::HeaderMap;
    use super::OAUTH_PRIVATE_KEY;
    use super::RSA_N;
    use super::SESSION_HEADER;

    const TEST_WINDOWS_PATH: &str = r"D:\owned\cluster";
    const TEST_PEM_BODY: &str = "c3RhYmxlLXByaXZhdGUta2V5LWJvZHktbWFya2Vy";
    const TEST_MULTILINE_PEM: &str =
        "-----BEGIN PRIVATE KEY-----\nc3RhYmxlLXByaXZhdGUta2V5LWJvZHktbWFya2Vy\n-----END PRIVATE KEY-----\n";

    #[test]
    fn initialize_accepts_stateless_or_visible_ascii_session_ids_and_rejects_invalid_values() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            session_id_from_initialize(&headers).expect("stateless initialize response must be accepted"),
            None
        );

        headers.insert(SESSION_HEADER, HeaderValue::from_static(""));
        assert!(session_id_from_initialize(&headers).is_err());

        headers.insert(SESSION_HEADER, HeaderValue::from_static(" "));
        assert!(session_id_from_initialize(&headers).is_err());

        headers.insert(
            SESSION_HEADER,
            HeaderValue::from_bytes(b"opaque-session\x80").expect("construct obs-text header fixture"),
        );
        assert!(session_id_from_initialize(&headers).is_err());

        headers.insert(SESSION_HEADER, HeaderValue::from_static("owned-session_123~"));
        assert_eq!(
            session_id_from_initialize(&headers).expect("visible ASCII session must be accepted"),
            Some("owned-session_123~".to_owned())
        );
    }

    #[test]
    fn request_builder_echoes_session_header_if_and_only_if_initialize_supplied_one() {
        let client = reqwest::Client::new();
        let stateless = build_json_request(
            &client,
            "test-token",
            "https://control.example.test/mcp",
            None,
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        )
        .build()
        .expect("build stateless MCP request");
        assert!(stateless.headers().get(SESSION_HEADER).is_none());

        let stateful = build_json_request(
            &client,
            "test-token",
            "https://control.example.test/mcp",
            Some("owned-session_123~"),
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .build()
        .expect("build MCP request with server-provided session");
        assert_eq!(
            stateful.headers().get(SESSION_HEADER).map(HeaderValue::as_bytes),
            Some(b"owned-session_123~".as_slice())
        );
    }

    #[test]
    fn json_rpc_ids_and_tool_content_semantics_are_strict() {
        let success = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 17,
            "result": {"content": [{"type": "text", "text": "{}"}], "isError": false}
        });
        assert!(validate_json_rpc_response(&success, 17).is_ok());
        assert_eq!(
            tool_response_text(&success, false).expect("accept success tool content"),
            "{}"
        );
        assert!(validate_json_rpc_response(&success, 18).is_err());

        let failure = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 18,
            "result": {"content": [{"type": "text", "text": "{}"}], "isError": true}
        });
        assert!(validate_json_rpc_response(&failure, 18).is_ok());
        assert!(tool_response_text(&failure, true).is_ok());
        assert!(tool_response_text(&failure, false).is_err());
    }

    #[test]
    fn response_redaction_detects_nested_json_escaped_paths_and_pem_material() {
        let tool_text = serde_json::to_string(&serde_json::json!({
            "diagnostic_path": TEST_WINDOWS_PATH,
            "private_key": TEST_MULTILINE_PEM,
        }))
        .expect("encode nested tool response");
        let response = serde_json::to_string(&serde_json::json!({
            "result": {"content": [{"type": "text", "text": tool_text}]}
        }))
        .expect("encode MCP response");

        assert!(!response.contains(TEST_WINDOWS_PATH));
        assert!(!response.contains(TEST_MULTILINE_PEM));
        assert!(response_contains_sensitive(&response, TEST_WINDOWS_PATH));
        assert!(response_contains_sensitive(&response, TEST_MULTILINE_PEM));
        assert!(private_key_markers(TEST_MULTILINE_PEM)
            .into_iter()
            .any(|marker| marker == TEST_PEM_BODY && response_contains_sensitive(&response, marker)));
    }

    #[test]
    fn generated_tls_private_key_markers_match_the_written_server_key_and_are_scanned() {
        let root = tempfile::tempdir().expect("create TLS marker test root");
        let material = write_tls_material(root.path()).expect("generate TLS marker test material");
        let written_key = std::fs::read_to_string(root.path().join("control-key.pem"))
            .expect("read generated TLS private key fixture");
        let written_markers = private_key_markers(&written_key)
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();

        assert!(
            material.private_key_markers == written_markers,
            "retained TLS private-key markers differed from the key file loaded by Control"
        );
        let oauth_private_key = String::from_utf8_lossy(OAUTH_PRIVATE_KEY);
        let has_server_only_body_marker = material.private_key_markers.iter().any(|marker| {
            !marker.starts_with("-----")
                && marker.len() >= 32
                && !oauth_private_key.contains(marker)
                && !RSA_N.contains(marker)
        });
        assert!(
            has_server_only_body_marker,
            "generated TLS key did not retain a body marker distinct from OAuth material"
        );
        let response = serde_json::to_string(&serde_json::json!({
            "result": {"content": [{"type": "text", "text": written_key}]}
        }))
        .expect("encode simulated TLS private-key leak");
        assert!(
            material
                .private_key_markers
                .iter()
                .any(|marker| response_contains_sensitive(&response, marker)),
            "generated TLS private-key leak was not detected"
        );
    }
}
