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

//! The protocol revisions that the server answers, through TLS and the whole HTTP stack.

use rocketmq_mcp_auth::MCP_PROTOCOL_VERSION;
use rocketmq_mcp_auth::SUPPORTED_MCP_PROTOCOL_VERSIONS;
use serde_json::Value;

use super::*;

const TOKEN: &str = "protocol-test-token";

/// A client of the real router behind a TLS listener on a loopback port.
struct HttpsClient {
    port: u16,
}

/// Serves the real router over TLS for as long as `exchange` talks to it.
///
/// The server runs beside the exchange in the same task, so nothing outlives the test.
async fn over_https<F>(exchange: impl FnOnce(HttpsClient) -> F)
where
    F: std::future::Future<Output = ()>,
{
    let directory = tempfile::tempdir().unwrap();
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = directory.path().join("server-cert.pem");
    let key_path = directory.path().join("server-key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, signing_key.serialize_pem()).unwrap();

    let mut config = McpConfig::load(example_config_path()).unwrap();
    config.audit.enabled = true;
    config.audit.sink = "memory".to_string();
    config.server.http.tls.cert_path = cert_path.to_string_lossy().into_owned();
    config.server.http.tls.key_path = key_path.to_string_lossy().into_owned();
    let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-protocol-version-test");
    let tls = TlsServerRuntime::initialize_with_service_context(
        tls_config(&config.server.http),
        &runtime.service_context("mcp-https-listener"),
    )
    .await
    .unwrap();
    let app = McpApp::new(
        config,
        runtime.service_context("mcp-app"),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .unwrap();
    let cancellation = CancellationToken::new();
    let router = build_router(app, cancellation.clone()).unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tcp.local_addr().unwrap().port();
    let stopped = cancellation.clone();
    let served = async move {
        axum::serve(HttpsListener { tcp, tls }, router)
            .with_graceful_shutdown(stopped.cancelled_owned())
            .await
    };
    let exchanged = async {
        exchange(HttpsClient { port }).await;
        cancellation.cancel();
    };
    let (served, ()) = tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(served, exchanged) })
        .await
        .expect("the exchange and the shutdown of the server finish in time");
    served.unwrap();
}

impl HttpsClient {
    /// Posts one JSON-RPC message, as the holder of `bearer` when there is one.
    async fn post(&self, bearer: Option<&str>, protocol_version: Option<&str>, message: &Value) -> (u16, Value) {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let mut request = client
            .post(format!("https://127.0.0.1:{}/mcp", self.port))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(bearer) = bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        if let Some(protocol_version) = protocol_version {
            request = request.header("mcp-protocol-version", protocol_version);
        }
        let response = request.body(message.to_string()).send().await.unwrap();
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, body)
    }
}

/// The metadata with which a request names `version` for itself, as a client that sends no
/// `initialize` does from `2026-07-28` on.
fn self_description(version: &str) -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": version,
        "io.modelcontextprotocol/clientInfo": {"name": "protocol-test", "version": "1.0.0"},
        "io.modelcontextprotocol/clientCapabilities": {}
    })
}

fn request_of(method: &str, version: Option<&str>) -> Value {
    let params = match version {
        Some(version) => json!({"_meta": self_description(version)}),
        None => json!({}),
    };
    json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
}

/// Whether `answer` is the Tool list of a request that was served.
fn lists_tools(answer: &(u16, Value)) -> bool {
    answer.0 == 200
        && answer.1["result"]["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty())
}

#[tokio::test]
async fn a_request_without_initialize_is_answered_only_in_the_supported_protocol_revision() {
    with_development_token(TOKEN, over_https(the_supported_revision_alone_is_answered)).await;
}

async fn the_supported_revision_alone_is_answered(server: HttpsClient) {
    let caller = Some(TOKEN);
    let refused = (400, Value::String(UNSUPPORTED_PROTOCOL_VERSION.to_owned()));

    // A request that names no revision in its header, or the supported one, is answered. No
    // `initialize` came before it: the transport keeps no session.
    for stated in [None, Some(MCP_PROTOCOL_VERSION)] {
        let listed = server.post(caller, stated, &request_of("tools/list", None)).await;
        assert!(lists_tools(&listed), "{stated:?}: {listed:?}");
    }

    // Any other revision in the header is refused, whether the SDK knows it or not, and the
    // answer repeats nothing that was sent.
    for other in ["2024-11-05", "2025-03-26", "2025-06-18", "2026-07-28", "1999-01-01"] {
        let listed = server.post(caller, Some(other), &request_of("tools/list", None)).await;
        assert_eq!(listed, refused, "{other}");
    }

    // So is a request that names another revision for itself in place of a handshake. The SDK
    // would answer `2026-07-28` this way, without any `initialize`.
    for other in ["2025-06-18", "2026-07-28"] {
        let described = request_of("tools/list", Some(other));
        assert_eq!(server.post(caller, Some(other), &described).await, refused, "{other}");
        // Under the header of the supported revision it is not answered either.
        let (status, body) = server.post(caller, Some(MCP_PROTOCOL_VERSION), &described).await;
        assert_eq!(status, 400, "{other}: {body}");
        assert!(body.get("result").is_none(), "{other}: {body}");
    }
    let described = request_of("tools/list", Some(MCP_PROTOCOL_VERSION));
    let listed = server.post(caller, Some(MCP_PROTOCOL_VERSION), &described).await;
    assert!(lists_tools(&listed), "{listed:?}");

    // What the server advertises is the set it answers.
    let discover = request_of("server/discover", Some(MCP_PROTOCOL_VERSION));
    let (status, discovered) = server.post(caller, Some(MCP_PROTOCOL_VERSION), &discover).await;
    assert_eq!(status, 200, "{discovered}");
    assert_eq!(
        discovered["result"]["supportedVersions"],
        json!(SUPPORTED_MCP_PROTOCOL_VERSIONS)
    );

    // The handshake keeps its own answer for a client that names another revision there.
    let handshake = |version: &str| {
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": version,
            "capabilities": {},
            "clientInfo": {"name": "protocol-test", "version": "1.0.0"}
        }})
    };
    let (status, declined) = server.post(caller, None, &handshake("2025-06-18")).await;
    assert_eq!(status, 200);
    assert_eq!(declined["error"]["code"], -32602);
    assert_eq!(
        declined["error"]["message"],
        format!("unsupported MCP protocol version; rocketmq-mcp requires {MCP_PROTOCOL_VERSION}")
    );
    assert_eq!(
        declined["error"]["data"]["supported"],
        json!(SUPPORTED_MCP_PROTOCOL_VERSIONS)
    );
    let (status, accepted) = server.post(caller, None, &handshake(MCP_PROTOCOL_VERSION)).await;
    assert_eq!(status, 200);
    assert_eq!(accepted["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);

    // Authentication comes first: a caller without a token learns nothing about revisions.
    let unauthenticated = server
        .post(None, Some("2026-07-28"), &request_of("tools/list", None))
        .await;
    assert_eq!(unauthenticated.0, 401);
    assert_ne!(unauthenticated.1, refused.1);
}
