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

// The protocol versions that the server answers, through TLS and the whole HTTP stack. Included
// into the transport tests, whose configuration and tokens it uses.

/// A client of the real router behind a TLS listener on a loopback port.
struct HttpsClient {
    port: u16,
}

/// Serves `router` over TLS for as long as `exchange` talks to it.
///
/// The server runs beside the exchange in the same task, so nothing outlives the test.
async fn over_https<F>(router: Router, exchange: impl FnOnce(HttpsClient) -> F)
where
    F: std::future::Future<Output = ()>,
{
    let directory = tempfile::tempdir().unwrap();
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = directory.path().join("server.pem");
    let key_path = directory.path().join("server-key.pem");
    tokio::fs::write(&cert_path, cert.pem()).await.unwrap();
    tokio::fs::write(&key_path, signing_key.serialize_pem()).await.unwrap();
    let mut config = config();
    config.server.tls.cert_path = cert_path.to_string_lossy().into_owned();
    config.server.tls.key_path = key_path.to_string_lossy().into_owned();
    let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-protocol-test");
    let context = runtime.service_context("mcp-control-protocol-test");
    let tls = TlsServerRuntime::initialize_with_service_context(tls_config(&config), &context)
        .await
        .unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tcp.local_addr().unwrap().port();
    let cancellation = CancellationToken::new();
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
    /// Posts one JSON-RPC message to the configured origin, as `bearer` when there is one.
    async fn post(
        &self,
        bearer: Option<&str>,
        protocol_version: Option<&str>,
        message: &serde_json::Value,
    ) -> (u16, serde_json::Value) {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let mut request = client
            .post(format!("https://127.0.0.1:{}/mcp", self.port))
            .header("host", "control.example.test")
            .header("origin", "https://control.example.test")
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
        let body = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        (status, body)
    }
}

/// The metadata with which a request names `version` for itself, as a client that sends no
/// `initialize` does from `2026-07-28` on.
fn self_description(version: &str) -> serde_json::Value {
    serde_json::json!({
        "io.modelcontextprotocol/protocolVersion": version,
        "io.modelcontextprotocol/clientInfo": {"name": "protocol-test", "version": "1.0.0"},
        "io.modelcontextprotocol/clientCapabilities": {}
    })
}

fn request_of(method: &str, version: Option<&str>) -> serde_json::Value {
    let params = match version {
        Some(version) => serde_json::json!({"_meta": self_description(version)}),
        None => serde_json::json!({}),
    };
    serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
}

#[tokio::test]
async fn a_request_without_initialize_is_answered_only_in_the_supported_protocol_revision() {
    over_https(router().await, the_supported_revision_alone_is_answered).await;
}

async fn the_supported_revision_alone_is_answered(server: HttpsClient) {
    const SUPPORTED: &str = rocketmq_mcp_auth::MCP_PROTOCOL_VERSION;
    let bearer = token(REQUIRED_WRITE_SCOPE);
    let caller = Some(bearer.as_str());
    let answered = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": []}});
    let refused = serde_json::to_value(ControlError::request_rejected().envelope()).unwrap();

    // A request that names no revision in its header, or the supported one, is answered. No
    // `initialize` came before it: the transport keeps no session.
    for stated in [None, Some(SUPPORTED)] {
        let listed = server.post(caller, stated, &request_of("tools/list", None)).await;
        assert_eq!(listed, (200, answered.clone()), "{stated:?}");
    }

    // Any other revision in the header is refused, whether the SDK knows it or not, and the
    // answer repeats nothing that was sent.
    for other in ["2024-11-05", "2025-03-26", "2025-06-18", "2026-07-28", "1999-01-01"] {
        let listed = server.post(caller, Some(other), &request_of("tools/list", None)).await;
        assert_eq!(listed, (400, refused.clone()), "{other}");
    }

    // So is a request that names another revision for itself in place of a handshake. The SDK
    // would answer `2026-07-28` this way, without any `initialize`.
    for other in ["2025-06-18", "2026-07-28"] {
        let described = request_of("tools/list", Some(other));
        assert_eq!(
            server.post(caller, Some(other), &described).await,
            (400, refused.clone()),
            "{other}"
        );
        // Under the header of the supported revision it is not answered either.
        let (status, body) = server.post(caller, Some(SUPPORTED), &described).await;
        assert_eq!(status, 400, "{other}: {body}");
        assert!(body.get("result").is_none(), "{other}: {body}");
    }
    let described = request_of("tools/list", Some(SUPPORTED));
    assert_eq!(server.post(caller, Some(SUPPORTED), &described).await, (200, answered));

    // What the server advertises is the set it answers.
    let (status, discovered) = server
        .post(caller, Some(SUPPORTED), &request_of("server/discover", Some(SUPPORTED)))
        .await;
    assert_eq!(status, 200, "{discovered}");
    assert_eq!(
        discovered["result"]["supportedVersions"],
        serde_json::json!(rocketmq_mcp_auth::SUPPORTED_MCP_PROTOCOL_VERSIONS)
    );

    // The handshake keeps its own answer for a client that names another revision there.
    let handshake = |version: &str| {
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": version,
            "capabilities": {},
            "clientInfo": {"name": "protocol-test", "version": "1.0.0"}
        }})
    };
    let (status, declined) = server.post(caller, None, &handshake("2025-06-18")).await;
    assert_eq!(status, 200);
    assert_eq!(declined["error"]["code"], -32602);
    assert_eq!(declined["error"]["message"], "unsupported MCP protocol version");
    let (status, accepted) = server.post(caller, None, &handshake(SUPPORTED)).await;
    assert_eq!(status, 200);
    assert_eq!(accepted["result"]["protocolVersion"], SUPPORTED);

    // Authentication comes first: a caller without a token learns nothing about revisions.
    let (status, unauthenticated) = server
        .post(None, Some("2026-07-28"), &request_of("tools/list", None))
        .await;
    assert_eq!(status, 401);
    assert_eq!(unauthenticated["code"], "unauthorized");
}
