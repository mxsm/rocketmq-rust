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

use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::Request;
use axum::extract::State;
use axum::http::header::HOST;
use axum::http::header::ORIGIN;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::serve::Listener;
use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::StreamableHttpService;
use rmcp::transport::StreamableHttpServerConfig;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_transport::api::TlsConfig as RocketmqTlsConfig;
use rocketmq_transport::api::TlsMode;
use rocketmq_transport::api::TlsServerConfig;
use rocketmq_transport::api::TlsServerRuntime;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tower_http::timeout::TimeoutLayer;

use crate::auth::oauth_middleware;
use crate::auth::AuthState;
use crate::auth::HttpJwksSource;
use crate::auth::JwksSource;
use crate::config::ControlConfig;
use crate::config::REQUIRED_WRITE_SCOPE;
use crate::error::ControlError;
use crate::server::ControlServer;

pub const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
pub const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn serve<F>(
    config: ControlConfig,
    service_context: ChildServiceContext,
    audit: crate::audit::AuditTrail,
    shutdown: F,
) -> Result<(), ControlError>
where
    F: FutureShutdown,
{
    config.validate()?;
    let metadata_url = resource_metadata_url(&config);
    let auth = AuthState::<HttpJwksSource>::initialize(&config.oauth, metadata_url)
        .await
        .map_err(|_| ControlError::invalid_config())?;
    let tls = TlsServerRuntime::initialize_with_service_context(tls_config(&config), &service_context)
        .await
        .map_err(|_| ControlError::invalid_config())?;
    if tls.active_generation() == 0 {
        return Err(ControlError::invalid_config());
    }
    let bind = config
        .server
        .bind
        .parse::<SocketAddr>()
        .map_err(|_| ControlError::invalid_config())?;
    let tcp = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| ControlError::invalid_config())?;
    let listener = HttpsListener { tcp, tls };
    let cancellation = CancellationToken::new();
    #[cfg(feature = "write-tools")]
    let server = ControlServer::from_config(&config, audit, service_context.component("mutation-tools"))?;
    #[cfg(not(feature = "write-tools"))]
    let server = {
        let _ = (audit, &service_context);
        ControlServer::new(config.mutations.mutations_enabled)
    };
    let router = build_router_with_auth(&config, server, cancellation.clone(), auth);

    tracing::info!("rocketmq-mcp-control authenticated HTTPS transport is ready");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.await;
            cancellation.cancel();
        })
        .await
        .map_err(|_| ControlError::execution_failed())
}

pub trait FutureShutdown: std::future::Future<Output = ()> + Send + 'static {}

impl<T> FutureShutdown for T where T: std::future::Future<Output = ()> + Send + 'static {}

pub(crate) fn build_router_with_auth<S: JwksSource + 'static>(
    config: &ControlConfig,
    server: ControlServer,
    cancellation: CancellationToken,
    auth: AuthState<S>,
) -> Router {
    let endpoint = config.server.endpoint.clone();
    let service = streamable_service(config, server, cancellation);
    let metadata_path = "/.well-known/oauth-protected-resource";
    let metadata = protected_resource_metadata(config);
    let mcp_router = Router::new()
        .nest_service(&endpoint, service)
        .layer(middleware::from_fn_with_state(auth, oauth_middleware::<S>));

    let policy = RequestOriginPolicy {
        host: config.server.public_base_url.host().to_string(),
        origin: config.server.public_base_url.as_str().to_string(),
    };
    apply_http_limits(
        Router::new()
            .route(
                metadata_path,
                get(move || {
                    let metadata = metadata.clone();
                    async move { axum::Json(metadata).into_response() }
                }),
            )
            .merge(mcp_router)
            .layer(middleware::from_fn_with_state(policy, validate_host_origin)),
    )
}

fn apply_http_limits(router: Router) -> Router {
    router.layer(TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        HTTP_REQUEST_TIMEOUT,
    ))
}

#[derive(Clone)]
struct RequestOriginPolicy {
    host: String,
    origin: String,
}

async fn validate_host_origin(State(policy): State<RequestOriginPolicy>, request: Request, next: Next) -> Response {
    let host_matches = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == policy.host);
    let origin_matches = request
        .headers()
        .get(ORIGIN)
        .map(|value| value.to_str().is_ok_and(|value| value == policy.origin))
        .unwrap_or(true);
    if !host_matches || !origin_matches {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ControlError::request_rejected().envelope()),
        )
            .into_response();
    }
    next.run(request).await
}

fn streamable_service(
    config: &ControlConfig,
    server: ControlServer,
    cancellation: CancellationToken,
) -> StreamableHttpService<ControlServer, LocalSessionManager> {
    let server_config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts(config))
        .with_allowed_origins(vec![config.server.public_base_url.as_str().to_string()])
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_max_request_body_bytes(MAX_HTTP_BODY_BYTES)
        .with_cancellation_token(cancellation);
    StreamableHttpService::new(move || Ok(server.clone()), Default::default(), server_config)
}

fn protected_resource_metadata(config: &ControlConfig) -> serde_json::Value {
    json!({
        "resource": format!("{}{}", config.server.public_base_url.as_str(), config.server.endpoint),
        "authorization_servers": [config.oauth.issuer],
        "scopes_supported": [REQUIRED_WRITE_SCOPE],
    })
}

fn resource_metadata_url(config: &ControlConfig) -> String {
    format!(
        "{}/.well-known/oauth-protected-resource",
        config.server.public_base_url.as_str()
    )
}

fn allowed_hosts(config: &ControlConfig) -> Vec<String> {
    let mut hosts = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];
    if let Ok(address) = config.server.bind.parse::<SocketAddr>() {
        match address.ip() {
            IpAddr::V4(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            IpAddr::V6(ip) if !ip.is_unspecified() => hosts.push(ip.to_string()),
            _ => {}
        }
    }
    hosts.push(config.server.public_base_url.host().to_string());
    hosts.sort();
    hosts.dedup();
    hosts
}

fn tls_config(config: &ControlConfig) -> RocketmqTlsConfig {
    RocketmqTlsConfig {
        enable: true,
        server: TlsServerConfig {
            mode: TlsMode::Enforcing,
            cert_path: Some(config.server.tls.cert_path.clone()),
            key_path: Some(config.server.tls.key_path.clone()),
            ..TlsServerConfig::default()
        },
        ..RocketmqTlsConfig::default()
    }
}

struct HttpsListener {
    tcp: tokio::net::TcpListener,
    tls: TlsServerRuntime,
}

impl Listener for HttpsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, remote_address) = match self.tcp.accept().await {
                Ok(accepted) => accepted,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if let Some(stream) = self.tls.accept_stream(stream, remote_address).await {
                return (stream, remote_address);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use axum::body::to_bytes;
    use axum::body::Body;
    use axum::http::header::AUTHORIZATION;
    use axum::http::header::HOST;
    use axum::http::header::ORIGIN;
    use axum::http::Request;
    use jsonwebtoken::encode;
    use jsonwebtoken::Algorithm;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use serde::Serialize;
    use tower::ServiceExt;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    #[cfg(feature = "write-tools")]
    use crate::audit::MemoryAuditSink;
    #[cfg(feature = "write-tools")]
    use crate::audit::ReliableAuditSink;
    use crate::auth::AuthError;
    use crate::config::AuditConfig;
    use crate::config::MutationPolicyConfig;
    use crate::config::OAuthConfig;
    use crate::config::ServerConfig;
    use crate::config::TlsConfig;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    struct StaticSource;

    impl JwksSource for StaticSource {
        async fn fetch(&self) -> Result<Vec<u8>, AuthError> {
            Ok(serde_json::to_vec(&serde_json::json!({"keys": [{
                "kty": "RSA", "kid": "test-key", "alg": "RS256", "use": "sig",
                "key_ops": ["verify"], "n": RSA_N, "e": "AQAB"
            }]}))
            .unwrap())
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<StdMutex<Vec<u8>>>);

    impl Write for CapturedLogs {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: usize,
        scope: &'a str,
        rocketmq_operations: Vec<&'a str>,
        rocketmq_clusters: Vec<&'a str>,
    }

    fn config() -> ControlConfig {
        ControlConfig {
            server: ServerConfig {
                bind: "127.0.0.1:8090".to_string(),
                endpoint: "/mcp".to_string(),
                public_base_url: crate::config::HttpsOrigin::try_new("https://control.example.test").unwrap(),
                tls: TlsConfig {
                    cert_path: "server.pem".to_string(),
                    key_path: "server-key.pem".to_string(),
                },
            },
            oauth: OAuthConfig {
                issuer: "https://issuer.example.test".to_string(),
                audience: "rocketmq-mcp-control".to_string(),
                jwks_url: "https://issuer.example.test/jwks".to_string(),
                jwks_ca_path: None,
            },
            mutations: MutationPolicyConfig::default(),
            clusters: Vec::new(),
            audit: AuditConfig {
                path: "audit.jsonl".to_string(),
                capacity: 64,
                max_record_bytes: 4096,
            },
        }
    }

    fn token(scope: &str) -> String {
        token_with_claims(scope, Vec::new(), Vec::new())
    }

    fn token_with_claims(scope: &str, operations: Vec<&str>, clusters: Vec<&str>) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".to_string());
        encode(
            &header,
            &TestClaims {
                sub: "operator@example.test",
                iss: "https://issuer.example.test",
                aud: "rocketmq-mcp-control",
                exp: 4_102_444_800,
                scope,
                rocketmq_operations: operations,
                rocketmq_clusters: clusters,
            },
            &EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/oauth-private-key.pem")).unwrap(),
        )
        .unwrap()
    }

    #[cfg(feature = "write-tools")]
    #[derive(Default)]
    struct ProtocolCounters {
        opens: std::sync::atomic::AtomicUsize,
        preflights: std::sync::atomic::AtomicUsize,
        executes: std::sync::atomic::AtomicUsize,
        shutdowns: std::sync::atomic::AtomicUsize,
    }

    #[cfg(feature = "write-tools")]
    struct ProtocolFactory {
        counters: Arc<ProtocolCounters>,
    }

    #[cfg(feature = "write-tools")]
    impl crate::tool_runtime::UpsertSessionFactory for ProtocolFactory {
        fn open<'a>(
            &'a self,
            _cluster: &'a crate::model::ClusterName,
        ) -> crate::tool_runtime::RuntimeFuture<'a, Result<Box<dyn crate::tool_runtime::UpsertSession>, ControlError>>
        {
            Box::pin(async move {
                self.counters.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Box::new(crate::tool_runtime::admin_session::AdminUpsertSession::new(
                    ProtocolBackend {
                        counters: self.counters.clone(),
                        topic_executed: false,
                        group_executed: false,
                    },
                )) as Box<dyn crate::tool_runtime::UpsertSession>)
            })
        }
    }

    #[cfg(feature = "write-tools")]
    struct ProtocolTopicPlan {
        targets: Vec<
            rocketmq_admin_core::core::supervised_mutation::MetadataPreflightTarget<
                rocketmq_admin_core::core::supervised_mutation::TopicReplacement,
            >,
        >,
        failures: Vec<rocketmq_admin_core::core::supervised_mutation::MutationTargetFailure>,
    }

    #[cfg(feature = "write-tools")]
    struct ProtocolGroupPlan {
        targets: Vec<
            rocketmq_admin_core::core::supervised_mutation::MetadataPreflightTarget<
                rocketmq_admin_core::core::supervised_mutation::SubscriptionGroupReplacement,
            >,
        >,
        failures: Vec<rocketmq_admin_core::core::supervised_mutation::MutationTargetFailure>,
    }

    #[cfg(feature = "write-tools")]
    struct ProtocolBackend {
        counters: Arc<ProtocolCounters>,
        topic_executed: bool,
        group_executed: bool,
    }

    #[cfg(feature = "write-tools")]
    impl crate::tool_runtime::admin_session::SupervisedUpsertBackend for ProtocolBackend {
        type TopicPlan = ProtocolTopicPlan;
        type GroupPlan = ProtocolGroupPlan;

        fn preflight_topic<'a>(
            &'a mut self,
            request: &'a rocketmq_admin_core::core::supervised_mutation::TopicMutationPreflightRequest,
            broker_names: &'a [String],
        ) -> crate::tool_runtime::RuntimeFuture<'a, Result<Self::TopicPlan, ControlError>> {
            Box::pin(async move {
                use rocketmq_admin_core::core::supervised_mutation::ExpectedState;
                use rocketmq_admin_core::core::supervised_mutation::MetadataPreflightTarget;
                self.counters
                    .preflights
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(ProtocolTopicPlan {
                    targets: broker_names
                        .iter()
                        .cloned()
                        .map(|broker_name| MetadataPreflightTarget {
                            broker_name,
                            state: if self.topic_executed {
                                ExpectedState::Present { version: 1 }
                            } else {
                                ExpectedState::Absent
                            },
                            current: self.topic_executed.then(|| request.replacement.clone()),
                        })
                        .collect(),
                    failures: Vec::new(),
                })
            })
        }

        fn topic_targets(
            plan: &Self::TopicPlan,
        ) -> Vec<
            rocketmq_admin_core::core::supervised_mutation::MetadataPreflightTarget<
                rocketmq_admin_core::core::supervised_mutation::TopicReplacement,
            >,
        > {
            plan.targets.clone()
        }

        fn topic_failures(
            plan: &Self::TopicPlan,
        ) -> &[rocketmq_admin_core::core::supervised_mutation::MutationTargetFailure] {
            &plan.failures
        }

        fn execute_topic<'a>(
            &'a mut self,
            plan: &'a Self::TopicPlan,
        ) -> crate::tool_runtime::RuntimeFuture<
            'a,
            Result<rocketmq_admin_core::core::supervised_mutation::MetadataMutationOutcome, ControlError>,
        > {
            Box::pin(async move {
                use rocketmq_admin_core::core::supervised_mutation::ExpectedState;
                use rocketmq_admin_core::core::supervised_mutation::MetadataMutationOutcome;
                use rocketmq_admin_core::core::supervised_mutation::MetadataMutationTargetOutcome;
                use rocketmq_admin_core::core::supervised_mutation::MutationPersistenceState;
                use rocketmq_admin_core::core::supervised_mutation::MutationVerificationState;
                self.counters.executes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.topic_executed = true;
                Ok(MetadataMutationOutcome {
                    targets: plan
                        .targets
                        .iter()
                        .map(|target| MetadataMutationTargetOutcome {
                            broker_name: target.broker_name.clone(),
                            expected_state: target.state,
                            resulting_state: Some(ExpectedState::Present { version: 1 }),
                            applied: true,
                            changed: true,
                            persistence: MutationPersistenceState::Persisted,
                            verification: MutationVerificationState::Verified,
                            failure: None,
                            retryable: false,
                        })
                        .collect(),
                    failures: Vec::new(),
                    order_reconciled: Some(true),
                })
            })
        }

        fn preflight_group<'a>(
            &'a mut self,
            request: &'a rocketmq_admin_core::core::supervised_mutation::SubscriptionGroupMutationPreflightRequest,
            broker_names: &'a [String],
        ) -> crate::tool_runtime::RuntimeFuture<'a, Result<Self::GroupPlan, ControlError>> {
            Box::pin(async move {
                use rocketmq_admin_core::core::supervised_mutation::ExpectedState;
                use rocketmq_admin_core::core::supervised_mutation::MetadataPreflightTarget;
                self.counters
                    .preflights
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(ProtocolGroupPlan {
                    targets: broker_names
                        .iter()
                        .cloned()
                        .map(|broker_name| MetadataPreflightTarget {
                            broker_name,
                            state: if self.group_executed {
                                ExpectedState::Present { version: 1 }
                            } else {
                                ExpectedState::Absent
                            },
                            current: self.group_executed.then(|| request.replacement.clone()),
                        })
                        .collect(),
                    failures: Vec::new(),
                })
            })
        }

        fn group_targets(
            plan: &Self::GroupPlan,
        ) -> Vec<
            rocketmq_admin_core::core::supervised_mutation::MetadataPreflightTarget<
                rocketmq_admin_core::core::supervised_mutation::SubscriptionGroupReplacement,
            >,
        > {
            plan.targets.clone()
        }

        fn group_failures(
            plan: &Self::GroupPlan,
        ) -> &[rocketmq_admin_core::core::supervised_mutation::MutationTargetFailure] {
            &plan.failures
        }

        fn execute_group<'a>(
            &'a mut self,
            plan: &'a Self::GroupPlan,
        ) -> crate::tool_runtime::RuntimeFuture<
            'a,
            Result<rocketmq_admin_core::core::supervised_mutation::MetadataMutationOutcome, ControlError>,
        > {
            Box::pin(async move {
                use rocketmq_admin_core::core::supervised_mutation::ExpectedState;
                use rocketmq_admin_core::core::supervised_mutation::MetadataMutationOutcome;
                use rocketmq_admin_core::core::supervised_mutation::MetadataMutationTargetOutcome;
                use rocketmq_admin_core::core::supervised_mutation::MutationPersistenceState;
                use rocketmq_admin_core::core::supervised_mutation::MutationVerificationState;
                self.counters.executes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.group_executed = true;
                Ok(MetadataMutationOutcome {
                    targets: plan
                        .targets
                        .iter()
                        .map(|target| MetadataMutationTargetOutcome {
                            broker_name: target.broker_name.clone(),
                            expected_state: target.state,
                            resulting_state: Some(ExpectedState::Present { version: 1 }),
                            applied: true,
                            changed: true,
                            persistence: MutationPersistenceState::Persisted,
                            verification: MutationVerificationState::Verified,
                            failure: None,
                            retryable: false,
                        })
                        .collect(),
                    failures: Vec::new(),
                    order_reconciled: None,
                })
            })
        }

        fn shutdown(&mut self) -> crate::tool_runtime::RuntimeFuture<'_, Result<(), ControlError>> {
            Box::pin(async move {
                self.counters
                    .shutdowns
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[cfg(feature = "write-tools")]
    async fn write_router() -> (Router, Arc<ProtocolCounters>, Arc<MemoryAuditSink>) {
        use crate::audit::AuditTrail;
        use crate::model::ClusterName;
        use crate::model::ControlOperation;

        let mut config = config();
        config.mutations = MutationPolicyConfig {
            mutations_enabled: true,
            dry_run: true,
            allowed_operations: vec![ControlOperation::TopicUpsert, ControlOperation::ConsumerGroupUpsert],
            allowed_clusters: vec![ClusterName::try_new("cluster-a").unwrap()],
            operation_timeout_seconds: 2,
        };
        let runtime = rocketmq_runtime::RuntimeContext::from_current("control-protocol-write-test");
        let owner = runtime
            .service_context("control-protocol-write-test")
            .task_group()
            .clone();
        let counters = Arc::new(ProtocolCounters::default());
        let sink = Arc::new(MemoryAuditSink::new(64, 4096));
        let server = ControlServer::with_test_factory(
            &config.mutations,
            std::collections::BTreeSet::from([ClusterName::try_new("cluster-a").unwrap()]),
            AuditTrail::new(sink.clone()),
            Arc::new(ProtocolFactory {
                counters: counters.clone(),
            }),
            owner,
        );
        let auth = AuthState::from_source(&config.oauth, resource_metadata_url(&config), StaticSource)
            .await
            .unwrap();
        (
            build_router_with_auth(&config, server, CancellationToken::new(), auth),
            counters,
            sink,
        )
    }

    async fn router() -> Router {
        router_with_server(ControlServer::new(false)).await
    }

    async fn router_with_server(server: ControlServer) -> Router {
        let config = config();
        let auth = AuthState::from_source(&config.oauth, resource_metadata_url(&config), StaticSource)
            .await
            .unwrap();
        build_router_with_auth(&config, server, CancellationToken::new(), auth)
    }

    fn request(path: &str, body: Body, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header(HOST, "control.example.test")
            .header(ORIGIN, "https://control.example.test")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-11-25");
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(body).unwrap()
    }

    #[cfg(feature = "write-tools")]
    fn topic_arguments(topic: &str, broker_names: Vec<String>) -> serde_json::Value {
        serde_json::json!({
            "schema_version": crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": topic,
            "broker_names": broker_names,
            "read_queue_nums": 8,
            "write_queue_nums": 8,
            "perm": 6,
            "order": false,
            "message_type": "NORMAL"
        })
    }

    #[cfg(feature = "write-tools")]
    fn consumer_group_arguments(consumer_group: &str, broker_names: Vec<String>) -> serde_json::Value {
        serde_json::json!({
            "schema_version": crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "consumer_group": consumer_group,
            "broker_names": broker_names,
            "consume_enable": true,
            "consume_from_min_enable": false,
            "consume_broadcast_enable": false,
            "consume_message_orderly": false,
            "retry_queue_nums": 1,
            "retry_max_times": 16,
            "broker_id": 0,
            "which_broker_when_consume_slowly": 1,
            "notify_consumer_ids_changed_enable": true,
            "group_sys_flag": 0,
            "consume_timeout_minute": 15
        })
    }

    #[cfg(feature = "write-tools")]
    async fn authenticated_tool_call(
        router: &Router,
        token: &str,
        id: usize,
        tool: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments}
        });
        let response = router
            .clone()
            .oneshot(request("/mcp", Body::from(body.to_string()), Some(token)))
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[cfg(feature = "write-tools")]
    fn expected_upsert_response(
        operation: &str,
        resource_field: &str,
        resource_name: &str,
        requested: serde_json::Value,
        execute: bool,
    ) -> serde_json::Value {
        let brokers = ["broker-a", "broker-b"];
        let before = brokers
            .into_iter()
            .map(|broker| (broker.to_owned(), serde_json::json!({"kind": "absent"})))
            .collect::<serde_json::Map<_, _>>();
        let after = execute.then(|| {
            brokers
                .into_iter()
                .map(|broker| {
                    (
                        broker.to_owned(),
                        serde_json::json!({"kind": "present", "version": 1, "value": requested}),
                    )
                })
                .collect::<serde_json::Map<_, _>>()
        });
        let targets = brokers
            .into_iter()
            .map(|broker| {
                serde_json::json!({
                    "target": {"broker_name": broker},
                    "before": {"kind": "absent"},
                    "requested": requested,
                    "after": execute.then(|| serde_json::json!({
                        "kind": "present",
                        "version": 1,
                        "value": requested
                    })),
                    "applied": execute,
                    "changed": execute,
                    "persistence": if execute { "persisted" } else { "not_required" },
                    "verification": if execute { "verified" } else { "not_performed" },
                    "failure": null,
                    "retryable": false
                })
            })
            .collect::<Vec<_>>();
        let mut target = serde_json::Map::new();
        target.insert(resource_field.to_owned(), serde_json::json!(resource_name));
        target.insert("brokers".to_owned(), serde_json::json!(brokers));
        serde_json::json!({
            "schema_version": crate::tools::MUTATION_RESULT_SCHEMA_VERSION,
            "operation": operation,
            "cluster": "cluster-a",
            "mode": if execute { "execute" } else { "dry_run" },
            "status": if execute { "applied" } else { "planned" },
            "target": target,
            "before": before,
            "requested": requested,
            "after": after,
            "targets": targets,
            "warnings": []
        })
    }

    #[tokio::test]
    async fn oauth_runs_before_mcp_schema_and_body_limits_are_enforced() {
        let router = router().await;
        let missing = router
            .clone()
            .oneshot(request("/mcp", Body::from("not-json"), None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert!(!missing
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("127.0.0.1"));

        let read_token = token("rocketmq:read");
        let missing_scope = router
            .clone()
            .oneshot(request("/mcp", Body::from("not-json"), Some(&read_token)))
            .await
            .unwrap();
        assert_eq!(missing_scope.status(), StatusCode::FORBIDDEN);

        let write_token = token(REQUIRED_WRITE_SCOPE);
        let oversized = router
            .oneshot(request(
                "/mcp",
                Body::from(vec![b'x'; MAX_HTTP_BODY_BYTES + 1]),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn protected_metadata_and_authenticated_tools_list_are_real_protocol_surfaces() {
        let router = router().await;
        let metadata = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource")
                    .header(HOST, "control.example.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metadata.status(), StatusCode::OK);
        let metadata_body = to_bytes(metadata.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let metadata_text = String::from_utf8(metadata_body.to_vec()).unwrap();
        assert!(!metadata_text.contains("127.0.0.1"));
        assert!(!metadata_text.contains(":8090"));

        let write_token = token(REQUIRED_WRITE_SCOPE);
        let initialize = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"control-test","version":"1.0"}}}"#),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(initialize.status(), StatusCode::OK);
        let tools = router
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(tools.status(), StatusCode::OK);
        let body = to_bytes(tools.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["result"]["tools"], serde_json::json!([]));
    }

    #[cfg(feature = "write-tools")]
    #[tokio::test]
    async fn authenticated_tool_discovery_and_call_enforce_claims_before_schema() {
        let (router, counters, _sink) = write_router().await;
        let both = token_with_claims(
            REQUIRED_WRITE_SCOPE,
            vec!["topic_upsert", "consumer_group_upsert"],
            vec!["cluster-a"],
        );
        let tools = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":20,"method":"tools/list","params":{}}"#),
                Some(&both),
            ))
            .await
            .unwrap();
        let body = to_bytes(tools.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let listed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 2);
        for tool in listed["result"]["tools"].as_array().unwrap() {
            assert_eq!(tool["annotations"]["readOnlyHint"], false);
            assert_eq!(tool["annotations"]["destructiveHint"], true);
            assert_eq!(tool["annotations"]["idempotentHint"], true);
            assert_eq!(tool["annotations"]["openWorldHint"], true);
        }

        let topic_only = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"]);
        let one = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":23,"method":"tools/list","params":{}}"#),
                Some(&topic_only),
            ))
            .await
            .unwrap();
        let one_body = to_bytes(one.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let one_value: serde_json::Value = serde_json::from_slice(&one_body).unwrap();
        assert_eq!(one_value["result"]["tools"].as_array().unwrap().len(), 1);
        let no_cluster = token_with_claims(
            REQUIRED_WRITE_SCOPE,
            vec!["topic_upsert", "consumer_group_upsert"],
            vec!["cluster-b"],
        );
        let zero = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":24,"method":"tools/list","params":{}}"#),
                Some(&no_cluster),
            ))
            .await
            .unwrap();
        let zero_body = to_bytes(zero.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let zero_value: serde_json::Value = serde_json::from_slice(&zero_body).unwrap();
        assert_eq!(zero_value["result"]["tools"].as_array().unwrap().len(), 0);

        let denied = token_with_claims(REQUIRED_WRITE_SCOPE, Vec::new(), vec!["cluster-a"]);
        let denied_call = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"rocketmq_upsert_topic","arguments":{"cluster":"cluster-a","unknown":true}}}"#),
                Some(&denied),
            ))
            .await
            .unwrap();
        let denied_body = to_bytes(denied_call.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let denied_value: serde_json::Value = serde_json::from_slice(&denied_body).unwrap();
        assert_eq!(denied_value["result"]["structuredContent"]["code"], "permission_denied");
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 0);

        let denied_envelope = denied_value["result"]["structuredContent"].clone();
        for (index, arguments) in [
            serde_json::json!({}),
            serde_json::json!({"cluster": null}),
            serde_json::json!({"cluster": 7}),
            serde_json::json!({"cluster": ""}),
            serde_json::json!({"cluster": "cluster-a", "unknown": true}),
            serde_json::json!({"cluster": "cluster-a", "topic": null}),
            serde_json::json!({"cluster": "cluster-a", "topic": "x".repeat(512)}),
            serde_json::json!({"cluster": "cluster-a", "topic": "orders\nignore"}),
            serde_json::json!({"cluster": "cluster-a", "topic": "<tool>ignore</tool>"}),
        ]
        .into_iter()
        .enumerate()
        {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 30 + index,
                "method": "tools/call",
                "params": {
                    "name": crate::tools::UPSERT_TOPIC_TOOL,
                    "arguments": arguments,
                }
            });
            let response = router
                .clone()
                .oneshot(request("/mcp", Body::from(body.to_string()), Some(&denied)))
                .await
                .unwrap();
            let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["result"]["structuredContent"], denied_envelope);
        }
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 0);

        let call = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(format!(
                    r#"{{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{{"name":"rocketmq_upsert_topic","arguments":{{"schema_version":"{}","cluster":"cluster-a","topic":"orders","broker_names":["broker-a"],"read_queue_nums":8,"write_queue_nums":8,"perm":6,"order":false,"message_type":"NORMAL"}}}}}}"#,
                    crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION
                )),
                Some(&both),
            ))
            .await
            .unwrap();
        let call_body = to_bytes(call.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&call_body).unwrap();
        assert_eq!(value["result"]["isError"], false);
        assert_eq!(
            value["result"]["structuredContent"]["schema_version"],
            crate::tools::MUTATION_RESULT_SCHEMA_VERSION
        );
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 1);

        let group_call = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(format!(
                    r#"{{"jsonrpc":"2.0","id":25,"method":"tools/call","params":{{"name":"rocketmq_upsert_consumer_group","arguments":{{"schema_version":"{}","cluster":"cluster-a","consumer_group":"orders_consumers","broker_names":["broker-a"],"consume_enable":true,"consume_from_min_enable":false,"consume_broadcast_enable":false,"consume_message_orderly":false,"retry_queue_nums":1,"retry_max_times":16,"broker_id":0,"which_broker_when_consume_slowly":1,"notify_consumer_ids_changed_enable":true,"group_sys_flag":0,"consume_timeout_minute":15}}}}}}"#,
                    crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION
                )),
                Some(&both),
            ))
            .await
            .unwrap();
        let group_body = to_bytes(group_call.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let group_value: serde_json::Value = serde_json::from_slice(&group_body).unwrap();
        assert_eq!(group_value["result"]["isError"], false);
        assert_eq!(
            group_value["result"]["structuredContent"]["operation"],
            "consumer_group_upsert"
        );
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(counters.preflights.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(counters.executes.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(counters.shutdowns.load(std::sync::atomic::Ordering::SeqCst), 2);

        let execute_topic = router
            .clone()
            .oneshot(request(
                "/mcp",
                Body::from(format!(
                    r#"{{"jsonrpc":"2.0","id":26,"method":"tools/call","params":{{"name":"rocketmq_upsert_topic","arguments":{{"schema_version":"{}","cluster":"cluster-a","topic":"orders","broker_names":["broker-a"],"read_queue_nums":8,"write_queue_nums":8,"perm":6,"order":false,"message_type":"NORMAL","dry_run":false,"confirm":true,"reason":"approved rollout"}}}}}}"#,
                    crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION
                )),
                Some(&both),
            ))
            .await
            .unwrap();
        let execute_topic_body = to_bytes(execute_topic.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let execute_topic_value: serde_json::Value = serde_json::from_slice(&execute_topic_body).unwrap();
        assert_eq!(execute_topic_value["result"]["isError"], false);
        assert_eq!(execute_topic_value["result"]["structuredContent"]["status"], "applied");
        assert_eq!(
            execute_topic_value["result"]["structuredContent"]["targets"][0]["target"]["broker_name"],
            "broker-a"
        );

        let execute_group = router
            .oneshot(request(
                "/mcp",
                Body::from(format!(
                    r#"{{"jsonrpc":"2.0","id":27,"method":"tools/call","params":{{"name":"rocketmq_upsert_consumer_group","arguments":{{"schema_version":"{}","cluster":"cluster-a","consumer_group":"orders_consumers","broker_names":["broker-a"],"consume_enable":true,"consume_from_min_enable":false,"consume_broadcast_enable":false,"consume_message_orderly":false,"retry_queue_nums":1,"retry_max_times":16,"broker_id":0,"which_broker_when_consume_slowly":1,"notify_consumer_ids_changed_enable":true,"group_sys_flag":0,"consume_timeout_minute":15,"dry_run":false,"confirm":true,"reason":"approved rollout"}}}}}}"#,
                    crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION
                )),
                Some(&both),
            ))
            .await
            .unwrap();
        let execute_group_body = to_bytes(execute_group.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let execute_group_value: serde_json::Value = serde_json::from_slice(&execute_group_body).unwrap();
        assert_eq!(execute_group_value["result"]["isError"], false);
        assert_eq!(execute_group_value["result"]["structuredContent"]["status"], "applied");
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(counters.preflights.load(std::sync::atomic::Ordering::SeqCst), 6);
        assert_eq!(counters.executes.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[cfg(feature = "write-tools")]
    #[tokio::test]
    async fn authenticated_upserts_preserve_complete_contract_for_dry_run_execute_and_cache_hit() {
        let (router, counters, sink) = write_router().await;
        let token = token_with_claims(
            REQUIRED_WRITE_SCOPE,
            vec!["topic_upsert", "consumer_group_upsert"],
            vec!["cluster-a"],
        );

        let topic_requested = serde_json::json!({
            "read_queue_nums": 8,
            "write_queue_nums": 8,
            "perm": 6,
            "order": false,
            "message_type": "NORMAL"
        });
        let topic_dry_arguments = topic_arguments("orders", vec!["broker-b".to_owned(), "broker-a".to_owned()]);
        let topic_dry = authenticated_tool_call(
            &router,
            &token,
            300,
            crate::tools::UPSERT_TOPIC_TOOL,
            topic_dry_arguments.clone(),
        )
        .await;
        assert_eq!(
            topic_dry["result"]["structuredContent"],
            expected_upsert_response("topic_upsert", "topic", "orders", topic_requested.clone(), false)
        );
        let mut topic_execute_arguments = topic_dry_arguments;
        let topic_object = topic_execute_arguments.as_object_mut().unwrap();
        topic_object.insert("dry_run".to_owned(), serde_json::json!(false));
        topic_object.insert("confirm".to_owned(), serde_json::json!(true));
        topic_object.insert("reason".to_owned(), serde_json::json!("approved topic rollout"));
        topic_object.insert("request_key".to_owned(), serde_json::json!("topic-request-0001"));
        let topic_execute = authenticated_tool_call(
            &router,
            &token,
            301,
            crate::tools::UPSERT_TOPIC_TOOL,
            topic_execute_arguments.clone(),
        )
        .await;
        let expected_topic_execute = expected_upsert_response("topic_upsert", "topic", "orders", topic_requested, true);
        assert_eq!(topic_execute["result"]["structuredContent"], expected_topic_execute);
        let topic_cache_hit = authenticated_tool_call(
            &router,
            &token,
            302,
            crate::tools::UPSERT_TOPIC_TOOL,
            topic_execute_arguments,
        )
        .await;
        assert_eq!(topic_cache_hit["result"]["structuredContent"], expected_topic_execute);

        let group_requested = serde_json::json!({
            "consume_enable": true,
            "consume_from_min_enable": false,
            "consume_broadcast_enable": false,
            "consume_message_orderly": false,
            "retry_queue_nums": 1,
            "retry_max_times": 16,
            "broker_id": 0,
            "which_broker_when_consume_slowly": 1,
            "notify_consumer_ids_changed_enable": true,
            "group_sys_flag": 0,
            "consume_timeout_minute": 15
        });
        let group_dry_arguments =
            consumer_group_arguments("orders_consumers", vec!["broker-b".to_owned(), "broker-a".to_owned()]);
        let group_dry = authenticated_tool_call(
            &router,
            &token,
            303,
            crate::tools::UPSERT_CONSUMER_GROUP_TOOL,
            group_dry_arguments.clone(),
        )
        .await;
        assert_eq!(
            group_dry["result"]["structuredContent"],
            expected_upsert_response(
                "consumer_group_upsert",
                "consumer_group",
                "orders_consumers",
                group_requested.clone(),
                false,
            )
        );
        let mut group_execute_arguments = group_dry_arguments;
        let group_object = group_execute_arguments.as_object_mut().unwrap();
        group_object.insert("dry_run".to_owned(), serde_json::json!(false));
        group_object.insert("confirm".to_owned(), serde_json::json!(true));
        group_object.insert("reason".to_owned(), serde_json::json!("approved group rollout"));
        group_object.insert("request_key".to_owned(), serde_json::json!("group-request-0001"));
        let group_execute = authenticated_tool_call(
            &router,
            &token,
            304,
            crate::tools::UPSERT_CONSUMER_GROUP_TOOL,
            group_execute_arguments.clone(),
        )
        .await;
        let expected_group_execute = expected_upsert_response(
            "consumer_group_upsert",
            "consumer_group",
            "orders_consumers",
            group_requested,
            true,
        );
        assert_eq!(group_execute["result"]["structuredContent"], expected_group_execute);
        let group_cache_hit = authenticated_tool_call(
            &router,
            &token,
            305,
            crate::tools::UPSERT_CONSUMER_GROUP_TOOL,
            group_execute_arguments,
        )
        .await;
        assert_eq!(group_cache_hit["result"]["structuredContent"], expected_group_execute);

        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(counters.preflights.load(std::sync::atomic::Ordering::SeqCst), 6);
        assert_eq!(counters.executes.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(sink.records().await.unwrap().len(), 12);
    }

    #[cfg(feature = "write-tools")]
    #[tokio::test]
    async fn authenticated_closed_name_and_target_limits_run_before_audit_and_session() {
        let (router, counters, sink) = write_router().await;
        let token = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"]);
        let system_topics = [
            "TBW102",
            "SCHEDULE_TOPIC_XXXX",
            "BenchmarkTest",
            "RMQ_SYS_TRANS_HALF_TOPIC",
            "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC",
            "RMQ_SYS_TRACE_TOPIC",
            "RMQ_SYS_TRANS_OP_HALF_TOPIC",
            "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC",
            "TRANS_CHECK_MAX_TIME_TOPIC",
            "SELF_TEST_TOPIC",
            "OFFSET_MOVED_EVENT",
            "CHECKPOINT_TOPIC",
        ];
        let mut invalid = system_topics
            .into_iter()
            .map(|topic| topic_arguments(topic, vec!["broker-a".to_owned()]))
            .collect::<Vec<_>>();
        invalid.extend([
            topic_arguments(&"a".repeat(128), vec!["broker-a".to_owned()]),
            topic_arguments("10.0.0.1", vec!["broker-a".to_owned()]),
            topic_arguments("10%2e0%2e0%2e1", vec!["broker-a".to_owned()]),
            topic_arguments("token=secret", vec!["broker-a".to_owned()]),
            topic_arguments("token%3dsecret", vec!["broker-a".to_owned()]),
            topic_arguments("orders", vec!["broker-a".to_owned(), "broker-a".to_owned()]),
            topic_arguments("orders", vec!["10.0.0.1".to_owned()]),
            topic_arguments("orders", vec!["token%3dsecret".to_owned()]),
            topic_arguments("orders", (0..65).map(|index| format!("broker-{index:02}")).collect()),
        ]);
        for (index, arguments) in invalid.into_iter().enumerate() {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 100 + index,
                "method": "tools/call",
                "params": {"name": crate::tools::UPSERT_TOPIC_TOOL, "arguments": arguments}
            });
            let response = router
                .clone()
                .oneshot(request("/mcp", Body::from(body.to_string()), Some(&token)))
                .await
                .unwrap();
            let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                value["result"]["structuredContent"]["code"], "invalid_arguments",
                "case {index}"
            );
        }
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(counters.preflights.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(counters.executes.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(sink.records().await.unwrap().is_empty());

        let group_token = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["consumer_group_upsert"], vec!["cluster-a"]);
        for (index, consumer_group) in [
            "DEFAULT_CONSUMER",
            "TOOLS_CONSUMER",
            "SCHEDULE_CONSUMER",
            "FILTERSRV_CONSUMER",
            "__MONITOR_CONSUMER",
            "SELF_TEST_C_GROUP",
            "CID_ONS-HTTP-PROXY",
            "CID_ONSAPI_PULL",
            "CID_ONSAPI_PERMISSION",
            "CID_ONSAPI_OWNER",
            "CID_RMQ_SYS_TRANS",
            "CID_RMQ_SYS_INTERNAL",
            "CID_DefaultHeartBeatSyncerTopic",
            "%SYS%INTERNAL",
        ]
        .into_iter()
        .enumerate()
        {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 150 + index,
                "method": "tools/call",
                "params": {
                    "name": crate::tools::UPSERT_CONSUMER_GROUP_TOOL,
                    "arguments": consumer_group_arguments(consumer_group, vec!["broker-a".to_owned()])
                }
            });
            let response = router
                .clone()
                .oneshot(request("/mcp", Body::from(body.to_string()), Some(&group_token)))
                .await
                .unwrap();
            let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                value["result"]["structuredContent"]["code"], "invalid_arguments",
                "protected group case {index}"
            );
        }
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(counters.preflights.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(counters.executes.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(sink.records().await.unwrap().is_empty());

        for (index, arguments) in [
            topic_arguments(&"a".repeat(127), vec!["broker-a".to_owned()]),
            topic_arguments("orders", (0..64).map(|item| format!("broker-{item:02}")).collect()),
        ]
        .into_iter()
        .enumerate()
        {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 200 + index,
                "method": "tools/call",
                "params": {"name": crate::tools::UPSERT_TOPIC_TOOL, "arguments": arguments}
            });
            let response = router
                .clone()
                .oneshot(request("/mcp", Body::from(body.to_string()), Some(&token)))
                .await
                .unwrap();
            let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["result"]["isError"], false);
        }
        assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(counters.executes.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(counters.shutdowns.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(sink.records().await.unwrap().len(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn authenticated_mcp_request_timeout_does_not_use_a_dummy_route() {
        let router = router_with_server(
            ControlServer::new(false).with_response_delay(HTTP_REQUEST_TIMEOUT + Duration::from_secs(1)),
        )
        .await;
        let write_token = token(REQUIRED_WRITE_SCOPE);
        let response = router
            .oneshot(request(
                "/mcp",
                Body::from(r#"{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}"#),
                Some(&write_token),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn hostile_host_and_origin_are_rejected_before_oauth_without_echo_or_logs() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let router = router().await;
        let hostile_host = "token%3Dsecret.example.test";
        let hostile_origin = "https://evil.example.test/%31%32%37%2e%30%2e%30%2e%31";
        let make_request = |authorization: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(HOST, hostile_host)
                .header(ORIGIN, hostile_origin)
                .header("content-type", "application/json");
            if let Some(authorization) = authorization {
                builder = builder.header(AUTHORIZATION, authorization);
            }
            builder.body(Body::from("not-json")).unwrap()
        };
        let first = router.clone().oneshot(make_request(None)).await.unwrap();
        let second = router
            .oneshot(make_request(Some("Bearer attacker-controlled")))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::BAD_REQUEST);
        assert_eq!(second.status(), StatusCode::BAD_REQUEST);
        let first_body = to_bytes(first.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        let second_body = to_bytes(second.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
        assert_eq!(first_body, second_body);
        let body = String::from_utf8(first_body.to_vec()).unwrap();
        let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        for forbidden in [hostile_host, hostile_origin, "token=secret", "127.0.0.1"] {
            assert!(!body.contains(forbidden));
            assert!(!captured.contains(forbidden));
        }
    }

    #[tokio::test]
    async fn tls_listener_accepts_https_and_rejects_plaintext() {
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
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-tls-test");
        let context = runtime.service_context("mcp-control-tls-test");
        let tls = TlsServerRuntime::initialize_with_service_context(tls_config(&config), &context)
            .await
            .unwrap();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(
                HttpsListener { tcp, tls },
                Router::new().route("/health", get(|| async { "ok" })),
            )
            .with_graceful_shutdown(server_cancellation.cancelled_owned())
            .await
        });
        let https = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(format!("https://127.0.0.1:{}/health", address.port()))
            .send()
            .await
            .unwrap();
        assert_eq!(https.status(), reqwest::StatusCode::OK);
        let plaintext = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(format!("http://127.0.0.1:{}/health", address.port()))
            .send()
            .await;
        assert!(plaintext.is_err());
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
