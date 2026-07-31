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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::Issuer;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use rocketmq_sre_model_gateway::FinishReason;
use rocketmq_sre_model_gateway::GrpcProviderSpiClient;
use rocketmq_sre_model_gateway::GrpcSpiClientTlsConfig;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ModelStreamEvent;
use rocketmq_sre_model_gateway::PROVIDER_SPI_WIRE_VERSION;
use rocketmq_sre_model_gateway::ProviderCapabilities;
use rocketmq_sre_model_gateway::ProviderCapability;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::ProviderHealth;
use rocketmq_sre_model_gateway::bounded_provider_adapter_service;
use rocketmq_sre_model_gateway::provider_spi_wire as wire;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::Stream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Certificate;
use tonic::transport::Identity;
use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;

const GATEWAY_IDENTITY: &str = "spiffe://rocketmq-sre/control-plane";
const ADAPTER_IDENTITY: &str = "spiffe://rocketmq-sre/provider/mock";
const SERVER_NAME: &str = "provider.test";

#[derive(Clone, Default)]
struct MockProviderAdapter {
    cancelled: Arc<Mutex<BTreeSet<String>>>,
}

impl MockProviderAdapter {
    fn cancelled(&self, invocation_id: &str) -> bool {
        self.cancelled
            .lock()
            .map(|cancelled| cancelled.contains(invocation_id))
            .unwrap_or(false)
    }
}

impl wire::provider_adapter_server::ProviderAdapter for MockProviderAdapter {
    fn handshake<'life0, 'async_trait>(
        &'life0 self,
        request: Request<wire::HandshakeRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<wire::HandshakeResponse>, Status>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let request = request.into_inner();
            if !request.gateway_identity.starts_with("spiffe://") || request.max_payload_bytes == 0 {
                return Err(Status::permission_denied("invalid gateway identity"));
            }
            let wire_version = if request.gateway_identity.ends_with("/bad-version") {
                "rocketmq-sre.provider-spi.v999"
            } else {
                PROVIDER_SPI_WIRE_VERSION
            };
            let capabilities = ProviderCapabilities::chat_default().with([ProviderCapability::JsonSchema]);
            Ok(Response::new(wire::HandshakeResponse {
                wire_version: wire_version.to_owned(),
                adapter_identity: ADAPTER_IDENTITY.to_owned(),
                credential_owner: "adapter".to_owned(),
                capabilities_json: serde_json::to_vec(&capabilities).map_err(|_| Status::internal("capabilities"))?,
                credential_version_fingerprint: "mock-key-v1".to_owned(),
            }))
        })
    }

    fn invoke<'life0, 'async_trait>(
        &'life0 self,
        request: Request<wire::InvokeRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<wire::InvokeResponse>, Status>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let request = request.into_inner();
            let canonical: CanonicalModelRequest = serde_json::from_slice(&request.canonical_request_json)
                .map_err(|_| Status::invalid_argument("canonical request"))?;
            if canonical.model == "wire-error" {
                return Ok(Response::new(wire::InvokeResponse {
                    canonical_response_json: Vec::new(),
                    error: Some(wire::ProviderError {
                        code: "rate_limited".to_owned(),
                        message: "adapter internal credential detail".to_owned(),
                        retryable: true,
                    }),
                }));
            }
            let response =
                CanonicalModelResponse::text("mock-spi", canonical.model, "bounded response", FinishReason::Stop);
            Ok(Response::new(wire::InvokeResponse {
                canonical_response_json: serde_json::to_vec(&response).map_err(|_| Status::internal("response"))?,
                error: None,
            }))
        })
    }

    type InvokeStreamStream = Pin<Box<dyn Stream<Item = Result<wire::StreamEvent, Status>> + Send>>;

    fn invoke_stream<'life0, 'async_trait>(
        &'life0 self,
        request: Request<wire::InvokeRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<Self::InvokeStreamStream>, Status>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let request = request.into_inner();
            if request.max_stream_events == 0 || request.max_stream_bytes == 0 {
                return Err(Status::invalid_argument("stream bounds"));
            }
            let event = ModelStreamEvent::Finish {
                reason: FinishReason::Stop,
            };
            let stream: Self::InvokeStreamStream = Box::pin(tokio_stream::iter([Ok(wire::StreamEvent {
                canonical_event_json: serde_json::to_vec(&event).map_err(|_| Status::internal("event"))?,
                error: None,
            })]));
            Ok(Response::new(stream))
        })
    }

    fn cancel<'life0, 'async_trait>(
        &'life0 self,
        request: Request<wire::CancelRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<wire::CancelResponse>, Status>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let request = request.into_inner();
            self.cancelled
                .lock()
                .map_err(|_| Status::unavailable("cancel state"))?
                .insert(request.invocation_id);
            Ok(Response::new(wire::CancelResponse {}))
        })
    }

    fn health<'life0, 'async_trait>(
        &'life0 self,
        _request: Request<wire::HealthRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<wire::HealthResponse>, Status>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            Ok(Response::new(wire::HealthResponse {
                status: "healthy".to_owned(),
                credential_version_fingerprint: "mock-key-v1".to_owned(),
            }))
        })
    }
}

struct TlsFixture {
    ca_certificate_pem: String,
    server_certificate_pem: String,
    server_private_key_pem: String,
    client_certificate_pem: String,
    client_private_key_pem: String,
    untrusted_client_certificate_pem: String,
    untrusted_client_private_key_pem: String,
}

#[tokio::test]
async fn process_external_spi_enforces_mtls_version_health_cancel_and_error_contracts() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = tls_fixture();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let incoming = TcpListenerStream::new(listener);
    let adapter = MockProviderAdapter::default();
    let service = bounded_provider_adapter_service(adapter.clone(), 64 * 1024);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            tls.server_certificate_pem.clone(),
            tls.server_private_key_pem.clone(),
        ))
        .client_ca_root(Certificate::from_pem(tls.ca_certificate_pem.clone()));
    let server = Server::builder()
        .tls_config(server_tls)
        .expect("server TLS")
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, async {
            let _ = shutdown_rx.await;
        });
    let server_task = tokio::spawn(server);
    let endpoint = format!("https://{address}");
    let valid_config = client_config(
        &tls,
        tls.client_certificate_pem.clone(),
        tls.client_private_key_pem.clone(),
        GATEWAY_IDENTITY,
        ADAPTER_IDENTITY,
    );
    let mut client = GrpcProviderSpiClient::connect(&endpoint, valid_config)
        .await
        .expect("mTLS SPI handshake");

    assert_eq!(client.gateway_identity(), GATEWAY_IDENTITY);
    assert_eq!(client.adapter_identity(), ADAPTER_IDENTITY);
    assert!(
        client
            .capabilities()
            .supported
            .contains(&ProviderCapability::JsonSchema)
    );
    assert_eq!(client.credential_version_fingerprint(), Some("mock-key-v1"));

    let correlation_id = CorrelationId::new();
    let context = InvocationContext::new(correlation_id);
    let response = client
        .invoke(&context, &request(correlation_id, "mock-model"))
        .await
        .expect("unary invocation");
    assert_eq!(response.content, "bounded response");

    let mut stream = client
        .invoke_stream(&context, &request(correlation_id, "mock-model"))
        .await
        .expect("stream invocation");
    assert!(matches!(
        stream.message().await.expect("stream event"),
        Some(ModelStreamEvent::Finish {
            reason: FinishReason::Stop
        })
    ));
    assert!(stream.message().await.expect("stream completed").is_none());

    let health = client.health().await.expect("health");
    assert_eq!(health.status, ProviderHealth::Healthy);
    assert_eq!(health.credential_version_fingerprint.as_deref(), Some("mock-key-v1"));

    client.cancel("spi-cancel-test", correlation_id).await.expect("cancel");
    assert!(adapter.cancelled("spi-cancel-test"));

    let error = client
        .invoke(&context, &request(correlation_id, "wire-error"))
        .await
        .expect_err("wire error");
    assert_eq!(error.code, ProviderErrorCode::RateLimited);
    assert!(!error.message.contains("credential"));

    let bad_version = client_config(
        &tls,
        tls.client_certificate_pem.clone(),
        tls.client_private_key_pem.clone(),
        "spiffe://rocketmq-sre/bad-version",
        ADAPTER_IDENTITY,
    );
    assert_eq!(
        GrpcProviderSpiClient::connect(&endpoint, bad_version)
            .await
            .expect_err("version mismatch")
            .code,
        ProviderErrorCode::UnsupportedWireVersion
    );

    let untrusted = client_config(
        &tls,
        tls.untrusted_client_certificate_pem.clone(),
        tls.untrusted_client_private_key_pem.clone(),
        GATEWAY_IDENTITY,
        ADAPTER_IDENTITY,
    );
    assert_eq!(
        GrpcProviderSpiClient::connect(&endpoint, untrusted)
            .await
            .expect_err("untrusted client certificate")
            .code,
        ProviderErrorCode::MutualTlsFailed
    );

    shutdown_tx.send(()).expect("shutdown signal");
    server_task.await.expect("server task").expect("server shutdown");
}

fn request(correlation_id: CorrelationId, model: &str) -> CanonicalModelRequest {
    CanonicalModelRequest::new(
        correlation_id,
        model,
        vec![ModelMessage::text(ModelRole::User, "bounded prompt")],
    )
}

fn client_config(
    tls: &TlsFixture,
    certificate: String,
    private_key: String,
    gateway_identity: &str,
    adapter_identity: &str,
) -> GrpcSpiClientTlsConfig {
    GrpcSpiClientTlsConfig::mutual_tls(
        tls.ca_certificate_pem.clone(),
        certificate,
        private_key,
        SERVER_NAME,
        gateway_identity,
        adapter_identity,
    )
    .with_timeouts(Duration::from_secs(2), Duration::from_secs(2))
    .with_max_payload_bytes(64 * 1024)
}

fn tls_fixture() -> TlsFixture {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("CA key");
    let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");
    let issuer = Issuer::new(ca_params, ca_key);
    let (server_certificate_pem, server_private_key_pem) =
        signed_leaf(&issuer, SERVER_NAME, ExtendedKeyUsagePurpose::ServerAuth);
    let (client_certificate_pem, client_private_key_pem) =
        signed_leaf(&issuer, "gateway.test", ExtendedKeyUsagePurpose::ClientAuth);
    let untrusted =
        rcgen::generate_simple_self_signed(vec!["intruder.test".to_owned()]).expect("untrusted client certificate");

    TlsFixture {
        ca_certificate_pem: ca_certificate.pem(),
        server_certificate_pem,
        server_private_key_pem,
        client_certificate_pem,
        client_private_key_pem,
        untrusted_client_certificate_pem: untrusted.cert.pem(),
        untrusted_client_private_key_pem: untrusted.signing_key.serialize_pem(),
    }
}

fn signed_leaf(issuer: &Issuer<'_, KeyPair>, name: &str, usage: ExtendedKeyUsagePurpose) -> (String, String) {
    let mut params = CertificateParams::new(vec![name.to_owned()]).expect("leaf params");
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate().expect("leaf key");
    let certificate = params.signed_by(&key, issuer).expect("signed leaf");
    (certificate.pem(), key.serialize_pem())
}
