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

#![recursion_limit = "256"]

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use hmac::digest::KeyInit;
use hmac::Hmac;
use hmac::Mac;
#[cfg(feature = "tls")]
use rcgen::BasicConstraints;
#[cfg(feature = "tls")]
use rcgen::CertificateParams;
#[cfg(feature = "tls")]
use rcgen::DnType;
#[cfg(feature = "tls")]
use rcgen::ExtendedKeyUsagePurpose;
#[cfg(feature = "tls")]
use rcgen::IsCa;
#[cfg(feature = "tls")]
use rcgen::Issuer;
#[cfg(feature = "tls")]
use rcgen::KeyPair;
#[cfg(feature = "tls")]
use rcgen::KeyUsagePurpose;
use rocketmq_model::result::SendStatus;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_proxy::v2;
use rocketmq_proxy::v2::messaging_service_client::MessagingServiceClient;
use rocketmq_proxy::AckMessageRequest;
use rocketmq_proxy::AckMessageResultEntry;
use rocketmq_proxy::ChangeInvisibleDurationPlan;
use rocketmq_proxy::ChangeInvisibleDurationRequest;
use rocketmq_proxy::ClusterServiceManager;
use rocketmq_proxy::ConsumerService;
use rocketmq_proxy::DefaultAssignmentService;
use rocketmq_proxy::DefaultConsumerService;
use rocketmq_proxy::DefaultMessageService;
use rocketmq_proxy::DefaultTransactionService;
use rocketmq_proxy::ForwardMessageToDeadLetterQueuePlan;
use rocketmq_proxy::ForwardMessageToDeadLetterQueueRequest;
use rocketmq_proxy::GetOffsetPlan;
use rocketmq_proxy::GetOffsetRequest;
use rocketmq_proxy::GrpcConfig;
#[cfg(feature = "tls")]
use rocketmq_proxy::GrpcTlsClientAuth;
#[cfg(feature = "tls")]
use rocketmq_proxy::GrpcTlsConfig;
use rocketmq_proxy::MetadataService;
use rocketmq_proxy::ProxyAuthConfig;
use rocketmq_proxy::ProxyAuthRuntime;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyError;
use rocketmq_proxy::ProxyPayloadStatus;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_proxy::ProxyTopicMessageType;
use rocketmq_proxy::PullMessagePlan;
use rocketmq_proxy::PullMessageRequest;
use rocketmq_proxy::QueryOffsetPlan;
use rocketmq_proxy::QueryOffsetRequest;
use rocketmq_proxy::ReceiveMessagePlan;
use rocketmq_proxy::ReceiveMessageRequest;
use rocketmq_proxy::ReceivedMessage;
use rocketmq_proxy::ResolvedEndpoint;
use rocketmq_proxy::ResourceIdentity;
use rocketmq_proxy::RouteService;
use rocketmq_proxy::StaticMessageService;
use rocketmq_proxy::StaticMetadataService;
use rocketmq_proxy::StaticRouteService;
use rocketmq_proxy::SubscriptionGroupMetadata;
use rocketmq_proxy::UpdateOffsetPlan;
use rocketmq_proxy::UpdateOffsetRequest;
use rocketmq_proxy_core::ProxyContext;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ProxyMessageExt;
use rocketmq_proxy_core::ProxyServiceFuture;
use rocketmq_runtime::RuntimeContext;
use sha1::Sha1;
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;
#[cfg(feature = "tls")]
use tonic::transport::Certificate;
#[cfg(feature = "tls")]
use tonic::transport::ClientTlsConfig;
#[cfg(feature = "tls")]
use tonic::transport::Endpoint;
#[cfg(feature = "tls")]
use tonic::transport::Identity;
use tonic::Request;

type HmacSha1 = Hmac<Sha1>;
const AUTH_TEST_DATETIME: &str = "20231227T194619Z";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedRouteContext {
    local_addr: Option<String>,
    remote_addr: Option<String>,
}

#[derive(Default)]
struct RecordingRouteService {
    observed: Mutex<Vec<ObservedRouteContext>>,
}

impl RecordingRouteService {
    fn observed(&self) -> Vec<ObservedRouteContext> {
        self.observed.lock().expect("route service mutex poisoned").clone()
    }
}

impl RouteService for RecordingRouteService {
    fn query_route<'a>(
        &'a self,
        context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, TopicRouteData> {
        Box::pin(async move {
            self.observed
                .lock()
                .expect("route service mutex poisoned")
                .push(ObservedRouteContext {
                    local_addr: context.local_addr().map(str::to_owned),
                    remote_addr: context.remote_addr().map(str::to_owned),
                });
            Ok(TopicRouteData::default())
        })
    }
}

#[derive(Default)]
struct NormalMetadataService;

impl MetadataService for NormalMetadataService {
    fn topic_message_type<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, ProxyTopicMessageType> {
        Box::pin(async { Ok(ProxyTopicMessageType::Normal) })
    }

    fn subscription_group<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
        _group: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Default)]
struct StreamingConsumerService;

impl ConsumerService for StreamingConsumerService {
    fn receive_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan> {
        Box::pin(async move {
            let mut payload = ProxyMessage::new(request.target.topic.to_string(), b"integration-body".to_vec());
            payload.put_property("POP_CK", "integration-receipt-handle");
            let message = ProxyMessageExt {
                message: payload,
                queue_id: request.target.queue_id,
                queue_offset: 7,
                msg_id: "integration-msg-id".to_owned(),
                ..ProxyMessageExt::default()
            };

            Ok(ReceiveMessagePlan {
                status: ProxyPayloadStatus::new(v2::Code::Ok as i32, "OK"),
                delivery_timestamp_ms: Some(1_710_000_000_000),
                messages: vec![ReceivedMessage {
                    message,
                    invisible_duration: Duration::from_secs(30),
                }],
            })
        })
    }

    fn pull_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan> {
        Box::pin(async { Err(ProxyError::not_implemented("integration pull")) })
    }

    fn ack_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a AckMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<AckMessageResultEntry>> {
        Box::pin(async { Err(ProxyError::not_implemented("integration ack")) })
    }

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
        Box::pin(async { Err(ProxyError::not_implemented("integration dlq")) })
    }

    fn change_invisible_duration<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("integration change invisible")) })
    }

    fn update_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("integration update offset")) })
    }

    fn get_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("integration get offset")) })
    }

    fn query_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("integration query offset")) })
    }
}

#[tokio::test]
async fn query_route_integration_injects_transport_context() {
    let route_service = Arc::new(RecordingRouteService::default());
    let (listen_addr, shutdown_tx, server_task) = spawn_runtime(Arc::new(ClusterServiceManager::with_services(
        route_service.clone(),
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    )))
    .await;
    let mut client = connect_with_retry(listen_addr).await;

    let response = client
        .query_route(route_request("TopicA"))
        .await
        .expect("query route should succeed")
        .into_inner();
    assert_eq!(
        response.status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );

    let observed = route_service.observed();
    let expected_local_addr = listen_addr.to_string();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].local_addr.as_deref(), Some(expected_local_addr.as_str()));
    assert!(
        observed[0]
            .remote_addr
            .as_deref()
            .is_some_and(|remote| remote.starts_with("127.0.0.1:")),
        "expected remote address to be recorded, got {:?}",
        observed[0].remote_addr
    );
}

#[tokio::test]
async fn query_route_integration_enforces_auth_enabled_runtime() {
    let test_dir = std::env::temp_dir().join(format!("rocketmq-rust-proxy-auth-e2e-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&test_dir).expect("create proxy auth e2e test dir");
    let acl_file = test_dir.join("plain_acl.yml");
    fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: secret
    admin: true
"#,
    )
    .expect("write proxy auth e2e acl file");

    let runtime_context = RuntimeContext::from_current("proxy-grpc-auth-test");
    let auth_runtime = ProxyAuthRuntime::from_proxy_config(
        &ProxyAuthConfig {
            auth_config_path: test_dir.join("auth-store").to_string_lossy().into_owned(),
            acl_file: acl_file.to_string_lossy().into_owned(),
            authentication_enabled: true,
            authorization_enabled: true,
            ..ProxyAuthConfig::default()
        },
        &runtime_context.service_context("proxy-grpc-auth-runtime"),
    )
    .await
    .expect("proxy auth runtime should build")
    .expect("proxy auth runtime should be enabled");

    let route_service = Arc::new(RecordingRouteService::default());
    let service_manager = Arc::new(ClusterServiceManager::with_services(
        route_service.clone(),
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    ));
    let listen_addr = reserve_loopback_addr();
    let runtime = ProxyRuntime::builder(
        ProxyConfig {
            grpc: GrpcConfig {
                listen_addr: listen_addr.to_string(),
                ..GrpcConfig::default()
            },
            ..ProxyConfig::default()
        },
        runtime_context.service_context("proxy-grpc-auth"),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .with_service_manager(service_manager)
    .with_auth_runtime(auth_runtime)
    .build()
    .expect("the injected child context should build the proxy runtime");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut server_task = tokio::spawn(async move {
        runtime
            .serve_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    wait_for_server_ready(listen_addr, &mut server_task)
        .await
        .expect("proxy runtime should become ready");
    let mut client = connect_with_retry(listen_addr).await;

    let unauthorized = client
        .query_route(route_request("TopicA"))
        .await
        .expect("unauthenticated query route should return payload status")
        .into_inner();
    assert_eq!(
        unauthorized.status.as_ref().map(|status| status.code),
        Some(v2::Code::Unauthorized as i32)
    );

    let mut authorized_request = route_request("TopicA");
    apply_auth_headers(&mut authorized_request, "alice", "secret");
    let authorized = client
        .query_route(authorized_request)
        .await
        .expect("authenticated query route should succeed")
        .into_inner();
    assert_eq!(
        authorized.status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );
    assert_eq!(route_service.observed().len(), 1);

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "proxy runtime should shut down cleanly: {serve_result:?}"
    );
    let _ = fs::remove_dir_all(test_dir);
}

#[tokio::test]
async fn query_route_integration_rejects_invalid_grpc_timeout_before_business_logic() {
    let route_service = Arc::new(RecordingRouteService::default());
    let (listen_addr, shutdown_tx, server_task) = spawn_runtime(Arc::new(ClusterServiceManager::with_services(
        route_service.clone(),
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    )))
    .await;
    let mut client = connect_with_retry(listen_addr).await;

    let mut request = route_request("TopicA");
    request
        .metadata_mut()
        .insert("grpc-timeout", MetadataValue::from_static("bad-timeout"));
    let error = client
        .query_route(request)
        .await
        .expect_err("invalid timeout metadata should fail ingress");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("grpc-timeout"));

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );

    assert!(
        route_service.observed().is_empty(),
        "business route service should not run when ingress metadata is invalid",
    );
}

#[tokio::test]
async fn query_route_integration_keeps_topic_not_found_as_payload_status() {
    let (listen_addr, shutdown_tx, server_task) = spawn_runtime(Arc::new(ClusterServiceManager::with_services(
        Arc::new(StaticRouteService::default()),
        Arc::new(StaticMetadataService::default()),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    )))
    .await;
    let mut client = connect_with_retry(listen_addr).await;

    let response = client
        .query_route(route_request("MissingTopic"))
        .await
        .expect("business route failures should stay in payload")
        .into_inner();
    assert_eq!(
        response.status.as_ref().map(|status| status.code),
        Some(v2::Code::TopicNotFound as i32)
    );

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );
}

#[tokio::test]
async fn send_message_integration_returns_payload_entries() {
    let (listen_addr, shutdown_tx, server_task) = spawn_runtime(Arc::new(ClusterServiceManager::with_services(
        Arc::new(StaticRouteService::default()),
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    )))
    .await;
    let mut client = connect_with_retry(listen_addr).await;

    let response = client
        .send_message(send_message_request("TopicA", "msg-1"))
        .await
        .expect("sendMessage should succeed")
        .into_inner();
    assert_eq!(
        response.status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );
    assert_eq!(response.entries.len(), 1);
    assert_eq!(response.entries[0].message_id, "msg-1");
    assert_eq!(
        response.entries[0].status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );
}

#[tokio::test]
async fn receive_message_integration_streams_delivery_message_and_status() {
    let (listen_addr, shutdown_tx, server_task) = spawn_runtime(Arc::new(ClusterServiceManager::with_services(
        Arc::new(StaticRouteService::default()),
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(StreamingConsumerService),
        Arc::new(DefaultTransactionService),
    )))
    .await;
    let mut client = connect_with_retry(listen_addr).await;

    let stream = client
        .receive_message(receive_message_request("TopicA", "GroupA"))
        .await
        .expect("receiveMessage should succeed")
        .into_inner();
    let responses = stream.collect::<Vec<_>>().await;

    assert_eq!(responses.len(), 3);
    assert!(matches!(
        responses[0].as_ref().expect("delivery timestamp item").content,
        Some(v2::receive_message_response::Content::DeliveryTimestamp(_))
    ));
    match responses[1].as_ref().expect("message item").content.as_ref() {
        Some(v2::receive_message_response::Content::Message(message)) => {
            assert_eq!(message.body.as_ref(), b"integration-body");
            assert_eq!(
                message
                    .system_properties
                    .as_ref()
                    .and_then(|properties| properties.receipt_handle.as_deref()),
                Some("integration-receipt-handle")
            );
        }
        other => panic!("expected message item, got {other:?}"),
    }
    match responses[2].as_ref().expect("status item").content.as_ref() {
        Some(v2::receive_message_response::Content::Status(status)) => {
            assert_eq!(status.code, v2::Code::Ok as i32);
        }
        other => panic!("expected status item, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );
}

#[tokio::test]
async fn telemetry_integration_streams_settings_response() {
    let (listen_addr, shutdown_tx, server_task) = spawn_runtime(Arc::new(ClusterServiceManager::with_services(
        Arc::new(StaticRouteService::default()),
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    )))
    .await;
    let mut client = connect_with_retry(listen_addr).await;
    let command = v2::TelemetryCommand {
        status: None,
        command: Some(v2::telemetry_command::Command::Settings(v2::Settings {
            client_type: Some(v2::ClientType::Producer as i32),
            access_point: None,
            backoff_policy: None,
            request_timeout: None,
            pub_sub: Some(v2::settings::PubSub::Publishing(v2::Publishing {
                topics: Vec::new(),
                max_body_size: 0,
                validate_message_type: false,
            })),
            user_agent: None,
            metric: None,
        })),
    };
    let mut request = Request::new(tokio_stream::iter(vec![command]));
    request
        .metadata_mut()
        .insert("x-mq-client-id", MetadataValue::from_static("telemetry-client"));

    let mut response_stream = client
        .telemetry(request)
        .await
        .expect("telemetry stream should open")
        .into_inner();
    let response = tokio::time::timeout(Duration::from_secs(2), response_stream.message())
        .await
        .expect("telemetry response should arrive")
        .expect("telemetry stream should not fail")
        .expect("telemetry stream should produce one response");

    assert_eq!(
        response.status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );
    match response.command {
        Some(v2::telemetry_command::Command::Settings(settings)) => match settings.pub_sub {
            Some(v2::settings::PubSub::Publishing(publishing)) => {
                assert_eq!(publishing.max_body_size, 4 * 1024 * 1024);
                assert!(publishing.validate_message_type);
            }
            other => panic!("expected publishing settings, got {other:?}"),
        },
        other => panic!("expected settings response, got {other:?}"),
    }
    let end = tokio::time::timeout(Duration::from_secs(2), response_stream.message())
        .await
        .expect("telemetry stream should finish")
        .expect("telemetry stream should finish cleanly");
    assert!(end.is_none());

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );
}

#[tokio::test]
async fn spawn_runtime_retries_when_initial_candidate_port_is_occupied() {
    let occupied_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind occupied test port");
    let occupied_addr = occupied_listener.local_addr().expect("discover occupied test port");

    let fallback_probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fallback test port");
    let fallback_addr = fallback_probe.local_addr().expect("discover fallback test port");
    drop(fallback_probe);
    let route_service = Arc::new(RecordingRouteService::default());

    let (listen_addr, shutdown_tx, server_task) = spawn_runtime_with_candidates(
        Arc::new(ClusterServiceManager::with_services(
            route_service.clone(),
            Arc::new(NormalMetadataService),
            Arc::new(DefaultAssignmentService),
            Arc::new(DefaultMessageService),
            Arc::new(DefaultConsumerService),
            Arc::new(DefaultTransactionService),
        )),
        [occupied_addr, fallback_addr],
    )
    .await;

    assert_eq!(listen_addr, fallback_addr);

    let mut client = connect_with_retry(listen_addr).await;
    let response = client
        .query_route(route_request("TopicA"))
        .await
        .expect("query route should succeed on fallback address")
        .into_inner();
    assert_eq!(
        response.status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );
    assert_eq!(route_service.observed().len(), 1);

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );
}

#[tokio::test]
async fn proxy_runtime_shutdown_stops_injected_auth_acl_file_watcher() {
    let test_dir = std::env::temp_dir().join(format!("rocketmq-rust-proxy-auth-shutdown-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&test_dir).expect("create proxy auth shutdown test dir");
    let acl_file = test_dir.join("plain_acl.yml");
    fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
    )
    .expect("write initial proxy acl file");

    let runtime_context = RuntimeContext::from_current("proxy-grpc-auth-watcher-test");
    let auth_runtime = ProxyAuthRuntime::from_proxy_config(
        &ProxyAuthConfig {
            auth_config_path: test_dir.join("auth-store").to_string_lossy().into_owned(),
            acl_file: acl_file.to_string_lossy().into_owned(),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 20,
            authentication_enabled: true,
            ..ProxyAuthConfig::default()
        },
        &runtime_context.service_context("proxy-grpc-auth-watcher-runtime"),
    )
    .await
    .expect("proxy auth runtime should build")
    .expect("proxy auth runtime should be enabled");
    let observed_auth_runtime = auth_runtime.clone();

    let route_service = Arc::new(RecordingRouteService::default());
    let service_manager = Arc::new(ClusterServiceManager::with_services(
        route_service,
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    ));
    let listen_addr = reserve_loopback_addr();
    let runtime = ProxyRuntime::builder(
        ProxyConfig {
            grpc: GrpcConfig {
                listen_addr: listen_addr.to_string(),
                ..GrpcConfig::default()
            },
            ..ProxyConfig::default()
        },
        runtime_context.service_context("proxy-grpc-auth-watcher"),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .with_service_manager(service_manager)
    .with_auth_runtime(auth_runtime)
    .build()
    .expect("the injected child context should build the proxy runtime");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut server_task = tokio::spawn(async move {
        runtime
            .serve_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    wait_for_server_ready(listen_addr, &mut server_task)
        .await
        .expect("proxy runtime should become ready");

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "proxy runtime should shut down cleanly: {serve_result:?}"
    );

    let generation = observed_auth_runtime.acl_generation();
    let reload_attempts = observed_auth_runtime.auth_metrics_snapshot().acl_reload_attempts;
    fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: second
"#,
    )
    .expect("write changed proxy acl file after shutdown");
    tokio::time::sleep(Duration::from_millis(120)).await;

    assert_eq!(observed_auth_runtime.acl_generation(), generation);
    assert_eq!(
        observed_auth_runtime.auth_metrics_snapshot().acl_reload_attempts,
        reload_attempts,
        "proxy shutdown must stop the injected auth ACL watcher",
    );
    observed_auth_runtime
        .shutdown()
        .await
        .expect("auth runtime shutdown should be idempotent");
    let _ = fs::remove_dir_all(test_dir);
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn proxy_runtime_rejects_partial_grpc_tls_material_before_startup() {
    let runtime_context = RuntimeContext::from_current("proxy-grpc-partial-tls-test");
    let result = ProxyRuntime::builder(
        ProxyConfig {
            grpc: GrpcConfig {
                tls: GrpcTlsConfig {
                    enabled: true,
                    certificate_path: "server.pem".to_owned(),
                    ..GrpcTlsConfig::default()
                },
                ..GrpcConfig::default()
            },
            ..ProxyConfig::default()
        },
        runtime_context.service_context("proxy-grpc-partial-tls"),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .build();

    let error = match result {
        Ok(_) => panic!("certificate-only TLS must fail before the listener starts"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("privateKeyPath"), "{error}");
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn grpc_mtls_enforces_server_trust_and_client_identity() {
    let trusted_ca = TestCertificateAuthority::new("trusted-ca");
    let rogue_ca = TestCertificateAuthority::new("rogue-ca");
    let server = trusted_ca.server_identity("localhost");
    let trusted_client = trusted_ca.client_identity("trusted-client");
    let rogue_client = rogue_ca.client_identity("rogue-client");
    let directory = tempfile::tempdir().expect("TLS fixture directory");
    let certificate_path = directory.path().join("server.pem");
    let private_key_path = directory.path().join("server.key");
    let client_ca_path = directory.path().join("client-ca.pem");
    fs::write(&certificate_path, &server.certificate_pem).expect("server certificate");
    fs::write(&private_key_path, &server.private_key_pem).expect("server private key");
    fs::write(&client_ca_path, &trusted_ca.certificate_pem).expect("client CA");

    let route_service = Arc::new(RecordingRouteService::default());
    let listen_addr = reserve_loopback_addr();
    let (shutdown_tx, mut server_task) = spawn_runtime_on_addr_with_grpc_config(
        service_manager_with_route(route_service.clone()),
        GrpcConfig {
            listen_addr: listen_addr.to_string(),
            tls: GrpcTlsConfig {
                enabled: true,
                certificate_path: certificate_path.to_string_lossy().into_owned(),
                private_key_path: private_key_path.to_string_lossy().into_owned(),
                client_ca_path: Some(client_ca_path.to_string_lossy().into_owned()),
                client_auth: GrpcTlsClientAuth::Require,
                reload_interval_ms: 20,
                ..GrpcTlsConfig::default()
            },
            ..GrpcConfig::default()
        },
    );
    wait_for_server_ready(listen_addr, &mut server_task)
        .await
        .expect("mTLS listener should become ready");

    assert_tls_connection_rejected(listen_addr, &trusted_ca.certificate_pem, None).await;

    assert_tls_connection_rejected(
        listen_addr,
        &trusted_ca.certificate_pem,
        Some((&rogue_client.certificate_pem, &rogue_client.private_key_pem)),
    )
    .await;

    assert_tls_connection_rejected(
        listen_addr,
        &rogue_ca.certificate_pem,
        Some((&trusted_client.certificate_pem, &trusted_client.private_key_pem)),
    )
    .await;

    let mut client = connect_tls(
        listen_addr,
        &trusted_ca.certificate_pem,
        Some((&trusted_client.certificate_pem, &trusted_client.private_key_pem)),
    )
    .await
    .expect("trusted mTLS identity should connect");
    let response = client
        .query_route(route_request("TopicA"))
        .await
        .expect("trusted mTLS request should reach the Proxy")
        .into_inner();
    assert_eq!(
        response.status.as_ref().map(|status| status.code),
        Some(v2::Code::Ok as i32)
    );
    assert_eq!(route_service.observed().len(), 1);

    let _ = shutdown_tx.send(());
    assert!(server_task.await.expect("server task join").is_ok());
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn grpc_tls_reload_is_atomic_and_preserves_existing_connections() {
    let original_ca = TestCertificateAuthority::new("original-ca");
    let replacement_ca = TestCertificateAuthority::new("replacement-ca");
    let original = original_ca.server_identity("localhost");
    let replacement = replacement_ca.server_identity("localhost");
    let directory = tempfile::tempdir().expect("TLS rotation fixture directory");
    let certificate_path = directory.path().join("server.pem");
    let private_key_path = directory.path().join("server.key");
    fs::write(&certificate_path, &original.certificate_pem).expect("original certificate");
    fs::write(&private_key_path, &original.private_key_pem).expect("original private key");

    let route_service = Arc::new(RecordingRouteService::default());
    let listen_addr = reserve_loopback_addr();
    let (shutdown_tx, mut server_task) = spawn_runtime_on_addr_with_grpc_config(
        service_manager_with_route(route_service),
        GrpcConfig {
            listen_addr: listen_addr.to_string(),
            tls: GrpcTlsConfig {
                enabled: true,
                certificate_path: certificate_path.to_string_lossy().into_owned(),
                private_key_path: private_key_path.to_string_lossy().into_owned(),
                reload_interval_ms: 20,
                ..GrpcTlsConfig::default()
            },
            ..GrpcConfig::default()
        },
    );
    wait_for_server_ready(listen_addr, &mut server_task)
        .await
        .expect("TLS listener should become ready");
    let mut established = connect_tls(listen_addr, &original_ca.certificate_pem, None)
        .await
        .expect("original TLS generation should connect");

    tokio::time::sleep(Duration::from_millis(30)).await;
    fs::write(&certificate_path, &replacement.certificate_pem).expect("mismatched replacement certificate");
    tokio::time::sleep(Duration::from_millis(80)).await;
    connect_tls(listen_addr, &original_ca.certificate_pem, None)
        .await
        .expect("invalid generation must retain the last-known-good certificate");

    fs::write(&private_key_path, &replacement.private_key_pem).expect("matching replacement private key");
    let mut replacement_client = connect_tls_with_retry(listen_addr, &replacement_ca.certificate_pem, None)
        .await
        .expect("complete replacement generation should become active");
    replacement_client
        .query_route(route_request("ReplacementTopic"))
        .await
        .expect("replacement generation request should succeed");
    established
        .query_route(route_request("EstablishedTopic"))
        .await
        .expect("existing TLS connection must survive listener rotation");

    let _ = shutdown_tx.send(());
    assert!(server_task.await.expect("server task join").is_ok());
}

async fn spawn_runtime(
    service_manager: Arc<dyn rocketmq_proxy::ServiceManager>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<rocketmq_proxy::ProxyResult<()>>,
) {
    spawn_runtime_with_candidates(service_manager, (0..16).map(|_| reserve_loopback_addr())).await
}

async fn spawn_runtime_with_candidates<I>(
    service_manager: Arc<dyn rocketmq_proxy::ServiceManager>,
    listen_addrs: I,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<rocketmq_proxy::ProxyResult<()>>,
)
where
    I: IntoIterator<Item = SocketAddr>,
{
    let mut last_bind_error = None;
    for listen_addr in listen_addrs {
        let (shutdown_tx, mut server_task) = spawn_runtime_on_addr(service_manager.clone(), listen_addr);

        match wait_for_server_ready(listen_addr, &mut server_task).await {
            Ok(()) => return (listen_addr, shutdown_tx, server_task),
            Err(startup_error) if is_address_in_use_startup_error(&startup_error) => {
                last_bind_error = Some(startup_error);
            }
            Err(startup_error) => {
                panic!("proxy runtime failed to start on {listen_addr}: {startup_error}");
            }
        }
    }

    panic!("proxy runtime failed to bind after retries: {last_bind_error:?}");
}

fn spawn_runtime_on_addr(
    service_manager: Arc<dyn rocketmq_proxy::ServiceManager>,
    listen_addr: SocketAddr,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<rocketmq_proxy::ProxyResult<()>>,
) {
    spawn_runtime_on_addr_with_grpc_config(
        service_manager,
        GrpcConfig {
            listen_addr: listen_addr.to_string(),
            ..GrpcConfig::default()
        },
    )
}

fn spawn_runtime_on_addr_with_grpc_config(
    service_manager: Arc<dyn rocketmq_proxy::ServiceManager>,
    grpc: GrpcConfig,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<rocketmq_proxy::ProxyResult<()>>,
) {
    let runtime_context = RuntimeContext::from_current("proxy-grpc-listener-test");
    let runtime = ProxyRuntime::builder(
        ProxyConfig {
            grpc,
            ..ProxyConfig::default()
        },
        runtime_context.service_context("proxy-grpc-listener"),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .with_service_manager(service_manager)
    .build()
    .expect("the injected child context should build the proxy runtime");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        runtime
            .serve_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    (shutdown_tx, server_task)
}

fn service_manager_with_route(route_service: Arc<RecordingRouteService>) -> Arc<dyn rocketmq_proxy::ServiceManager> {
    Arc::new(ClusterServiceManager::with_services(
        route_service,
        Arc::new(NormalMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    ))
}

#[cfg(feature = "tls")]
async fn connect_tls(
    addr: SocketAddr,
    ca_pem: &str,
    identity: Option<(&str, &str)>,
) -> Result<MessagingServiceClient<tonic::transport::Channel>, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(ca_pem));
    if let Some((certificate, private_key)) = identity {
        tls = tls.identity(Identity::from_pem(certificate, private_key));
    }
    let channel = Endpoint::from_shared(format!("https://{addr}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(MessagingServiceClient::new(channel))
}

#[cfg(feature = "tls")]
async fn connect_tls_with_retry(
    addr: SocketAddr,
    ca_pem: &str,
    identity: Option<(&str, &str)>,
) -> Result<MessagingServiceClient<tonic::transport::Channel>, tonic::transport::Error> {
    let mut last_error = None;
    for _ in 0..30 {
        match connect_tls(addr, ca_pem, identity).await {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(last_error.expect("TLS retry loop must attempt a connection"))
}

#[cfg(feature = "tls")]
async fn assert_tls_connection_rejected(addr: SocketAddr, ca_pem: &str, identity: Option<(&str, &str)>) {
    if let Ok(mut client) = connect_tls(addr, ca_pem, identity).await {
        client
            .query_route(route_request("RejectedTlsClient"))
            .await
            .expect_err("TLS policy must reject this connection before processing a request");
    }
}

#[cfg(feature = "tls")]
struct TestCertificateAuthority {
    certificate_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

#[cfg(feature = "tls")]
impl TestCertificateAuthority {
    fn new(common_name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        params.distinguished_name.push(DnType::CommonName, common_name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().expect("CA key");
        let certificate = params.self_signed(&key).expect("CA certificate");
        Self {
            certificate_pem: certificate.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    fn server_identity(&self, common_name: &str) -> TestTlsIdentity {
        self.identity(
            common_name,
            ExtendedKeyUsagePurpose::ServerAuth,
            vec!["localhost".to_owned()],
        )
    }

    fn client_identity(&self, common_name: &str) -> TestTlsIdentity {
        self.identity(common_name, ExtendedKeyUsagePurpose::ClientAuth, Vec::new())
    }

    fn identity(
        &self,
        common_name: &str,
        usage: ExtendedKeyUsagePurpose,
        subject_alt_names: Vec<String>,
    ) -> TestTlsIdentity {
        let mut params = CertificateParams::new(subject_alt_names).expect("leaf parameters");
        params.distinguished_name.push(DnType::CommonName, common_name);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![usage];
        let key = KeyPair::generate().expect("leaf key");
        let certificate = params.signed_by(&key, &self.issuer).expect("signed leaf certificate");
        TestTlsIdentity {
            certificate_pem: certificate.pem(),
            private_key_pem: key.serialize_pem(),
        }
    }
}

#[cfg(feature = "tls")]
struct TestTlsIdentity {
    certificate_pem: String,
    private_key_pem: String,
}

async fn wait_for_server_ready(
    listen_addr: SocketAddr,
    server_task: &mut tokio::task::JoinHandle<rocketmq_proxy::ProxyResult<()>>,
) -> Result<(), String> {
    for _ in 0..20 {
        if server_task.is_finished() {
            let result = server_task.await.expect("server task should join during startup");
            return match result {
                Ok(()) => Err(format!("proxy runtime exited before becoming ready on {listen_addr}")),
                Err(error) => Err(error.to_string()),
            };
        }

        if tokio::net::TcpStream::connect(listen_addr).await.is_ok() {
            tokio::task::yield_now().await;
            if server_task.is_finished() {
                let result = server_task.await.expect("server task should join during startup");
                return match result {
                    Ok(()) => Err(format!("proxy runtime exited before becoming ready on {listen_addr}")),
                    Err(error) => Err(error.to_string()),
                };
            }
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    Err(format!(
        "timed out waiting for proxy runtime to accept connections on {listen_addr}"
    ))
}

fn reserve_loopback_addr() -> SocketAddr {
    let port_probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local port probe");
    let listen_addr = port_probe.local_addr().expect("discover local addr");
    drop(port_probe);
    listen_addr
}

fn is_address_in_use_startup_error(startup_error: &str) -> bool {
    startup_error.contains("failed to bind")
        && (startup_error.contains("Address already in use")
            || startup_error.contains("(os error 48)")
            || startup_error.contains("(os error 98)")
            || startup_error.contains("(os error 10048)"))
}

async fn connect_with_retry(addr: SocketAddr) -> MessagingServiceClient<tonic::transport::Channel> {
    let endpoint = format!("http://{addr}");
    let mut last_error = None;
    for _ in 0..20 {
        match MessagingServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return client,
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    panic!("gRPC client failed to connect to {endpoint}: {last_error:?}");
}

fn route_request(topic: &str) -> Request<v2::QueryRouteRequest> {
    let mut request = Request::new(v2::QueryRouteRequest {
        topic: Some(v2::Resource {
            resource_namespace: String::new(),
            name: topic.to_owned(),
        }),
        endpoints: Some(v2::Endpoints {
            scheme: v2::AddressScheme::IPv4 as i32,
            addresses: vec![v2::Address {
                host: "127.0.0.1".to_owned(),
                port: 8081,
            }],
        }),
    });
    request
        .metadata_mut()
        .insert("x-mq-client-id", MetadataValue::from_static("integration-client"));
    request
}

fn apply_auth_headers<T>(request: &mut Request<T>, username: &str, secret: &str) {
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(AUTH_TEST_DATETIME.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    let authorization =
        format!("MQv2-HMAC-SHA1 Credential={username}, SignedHeaders=x-mq-date-time, Signature={signature}");

    request
        .metadata_mut()
        .insert("x-mq-date-time", MetadataValue::from_static(AUTH_TEST_DATETIME));
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(authorization.as_str()).expect("auth metadata"),
    );
    request
        .metadata_mut()
        .insert("channel-id", MetadataValue::from_static("integration-auth-channel"));
}

fn send_message_request(topic: &str, message_id: &str) -> Request<v2::SendMessageRequest> {
    let mut request = Request::new(v2::SendMessageRequest {
        messages: vec![v2::Message {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: topic.to_owned(),
            }),
            user_properties: HashMap::new(),
            system_properties: Some(v2::SystemProperties {
                message_id: message_id.to_owned(),
                body_encoding: v2::Encoding::Identity as i32,
                ..Default::default()
            }),
            body: Bytes::from_static(b"integration-body"),
        }],
    });
    request
        .metadata_mut()
        .insert("x-mq-client-id", MetadataValue::from_static("integration-client"));
    request
}

fn receive_message_request(topic: &str, group: &str) -> Request<v2::ReceiveMessageRequest> {
    let mut request = Request::new(v2::ReceiveMessageRequest {
        group: Some(v2::Resource {
            resource_namespace: String::new(),
            name: group.to_owned(),
        }),
        message_queue: Some(v2::MessageQueue {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: topic.to_owned(),
            }),
            id: 1,
            permission: v2::Permission::ReadWrite as i32,
            broker: None,
            accept_message_types: vec![v2::MessageType::Normal as i32],
        }),
        filter_expression: None,
        batch_size: 1,
        invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
        auto_renew: false,
        long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
        attempt_id: None,
    });
    request
        .metadata_mut()
        .insert("x-mq-client-id", MetadataValue::from_static("integration-client"));
    request
}
