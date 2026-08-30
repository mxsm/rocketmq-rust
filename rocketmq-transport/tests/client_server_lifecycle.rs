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

#![cfg(feature = "test-support")]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::EncodedFrame;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::OutboundSigner;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_security_api::Secret;
use rocketmq_security_api::SecurityRequestView;
use rocketmq_security_api::Signature;
use rocketmq_security_api::SigningError;
use rocketmq_transport::api::AdmissionClass;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::AdmissionResource;
use rocketmq_transport::api::AdmissionScope;
use rocketmq_transport::api::FrameLimits;
use rocketmq_transport::api::OneShotTransportClient;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::ResourceLimit;
use rocketmq_transport::api::TlsClientConfig;
use rocketmq_transport::api::TlsConfig;
use rocketmq_transport::api::TlsMode;
use rocketmq_transport::api::TlsServerRuntime;
use rocketmq_transport::api::TransportSecurity;
#[cfg(feature = "tls")]
use rocketmq_transport::test_support::transport_io_snapshot;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::ConnectionHandler;
use rocketmq_transport::test_support::SessionHandle;
use rocketmq_transport::test_support::SessionProcessor;
use rocketmq_transport::test_support::SessionTransportServer;
use rocketmq_transport::test_support::SessionTransportServerConfig;
use rocketmq_transport::test_support::TransportListener;

struct EchoProcessor;

struct CountingEchoProcessor(AtomicUsize);

impl SessionProcessor for CountingEchoProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()))
        })
    }
}

impl SessionProcessor for EchoProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            Ok(RemotingCommand::create_response_command_with_code(0)
                .set_opaque(request.opaque())
                .set_body(request.body().cloned().unwrap_or_default()))
        })
    }
}

struct HungProcessor;

impl SessionProcessor for HungProcessor {
    fn process(
        &self,
        _request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}

struct WrongOpaqueProcessor;

impl SessionProcessor for WrongOpaqueProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(
            async move { Ok(RemotingCommand::create_response_command_with_code(0).set_opaque(request.opaque() + 1)) },
        )
    }
}

struct ControlledProcessor {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: AtomicUsize,
}

struct AllowAuthenticated;

impl RequestPolicy for AllowAuthenticated {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

struct MarkerSigner;

impl OutboundSigner for MarkerSigner {
    fn sign(&self, _request: SecurityRequestView<'_>) -> Result<Signature, SigningError> {
        Ok(Signature::new(vec![(
            CheetahString::from_static_str("TransportSignature"),
            Secret::new(CheetahString::from_static_str("signed")),
        )]))
    }
}

struct SignatureProcessor;

struct RecordPeerTls {
    calls: AtomicUsize,
    saw_tls: std::sync::atomic::AtomicBool,
}

impl RecordPeerTls {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            saw_tls: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl RequestPolicy for RecordPeerTls {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.saw_tls.store(
            context.request().peer().expect("transport peer metadata").is_tls(),
            Ordering::SeqCst,
        );
        Decision::Allow
    }
}

struct SessionEchoHandler {
    calls: AtomicUsize,
}

struct SegmentedResponseHandler {
    rejected: Arc<AtomicUsize>,
}

impl ConnectionHandler for SegmentedResponseHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let body_len = if command.code() == RequestCode::HeartBeat.to_i32() {
                32
            } else {
                33
            };
            let wire = EncodedFrame::from_command(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                    .set_opaque(command.opaque())
                    .set_body(vec![7_u8; body_len]),
            )
            .unwrap()
            .into_bytes();
            let header_end = 8 + (u32::from_be_bytes(wire[4..8].try_into().unwrap()) & 0x00ff_ffff) as usize;
            let segments = vec![wire.slice(..header_end), wire.slice(header_end..)];
            if session.connection().send_frame_segments(segments).await.is_err() {
                self.rejected.fetch_add(1, Ordering::SeqCst);
            }
        })
    }
}

impl ConnectionHandler for SessionEchoHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut connection = session.connection();
            let _ = connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(command.opaque()),
                )
                .await;
        })
    }
}

impl SessionProcessor for SignatureProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            assert_eq!(
                request
                    .ext_fields()
                    .and_then(|fields| fields.get("TransportSignature"))
                    .map(CheetahString::as_str),
                Some("signed")
            );
            Ok(RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()))
        })
    }
}

impl SessionProcessor for ControlledProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if request.code() == RequestCode::SendMessage.to_i32() {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()))
        })
    }
}

#[tokio::test]
async fn real_request_uses_one_deadline_and_drains_all_owned_tasks() {
    let runtime = RuntimeContext::from_current("transport-lifecycle-test");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let server = SessionTransportServer::bind(
        runtime.service_context("transport-server"),
        SessionTransportServerConfig::loopback(),
        Arc::new(EchoProcessor),
        admission.clone(),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let client = OneShotTransportClient::new(runtime.service_context("transport-client"), admission);
    let response = client
        .invoke(
            address,
            RemotingCommand::create_remoting_command(105).set_body("payload"),
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(response.code(), 0);
    assert_eq!(response.body().unwrap().as_ref(), b"payload");
    assert_eq!(client.pending_usage().count, 0);
    assert_eq!(client.pending_usage().bytes, 0);

    let report = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    report.assert_no_task_leak().unwrap();
    let root = runtime.shutdown_tasks(Duration::from_secs(2)).await;
    root.assert_no_task_leak().unwrap();
}

#[tokio::test]
async fn hung_processor_and_send_failure_complete_without_leaking_pending_or_tasks() {
    let runtime = RuntimeContext::from_current("transport-timeout-test");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let mut config = SessionTransportServerConfig::loopback();
    config.request_timeout = Duration::from_millis(20);
    let server = SessionTransportServer::bind(
        runtime.service_context("transport-server"),
        config,
        Arc::new(HungProcessor),
        admission.clone(),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let client = OneShotTransportClient::new(runtime.service_context("transport-client"), admission);
    assert!(client
        .invoke(
            address,
            RemotingCommand::create_remoting_command(105),
            RequestDeadline::after(Duration::from_millis(100)),
        )
        .await
        .is_err());
    assert!(client
        .invoke(
            "127.0.0.1:1".parse().unwrap(),
            RemotingCommand::create_remoting_command(105),
            RequestDeadline::after(Duration::from_millis(100)),
        )
        .await
        .is_err());
    assert_eq!(client.pending_usage().count, 0);

    let report = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    report.assert_no_task_leak().unwrap();
    let root = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    root.assert_no_task_leak().unwrap();
}

#[tokio::test]
async fn low_level_processor_response_is_rebound_to_the_original_opaque() {
    let runtime = RuntimeContext::from_current("transport-wrong-opaque-test");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let server = SessionTransportServer::bind(
        runtime.service_context("transport-server"),
        SessionTransportServerConfig::loopback(),
        Arc::new(WrongOpaqueProcessor),
        admission.clone(),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let client = OneShotTransportClient::new(runtime.service_context("transport-client"), admission);

    let result = client
        .invoke(
            address,
            RemotingCommand::create_remoting_command(105),
            RequestDeadline::after(Duration::from_millis(100)),
        )
        .await;

    let response = result.expect("transport must bind the processor response to the request opaque");
    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    assert_eq!(client.pending_usage().count, 0);
    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn data_overload_rejects_without_closing_and_control_reserve_survives() {
    let runtime = RuntimeContext::from_current("transport-overload-server-test");
    let limits = AdmissionLimits {
        processors: ResourceLimit {
            count: 2,
            bytes: 1024 * 1024,
        },
        control_reserve: ResourceLimit { count: 1, bytes: 1 },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let processor = Arc::new(ControlledProcessor {
        entered: entered.clone(),
        release: release.clone(),
        calls: AtomicUsize::new(0),
    });
    let server = SessionTransportServer::bind(
        runtime.service_context("transport-server"),
        SessionTransportServerConfig::loopback(),
        processor.clone(),
        admission,
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();

    let first_socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut first = Connection::new(first_socket);
    first
        .send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("first data request should occupy the data processor slot");

    let second_socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut second = Connection::new(second_socket);
    second
        .send_command(RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(2))
        .await
        .unwrap();
    let rejection = tokio::time::timeout(Duration::from_secs(1), second.receive_command())
        .await
        .expect("overload must respond instead of hanging")
        .expect("connection must remain open")
        .expect("typed rejection response");
    assert_eq!(rejection.code(), ResponseCode::SystemBusy.to_i32());
    assert_eq!(rejection.opaque(), 2);

    second
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(3))
        .await
        .unwrap();
    let control = tokio::time::timeout(Duration::from_secs(1), second.receive_command())
        .await
        .expect("control reserve must remain responsive")
        .expect("connection remains open")
        .expect("control response");
    assert_eq!(control.code(), ResponseCode::Success.to_i32());
    assert_eq!(control.opaque(), 3);

    release.notify_one();
    let _ = first.receive_command().await;
    assert_eq!(processor.calls.load(Ordering::SeqCst), 2);
    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn transport_security_signs_outbound_and_fails_closed_without_a_principal() {
    let runtime = RuntimeContext::from_current("transport-security-lifecycle-test");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let server_security = Arc::new(TransportSecurity::secure_enforced(
        Some(Arc::new(AllowAuthenticated)),
        None,
    ));
    let server = SessionTransportServer::bind_with_security(
        runtime.service_context("transport-server"),
        SessionTransportServerConfig::loopback(),
        Arc::new(SignatureProcessor),
        admission.clone(),
        server_security,
        None,
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();

    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut unauthenticated = Connection::new(socket);
    unauthenticated
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(11))
        .await
        .unwrap();
    let denied = unauthenticated
        .receive_command()
        .await
        .expect("denial frame")
        .expect("denial response");
    assert_eq!(denied.code(), ResponseCode::NoPermission.to_i32());

    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;

    let open_server = SessionTransportServer::bind_with_security(
        runtime.service_context("signed-server"),
        SessionTransportServerConfig::loopback(),
        Arc::new(SignatureProcessor),
        admission.clone(),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        Some(Principal::new("authenticated")),
    )
    .await
    .unwrap();
    let signed_address = open_server.local_addr();
    open_server.start().unwrap();
    let client = OneShotTransportClient::new_with_security(
        runtime.service_context("signed-client"),
        admission,
        Arc::new(TransportSecurity::development_insecure_loopback(
            None,
            Some(Arc::new(MarkerSigner)),
        )),
    );
    let response = client
        .invoke(
            signed_address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("signed request");
    assert_eq!(response.code(), ResponseCode::Success.to_i32());

    let _ = open_server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn plaintext_server_and_client_enforce_their_owned_frame_profile() {
    let runtime = RuntimeContext::from_current("transport-frame-profile-test");
    let frame_limits = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 32,
        initial_read_bytes: 8,
    };
    let server_config = SessionTransportServerConfig::loopback();
    let server = SessionTransportServer::bind_with_frame_limits(
        runtime.service_context("transport-server"),
        server_config,
        frame_limits,
        Arc::new(EchoProcessor),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();

    let bounded = OneShotTransportClient::new(
        runtime.service_context("bounded-client"),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .try_with_frame_limits(frame_limits)
    .unwrap();
    let response = bounded
        .invoke(
            address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_body(vec![7_u8; 32]),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("exact endpoint body limit");
    assert_eq!(response.body().map(bytes::Bytes::len), Some(32));

    let permissive = OneShotTransportClient::new(
        runtime.service_context("permissive-client"),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .try_with_frame_limits(FrameLimits::java_compatibility())
    .unwrap();
    assert!(permissive
        .invoke(
            address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_body(vec![7_u8; 33]),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .is_err());

    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

async fn queued_segmented_response_obeys_owner_limits(tls_enabled: bool) {
    let runtime = RuntimeContext::from_current("transport-segmented-frame-profile-test");
    let service = runtime.service_context("transport-listener");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let frame_limits = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 32,
        initial_read_bytes: 8,
    };
    let mut server_tls = TlsConfig::default();
    let mut client_tls = TlsConfig::default();
    if tls_enabled {
        server_tls.test_mode_enable = true;
        server_tls.server.mode = TlsMode::Permissive;
        client_tls.enable = true;
        client_tls.test_mode_enable = true;
        client_tls.client = TlsClientConfig {
            auth_server: false,
            ..TlsClientConfig::default()
        };
    } else {
        server_tls.server.mode = TlsMode::Disabled;
    }
    let transport = TransportListener::new(
        listener,
        service.task_group().clone(),
        TlsServerRuntime::initialize_with_service_context(server_tls, &service)
            .await
            .unwrap(),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
        Duration::from_secs(1),
    )
    .try_with_frame_limits(frame_limits)
    .unwrap();
    let rejected = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(SegmentedResponseHandler {
        rejected: rejected.clone(),
    });
    service
        .spawn_service("segmented-listener", async move {
            let _ = transport.run(handler).await;
        })
        .unwrap();

    let client = OneShotTransportClient::new(
        runtime.service_context("transport-client"),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .try_with_frame_limits(FrameLimits::java_compatibility())
    .unwrap();
    let exact = client
        .invoke_with_config(
            address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat),
            &client_tls,
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .expect("exact segmented response");
    assert_eq!(exact.body().map(Bytes::len), Some(32));

    assert!(client
        .invoke_with_config(
            address,
            RemotingCommand::create_remoting_command(RequestCode::SendMessage),
            &client_tls,
            RequestDeadline::after(Duration::from_millis(100)),
        )
        .await
        .is_err());
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn queued_plaintext_segmented_responses_enforce_the_complete_frame_profile() {
    queued_segmented_response_obeys_owner_limits(false).await;
}

#[tokio::test]
#[cfg(feature = "tls")]
async fn queued_tls_segmented_responses_enforce_the_complete_frame_profile() {
    queued_segmented_response_obeys_owner_limits(true).await;
}

#[tokio::test]
#[cfg(feature = "tls")]
async fn tls_client_invocation_releases_pending_and_server_ownership() {
    let runtime = RuntimeContext::from_current("transport-tls-client-convergence-test");
    let frame_limits = FrameLimits {
        max_frame_bytes: 4096,
        max_header_bytes: 2048,
        max_body_bytes: 1024,
        initial_read_bytes: 8,
    };
    let mut server_config = SessionTransportServerConfig::loopback();
    server_config.tls.test_mode_enable = true;
    server_config.tls.server.mode = TlsMode::Permissive;
    let server = SessionTransportServer::bind_with_frame_limits(
        runtime.service_context("transport-server"),
        server_config,
        frame_limits,
        Arc::new(EchoProcessor),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let baseline_tasks = server.live_task_count();
    let baseline_components = server.owned_component_group_count();

    let client = OneShotTransportClient::new(
        runtime.service_context("transport-client"),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .try_with_frame_limits(frame_limits)
    .unwrap();
    let client_tls = TlsConfig {
        enable: true,
        test_mode_enable: true,
        client: TlsClientConfig {
            auth_server: false,
            ..TlsClientConfig::default()
        },
        ..TlsConfig::default()
    };
    let io_before = transport_io_snapshot();
    let response = client
        .invoke_with_config(
            address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_body(vec![7_u8; 1024]),
            &client_tls,
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .expect("TLS invocation");
    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    assert_eq!(response.body().map(bytes::Bytes::len), Some(1024));
    assert_eq!(client.pending_usage().count, 0);
    assert_eq!(client.pending_usage().bytes, 0);

    tokio::time::timeout(Duration::from_secs(1), async {
        while server.live_task_count() != baseline_tasks {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server session and processor tasks should converge");
    assert_eq!(server.owned_component_group_count(), baseline_components);
    assert!(transport_io_snapshot().encoded_bytes_written > io_before.encoded_bytes_written);

    let permissive_client = OneShotTransportClient::new(
        runtime.service_context("permissive-tls-client"),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .try_with_frame_limits(FrameLimits::java_compatibility())
    .unwrap();
    assert!(permissive_client
        .invoke_with_config(
            address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_body(vec![7_u8; 1025]),
            &client_tls,
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .is_err());

    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn transport_server_reports_plaintext_for_permissive_tls_connections() {
    let runtime = RuntimeContext::from_current("transport-server-negotiated-tls-test");
    let policy = Arc::new(RecordPeerTls::new());
    let mut config = SessionTransportServerConfig::loopback();
    config.tls.server.mode = TlsMode::Permissive;
    let server = SessionTransportServer::bind_with_security(
        runtime.service_context("transport-server"),
        config,
        Arc::new(EchoProcessor),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
        Arc::new(TransportSecurity::secure_enforced(Some(policy.clone()), None)),
        Some(Principal::new("test")),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();

    let mut connection = Connection::new(tokio::net::TcpStream::connect(address).await.unwrap());
    connection
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(41))
        .await
        .unwrap();
    let response = connection.receive_command().await.unwrap().unwrap();
    assert_eq!(response.opaque(), 41);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    assert!(!policy.saw_tls.load(Ordering::SeqCst));

    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn canonical_listener_reports_plaintext_for_permissive_tls_connections() {
    let runtime = RuntimeContext::from_current("transport-listener-negotiated-tls-test");
    let service = runtime.service_context("listener");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let policy = Arc::new(RecordPeerTls::new());
    let transport = TransportListener::new(
        listener,
        service.task_group().clone(),
        TlsServerRuntime::initialize_with_service_context(Default::default(), &service)
            .await
            .unwrap(),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
        Duration::from_secs(1),
    )
    .with_security(
        Arc::new(TransportSecurity::secure_enforced(Some(policy.clone()), None)),
        Some(Principal::new("test")),
    );
    service
        .spawn_service("listener", async move {
            let _ = transport
                .run(Arc::new(SessionEchoHandler {
                    calls: AtomicUsize::new(0),
                }))
                .await;
        })
        .unwrap();

    let mut connection = Connection::new(tokio::net::TcpStream::connect(address).await.unwrap());
    connection
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(42))
        .await
        .unwrap();
    let response = connection.receive_command().await.unwrap().unwrap();
    assert_eq!(response.opaque(), 42);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    assert!(!policy.saw_tls.load(Ordering::SeqCst));

    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn large_decoded_headers_count_toward_admission_in_both_server_paths() {
    let limits = AdmissionLimits {
        inflight: ResourceLimit { count: 8, bytes: 256 },
        queued: ResourceLimit { count: 8, bytes: 256 },
        processors: ResourceLimit { count: 8, bytes: 256 },
        ..AdmissionLimits::default()
    };
    let runtime = RuntimeContext::from_current("transport-large-header-admission-test");
    let processor = Arc::new(CountingEchoProcessor(AtomicUsize::new(0)));
    let server = SessionTransportServer::bind(
        runtime.service_context("transport-server"),
        SessionTransportServerConfig::loopback(),
        processor.clone(),
        Arc::new(AdmissionController::new(limits)),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let mut direct = Connection::new(tokio::net::TcpStream::connect(address).await.unwrap());
    let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(51);
    request.ensure_ext_fields_initialized();
    request.add_ext_field("large-header", "x".repeat(2048));
    direct.send_command(request).await.unwrap();
    let rejection = direct.receive_command().await.unwrap().unwrap();
    assert_eq!(rejection.code(), ResponseCode::SystemBusy.to_i32());
    assert_eq!(processor.0.load(Ordering::SeqCst), 0);
    let _ = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;

    let service = runtime.service_context("transport-listener");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let admission = Arc::new(AdmissionController::new(limits));
    let handler = Arc::new(SessionEchoHandler {
        calls: AtomicUsize::new(0),
    });
    let transport = TransportListener::new(
        listener,
        service.task_group().clone(),
        TlsServerRuntime::initialize_with_service_context(Default::default(), &service)
            .await
            .unwrap(),
        admission,
        Duration::from_secs(1),
    );
    let listener_handler = handler.clone();
    service
        .spawn_service("listener", async move {
            let _ = transport.run(listener_handler).await;
        })
        .unwrap();
    let mut canonical = Connection::new(tokio::net::TcpStream::connect(address).await.unwrap());
    let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(52);
    request.ensure_ext_fields_initialized();
    request.add_ext_field("large-header", "x".repeat(2048));
    canonical.send_command(request).await.unwrap();
    let rejection = canonical.receive_command().await.unwrap().unwrap();
    assert_eq!(rejection.code(), ResponseCode::SystemBusy.to_i32());
    assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn canonical_control_response_inherits_request_class_for_writer_reserve() {
    let limits = AdmissionLimits {
        queued: ResourceLimit { count: 4, bytes: 4096 },
        control_reserve: ResourceLimit { count: 2, bytes: 2048 },
        ..AdmissionLimits::default()
    };
    let runtime = RuntimeContext::from_current("transport-control-response-class-test");
    let service = runtime.service_context("listener");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let admission = Arc::new(AdmissionController::new(limits));
    let scope = AdmissionScope::new(address.ip());
    let _data_one = admission
        .try_acquire(AdmissionResource::Queued, scope, 1, AdmissionClass::Data)
        .unwrap();
    let _data_two = admission
        .try_acquire(AdmissionResource::Queued, scope, 1, AdmissionClass::Data)
        .unwrap();
    let transport = TransportListener::new(
        listener,
        service.task_group().clone(),
        TlsServerRuntime::initialize_with_service_context(Default::default(), &service)
            .await
            .unwrap(),
        admission,
        Duration::from_secs(1),
    );
    service
        .spawn_service("listener", async move {
            let _ = transport
                .run(Arc::new(SessionEchoHandler {
                    calls: AtomicUsize::new(0),
                }))
                .await;
        })
        .unwrap();
    let mut connection = Connection::new(tokio::net::TcpStream::connect(address).await.unwrap());
    connection
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(61))
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_millis(250), connection.receive_command())
        .await
        .expect("control response should use the control reserve")
        .unwrap()
        .unwrap();
    assert_eq!(response.opaque(), 61);
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}
