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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
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
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::admission::ResourceLimit;
use rocketmq_transport::client::TransportClient;
use rocketmq_transport::connection::Connection;
use rocketmq_transport::security::TransportSecurity;
use rocketmq_transport::server::RequestProcessor;
use rocketmq_transport::server::TransportServer;
use rocketmq_transport::server::TransportServerConfig;

struct EchoProcessor;

impl RequestProcessor for EchoProcessor {
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

impl RequestProcessor for HungProcessor {
    fn process(
        &self,
        _request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}

struct WrongOpaqueProcessor;

impl RequestProcessor for WrongOpaqueProcessor {
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

impl RequestProcessor for SignatureProcessor {
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

impl RequestProcessor for ControlledProcessor {
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
    let server = TransportServer::bind(
        runtime.service_context("transport-server"),
        TransportServerConfig::loopback(),
        Arc::new(EchoProcessor),
        admission.clone(),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let client = TransportClient::new(runtime.service_context("transport-client"), admission);
    let response = client
        .invoke(
            address,
            RemotingCommand::create_remoting_command(105).set_body("payload"),
            ShutdownDeadline::after(Duration::from_secs(2)),
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
    let mut config = TransportServerConfig::loopback();
    config.request_timeout = Duration::from_millis(20);
    let server = TransportServer::bind(
        runtime.service_context("transport-server"),
        config,
        Arc::new(HungProcessor),
        admission.clone(),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let client = TransportClient::new(runtime.service_context("transport-client"), admission);
    assert!(client
        .invoke(
            address,
            RemotingCommand::create_remoting_command(105),
            ShutdownDeadline::after(Duration::from_millis(100)),
        )
        .await
        .is_err());
    assert!(client
        .invoke(
            "127.0.0.1:1".parse().unwrap(),
            RemotingCommand::create_remoting_command(105),
            ShutdownDeadline::after(Duration::from_millis(100)),
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
async fn wrong_response_opaque_is_rejected_and_cannot_complete_the_request() {
    let runtime = RuntimeContext::from_current("transport-wrong-opaque-test");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let server = TransportServer::bind(
        runtime.service_context("transport-server"),
        TransportServerConfig::loopback(),
        Arc::new(WrongOpaqueProcessor),
        admission.clone(),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    server.start().unwrap();
    let client = TransportClient::new(runtime.service_context("transport-client"), admission);

    let result = client
        .invoke(
            address,
            RemotingCommand::create_remoting_command(105),
            ShutdownDeadline::after(Duration::from_millis(100)),
        )
        .await;

    assert!(
        result.is_err(),
        "a mismatched response opaque must not complete the request"
    );
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
        processors: ResourceLimit { count: 2, bytes: 2 },
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
    let server = TransportServer::bind(
        runtime.service_context("transport-server"),
        TransportServerConfig::loopback(),
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
    let server_security = Arc::new(TransportSecurity::new(Some(Arc::new(AllowAuthenticated)), None));
    let server = TransportServer::bind_with_security(
        runtime.service_context("transport-server"),
        TransportServerConfig::loopback(),
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

    let open_server = TransportServer::bind_with_security(
        runtime.service_context("signed-server"),
        TransportServerConfig::loopback(),
        Arc::new(SignatureProcessor),
        admission.clone(),
        Arc::new(TransportSecurity::new(None, None)),
        Some(Principal::new("authenticated")),
    )
    .await
    .unwrap();
    let signed_address = open_server.local_addr();
    open_server.start().unwrap();
    let client = TransportClient::new_with_security(
        runtime.service_context("signed-client"),
        admission,
        Arc::new(TransportSecurity::new(None, Some(Arc::new(MarkerSigner)))),
    );
    let response = client
        .invoke(
            signed_address,
            RemotingCommand::create_remoting_command(RequestCode::HeartBeat),
            ShutdownDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("signed request");
    assert_eq!(response.code(), ResponseCode::Success.to_i32());

    let _ = open_server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    let _ = runtime.shutdown_tasks(Duration::from_secs(1)).await;
}
