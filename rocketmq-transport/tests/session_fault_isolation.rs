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

use std::error::Error as _;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorContext;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::FrameLimits;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::TlsConfig;
use rocketmq_transport::test_support::connect_with_config;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::SessionProcessor;
use rocketmq_transport::test_support::SessionTransportServer;
use rocketmq_transport::test_support::SessionTransportServerConfig;

struct FaultSelectingProcessor;

#[derive(Debug)]
struct InjectedProcessorFailure {
    detail: &'static str,
}

impl fmt::Display for InjectedProcessorFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail)
    }
}

impl std::error::Error for InjectedProcessorFailure {}

fn injected_processor_failure() -> RocketMQError {
    RocketMQError::Shared(Arc::new(
        CanonicalError::caused_by(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            InjectedProcessorFailure {
                detail: "injected processor failure",
            },
        )
        .with_context(
            ErrorContext::new()
                .with_text(rocketmq_error::fields::PHASE, "connect")
                .with_secret_presence(rocketmq_error::fields::REMOTE_ADDR_PRESENT)
                .with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT),
        ),
    ))
}

impl SessionProcessor for FaultSelectingProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            match request.opaque() {
                2 => Err(injected_processor_failure()),
                5 => std::future::pending().await,
                _ => Ok(RemotingCommand::create_response_command_with_code(0).set_opaque(request.opaque())),
            }
        })
    }
}

#[test]
fn injected_fault_preserves_canonical_network_policy_and_typed_source() {
    let error = injected_processor_failure();
    assert_eq!(
        error.descriptor().code(),
        rocketmq_error::TRANSPORT_CONNECTION_FAILED.code()
    );
    assert_eq!(
        error.boundary_view().remoting().code.as_i32(),
        2,
        "processor faults retain the legacy SystemError remoting code"
    );

    let RocketMQError::Shared(canonical) = &error else {
        panic!("injected processor failure must remain a canonical Shared error")
    };
    assert_eq!(canonical.fault(), rocketmq_error::FaultAttribution::Dependency);
    let source = canonical
        .source()
        .expect("canonical error retains the typed processor failure");
    let typed = source
        .downcast_ref::<InjectedProcessorFailure>()
        .expect("processor failure source remains downcastable");
    assert_eq!(typed.detail, "injected processor failure");

    for rendered in [format!("{error}"), format!("{error:?}")] {
        assert!(!rendered.contains("test-processor"));
        assert!(!rendered.contains("injected processor failure"));
    }
}

async fn connect(address: std::net::SocketAddr) -> Connection {
    connect_with_config(
        &address.to_string(),
        &TlsConfig::default(),
        FrameLimits::default(),
        RequestDeadline::after(Duration::from_secs(2)),
    )
    .await
    .expect("connect transport client")
    .into_parts()
    .0
}

async fn assert_echo(connection: &mut Connection, opaque: i32) {
    connection
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat.to_i32()).set_opaque(opaque))
        .await
        .expect("send echo request");
    let response = tokio::time::timeout(Duration::from_secs(2), connection.receive_command())
        .await
        .expect("echo response deadline")
        .expect("echo response frame")
        .expect("echo response command");
    assert_eq!(response.opaque(), opaque);
}

async fn assert_fault_closes_only_connection(connection: &mut Connection, opaque: i32) {
    connection
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat.to_i32()).set_opaque(opaque))
        .await
        .expect("send injected fault request");
    let response = tokio::time::timeout(Duration::from_secs(2), connection.receive_command())
        .await
        .expect("faulted session should close within request timeout");
    assert!(
        response.is_none() || response.is_some_and(|result| result.is_err()),
        "faulted session must close without a successful response"
    );
}

#[tokio::test]
async fn processor_failure_and_timeout_do_not_cancel_sibling_or_future_sessions() {
    let runtime = RuntimeContext::from_current("transport-session-fault-isolation-test");
    let service = runtime.service_context("transport-server");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let mut config = SessionTransportServerConfig::loopback();
    config.request_timeout = Duration::from_millis(200);
    let server = SessionTransportServer::bind(
        service.clone(),
        config,
        Arc::new(FaultSelectingProcessor),
        admission.clone(),
    )
    .await
    .expect("bind transport server");
    let baseline_components = server.owned_component_group_count();
    let address = server.local_addr();
    server.start().expect("start transport server");

    let mut sibling = connect(address).await;
    assert_echo(&mut sibling, 1).await;

    let mut error_session = connect(address).await;
    assert_fault_closes_only_connection(&mut error_session, 2).await;
    assert!(!service.task_group().cancellation_token().is_cancelled());
    assert_echo(&mut sibling, 3).await;

    let mut future_session = connect(address).await;
    assert_echo(&mut future_session, 4).await;

    let mut timeout_session = connect(address).await;
    assert_fault_closes_only_connection(&mut timeout_session, 5).await;
    assert!(!service.task_group().cancellation_token().is_cancelled());
    assert_echo(&mut sibling, 6).await;
    assert_echo(&mut future_session, 7).await;

    sibling.shutdown().await.expect("shutdown sibling session");
    future_session.shutdown().await.expect("shutdown future session");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = admission.snapshot();
            if snapshot.connections.current_count == 0 && server.owned_component_group_count() == baseline_components {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session groups and connection permits should return to baseline");

    let report = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    report.assert_no_task_leak().expect("transport server task leak");
    let root = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    root.assert_no_task_leak().expect("runtime task leak");
}
