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
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::client::TransportClient;
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
