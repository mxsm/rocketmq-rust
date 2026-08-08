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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::v1::ClientShutdownReport;
use rocketmq_transport::api::v1::ClientSnapshot;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::PendingUsage;
use rocketmq_transport::api::v1::RemotingClient;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::RequestTarget;
use rocketmq_transport::api::v1::SendReceipt;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportClient;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportServer;

fn assert_stable_result_types(
    snapshot: ClientSnapshot,
    pending: PendingUsage,
    shutdown: ClientShutdownReport,
) -> (ClientSnapshot, PendingUsage, ClientShutdownReport) {
    (snapshot, pending, shutdown)
}

fn assert_stable_async_methods(client: &TransportClient<DefaultRequestProcessor>, target: RequestTarget) {
    let request = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_request_command(
        0,
        rocketmq_protocol::protocol::header::empty_header::EmptyHeader::default(),
    );
    let deadline = RequestDeadline::after(Duration::from_millis(1));
    let request_future = client.request(target.clone(), request.clone(), deadline);
    let oneway_future = client.send_oneway(target, request, deadline);
    let _: &dyn std::future::Future<Output = rocketmq_error::RocketMQResult<_>> = &request_future;
    let _: &dyn std::future::Future<Output = rocketmq_error::RocketMQResult<SendReceipt>> = &oneway_future;
}

#[tokio::test]
async fn versioned_api_constructs_canonical_client_and_server() {
    let runtime = RuntimeContext::try_from_current("transport-public-api-v1").unwrap();
    let config = Arc::new(TransportClientConfig::default());

    let transport = TransportClient::builder(
        Arc::clone(&config),
        DefaultRequestProcessor,
        runtime.service_context("transport-client"),
    )
    .build()
    .unwrap();
    assert_stable_async_methods(&transport, RequestTarget::NameServer);

    let remoting = RemotingClient::builder(
        config,
        DefaultRequestProcessor,
        runtime.service_context("remoting-client"),
    )
    .build()
    .unwrap();
    remoting.update_name_server_address_list(Vec::new()).await;

    let snapshot = remoting.snapshot();
    let pending = snapshot.pending;
    let shutdown = ClientShutdownReport::default();
    let _ = assert_stable_result_types(snapshot, pending, shutdown);

    let _server: TransportServer<DefaultRequestProcessor> = TransportServer::new(
        Arc::new(ServerConfig::default()),
        runtime.service_context("transport-server"),
    );
}
