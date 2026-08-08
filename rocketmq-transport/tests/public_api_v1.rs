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
use tokio::net::TcpListener;
use tokio::sync::oneshot;

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

#[tokio::test]
async fn versioned_api_runs_plaintext_request_and_fallible_oneway() {
    let runtime = RuntimeContext::try_from_current("transport-public-api-v1-flow").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut server = TransportServer::new(
        Arc::new(ServerConfig::default()),
        runtime.service_context("transport-public-api-v1-server"),
    );
    let server_task = tokio::spawn(async move {
        server
            .serve_bound_listener_until(listener, DefaultRequestProcessor, None, None, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = TransportClient::builder(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        runtime.service_context("transport-public-api-v1-client"),
    )
    .build()
    .unwrap();
    let target = RequestTarget::Endpoint(address.to_string().into());
    let request = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(105);
    let opaque = request.opaque();
    let response = client
        .request(target.clone(), request, RequestDeadline::after(Duration::from_secs(2)))
        .await
        .unwrap();
    assert_eq!(response.code(), 105);
    assert_eq!(response.opaque(), opaque);

    let receipt = client
        .send_oneway(
            target,
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(106),
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(receipt.endpoint.as_str(), address.to_string());

    assert!(client.shutdown_with_report(Duration::from_secs(1)).await.is_healthy());
    let _ = shutdown_tx.send(());
    let report = server_task.await.unwrap().unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
}
