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

#![allow(
    deprecated,
    reason = "this integration test intentionally freezes the deprecated V1 public compatibility surface"
)]

use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use rocketmq_transport::api::v1::CachedConnectionState;
use rocketmq_transport::api::v1::ClientShutdownReport;
use rocketmq_transport::api::v1::ClientSnapshot;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::PendingUsage;
use rocketmq_transport::api::v1::RemotingClient;
use rocketmq_transport::api::v1::RequestContext;
use rocketmq_transport::api::v1::RequestContextError;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::RequestId;
use rocketmq_transport::api::v1::RequestTarget;
use rocketmq_transport::api::v1::RequestTransport;
use rocketmq_transport::api::v1::ResponseDisposition;
use rocketmq_transport::api::v1::ResponseError;
use rocketmq_transport::api::v1::ResponseErrorKind;
use rocketmq_transport::api::v1::ResponseReceipt;
use rocketmq_transport::api::v1::ResponseTerminalState;
use rocketmq_transport::api::v1::SendReceipt;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::ServerStartError;
use rocketmq_transport::api::v1::TransportClient;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportServer;
use rocketmq_transport::api::v1::WriteProgress;
use rocketmq_transport::prelude::OneShotTransportClient as PreludeOneShotTransportClient;
use rocketmq_transport::prelude::RemotingClient as PreludeRemotingClient;
use rocketmq_transport::prelude::RequestDeadline as PreludeRequestDeadline;
use rocketmq_transport::prelude::RequestProcessor as PreludeRequestProcessor;
use rocketmq_transport::prelude::ServerConfig as PreludeServerConfig;
use rocketmq_transport::prelude::TransportClient as PreludeTransportClient;
use rocketmq_transport::prelude::TransportClientConfig as PreludeTransportClientConfig;
use rocketmq_transport::prelude::TransportServer as PreludeTransportServer;
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

fn assert_stable_shutdown_methods(client: &TransportClient<DefaultRequestProcessor>) {
    let graceful = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_millis(1)));
    let _: &dyn std::future::Future<Output = ClientShutdownReport> = &graceful;
    let _: fn(&TransportClient<DefaultRequestProcessor>) -> ClientShutdownReport = TransportClient::shutdown_now;
}

fn assert_cached_connection_state(state: CachedConnectionState) {
    assert!(matches!(
        state,
        CachedConnectionState::Healthy | CachedConnectionState::UnhealthyRetired | CachedConnectionState::Absent
    ));
}

/// This compatibility assertion intentionally exercises retained deprecated names.
#[allow(deprecated)]
fn assert_legacy_compatibility_methods(client: &TransportClient<DefaultRequestProcessor>) {
    let address = "127.0.0.1:10911".into();
    client.is_address_reachable(&address);
    client.close_clients(Vec::new());
    client.register_processor(DefaultRequestProcessor);
}

fn assert_prelude_processor<T: PreludeRequestProcessor>() {}

fn assert_versioned_response_contract(
    request_id: Option<RequestId>,
    receipt: Option<ResponseReceipt>,
    error: ResponseError,
) {
    if let Some(request_id) = request_id {
        let _ = (request_id.owner_id(), request_id.sequence());
    }
    if let Some(receipt) = receipt {
        let _ = (receipt.request_id(), receipt.disposition());
    }
    let _: ResponseErrorKind = error.kind();
    let _: Option<WriteProgress> = error.write_progress();
    let _: bool = error.retryable();
}

fn assert_v1_request_context_method_signatures(context: &RequestContext) {
    let _: fn(PeerInfo, Option<Principal>, Option<RequestDeadline>) -> RequestContext = RequestContext::network;
    let _: fn(Option<Principal>, Option<RequestDeadline>) -> Result<RequestContext, RequestContextError> =
        RequestContext::try_embedded;
    let _: fn(&RequestContext) -> RequestTransport = RequestContext::transport;
    let _: for<'a> fn(&'a RequestContext) -> Option<&'a PeerInfo> = RequestContext::peer;
    let _: for<'a> fn(&'a RequestContext) -> Option<&'a Principal> = RequestContext::principal;
    let _: fn(&RequestContext) -> Option<RequestDeadline> = RequestContext::deadline;
    let _ = context;
}

#[allow(
    deprecated,
    reason = "the V1 public API contract intentionally freezes deprecated compatibility signatures"
)]
fn assert_v1_response_context_method_signatures(context: &ConnectionHandlerContext) {
    let command = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(1);
    let write = context.try_write_response(command);
    let _: &dyn std::future::Future<Output = Result<ResponseReceipt, ResponseError>> = &write;
    drop(write);

    let mut borrowed = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(2);
    let write_ref = context.try_write_response_ref(&mut borrowed);
    let _: &dyn std::future::Future<Output = Result<ResponseReceipt, ResponseError>> = &write_ref;
    drop(write_ref);

    let compatibility = context
        .write_response(rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(3));
    let _: &dyn std::future::Future<Output = ()> = &compatibility;
    drop(compatibility);

    let mut borrowed_compatibility =
        rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(4);
    let compatibility_ref = context.write_response_ref(&mut borrowed_compatibility);
    let _: &dyn std::future::Future<Output = ()> = &compatibility_ref;
    drop(compatibility_ref);

    #[allow(
        deprecated,
        reason = "public API contract test intentionally freezes legacy future outputs"
    )]
    {
        let write =
            context.write(rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(5));
        let _: &dyn std::future::Future<Output = ()> = &write;
        drop(write);

        let mut borrowed_write =
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(6);
        let write_ref = context.write_ref(&mut borrowed_write);
        let _: &dyn std::future::Future<Output = ()> = &write_ref;
        drop(write_ref);
    }
}

async fn assert_checked_server_method_signatures() {
    let runtime = RuntimeContext::try_from_current("transport-public-api-v1-checked-signatures").unwrap();
    let config = Arc::new(ServerConfig::default());
    let mut config_server = TransportServer::new(config.clone(), runtime.service_context("config-server"));
    let config_future =
        config_server.try_run_with_shutdown_report(DefaultRequestProcessor, None, std::future::pending::<()>());
    let _: &dyn std::future::Future<Output = Result<ShutdownReport, ServerStartError>> = &config_future;
    drop(config_future);

    let mut config_startup_server = TransportServer::new(config.clone(), runtime.service_context("config-startup"));
    let (config_startup_tx, _config_startup_rx) = oneshot::channel();
    let config_startup_future = config_startup_server.try_run_with_shutdown_report_and_startup(
        DefaultRequestProcessor,
        None,
        std::future::pending::<()>(),
        config_startup_tx,
    );
    let _: &dyn std::future::Future<Output = Result<ShutdownReport, ServerStartError>> = &config_startup_future;
    drop(config_startup_future);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut bound_server = TransportServer::new(config.clone(), runtime.service_context("bound-server"));
    let bound_future = bound_server.try_serve_bound_listener_until(
        listener,
        DefaultRequestProcessor,
        None,
        None,
        std::future::pending::<()>(),
    );
    let _: &dyn std::future::Future<Output = Result<ShutdownReport, ServerStartError>> = &bound_future;
    drop(bound_future);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut bound_startup_server = TransportServer::new(config, runtime.service_context("bound-startup"));
    let (bound_startup_tx, _bound_startup_rx) = oneshot::channel();
    let bound_startup_future = bound_startup_server.try_serve_bound_listener_until_with_startup(
        listener,
        DefaultRequestProcessor,
        None,
        None,
        std::future::pending::<()>(),
        bound_startup_tx,
    );
    let _: &dyn std::future::Future<Output = Result<ShutdownReport, ServerStartError>> = &bound_startup_future;
    drop(bound_startup_future);
}

#[test]
fn prelude_reexports_the_curated_composition_root_surface() {
    let _ = PreludeServerConfig::default();
    let _ = PreludeTransportClientConfig::default();
    let _ = PreludeRequestDeadline::after(Duration::from_millis(1));
    let _: Option<PreludeOneShotTransportClient> = None;
    let _: Option<PreludeTransportClient<DefaultRequestProcessor>> = None;
    let _: Option<PreludeRemotingClient<DefaultRequestProcessor>> = None;
    let _: Option<PreludeTransportServer<DefaultRequestProcessor>> = None;
    assert_prelude_processor::<DefaultRequestProcessor>();
}

#[test]
fn versioned_api_exposes_response_contract_types_without_prelude_exports() {
    assert_versioned_response_contract(None, None, ResponseError::DeadlineExceeded);
    let _ = assert_v1_response_context_method_signatures as fn(&ConnectionHandlerContext);
    let _ = assert_v1_request_context_method_signatures as fn(&RequestContext);
    let _: Option<ResponseTerminalState> = Some(ResponseTerminalState::Completed);
    let _: Option<ResponseDisposition> = Some(ResponseDisposition::TransportWritten);
    let _: Option<WriteProgress> = Some(WriteProgress::NotStarted);
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
    assert_stable_shutdown_methods(&transport);
    assert_cached_connection_state(transport.reconcile_cached_connection(&"127.0.0.1:10911".into()));
    assert_legacy_compatibility_methods(&transport);

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
    let _: Option<ServerStartError> = None;
    assert_checked_server_method_signatures().await;
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
