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

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rocketmq_proxy::context::ProxyContext;
use rocketmq_proxy::context::ResolvedEndpoint;
use rocketmq_proxy::ClusterServiceManager;
use rocketmq_proxy::DefaultAssignmentService;
use rocketmq_proxy::DefaultConsumerService;
use rocketmq_proxy::DefaultMessageService;
use rocketmq_proxy::DefaultTransactionService;
use rocketmq_proxy::GrpcConfig;
use rocketmq_proxy::MetadataService;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyResult;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_proxy::ProxyTopicMessageType;
use rocketmq_proxy::RemotingConfig;
use rocketmq_proxy::ResourceIdentity;
use rocketmq_proxy::RouteService;
use rocketmq_proxy::SubscriptionGroupMetadata;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::connection::Connection;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use tokio::sync::oneshot;
use tokio::time::timeout;

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

#[async_trait]
impl RouteService for RecordingRouteService {
    async fn query_route(
        &self,
        context: &ProxyContext,
        _topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        self.observed
            .lock()
            .expect("route service mutex poisoned")
            .push(ObservedRouteContext {
                local_addr: context.local_addr().map(str::to_owned),
                remote_addr: context.remote_addr().map(str::to_owned),
            });
        Ok(TopicRouteData::default())
    }
}

#[derive(Default)]
struct StaticMetadataService;

#[async_trait]
impl MetadataService for StaticMetadataService {
    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        Ok(ProxyTopicMessageType::Normal)
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(None)
    }
}

#[tokio::test]
async fn query_route_over_remoting_integration_injects_transport_context() {
    let route_service = Arc::new(RecordingRouteService::default());
    let service_manager = Arc::new(ClusterServiceManager::with_services(
        route_service.clone(),
        Arc::new(StaticMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    ));

    let grpc_addr = free_local_addr();
    let remoting_addr = free_local_addr();
    let runtime = ProxyRuntime::builder(ProxyConfig {
        grpc: GrpcConfig {
            listen_addr: grpc_addr.to_string(),
            ..GrpcConfig::default()
        },
        remoting: RemotingConfig {
            enabled: true,
            listen_addr: remoting_addr.to_string(),
        },
        ..ProxyConfig::default()
    })
    .with_service_manager(service_manager)
    .build();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        runtime
            .serve_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut connection = connect_remoting_with_retry(remoting_addr).await;
    let request = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new("TopicA", None),
    );
    let opaque = request.opaque();
    connection
        .send_command(request)
        .await
        .expect("remoting request should be sent");

    let response = timeout(Duration::from_secs(3), async {
        loop {
            match connection.receive_command().await {
                Some(Ok(response)) => break response,
                Some(Err(error)) => panic!("failed to decode remoting response: {error}"),
                None => continue,
            }
        }
    })
    .await
    .expect("remoting response should arrive before timeout");

    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(response.opaque(), opaque);

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );

    let observed = route_service.observed();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0].local_addr.as_deref(),
        Some(remoting_addr.to_string().as_str())
    );
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
async fn request_code_not_supported_over_remoting_integration_returns_compatible_response() {
    let service_manager = Arc::new(ClusterServiceManager::with_services(
        Arc::new(RecordingRouteService::default()),
        Arc::new(StaticMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    ));

    let grpc_addr = free_local_addr();
    let remoting_addr = free_local_addr();
    let runtime = ProxyRuntime::builder(ProxyConfig {
        grpc: GrpcConfig {
            listen_addr: grpc_addr.to_string(),
            ..GrpcConfig::default()
        },
        remoting: RemotingConfig {
            enabled: true,
            listen_addr: remoting_addr.to_string(),
        },
        ..ProxyConfig::default()
    })
    .with_service_manager(service_manager)
    .build();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        runtime
            .serve_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut connection = connect_remoting_with_retry(remoting_addr).await;
    let request = RemotingCommand::create_remoting_command(RequestCode::AuthCreateUser).set_opaque(88);
    let opaque = request.opaque();
    connection
        .send_command(request)
        .await
        .expect("unsupported remoting request should be sent");

    let response = receive_remoting_response(&mut connection).await;
    assert_eq!(response.code(), ResponseCode::RequestCodeNotSupported as i32);
    assert_eq!(response.opaque(), opaque);
    assert!(
        response
            .remark()
            .is_some_and(|remark| remark.contains("does not support request code 3001")),
        "unsupported response should explain proxy remoting coverage, got {:?}",
        response.remark()
    );

    let _ = shutdown_tx.send(());
    let serve_result = server_task.await.expect("server task should join");
    assert!(
        serve_result.is_ok(),
        "server should shut down cleanly: {serve_result:?}"
    );
}

async fn connect_remoting_with_retry(addr: SocketAddr) -> Connection {
    let mut last_error = None;
    for _ in 0..20 {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => return Connection::new(stream),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }

    panic!("remoting client failed to connect to {addr}: {last_error:?}");
}

async fn receive_remoting_response(connection: &mut Connection) -> RemotingCommand {
    timeout(Duration::from_secs(3), async {
        loop {
            match connection.receive_command().await {
                Some(Ok(response)) => break response,
                Some(Err(error)) => panic!("failed to decode remoting response: {error}"),
                None => continue,
            }
        }
    })
    .await
    .expect("remoting response should arrive before timeout")
}

fn free_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local port probe");
    let addr = listener.local_addr().expect("discover local addr");
    drop(listener);
    addr
}
