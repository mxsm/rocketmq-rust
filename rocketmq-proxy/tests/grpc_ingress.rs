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
use rocketmq_proxy::proto::v2;
use rocketmq_proxy::proto::v2::messaging_service_client::MessagingServiceClient;
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
use rocketmq_proxy::ResourceIdentity;
use rocketmq_proxy::RouteService;
use rocketmq_proxy::StaticMetadataService;
use rocketmq_proxy::StaticRouteService;
use rocketmq_proxy::SubscriptionGroupMetadata;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;
use tonic::Request;

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
struct NormalMetadataService;

#[async_trait]
impl MetadataService for NormalMetadataService {
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

async fn spawn_runtime(
    service_manager: Arc<dyn rocketmq_proxy::ServiceManager>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<rocketmq_proxy::ProxyResult<()>>,
) {
    let port_probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local port probe");
    let listen_addr = port_probe.local_addr().expect("discover local addr");
    drop(port_probe);

    let runtime = ProxyRuntime::builder(ProxyConfig {
        grpc: GrpcConfig {
            listen_addr: listen_addr.to_string(),
            ..GrpcConfig::default()
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

    (listen_addr, shutdown_tx, server_task)
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
