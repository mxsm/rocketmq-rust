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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cheetah_string::CheetahString;
use futures::StreamExt;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_proxy::context::ProxyContext;
use rocketmq_proxy::context::ResolvedEndpoint;
use rocketmq_proxy::proto::v2;
use rocketmq_proxy::proto::v2::messaging_service_client::MessagingServiceClient;
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
use rocketmq_proxy::MetadataService;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyError;
use rocketmq_proxy::ProxyPayloadStatus;
use rocketmq_proxy::ProxyResult;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_proxy::ProxyTopicMessageType;
use rocketmq_proxy::PullMessagePlan;
use rocketmq_proxy::PullMessageRequest;
use rocketmq_proxy::QueryOffsetPlan;
use rocketmq_proxy::QueryOffsetRequest;
use rocketmq_proxy::ReceiveMessagePlan;
use rocketmq_proxy::ReceiveMessageRequest;
use rocketmq_proxy::ReceivedMessage;
use rocketmq_proxy::ResourceIdentity;
use rocketmq_proxy::RouteService;
use rocketmq_proxy::StaticMessageService;
use rocketmq_proxy::StaticMetadataService;
use rocketmq_proxy::StaticRouteService;
use rocketmq_proxy::SubscriptionGroupMetadata;
use rocketmq_proxy::UpdateOffsetPlan;
use rocketmq_proxy::UpdateOffsetRequest;
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

#[derive(Default)]
struct StreamingConsumerService;

#[async_trait]
impl ConsumerService for StreamingConsumerService {
    async fn receive_message(
        &self,
        _context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from(request.target.topic.to_string()));
        message.set_body(Some(Bytes::from_static(b"integration-body")));
        message.set_msg_id(CheetahString::from("integration-msg-id"));
        message.set_queue_id(request.target.queue_id);
        message.set_queue_offset(7);
        rocketmq_common::common::message::MessageTrait::put_property(
            &mut message,
            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
            CheetahString::from("integration-receipt-handle"),
        );

        Ok(ReceiveMessagePlan {
            status: ProxyPayloadStatus::new(v2::Code::Ok as i32, "OK"),
            delivery_timestamp_ms: Some(1_710_000_000_000),
            messages: vec![ReceivedMessage {
                message,
                invisible_duration: Duration::from_secs(30),
            }],
        })
    }

    async fn pull_message(
        &self,
        _context: &ProxyContext,
        _request: &PullMessageRequest,
    ) -> ProxyResult<PullMessagePlan> {
        Err(ProxyError::not_implemented("integration pull"))
    }

    async fn ack_message(
        &self,
        _context: &ProxyContext,
        _request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        Err(ProxyError::not_implemented("integration ack"))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        _context: &ProxyContext,
        _request: &ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        Err(ProxyError::not_implemented("integration dlq"))
    }

    async fn change_invisible_duration(
        &self,
        _context: &ProxyContext,
        _request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        Err(ProxyError::not_implemented("integration change invisible"))
    }

    async fn update_offset(
        &self,
        _context: &ProxyContext,
        _request: &UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan> {
        Err(ProxyError::not_implemented("integration update offset"))
    }

    async fn get_offset(&self, _context: &ProxyContext, _request: &GetOffsetRequest) -> ProxyResult<GetOffsetPlan> {
        Err(ProxyError::not_implemented("integration get offset"))
    }

    async fn query_offset(
        &self,
        _context: &ProxyContext,
        _request: &QueryOffsetRequest,
    ) -> ProxyResult<QueryOffsetPlan> {
        Err(ProxyError::not_implemented("integration query offset"))
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
            assert_eq!(message.body, b"integration-body");
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

    (shutdown_tx, server_task)
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
            body: Bytes::from_static(b"integration-body").to_vec(),
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
