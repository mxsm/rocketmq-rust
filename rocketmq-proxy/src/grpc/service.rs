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

use std::pin::Pin;
use std::sync::Arc;

use futures::stream;
use futures::Stream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::adapter;
use crate::processor::MessagingProcessor;
use crate::proto::v2;
use crate::session::ClientSessionRegistry;
use crate::status::ProxyStatusMapper;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
struct ExecutionGuards {
    route: Arc<Semaphore>,
    producer: Arc<Semaphore>,
    consumer: Arc<Semaphore>,
    client_manager: Arc<Semaphore>,
}

impl ExecutionGuards {
    fn from_config(config: &ProxyConfig) -> Self {
        Self {
            route: Arc::new(Semaphore::new(config.runtime.route_permits)),
            producer: Arc::new(Semaphore::new(config.runtime.producer_permits)),
            consumer: Arc::new(Semaphore::new(config.runtime.consumer_permits)),
            client_manager: Arc::new(Semaphore::new(config.runtime.client_manager_permits)),
        }
    }

    fn try_route(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.route
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("route"))
    }

    fn try_consumer(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.consumer
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("consumer"))
    }

    fn try_producer(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.producer
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("producer"))
    }

    fn try_client_manager(&self) -> ProxyResult<OwnedSemaphorePermit> {
        self.client_manager
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::too_many_requests("client-manager"))
    }
}

#[derive(Clone)]
pub struct ProxyGrpcService<P> {
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    guards: ExecutionGuards,
}

impl<P> ProxyGrpcService<P> {
    pub fn new(config: Arc<ProxyConfig>, processor: Arc<P>, sessions: ClientSessionRegistry) -> Self {
        Self {
            guards: ExecutionGuards::from_config(config.as_ref()),
            config,
            processor,
            sessions,
        }
    }

    fn context<T>(&self, rpc_name: &'static str, request: &Request<T>) -> ProxyContext {
        ProxyContext::from_grpc_request(rpc_name, request)
    }

    fn status_stream<T>(&self, item: T) -> ResponseStream<T>
    where
        T: Send + 'static,
    {
        Box::pin(stream::iter(vec![Ok(item)]))
    }
}

impl<P> ProxyGrpcService<P>
where
    P: MessagingProcessor + 'static,
{
    fn not_implemented_status(&self, feature: &'static str) -> v2::Status {
        ProxyStatusMapper::from_error(&ProxyError::not_implemented(feature))
    }

    fn validate_client_context<'a>(&self, context: &'a ProxyContext) -> ProxyResult<&'a str> {
        context.require_client_id()
    }

    fn validate_heartbeat_request(&self, context: &ProxyContext, client_type: i32) -> ProxyResult<()> {
        self.validate_client_context(context)?;

        match v2::ClientType::try_from(client_type) {
            Ok(v2::ClientType::Producer)
            | Ok(v2::ClientType::PushConsumer)
            | Ok(v2::ClientType::SimpleConsumer)
            | Ok(v2::ClientType::PullConsumer)
            | Ok(v2::ClientType::LitePushConsumer)
            | Ok(v2::ClientType::LiteSimpleConsumer) => Ok(()),
            _ => Err(ProxyError::UnrecognizedClientType(client_type)),
        }
    }

    fn receive_status(status: v2::Status) -> v2::ReceiveMessageResponse {
        v2::ReceiveMessageResponse {
            content: Some(v2::receive_message_response::Content::Status(status)),
        }
    }

    fn pull_status(status: v2::Status) -> v2::PullMessageResponse {
        v2::PullMessageResponse {
            content: Some(v2::pull_message_response::Content::Status(status)),
        }
    }

    fn telemetry_status(status: v2::Status) -> v2::TelemetryCommand {
        v2::TelemetryCommand {
            status: Some(status),
            command: None,
        }
    }
}

#[tonic::async_trait]
impl<P> v2::messaging_service_server::MessagingService for ProxyGrpcService<P>
where
    P: MessagingProcessor + 'static,
{
    type ReceiveMessageStream = ResponseStream<v2::ReceiveMessageResponse>;
    type PullMessageStream = ResponseStream<v2::PullMessageResponse>;
    type TelemetryStream = ResponseStream<v2::TelemetryCommand>;

    async fn query_route(
        &self,
        request: Request<v2::QueryRouteRequest>,
    ) -> Result<Response<v2::QueryRouteResponse>, Status> {
        let context = self.context("QueryRoute", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_query_route_request(self.config.as_ref(), &request))
        {
            Ok(input) => match self.guards.try_route() {
                Ok(_permit) => match self.processor.query_route(&context, input).await {
                    Ok(plan) => adapter::build_query_route_response(&request, &plan),
                    Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_query_route_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<v2::HeartbeatRequest>,
    ) -> Result<Response<v2::HeartbeatResponse>, Status> {
        let context = self.context("Heartbeat", &request);
        let request = request.into_inner();

        let status = match self.guards.try_client_manager() {
            Ok(_permit) => match self.validate_heartbeat_request(&context, request.client_type) {
                Ok(()) => {
                    self.sessions.upsert_from_context(&context);
                    ProxyStatusMapper::ok()
                }
                Err(error) => ProxyStatusMapper::from_error(&error),
            },
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(v2::HeartbeatResponse { status: Some(status) }))
    }

    async fn send_message(
        &self,
        request: Request<v2::SendMessageRequest>,
    ) -> Result<Response<v2::SendMessageResponse>, Status> {
        let context = self.context("SendMessage", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_send_message_request(&context, &request))
        {
            Ok(input) => match self.guards.try_producer() {
                Ok(_permit) => match self.processor.send_message(&context, input.clone()).await {
                    Ok(plan) => adapter::build_send_message_response(&plan, &input),
                    Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_send_message_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn query_assignment(
        &self,
        request: Request<v2::QueryAssignmentRequest>,
    ) -> Result<Response<v2::QueryAssignmentResponse>, Status> {
        let context = self.context("QueryAssignment", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_query_assignment_request(self.config.as_ref(), &request))
        {
            Ok(input) => match self.guards.try_route() {
                Ok(_permit) => match self.processor.query_assignment(&context, input).await {
                    Ok(plan) => adapter::build_query_assignment_response(&request, &plan),
                    Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_query_assignment_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
    }

    async fn receive_message(
        &self,
        request: Request<v2::ReceiveMessageRequest>,
    ) -> Result<Response<Self::ReceiveMessageStream>, Status> {
        let context = self.context("ReceiveMessage", &request);
        let status = match self
            .validate_client_context(&context)
            .and_then(|_| self.guards.try_consumer().map(|_| ()))
        {
            Ok(()) => self.not_implemented_status("ReceiveMessage"),
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(self.status_stream(Self::receive_status(status))))
    }

    async fn ack_message(
        &self,
        _request: Request<v2::AckMessageRequest>,
    ) -> Result<Response<v2::AckMessageResponse>, Status> {
        Ok(Response::new(v2::AckMessageResponse {
            status: Some(self.not_implemented_status("AckMessage")),
            entries: Vec::new(),
        }))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        _request: Request<v2::ForwardMessageToDeadLetterQueueRequest>,
    ) -> Result<Response<v2::ForwardMessageToDeadLetterQueueResponse>, Status> {
        Ok(Response::new(v2::ForwardMessageToDeadLetterQueueResponse {
            status: Some(self.not_implemented_status("ForwardMessageToDeadLetterQueue")),
        }))
    }

    async fn pull_message(
        &self,
        request: Request<v2::PullMessageRequest>,
    ) -> Result<Response<Self::PullMessageStream>, Status> {
        let context = self.context("PullMessage", &request);
        let status = match self
            .validate_client_context(&context)
            .and_then(|_| self.guards.try_consumer().map(|_| ()))
        {
            Ok(()) => self.not_implemented_status("PullMessage"),
            Err(error) => ProxyStatusMapper::from_error(&error),
        };
        Ok(Response::new(self.status_stream(Self::pull_status(status))))
    }

    async fn update_offset(
        &self,
        _request: Request<v2::UpdateOffsetRequest>,
    ) -> Result<Response<v2::UpdateOffsetResponse>, Status> {
        Ok(Response::new(v2::UpdateOffsetResponse {
            status: Some(self.not_implemented_status("UpdateOffset")),
        }))
    }

    async fn get_offset(
        &self,
        _request: Request<v2::GetOffsetRequest>,
    ) -> Result<Response<v2::GetOffsetResponse>, Status> {
        Ok(Response::new(v2::GetOffsetResponse {
            status: Some(self.not_implemented_status("GetOffset")),
            offset: 0,
        }))
    }

    async fn query_offset(
        &self,
        _request: Request<v2::QueryOffsetRequest>,
    ) -> Result<Response<v2::QueryOffsetResponse>, Status> {
        Ok(Response::new(v2::QueryOffsetResponse {
            status: Some(self.not_implemented_status("QueryOffset")),
            offset: 0,
        }))
    }

    async fn end_transaction(
        &self,
        _request: Request<v2::EndTransactionRequest>,
    ) -> Result<Response<v2::EndTransactionResponse>, Status> {
        Ok(Response::new(v2::EndTransactionResponse {
            status: Some(self.not_implemented_status("EndTransaction")),
        }))
    }

    async fn telemetry(
        &self,
        request: Request<tonic::Streaming<v2::TelemetryCommand>>,
    ) -> Result<Response<Self::TelemetryStream>, Status> {
        let context = self.context("Telemetry", &request);
        let status = match self
            .validate_client_context(&context)
            .and_then(|_| self.guards.try_client_manager().map(|_| ()))
        {
            Ok(()) => {
                self.sessions.upsert_from_context(&context);
                self.not_implemented_status("Telemetry")
            }
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(self.status_stream(Self::telemetry_status(status))))
    }

    async fn notify_client_termination(
        &self,
        request: Request<v2::NotifyClientTerminationRequest>,
    ) -> Result<Response<v2::NotifyClientTerminationResponse>, Status> {
        let context = self.context("NotifyClientTermination", &request);
        let status = match self
            .guards
            .try_client_manager()
            .and_then(|_| self.validate_client_context(&context))
        {
            Ok(client_id) => {
                self.sessions.remove(client_id);
                ProxyStatusMapper::ok()
            }
            Err(error) => ProxyStatusMapper::from_error(&error),
        };

        Ok(Response::new(v2::NotifyClientTerminationResponse {
            status: Some(status),
        }))
    }

    async fn change_invisible_duration(
        &self,
        _request: Request<v2::ChangeInvisibleDurationRequest>,
    ) -> Result<Response<v2::ChangeInvisibleDurationResponse>, Status> {
        Ok(Response::new(v2::ChangeInvisibleDurationResponse {
            status: Some(self.not_implemented_status("ChangeInvisibleDuration")),
            receipt_handle: String::new(),
        }))
    }

    async fn recall_message(
        &self,
        _request: Request<v2::RecallMessageRequest>,
    ) -> Result<Response<v2::RecallMessageResponse>, Status> {
        Ok(Response::new(v2::RecallMessageResponse {
            status: Some(self.not_implemented_status("RecallMessage")),
            message_id: String::new(),
        }))
    }

    async fn sync_lite_subscription(
        &self,
        _request: Request<v2::SyncLiteSubscriptionRequest>,
    ) -> Result<Response<v2::SyncLiteSubscriptionResponse>, Status> {
        Ok(Response::new(v2::SyncLiteSubscriptionResponse {
            status: Some(self.not_implemented_status("SyncLiteSubscription")),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use super::ProxyGrpcService;
    use crate::config::ProxyConfig;
    use crate::processor::DefaultMessagingProcessor;
    use crate::proto::v2;
    use crate::proto::v2::messaging_service_server::MessagingService;
    use crate::service::ClusterServiceManager;
    use crate::service::ProxyTopicMessageType;
    use crate::service::ResourceIdentity;
    use crate::service::StaticMessageService;
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;
    use crate::service::SubscriptionGroupMetadata;
    use crate::session::ClientSessionRegistry;

    fn test_service(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        test_service_with_message_service(
            route_service,
            metadata_service,
            Arc::new(crate::service::DefaultMessageService),
        )
    }

    fn test_service_with_message_service(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
        message_service: Arc<dyn crate::service::MessageService>,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        let manager = ClusterServiceManager::with_services(
            Arc::new(route_service),
            Arc::new(metadata_service),
            Arc::new(crate::service::DefaultAssignmentService),
            message_service,
        );
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(manager)));
        ProxyGrpcService::new(
            Arc::new(ProxyConfig::default()),
            processor,
            ClientSessionRegistry::default(),
        )
    }

    fn sample_route() -> TopicRouteData {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));

        TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 1, 1, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn query_route_returns_route_entries() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());

        let metadata_service = StaticMetadataService::default();
        metadata_service.set_topic_message_type(ResourceIdentity::new("", "TopicA"), ProxyTopicMessageType::Normal);

        let service = test_service(route_service, metadata_service);
        let mut request = Request::new(v2::QueryRouteRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
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
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.query_route(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.message_queues.len(), 1);
    }

    #[tokio::test]
    async fn query_assignment_uses_fifo_group_semantics() {
        let route_service = StaticRouteService::default();
        route_service.insert(ResourceIdentity::new("", "TopicA"), sample_route());

        let metadata_service = StaticMetadataService::default();
        metadata_service.set_subscription_group(
            ResourceIdentity::new("", "GroupA"),
            SubscriptionGroupMetadata {
                consume_message_orderly: true,
                lite_bind_topic: None,
            },
        );

        let service = test_service(route_service, metadata_service);
        let mut request = Request::new(v2::QueryAssignmentRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
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
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.query_assignment(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.assignments[0].message_queue.as_ref().unwrap().id, 0);
    }

    #[tokio::test]
    async fn heartbeat_requires_client_id_header() {
        let service = test_service(StaticRouteService::default(), StaticMetadataService::default());
        let request = Request::new(v2::HeartbeatRequest {
            group: None,
            client_type: v2::ClientType::Producer as i32,
        });

        let response = service.heartbeat(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::ClientIdRequired as i32);
    }

    #[tokio::test]
    async fn send_message_returns_send_result_entry() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
        );
        let mut request = Request::new(v2::SendMessageRequest {
            messages: vec![v2::Message {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                user_properties: HashMap::new(),
                system_properties: Some(v2::SystemProperties {
                    message_id: "msg-1".to_owned(),
                    body_encoding: v2::Encoding::Identity as i32,
                    ..Default::default()
                }),
                body: Bytes::from_static(b"hello").to_vec(),
            }],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.send_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].message_id, "msg-1");
    }

    #[tokio::test]
    async fn send_message_rejects_batch_until_partial_results_are_implemented() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
        );
        let mut request = Request::new(v2::SendMessageRequest {
            messages: vec![
                v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-1".to_owned(),
                        body_encoding: v2::Encoding::Identity as i32,
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"hello").to_vec(),
                },
                v2::Message {
                    topic: Some(v2::Resource {
                        resource_namespace: String::new(),
                        name: "TopicA".to_owned(),
                    }),
                    user_properties: HashMap::new(),
                    system_properties: Some(v2::SystemProperties {
                        message_id: "msg-2".to_owned(),
                        body_encoding: v2::Encoding::Identity as i32,
                        ..Default::default()
                    }),
                    body: Bytes::from_static(b"world").to_vec(),
                },
            ],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.send_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::NotImplemented as i32);
        assert!(response.entries.is_empty());
    }
}
