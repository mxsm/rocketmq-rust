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

    fn items_stream<T>(&self, items: Vec<T>) -> ResponseStream<T>
    where
        T: Send + 'static,
    {
        Box::pin(stream::iter(items.into_iter().map(Ok)))
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
        let request = request.into_inner();
        let responses = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_receive_message_request(&request))
        {
            Ok(input) => match self.guards.try_consumer() {
                Ok(_permit) => {
                    self.sessions.upsert_from_context(&context);
                    match self.processor.receive_message(&context, input).await {
                        Ok(plan) => adapter::build_receive_message_responses(&plan),
                        Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
                    }
                }
                Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_receive_message_responses(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(self.items_stream(responses)))
    }

    async fn ack_message(
        &self,
        request: Request<v2::AckMessageRequest>,
    ) -> Result<Response<v2::AckMessageResponse>, Status> {
        let context = self.context("AckMessage", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_ack_message_request(&request))
        {
            Ok(input) => match self.guards.try_consumer() {
                Ok(_permit) => match self.processor.ack_message(&context, input).await {
                    Ok(plan) => adapter::build_ack_message_response(&plan),
                    Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
                },
                Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_ack_message_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
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
        request: Request<v2::ChangeInvisibleDurationRequest>,
    ) -> Result<Response<v2::ChangeInvisibleDurationResponse>, Status> {
        let context = self.context("ChangeInvisibleDuration", &request);
        let request = request.into_inner();

        let response = match self
            .validate_client_context(&context)
            .and_then(|_| adapter::build_change_invisible_duration_request(&request))
        {
            Ok(input) => match self.guards.try_consumer() {
                Ok(_permit) => match self.processor.change_invisible_duration(&context, input).await {
                    Ok(plan) => adapter::build_change_invisible_duration_response(&plan),
                    Err(error) => {
                        adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error))
                    }
                },
                Err(error) => adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error)),
            },
            Err(error) => adapter::error_change_invisible_duration_response(ProxyStatusMapper::from_error(&error)),
        };

        Ok(Response::new(response))
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

    use async_trait::async_trait;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use futures::StreamExt;
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use super::ProxyGrpcService;
    use crate::config::ProxyConfig;
    use crate::processor::AckMessageRequest;
    use crate::processor::AckMessageResultEntry;
    use crate::processor::ChangeInvisibleDurationPlan;
    use crate::processor::ChangeInvisibleDurationRequest;
    use crate::processor::DefaultMessagingProcessor;
    use crate::processor::ReceiveMessagePlan;
    use crate::processor::ReceiveMessageRequest;
    use crate::processor::ReceivedMessage;
    use crate::processor::SendMessageRequest;
    use crate::processor::SendMessageResultEntry;
    use crate::proto::v2;
    use crate::proto::v2::messaging_service_server::MessagingService;
    use crate::service::ClusterServiceManager;
    use crate::service::ConsumerService;
    use crate::service::DefaultConsumerService;
    use crate::service::MessageService;
    use crate::service::ProxyTopicMessageType;
    use crate::service::ResourceIdentity;
    use crate::service::StaticMessageService;
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;
    use crate::service::SubscriptionGroupMetadata;
    use crate::session::ClientSessionRegistry;
    use crate::status::ProxyPayloadStatus;
    use crate::status::ProxyStatusMapper;

    struct PartialMessageService;

    struct TestConsumerService;

    #[async_trait]
    impl MessageService for PartialMessageService {
        async fn send_message(
            &self,
            _context: &crate::context::ProxyContext,
            request: &SendMessageRequest,
        ) -> crate::error::ProxyResult<Vec<SendMessageResultEntry>> {
            Ok(request
                .messages
                .iter()
                .enumerate()
                .map(|(index, message)| {
                    if index == 0 {
                        let send_result = SendResult::new(
                            SendStatus::SendOk,
                            Some(CheetahString::from(message.client_message_id.as_str())),
                            None,
                            None,
                            0,
                        );
                        SendMessageResultEntry {
                            status: ProxyStatusMapper::from_send_result_payload(&send_result),
                            send_result: Some(send_result),
                        }
                    } else {
                        SendMessageResultEntry {
                            status: ProxyPayloadStatus::new(
                                v2::Code::TopicNotFound as i32,
                                "Topic 'TopicA' does not exist",
                            ),
                            send_result: None,
                        }
                    }
                })
                .collect())
        }
    }

    #[async_trait]
    impl ConsumerService for TestConsumerService {
        async fn receive_message(
            &self,
            _context: &crate::context::ProxyContext,
            _request: &ReceiveMessageRequest,
        ) -> crate::error::ProxyResult<ReceiveMessagePlan> {
            let mut message = MessageExt::default();
            message.set_topic(CheetahString::from("TopicA"));
            message.set_body(Bytes::from_static(b"hello"));
            message.set_msg_id(CheetahString::from("server-msg-id"));
            message.set_queue_id(3);
            message.set_queue_offset(42);
            message.set_reconsume_times(1);
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                CheetahString::from("receipt-handle"),
            );

            Ok(ReceiveMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                delivery_timestamp_ms: Some(1_710_000_000_000),
                messages: vec![ReceivedMessage {
                    message,
                    invisible_duration: std::time::Duration::from_secs(30),
                }],
            })
        }

        async fn ack_message(
            &self,
            _context: &crate::context::ProxyContext,
            request: &AckMessageRequest,
        ) -> crate::error::ProxyResult<Vec<AckMessageResultEntry>> {
            Ok(request
                .entries
                .iter()
                .map(|entry| AckMessageResultEntry {
                    message_id: entry.message_id.clone(),
                    receipt_handle: entry.receipt_handle.clone(),
                    status: ProxyStatusMapper::ok_payload(),
                })
                .collect())
        }

        async fn change_invisible_duration(
            &self,
            _context: &crate::context::ProxyContext,
            request: &ChangeInvisibleDurationRequest,
        ) -> crate::error::ProxyResult<ChangeInvisibleDurationPlan> {
            Ok(ChangeInvisibleDurationPlan {
                status: ProxyStatusMapper::ok_payload(),
                receipt_handle: format!("{}-renewed", request.receipt_handle),
            })
        }
    }

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
        test_service_with_services(
            route_service,
            metadata_service,
            message_service,
            Arc::new(DefaultConsumerService),
        )
    }

    fn test_service_with_services(
        route_service: StaticRouteService,
        metadata_service: StaticMetadataService,
        message_service: Arc<dyn crate::service::MessageService>,
        consumer_service: Arc<dyn crate::service::ConsumerService>,
    ) -> ProxyGrpcService<DefaultMessagingProcessor> {
        let manager = ClusterServiceManager::with_services(
            Arc::new(route_service),
            Arc::new(metadata_service),
            Arc::new(crate::service::DefaultAssignmentService),
            message_service,
            consumer_service,
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
    async fn send_message_accepts_batch_requests() {
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
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.entries.len(), 2);
        assert_eq!(response.entries[0].message_id, "msg-1");
        assert_eq!(response.entries[1].message_id, "msg-2");
    }

    #[tokio::test]
    async fn send_message_uses_multiple_results_for_partial_failures() {
        let service = test_service_with_message_service(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(PartialMessageService),
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
        assert_eq!(response.status.unwrap().code, v2::Code::MultipleResults as i32);
        assert_eq!(response.entries.len(), 2);
        assert_eq!(response.entries[0].status.as_ref().unwrap().code, v2::Code::Ok as i32);
        assert_eq!(
            response.entries[1].status.as_ref().unwrap().code,
            v2::Code::TopicNotFound as i32
        );
    }

    #[tokio::test]
    async fn receive_message_streams_delivery_timestamp_message_and_status() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::ReceiveMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            message_queue: Some(v2::MessageQueue {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                id: 3,
                permission: v2::Permission::ReadWrite as i32,
                broker: Some(v2::Broker {
                    name: "broker-a".to_owned(),
                    id: 0,
                    endpoints: Some(v2::Endpoints {
                        scheme: v2::AddressScheme::IPv4 as i32,
                        addresses: vec![v2::Address {
                            host: "127.0.0.1".to_owned(),
                            port: 10911,
                        }],
                    }),
                }),
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
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let mut stream = service.receive_message(request).await.unwrap().into_inner();
        let responses: Vec<_> = stream.by_ref().collect::<Vec<_>>().await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(
            responses[0].as_ref().unwrap().content,
            Some(v2::receive_message_response::Content::DeliveryTimestamp(_))
        ));
        assert!(matches!(
            responses[1].as_ref().unwrap().content,
            Some(v2::receive_message_response::Content::Message(_))
        ));
        assert_eq!(
            match responses[2].as_ref().unwrap().content.as_ref().unwrap() {
                v2::receive_message_response::Content::Status(status) => status.code,
                _ => 0,
            },
            v2::Code::Ok as i32
        );
    }

    #[tokio::test]
    async fn ack_message_returns_entry_results() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::AckMessageRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            entries: vec![v2::AckMessageEntry {
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                lite_topic: None,
            }],
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.ack_message(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].message_id, "msg-1");
    }

    #[tokio::test]
    async fn change_invisible_duration_returns_new_receipt_handle() {
        let service = test_service_with_services(
            StaticRouteService::default(),
            StaticMetadataService::default(),
            Arc::new(StaticMessageService::with_send_status(SendStatus::SendOk)),
            Arc::new(TestConsumerService),
        );
        let mut request = Request::new(v2::ChangeInvisibleDurationRequest {
            group: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            receipt_handle: "handle-1".to_owned(),
            invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
            message_id: "msg-1".to_owned(),
            lite_topic: None,
            suspend: None,
        });
        request
            .metadata_mut()
            .insert("x-mq-client-id", MetadataValue::from_static("client-a"));

        let response = service.change_invisible_duration(request).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.receipt_handle, "handle-1-renewed");
    }
}
