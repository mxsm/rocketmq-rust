//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::prelude::RemotingDeserializable;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;

use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::processor::MessagingProcessor;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryRouteRequest;
use crate::service::ResourceIdentity;

pub struct ProxyRemotingDispatcher<P> {
    processor: Arc<P>,
}

impl<P> ProxyRemotingDispatcher<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn new(processor: Arc<P>) -> Self {
        Self { processor }
    }

    pub async fn dispatch(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        match RequestCode::from(request.code()) {
            RequestCode::GetRouteinfoByTopic => self.dispatch_query_route(context, request).await,
            RequestCode::QueryAssignment => self.dispatch_query_assignment(context, request).await,
            _ => RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::RequestCodeNotSupported,
                format!(
                    "proxy remoting ingress does not support request code {}",
                    request.code()
                ),
            )
            .set_opaque(request.opaque()),
        }
    }

    async fn dispatch_query_route(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetRouteInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode queryRoute header", error),
        };
        let plan = match self
            .processor
            .query_route(
                context,
                QueryRouteRequest {
                    topic: ResourceIdentity::new(String::new(), header.topic.to_string()),
                    endpoints: Vec::new(),
                },
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };

        let body = match plan.route.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode topic route response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(request.opaque())
    }

    async fn dispatch_query_assignment(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let body = match request.body() {
            Some(body) => body,
            None => {
                return RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    "queryAssignment request body is missing",
                )
                .set_opaque(request.opaque())
            }
        };
        let request_body = match QueryAssignmentRequestBody::decode(body.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                return transport_error_response(request.opaque(), "decode queryAssignment request body", error)
            }
        };
        let plan = match self
            .processor
            .query_assignment(
                context,
                QueryAssignmentRequest {
                    topic: ResourceIdentity::new(String::new(), request_body.topic.to_string()),
                    group: ResourceIdentity::new(String::new(), request_body.consumer_group.to_string()),
                    endpoints: Vec::new(),
                },
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };

        let response_body = QueryAssignmentResponseBody {
            message_queue_assignments: plan.assignments.unwrap_or_default().into_iter().collect(),
        };
        let encoded = match response_body.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode queryAssignment response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }
}

fn decode_error_response(opaque: i32, operation: &'static str, error: RocketMQError) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        format!("{operation} failed: {error}"),
    )
    .set_opaque(opaque)
}

fn transport_error_response(opaque: i32, operation: &'static str, error: impl std::fmt::Display) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        format!("{operation} failed: {error}"),
    )
    .set_opaque(opaque)
}

fn proxy_error_response(opaque: i32, error: ProxyError) -> RemotingCommand {
    let code = match &error {
        ProxyError::RocketMQ(RocketMQError::TopicNotExist { .. })
        | ProxyError::RocketMQ(RocketMQError::RouteNotFound { .. }) => ResponseCode::TopicNotExist,
        ProxyError::RocketMQ(RocketMQError::SubscriptionGroupNotExist { .. }) => {
            ResponseCode::SubscriptionGroupNotExist
        }
        ProxyError::RocketMQ(RocketMQError::BrokerPermissionDenied { .. })
        | ProxyError::RocketMQ(RocketMQError::TopicSendingForbidden { .. }) => ResponseCode::NoPermission,
        ProxyError::TooManyRequests { .. } => ResponseCode::SystemBusy,
        _ => ResponseCode::SystemError,
    };
    RemotingCommand::create_response_command_with_code_remark(code, error.to_string()).set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::prelude::RemotingDeserializable;
    use rocketmq_remoting::prelude::RemotingSerializable;
    use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
    use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
    use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

    use super::ProxyRemotingDispatcher;
    use crate::context::ProxyContext;
    use crate::processor::DefaultMessagingProcessor;
    use crate::service::DefaultAssignmentService;
    use crate::service::DefaultConsumerService;
    use crate::service::DefaultTransactionService;
    use crate::service::LocalServiceManager;
    use crate::service::ResourceIdentity;
    use crate::service::StaticMessageService;
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;

    fn test_context() -> ProxyContext {
        ProxyContext::for_internal_client("Remoting", "remoting-client")
    }

    #[tokio::test]
    async fn dispatch_query_route_returns_success_response() {
        let route_service = StaticRouteService::default();
        route_service.insert(
            ResourceIdentity::new(String::new(), "TopicA"),
            TopicRouteData {
                queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0)],
                broker_datas: vec![BrokerData::new(
                    CheetahString::from("cluster-a"),
                    CheetahString::from("broker-a"),
                    HashMap::from([(0_u64, CheetahString::from("127.0.0.1:10911"))]),
                    None,
                )],
                ..Default::default()
            },
        );
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(
            LocalServiceManager::with_services(
                Arc::new(route_service),
                Arc::new(StaticMetadataService::default()),
                Arc::new(DefaultAssignmentService),
                Arc::new(StaticMessageService::default()),
                Arc::new(DefaultConsumerService),
                Arc::new(DefaultTransactionService),
            ),
        )));
        let dispatcher = ProxyRemotingDispatcher::new(processor);
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TopicA", Some(true)),
        );
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(response.body().is_some());
    }

    #[tokio::test]
    async fn dispatch_query_assignment_returns_supported_response() {
        let route_service = StaticRouteService::default();
        route_service.insert(
            ResourceIdentity::new(String::new(), "TopicA"),
            TopicRouteData::default(),
        );
        let assignment_service = Arc::new(TestAssignmentService);
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(
            LocalServiceManager::with_services(
                Arc::new(route_service),
                Arc::new(StaticMetadataService::default()),
                assignment_service,
                Arc::new(StaticMessageService::default()),
                Arc::new(DefaultConsumerService),
                Arc::new(DefaultTransactionService),
            ),
        )));
        let dispatcher = ProxyRemotingDispatcher::new(processor);
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("TopicA"),
            consumer_group: CheetahString::from("GroupA"),
            client_id: CheetahString::from("client-a"),
            strategy_name: CheetahString::from("AVG"),
            message_model: rocketmq_remoting::protocol::heartbeat::message_model::MessageModel::Clustering,
        }
        .encode()
        .expect("assignment request body should encode");
        let request = RemotingCommand::new_request(RequestCode::QueryAssignment, body);

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let decoded = QueryAssignmentResponseBody::decode(
            response
                .body()
                .expect("queryAssignment response body should exist")
                .as_ref(),
        )
        .expect("queryAssignment response should decode");
        assert_eq!(decoded.message_queue_assignments.len(), 1);
    }

    #[derive(Default)]
    struct TestAssignmentService;

    #[async_trait::async_trait]
    impl crate::service::AssignmentService for TestAssignmentService {
        async fn query_assignment(
            &self,
            _context: &ProxyContext,
            _topic: &ResourceIdentity,
            _group: &ResourceIdentity,
            _endpoints: &[crate::context::ResolvedEndpoint],
        ) -> crate::error::ProxyResult<Option<Vec<MessageQueueAssignment>>> {
            Ok(Some(vec![MessageQueueAssignment {
                message_queue: Some(
                    rocketmq_common::common::message::message_queue::MessageQueue::from_parts(
                        "TopicA",
                        CheetahString::from("broker-a"),
                        0,
                    ),
                ),
                mode: MessageRequestMode::Pull,
                attachments: None,
            }]))
        }
    }
}
