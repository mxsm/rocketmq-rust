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

//! Tests for categorized re-exports in headers and bodies modules

#[test]
fn test_protocol_headers_reexports() {
    // Test importing from protocol::headers module
    use rocketmq_remoting::protocol::headers::client::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::headers::message::SendMessageRequestHeader;
    use rocketmq_remoting::protocol::headers::namesrv::RegisterBrokerRequestHeader;
    use rocketmq_remoting::protocol::headers::namesrv::UnRegisterBrokerRequestHeader;
    use rocketmq_remoting::protocol::headers::polling::PullMessageRequestHeader;

    // Verify types are accessible (just checking type resolution)
    let _send_header: Option<SendMessageRequestHeader> = None;
    let _pull_header: Option<PullMessageRequestHeader> = None;
    let _route_header: Option<GetRouteInfoRequestHeader> = None;
    let _reg_header: Option<RegisterBrokerRequestHeader> = None;
    let _unreg_header: Option<UnRegisterBrokerRequestHeader> = None;
}

#[test]
fn test_protocol_bodies_reexports() {
    // Test importing from protocol::bodies module
    use rocketmq_remoting::protocol::bodies::broker::ClusterInfo;
    use rocketmq_remoting::protocol::bodies::consumer::ConsumerConnection;
    use rocketmq_remoting::protocol::bodies::message::BatchAckMessageRequestBody;

    // Verify types are accessible (just checking type resolution)
    let _ack_body: Option<BatchAckMessageRequestBody> = None;
    let _cluster_info: Option<ClusterInfo> = None;
    let _consumer_conn: Option<ConsumerConnection> = None;
}

#[test]
fn test_protocol_top_level_reexports() {
    // Test importing top-level protocol types
    use rocketmq_remoting::protocol::RemotingCommand;

    // Verify RemotingCommand is accessible
    let _command: Option<RemotingCommand> = None;
}

#[test]
fn test_headers_top_level_reexports() {
    // Test importing most common headers directly from protocol::headers
    use rocketmq_remoting::protocol::headers::AckMessageRequestHeader;
    use rocketmq_remoting::protocol::headers::CreateTopicRequestHeader;
    use rocketmq_remoting::protocol::headers::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::headers::HeartbeatRequestHeader;
    use rocketmq_remoting::protocol::headers::PullMessageRequestHeader;
    use rocketmq_remoting::protocol::headers::SendMessageRequestHeader;

    // Verify types are accessible (just checking type resolution)
    let _ack_header: Option<AckMessageRequestHeader> = None;
    let _create_header: Option<CreateTopicRequestHeader> = None;
    let _route_header: Option<GetRouteInfoRequestHeader> = None;
    let _heartbeat_header: Option<HeartbeatRequestHeader> = None;
    let _pull_header: Option<PullMessageRequestHeader> = None;
    let _send_header: Option<SendMessageRequestHeader> = None;
}

#[test]
fn test_bodies_top_level_reexports() {
    // Test importing most common bodies directly from protocol::bodies
    use rocketmq_remoting::protocol::bodies::BatchAckMessageRequestBody;
    use rocketmq_remoting::protocol::bodies::ClusterInfo;
    use rocketmq_remoting::protocol::bodies::ConsumerConnection;

    // Verify types are accessible (just checking type resolution)
    let _ack_body: Option<BatchAckMessageRequestBody> = None;
    let _cluster_info: Option<ClusterInfo> = None;
    let _consumer_conn: Option<ConsumerConnection> = None;
}

#[test]
fn test_backward_compatibility() {
    // Verify that old import paths still work
    use rocketmq_remoting::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
    use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;

    // Old paths should still work (just checking type resolution)
    let _pull_header: Option<PullMessageRequestHeader> = None;
    let _send_header: Option<SendMessageRequestHeader> = None;
    let _ack_body: Option<BatchAckMessageRequestBody> = None;
}

#[test]
fn test_categorized_imports_by_functionality() {
    // Test importing related headers together using category modules
    use rocketmq_remoting::protocol::headers::ack::AckMessageRequestHeader;
    use rocketmq_remoting::protocol::headers::client::HeartbeatRequestHeader;
    use rocketmq_remoting::protocol::headers::message::SendMessageRequestHeader;
    use rocketmq_remoting::protocol::headers::polling::PullMessageRequestHeader;

    // Verify types are accessible (just checking type resolution)
    let _ack: Option<AckMessageRequestHeader> = None;
    let _heartbeat: Option<HeartbeatRequestHeader> = None;
    let _send: Option<SendMessageRequestHeader> = None;
    let _pull: Option<PullMessageRequestHeader> = None;
}

#[test]
fn test_static_topic_reexports() {
    // Test importing static_topic types through categorized re-exports
    use rocketmq_remoting::protocol::bodies::static_topic::LogicQueueMappingItem;
    use rocketmq_remoting::protocol::bodies::static_topic::MappingAllocator;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicConfigAndQueueMapping;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicQueueMappingContext;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicQueueMappingDetail;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicQueueMappingInfo;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicQueueMappingOne;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicQueueMappingUtils;
    use rocketmq_remoting::protocol::bodies::static_topic::TopicRemappingDetailWrapper;

    // Verify all types are accessible
    let _item: Option<LogicQueueMappingItem> = None;
    let _config: Option<TopicConfigAndQueueMapping> = None;
    let _context: Option<TopicQueueMappingContext> = None;
    let _detail: Option<TopicQueueMappingDetail> = None;
    let _info: Option<TopicQueueMappingInfo> = None;
    let _one: Option<TopicQueueMappingOne> = None;
    let _allocator: Option<MappingAllocator> = None;
    let _utils: Option<TopicQueueMappingUtils> = None;
    let _wrapper: Option<TopicRemappingDetailWrapper> = None;
}
