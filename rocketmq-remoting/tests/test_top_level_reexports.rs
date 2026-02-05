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

//! Tests for top-level re-exports and prelude module functionality

#[test]
fn test_top_level_header_imports() {
    // Test importing headers directly from crate root
    use rocketmq_remoting::AckMessageRequestHeader;
    use rocketmq_remoting::CreateTopicRequestHeader;
    use rocketmq_remoting::DeleteTopicRequestHeader;
    use rocketmq_remoting::GetMaxOffsetResponseHeader;
    use rocketmq_remoting::GetMinOffsetResponseHeader;
    use rocketmq_remoting::GetRouteInfoRequestHeader;
    use rocketmq_remoting::HeartbeatRequestHeader;
    use rocketmq_remoting::PopMessageRequestHeader;
    use rocketmq_remoting::PullMessageRequestHeader;
    use rocketmq_remoting::PullMessageResponseHeader;
    use rocketmq_remoting::RegisterBrokerRequestHeader;
    use rocketmq_remoting::SendMessageRequestHeader;
    use rocketmq_remoting::UnRegisterBrokerRequestHeader;

    // Verify types are accessible
    let _ack: Option<AckMessageRequestHeader> = None;
    let _create: Option<CreateTopicRequestHeader> = None;
    let _delete: Option<DeleteTopicRequestHeader> = None;
    let _max: Option<GetMaxOffsetResponseHeader> = None;
    let _min: Option<GetMinOffsetResponseHeader> = None;
    let _route: Option<GetRouteInfoRequestHeader> = None;
    let _heartbeat: Option<HeartbeatRequestHeader> = None;
    let _pop: Option<PopMessageRequestHeader> = None;
    let _pull: Option<PullMessageRequestHeader> = None;
    let _pull_resp: Option<PullMessageResponseHeader> = None;
    let _reg: Option<RegisterBrokerRequestHeader> = None;
    let _send: Option<SendMessageRequestHeader> = None;
    let _unreg: Option<UnRegisterBrokerRequestHeader> = None;
}

#[test]
fn test_top_level_body_imports() {
    // Test importing bodies directly from crate root
    use rocketmq_remoting::BatchAckMessageRequestBody;
    use rocketmq_remoting::ClusterInfo;
    use rocketmq_remoting::ConsumerConnection;
    use rocketmq_remoting::ConsumerRunningInfo;
    use rocketmq_remoting::GetConsumerListByGroupResponseBody;
    use rocketmq_remoting::KVTable;
    use rocketmq_remoting::LockBatchRequestBody;
    use rocketmq_remoting::LockBatchResponseBody;
    use rocketmq_remoting::QueryAssignmentResponseBody;
    use rocketmq_remoting::SubscriptionGroupWrapper;
    use rocketmq_remoting::TopicConfigSerializeWrapper;

    // Verify types are accessible
    let _ack_body: Option<BatchAckMessageRequestBody> = None;
    let _cluster: Option<ClusterInfo> = None;
    let _consumer_conn: Option<ConsumerConnection> = None;
    let _consumer_running: Option<ConsumerRunningInfo> = None;
    let _consumer_list: Option<GetConsumerListByGroupResponseBody> = None;
    let _kv_table: Option<KVTable> = None;
    let _lock_req: Option<LockBatchRequestBody> = None;
    let _lock_resp: Option<LockBatchResponseBody> = None;
    let _query: Option<QueryAssignmentResponseBody> = None;
    let _sub: Option<SubscriptionGroupWrapper> = None;
    let _topic: Option<TopicConfigSerializeWrapper> = None;
}

#[test]
fn test_top_level_core_types() {
    // Test importing core types directly from crate root
    use rocketmq_remoting::CompositeCodec;
    use rocketmq_remoting::RemotingCommand;
    use rocketmq_remoting::RemotingCommandCodec;

    // Verify types are accessible
    let _command: Option<RemotingCommand> = None;
    let _codec: Option<CompositeCodec> = None;
    let _cmd_codec: Option<RemotingCommandCodec> = None;
}

#[test]
fn test_prelude_module() {
    // Test importing from prelude module
    use rocketmq_remoting::prelude::*;

    // Verify common types are accessible
    let _command: Option<RemotingCommand> = None;
    let _pull_header: Option<PullMessageRequestHeader> = None;
    let _send_header: Option<SendMessageRequestHeader> = None;
    let _route_header: Option<GetRouteInfoRequestHeader> = None;
    let _heartbeat_header: Option<HeartbeatRequestHeader> = None;
    let _reg_header: Option<RegisterBrokerRequestHeader> = None;
    let _unreg_header: Option<UnRegisterBrokerRequestHeader> = None;
    let _create_header: Option<CreateTopicRequestHeader> = None;
    let _get_topic_header: Option<GetTopicConfigRequestHeader> = None;
    let _pull_resp_header: Option<PullMessageResponseHeader> = None;
    let _max_header: Option<GetMaxOffsetResponseHeader> = None;
    let _min_header: Option<GetMinOffsetResponseHeader> = None;
    let _ack_body: Option<BatchAckMessageRequestBody> = None;
    let _cluster: Option<ClusterInfo> = None;
    let _consumer_conn: Option<ConsumerConnection> = None;
}

#[test]
fn test_prelude_with_traits_and_enums() {
    // Test that prelude includes traits and enums
    use rocketmq_remoting::prelude::*;

    // Verify traits are accessible
    fn _check_trait<T: RemotingSerializable>(_: &T) {}
    fn _check_deserialize_trait<T: RemotingDeserializable>(_: &T) {}

    // Verify enums are accessible
    let _lang: Option<LanguageCode> = None;
    let _cmd_type: Option<RemotingCommandType> = None;
    let _ser_type: Option<SerializeType> = None;
}

#[test]
fn test_mixed_import_styles() {
    // Test that we can mix import styles
    use rocketmq_remoting::protocol::bodies::broker::ClusterInfo;
    use rocketmq_remoting::protocol::headers::client::GetRouteInfoRequestHeader;
    use rocketmq_remoting::RemotingCommand;
    use rocketmq_remoting::RequestCode;

    // Verify all imports work together
    let _cmd: Option<RemotingCommand> = None;
    let _req_code: Option<RequestCode> = None;
    let _route: Option<GetRouteInfoRequestHeader> = None;
    let _cluster: Option<ClusterInfo> = None;
}

#[test]
fn test_comparison_of_import_paths() {
    // Verify that all three import styles work

    // Style 1: Old full path (still works)
    use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader as OldPath;

    // Style 2: Via protocol::headers module
    use rocketmq_remoting::protocol::headers::PullMessageRequestHeader as HeadersPath;

    // Style 3: Direct from crate root (top-level re-export)
    use rocketmq_remoting::PullMessageRequestHeader as TopLevelPath;

    // All should resolve to the same type
    let _old: Option<OldPath> = None;
    let _headers: Option<HeadersPath> = None;
    let _toplevel: Option<TopLevelPath> = None;
}
