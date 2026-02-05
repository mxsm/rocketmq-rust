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

//! Tests for enhanced re-exports
//!
//! This test module verifies the additional types added to the top-level
//! exports and prelude module based on actual usage frequency analysis.
//!
//! The enhancements include:
//! - Transaction operation headers (CheckTransactionStateRequestHeader,
//!   EndTransactionRequestHeader)
//! - Offset management headers (QueryConsumerOffsetRequestHeader,
//!   UpdateConsumerOffsetRequestHeader)
//! - Additional response headers (PopMessageResponseHeader)
//! - Client management headers (UnregisterClientRequestHeader)
//! - Additional body types (ProducerConnection, RegisterBrokerBody)

#[test]
fn test_enhanced_response_headers() {
    // Verifies that PopMessageResponseHeader is accessible at top level
    use rocketmq_remoting::PopMessageResponseHeader;

    let _response: Option<PopMessageResponseHeader> = None;
}

#[test]
fn test_transaction_headers() {
    // Verifies transaction operation headers are accessible at top level
    use rocketmq_remoting::CheckTransactionStateRequestHeader;
    use rocketmq_remoting::EndTransactionRequestHeader;

    let _check_tx: Option<CheckTransactionStateRequestHeader> = None;
    let _end_tx: Option<EndTransactionRequestHeader> = None;
}

#[test]
fn test_offset_headers() {
    // Verifies offset management headers are accessible at top level
    use rocketmq_remoting::QueryConsumerOffsetRequestHeader;
    use rocketmq_remoting::UpdateConsumerOffsetRequestHeader;

    let _query: Option<QueryConsumerOffsetRequestHeader> = None;
    let _update: Option<UpdateConsumerOffsetRequestHeader> = None;
}

#[test]
fn test_client_headers() {
    // Verifies client management header is accessible at top level
    use rocketmq_remoting::UnregisterClientRequestHeader;

    let _unregister: Option<UnregisterClientRequestHeader> = None;
}

#[test]
fn test_enhanced_bodies() {
    // Verifies enhanced body types are accessible at top level
    use rocketmq_remoting::ProducerConnection;
    use rocketmq_remoting::RegisterBrokerBody;

    let _producer: Option<ProducerConnection> = None;
    let _register: Option<RegisterBrokerBody> = None;
}

#[test]
fn test_prelude_enhanced_headers() {
    // Verifies all enhanced headers are available through prelude
    use rocketmq_remoting::prelude::*;

    // Message operations with both request and response headers
    let _ack: Option<AckMessageRequestHeader> = None;
    let _send: Option<SendMessageRequestHeader> = None;
    let _pop_req: Option<PopMessageRequestHeader> = None;
    let _pop_resp: Option<PopMessageResponseHeader> = None;
    let _pull_req: Option<PullMessageRequestHeader> = None;
    let _pull_resp: Option<PullMessageResponseHeader> = None;

    // Client management including lifecycle operations
    let _route: Option<GetRouteInfoRequestHeader> = None;
    let _heartbeat: Option<HeartbeatRequestHeader> = None;
    let _unregister: Option<UnregisterClientRequestHeader> = None;

    // Transaction operations for distributed transactions
    let _check_tx: Option<CheckTransactionStateRequestHeader> = None;
    let _end_tx: Option<EndTransactionRequestHeader> = None;

    // Offset management for consumer tracking
    let _query_offset: Option<QueryConsumerOffsetRequestHeader> = None;
    let _update_offset: Option<UpdateConsumerOffsetRequestHeader> = None;
}

#[test]
fn test_prelude_enhanced_bodies() {
    // Verifies all enhanced bodies are available through prelude
    use rocketmq_remoting::prelude::*;

    // Message acknowledgment and consumption
    let _ack_body: Option<BatchAckMessageRequestBody> = None;

    // Broker and cluster management
    let _cluster: Option<ClusterInfo> = None;
    let _register_broker: Option<RegisterBrokerBody> = None;

    // Consumer operations
    let _consumer_conn: Option<ConsumerConnection> = None;
    let _consumer_info: Option<ConsumerRunningInfo> = None;
    let _consumer_list: Option<GetConsumerListByGroupResponseBody> = None;

    // Producer operations
    let _producer_conn: Option<ProducerConnection> = None;

    // Queue assignment and load balancing
    let _query_assign: Option<QueryAssignmentResponseBody> = None;

    // Topic and subscription management
    let _subscription: Option<SubscriptionGroupWrapper> = None;
    let _topic_config: Option<TopicConfigSerializeWrapper> = None;
}

#[test]
fn test_top_level_all_enhanced_exports() {
    // Verifies all enhanced top-level exports are accessible
    use rocketmq_remoting::{
        // Transaction operation headers
        CheckTransactionStateRequestHeader,
        EndTransactionRequestHeader,

        // Response headers for pop message operations
        PopMessageResponseHeader,

        // Enhanced body types
        ProducerConnection,
        // Offset management headers
        QueryConsumerOffsetRequestHeader,
        RegisterBrokerBody,
        // Client lifecycle management
        UnregisterClientRequestHeader,

        UpdateConsumerOffsetRequestHeader,
    };

    let _pop_resp: Option<PopMessageResponseHeader> = None;
    let _check_tx: Option<CheckTransactionStateRequestHeader> = None;
    let _end_tx: Option<EndTransactionRequestHeader> = None;
    let _query_offset: Option<QueryConsumerOffsetRequestHeader> = None;
    let _update_offset: Option<UpdateConsumerOffsetRequestHeader> = None;
    let _unregister: Option<UnregisterClientRequestHeader> = None;
    let _producer: Option<ProducerConnection> = None;
    let _register: Option<RegisterBrokerBody> = None;
}

#[test]
fn test_complete_prelude_coverage() {
    // Verifies prelude provides comprehensive coverage for common operations
    use rocketmq_remoting::prelude::*;

    // Core codec and command types
    let _codec: Option<CompositeCodec> = None;
    let _command: Option<RemotingCommand> = None;

    // Request/Response protocol codes
    let _req_code: Option<RequestCode> = None;
    let _resp_code: Option<RemotingSysResponseCode> = None;

    // Primary message operation headers
    let _send_msg: Option<SendMessageRequestHeader> = None;
    let _pull_msg: Option<PullMessageRequestHeader> = None;
    let _pop_msg: Option<PopMessageRequestHeader> = None;
    let _ack_msg: Option<AckMessageRequestHeader> = None;

    // Client management and discovery
    let _route: Option<GetRouteInfoRequestHeader> = None;
    let _heartbeat: Option<HeartbeatRequestHeader> = None;

    // NameServer registration operations
    let _register_broker: Option<RegisterBrokerRequestHeader> = None;
    let _unregister_broker: Option<UnRegisterBrokerRequestHeader> = None;

    // Common data structures
    let _cluster_info: Option<ClusterInfo> = None;
    let _consumer_conn: Option<ConsumerConnection> = None;
    let _producer_conn: Option<ProducerConnection> = None;

    // Essential traits for serialization
    fn _check_serializable<T: RemotingSerializable>(_: T) {}
    fn _check_deserializable<T: RemotingDeserializable>(_: T) {}
    fn _check_fast_codes<T: FastCodesHeader>(_: T) {}

    // Common enumerations
    let _lang: Option<LanguageCode> = None;
    let _cmd_type: Option<RemotingCommandType> = None;
    let _serial_type: Option<SerializeType> = None;
}

#[test]
fn test_backward_compatibility() {
    // Verifies that enhanced exports maintain backward compatibility
    // All original deep import paths continue to work
    use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
    use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
    use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;

    let _check_tx: Option<CheckTransactionStateRequestHeader> = None;
    let _end_tx: Option<EndTransactionRequestHeader> = None;
    let _pop_resp: Option<PopMessageResponseHeader> = None;
}

#[test]
fn test_categorized_imports() {
    // Verifies that categorized imports remain functional
    // This provides organized access to related types
    use rocketmq_remoting::protocol::headers::transaction::CheckTransactionStateRequestHeader;
    use rocketmq_remoting::protocol::headers::transaction::EndTransactionRequestHeader;

    let _check_tx: Option<CheckTransactionStateRequestHeader> = None;
    let _end_tx: Option<EndTransactionRequestHeader> = None;
}
