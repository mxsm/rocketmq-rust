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

//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from
//! rocketmq-remoting. It's designed to be imported with a glob import for
//! quick access to essential types.
//!
//! # Example
//!
//! ```rust
//! use rocketmq_remoting::prelude::*;
//!
//! let header = PullMessageRequestHeader::default();
//! let command = RemotingCommand::create_request_command(RequestCode::PullMessage, header);
//! ```

// Core Types

pub use crate::codec::remoting_command_codec::CompositeCodec;
pub use crate::codec::remoting_command_codec::RemotingCommandCodec;
pub use crate::protocol::command_custom_header::CommandCustomHeader;
pub use crate::protocol::command_custom_header::FromMap;
pub use crate::protocol::RemotingCommand;

// Request/Response Codes

pub use crate::code::request_code::RequestCode;
pub use crate::code::response_code::RemotingSysResponseCode;

// Most Common Headers (for typical usage)

// Message operations
pub use crate::protocol::header::ack_message_request_header::AckMessageRequestHeader;
pub use crate::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
pub use crate::protocol::header::pop_message_request_header::PopMessageRequestHeader;
pub use crate::protocol::header::pop_message_response_header::PopMessageResponseHeader;
pub use crate::protocol::header::pull_message_request_header::PullMessageRequestHeader;
pub use crate::protocol::header::pull_message_response_header::PullMessageResponseHeader;

// Client management
pub use crate::protocol::header::client_request_header::GetRouteInfoRequestHeader;
pub use crate::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
pub use crate::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;

// RPC types (remote procedure call infrastructure)
pub use crate::rpc::rpc_request::RpcRequest;
pub use crate::rpc::rpc_request_header::RpcRequestHeader;
pub use crate::rpc::rpc_response::RpcResponse;
pub use crate::rpc::topic_request_header::TopicRequestHeader;

// NameServer operations
pub use crate::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
pub use crate::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;

// Administrative operations
pub use crate::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
pub use crate::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
pub use crate::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;

// Transaction operations
pub use crate::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
pub use crate::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;

// Offset management
pub use crate::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
pub use crate::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;

// Common response headers
pub use crate::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
pub use crate::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;

// Most Common Bodies

pub use crate::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
pub use crate::protocol::body::broker_body::cluster_info::ClusterInfo;
pub use crate::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
pub use crate::protocol::body::consumer_connection::ConsumerConnection;
pub use crate::protocol::body::consumer_running_info::ConsumerRunningInfo;
pub use crate::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
pub use crate::protocol::body::producer_connection::ProducerConnection;
pub use crate::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
pub use crate::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
pub use crate::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;

// Essential Traits

pub use crate::protocol::FastCodesHeader;
pub use crate::protocol::RemotingDeserializable;
pub use crate::protocol::RemotingSerializable;

// Common Enums
pub use crate::protocol::LanguageCode;
pub use crate::protocol::RemotingCommandType;
pub use crate::protocol::SerializeType;
