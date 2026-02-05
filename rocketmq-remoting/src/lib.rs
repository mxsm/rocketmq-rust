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

#![allow(dead_code)]
#![allow(incomplete_features)]
#![feature(sync_unsafe_cell)]
#![feature(duration_constructors)]
#![feature(fn_traits)]
#![feature(impl_trait_in_assoc_type)]
extern crate core;

pub mod clients;
pub mod code;
pub mod codec;
pub mod connection;
pub mod net;
pub mod prelude;
pub mod protocol;

pub use crate::protocol::rocketmq_serializable;

pub mod base;
pub mod common;
pub mod remoting;
pub mod remoting_server;
pub mod request_processor;
pub mod rpc;
pub mod runtime;

// Error helpers for unified error system
pub mod connection_v2;
pub mod error_helpers;
pub mod smart_encode_buffer;

// Public Re-exports - Simplified Import Paths
// This section provides convenient access to commonly used types through
// re-exports. Original module paths remain functional for backward compatibility.

// Core codec types - commonly used for network encoding/decoding
pub use crate::codec::remoting_command_codec::CompositeCodec;
pub use crate::codec::remoting_command_codec::RemotingCommandCodec;

// Core protocol types - most frequently used across the codebase
pub use crate::protocol::command_custom_header::CommandCustomHeader;
pub use crate::protocol::command_custom_header::FromMap;
pub use crate::protocol::RemotingCommand;

// Request/Response codes - used for command identification
pub use crate::code::request_code::RequestCode;
pub use crate::code::response_code::RemotingSysResponseCode;

// Most Common Headers (Top-Level Exports)
// These headers are used frequently throughout the codebase and are available
// at the crate root for convenient access:
//   use rocketmq_remoting::{PullMessageRequestHeader, SendMessageRequestHeader};

// Message operations (highest frequency - used 125+ times)
pub use crate::protocol::header::ack_message_request_header::AckMessageRequestHeader;
pub use crate::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
pub use crate::protocol::header::pop_message_request_header::PopMessageRequestHeader;
pub use crate::protocol::header::pull_message_request_header::PullMessageRequestHeader;

// Client management (heartbeat, routing, etc.)
pub use crate::protocol::header::client_request_header::GetRouteInfoRequestHeader;
pub use crate::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;

// NameServer operations (broker registration, config management)
pub use crate::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
pub use crate::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;

// Administrative operations (topic management, etc.)
pub use crate::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
pub use crate::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
pub use crate::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;

// Common response headers
pub use crate::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
pub use crate::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
pub use crate::protocol::header::pop_message_response_header::PopMessageResponseHeader;
pub use crate::protocol::header::pull_message_response_header::PullMessageResponseHeader;

// Transaction operations (frequently used in transactional messaging)
pub use crate::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
pub use crate::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;

// Offset management (common in consumer operations)
pub use crate::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
pub use crate::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;

// Client management
pub use crate::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;

// Most Common Bodies (Top-Level Exports)

// Message operations
pub use crate::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;

// Broker operations
pub use crate::protocol::body::broker_body::cluster_info::ClusterInfo;

// Consumer operations
pub use crate::protocol::body::consumer_connection::ConsumerConnection;
pub use crate::protocol::body::consumer_running_info::ConsumerRunningInfo;
pub use crate::protocol::body::get_consumer_listby_group_response_body::GetConsumerListByGroupResponseBody;

// Topic operations
pub use crate::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;

// Subscription operations
pub use crate::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;

// Lock operations
pub use crate::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
pub use crate::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;

// Query operations
pub use crate::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;

// NameServer operations
pub use crate::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;

// Producer operations
pub use crate::protocol::body::producer_connection::ProducerConnection;

// Utility types
pub use crate::protocol::body::kv_table::KVTable;
