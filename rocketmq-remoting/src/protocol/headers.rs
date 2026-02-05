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

//! Re-exported request/response headers organized by category
//!
//! This module provides simplified access to commonly used headers through re-exports.
//! Original module paths remain functional for backward compatibility.
//!
//! # Usage Examples
//!
//! ## Import by category
//! ```rust
//! use rocketmq_remoting::protocol::headers::message::SendMessageRequestHeader;
//! use rocketmq_remoting::protocol::headers::namesrv::RegisterBrokerRequestHeader;
//! use rocketmq_remoting::protocol::headers::namesrv::UnRegisterBrokerRequestHeader;
//! ```
//!
//! ## Import most common types directly
//! ```rust
//! use rocketmq_remoting::protocol::headers::PullMessageRequestHeader;
//! use rocketmq_remoting::protocol::headers::SendMessageRequestHeader;
//! ```

/// Send and receive message headers
pub mod message {
    pub use super::super::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    pub use super::super::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
    pub use super::super::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
}

/// Pull and query message headers
pub mod polling {
    pub use super::super::header::peek_message_request_header::PeekMessageRequestHeader;
    pub use super::super::header::polling_info_request_header::PollingInfoRequestHeader;
    pub use super::super::header::polling_info_response_header::PollingInfoResponseHeader;
    pub use super::super::header::pop_message_request_header::PopMessageRequestHeader;
    pub use super::super::header::pop_message_response_header::PopMessageResponseHeader;
    pub use super::super::header::pull_message_request_header::PullMessageRequestHeader;
    pub use super::super::header::pull_message_response_header::PullMessageResponseHeader;
}

/// Acknowledgment headers
pub mod ack {
    pub use super::super::header::ack_message_request_header::AckMessageRequestHeader;
    pub use super::super::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
    pub use super::super::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
}

/// Query message headers
pub mod query {
    pub use super::super::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
    pub use super::super::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
    pub use super::super::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    pub use super::super::header::query_message_request_header::QueryMessageRequestHeader;
    pub use super::super::header::query_message_response_header::QueryMessageResponseHeader;
    pub use super::super::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
    pub use super::super::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
    pub use super::super::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
}

/// View and search message headers
pub mod view {
    pub use super::super::header::search_offset_request_header::SearchOffsetRequestHeader;
    pub use super::super::header::search_offset_response_header::SearchOffsetResponseHeader;
    pub use super::super::header::view_message_request_header::ViewMessageRequestHeader;
    pub use super::super::header::view_message_response_header::ViewMessageResponseHeader;
}

pub mod client {
    pub use super::super::header::client_request_header::GetRouteInfoRequestHeader;
    pub use super::super::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
    pub use super::super::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
    pub use super::super::header::get_consumer_listby_group_response_header::GetConsumerListByGroupResponseHeader;
    pub use super::super::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
    pub use super::super::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
    pub use super::super::header::heartbeat_request_header::HeartbeatRequestHeader;
    pub use super::super::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
    pub use super::super::header::unregister_client_request_header::UnregisterClientRequestHeader;
}

pub mod namesrv {
    pub use super::super::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
    pub use super::super::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
    pub use super::super::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
    pub use super::super::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
    pub use super::super::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
    pub use super::super::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
    pub use super::super::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
    pub use super::super::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
    pub use super::super::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
    pub use super::super::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
    pub use super::super::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
    pub use super::super::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
    pub use super::super::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
    pub use super::super::header::namesrv::query_data_version_header::QueryDataVersionRequestHeader;
    pub use super::super::header::namesrv::query_data_version_header::QueryDataVersionResponseHeader;
    pub use super::super::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
    pub use super::super::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
    pub use super::super::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
    pub use super::super::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader;
    pub use super::super::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
    pub use super::super::header::namesrv::topic_operation_header::TopicRequestHeader;
}

pub mod broker {
    pub use super::super::header::broker::broker_heartbeat_request_header::BrokerHeartbeatRequestHeader;
}

pub mod controller {
    pub use super::super::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
    pub use super::super::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader;
    pub use super::super::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
    pub use super::super::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
    pub use super::super::header::controller::elect_master_request_header::ElectMasterRequestHeader;
    pub use super::super::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
    pub use super::super::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader;
    pub use super::super::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
    pub use super::super::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
    pub use super::super::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
    pub use super::super::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
}

pub mod admin {
    pub use super::super::header::create_topic_request_header::CreateTopicRequestHeader;
    pub use super::super::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
    pub use super::super::header::delete_topic_request_header::DeleteTopicRequestHeader;
    pub use super::super::header::get_all_topic_config_response_header::GetAllTopicConfigResponseHeader;
    pub use super::super::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
    pub use super::super::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
    pub use super::super::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
    pub use super::super::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
    pub use super::super::header::get_topic_stats_request_header::GetTopicStatsRequestHeader;
}

pub mod transaction {
    pub use super::super::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
    pub use super::super::header::end_transaction_request_header::EndTransactionRequestHeader;
}

pub mod offset {
    pub use super::super::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
    pub use super::super::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    pub use super::super::header::reset_offset_request_header::ResetOffsetRequestHeader;
    pub use super::super::header::search_offset_request_header::SearchOffsetRequestHeader;
    pub use super::super::header::search_offset_response_header::SearchOffsetResponseHeader;
    pub use super::super::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
    pub use super::super::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader;
}

pub mod lock {
    pub use super::super::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
    pub use super::super::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
}

pub mod min_offset {
    pub use super::super::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
    pub use super::super::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
}

pub mod max_offset {
    pub use super::super::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
    pub use super::super::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
}

pub mod acl {
    pub use super::super::header::create_user_request_header::CreateUserRequestHeader;
    pub use super::super::header::delete_acl_request_header::DeleteAclRequestHeader;
    pub use super::super::header::delete_user_request_header::DeleteUserRequestHeader;
    pub use super::super::header::get_user_request_headers::GetUserRequestHeader;
    pub use super::super::header::list_acl_request_header::ListAclRequestHeader;
    pub use super::super::header::list_users_request_header::ListUsersRequestHeader;
    pub use super::super::header::update_user_request_header::UpdateUserRequestHeader;
}

pub mod special {
    pub use super::super::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
    pub use super::super::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
    pub use super::super::header::reply_message_request_header::ReplyMessageRequestHeader;
}

pub mod notification {
    pub use super::super::header::notification_request_header::NotificationRequestHeader;
    pub use super::super::header::notification_response_header::NotificationResponseHeader;
    pub use super::super::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
}

pub mod ha {
    pub use super::super::header::exchange_ha_info_request_header::ExchangeHAInfoRequestHeader;
    pub use super::super::header::exchange_ha_info_response_header::ExchangeHaInfoResponseHeader;
    pub use super::super::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader;
}

pub mod util {
    pub use super::super::header::elect_master_response_header::ElectMasterResponseHeader;
    pub use super::super::header::empty_header::EmptyHeader;
    pub use super::super::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
    pub use super::super::header::get_meta_data_response_header::GetMetaDataResponseHeader;
}

// These are available as: rocketmq_remoting::protocol::headers::<TypeName>
// Message operations (most frequently used)
pub use super::header::ack_message_request_header::AckMessageRequestHeader;
pub use super::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
pub use super::header::pop_message_request_header::PopMessageRequestHeader;
pub use super::header::pull_message_request_header::PullMessageRequestHeader;

// Client operations
pub use super::header::client_request_header::GetRouteInfoRequestHeader;
pub use super::header::heartbeat_request_header::HeartbeatRequestHeader;

// NameServer operations
pub use super::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
pub use super::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;

// Common response headers
pub use super::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
pub use super::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
pub use super::header::pull_message_response_header::PullMessageResponseHeader;

// Administrative operations
pub use super::header::create_topic_request_header::CreateTopicRequestHeader;
pub use super::header::delete_topic_request_header::DeleteTopicRequestHeader;
pub use super::header::get_topic_config_request_header::GetTopicConfigRequestHeader;

// Offset operations
pub use super::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
pub use super::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;

// Utility
pub use super::header::empty_header::EmptyHeader;
