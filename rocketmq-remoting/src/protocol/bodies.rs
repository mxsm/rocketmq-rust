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

//! Re-exported request/response bodies organized by category
//!
//! This module provides simplified access to commonly used bodies through re-exports.
//! Original module paths remain functional for backward compatibility.
//!
//! # Usage Examples
//!
//! ## Import by category
//! ```rust
//! use rocketmq_remoting::protocol::bodies::broker::ClusterInfo;
//! use rocketmq_remoting::protocol::bodies::consumer::ConsumerConnection;
//! ```
//!
//! ## Import most common types directly
//! ```rust
//! use rocketmq_remoting::protocol::bodies::BatchAckMessageRequestBody;
//! use rocketmq_remoting::protocol::bodies::ClusterInfo;
//! ```

// Message Bodies
pub mod message {
    pub use super::super::body::batch_ack::BatchAck;
    pub use super::super::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
    pub use super::super::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
}

// Broker Bodies
pub mod broker {
    pub use super::super::body::broker_body::broker_member_group::BrokerMemberGroup;
    pub use super::super::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
    pub use super::super::body::broker_body::cluster_info::ClusterInfo;
    pub use super::super::body::broker_body::register_broker_body::RegisterBrokerBody;
    pub use super::super::body::broker_replicas_info::BrokerReplicasInfo;
    pub use super::super::body::broker_replicas_info::ReplicaIdentity;
    pub use super::super::body::broker_replicas_info::ReplicasInfo;
    pub use super::super::body::broker_stats_item::BrokerStatsItem;
    pub use super::super::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
}

// Consumer Bodies
pub mod consumer {
    pub use super::super::body::connection::Connection;
    pub use super::super::body::consume_by_who::ConsumeByWho;
    pub use super::super::body::consume_queue_data::ConsumeQueueData;
    pub use super::super::body::consume_status::ConsumeStatus;
    pub use super::super::body::consumer_connection::ConsumerConnection;
    pub use super::super::body::consumer_offset_serialize_wrapper::ConsumerOffsetSerializeWrapper;
    pub use super::super::body::consumer_running_info::ConsumerRunningInfo;
    pub use super::super::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
    pub use super::super::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
    pub use super::super::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
    pub use super::super::body::lite_lag_info::LiteLagInfo;
    pub use super::super::body::pop_process_queue_info::PopProcessQueueInfo;
    pub use super::super::body::process_queue_info::ProcessQueueInfo;
}

// Producer Bodies
pub mod producer {
    pub use super::super::body::producer_connection::ProducerConnection;
    pub use super::super::body::producer_info::ProducerInfo;
    pub use super::super::body::producer_table_info::ProducerTableInfo;
}

// Topic Bodies
pub mod topic {
    pub use super::super::body::create_topic_list_request_body::CreateTopicListRequestBody;
    pub use super::super::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
    pub use super::super::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
    pub use super::super::body::topic::topic_list::TopicList;
    pub use super::super::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
    pub use super::super::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
    pub use super::super::body::topic_info_wrapper::topic_queue_wrapper::TopicQueueMappingSerializeWrapper;
}

// Subscription Bodies
pub mod subscription {
    pub use super::super::body::message_request_mode_serialize_wrapper::MessageRequestModeSerializeWrapper;
    pub use super::super::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
    pub use super::super::body::subscription_group_wrapper::SubscriptionGroupWrapper;
}

// Response Bodies
pub mod response {
    pub use super::super::body::response::get_consumer_status_body::GetConsumerStatusBody;
    pub use super::super::body::response::lock_batch_response_body::LockBatchResponseBody;
    pub use super::super::body::response::reset_offset_body::ResetOffsetBody;
    pub use super::super::body::response::reset_offset_body::ResetOffsetBodyForC;
}

// Request Bodies
pub mod request {
    pub use super::super::body::check_client_request_body::CheckClientRequestBody;
    pub use super::super::body::query_assignment_request_body::QueryAssignmentRequestBody;
    pub use super::super::body::request::lock_batch_request_body::LockBatchRequestBody;
    pub use super::super::body::unlock_batch_request_body::UnlockBatchRequestBody;
}

// Query Bodies
pub mod query {
    pub use super::super::body::query_assignment_response_body::QueryAssignmentResponseBody;
    pub use super::super::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
}

// High Availability (HA) Bodies
pub mod ha {
    pub use super::super::body::ha_client_runtime_info::HAClientRuntimeInfo;
    pub use super::super::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;
    pub use super::super::body::ha_runtime_info::HARuntimeInfo;
}

// ACL and Security Bodies
pub mod acl {
    pub use super::super::body::acl_info::AclInfo;
    pub use super::super::body::acl_info::PolicyEntryInfo;
    pub use super::super::body::acl_info::PolicyInfo;
    pub use super::super::body::cluster_acl_version_info::ClusterAclVersionInfo;
}

// User Management Bodies
pub mod user {
    pub use super::super::body::user_info::UserInfo;
}

// Static Topic Bodies
pub mod static_topic {
    pub use super::super::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
    pub use super::super::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
    pub use super::super::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
    pub use super::super::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
    pub use super::super::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
    pub use super::super::static_topic::topic_queue_mapping_one::TopicQueueMappingOne;
    pub use super::super::static_topic::topic_queue_mapping_utils::MappingAllocator;
    pub use super::super::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
    pub use super::super::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;
}

// Controller and Sync Bodies
pub mod controller {
    pub use super::super::body::elect_master_response_body::ElectMasterResponseBody;
    pub use super::super::body::sync_state_set_body::SyncStateSet;
}

// Utility Bodies
pub mod util {
    pub use super::super::body::cm_result::CMResult;
    pub use super::super::body::epoch_entry_cache::EpochEntry;
    pub use super::super::body::epoch_entry_cache::EpochEntryCache;
    pub use super::super::body::group_list::GroupList;
    pub use super::super::body::kv_table::KVTable;
    pub use super::super::body::queue_time_span::QueueTimeSpan;
    pub use super::super::body::timer_metrics_serialize_wrapper::Metric;
    pub use super::super::body::timer_metrics_serialize_wrapper::TimerMetricsSerializeWrapper;
}

// Top-Level Exports (Most Frequently Used Bodies)
// These are available as: rocketmq_remoting::protocol::bodies::<TypeName>
// Message operations (most frequently used)
pub use super::body::batch_ack_message_request_body::BatchAckMessageRequestBody;

// Broker operations
pub use super::body::broker_body::cluster_info::ClusterInfo;

// Consumer operations
pub use super::body::consumer_connection::ConsumerConnection;
pub use super::body::consumer_running_info::ConsumerRunningInfo;
pub use super::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;

// Topic operations
pub use super::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;

// Subscription operations
pub use super::body::subscription_group_wrapper::SubscriptionGroupWrapper;

// Lock operations
pub use super::body::request::lock_batch_request_body::LockBatchRequestBody;
pub use super::body::response::lock_batch_response_body::LockBatchResponseBody;

// Query operations
pub use super::body::query_assignment_response_body::QueryAssignmentResponseBody;

// Utility
pub use super::body::kv_table::KVTable;
