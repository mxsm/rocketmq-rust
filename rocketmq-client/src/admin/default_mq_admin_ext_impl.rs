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
#[cfg(feature = "admin-full")]
use std::collections::HashMap;
#[cfg(feature = "admin-full")]
use std::collections::HashSet;
use std::sync::Arc;
#[cfg(feature = "admin-full")]
use std::sync::OnceLock;
use std::time::Duration;

#[cfg(feature = "admin-full")]
use crate::admin::capability::{AuthAdmin, BrokerAdmin, ConsumerAdmin, OffsetAdmin, RouteAdmin, TopicAdmin};
use crate::base::client_config::ClientConfig;
#[cfg(feature = "admin-full")]
use crate::base::validators::Validators;
#[cfg(feature = "admin-full")]
use crate::common::admin_tool_result::AdminToolResult;
#[cfg(feature = "admin-full")]
use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;
#[cfg(feature = "admin-full")]
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
#[cfg(feature = "admin-full")]
use crate::consumer::pull_callback::PullCallback;
#[cfg(feature = "admin-full")]
use crate::consumer::pull_status::PullStatus;
use crate::factory::mq_client_instance::MQClientInstance;
#[cfg(feature = "admin-full")]
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::implementation::mq_client_manager::ClientPool;
use crate::implementation::mq_client_manager::ClientPoolToken;
use crate::runtime::ClientRuntime;
use cheetah_string::CheetahString;
#[cfg(feature = "admin-full")]
use rand::seq::IndexedRandom;
#[cfg(feature = "admin-full")]
use rocketmq_error::RocketMQError;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::attribute::attribute_parser::AttributeParser;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::attribute::topic_attributes::TopicAttributes;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::attribute::Attribute;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_model::common::base::service_state::ServiceState;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::config::TopicConfig;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::constant::PermName;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::message::message_enum::MessageRequestMode;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::message::message_ext::MessageExt;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::message::message_queue::MessageQueue;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::message::MessageConst;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::message::MessageTrait;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::mix_all;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
#[allow(deprecated)]
#[cfg(feature = "admin-full")]
use rocketmq_model::common::tools::broker_operator_result::BrokerOperatorResult;
#[allow(deprecated)]
#[cfg(feature = "admin-full")]
use rocketmq_model::common::tools::message_track::MessageTrack;
#[allow(deprecated)]
#[cfg(feature = "admin-full")]
use rocketmq_model::common::tools::track_type::TrackType;
#[cfg(feature = "admin-full")]
use rocketmq_model::common::topic::TopicValidator;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::code::response_code::ResponseCode;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::admin::topic_offset::TopicOffset;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::acl_info::PolicyEntryInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::acl_info::PolicyInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::broker_replicas_info::BrokerReplicasInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::epoch_entry_cache::EpochEntryCache;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::group_list::GroupList;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::kv_table::KVTable;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::queue_time_span::QueueTimeSpan;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::body::user_info::UserInfo;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::subscription::group_forbidden::GroupForbidden;
#[cfg(feature = "admin-full")]
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_transport::api::RPCHook;
#[cfg(feature = "admin-full")]
use rocketmq_transport::api::RpcRequestHeader;
#[cfg(feature = "admin-full")]
use tracing::info;
#[cfg(feature = "admin-full")]
use tracing::warn;

#[cfg(feature = "admin-full")]
static SYSTEM_GROUP_SET: OnceLock<HashSet<CheetahString>> = OnceLock::new();

const SOCKS_PROXY_JSON: &str = "socksProxyJson";
const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
#[cfg(feature = "admin-full")]
const ROCKSDB_CONFIG_TYPE_CONSUMER_OFFSETS: &str = "consumerOffsets";

pub struct DefaultMQAdminExtImpl {
    client_pool: ClientPool,
    client_pool_token: Option<ClientPoolToken>,
    service_state: ServiceState,
    client_instance: Option<Arc<MQClientInstance>>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    timeout_millis: Duration,
    kv_namespace_to_delete_list: Vec<CheetahString>,
    client_config: ClientConfig,
    admin_ext_group: CheetahString,
}

#[cfg(feature = "admin-full")]
mod admin_api;
#[cfg(feature = "admin-full")]
mod broker;
#[cfg(feature = "admin-full")]
mod group;
mod lifecycle;
#[cfg(feature = "admin-mutation")]
mod mutation_api;
#[cfg(feature = "admin-full")]
mod security;
#[cfg(feature = "admin-full")]
mod topic;

#[cfg(all(test, feature = "admin-full"))]
#[path = "../../tests/admin/default_mq_admin_ext_impl/unit.rs"]
mod tests;
