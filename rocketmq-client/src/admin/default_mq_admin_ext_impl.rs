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
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use crate::admin::mq_admin_ext_async::MQAdminExt;
use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::common::admin_tool_result::AdminToolResult;
use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::pull_callback::PullCallback;
use crate::consumer::pull_status::PullStatus;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::implementation::mq_client_manager::ClientPool;
use crate::implementation::mq_client_manager::ClientPoolToken;
use crate::runtime::ClientRuntime;
use cheetah_string::CheetahString;
use rand::seq::IndexedRandom;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::attribute::attribute_parser::AttributeParser;
use rocketmq_model::common::attribute::topic_attributes::TopicAttributes;
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_model::common::attribute::Attribute;
use rocketmq_model::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_model::common::base::service_state::ServiceState;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
#[allow(deprecated)]
use rocketmq_model::common::tools::broker_operator_result::BrokerOperatorResult;
#[allow(deprecated)]
use rocketmq_model::common::tools::message_track::MessageTrack;
#[allow(deprecated)]
use rocketmq_model::common::tools::track_type::TrackType;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_protocol::protocol::admin::topic_offset::TopicOffset;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_protocol::protocol::body::acl_info::PolicyInfo;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_protocol::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_protocol::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::group_list::GroupList;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_protocol::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_protocol::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;
use rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_protocol::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_protocol::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_transport::RPCHook;
use rocketmq_transport::RpcRequestHeader;
use tracing::info;
use tracing::warn;

static SYSTEM_GROUP_SET: OnceLock<HashSet<CheetahString>> = OnceLock::new();

const SOCKS_PROXY_JSON: &str = "socksProxyJson";
const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
const ROCKSDB_CONFIG_TYPE_CONSUMER_OFFSETS: &str = "consumerOffsets";

use self::group::timeout_millis_to_u64;

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

impl DefaultMQAdminExtImpl {
    pub fn new(
        client_runtime: Arc<ClientRuntime>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        timeout_millis: Duration,
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
    ) -> Self {
        DefaultMQAdminExtImpl {
            client_pool: client_runtime.pool().clone(),
            client_pool_token: None,
            service_state: ServiceState::CreateJust,
            client_instance: None,
            rpc_hook,
            timeout_millis,
            kv_namespace_to_delete_list: vec![CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG)],
            client_config,
            admin_ext_group,
        }
    }

    /// Returns whether the facade has a usable concrete implementation.
    ///
    /// The implementation is now owned directly, so this compatibility query is always true.
    pub fn has_inner(&self) -> bool {
        true
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    #[inline]
    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client_config
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    #[inline]
    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.client_config.set_use_tls(use_tls);
    }

    pub(super) fn mq_client_api(&self) -> rocketmq_error::RocketMQResult<Arc<MQClientAPIImpl>> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()
    }

    pub(super) fn remoting_timeout_millis(&self) -> rocketmq_error::RocketMQResult<u64> {
        timeout_millis_to_u64(self.timeout_millis)
    }
}

mod admin_api;
mod broker;
mod group;
mod security;
mod topic;

#[cfg(test)]
#[path = "../../tests/admin/default_mq_admin_ext_impl/unit.rs"]
mod tests;
