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
use crate::admin::mq_admin_ext_async_inner::MQAdminExtInnerImpl;
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
use crate::implementation::mq_client_manager::MQClientManager;
use cheetah_string::CheetahString;
use rand::seq::IndexedRandom;
use rocketmq_common::common::attribute::attribute_parser::AttributeParser;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::attribute::Attribute;
use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
#[allow(deprecated)]
use rocketmq_common::common::tools::broker_operator_result::BrokerOperatorResult;
#[allow(deprecated)]
use rocketmq_common::common::tools::message_track::MessageTrack;
#[allow(deprecated)]
use rocketmq_common::common::tools::track_type::TrackType;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TopicAttributes::TopicAttributes;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::protocol::admin::topic_offset::TopicOffset;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyInfo;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::group_list::GroupList;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_remoting::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_remoting::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_remoting::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader;
use rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_remoting::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_remoting::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_remoting::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_remoting::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_remoting::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader;
use rocketmq_remoting::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::route_facade::BrokerDataExt;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_remoting::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::info;
use tracing::warn;

static SYSTEM_GROUP_SET: OnceLock<HashSet<CheetahString>> = OnceLock::new();

fn get_system_group_set() -> &'static HashSet<CheetahString> {
    SYSTEM_GROUP_SET.get_or_init(|| {
        let mut set = HashSet::new();
        set.insert(CheetahString::from(mix_all::DEFAULT_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::DEFAULT_PRODUCER_GROUP));
        set.insert(CheetahString::from(mix_all::TOOLS_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::SCHEDULE_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::FILTERSRV_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::MONITOR_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::CLIENT_INNER_PRODUCER_GROUP));
        set.insert(CheetahString::from(mix_all::SELF_TEST_PRODUCER_GROUP));
        set.insert(CheetahString::from(mix_all::SELF_TEST_CONSUMER_GROUP));
        set.insert(CheetahString::from(mix_all::ONS_HTTP_PROXY_GROUP));
        set.insert(CheetahString::from(mix_all::CID_ONSAPI_PERMISSION_GROUP));
        set.insert(CheetahString::from(mix_all::CID_ONSAPI_OWNER_GROUP));
        set.insert(CheetahString::from(mix_all::CID_ONSAPI_PULL_GROUP));
        set.insert(CheetahString::from(mix_all::CID_SYS_RMQ_TRANS));
        set
    })
}

const SOCKS_PROXY_JSON: &str = "socksProxyJson";
const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
const ROCKSDB_CONFIG_TYPE_CONSUMER_OFFSETS: &str = "consumerOffsets";

fn sync_pull_result_missing(operation: &'static str) -> RocketMQError {
    RocketMQError::ClientInvalidState {
        expected: "PullResultExt returned by sync pull_message",
        actual: format!("{operation} returned None"),
    }
}

fn encode_topic_attributes(attributes: &HashMap<CheetahString, CheetahString>) -> Option<CheetahString> {
    if attributes.is_empty() {
        return None;
    }

    let serialized = AttributeParser::parse_to_string(
        &attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>(),
    );

    if serialized.is_empty() {
        None
    } else {
        Some(serialized.into())
    }
}

fn offset_to_java_long(operation: &'static str, offset: u64) -> rocketmq_error::RocketMQResult<i64> {
    i64::try_from(offset)
        .map_err(|_| RocketMQError::illegal_argument(format!("{operation} offset exceeds Java long range")))
}

fn timestamp_to_java_long(operation: &'static str, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
    i64::try_from(timestamp)
        .map_err(|_| RocketMQError::illegal_argument(format!("{operation} timestamp exceeds Java long range")))
}

fn timeout_millis_to_u64(timeout_millis: Duration) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(timeout_millis.as_millis()).map_err(|_| {
        RocketMQError::illegal_argument("DefaultMQAdminExt timeoutMillis exceeds Rust u64 millisecond range")
    })
}

fn java_long_to_u64(operation: &'static str, field: &'static str, value: i64) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(value).map_err(|_| {
        RocketMQError::illegal_argument(format!(
            "{operation} {field} is negative and cannot be represented as Rust u64"
        ))
    })
}

fn merge_consume_status_result(
    target: &mut HashMap<CheetahString, HashMap<MessageQueue, u64>>,
    source: HashMap<CheetahString, HashMap<MessageQueue, i64>>,
) -> rocketmq_error::RocketMQResult<()> {
    for (client_id, offsets) in source {
        let target_offsets = target.entry(client_id).or_default();
        for (mq, offset) in offsets {
            target_offsets.insert(mq, java_long_to_u64("getConsumeStatus", "offset", offset)?);
        }
    }
    Ok(())
}

fn master_flush_offset_to_java_long(master_flush_offset: u64) -> rocketmq_error::RocketMQResult<i64> {
    offset_to_java_long("resetMasterFlushOffset", master_flush_offset)
}

fn query_consume_queue_index_to_java_long(index: u64) -> rocketmq_error::RocketMQResult<i64> {
    offset_to_java_long("queryConsumeQueue", index)
}

fn search_offset_timestamp_to_java_long(timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
    timestamp_to_java_long("searchOffset", timestamp)
}

fn validate_acl_file_path_for_global_white_addr_config(
    acl_file_full_path: Option<&CheetahString>,
) -> rocketmq_error::RocketMQResult<()> {
    if acl_file_full_path.is_some_and(|acl_file_full_path| !acl_file_full_path.is_empty()) {
        return Err(RocketMQError::illegal_argument(
            "acl_file_full_path is not supported by RocketMQ ACL 2.0 global white address updates",
        ));
    }
    Ok(())
}

#[allow(deprecated)]
fn broker_operator_result(success_list: Vec<CheetahString>, failure_list: Vec<CheetahString>) -> BrokerOperatorResult {
    let mut result = BrokerOperatorResult::new();
    result.set_success_list(success_list);
    result.set_failure_list(failure_list);
    result
}

fn controller_servers_or_namesrv(
    controller_servers: Vec<CheetahString>,
    name_server_address_list: &[CheetahString],
) -> Vec<CheetahString> {
    if controller_servers.is_empty() {
        name_server_address_list.to_vec()
    } else {
        controller_servers
    }
}

fn update_consume_offset_request_header(
    consume_group: CheetahString,
    mq: &MessageQueue,
    offset: u64,
) -> rocketmq_error::RocketMQResult<UpdateConsumerOffsetRequestHeader> {
    let commit_offset = offset_to_java_long("updateConsumeOffset", offset)?;

    Ok(UpdateConsumerOffsetRequestHeader {
        consumer_group: consume_group,
        topic: mq.topic().clone(),
        queue_id: mq.queue_id(),
        commit_offset,
        topic_request_header: Some(TopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: Some(mq.broker_name().clone()),
                oneway: None,
            }),
        }),
    })
}

fn reset_offset_by_queue_id_request_headers(
    consumer_group: CheetahString,
    topic_name: CheetahString,
    queue_id: i32,
    reset_offset: u64,
) -> rocketmq_error::RocketMQResult<(UpdateConsumerOffsetRequestHeader, ResetOffsetRequestHeader)> {
    let reset_offset = offset_to_java_long("resetOffsetByQueueId", reset_offset)?;

    let update_header = UpdateConsumerOffsetRequestHeader {
        consumer_group: consumer_group.clone(),
        topic: topic_name.clone(),
        queue_id,
        commit_offset: reset_offset,
        topic_request_header: None,
    };

    let reset_header = ResetOffsetRequestHeader {
        topic: topic_name,
        group: consumer_group,
        queue_id,
        offset: Some(reset_offset),
        timestamp: 0,
        is_force: false,
        topic_request_header: None,
    };

    Ok((update_header, reset_header))
}

fn lite_pull_update_consumer_offset_request_header(
    topic: CheetahString,
    group: CheetahString,
    queue_id: i32,
    offset: u64,
) -> rocketmq_error::RocketMQResult<UpdateConsumerOffsetRequestHeader> {
    let commit_offset = offset_to_java_long("updateLitePullConsumerOffset", offset)?;

    Ok(UpdateConsumerOffsetRequestHeader {
        consumer_group: group,
        topic,
        queue_id,
        commit_offset,
        topic_request_header: None,
    })
}

fn optional_non_empty(value: Option<CheetahString>) -> Option<CheetahString> {
    value.filter(|value| !value.is_empty())
}

fn notify_min_broker_id_change_request_header(
    min_broker_id: u64,
    min_broker_addr: CheetahString,
    offline_broker_addr: Option<CheetahString>,
    ha_broker_addr: Option<CheetahString>,
) -> rocketmq_error::RocketMQResult<NotifyMinBrokerIdChangeRequestHeader> {
    if min_broker_addr.is_empty() {
        return Err(RocketMQError::illegal_argument(
            "notifyMinBrokerIdChanged requires minBrokerAddr",
        ));
    }

    Ok(NotifyMinBrokerIdChangeRequestHeader::new(
        Some(min_broker_id),
        None,
        Some(min_broker_addr),
        optional_non_empty(offline_broker_addr),
        optional_non_empty(ha_broker_addr),
    ))
}

fn choose_min_broker_notify_addrs(
    broker_addrs: &HashMap<u64, CheetahString>,
    min_broker_id: u64,
    offline_broker_addr: Option<&CheetahString>,
) -> Vec<CheetahString> {
    let notify_all = broker_addrs.len() == 1
        || offline_broker_addr
            .map(|broker_addr| !broker_addr.is_empty())
            .unwrap_or(false);
    let mut entries = broker_addrs.iter().collect::<Vec<_>>();
    entries.sort_by_key(|entry| *entry.0);
    entries
        .into_iter()
        .filter(|entry| notify_all || *entry.0 != min_broker_id)
        .map(|entry| entry.1.clone())
        .collect()
}

fn update_group_forbidden_request_header(
    group_name: CheetahString,
    topic_name: CheetahString,
    readable: Option<bool>,
) -> UpdateGroupForbiddenRequestHeader {
    UpdateGroupForbiddenRequestHeader {
        group: group_name,
        topic: topic_name,
        readable,
        topic_request_header: None,
    }
}

#[derive(Debug, Clone, Copy)]
enum BrokerCleanupOperation {
    CleanExpiredConsumerQueue,
    DeleteExpiredCommitLog,
    CleanUnusedTopic,
}

impl BrokerCleanupOperation {
    async fn execute(
        self,
        api: &Arc<MQClientAPIImpl>,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<bool> {
        match self {
            BrokerCleanupOperation::CleanExpiredConsumerQueue => {
                api.clean_expired_consume_queue(addr, timeout_millis).await
            }
            BrokerCleanupOperation::DeleteExpiredCommitLog => api.delete_expired_commit_log(addr, timeout_millis).await,
            BrokerCleanupOperation::CleanUnusedTopic => api.clean_unused_topic(addr, timeout_millis).await,
        }
    }
}

fn cluster_names_for_admin_operation(cluster_info: &ClusterInfo, cluster: Option<CheetahString>) -> Vec<CheetahString> {
    if let Some(cluster) = cluster.filter(|cluster| !cluster.is_empty()) {
        return vec![cluster];
    }

    cluster_info
        .cluster_addr_table
        .as_ref()
        .map(|table| table.keys().cloned().collect())
        .unwrap_or_default()
}

fn broker_addrs_for_cluster(cluster_info: &ClusterInfo, cluster: &CheetahString) -> Vec<CheetahString> {
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return Vec::new();
    };
    let Some(broker_names) = cluster_addr_table.get(cluster) else {
        return Vec::new();
    };
    let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
        return Vec::new();
    };

    let mut addrs = Vec::new();
    for broker_name in broker_names {
        if let Some(broker_data) = broker_addr_table.get(broker_name) {
            addrs.extend(broker_data.broker_addrs().values().cloned());
        }
    }
    addrs
}

fn topic_list_from_lite_topic_names(
    broker_addr: Option<CheetahString>,
    topic_names: impl IntoIterator<Item = CheetahString>,
) -> TopicList {
    let mut topic_list = topic_names.into_iter().collect::<Vec<_>>();
    topic_list.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    topic_list.dedup();
    TopicList {
        topic_list,
        broker_addr,
    }
}

fn lite_topic_list_from_broker_lite_info(
    broker_addr: Option<CheetahString>,
    lite_info: &GetBrokerLiteInfoResponseBody,
) -> TopicList {
    topic_list_from_lite_topic_names(broker_addr, lite_info.get_topic_meta().keys().cloned())
}

fn lite_subscription_group_list_from_broker_lite_info(
    topic: &CheetahString,
    lite_info: &GetBrokerLiteInfoResponseBody,
) -> GroupList {
    GroupList::new(lite_info.get_group_meta().get(topic).cloned().unwrap_or_default())
}

fn resolve_lite_pull_queue_num(
    field_name: &'static str,
    value: i32,
    fallback_queue_num: i32,
    allow_fallback: bool,
) -> rocketmq_error::RocketMQResult<u32> {
    let resolved = if value > 0 {
        value
    } else if allow_fallback && fallback_queue_num > 0 {
        fallback_queue_num
    } else {
        return Err(mq_client_err!(format!("{field_name} must be positive")));
    };
    u32::try_from(resolved).map_err(|error| mq_client_err!(format!("{field_name} is out of range: {error}")))
}

fn lite_pull_topic_config(
    topic: CheetahString,
    queue_num: i32,
    topic_sys_flag: i32,
    read_queue_nums: i32,
    write_queue_nums: i32,
    update_existing: bool,
) -> rocketmq_error::RocketMQResult<TopicConfig> {
    if topic.is_empty() {
        return Err(mq_client_err!("Lite pull topic cannot be empty"));
    }
    if topic_sys_flag < 0 {
        return Err(mq_client_err!("topicSysFlag must be non-negative"));
    }

    let read_queue_nums = resolve_lite_pull_queue_num("readQueueNums", read_queue_nums, queue_num, !update_existing)?;
    let write_queue_nums =
        resolve_lite_pull_queue_num("writeQueueNums", write_queue_nums, queue_num, !update_existing)?;
    let topic_sys_flag = u32::try_from(topic_sys_flag)
        .map_err(|error| mq_client_err!(format!("topicSysFlag is out of range: {error}")))?;

    let mut config = TopicConfig::with_sys_flag(
        topic,
        read_queue_nums,
        write_queue_nums,
        PermName::PERM_READ | PermName::PERM_WRITE,
        topic_sys_flag,
    );
    config.attributes.insert(
        TopicAttributes::topic_message_type_attribute().name().clone(),
        CheetahString::from_static_str(TopicMessageType::Lite.as_str()),
    );
    Ok(config)
}

pub struct DefaultMQAdminExtImpl {
    service_state: ServiceState,
    client_instance: Option<ArcMut<MQClientInstance>>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    timeout_millis: Duration,
    kv_namespace_to_delete_list: Vec<CheetahString>,
    client_config: ClientConfig,
    admin_ext_group: CheetahString,
}

impl DefaultMQAdminExtImpl {
    pub fn new(
        rpc_hook: Option<Arc<dyn RPCHook>>,
        timeout_millis: Duration,
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
    ) -> Self {
        DefaultMQAdminExtImpl {
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

    fn mq_client_api(&self) -> rocketmq_error::RocketMQResult<Arc<MQClientAPIImpl>> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()
    }

    fn remoting_timeout_millis(&self) -> rocketmq_error::RocketMQResult<u64> {
        timeout_millis_to_u64(self.timeout_millis)
    }

    async fn query_topics_by_consumer_from_route(
        &self,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        let timeout = self.remoting_timeout_millis()?;
        let retry_topic: CheetahString = mix_all::get_retry_topic(&group).into();
        let topic_route = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&retry_topic, timeout)
            .await?;
        let Some(route_data) = topic_route else {
            return Ok(TopicList::default());
        };

        let mut result = TopicList::default();
        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let topic_list = self
                .mq_client_api()?
                .query_topics_by_consumer(&addr, QueryTopicsByConsumerRequestHeader::new(group.clone()), timeout)
                .await?;
            for topic in topic_list.topic_list {
                if !result.topic_list.contains(&topic) {
                    result.topic_list.push(topic);
                }
            }
        }

        Ok(result)
    }

    async fn execute_broker_cleanup_operation(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
        operation: BrokerCleanupOperation,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;

        if let Some(addr) = addr.filter(|addr| !addr.is_empty()) {
            return operation.execute(&api, &addr, timeout).await;
        }

        let cluster_info = api.get_broker_cluster_info(timeout).await?;
        let mut result = false;
        for cluster_name in cluster_names_for_admin_operation(&cluster_info, cluster) {
            for broker_addr in broker_addrs_for_cluster(&cluster_info, &cluster_name) {
                result = operation.execute(&api, &broker_addr, timeout).await?;
            }
        }

        Ok(result)
    }

    pub async fn create_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        if acl_info.subject.as_ref().is_none_or(|subject| subject.is_empty()) {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(
                "ACL subject is required".into(),
            ));
        }

        if let Some(ref client_instance) = self.client_instance {
            client_instance
                .get_mq_client_api_impl()?
                .create_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    pub async fn update_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        if acl_info.subject.as_ref().is_none_or(|subject| subject.is_empty()) {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(
                "ACL subject is required".into(),
            ));
        }

        if let Some(ref client_instance) = self.client_instance {
            client_instance
                .get_mq_client_api_impl()?
                .update_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    pub async fn create_user_with_user_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("User username is required".into()))?;

        let password = user_info.password.clone().unwrap_or_default();
        let user_type = user_info.user_type.clone().unwrap_or_default();

        self.create_user(broker_addr, username, password, user_type).await
    }

    pub async fn update_user_with_user_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("User username is required".into()))?;

        let password = user_info.password.clone().unwrap_or_default();
        let user_type = user_info.user_type.clone().unwrap_or_default();
        let user_status = user_info.user_status.clone().unwrap_or_default();

        self.update_user(broker_addr, username, password, user_type, user_status)
            .await
    }

    pub async fn pull_message_from_queue(
        &self,
        broker_addr: &str,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<crate::consumer::pull_result::PullResult> {
        let sys_flag = PullSysFlag::build_sys_flag(false, false, true, false);

        let request_header = PullMessageRequestHeader {
            consumer_group: CheetahString::from_static_str(mix_all::TOOLS_CONSUMER_GROUP),
            topic: mq.topic().clone(),
            lite_topic: None,
            queue_id: mq.queue_id(),
            queue_offset: offset,
            max_msg_nums: max_nums,
            sys_flag: sys_flag as i32,
            commit_offset: 0,
            suspend_timeout_millis: 0,
            sub_version: 0,
            subscription: Some(CheetahString::from(sub_expression)),
            expression_type: None,
            max_msg_bytes: None,
            request_source: None,
            proxy_forward_client_id: None,
            topic_request: None,
        };

        struct NoopPullCallback;
        impl PullCallback for NoopPullCallback {
            async fn on_success(&mut self, _pull_result: PullResultExt) {}
            fn on_exception(&mut self, _e: rocketmq_error::RocketMQError) {}
        }

        let api_impl = self.mq_client_api()?;

        let mut result = MQClientAPIImpl::pull_message(
            api_impl,
            CheetahString::from(broker_addr),
            request_header,
            timeout_millis,
            CommunicationMode::Sync,
            NoopPullCallback,
        )
        .await?
        .ok_or_else(|| sync_pull_result_missing("DefaultMQAdminExtImpl::pull_message_from_queue"))?;

        if result.pull_result.pull_status == PullStatus::Found {
            if let Some(mut message_binary) = result.message_binary.take() {
                let msg_vec = message_decoder::decodes_batch(&mut message_binary, true, true);
                result.pull_result.msg_found_list = Some(msg_vec);
            }
        }

        Ok(result.pull_result)
    }

    pub async fn query_message_by_key(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        _key_type: CheetahString,
        _last_key: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<crate::base::query_result::QueryResult> {
        self.query_message_by_key_internal(cluster_name, topic, key, max_num, begin_timestamp, end_timestamp, false)
            .await
    }

    pub async fn query_message_by_unique_key(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        unique_key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> rocketmq_error::RocketMQResult<crate::base::query_result::QueryResult> {
        self.query_message_by_key_internal(
            cluster_name,
            topic,
            unique_key,
            max_num,
            begin_timestamp,
            end_timestamp,
            true,
        )
        .await
    }

    async fn query_message_by_key_internal(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        unique_key_flag: bool,
    ) -> rocketmq_error::RocketMQResult<crate::base::query_result::QueryResult> {
        let route_topic = cluster_name.unwrap_or_else(|| topic.clone());
        let topic_route_data = self
            .examine_topic_route_info(route_topic.clone())
            .await?
            .ok_or_else(|| admin_route_not_found(&route_topic))?;

        let mut message_list: Vec<MessageExt> = Vec::new();
        let mut index_last_update_timestamp: u64 = 0;

        let api_impl = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;

        for broker_data in &topic_route_data.broker_datas {
            let broker_addr = match broker_data.select_broker_addr() {
                Some(addr) => addr,
                None => continue,
            };

            let request_header =
                rocketmq_remoting::protocol::header::query_message_request_header::QueryMessageRequestHeader {
                    topic: topic.clone(),
                    key: key.clone(),
                    max_num,
                    begin_timestamp,
                    end_timestamp,
                    index_type: Some(CheetahString::from_static_str(if unique_key_flag {
                        MessageConst::INDEX_UNIQUE_TYPE
                    } else {
                        MessageConst::INDEX_KEY_TYPE
                    })),
                    last_key: None,
                    topic_request_header: None,
                };

            match MQClientAPIImpl::query_message(&api_impl, &broker_addr, request_header, unique_key_flag, timeout)
                .await
            {
                Ok(Some((response_header, body))) => {
                    if let Some(mut body_bytes) = body {
                        let msgs = message_decoder::decodes_batch(&mut body_bytes, true, true);
                        message_list.extend(msgs);
                    }
                    let response_index_timestamp = java_long_to_u64(
                        "queryMessage",
                        "indexLastUpdateTimestamp",
                        response_header.index_last_update_timestamp,
                    )?;
                    if response_index_timestamp > index_last_update_timestamp {
                        index_last_update_timestamp = response_index_timestamp;
                    }
                }
                Ok(None) => {
                    // No messages found on this broker, continue
                }
                Err(e) => {
                    tracing::warn!("Failed to query message by key from broker {}: {}", broker_addr, e);
                }
            }
        }

        Ok(crate::base::query_result::QueryResult::new(
            index_last_update_timestamp,
            message_list,
        ))
    }
}

#[allow(unused_variables)]
#[allow(unused_mut)]
impl MQAdminExt for DefaultMQAdminExtImpl {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                self.client_config.change_instance_name_to_pid();
                if "{}".eq(&self.client_config.socks_proxy_config) {
                    self.client_config.socks_proxy_config =
                        env::var(SOCKS_PROXY_JSON).unwrap_or_else(|_| "{}".to_string()).into();
                }
                self.client_instance = Some(
                    MQClientManager::get_instance()
                        .get_or_create_mq_client_instance(self.client_config.clone(), self.rpc_hook.clone()),
                );

                let group = &self.admin_ext_group.clone();
                let register_ok = self
                    .client_instance
                    .as_mut()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
                    .register_admin_ext(group, MQAdminExtInnerImpl)
                    .await;
                if !register_ok {
                    self.service_state = ServiceState::StartFailed;
                    return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                        "The adminExt group[{}] has created already, specified another name please.{}",
                        self.admin_ext_group,
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                let arc_mut = self
                    .client_instance
                    .clone()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;
                self.client_instance
                    .as_mut()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
                    .start(arc_mut)
                    .await?;
                self.service_state = ServiceState::Running;
                info!("the adminExt [{}] start OK", self.admin_ext_group);
                Ok(())
            }
            ServiceState::Running | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                Err(rocketmq_error::RocketMQError::ClientAlreadyStarted)
            }
        }
    }

    async fn shutdown(&mut self) {
        match self.service_state {
            ServiceState::CreateJust | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                // do nothing
            }
            ServiceState::Running => {
                if let Some(instance) = self.client_instance.as_mut() {
                    instance.unregister_admin_ext(&self.admin_ext_group).await;
                    instance.shutdown().await;
                }
                self.service_state = ServiceState::ShutdownAlready;
            }
        }
    }

    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .add_broker(&broker_container_addr, broker_config, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .remove_broker(
                    &broker_container_addr,
                    cluster_name,
                    broker_name,
                    broker_id,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let validator_input = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>();
        Validators::check_broker_config(&validator_input)?;

        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .update_broker_config(&broker_addr, properties, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, CheetahString>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_config(&broker_addr, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        let topic = config
            .topic_name
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("Topic name is required".into()))?;
        let attributes = encode_topic_attributes(&config.attributes);
        let request_header = CreateTopicRequestHeader {
            topic,
            default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            read_queue_nums: config.read_queue_nums as i32,
            write_queue_nums: config.write_queue_nums as i32,
            perm: config.perm as i32,
            topic_filter_type: CheetahString::from_static_str(config.topic_filter_type.as_str()),
            topic_sys_flag: Some(config.topic_sys_flag as i32),
            order: config.order,
            attributes,
            force: Some(false),
            topic_request_header: None,
        };

        self.mq_client_api()?
            .update_or_create_topic(&addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        for config in topic_config_list {
            self.create_and_update_topic_config(addr.clone(), config).await?;
        }
        Ok(())
    }

    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .create_and_update_plain_access_config(addr, &config, self.remoting_timeout_millis()?)
            .await
    }

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_plain_access_config(addr, access_key, self.remoting_timeout_millis()?)
            .await
    }

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mq_client_api = self.mq_client_api()?;

        validate_acl_file_path_for_global_white_addr_config(acl_file_full_path.as_ref())?;

        mq_client_api
            .update_global_white_addrs_config(addr, global_white_addrs, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_broker_cluster_acl_version_info(
        &self,
        addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        let version_info = self
            .mq_client_api()?
            .get_broker_cluster_acl_version_info(addr, self.remoting_timeout_millis()?)
            .await?;
        serde_json::to_string(&version_info)
            .map(CheetahString::from_string)
            .map_err(|error| mq_client_err!(format!("encode ClusterAclVersionInfo failed: {error}")))
    }

    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .create_subscription_group(&addr, &config, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        for config in configs {
            self.create_and_update_subscription_group_config(broker_addr.clone(), config)
                .await?;
        }
        Ok(())
    }

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfig> {
        self.mq_client_api()?
            .get_subscription_group_config(&addr, group, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        let timeout = self.remoting_timeout_millis()?;
        let request_header = GetTopicStatsInfoRequestHeader {
            topic: topic.clone(),
            topic_request_header: None,
        };
        if let Some(addr) = broker_addr {
            return self
                .mq_client_api()?
                .get_topic_stats_info(&addr, request_header, timeout)
                .await;
        }

        let topic_route = self.examine_topic_route_info(topic).await?;
        let mut result = TopicStatsTable::new();
        if let Some(route_data) = topic_route {
            for broker_data in &route_data.broker_datas {
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let stats = self
                        .mq_client_api()?
                        .get_topic_stats_info(master_addr, request_header.clone(), timeout)
                        .await?;
                    result.get_offset_table_mut().extend(stats.into_offset_table());
                }
            }
        }

        Ok(result)
    }

    async fn examine_topic_stats_concurrent(&self, topic: CheetahString) -> AdminToolResult<TopicStatsTable> {
        match self.examine_topic_stats(topic, None).await {
            Ok(stats) => AdminToolResult::success(stats),
            Err(error) => AdminToolResult::failure(
                crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum::RemotingError,
                error.to_string(),
            ),
        }
    }

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList> {
        self.mq_client_api()?
            .get_all_topic_list_from_name_server(self.remoting_timeout_millis()?)
            .await
    }

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.mq_client_api()?
            .get_topics_by_cluster(cluster_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.mq_client_api()?
            .get_broker_runtime_info(&broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        let timeout = timeout_millis.unwrap_or(self.remoting_timeout_millis()?);
        let topic_str = topic.clone().unwrap_or_default();

        if let Some(addr) = broker_addr {
            let request_header = GetConsumeStatsRequestHeader {
                consumer_group,
                topic: topic_str,
                topic_request_header: None,
            };
            return self
                .mq_client_api()?
                .get_consume_stats(&addr, request_header, timeout)
                .await;
        }

        let retry_topic: CheetahString = rocketmq_common::common::mix_all::get_retry_topic(&consumer_group).into();
        let topic_route = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&retry_topic, timeout)
            .await?;

        let mut result = ConsumeStats::new();

        if let Some(route_data) = topic_route {
            for bd in &route_data.broker_datas {
                if let Some(master_addr) = bd.broker_addrs().get(&rocketmq_common::common::mix_all::MASTER_ID) {
                    let request_header = GetConsumeStatsRequestHeader {
                        consumer_group: consumer_group.clone(),
                        topic: topic_str.clone(),
                        topic_request_header: None,
                    };
                    let cs = self
                        .mq_client_api()?
                        .get_consume_stats(master_addr, request_header, timeout)
                        .await?;

                    result.get_offset_table_mut().extend(cs.offset_table);
                    let new_tps = result.get_consume_tps() + cs.consume_tps;
                    result.set_consume_tps(new_tps);
                }
            }
        }

        Ok(result)
    }

    async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        check_store_time: i64,
    ) -> rocketmq_error::RocketMQResult<CheckRocksdbCqWriteResult> {
        self.mq_client_api()?
            .check_rocksdb_cq_write_progress(&broker_addr, topic, check_store_time, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_broker_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        self.mq_client_api()?
            .get_broker_cluster_info(self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection> {
        let mut result = ConsumerConnection::new();
        let timeout = self.remoting_timeout_millis()?;

        let selected_addr = if let Some(broker_addr) = broker_addr {
            Some(broker_addr)
        } else {
            let topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_group.as_str()));
            let topic_route_data = self
                .mq_client_api()?
                .get_topic_route_info_from_name_server(&topic, timeout)
                .await?;

            topic_route_data.and_then(|topic_route_data| {
                topic_route_data
                    .broker_datas
                    .choose(&mut rand::rng())
                    .and_then(|broker_data| broker_data.select_broker_addr())
            })
        };

        if let Some(broker_addr) = selected_addr {
            result = self
                .mq_client_api()?
                .get_consumer_connection_list(broker_addr.as_str(), consumer_group.clone(), timeout)
                .await?;
        }

        if result.get_connection_set().is_empty() {
            return Err(mq_client_err!(
                rocketmq_remoting::code::response_code::ResponseCode::ConsumerNotOnline,
                "Not found the consumer group connection"
            ));
        }

        Ok(result)
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        let mut result = ProducerConnection::new();
        let timeout = self.remoting_timeout_millis()?;

        if let Some(topic_route_data) = self.examine_topic_route_info(topic).await? {
            let brokers = &topic_route_data.broker_datas;
            let selected_addr = brokers
                .choose(&mut rand::rng())
                .and_then(|broker_data| broker_data.select_broker_addr());
            if let Some(addr) = selected_addr {
                result = self
                    .mq_client_api()?
                    .get_producer_connection_list(addr.as_str(), producer_group.clone(), timeout)
                    .await?;
            }
        }

        if result.connection_set().is_empty() {
            return Err(mq_client_err!("Not found the producer group connection"));
        }

        Ok(result)
    }

    async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        self.mq_client_api()?
            .get_all_producer_info(broker_addr.as_str(), self.remoting_timeout_millis()?)
            .await
    }

    async fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        self.client_instance
            .as_ref()
            .map(|client_instance| {
                client_instance
                    .get_mq_client_api_impl()
                    .map(|mq_client_api| mq_client_api.get_name_server_address_list().to_vec())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.mq_client_api()?
            .wipe_write_perm_of_broker(namesrv_addr, broker_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.mq_client_api()?
            .add_write_perm_of_broker(namesrv_addr, broker_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn put_kv_config(&self, namespace: CheetahString, key: CheetahString, value: CheetahString) {
        if let Err(error) = self.create_and_update_kv_config(namespace, key, value).await {
            warn!("put_kv_config failed: {}", error);
        }
    }

    async fn get_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        Ok(self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .get_kvconfig_value(namespace, key, self.remoting_timeout_millis()?)
            .await?
            .unwrap_or_default())
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.mq_client_api()?
            .get_kvlist_by_namespace(namespace, self.remoting_timeout_millis()?)
            .await
    }

    async fn delete_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let cluster_info = self.examine_broker_cluster_info().await?;
        let mut broker_addrs = HashSet::new();
        if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
            if let Some(broker_names) = cluster_addr_table.get(&cluster_name) {
                if let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() {
                    for broker_name in broker_names {
                        if let Some(broker_data) = broker_addr_table.get(broker_name) {
                            broker_addrs.extend(broker_data.broker_addrs().values().cloned());
                        }
                    }
                }
            }
        }
        self.delete_topic_in_broker(broker_addrs, topic_name.clone()).await?;

        let namesrv_addrs: HashSet<CheetahString> = self.get_name_server_address_list().await.into_iter().collect();
        self.delete_topic_in_name_server(namesrv_addrs, Some(cluster_name), topic_name)
            .await
    }

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = DeleteTopicRequestHeader {
            topic: topic.clone(),
            topic_request_header: None,
        };
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        for addr in addrs {
            api.delete_topic_in_broker(
                &addr,
                DeleteTopicRequestHeader {
                    topic: request_header.topic.clone(),
                    topic_request_header: None,
                },
                timeout,
            )
            .await?;
        }
        Ok(())
    }

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = DeleteTopicFromNamesrvRequestHeader::new(topic, cluster_name);
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        for addr in addrs {
            api.delete_topic_in_nameserver(&addr, request_header.clone(), timeout)
                .await?;
        }
        Ok(())
    }

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_subscription_group(
                &addr,
                group_name,
                remove_offset.unwrap_or(false),
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .put_kvconfig_value(namespace, key, value, self.remoting_timeout_millis()?)
            .await
    }

    async fn delete_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_kvconfig_value(namespace, key, self.remoting_timeout_millis()?)
            .await
    }

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestamp", timestamp)?;
        let topic_route = self.examine_topic_route_info(topic.clone()).await?;
        let mut offset_table = HashMap::new();
        let timeout = self.remoting_timeout_millis()?;

        if let Some(route_data) = topic_route {
            for broker_data in &route_data.broker_datas {
                if let Some(expected_cluster) = cluster_name.as_ref() {
                    if broker_data.cluster() != expected_cluster {
                        continue;
                    }
                }
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let request_header = ResetOffsetRequestHeader {
                        topic: topic.clone(),
                        group: group.clone(),
                        queue_id: -1,
                        offset: Some(-1),
                        timestamp,
                        is_force,
                        topic_request_header: None,
                    };
                    let offsets = self
                        .mq_client_api()?
                        .invoke_broker_to_reset_offset(master_addr, request_header, timeout)
                        .await?;
                    for (mq, offset) in offsets {
                        offset_table.insert(mq, java_long_to_u64("resetOffsetByTimestamp", "offset", offset)?);
                    }
                }
            }
        }

        Ok(offset_table)
    }

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        match self
            .reset_offset_by_timestamp(None, topic.clone(), consumer_group.clone(), timestamp, true)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_consumer_not_online_error(&error) => {
                self.reset_offset_by_timestamp_old(None, consumer_group, topic, timestamp, true)
                    .await?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        let Some(route_data) = self.examine_topic_route_info(topic.clone()).await? else {
            return Ok(HashMap::new());
        };

        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        let mut merged = HashMap::new();
        let mut queried = false;
        let mut last_error = None;

        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            queried = true;
            match api
                .invoke_broker_to_get_consumer_status(
                    addr.as_str(),
                    topic.clone(),
                    group.clone(),
                    client_addr.clone(),
                    timeout,
                )
                .await
            {
                Ok(result) => merge_consume_status_result(&mut merged, result)?,
                Err(error) => {
                    warn!("get_consume_status failed on broker {}: {}", addr, error);
                    last_error = Some(error);
                }
            }
        }

        if !merged.is_empty() || last_error.is_none() || !queried {
            return Ok(merged);
        }

        if let Some(error) = last_error {
            Err(error)
        } else {
            Ok(merged)
        }
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if is_cluster {
            return self
                .mq_client_api()?
                .put_kvconfig_value(
                    CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                    key,
                    value,
                    self.remoting_timeout_millis()?,
                )
                .await;
        }

        let existing = self
            .mq_client_api()?
            .get_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                key.clone(),
                self.remoting_timeout_millis()?,
            )
            .await?
            .unwrap_or_default();

        let merged_order_conf = merge_order_conf_entries(existing.as_str(), value.as_str());

        self.mq_client_api()?
            .put_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                key,
                merged_order_conf.into(),
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList> {
        let topic_route = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, self.remoting_timeout_millis()?)
            .await?;

        if let Some(route_data) = topic_route {
            for bd in &route_data.broker_datas {
                if let Some(master_addr) = bd.broker_addrs().get(&rocketmq_common::common::mix_all::MASTER_ID) {
                    let request_header = QueryTopicConsumeByWhoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    };
                    return self
                        .mq_client_api()?
                        .query_topic_consume_by_who(master_addr, request_header, self.remoting_timeout_millis()?)
                        .await;
                }
            }
        }

        Ok(GroupList::default())
    }

    async fn query_topics_by_consumer(&self, group: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.query_topics_by_consumer_from_route(group).await
    }

    async fn query_topics_by_consumer_concurrent(&self, group: CheetahString) -> AdminToolResult<TopicList> {
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let retry_topic: CheetahString = mix_all::get_retry_topic(&group).into();
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::MQClientError, error.to_string());
            }
        };
        let topic_route = match api.get_topic_route_info_from_name_server(&retry_topic, timeout).await {
            Ok(Some(route_data)) if !route_data.broker_datas.is_empty() => route_data,
            Ok(_) => {
                return AdminToolResult::failure(
                    AdminToolsResultCodeEnum::TopicRouteInfoNotExist,
                    "router info not found.".to_string(),
                )
            }
            Err(error) => {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::MQClientError, error.to_string());
            }
        };

        let mut result = TopicList::default();
        for broker_data in &topic_route.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            match api
                .query_topics_by_consumer(&addr, QueryTopicsByConsumerRequestHeader::new(group.clone()), timeout)
                .await
            {
                Ok(topic_list) => {
                    for topic in topic_list.topic_list {
                        if !result.topic_list.contains(&topic) {
                            result.topic_list.push(topic);
                        }
                    }
                }
                Err(error) => {
                    warn!("query_topics_by_consumer error. group={}, error={}", group, error);
                }
            }
        }

        AdminToolResult::success(result)
    }

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionData> {
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let topic_route = api.get_topic_route_info_from_name_server(&topic, timeout).await?;
        let Some(route_data) = topic_route else {
            return Err(mq_client_err!(format!("Topic route not found for: {topic}")));
        };

        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let request_header = QuerySubscriptionByConsumerRequestHeader {
                group: group.clone(),
                topic: topic.clone(),
                topic_request_header: None,
            };
            return api.query_subscription_by_consumer(&addr, request_header, timeout).await;
        }

        Err(mq_client_err!(format!("Broker address not found for topic: {topic}")))
    }

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.execute_broker_cleanup_operation(cluster, addr, BrokerCleanupOperation::CleanExpiredConsumerQueue)
            .await
    }

    async fn clean_expired_consumer_queue_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.clean_expired_consumer_queue(None, Some(addr)).await
    }

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.execute_broker_cleanup_operation(cluster, addr, BrokerCleanupOperation::DeleteExpiredCommitLog)
            .await
    }

    async fn delete_expired_commit_log_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.delete_expired_commit_log(None, Some(addr)).await
    }

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.execute_broker_cleanup_operation(cluster, addr, BrokerCleanupOperation::CleanUnusedTopic)
            .await
    }

    async fn clean_unused_topic_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.clean_unused_topic(None, Some(addr)).await
    }

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        _metrics: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo> {
        let broker_addr = self
            .examine_consumer_connection_info(consumer_group.clone(), None)
            .await?
            .get_connection_set()
            .iter()
            .find(|connection| connection.get_client_id() == client_id)
            .map(|connection| connection.get_client_addr().clone())
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::IllegalArgument(format!(
                    "Client `{}` was not found in consumer group `{}`",
                    client_id, consumer_group
                ))
            })?;

        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .get_consumer_running_info(
                &broker_addr,
                consumer_group,
                client_id,
                jstack,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        let consumer_connection = self
            .examine_consumer_connection_info(consumer_group.clone(), None)
            .await?;
        let (resolved_client_id, client_addr) =
            select_consumer_direct_connection(&consumer_group, &consumer_connection, Some(&client_id))?;
        let message = MQAdminExt::query_message(self, CheetahString::default(), topic.clone(), msg_id.clone()).await?;
        let request_header = ConsumeMessageDirectlyResultRequestHeader {
            consumer_group,
            client_id: Some(resolved_client_id),
            msg_id: Some(msg_id),
            broker_name: (!message.broker_name().is_empty()).then(|| message.broker_name.clone()),
            topic: Some(topic),
            topic_sys_flag: None,
            group_sys_flag: None,
            topic_request_header: None,
        };

        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .consume_message_directly(&client_addr, request_header, &message, self.remoting_timeout_millis()?)
            .await
    }

    async fn consume_message_directly_ext(
        &self,
        _cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        self.consume_message_directly(consumer_group, client_id, topic, msg_id)
            .await
    }

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let retry_topic: CheetahString = mix_all::get_retry_topic(src_group.as_str()).into();
        let topic_route_data = self
            .examine_topic_route_info(retry_topic.clone())
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for retry topic: {retry_topic}")))?;
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;

        for broker_data in &topic_route_data.broker_datas {
            if let Some(addr) = broker_data.select_broker_addr() {
                api.clone_group_offset(
                    &addr,
                    src_group.clone(),
                    dest_group.clone(),
                    topic.clone(),
                    is_offline,
                    timeout,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn get_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        self.get_topic_cluster_list(topic).await
    }

    async fn get_topic_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        let cluster_info = self.examine_broker_cluster_info().await?;
        let topic_route_data = self
            .examine_topic_route_info(topic.clone().into())
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for: {topic}")))?;
        let broker_data = topic_route_data
            .broker_datas
            .first()
            .ok_or_else(|| mq_client_err!("Broker datas is empty"))?;
        let mut cluster_set = HashSet::new();
        let broker_name = broker_data.broker_name();
        if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
            cluster_set.extend(
                cluster_addr_table
                    .iter()
                    .filter(|(cluster_name, broker_names)| broker_names.contains(broker_name))
                    .map(|(cluster_name, broker_names)| cluster_name.clone()),
            );
        }
        Ok(cluster_set)
    }

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        self.mq_client_api()?
            .get_all_topic_config(&broker_addr, timeout_millis)
            .await
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        let mut topic_config_wrapper = self.get_all_topic_config(broker_addr.clone(), timeout_millis).await?;
        let system_topic_list = self.get_system_topic_list_from_broker(broker_addr).await?;

        if let Some(ref mut topic_table) = topic_config_wrapper.topic_config_table_mut() {
            retain_java_user_topic_config(topic_table, &system_topic_list.topic_list, special_topic);
        }

        Ok(topic_config_wrapper)
    }

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = update_consume_offset_request_header(consume_group, &mq, offset)?;
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;

        client_instance
            .get_mq_client_api_impl()?
            .update_consumer_offset(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        name_servers: Option<Vec<CheetahString>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .update_name_server_config(properties, name_servers, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        Ok(self
            .mq_client_api()?
            .get_name_server_config(Some(name_servers), self.timeout_millis)
            .await?
            .unwrap_or_default())
    }

    async fn probe_name_server(&self, name_server: CheetahString) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .probe_name_server(&name_server, self.timeout_millis)
            .await
    }

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let message = self
            .query_message(CheetahString::default(), topic.clone(), msg_id.clone())
            .await?;
        let broker_addr = CheetahString::from_string(message.store_host().to_string());
        let broker_msg_id = if message
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            ))
            .is_some()
        {
            message.msg_id().clone()
        } else {
            msg_id
        };

        self.mq_client_api()?
            .resume_check_half_message(&broker_addr, topic, broker_msg_id, self.remoting_timeout_millis()?)
            .await
    }

    async fn set_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mq_client_api = self.mq_client_api()?;
        match mq_client_api
            .set_message_request_mode(
                &broker_addr,
                &topic,
                &consumer_group,
                mode,
                pop_work_group_size,
                timeout_millis,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn reset_offset_by_queue_id(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        reset_offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let (update_header, reset_header) =
            reset_offset_by_queue_id_request_headers(consumer_group, topic_name, queue_id, reset_offset)?;
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;
        let timeout_millis = self.remoting_timeout_millis()?;

        client_instance
            .get_mq_client_api_impl()?
            .update_consumer_offset(&broker_addr, update_header, timeout_millis)
            .await?;

        let offsets = client_instance
            .get_mq_client_api_impl()?
            .invoke_broker_to_reset_offset(&broker_addr, reset_header, timeout_millis)
            .await?;

        for (mq, old_offset) in offsets {
            info!(
                "Reset single message queue {} offset from {} to {}",
                mq, old_offset, reset_offset
            );
        }

        Ok(())
    }

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_topic_config(&addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .create_static_topic(
                &addr,
                default_topic,
                topic_config,
                mapping_detail,
                force,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn get_controller_meta_data(
        &self,
        controller_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetMetaDataResponseHeader> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_controller_metadata(controller_addr, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let master_flush_offset = master_flush_offset_to_java_long(master_flush_offset)?;
            mq_client_instance
                .get_mq_client_api_impl()?
                .reset_master_flush_offset(&broker_addr, master_flush_offset)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mut result: HashMap<CheetahString, HashMap<CheetahString, CheetahString>> = HashMap::new();
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let controller_servers =
                controller_servers_or_namesrv(controller_servers, &mq_client_api.get_name_server_address_list());

            for controller_addr in controller_servers {
                let config = mq_client_api
                    .get_controller_config(controller_addr.clone(), timeout_millis)
                    .await?;
                result.insert(controller_addr, config);
            }

            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .update_controller_config(properties, controllers, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .clean_controller_broker_data(
                    controller_addr,
                    cluster_name,
                    broker_name,
                    broker_controller_ids_to_clean,
                    is_clean_living_broker,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .update_cold_data_flow_ctr_group_config(broker_addr, properties, self.remoting_timeout_millis()?)
            .await
    }

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .remove_cold_data_flow_ctr_group_config(broker_addr, consumer_group, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.mq_client_api()?
            .get_cold_data_flow_ctr_info(broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.mq_client_api()?
            .set_commit_log_read_ahead_mode(broker_addr, mode, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let user_info = UserInfo {
            username: Some(username),
            user_type: Some(user_type),
            password: Some(password),
            user_status: None,
        };

        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            mq_client_api
                .create_user(broker_addr, &user_info, timeout_millis)
                .await?;
            Ok(())
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut user_info = UserInfo {
            username: Some(username),
            user_type: Some(user_type),
            password: Some(password),
            user_status: Some(user_status),
        };

        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            mq_client_api
                .update_user(broker_addr, &user_info, timeout_millis)
                .await?;
            Ok(())
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn delete_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            mq_client_api.delete_user(broker_addr, username, timeout_millis).await?;
            Ok(())
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let acl_info = build_acl_info(subject, resources, actions, source_ips, decision);
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .create_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let acl_info = build_acl_info(subject, resources, actions, source_ips, decision);
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .update_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref client_instance) = self.client_instance {
            let mq_client_api = client_instance.get_mq_client_api_impl()?;
            mq_client_api
                .delete_acl(broker_addr, subject, resource, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        queue_num: i32,
        topic_sys_flag: i32,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let config = lite_pull_topic_config(
            topic,
            queue_num,
            topic_sys_flag,
            read_queue_nums,
            write_queue_nums,
            false,
        )?;
        self.create_and_update_topic_config(addr, config).await
    }

    async fn update_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let config = lite_pull_topic_config(topic, 0, 0, read_queue_nums, write_queue_nums, true)?;
        self.create_and_update_topic_config(addr, config).await
    }

    async fn get_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        let lite_info = self
            .mq_client_api()?
            .get_broker_lite_info(&addr, self.remoting_timeout_millis()?)
            .await?;
        if !lite_info.get_topic_meta().contains_key(&topic) {
            return Err(mq_client_err!(format!("Lite pull topic not found: {topic}")));
        }
        self.examine_topic_config(addr, topic).await
    }

    async fn delete_lite_pull_topic(
        &self,
        addr: CheetahString,
        cluster_name: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.delete_topic_in_broker(HashSet::from([addr]), topic.clone())
            .await?;
        if cluster_name.is_empty() {
            return Ok(());
        }
        let namesrv_addrs = self.get_name_server_address_list().await.into_iter().collect();
        self.delete_topic_in_name_server(namesrv_addrs, Some(cluster_name), topic)
            .await
    }

    async fn query_lite_pull_topic_list(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        let lite_info = self
            .mq_client_api()?
            .get_broker_lite_info(&addr, self.remoting_timeout_millis()?)
            .await?;
        Ok(lite_topic_list_from_broker_lite_info(Some(addr), &lite_info))
    }

    async fn query_lite_pull_topic_by_cluster(
        &self,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        let timeout_millis = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let cluster_info = api.get_broker_cluster_info(timeout_millis).await?;
        let mut topic_names = HashSet::new();

        for broker_addr in broker_addrs_for_cluster(&cluster_info, &cluster_name) {
            let lite_info = api.get_broker_lite_info(&broker_addr, timeout_millis).await?;
            topic_names.extend(lite_info.get_topic_meta().keys().cloned());
        }

        Ok(topic_list_from_lite_topic_names(None, topic_names))
    }

    async fn query_lite_pull_subscription_list(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GroupList> {
        let lite_info = self
            .mq_client_api()?
            .get_broker_lite_info(&addr, self.remoting_timeout_millis()?)
            .await?;
        Ok(lite_subscription_group_list_from_broker_lite_info(&topic, &lite_info))
    }

    async fn update_lite_pull_consumer_offset(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        group: CheetahString,
        queue_id: i32,
        offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = lite_pull_update_consumer_offset_request_header(topic, group, queue_id, offset)?;
        self.mq_client_api()?
            .update_consumer_offset(&addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_consume_stats_with_queue(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        queue_id: Option<i32>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        let mut stats = self
            .examine_consume_stats(consumer_group, topic.clone(), None, None, None)
            .await?;
        filter_consume_stats(&mut stats, topic.as_ref(), queue_id);
        Ok(stats)
    }

    async fn examine_consume_stats_concurrent(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        match self
            .examine_consume_stats(consumer_group, topic, None, None, None)
            .await
        {
            Ok(stats) => AdminToolResult::success(stats),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }

    async fn examine_consume_stats_concurrent_with_cluster(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        match self
            .examine_consume_stats(consumer_group, topic, cluster_name, None, None)
            .await
        {
            Ok(stats) => AdminToolResult::success(stats),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }

    async fn export_rocksdb_consumer_offset_to_json(
        &self,
        broker_addr: CheetahString,
        file_path: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !file_path.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "exportRocksDB consumerOffsets filePath is local-mode only and cannot be sent over RPC",
            ));
        }

        self.mq_client_api()?
            .export_rocksdb_config_to_json(
                broker_addr,
                vec![CheetahString::from_static_str(ROCKSDB_CONFIG_TYPE_CONSUMER_OFFSETS)],
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn export_rocksdb_consumer_offset_from_memory(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.mq_client_api()?
            .get_all_consumer_offset_json(broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn sync_broker_member_group(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !controller_addr.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "syncBrokerMemberGroup uses NameServer; controllerAddr is not supported by this facade",
            ));
        }

        self.mq_client_api()?
            .sync_broker_member_group(&cluster_name, &broker_name, false)
            .await?;
        Ok(())
    }

    async fn get_topic_config_by_topic_name(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_topic_config(&broker_addr, topic_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn notify_min_broker_id_changed(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        min_broker_id: u64,
        min_broker_addr: CheetahString,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = notify_min_broker_id_change_request_header(
            min_broker_id,
            min_broker_addr,
            offline_broker_addr,
            ha_broker_addr,
        )?;
        let mq_client_api = self.mq_client_api()?;
        let broker_member_group = mq_client_api
            .sync_broker_member_group(&cluster_name, &broker_name, false)
            .await?
            .unwrap_or_else(|| BrokerMemberGroup::new(cluster_name.clone(), broker_name.clone()));
        let broker_addrs = choose_min_broker_notify_addrs(
            &broker_member_group.broker_addrs,
            min_broker_id,
            request_header.offline_broker_addr.as_ref(),
        );

        if broker_addrs.is_empty() {
            return Err(RocketMQError::illegal_argument(format!(
                "notifyMinBrokerIdChanged cannot resolve broker addresses for cluster `{}` broker `{}`",
                cluster_name, broker_name
            )));
        }

        for broker_addr in broker_addrs {
            mq_client_api
                .notify_min_broker_id_changed(&broker_addr, request_header.clone(), 300)
                .await?;
        }
        Ok(())
    }

    async fn get_topic_stats_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        self.mq_client_api()?
            .get_topic_stats_info(
                &broker_addr,
                GetTopicStatsInfoRequestHeader {
                    topic,
                    topic_request_header: None,
                },
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn query_broker_has_topic(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        map_topic_config_lookup_result(self.get_topic_config_by_topic_name(broker_addr, topic).await)
    }

    async fn get_system_topic_list_from_broker(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        self.mq_client_api()?
            .get_system_topic_list_from_broker(&broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_route_info_with_timeout(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, timeout_millis)
            .await
    }

    async fn export_pop_records(&self, broker_addr: CheetahString, timeout: u64) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?.export_pop_record(broker_addr, timeout).await
    }

    async fn switch_timer_engine(
        &self,
        broker_addr: CheetahString,
        des_timer_engine: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .switch_timer_engine(&broker_addr, des_timer_engine, self.remoting_timeout_millis()?)
            .await
    }

    async fn trigger_lite_dispatch(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .trigger_lite_dispatch(&broker_addr, group, client_id, self.remoting_timeout_millis()?)
            .await
    }
    #[allow(deprecated)]
    async fn delete_topic_in_broker_concurrent(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> AdminToolResult<BrokerOperatorResult> {
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => {
                return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string());
            }
        };
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let mut success_list = Vec::new();
        let mut failure_list = Vec::new();

        for addr in addrs {
            let request_header = DeleteTopicRequestHeader {
                topic: topic.clone(),
                topic_request_header: None,
            };
            match api.delete_topic_in_broker(&addr, request_header, timeout).await {
                Ok(()) => success_list.push(addr),
                Err(error) => {
                    warn!("deleteTopicInBroker error. topic={}, broker={}, {}", topic, addr, error);
                    failure_list.push(addr);
                }
            }
        }

        AdminToolResult::success(broker_operator_result(success_list, failure_list))
    }

    async fn reset_offset_by_timestamp_old(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestampOld", timestamp)?;
        let mut route_topic = topic.clone();
        if !topic.is_empty()
            && (mix_all::is_lmq(Some(topic.as_str()))
                || topic.as_str() == format!("{}wheel_timer", TopicValidator::SYSTEM_TOPIC_PREFIX))
            && cluster_name.as_ref().is_some_and(|name| !name.is_empty())
        {
            if let Some(cluster_name) = cluster_name {
                route_topic = cluster_name;
            }
        }
        let topic_route_data = self.examine_topic_route_info(route_topic).await?;
        let mut rollback_stats_list = Vec::new();

        if let Some(route_data) = topic_route_data {
            let mut topic_route_map = HashMap::new();
            for queue_data in &route_data.queue_datas {
                topic_route_map.insert(queue_data.broker_name().to_string(), queue_data.clone());
            }

            for broker_data in &route_data.broker_datas {
                if let Some(addr) = broker_data.select_broker_addr() {
                    if let Some(queue_data) = topic_route_map.get(broker_data.broker_name().as_str()) {
                        let mut rollback_stats = self
                            .reset_offset_by_timestamp_old_on_broker(
                                addr,
                                queue_data,
                                consumer_group.clone(),
                                topic.clone(),
                                timestamp,
                                force,
                            )
                            .await?;
                        rollback_stats_list.append(&mut rollback_stats);
                    }
                }
            }
        }

        Ok(rollback_stats_list)
    }
    #[allow(deprecated)]
    async fn reset_offset_new_concurrent(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult> {
        let timestamp = match timestamp_to_java_long("resetOffsetNewConcurrent", timestamp) {
            Ok(timestamp) => timestamp,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let route_data = match api.get_topic_route_info_from_name_server(&topic, timeout).await {
            Ok(Some(route_data)) if !route_data.broker_datas.is_empty() => route_data,
            Ok(_) => {
                return AdminToolResult::failure(
                    AdminToolsResultCodeEnum::TopicRouteInfoNotExist,
                    "topic router info not found".to_string(),
                );
            }
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };

        let mut topic_route_map = HashMap::new();
        for queue_data in &route_data.queue_datas {
            topic_route_map.insert(queue_data.broker_name().clone(), queue_data.clone());
        }

        let mut success_list = Vec::new();
        let mut failure_list = Vec::new();
        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };

            let reset_header = ResetOffsetRequestHeader {
                topic: topic.clone(),
                group: group.clone(),
                queue_id: -1,
                offset: Some(-1),
                timestamp,
                is_force: true,
                topic_request_header: None,
            };

            match api.invoke_broker_to_reset_offset(&addr, reset_header, timeout).await {
                Ok(_) => success_list.push(addr),
                Err(error) if is_consumer_not_online_error(&error) => {
                    match topic_route_map.get(broker_data.broker_name()) {
                        Some(queue_data) => {
                            match self
                                .reset_offset_by_timestamp_old_on_broker(
                                    addr.clone(),
                                    queue_data,
                                    group.clone(),
                                    topic.clone(),
                                    timestamp,
                                    true,
                                )
                                .await
                            {
                                Ok(_) => success_list.push(addr),
                                Err(error) => {
                                    warn!(
                                        "resetOffsetByTimestampOld error. addr={}, topic={}, group={}, timestamp={}, \
                                         {}",
                                        addr, topic, group, timestamp, error
                                    );
                                    failure_list.push(addr);
                                }
                            }
                        }
                        None => {
                            warn!(
                                "resetOffsetByTimestampOld error. addr={}, topic={}, group={}, timestamp={}, missing \
                                 queue data for broker {}",
                                addr,
                                topic,
                                group,
                                timestamp,
                                broker_data.broker_name()
                            );
                            failure_list.push(addr);
                        }
                    }
                }
                Err(error) if response_code_from_error(&error) == Some(ResponseCode::SystemError) => {
                    success_list.push(addr);
                }
                Err(error) => {
                    warn!(
                        "resetOffsetNewConcurrent error. addr={}, topic={}, group={}, timestamp={}, {}",
                        addr, topic, group, timestamp, error
                    );
                    failure_list.push(addr);
                }
            }
        }

        let success = success_list.len() == route_data.broker_datas.len();
        let result = broker_operator_result(success_list, failure_list);
        if success {
            AdminToolResult::success(result)
        } else {
            AdminToolResult::failure_with_data(
                AdminToolsResultCodeEnum::MQBrokerError,
                "operator failure".into(),
                result,
            )
        }
    }

    async fn query_consume_time_span(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<QueueTimeSpan>> {
        let timeout = self.remoting_timeout_millis()?;
        let mut result = Vec::new();
        if let Some(route_data) = self.examine_topic_route_info(topic.clone()).await? {
            for broker_data in &route_data.broker_datas {
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let spans = self
                        .mq_client_api()?
                        .query_consume_time_span(
                            master_addr,
                            QueryConsumeTimeSpanRequestHeader {
                                topic: topic.clone(),
                                group: group.clone(),
                                topic_request_header: None,
                            },
                            timeout,
                        )
                        .await?;
                    result.extend(spans);
                }
            }
        }
        Ok(result)
    }

    async fn query_consume_time_span_concurrent(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>> {
        match self.query_consume_time_span(topic, group).await {
            Ok(spans) => AdminToolResult::success(spans),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }
    #[allow(deprecated)]
    async fn message_track_detail(&self, msg: MessageExt) -> rocketmq_error::RocketMQResult<Vec<MessageTrack>> {
        let group_list = self.query_topic_consume_by_who(msg.topic().clone()).await?;
        let mut result = Vec::with_capacity(group_list.get_group_list().len());

        for group in group_list.get_group_list() {
            let mut track = build_message_track(group.as_str());
            let consumer_connection = match self.examine_consumer_connection_info(group.clone(), None).await {
                Ok(connection) => connection,
                Err(error) => {
                    apply_track_error(&mut track, &error);
                    result.push(track);
                    continue;
                }
            };

            match consumer_connection.get_consume_type() {
                Some(ConsumeType::ConsumeActively) => {
                    track.set_track_type(TrackType::Pull);
                }
                Some(ConsumeType::ConsumePassively) => {
                    if consumer_connection.get_message_model() == Some(MessageModel::Broadcasting) {
                        track.set_track_type(TrackType::ConsumeBroadcasting);
                        result.push(track);
                        continue;
                    }

                    let consumed = match self.message_consumed_by_group(&msg, group).await {
                        Ok(consumed) => consumed,
                        Err(error) => {
                            apply_track_error(&mut track, &error);
                            result.push(track);
                            continue;
                        }
                    };

                    if consumed {
                        track.set_track_type(resolve_consumed_track_type(&msg, &consumer_connection));
                    } else {
                        track.set_track_type(TrackType::NotConsumedYet);
                    }
                }
                _ => {}
            }

            result.push(track);
        }

        result.sort_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
        Ok(result)
    }
    #[allow(deprecated)]
    async fn message_track_detail_concurrent(&self, msg: MessageExt) -> AdminToolResult<Vec<MessageTrack>> {
        match self.message_track_detail(msg).await {
            Ok(data) => AdminToolResult::success(data),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }

    async fn view_broker_stats_data(
        &self,
        broker_addr: CheetahString,
        stats_name: CheetahString,
        stats_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerStatsData> {
        let request_header = ViewBrokerStatsDataRequestHeader { stats_name, stats_key };
        self.mq_client_api()?
            .view_broker_stats_data(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: CheetahString,
        is_order: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumeStatsList> {
        self.mq_client_api()?
            .fetch_consume_stats_in_broker(&broker_addr, GetConsumeStatsInBrokerHeader { is_order }, timeout_millis)
            .await
    }

    async fn get_all_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        self.mq_client_api()?
            .get_all_subscription_group_config(&broker_addr, timeout_millis)
            .await
    }

    async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        let mut subscription_group_wrapper = self.get_all_subscription_group(broker_addr, timeout_millis).await?;

        let system_group_set = get_system_group_set();
        let table = subscription_group_wrapper.get_subscription_group_table_mut();
        // Remove system consumer groups
        table.retain(|key, _| !mix_all::is_sys_consumer_group(key.as_str()) && !system_group_set.contains(key));

        Ok(subscription_group_wrapper)
    }

    async fn query_consume_queue(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: u64,
        count: i32,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<QueryConsumeQueueResponseBody> {
        let index = query_consume_queue_index_to_java_long(index)?;
        self.mq_client_api()?
            .query_consume_queue(
                &broker_addr,
                topic,
                queue_id,
                index,
                count,
                consumer_group,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        topic_name: CheetahString,
        readable: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<GroupForbidden> {
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;
        let request_header = update_group_forbidden_request_header(group_name, topic_name, readable);

        client_instance
            .get_mq_client_api_impl()?
            .update_and_get_group_forbidden(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn query_message(
        &self,
        _cluster_name: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;

        let msg_id_str = msg_id.as_str();

        if let Err(e) = message_decoder::validate_message_id(msg_id_str) {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(format!(
                "Invalid message ID: {}",
                e
            )));
        }

        let message_id = message_decoder::decode_message_id(msg_id_str).map_err(|e| {
            rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {}", e))
        })?;
        let broker_addr =
            CheetahString::from_string(format!("{}:{}", message_id.address.ip(), message_id.address.port()));

        let request_header = ViewMessageRequestHeader {
            topic: Some(topic),
            offset: message_id.offset,
        };

        client_instance
            .get_mq_client_api_impl()?
            .view_message(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<HARuntimeInfo> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_ha_status(broker_addr, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerReplicasInfo> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_in_sync_state_data(controller_address, brokers, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<EpochEntryCache> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_epoch_cache(broker_addr, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .elect_master(
                    controller_addr,
                    cluster_name,
                    broker_name,
                    broker_id,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let user_info = UserInfo {
            username: Some(username),
            password: Some(password),
            user_type: None,
            user_status: None,
        };

        self.mq_client_api()?
            .create_user(broker_addr, &user_info, self.remoting_timeout_millis()?)
            .await
    }

    async fn update_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let user_info = UserInfo {
            username: Some(username),
            password: Some(password),
            user_type: None,
            user_status: None,
        };

        self.mq_client_api()?
            .update_user(broker_addr, &user_info, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<UserInfo>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let result = mq_client_api.get_user(broker_addr, username, timeout_millis).await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn list_users(
        &self,
        broker_addr: CheetahString,
        filter: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<UserInfo>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let result = mq_client_api.list_users(broker_addr, filter, timeout_millis).await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.create_acl_with_acl_info(
            broker_addr,
            AclInfo {
                subject: Some(subject),
                policies: None,
            },
        )
        .await
    }

    async fn update_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.update_acl_with_acl_info(
            broker_addr,
            AclInfo {
                subject: Some(subject),
                policies: None,
            },
        )
        .await
    }

    async fn get_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<AclInfo> {
        let acl_infos = self
            .list_acl(broker_addr.clone(), subject.clone(), CheetahString::default())
            .await?;
        acl_infos
            .into_iter()
            .find(|acl_info| acl_info.subject.as_ref() == Some(&subject))
            .ok_or_else(|| {
                RocketMQError::illegal_argument(format!(
                    "ACL with subject {} was not found on broker {}",
                    subject, broker_addr
                ))
            })
    }

    async fn list_acl(
        &self,
        broker_addr: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<AclInfo>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let result = mq_client_api
                .list_acl(broker_addr, subject_filter, resource_filter, timeout_millis)
                .await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_broker_lite_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetBrokerLiteInfoResponseBody> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_lite_info(&broker_addr, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_parent_topic_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetParentTopicInfoResponseBody> {
        self.mq_client_api()?
            .get_parent_topic_info(&broker_addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_lite_topic_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        lite_topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteTopicInfoResponseBody> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .get_lite_topic_info(
                    &broker_addr,
                    &parent_topic,
                    &lite_topic,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_lite_client_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteClientInfoResponseBody> {
        self.mq_client_api()?
            .get_lite_client_info(
                &broker_addr,
                parent_topic,
                group,
                client_id,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn get_lite_group_info(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
    ) -> rocketmq_error::RocketMQResult<GetLiteGroupInfoResponseBody> {
        self.mq_client_api()?
            .get_lite_group_info(&broker_addr, group, lite_topic, top_k, self.remoting_timeout_millis()?)
            .await
    }

    async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .export_rocksdb_config_to_json(broker_addr, config_types, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn search_offset(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        timestamp: u64,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<u64> {
        let timestamp = search_offset_timestamp_to_java_long(timestamp)?;
        let mq = MessageQueue::from_parts(&topic_name, "", queue_id);
        let offset = self
            .mq_client_api()?
            .search_offset_by_timestamp(
                broker_addr.as_str(),
                &mq,
                timestamp,
                rocketmq_common::common::boundary_type::BoundaryType::Lower,
                timeout_millis,
            )
            .await?;
        java_long_to_u64("searchOffset", "offset", offset)
    }

    async fn min_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.mq_client_api()?
            .get_min_offset(broker_addr.as_str(), &message_queue, timeout_millis)
            .await
    }

    async fn max_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.mq_client_api()?
            .get_max_offset(broker_addr.as_str(), &message_queue, timeout_millis)
            .await
    }
}

impl DefaultMQAdminExtImpl {
    async fn reset_offset_by_timestamp_old_on_broker(
        &self,
        broker_addr: CheetahString,
        queue_data: &QueueData,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let consume_stats = self
            .mq_client_api()?
            .get_consume_stats(
                &broker_addr,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: CheetahString::empty(),
                    topic_request_header: None,
                },
                self.remoting_timeout_millis()?,
            )
            .await?;

        let mut rollback_stats_list = Vec::new();
        let mut has_consumed = false;

        for (queue, offset_wrapper) in &consume_stats.offset_table {
            if queue.topic() == &topic {
                has_consumed = true;
                rollback_stats_list.push(
                    self.reset_offset_consume_offset(
                        broker_addr.clone(),
                        consumer_group.clone(),
                        queue.clone(),
                        offset_wrapper,
                        timestamp,
                        force,
                    )
                    .await?,
                );
            }
        }

        if !has_consumed {
            let topic_status = self
                .mq_client_api()?
                .get_topic_stats_info(
                    &broker_addr,
                    GetTopicStatsInfoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    },
                    self.remoting_timeout_millis()?,
                )
                .await?;

            for queue_id in 0..queue_data.read_queue_nums() {
                let queue = MessageQueue::from_parts(topic.clone(), queue_data.broker_name().clone(), queue_id as i32);
                let mut offset_wrapper = OffsetWrapper::new();
                let topic_offset = topic_status
                    .get_offset_table()
                    .get(&queue)
                    .cloned()
                    .unwrap_or_else(TopicOffset::new);
                offset_wrapper.set_broker_offset(topic_offset.get_max_offset());
                offset_wrapper.set_consumer_offset(topic_offset.get_min_offset());
                rollback_stats_list.push(
                    self.reset_offset_consume_offset(
                        broker_addr.clone(),
                        consumer_group.clone(),
                        queue,
                        &offset_wrapper,
                        timestamp,
                        force,
                    )
                    .await?,
                );
            }
        }

        Ok(rollback_stats_list)
    }

    async fn reset_offset_consume_offset(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        queue: MessageQueue,
        offset_wrapper: &OffsetWrapper,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<RollbackStats> {
        let reset_offset = if timestamp == -1 {
            self.mq_client_api()?
                .get_max_offset(broker_addr.as_str(), &queue, self.remoting_timeout_millis()?)
                .await?
        } else {
            self.mq_client_api()?
                .search_offset_by_timestamp(
                    broker_addr.as_str(),
                    &queue,
                    timestamp,
                    rocketmq_common::common::boundary_type::BoundaryType::Lower,
                    self.remoting_timeout_millis()?,
                )
                .await?
        };

        let mut rollback_stats = RollbackStats {
            broker_name: queue.broker_name().clone(),
            queue_id: queue.queue_id() as i64,
            broker_offset: offset_wrapper.get_broker_offset(),
            consumer_offset: offset_wrapper.get_consumer_offset(),
            timestamp_offset: reset_offset,
            rollback_offset: offset_wrapper.get_consumer_offset(),
        };

        if force || reset_offset <= offset_wrapper.get_consumer_offset() {
            rollback_stats.rollback_offset = reset_offset;
            self.mq_client_api()?
                .update_consumer_offset(
                    &broker_addr,
                    UpdateConsumerOffsetRequestHeader {
                        consumer_group,
                        topic: queue.topic().clone(),
                        queue_id: queue.queue_id(),
                        commit_offset: reset_offset,
                        topic_request_header: None,
                    },
                    self.remoting_timeout_millis()?,
                )
                .await?;
        }

        Ok(rollback_stats)
    }

    async fn message_consumed_by_group(
        &self,
        msg: &MessageExt,
        group: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let consume_stats = self
            .examine_consume_stats(group.clone(), None, None, None, None)
            .await?;
        let cluster_info = self.examine_broker_cluster_info().await?;

        Ok(is_message_consumed(msg, &consume_stats, &cluster_info))
    }
}

fn merge_order_conf_entries(existing: &str, value: &str) -> String {
    let mut entries = HashMap::new();
    for item in existing.split(';').filter(|item| !item.trim().is_empty()) {
        if let Some((broker_name, _)) = item.split_once(':') {
            entries.insert(broker_name.to_string(), item.to_string());
        }
    }
    if let Some((broker_name, _)) = value.split_once(':') {
        entries.insert(broker_name.to_string(), value.to_string());
    } else if !value.trim().is_empty() {
        entries.insert(value.to_string(), value.to_string());
    }

    let mut broker_names: Vec<String> = entries.keys().cloned().collect();
    broker_names.sort();
    broker_names
        .into_iter()
        .filter_map(|broker_name| entries.remove(&broker_name))
        .collect::<Vec<_>>()
        .join(";")
}

fn select_consumer_direct_connection(
    consumer_group: &CheetahString,
    consumer_connection: &ConsumerConnection,
    requested_client_id: Option<&CheetahString>,
) -> rocketmq_error::RocketMQResult<(CheetahString, CheetahString)> {
    let requested = requested_client_id.filter(|client_id| !client_id.is_empty());
    let connection = consumer_connection
        .get_connection_set()
        .iter()
        .find(|connection| {
            requested
                .map(|client_id| connection.get_client_id() == *client_id)
                .unwrap_or_else(|| !connection.get_client_id().is_empty())
        })
        .ok_or_else(|| {
            let message = requested
                .map(|client_id| {
                    format!(
                        "Client `{}` was not found in consumer group `{}`",
                        client_id, consumer_group
                    )
                })
                .unwrap_or_else(|| format!("NO CONSUMER for consumer group `{}`", consumer_group));
            rocketmq_error::RocketMQError::IllegalArgument(message)
        })?;

    Ok((connection.get_client_id(), connection.get_client_addr()))
}

fn build_acl_info(
    subject: CheetahString,
    resources: Vec<CheetahString>,
    actions: Vec<CheetahString>,
    source_ips: Vec<CheetahString>,
    decision: CheetahString,
) -> AclInfo {
    let entries = if resources.is_empty() {
        vec![PolicyEntryInfo {
            resource: None,
            actions: Some(actions),
            source_ips: Some(source_ips),
            decision: if decision.is_empty() { None } else { Some(decision) },
        }]
    } else {
        resources
            .into_iter()
            .map(|resource| PolicyEntryInfo {
                resource: Some(resource),
                actions: Some(actions.clone()),
                source_ips: Some(source_ips.clone()),
                decision: if decision.is_empty() {
                    None
                } else {
                    Some(decision.clone())
                },
            })
            .collect()
    };

    AclInfo {
        subject: Some(subject),
        policies: Some(vec![PolicyInfo {
            policy_type: Some(CheetahString::from_static_str("Custom")),
            entries: Some(entries),
        }]),
    }
}

#[allow(deprecated)]
fn build_message_track(consumer_group: &str) -> MessageTrack {
    MessageTrack {
        consumer_group: consumer_group.to_string(),
        track_type: Some(TrackType::Unknown),
        exception_desc: String::new(),
    }
}

#[allow(deprecated)]
fn resolve_consumed_track_type(msg: &MessageExt, consumer_connection: &ConsumerConnection) -> TrackType {
    let Some(subscription_data) = consumer_connection.get_subscription_table().get(msg.topic()) else {
        return TrackType::Consumed;
    };

    let Some(message_tag) = msg.get_tags() else {
        return TrackType::Consumed;
    };

    if subscription_data.tags_set.is_empty()
        || subscription_data
            .tags_set
            .contains(&CheetahString::from_static_str(SubscriptionData::SUB_ALL))
        || subscription_data.tags_set.contains(&message_tag)
    {
        TrackType::Consumed
    } else {
        TrackType::ConsumedButFiltered
    }
}

fn is_message_consumed(msg: &MessageExt, consume_stats: &ConsumeStats, cluster_info: &ClusterInfo) -> bool {
    consume_stats.get_offset_table().iter().any(|(queue, offset_wrapper)| {
        queue.topic() == msg.topic()
            && queue.queue_id() == msg.queue_id()
            && resolve_master_broker_addr(cluster_info, queue)
                .map(|broker_addr| {
                    broker_addr_matches_store_host(broker_addr, msg.store_host())
                        && offset_wrapper.get_consumer_offset() > msg.queue_offset()
                })
                .unwrap_or(false)
    })
}

fn resolve_master_broker_addr<'a>(cluster_info: &'a ClusterInfo, queue: &MessageQueue) -> Option<&'a CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()?
        .get(queue.broker_name())?
        .broker_addrs()
        .get(&mix_all::MASTER_ID)
}

fn broker_addr_matches_store_host(broker_addr: &CheetahString, store_host: std::net::SocketAddr) -> bool {
    broker_addr
        .parse::<std::net::SocketAddr>()
        .map(|parsed| parsed == store_host)
        .unwrap_or_else(|_| broker_addr.as_str() == store_host.to_string())
}

#[allow(deprecated)]
fn apply_track_error(track: &mut MessageTrack, error: &RocketMQError) {
    if let Some(code) = response_code_from_error(error) {
        match code {
            ResponseCode::ConsumerNotOnline => track.set_track_type(TrackType::NotOnline),
            ResponseCode::BroadcastConsumption => track.set_track_type(TrackType::ConsumeBroadcasting),
            _ => {}
        }
    }

    track.set_exception_desc(track_exception_desc(error));
}

fn response_code_from_error(error: &RocketMQError) -> Option<ResponseCode> {
    match error {
        RocketMQError::BrokerOperationFailed { code, .. } => Some(ResponseCode::from(*code)),
        RocketMQError::IllegalArgument(message) => parse_response_code_from_message(message),
        _ => None,
    }
}

fn is_consumer_not_online_error(error: &RocketMQError) -> bool {
    response_code_from_error(error) == Some(ResponseCode::ConsumerNotOnline)
}

fn map_topic_config_lookup_result<T>(
    result: rocketmq_error::RocketMQResult<T>,
) -> rocketmq_error::RocketMQResult<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(error) if response_code_from_error(&error) == Some(ResponseCode::TopicNotExist) => Ok(false),
        Err(error) => Err(error),
    }
}

fn parse_response_code_from_message(message: &str) -> Option<ResponseCode> {
    let code_start = message.find("CODE:")?;
    let digits = message[code_start + "CODE:".len()..]
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '-')
        .collect::<String>();

    if digits.is_empty() {
        return None;
    }

    digits.parse::<i32>().ok().map(ResponseCode::from)
}

fn track_exception_desc(error: &RocketMQError) -> String {
    match error {
        RocketMQError::BrokerOperationFailed { code, message, .. } => format!("CODE:{code} DESC:{message}"),
        _ => error.to_string(),
    }
}

fn admin_result_code_for_error(error: &RocketMQError) -> AdminToolsResultCodeEnum {
    match response_code_from_error(error) {
        Some(ResponseCode::ConsumerNotOnline) => AdminToolsResultCodeEnum::ConsumerNotOnline,
        Some(ResponseCode::BroadcastConsumption) => AdminToolsResultCodeEnum::BroadcastConsumption,
        Some(_) => AdminToolsResultCodeEnum::MQBrokerError,
        None => AdminToolsResultCodeEnum::MQClientError,
    }
}

fn filter_consume_stats(stats: &mut ConsumeStats, topic: Option<&CheetahString>, queue_id: Option<i32>) {
    if topic.is_none() && queue_id.is_none() {
        return;
    }

    stats.offset_table.retain(|queue, _| {
        let topic_matches = topic.is_none_or(|topic| queue.topic() == topic);
        let queue_matches = queue_id.is_none_or(|queue_id| queue.queue_id() == queue_id);
        topic_matches && queue_matches
    });
}

fn retain_java_user_topic_config(
    topic_table: &mut HashMap<CheetahString, TopicConfig>,
    broker_system_topics: &[CheetahString],
    special_topic: bool,
) {
    topic_table.retain(|topic_name, topic_config| {
        let topic = topic_config
            .topic_name
            .as_ref()
            .map(CheetahString::as_str)
            .unwrap_or_else(|| topic_name.as_str());
        if broker_system_topics
            .iter()
            .any(|system_topic| system_topic.as_str() == topic)
            || TopicValidator::is_system_topic(topic)
        {
            return false;
        }
        if !special_topic && (topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX))
        {
            return false;
        }
        PermName::is_valid(topic_config.perm)
    });
}

fn admin_route_not_found(route_topic: &CheetahString) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::route_not_found(route_topic.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::admin::mq_admin_ext_async::MQAdminExt;
    use crate::base::client_config::ClientConfig;
    use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
    use rocketmq_common::common::base::service_state::ServiceState;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::constant::PermName;
    use rocketmq_common::common::message::message_builder::MessageBuilder;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::mix_all;
    use rocketmq_common::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
    use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
    #[allow(deprecated)]
    use rocketmq_common::common::tools::track_type::TrackType;
    use rocketmq_common::common::topic::TopicValidator;
    use rocketmq_error::ErrorKind;
    use rocketmq_error::RocketMQError;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
    use rocketmq_remoting::protocol::body::acl_info::AclInfo;
    use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_remoting::protocol::body::connection::Connection;
    use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
    use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
    use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;

    use super::admin_route_not_found;
    use super::broker_addrs_for_cluster;
    use super::broker_operator_result;
    use super::choose_min_broker_notify_addrs;
    use super::cluster_names_for_admin_operation;
    use super::controller_servers_or_namesrv;
    use super::encode_topic_attributes;
    use super::filter_consume_stats;
    use super::is_consumer_not_online_error;
    use super::is_message_consumed;
    use super::java_long_to_u64;
    use super::lite_pull_topic_config;
    use super::lite_pull_update_consumer_offset_request_header;
    use super::lite_subscription_group_list_from_broker_lite_info;
    use super::lite_topic_list_from_broker_lite_info;
    use super::map_topic_config_lookup_result;
    use super::master_flush_offset_to_java_long;
    use super::merge_consume_status_result;
    use super::merge_order_conf_entries;
    use super::notify_min_broker_id_change_request_header;
    use super::parse_response_code_from_message;
    use super::query_consume_queue_index_to_java_long;
    use super::reset_offset_by_queue_id_request_headers;
    use super::resolve_consumed_track_type;
    use super::retain_java_user_topic_config;
    use super::search_offset_timestamp_to_java_long;
    use super::select_consumer_direct_connection;
    use super::sync_pull_result_missing;
    use super::timeout_millis_to_u64;
    use super::timestamp_to_java_long;
    use super::topic_list_from_lite_topic_names;
    use super::update_consume_offset_request_header;
    use super::update_group_forbidden_request_header;
    use super::validate_acl_file_path_for_global_white_addr_config;
    use super::DefaultMQAdminExtImpl;

    #[test]
    fn admin_route_not_found_uses_route_error_kind() {
        let error = admin_route_not_found(&CheetahString::from_static_str("RouteTopic"));

        assert_eq!(error.kind(), ErrorKind::RouteNotFound);
        assert!(error.to_string().contains("RouteTopic"));
    }

    #[test]
    fn sync_pull_result_missing_uses_client_invalid_state() {
        let error = sync_pull_result_missing("DefaultMQAdminExtImpl::pull_message_from_queue");

        assert_eq!(error.kind(), ErrorKind::ClientInvalidState);
        assert!(error
            .to_string()
            .contains("DefaultMQAdminExtImpl::pull_message_from_queue returned None"));
    }

    fn new_unstarted_admin() -> DefaultMQAdminExtImpl {
        DefaultMQAdminExtImpl::new(
            None,
            Duration::from_secs(3),
            ClientConfig::default(),
            CheetahString::from("admin-group"),
        )
    }

    #[test]
    fn retain_java_user_topic_config_filters_java_internal_topics() {
        let mut topic_table = HashMap::from([
            (
                CheetahString::from_static_str("UserTopic"),
                TopicConfig::new("UserTopic"),
            ),
            (
                CheetahString::from_static_str("BrokerInternalTopic"),
                TopicConfig::new("BrokerInternalTopic"),
            ),
            (
                CheetahString::from_static_str(TopicValidator::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC),
                TopicConfig::new(TopicValidator::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC),
            ),
            (
                CheetahString::from_string(format!("{RETRY_GROUP_TOPIC_PREFIX}group-a")),
                TopicConfig::new(format!("{RETRY_GROUP_TOPIC_PREFIX}group-a")),
            ),
            (
                CheetahString::from_string(format!("{DLQ_GROUP_TOPIC_PREFIX}group-a")),
                TopicConfig::new(format!("{DLQ_GROUP_TOPIC_PREFIX}group-a")),
            ),
            (
                CheetahString::from_static_str("InvalidPermTopic"),
                TopicConfig {
                    perm: PermName::PERM_PRIORITY,
                    ..TopicConfig::new("InvalidPermTopic")
                },
            ),
        ]);
        let broker_system_topics = vec![CheetahString::from_static_str("BrokerInternalTopic")];

        retain_java_user_topic_config(&mut topic_table, &broker_system_topics, false);

        assert_eq!(topic_table.len(), 1);
        assert!(topic_table.contains_key("UserTopic"));
    }

    #[test]
    fn retain_java_user_topic_config_keeps_retry_and_dlq_when_special_topic_enabled() {
        let retry_topic = CheetahString::from_string(format!("{RETRY_GROUP_TOPIC_PREFIX}group-a"));
        let dlq_topic = CheetahString::from_string(format!("{DLQ_GROUP_TOPIC_PREFIX}group-a"));
        let mut topic_table = HashMap::from([
            (retry_topic.clone(), TopicConfig::new(retry_topic.clone())),
            (dlq_topic.clone(), TopicConfig::new(dlq_topic.clone())),
            (
                CheetahString::from_static_str("InvalidPermTopic"),
                TopicConfig {
                    perm: PermName::PERM_PRIORITY,
                    ..TopicConfig::new("InvalidPermTopic")
                },
            ),
        ]);

        retain_java_user_topic_config(&mut topic_table, &[], true);

        assert_eq!(topic_table.len(), 2);
        assert!(topic_table.contains_key(&retry_topic));
        assert!(topic_table.contains_key(&dlq_topic));
    }

    #[test]
    fn use_tls_updates_admin_impl_client_config_before_start() {
        let mut admin = new_unstarted_admin();

        assert!(!admin.is_use_tls());

        admin.set_use_tls(true);

        assert!(admin.is_use_tls());
        assert!(admin.client_config.is_use_tls());
    }

    #[test]
    fn controller_servers_or_namesrv_matches_java_controller_config_target_selection() {
        let namesrv_addrs = vec![
            CheetahString::from("127.0.0.1:9876"),
            CheetahString::from("127.0.0.2:9876"),
        ];

        assert_eq!(
            controller_servers_or_namesrv(Vec::new(), &namesrv_addrs),
            namesrv_addrs.clone()
        );

        let explicit_controllers = vec![CheetahString::from("127.0.0.3:9878")];
        assert_eq!(
            controller_servers_or_namesrv(explicit_controllers.clone(), &namesrv_addrs),
            explicit_controllers
        );

        assert!(controller_servers_or_namesrv(Vec::new(), &[]).is_empty());
    }

    #[test]
    fn merge_order_conf_entries_replaces_existing_broker_value() {
        let merged = merge_order_conf_entries("broker-a:4;broker-b:4", "broker-a:8");
        assert_eq!(merged, "broker-a:8;broker-b:4");
    }

    #[test]
    fn merge_order_conf_entries_adds_new_broker_value() {
        let merged = merge_order_conf_entries("broker-a:4", "broker-b:8");
        assert_eq!(merged, "broker-a:4;broker-b:8");
    }

    #[test]
    fn encode_topic_attributes_matches_java_attribute_parser_format() {
        let mut attributes = HashMap::<CheetahString, CheetahString>::new();
        attributes.insert("+message.type".into(), "NORMAL".into());

        let encoded = encode_topic_attributes(&attributes);

        assert_eq!(encoded, Some(CheetahString::from("+message.type=NORMAL")));
    }

    #[test]
    fn merge_consume_status_result_combines_offsets_by_client() {
        let mut target = HashMap::new();
        let client_id = CheetahString::from_static_str("client-a");
        let mut first_offsets = HashMap::new();
        first_offsets.insert(MessageQueue::from_parts("TopicA", "broker-a", 0), 12);
        merge_consume_status_result(&mut target, HashMap::from([(client_id.clone(), first_offsets)]))
            .expect("first broker status should merge");

        let mut second_offsets = HashMap::new();
        second_offsets.insert(MessageQueue::from_parts("TopicA", "broker-b", 1), 34);
        merge_consume_status_result(&mut target, HashMap::from([(client_id.clone(), second_offsets)]))
            .expect("second broker status should merge");

        let offsets = target.get(&client_id).expect("client offsets should be present");
        assert_eq!(offsets.len(), 2);
        assert_eq!(
            offsets.get(&MessageQueue::from_parts("TopicA", "broker-a", 0)),
            Some(&12)
        );
        assert_eq!(
            offsets.get(&MessageQueue::from_parts("TopicA", "broker-b", 1)),
            Some(&34)
        );
    }

    #[test]
    fn lite_pull_topic_config_marks_lite_and_uses_queue_num_fallback() {
        let config = lite_pull_topic_config(CheetahString::from("LiteTopic"), 8, 3, 0, 0, false)
            .expect("create lite topic should use queueNum fallback");

        assert_eq!(config.topic_name.as_ref().map(CheetahString::as_str), Some("LiteTopic"));
        assert_eq!(config.read_queue_nums, 8);
        assert_eq!(config.write_queue_nums, 8);
        assert_eq!(config.topic_sys_flag, 3);
        assert_eq!(
            config.attributes.get(&CheetahString::from_static_str("message.type")),
            Some(&CheetahString::from_static_str("LITE"))
        );
    }

    #[test]
    fn lite_pull_topic_config_update_requires_explicit_positive_queue_nums() {
        let error = lite_pull_topic_config(CheetahString::from("LiteTopic"), 0, 0, 0, 8, true)
            .expect_err("update lite topic should not use queueNum fallback");

        assert!(error.to_string().contains("readQueueNums must be positive"));
    }

    #[test]
    fn lite_pull_topic_config_rejects_negative_topic_sys_flag() {
        let error = lite_pull_topic_config(CheetahString::from("LiteTopic"), 8, -1, 0, 0, false)
            .expect_err("negative topicSysFlag should be rejected");

        assert!(error.to_string().contains("topicSysFlag must be non-negative"));
    }

    #[test]
    fn timestamp_to_java_long_rejects_values_outside_java_range() {
        assert_eq!(
            timestamp_to_java_long("resetOffsetNewConcurrent", i64::MAX as u64).expect("max Java long is valid"),
            i64::MAX
        );

        let error = timestamp_to_java_long("resetOffsetNewConcurrent", i64::MAX as u64 + 1)
            .expect_err("value larger than Java long should be rejected");

        assert!(error
            .to_string()
            .contains("resetOffsetNewConcurrent timestamp exceeds Java long range"));
    }

    #[test]
    fn timeout_millis_to_u64_rejects_values_outside_rust_range() {
        assert_eq!(
            timeout_millis_to_u64(Duration::from_millis(u64::MAX)).expect("max u64 millis is valid"),
            u64::MAX
        );

        let error = timeout_millis_to_u64(Duration::from_secs(u64::MAX))
            .expect_err("duration larger than u64 milliseconds should be rejected");

        assert!(error
            .to_string()
            .contains("DefaultMQAdminExt timeoutMillis exceeds Rust u64 millisecond range"));
    }

    #[test]
    fn master_flush_offset_to_java_long_rejects_values_outside_java_range() {
        assert_eq!(
            master_flush_offset_to_java_long(i64::MAX as u64).expect("max Java long is valid"),
            i64::MAX
        );

        let error = master_flush_offset_to_java_long(i64::MAX as u64 + 1)
            .expect_err("value larger than Java long should be rejected");

        assert!(error
            .to_string()
            .contains("resetMasterFlushOffset offset exceeds Java long range"));
    }

    #[test]
    fn query_consume_queue_index_to_java_long_rejects_values_outside_java_range() {
        assert_eq!(
            query_consume_queue_index_to_java_long(i64::MAX as u64).expect("max Java long is valid"),
            i64::MAX
        );

        let error = query_consume_queue_index_to_java_long(i64::MAX as u64 + 1)
            .expect_err("value larger than Java long should be rejected");

        assert!(error
            .to_string()
            .contains("queryConsumeQueue offset exceeds Java long range"));
    }

    #[test]
    fn search_offset_timestamp_to_java_long_rejects_values_outside_java_range() {
        assert_eq!(
            search_offset_timestamp_to_java_long(i64::MAX as u64).expect("max Java long is valid"),
            i64::MAX
        );

        let error = search_offset_timestamp_to_java_long(i64::MAX as u64 + 1)
            .expect_err("value larger than Java long should be rejected");

        assert!(error
            .to_string()
            .contains("searchOffset timestamp exceeds Java long range"));
    }

    #[test]
    fn java_long_to_u64_rejects_negative_values_from_broker() {
        assert_eq!(
            java_long_to_u64("searchOffset", "offset", 42).expect("positive Java long should convert"),
            42
        );

        let error = java_long_to_u64("searchOffset", "offset", -1).expect_err("negative broker offset must not wrap");

        assert!(error
            .to_string()
            .contains("searchOffset offset is negative and cannot be represented as Rust u64"));
    }

    #[test]
    fn global_white_addr_config_rejects_legacy_acl_file_path() {
        validate_acl_file_path_for_global_white_addr_config(Some(&CheetahString::from(
            "/opt/rocketmq/conf/plain_acl.yml",
        )))
        .expect_err("modern ACL 2.0 global white address RPC has no aclFileFullPath field");

        validate_acl_file_path_for_global_white_addr_config(None).expect("missing path is valid");
        validate_acl_file_path_for_global_white_addr_config(Some(&CheetahString::new()))
            .expect("empty path follows Java optional parameter behavior");
    }

    #[test]
    #[allow(deprecated)]
    fn broker_operator_result_sets_success_and_failure_lists() {
        let result = broker_operator_result(
            vec![CheetahString::from("broker-a")],
            vec![CheetahString::from("broker-b")],
        );

        assert_eq!(result.get_success_list(), &vec![CheetahString::from("broker-a")]);
        assert_eq!(result.get_failure_list(), &vec![CheetahString::from("broker-b")]);
    }

    #[test]
    fn lite_topic_list_from_broker_info_sorts_topics_and_preserves_addr() {
        let mut lite_info = GetBrokerLiteInfoResponseBody::new();
        lite_info.get_topic_meta_mut().insert(CheetahString::from("topic-b"), 8);
        lite_info.get_topic_meta_mut().insert(CheetahString::from("topic-a"), 4);

        let topic_list =
            lite_topic_list_from_broker_lite_info(Some(CheetahString::from("127.0.0.1:10911")), &lite_info);

        assert_eq!(
            topic_list.topic_list,
            vec![CheetahString::from("topic-a"), CheetahString::from("topic-b")]
        );
        assert_eq!(topic_list.broker_addr, Some(CheetahString::from("127.0.0.1:10911")));
    }

    #[test]
    fn lite_topic_list_from_names_deduplicates_cluster_results() {
        let topic_list = topic_list_from_lite_topic_names(
            None,
            [
                CheetahString::from("topic-b"),
                CheetahString::from("topic-a"),
                CheetahString::from("topic-b"),
            ],
        );

        assert_eq!(
            topic_list.topic_list,
            vec![CheetahString::from("topic-a"), CheetahString::from("topic-b")]
        );
        assert_eq!(topic_list.broker_addr, None);
    }

    #[test]
    fn lite_subscription_group_list_filters_by_topic() {
        let mut lite_info = GetBrokerLiteInfoResponseBody::new();
        let topic = CheetahString::from("topic-a");
        let groups = HashSet::from([CheetahString::from("group-a"), CheetahString::from("group-b")]);
        let mut group_meta = HashMap::new();
        group_meta.insert(topic.clone(), groups.clone());
        lite_info.set_group_meta(group_meta);

        let group_list = lite_subscription_group_list_from_broker_lite_info(&topic, &lite_info);
        let missing_group_list =
            lite_subscription_group_list_from_broker_lite_info(&CheetahString::from("topic-missing"), &lite_info);

        assert_eq!(group_list.group_list, groups);
        assert!(missing_group_list.group_list.is_empty());
    }

    #[tokio::test]
    async fn acl_info_facades_without_started_client_return_typed_errors() {
        let admin = new_unstarted_admin();
        let acl_info = AclInfo {
            subject: Some(CheetahString::from("User:alice")),
            policies: None,
        };

        let error = admin
            .create_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info.clone())
            .await
            .expect_err("create_acl_with_acl_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .update_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info)
            .await
            .expect_err("update_acl_with_acl_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn acl_subject_facades_without_started_client_return_typed_errors() {
        let admin = new_unstarted_admin();

        let error = MQAdminExt::create_acl_with_info(
            &admin,
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("User:alice"),
        )
        .await
        .expect_err("create_acl_with_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = MQAdminExt::update_acl_with_info(
            &admin,
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("User:alice"),
        )
        .await
        .expect_err("update_acl_with_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn examine_broker_cluster_acl_version_info_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = MQAdminExt::examine_broker_cluster_acl_version_info(&admin, CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("examine_broker_cluster_acl_version_info should require a started client");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn acl_info_facades_reject_blank_subject_before_remoting() {
        let admin = new_unstarted_admin();
        let acl_info = AclInfo {
            subject: Some(CheetahString::default()),
            policies: None,
        };

        let error = admin
            .create_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info.clone())
            .await
            .expect_err("create_acl_with_acl_info should reject blank subject locally");
        assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));

        let error = admin
            .update_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info)
            .await
            .expect_err("update_acl_with_acl_info should reject blank subject locally");
        assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));
    }

    #[tokio::test]
    async fn acl_subject_facades_reject_blank_subject_before_remoting() {
        let admin = new_unstarted_admin();

        let error =
            MQAdminExt::create_acl_with_info(&admin, CheetahString::from("127.0.0.1:10911"), CheetahString::default())
                .await
                .expect_err("create_acl_with_info should reject blank subject locally");
        assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));

        let error =
            MQAdminExt::update_acl_with_info(&admin, CheetahString::from("127.0.0.1:10911"), CheetahString::default())
                .await
                .expect_err("update_acl_with_info should reject blank subject locally");
        assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));
    }

    #[test]
    fn producer_connection_empty_set_represents_offline_group() {
        let connection = ProducerConnection::new();
        assert!(connection.connection_set().is_empty());
    }

    #[test]
    fn producer_connection_with_entries_represents_online_group() {
        let mut connection = ProducerConnection::new();
        let mut entry = Connection::new();
        entry.set_client_id("client-a".into());
        connection.connection_set_mut().insert(entry);

        assert_eq!(connection.connection_set().len(), 1);
    }

    #[test]
    fn select_consumer_direct_connection_uses_requested_client_when_present() {
        let consumer_group = CheetahString::from("group-a");
        let requested_client_id = CheetahString::from("client-b");
        let mut consumer_connection = ConsumerConnection::new();
        let mut first = Connection::new();
        first.set_client_id("client-a".into());
        first.set_client_addr("127.0.0.1:1001".into());
        let mut second = Connection::new();
        second.set_client_id(requested_client_id.clone());
        second.set_client_addr("127.0.0.1:1002".into());
        consumer_connection.insert_connection(first);
        consumer_connection.insert_connection(second);

        let (client_id, client_addr) =
            select_consumer_direct_connection(&consumer_group, &consumer_connection, Some(&requested_client_id))
                .expect("requested client should be selected");

        assert_eq!(client_id, requested_client_id);
        assert_eq!(client_addr, CheetahString::from("127.0.0.1:1002"));
    }

    #[test]
    fn select_consumer_direct_connection_returns_first_available_client_when_unspecified() {
        let consumer_group = CheetahString::from("group-a");
        let mut consumer_connection = ConsumerConnection::new();
        let mut only = Connection::new();
        only.set_client_id("client-a".into());
        only.set_client_addr("127.0.0.1:1001".into());
        consumer_connection.insert_connection(only);

        let (client_id, client_addr) =
            select_consumer_direct_connection(&consumer_group, &consumer_connection, Some(&CheetahString::default()))
                .expect("single consumer should be selected");

        assert_eq!(client_id, CheetahString::from("client-a"));
        assert_eq!(client_addr, CheetahString::from("127.0.0.1:1001"));
    }

    #[test]
    fn select_consumer_direct_connection_errors_when_group_is_offline() {
        let consumer_group = CheetahString::from("group-a");
        let consumer_connection = ConsumerConnection::new();

        let error = select_consumer_direct_connection(&consumer_group, &consumer_connection, None)
            .expect_err("offline group should not resolve a client");

        assert!(error.to_string().contains("NO CONSUMER"));
    }

    #[test]
    #[allow(deprecated)]
    fn resolve_consumed_track_type_marks_filtered_subscription() {
        let message = MessageBuilder::new()
            .topic("TopicTest")
            .body_slice(b"payload")
            .tags("TagA")
            .build_unchecked();
        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);

        let mut subscription = SubscriptionData {
            topic: CheetahString::from("TopicTest"),
            ..Default::default()
        };
        subscription.tags_set = BTreeSet::from([CheetahString::from("TagB")]);

        let mut connection = ConsumerConnection::new();
        connection.set_consume_type(ConsumeType::ConsumePassively);
        connection
            .get_subscription_table_mut()
            .insert(CheetahString::from("TopicTest"), subscription);

        let track_type = resolve_consumed_track_type(&message_ext, &connection);

        assert_eq!(track_type, TrackType::ConsumedButFiltered);
    }

    #[test]
    fn is_message_consumed_returns_true_when_offset_has_advanced_on_master() {
        let message = MessageBuilder::new()
            .topic("TopicTest")
            .body_slice(b"payload")
            .build_unchecked();
        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.set_queue_id(1);
        message_ext.set_queue_offset(10);
        message_ext.set_store_host("127.0.0.1:10911".parse().expect("store host"));

        let mut consume_stats = ConsumeStats::new();
        let mut offset_wrapper = OffsetWrapper::default();
        offset_wrapper.set_consumer_offset(11);
        consume_stats
            .get_offset_table_mut()
            .insert(MessageQueue::from_parts("TopicTest", "broker-a", 1), offset_wrapper);

        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(mix_all::MASTER_ID, CheetahString::from("127.0.0.1:10911"));
        let broker_data = BrokerData::new(
            CheetahString::from("cluster-a"),
            CheetahString::from("broker-a"),
            broker_addrs,
            None,
        );
        let cluster_info = ClusterInfo::new(
            Some(HashMap::from([(CheetahString::from("broker-a"), broker_data)])),
            None,
        );

        assert!(is_message_consumed(&message_ext, &consume_stats, &cluster_info));
    }

    #[test]
    fn filter_consume_stats_keeps_only_matching_topic_and_queue() {
        let mut consume_stats = ConsumeStats::new();
        consume_stats.set_consume_tps(12.5);

        for (topic, queue_id, consumer_offset) in [("TopicTest", 0, 10), ("TopicTest", 1, 11), ("OtherTopic", 0, 12)] {
            let mut offset_wrapper = OffsetWrapper::default();
            offset_wrapper.set_consumer_offset(consumer_offset);
            consume_stats
                .get_offset_table_mut()
                .insert(MessageQueue::from_parts(topic, "broker-a", queue_id), offset_wrapper);
        }

        filter_consume_stats(&mut consume_stats, Some(&CheetahString::from("TopicTest")), Some(0));

        assert_eq!(consume_stats.get_consume_tps(), 12.5);
        assert_eq!(consume_stats.get_offset_table().len(), 1);
        let (queue, offset) = consume_stats
            .get_offset_table()
            .iter()
            .next()
            .expect("one queue should remain");
        assert_eq!(queue.topic(), &CheetahString::from("TopicTest"));
        assert_eq!(queue.queue_id(), 0);
        assert_eq!(offset.get_consumer_offset(), 10);
    }

    #[test]
    fn broker_cleanup_cluster_helpers_match_java_cluster_expansion() {
        let broker_a = BrokerData::new(
            CheetahString::from("cluster-a"),
            CheetahString::from("broker-a"),
            HashMap::from([
                (mix_all::MASTER_ID, CheetahString::from("127.0.0.1:10911")),
                (1, CheetahString::from("127.0.0.1:10912")),
            ]),
            None,
        );
        let broker_b = BrokerData::new(
            CheetahString::from("cluster-b"),
            CheetahString::from("broker-b"),
            HashMap::from([(mix_all::MASTER_ID, CheetahString::from("127.0.0.2:10911"))]),
            None,
        );
        let cluster_info = ClusterInfo::new(
            Some(HashMap::from([
                (CheetahString::from("broker-a"), broker_a),
                (CheetahString::from("broker-b"), broker_b),
            ])),
            Some(HashMap::from([
                (
                    CheetahString::from("cluster-a"),
                    HashSet::from([CheetahString::from("broker-a")]),
                ),
                (
                    CheetahString::from("cluster-b"),
                    HashSet::from([CheetahString::from("broker-b")]),
                ),
            ])),
        );

        let mut cluster_names = cluster_names_for_admin_operation(&cluster_info, None);
        cluster_names.sort();
        assert_eq!(
            cluster_names,
            vec![CheetahString::from("cluster-a"), CheetahString::from("cluster-b")]
        );

        let mut addrs = broker_addrs_for_cluster(&cluster_info, &CheetahString::from("cluster-a"));
        addrs.sort();
        assert_eq!(
            addrs,
            vec![
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("127.0.0.1:10912")
            ]
        );
    }

    #[test]
    fn parse_response_code_from_message_reads_consumer_not_online_code() {
        let code = parse_response_code_from_message("CODE: 206 DESC: Not found the consumer group connection");

        assert_eq!(code, Some(ResponseCode::ConsumerNotOnline));
    }

    #[test]
    fn topic_config_lookup_maps_topic_not_exist_to_false_only() {
        let missing = mq_client_err!(ResponseCode::TopicNotExist as i32, "topic not exist");
        assert!(!map_topic_config_lookup_result::<()>(Err(missing)).expect("TopicNotExist should map to false"));

        let client_not_started = RocketMQError::ClientNotStarted;
        let error = map_topic_config_lookup_result::<()>(Err(client_not_started))
            .expect_err("non broker topic-not-exist errors should stay typed errors");
        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }

    #[test]
    fn reset_offset_new_fallback_classifier_matches_java_consumer_not_online_branch() {
        let broker_error = RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            ResponseCode::ConsumerNotOnline as i32,
            "offline",
        )
        .with_broker_addr("127.0.0.1:10911");
        assert!(is_consumer_not_online_error(&broker_error));

        let legacy_client_error = mq_client_err!(
            ResponseCode::ConsumerNotOnline as i32,
            "Not found the consumer group connection"
        );
        assert!(is_consumer_not_online_error(&legacy_client_error));

        let other_error = RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            ResponseCode::SystemError as i32,
            "system error",
        );
        assert!(!is_consumer_not_online_error(&other_error));
    }

    #[test]
    fn update_consume_offset_request_header_matches_java_fields() {
        let mq = MessageQueue::from_parts("TopicTest", "broker-a", 3);

        let header = update_consume_offset_request_header(CheetahString::from("group-a"), &mq, 42)
            .expect("valid offset should build update offset header");

        assert_eq!(header.consumer_group, "group-a");
        assert_eq!(header.topic, "TopicTest");
        assert_eq!(header.queue_id, 3);
        assert_eq!(header.commit_offset, 42);
        assert_eq!(
            header
                .topic_request_header
                .as_ref()
                .and_then(|topic_header| topic_header.rpc.as_ref())
                .and_then(|rpc_header| rpc_header.broker_name.as_ref()),
            Some(&CheetahString::from("broker-a"))
        );
    }

    #[test]
    fn update_consume_offset_request_header_rejects_offsets_outside_java_long_range() {
        let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

        let error = update_consume_offset_request_header(CheetahString::from("group-a"), &mq, i64::MAX as u64 + 1)
            .expect_err("offset larger than Java long should be rejected");

        assert!(error.to_string().contains("offset exceeds Java long range"));
    }

    #[test]
    fn lite_pull_update_consumer_offset_request_header_matches_java_fields() {
        let header = lite_pull_update_consumer_offset_request_header(
            CheetahString::from("TopicTest"),
            CheetahString::from("group-a"),
            2,
            42,
        )
        .expect("valid lite pull offset should build update offset header");

        assert_eq!(header.consumer_group, "group-a");
        assert_eq!(header.topic, "TopicTest");
        assert_eq!(header.queue_id, 2);
        assert_eq!(header.commit_offset, 42);
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn lite_pull_update_consumer_offset_rejects_offsets_outside_java_long_range() {
        let error = lite_pull_update_consumer_offset_request_header(
            CheetahString::from("TopicTest"),
            CheetahString::from("group-a"),
            0,
            i64::MAX as u64 + 1,
        )
        .expect_err("offset larger than Java long should be rejected");

        assert!(error.to_string().contains("offset exceeds Java long range"));
    }

    #[test]
    fn notify_min_broker_id_change_request_header_matches_java_fields() {
        let header = notify_min_broker_id_change_request_header(
            1,
            CheetahString::from("127.0.0.1:10912"),
            Some(CheetahString::from("127.0.0.1:10911")),
            Some(CheetahString::from("127.0.0.1:10913")),
        )
        .expect("valid notify-min-broker header should build");

        assert_eq!(header.min_broker_id, Some(1));
        assert!(header.broker_name.is_none());
        assert_eq!(header.min_broker_addr.as_deref(), Some("127.0.0.1:10912"));
        assert_eq!(header.offline_broker_addr.as_deref(), Some("127.0.0.1:10911"));
        assert_eq!(header.ha_broker_addr.as_deref(), Some("127.0.0.1:10913"));
    }

    #[test]
    fn notify_min_broker_id_change_request_header_rejects_blank_min_broker_addr() {
        let error = notify_min_broker_id_change_request_header(1, CheetahString::new(), None, None)
            .expect_err("blank min broker address should be rejected before remoting");

        assert!(error.to_string().contains("requires minBrokerAddr"));
    }

    #[test]
    fn choose_min_broker_notify_addrs_matches_java_new_broker_rule() {
        let broker_addrs = HashMap::from([
            (0, CheetahString::from("127.0.0.1:10911")),
            (1, CheetahString::from("127.0.0.1:10912")),
        ]);

        let notify_addrs = choose_min_broker_notify_addrs(&broker_addrs, 0, None);

        assert_eq!(notify_addrs, vec![CheetahString::from("127.0.0.1:10912")]);
    }

    #[test]
    fn choose_min_broker_notify_addrs_matches_java_offline_and_single_broker_rules() {
        let broker_addrs = HashMap::from([
            (0, CheetahString::from("127.0.0.1:10911")),
            (1, CheetahString::from("127.0.0.1:10912")),
        ]);

        let notify_addrs =
            choose_min_broker_notify_addrs(&broker_addrs, 1, Some(&CheetahString::from("127.0.0.1:10911")));
        assert_eq!(
            notify_addrs,
            vec![
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("127.0.0.1:10912")
            ]
        );

        let single_broker = HashMap::from([(2, CheetahString::from("127.0.0.1:10913"))]);
        let notify_addrs = choose_min_broker_notify_addrs(&single_broker, 2, None);
        assert_eq!(notify_addrs, vec![CheetahString::from("127.0.0.1:10913")]);
    }

    #[tokio::test]
    async fn update_consume_offset_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();
        let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

        let error = admin
            .update_consume_offset(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("group-a"),
                mq,
                1,
            )
            .await
            .expect_err("unstarted admin should not try to send update offset");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn update_lite_pull_consumer_offset_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .update_lite_pull_consumer_offset(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("TopicTest"),
                CheetahString::from("group-a"),
                0,
                1,
            )
            .await
            .expect_err("unstarted admin should not try to send lite pull update offset");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn sync_broker_member_group_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .sync_broker_member_group(
                CheetahString::new(),
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
            )
            .await
            .expect_err("sync_broker_member_group should require a started client");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn sync_broker_member_group_rejects_controller_addr() {
        let admin = new_unstarted_admin();

        let error = admin
            .sync_broker_member_group(
                CheetahString::from("127.0.0.1:9878"),
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
            )
            .await
            .expect_err("sync_broker_member_group should not silently ignore controller_addr");

        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::IllegalArgument(message)
                if message.contains("controllerAddr is not supported")
        ));
    }

    #[tokio::test]
    async fn notify_min_broker_id_changed_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .notify_min_broker_id_changed(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                1,
                CheetahString::from("127.0.0.1:10912"),
                None,
                None,
            )
            .await
            .expect_err("notify_min_broker_id_changed should require a started client");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn export_rocksdb_consumer_offset_to_json_rejects_local_file_path() {
        let admin = new_unstarted_admin();

        let error = admin
            .export_rocksdb_consumer_offset_to_json(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("D:/tmp/consumerOffsets.json"),
            )
            .await
            .expect_err("RPC export cannot accept local export file path");

        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::IllegalArgument(message)
                if message.contains("filePath is local-mode only")
        ));
    }

    #[tokio::test]
    async fn export_rocksdb_consumer_offset_to_json_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .export_rocksdb_consumer_offset_to_json(CheetahString::from("127.0.0.1:10911"), CheetahString::new())
            .await
            .expect_err("RPC export should require a started client");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn export_rocksdb_consumer_offset_from_memory_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .export_rocksdb_consumer_offset_from_memory(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("memory export should require a started client");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[test]
    fn reset_offset_by_queue_id_request_headers_match_java_fields() {
        let (update_header, reset_header) = reset_offset_by_queue_id_request_headers(
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            2,
            100,
        )
        .expect("valid reset offset should build request headers");

        assert_eq!(update_header.consumer_group, "group-a");
        assert_eq!(update_header.topic, "TopicTest");
        assert_eq!(update_header.queue_id, 2);
        assert_eq!(update_header.commit_offset, 100);
        assert!(update_header.topic_request_header.is_none());

        assert_eq!(reset_header.group, "group-a");
        assert_eq!(reset_header.topic, "TopicTest");
        assert_eq!(reset_header.queue_id, 2);
        assert_eq!(reset_header.offset, Some(100));
        assert_eq!(reset_header.timestamp, 0);
        assert!(!reset_header.is_force);
        assert!(reset_header.topic_request_header.is_none());
    }

    #[test]
    fn reset_offset_by_queue_id_rejects_offsets_outside_java_long_range() {
        let error = reset_offset_by_queue_id_request_headers(
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            0,
            i64::MAX as u64 + 1,
        )
        .expect_err("offset larger than Java long should be rejected");

        assert!(error
            .to_string()
            .contains("resetOffsetByQueueId offset exceeds Java long range"));
    }

    #[tokio::test]
    async fn reset_offset_by_queue_id_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .reset_offset_by_queue_id(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("group-a"),
                CheetahString::from("TopicTest"),
                0,
                100,
            )
            .await
            .expect_err("unstarted admin should not try to reset offset");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[test]
    fn update_group_forbidden_request_header_matches_java_fields() {
        let header = update_group_forbidden_request_header(
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            Some(false),
        );

        assert_eq!(header.group, "group-a");
        assert_eq!(header.topic, "TopicTest");
        assert_eq!(header.readable, Some(false));
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn update_group_forbidden_request_header_preserves_unspecified_readable() {
        let header = update_group_forbidden_request_header(
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            None,
        );

        assert_eq!(header.readable, None);
    }

    #[tokio::test]
    async fn update_and_get_group_read_forbidden_without_started_client_returns_typed_error() {
        let admin = new_unstarted_admin();

        let error = admin
            .update_and_get_group_read_forbidden(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("group-a"),
                CheetahString::from("TopicTest"),
                Some(false),
            )
            .await
            .expect_err("unstarted admin should not try to update group forbidden state");

        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn directly_owned_impl_starts_and_stops_without_self_reference() {
        let mut admin = new_unstarted_admin();

        admin
            .start()
            .await
            .expect("directly owned admin implementation should start without self wiring");
        assert_eq!(admin.service_state, ServiceState::Running);

        admin.shutdown().await;

        assert_eq!(admin.service_state, ServiceState::ShutdownAlready);
    }

    #[tokio::test]
    async fn admin_api_facades_without_started_client_return_typed_errors() {
        let admin = new_unstarted_admin();
        let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

        let result = admin
            .pull_message_from_queue("127.0.0.1:10911", &mq, "*", 0, 32, 3000)
            .await;
        assert!(matches!(result, Err(rocketmq_error::RocketMQError::ClientNotStarted)));

        let error = admin
            .delete_topic_in_broker(
                HashSet::from([CheetahString::from("127.0.0.1:10911")]),
                CheetahString::from("TopicTest"),
            )
            .await
            .expect_err("delete_topic_in_broker should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .set_message_request_mode(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("TopicTest"),
                CheetahString::from("group-a"),
                MessageRequestMode::Pull,
                0,
                3000,
            )
            .await
            .expect_err("set_message_request_mode should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_parent_topic_info(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
            .await
            .expect_err("get_parent_topic_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .create_lite_pull_topic(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("LiteTopic"),
                4,
                0,
                0,
                0,
            )
            .await
            .expect_err("create_lite_pull_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .update_lite_pull_topic(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("LiteTopic"),
                4,
                4,
            )
            .await
            .expect_err("update_lite_pull_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_lite_pull_topic(CheetahString::from("127.0.0.1:10911"), CheetahString::from("LiteTopic"))
            .await
            .expect_err("get_lite_pull_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .delete_lite_pull_topic(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("cluster-a"),
                CheetahString::from("LiteTopic"),
            )
            .await
            .expect_err("delete_lite_pull_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .query_lite_pull_topic_list(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("query_lite_pull_topic_list should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .query_lite_pull_topic_by_cluster(CheetahString::from("cluster-a"))
            .await
            .expect_err("query_lite_pull_topic_by_cluster should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .query_lite_pull_subscription_list(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
            .await
            .expect_err("query_lite_pull_subscription_list should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .examine_topic_config(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
            .await
            .expect_err("examine_topic_config should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_topic_config_by_topic_name(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
            .await
            .expect_err("get_topic_config_by_topic_name should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_topic_stats_info(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
            .await
            .expect_err("get_topic_stats_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .query_broker_has_topic(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
            .await
            .expect_err("query_broker_has_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .fetch_topics_by_cluster(CheetahString::from("cluster-a"))
            .await
            .expect_err("fetch_topics_by_cluster should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .create_and_update_plain_access_config(
                CheetahString::from("127.0.0.1:10911"),
                PlainAccessConfig {
                    access_key: Some(CheetahString::from("AK")),
                    secret_key: Some(CheetahString::from("SK")),
                    admin: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("create_and_update_plain_access_config should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .delete_plain_access_config(CheetahString::from("127.0.0.1:10911"), CheetahString::from("AK"))
            .await
            .expect_err("delete_plain_access_config should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_system_topic_list_from_broker(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("get_system_topic_list_from_broker should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_kv_list_by_namespace(CheetahString::from("namespace-a"))
            .await
            .expect_err("get_kv_list_by_namespace should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .examine_topic_route_info_with_timeout(CheetahString::from("TopicTest"), 3000)
            .await
            .expect_err("examine_topic_route_info_with_timeout should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .query_consume_time_span(CheetahString::from("TopicTest"), CheetahString::from("group-a"))
            .await
            .expect_err("query_consume_time_span should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .reset_offset_new(
                CheetahString::from("group-a"),
                CheetahString::from("TopicTest"),
                1_700_000_000_000,
            )
            .await
            .expect_err("reset_offset_new should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let result = admin
            .reset_offset_new_concurrent(
                CheetahString::from("group-a"),
                CheetahString::from("TopicTest"),
                1_700_000_000_000,
            )
            .await;
        assert!(!result.is_success());
        assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

        let error = admin
            .query_topics_by_consumer(CheetahString::from("group-a"))
            .await
            .expect_err("query_topics_by_consumer should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let result = admin
            .query_topics_by_consumer_concurrent(CheetahString::from("group-a"))
            .await;
        assert!(!result.is_success());
        assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

        let error = admin
            .examine_consume_stats_with_queue(
                CheetahString::from("group-a"),
                Some(CheetahString::from("TopicTest")),
                Some(0),
            )
            .await
            .expect_err("examine_consume_stats_with_queue should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let result = admin
            .examine_consume_stats_concurrent(CheetahString::from("group-a"), Some(CheetahString::from("TopicTest")))
            .await;
        assert!(!result.is_success());
        assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

        let result = admin
            .examine_consume_stats_concurrent_with_cluster(
                CheetahString::from("group-a"),
                Some(CheetahString::from("TopicTest")),
                Some(CheetahString::from("cluster-a")),
            )
            .await;
        assert!(!result.is_success());
        assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

        let error = admin
            .query_subscription(CheetahString::from("group-a"), CheetahString::from("TopicTest"))
            .await
            .expect_err("query_subscription should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .clone_group_offset(
                CheetahString::from("source-group"),
                CheetahString::from("target-group"),
                CheetahString::from("TopicTest"),
                false,
            )
            .await
            .expect_err("clone_group_offset should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_cluster_list(String::from("TopicTest"))
            .await
            .expect_err("get_cluster_list should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let result = admin
            .fetch_consume_stats_in_broker(CheetahString::from("127.0.0.1:10911"), false, 3000)
            .await;
        assert!(matches!(result, Err(rocketmq_error::RocketMQError::ClientNotStarted)));

        let error = admin
            .update_global_white_addr_config(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("10.10.*.*"),
                None,
            )
            .await
            .expect_err("update_global_white_addr_config should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .update_global_white_addr_config(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("10.10.*.*"),
                Some(CheetahString::from("/opt/rocketmq/conf/plain_acl.yml")),
            )
            .await
            .expect_err("update_global_white_addr_config should require a started client before validating RPC fields");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .create_static_topic(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("TBW102"),
                TopicConfig::new("TopicTest"),
                TopicQueueMappingDetail::default(),
                true,
            )
            .await
            .expect_err("create_static_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .resume_check_half_message(
                CheetahString::from("TopicTest"),
                CheetahString::from("AC11000100002A9F0000000000000001"),
            )
            .await
            .expect_err("resume_check_half_message should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .switch_timer_engine(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from(MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE),
            )
            .await
            .expect_err("switch_timer_engine should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .remove_cold_data_flow_ctr_group_config(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("group-a"),
            )
            .await
            .expect_err("remove_cold_data_flow_ctr_group_config should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .get_cold_data_flow_ctr_info(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("get_cold_data_flow_ctr_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .set_commit_log_read_ahead_mode(CheetahString::from("127.0.0.1:10911"), CheetahString::from("1"))
            .await
            .expect_err("set_commit_log_read_ahead_mode should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .create_user_with_info(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("alice"),
                CheetahString::from("secret"),
            )
            .await
            .expect_err("create_user_with_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .update_user_with_info(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("alice"),
                CheetahString::from("new-secret"),
            )
            .await
            .expect_err("update_user_with_info should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .export_pop_records(CheetahString::from("127.0.0.1:10911"), 3000)
            .await
            .expect_err("export_pop_records should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .clean_expired_consumer_queue(None, Some(CheetahString::from("127.0.0.1:10911")))
            .await
            .expect_err("clean_expired_consumer_queue should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .clean_expired_consumer_queue_by_addr(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("clean_expired_consumer_queue_by_addr should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .delete_expired_commit_log(None, Some(CheetahString::from("127.0.0.1:10911")))
            .await
            .expect_err("delete_expired_commit_log should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .delete_expired_commit_log_by_addr(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("delete_expired_commit_log_by_addr should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .clean_unused_topic(None, Some(CheetahString::from("127.0.0.1:10911")))
            .await
            .expect_err("clean_unused_topic should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let error = admin
            .clean_unused_topic_by_addr(CheetahString::from("127.0.0.1:10911"))
            .await
            .expect_err("clean_unused_topic_by_addr should require a started client");
        assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

        let result = admin
            .delete_topic_in_broker_concurrent(
                HashSet::from([CheetahString::from("127.0.0.1:10911")]),
                CheetahString::from("TopicTest"),
            )
            .await;
        assert!(!result.is_success());
        assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

        let namesrv_list = admin.get_name_server_address_list().await;
        assert!(namesrv_list.is_empty());
    }
}
