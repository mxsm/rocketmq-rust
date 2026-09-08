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

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
#[allow(deprecated)]
use rocketmq_model::common::tools::broker_operator_result::BrokerOperatorResult;
#[allow(deprecated)]
use rocketmq_model::common::tools::message_track::MessageTrack;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
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
use rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_protocol::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use crate::common::admin_tool_result::AdminToolResult;

#[allow(dead_code, reason = "command families consume selected capability methods")]
#[allow(async_fn_in_trait)]
pub trait RouteAdmin: Send {
    async fn examine_broker_cluster_info(&self) -> crate::ClientResult<ClusterInfo>;

    async fn get_name_server_address_list(&self) -> Vec<CheetahString>;

    async fn put_kv_config(&self, namespace: CheetahString, key: CheetahString, value: CheetahString);

    async fn get_kv_config(&self, namespace: CheetahString, key: CheetahString) -> crate::ClientResult<CheetahString>;

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> crate::ClientResult<KVTable>;

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn delete_kv_config(&self, namespace: CheetahString, key: CheetahString) -> crate::ClientResult<()>;

    async fn get_cluster_list(&self, topic: String) -> crate::ClientResult<HashSet<CheetahString>>;

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        name_servers: Option<Vec<CheetahString>>,
    ) -> crate::ClientResult<()>;

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>;

    async fn probe_name_server(&self, name_server: CheetahString) -> crate::ClientResult<()>;
}

#[allow(dead_code, reason = "command families consume selected capability methods")]
#[allow(async_fn_in_trait)]
pub trait TopicAdmin: Send {
    async fn create_and_update_topic_config(&self, addr: CheetahString, config: TopicConfig)
        -> crate::ClientResult<()>;

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> crate::ClientResult<()>;

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> crate::ClientResult<TopicStatsTable>;

    async fn examine_topic_stats_concurrent(&self, topic: CheetahString) -> AdminToolResult<TopicStatsTable>;

    async fn fetch_all_topic_list(&self) -> crate::ClientResult<TopicList>;

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> crate::ClientResult<TopicList>;

    async fn examine_topic_route_info(&self, topic: CheetahString) -> crate::ClientResult<Option<TopicRouteData>>;

    async fn delete_topic(&self, topic_name: CheetahString, cluster_name: CheetahString) -> crate::ClientResult<()>;

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn delete_topic_in_broker_list(
        &self,
        addrs: HashSet<CheetahString>,
        topics: Vec<CheetahString>,
    ) -> crate::ClientResult<()>;
    #[allow(deprecated)]
    async fn delete_topic_in_broker_concurrent(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> AdminToolResult<BrokerOperatorResult>;

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> crate::ClientResult<()>;

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> crate::ClientResult<GroupList>;

    async fn query_topics_by_consumer(&self, group: CheetahString) -> crate::ClientResult<TopicList>;

    async fn query_topics_by_consumer_concurrent(&self, group: CheetahString) -> AdminToolResult<TopicList>;

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::ClientResult<bool>;

    async fn clean_unused_topic_by_addr(&self, addr: CheetahString) -> crate::ClientResult<bool>;

    async fn get_topic_cluster_list(&self, topic: String) -> crate::ClientResult<HashSet<CheetahString>>;

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<TopicConfigSerializeWrapper>;

    async fn set_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> crate::ClientResult<()>;

    async fn examine_topic_config(&self, addr: CheetahString, topic: CheetahString)
        -> crate::ClientResult<TopicConfig>;

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> crate::ClientResult<()>;

    async fn create_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        queue_num: i32,
        topic_sys_flag: i32,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> crate::ClientResult<()>;

    async fn update_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> crate::ClientResult<()>;

    async fn get_lite_pull_topic(&self, addr: CheetahString, topic: CheetahString) -> crate::ClientResult<TopicConfig>;

    async fn delete_lite_pull_topic(
        &self,
        addr: CheetahString,
        cluster_name: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn query_lite_pull_topic_list(&self, addr: CheetahString) -> crate::ClientResult<TopicList>;

    async fn query_lite_pull_topic_by_cluster(&self, cluster_name: CheetahString) -> crate::ClientResult<TopicList>;

    async fn get_topic_config_by_topic_name(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
    ) -> crate::ClientResult<TopicConfig>;

    async fn get_topic_stats_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<TopicStatsTable>;

    async fn query_broker_has_topic(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<bool>;

    async fn get_system_topic_list_from_broker(&self, broker_addr: CheetahString) -> crate::ClientResult<TopicList>;

    async fn examine_topic_route_info_with_timeout(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<Option<TopicRouteData>>;

    async fn get_parent_topic_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<GetParentTopicInfoResponseBody>;

    async fn get_lite_topic_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        lite_topic: CheetahString,
    ) -> crate::ClientResult<GetLiteTopicInfoResponseBody>;
}

#[allow(dead_code, reason = "command families consume selected capability methods")]
#[allow(async_fn_in_trait)]
pub trait ConsumerAdmin: Send {
    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> crate::ClientResult<()>;

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> crate::ClientResult<()>;

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> crate::ClientResult<SubscriptionGroupConfig>;

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> crate::ClientResult<ConsumeStats>;

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> crate::ClientResult<ConsumerConnection>;

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<ProducerConnection>;

    async fn get_all_producer_info(&self, broker_addr: CheetahString) -> crate::ClientResult<ProducerTableInfo>;

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> crate::ClientResult<()>;

    async fn delete_subscription_group_list(
        &self,
        addr: CheetahString,
        group_names: Vec<CheetahString>,
        clean_offset: bool,
    ) -> crate::ClientResult<()>;

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>>;

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<SubscriptionData>;

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::ClientResult<bool>;

    async fn clean_expired_consumer_queue_by_addr(&self, addr: CheetahString) -> crate::ClientResult<bool>;

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        metrics: Option<bool>,
    ) -> crate::ClientResult<ConsumerRunningInfo>;

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<ConsumeMessageDirectlyResult>;

    async fn consume_message_directly_ext(
        &self,
        cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<ConsumeMessageDirectlyResult>;
    #[allow(deprecated)]
    async fn message_track_detail(&self, msg: MessageExt) -> crate::ClientResult<Vec<MessageTrack>>;
    #[allow(deprecated)]
    async fn message_track_detail_concurrent(&self, msg: MessageExt) -> AdminToolResult<Vec<MessageTrack>>;

    async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: CheetahString,
        is_order: bool,
        timeout_millis: u64,
    ) -> crate::ClientResult<ConsumeStatsList>;

    async fn get_all_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<SubscriptionGroupWrapper>;

    async fn query_consume_queue(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: u64,
        count: i32,
        consumer_group: CheetahString,
    ) -> crate::ClientResult<QueryConsumeQueueResponseBody>;

    async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        topic_name: CheetahString,
        readable: Option<bool>,
    ) -> crate::ClientResult<GroupForbidden>;

    async fn query_message(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<MessageExt>;

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> crate::ClientResult<()>;

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn query_lite_pull_subscription_list(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<GroupList>;

    async fn examine_consume_stats_with_queue(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        queue_id: Option<i32>,
    ) -> crate::ClientResult<ConsumeStats>;

    async fn examine_consume_stats_concurrent(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats>;

    async fn examine_consume_stats_concurrent_with_cluster(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats>;

    async fn sync_broker_member_group(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn get_lite_group_info(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
    ) -> crate::ClientResult<GetLiteGroupInfoResponseBody>;
}

#[allow(dead_code, reason = "command families consume selected capability methods")]
#[allow(async_fn_in_trait)]
pub trait BrokerAdmin: Send {
    async fn start(&mut self) -> crate::ClientResult<()>;
    async fn shutdown(&mut self);
    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> crate::ClientResult<()>;

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> crate::ClientResult<()>;

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<HashMap<CheetahString, CheetahString>>;

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> crate::ClientResult<KVTable>;

    async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        check_store_time: i64,
    ) -> crate::ClientResult<CheckRocksdbCqWriteResult>;

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> crate::ClientResult<i32>;

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> crate::ClientResult<i32>;

    async fn view_broker_stats_data(
        &self,
        broker_addr: CheetahString,
        stats_name: CheetahString,
        stats_key: CheetahString,
    ) -> crate::ClientResult<BrokerStatsData>;

    async fn resume_check_half_message(&self, topic: CheetahString, msg_id: CheetahString)
        -> crate::ClientResult<bool>;

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> crate::ClientResult<HARuntimeInfo>;

    async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
    ) -> crate::ClientResult<BrokerReplicasInfo>;

    async fn get_broker_epoch_cache(&self, broker_addr: CheetahString) -> crate::ClientResult<EpochEntryCache>;

    async fn get_controller_meta_data(
        &self,
        controller_addr: CheetahString,
    ) -> crate::ClientResult<GetMetaDataResponseHeader>;

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>;

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> crate::ClientResult<()>;

    async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
    ) -> crate::ClientResult<(ElectMasterResponseHeader, BrokerMemberGroup)>;

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> crate::ClientResult<()>;

    async fn get_cold_data_flow_ctr_info(&self, broker_addr: CheetahString) -> crate::ClientResult<CheetahString>;

    async fn notify_min_broker_id_changed(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        min_broker_id: u64,
        min_broker_addr: CheetahString,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) -> crate::ClientResult<()>;

    async fn export_pop_records(&self, broker_addr: CheetahString, timeout: u64) -> crate::ClientResult<()>;

    async fn switch_timer_engine(
        &self,
        broker_addr: CheetahString,
        des_timer_engine: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn get_broker_lite_info(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<GetBrokerLiteInfoResponseBody>;

    async fn get_lite_client_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> crate::ClientResult<GetLiteClientInfoResponseBody>;

    async fn trigger_lite_dispatch(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
    ) -> crate::ClientResult<()>;
}

#[allow(dead_code, reason = "command families consume selected capability methods")]
#[allow(async_fn_in_trait)]
pub trait AuthAdmin: Send {
    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> crate::ClientResult<()>;

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> crate::ClientResult<()>;

    async fn examine_broker_cluster_acl_version_info(&self, addr: CheetahString) -> crate::ClientResult<CheetahString>;

    async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<SubscriptionGroupWrapper>;

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> crate::ClientResult<TopicConfigSerializeWrapper>;

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn create_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn update_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn delete_user(&self, broker_addr: CheetahString, username: CheetahString) -> crate::ClientResult<()>;

    async fn get_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> crate::ClientResult<Option<UserInfo>>;

    async fn list_users(&self, broker_addr: CheetahString, filter: CheetahString)
        -> crate::ClientResult<Vec<UserInfo>>;

    async fn list_user(&self, broker_addr: CheetahString, filter: CheetahString) -> crate::ClientResult<Vec<UserInfo>> {
        self.list_users(broker_addr, filter).await
    }

    async fn create_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn create_acl_with_info(&self, broker_addr: CheetahString, subject: CheetahString)
        -> crate::ClientResult<()>;

    async fn update_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn update_acl_with_info(&self, broker_addr: CheetahString, subject: CheetahString)
        -> crate::ClientResult<()>;

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn get_acl(&self, broker_addr: CheetahString, subject: CheetahString) -> crate::ClientResult<AclInfo>;

    async fn list_acl(
        &self,
        broker_addr: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
    ) -> crate::ClientResult<Vec<AclInfo>>;
}

#[allow(dead_code, reason = "command families consume selected capability methods")]
#[allow(async_fn_in_trait)]
pub trait OffsetAdmin: Send {
    async fn reset_offset_by_timestamp_old(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> crate::ClientResult<Vec<RollbackStats>>;

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> crate::ClientResult<HashMap<MessageQueue, u64>>;

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> crate::ClientResult<()>;
    #[allow(deprecated)]
    async fn reset_offset_new_concurrent(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult>;

    async fn query_consume_time_span(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> crate::ClientResult<Vec<QueueTimeSpan>>;

    async fn query_consume_time_span_concurrent(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>>;

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::ClientResult<bool>;

    async fn delete_expired_commit_log_by_addr(&self, addr: CheetahString) -> crate::ClientResult<bool>;

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> crate::ClientResult<()>;

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> crate::ClientResult<()>;

    async fn reset_offset_by_queue_id(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        reset_offset: u64,
    ) -> crate::ClientResult<()>;

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> crate::ClientResult<()>;

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> crate::ClientResult<CheetahString>;

    async fn update_lite_pull_consumer_offset(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        group: CheetahString,
        queue_id: i32,
        offset: u64,
    ) -> crate::ClientResult<()>;

    async fn export_rocksdb_consumer_offset_to_json(
        &self,
        broker_addr: CheetahString,
        file_path: CheetahString,
    ) -> crate::ClientResult<()>;

    async fn export_rocksdb_consumer_offset_from_memory(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<CheetahString>;

    async fn search_offset(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        timestamp: u64,
        timeout_millis: u64,
    ) -> crate::ClientResult<u64>;

    async fn min_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> crate::ClientResult<i64>;

    async fn max_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> crate::ClientResult<i64>;
}

#[allow(deprecated)]
impl RouteAdmin for crate::admin::default_mq_admin_ext::DefaultMQAdminExt {
    async fn examine_broker_cluster_info(&self) -> crate::ClientResult<ClusterInfo> {
        RouteAdmin::examine_broker_cluster_info(self.inner()).await
    }

    async fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        RouteAdmin::get_name_server_address_list(self.inner()).await
    }

    async fn put_kv_config(&self, namespace: CheetahString, key: CheetahString, value: CheetahString) {
        RouteAdmin::put_kv_config(self.inner(), namespace, key, value).await
    }

    async fn get_kv_config(&self, namespace: CheetahString, key: CheetahString) -> crate::ClientResult<CheetahString> {
        RouteAdmin::get_kv_config(self.inner(), namespace, key).await
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> crate::ClientResult<KVTable> {
        RouteAdmin::get_kv_list_by_namespace(self.inner(), namespace).await
    }

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> crate::ClientResult<()> {
        RouteAdmin::create_and_update_kv_config(self.inner(), namespace, key, value).await
    }

    async fn delete_kv_config(&self, namespace: CheetahString, key: CheetahString) -> crate::ClientResult<()> {
        RouteAdmin::delete_kv_config(self.inner(), namespace, key).await
    }

    async fn get_cluster_list(&self, topic: String) -> crate::ClientResult<HashSet<CheetahString>> {
        RouteAdmin::get_cluster_list(self.inner(), topic).await
    }

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        name_servers: Option<Vec<CheetahString>>,
    ) -> crate::ClientResult<()> {
        RouteAdmin::update_name_server_config(self.inner(), properties, name_servers).await
    }

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        RouteAdmin::get_name_server_config(self.inner(), name_servers).await
    }

    async fn probe_name_server(&self, name_server: CheetahString) -> crate::ClientResult<()> {
        RouteAdmin::probe_name_server(self.inner(), name_server).await
    }
}

impl TopicAdmin for crate::admin::default_mq_admin_ext::DefaultMQAdminExt {
    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> crate::ClientResult<()> {
        TopicAdmin::create_and_update_topic_config(self.inner(), addr, config).await
    }

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> crate::ClientResult<()> {
        TopicAdmin::create_and_update_topic_config_list(self.inner(), addr, topic_config_list).await
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> crate::ClientResult<TopicStatsTable> {
        TopicAdmin::examine_topic_stats(self.inner(), topic, broker_addr).await
    }

    async fn examine_topic_stats_concurrent(&self, topic: CheetahString) -> AdminToolResult<TopicStatsTable> {
        TopicAdmin::examine_topic_stats_concurrent(self.inner(), topic).await
    }

    async fn fetch_all_topic_list(&self) -> crate::ClientResult<TopicList> {
        TopicAdmin::fetch_all_topic_list(self.inner()).await
    }

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> crate::ClientResult<TopicList> {
        TopicAdmin::fetch_topics_by_cluster(self.inner(), cluster_name).await
    }

    async fn examine_topic_route_info(&self, topic: CheetahString) -> crate::ClientResult<Option<TopicRouteData>> {
        TopicAdmin::examine_topic_route_info(self.inner(), topic).await
    }

    async fn delete_topic(&self, topic_name: CheetahString, cluster_name: CheetahString) -> crate::ClientResult<()> {
        TopicAdmin::delete_topic(self.inner(), topic_name, cluster_name).await
    }

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> crate::ClientResult<()> {
        TopicAdmin::delete_topic_in_broker(self.inner(), addrs, topic).await
    }

    async fn delete_topic_in_broker_list(
        &self,
        addrs: HashSet<CheetahString>,
        topics: Vec<CheetahString>,
    ) -> crate::ClientResult<()> {
        TopicAdmin::delete_topic_in_broker_list(self.inner(), addrs, topics).await
    }

    #[allow(deprecated)]
    async fn delete_topic_in_broker_concurrent(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> AdminToolResult<BrokerOperatorResult> {
        TopicAdmin::delete_topic_in_broker_concurrent(self.inner(), addrs, topic).await
    }

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> crate::ClientResult<()> {
        TopicAdmin::delete_topic_in_name_server(self.inner(), addrs, cluster_name, topic).await
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> crate::ClientResult<()> {
        TopicAdmin::create_or_update_order_conf(self.inner(), key, value, is_cluster).await
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> crate::ClientResult<GroupList> {
        TopicAdmin::query_topic_consume_by_who(self.inner(), topic).await
    }

    async fn query_topics_by_consumer(&self, group: CheetahString) -> crate::ClientResult<TopicList> {
        TopicAdmin::query_topics_by_consumer(self.inner(), group).await
    }

    async fn query_topics_by_consumer_concurrent(&self, group: CheetahString) -> AdminToolResult<TopicList> {
        TopicAdmin::query_topics_by_consumer_concurrent(self.inner(), group).await
    }

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::ClientResult<bool> {
        TopicAdmin::clean_unused_topic(self.inner(), cluster, addr).await
    }

    async fn clean_unused_topic_by_addr(&self, addr: CheetahString) -> crate::ClientResult<bool> {
        TopicAdmin::clean_unused_topic_by_addr(self.inner(), addr).await
    }

    async fn get_topic_cluster_list(&self, topic: String) -> crate::ClientResult<HashSet<CheetahString>> {
        TopicAdmin::get_topic_cluster_list(self.inner(), topic).await
    }

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<TopicConfigSerializeWrapper> {
        TopicAdmin::get_all_topic_config(self.inner(), broker_addr, timeout_millis).await
    }

    async fn set_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> crate::ClientResult<()> {
        TopicAdmin::set_message_request_mode(
            self.inner(),
            broker_addr,
            topic,
            consumer_group,
            mode,
            pop_work_group_size,
            timeout_millis,
        )
        .await
    }

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<TopicConfig> {
        TopicAdmin::examine_topic_config(self.inner(), addr, topic).await
    }

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> crate::ClientResult<()> {
        TopicAdmin::create_static_topic(self.inner(), addr, default_topic, topic_config, mapping_detail, force).await
    }

    async fn create_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        queue_num: i32,
        topic_sys_flag: i32,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> crate::ClientResult<()> {
        TopicAdmin::create_lite_pull_topic(
            self.inner(),
            addr,
            topic,
            queue_num,
            topic_sys_flag,
            read_queue_nums,
            write_queue_nums,
        )
        .await
    }

    async fn update_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> crate::ClientResult<()> {
        TopicAdmin::update_lite_pull_topic(self.inner(), addr, topic, read_queue_nums, write_queue_nums).await
    }

    async fn get_lite_pull_topic(&self, addr: CheetahString, topic: CheetahString) -> crate::ClientResult<TopicConfig> {
        TopicAdmin::get_lite_pull_topic(self.inner(), addr, topic).await
    }

    async fn delete_lite_pull_topic(
        &self,
        addr: CheetahString,
        cluster_name: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<()> {
        TopicAdmin::delete_lite_pull_topic(self.inner(), addr, cluster_name, topic).await
    }

    async fn query_lite_pull_topic_list(&self, addr: CheetahString) -> crate::ClientResult<TopicList> {
        TopicAdmin::query_lite_pull_topic_list(self.inner(), addr).await
    }

    async fn query_lite_pull_topic_by_cluster(&self, cluster_name: CheetahString) -> crate::ClientResult<TopicList> {
        TopicAdmin::query_lite_pull_topic_by_cluster(self.inner(), cluster_name).await
    }

    async fn get_topic_config_by_topic_name(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
    ) -> crate::ClientResult<TopicConfig> {
        TopicAdmin::get_topic_config_by_topic_name(self.inner(), broker_addr, topic_name).await
    }

    async fn get_topic_stats_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<TopicStatsTable> {
        TopicAdmin::get_topic_stats_info(self.inner(), broker_addr, topic).await
    }

    async fn query_broker_has_topic(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<bool> {
        TopicAdmin::query_broker_has_topic(self.inner(), broker_addr, topic).await
    }

    async fn get_system_topic_list_from_broker(&self, broker_addr: CheetahString) -> crate::ClientResult<TopicList> {
        TopicAdmin::get_system_topic_list_from_broker(self.inner(), broker_addr).await
    }

    async fn examine_topic_route_info_with_timeout(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<Option<TopicRouteData>> {
        TopicAdmin::examine_topic_route_info_with_timeout(self.inner(), topic, timeout_millis).await
    }

    async fn get_parent_topic_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<GetParentTopicInfoResponseBody> {
        TopicAdmin::get_parent_topic_info(self.inner(), broker_addr, topic).await
    }

    async fn get_lite_topic_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        lite_topic: CheetahString,
    ) -> crate::ClientResult<GetLiteTopicInfoResponseBody> {
        TopicAdmin::get_lite_topic_info(self.inner(), broker_addr, parent_topic, lite_topic).await
    }
}

impl ConsumerAdmin for crate::admin::default_mq_admin_ext::DefaultMQAdminExt {
    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::create_and_update_subscription_group_config(self.inner(), addr, config).await
    }

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::create_and_update_subscription_group_config_list(self.inner(), broker_addr, configs).await
    }

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> crate::ClientResult<SubscriptionGroupConfig> {
        ConsumerAdmin::examine_subscription_group_config(self.inner(), addr, group).await
    }

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> crate::ClientResult<ConsumeStats> {
        ConsumerAdmin::examine_consume_stats(
            self.inner(),
            consumer_group,
            topic,
            cluster_name,
            broker_addr,
            timeout_millis,
        )
        .await
    }

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> crate::ClientResult<ConsumerConnection> {
        ConsumerAdmin::examine_consumer_connection_info(self.inner(), consumer_group, broker_addr).await
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<ProducerConnection> {
        ConsumerAdmin::examine_producer_connection_info(self.inner(), producer_group, topic).await
    }

    async fn get_all_producer_info(&self, broker_addr: CheetahString) -> crate::ClientResult<ProducerTableInfo> {
        ConsumerAdmin::get_all_producer_info(self.inner(), broker_addr).await
    }

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::delete_subscription_group(self.inner(), addr, group_name, remove_offset).await
    }

    async fn delete_subscription_group_list(
        &self,
        addr: CheetahString,
        group_names: Vec<CheetahString>,
        clean_offset: bool,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::delete_subscription_group_list(self.inner(), addr, group_names, clean_offset).await
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        ConsumerAdmin::get_consume_status(self.inner(), topic, group, client_addr).await
    }

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<SubscriptionData> {
        ConsumerAdmin::query_subscription(self.inner(), group, topic).await
    }

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::ClientResult<bool> {
        ConsumerAdmin::clean_expired_consumer_queue(self.inner(), cluster, addr).await
    }

    async fn clean_expired_consumer_queue_by_addr(&self, addr: CheetahString) -> crate::ClientResult<bool> {
        ConsumerAdmin::clean_expired_consumer_queue_by_addr(self.inner(), addr).await
    }

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        metrics: Option<bool>,
    ) -> crate::ClientResult<ConsumerRunningInfo> {
        ConsumerAdmin::get_consumer_running_info(self.inner(), consumer_group, client_id, jstack, metrics).await
    }

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<ConsumeMessageDirectlyResult> {
        ConsumerAdmin::consume_message_directly(self.inner(), consumer_group, client_id, topic, msg_id).await
    }

    async fn consume_message_directly_ext(
        &self,
        cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<ConsumeMessageDirectlyResult> {
        ConsumerAdmin::consume_message_directly_ext(
            self.inner(),
            cluster_name,
            consumer_group,
            client_id,
            topic,
            msg_id,
        )
        .await
    }

    #[allow(deprecated)]
    async fn message_track_detail(&self, msg: MessageExt) -> crate::ClientResult<Vec<MessageTrack>> {
        ConsumerAdmin::message_track_detail(self.inner(), msg).await
    }

    #[allow(deprecated)]
    async fn message_track_detail_concurrent(&self, msg: MessageExt) -> AdminToolResult<Vec<MessageTrack>> {
        ConsumerAdmin::message_track_detail_concurrent(self.inner(), msg).await
    }

    async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: CheetahString,
        is_order: bool,
        timeout_millis: u64,
    ) -> crate::ClientResult<ConsumeStatsList> {
        ConsumerAdmin::fetch_consume_stats_in_broker(self.inner(), broker_addr, is_order, timeout_millis).await
    }

    async fn get_all_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<SubscriptionGroupWrapper> {
        ConsumerAdmin::get_all_subscription_group(self.inner(), broker_addr, timeout_millis).await
    }

    async fn query_consume_queue(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: u64,
        count: i32,
        consumer_group: CheetahString,
    ) -> crate::ClientResult<QueryConsumeQueueResponseBody> {
        ConsumerAdmin::query_consume_queue(self.inner(), broker_addr, topic, queue_id, index, count, consumer_group)
            .await
    }

    async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        topic_name: CheetahString,
        readable: Option<bool>,
    ) -> crate::ClientResult<GroupForbidden> {
        ConsumerAdmin::update_and_get_group_read_forbidden(self.inner(), broker_addr, group_name, topic_name, readable)
            .await
    }

    async fn query_message(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<MessageExt> {
        ConsumerAdmin::query_message(self.inner(), cluster_name, topic, msg_id).await
    }

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::update_cold_data_flow_ctr_group_config(self.inner(), broker_addr, properties).await
    }

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::remove_cold_data_flow_ctr_group_config(self.inner(), broker_addr, consumer_group).await
    }

    async fn query_lite_pull_subscription_list(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<GroupList> {
        ConsumerAdmin::query_lite_pull_subscription_list(self.inner(), addr, topic).await
    }

    async fn examine_consume_stats_with_queue(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        queue_id: Option<i32>,
    ) -> crate::ClientResult<ConsumeStats> {
        ConsumerAdmin::examine_consume_stats_with_queue(self.inner(), consumer_group, topic, queue_id).await
    }

    async fn examine_consume_stats_concurrent(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        ConsumerAdmin::examine_consume_stats_concurrent(self.inner(), consumer_group, topic).await
    }

    async fn examine_consume_stats_concurrent_with_cluster(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        ConsumerAdmin::examine_consume_stats_concurrent_with_cluster(self.inner(), consumer_group, topic, cluster_name)
            .await
    }

    async fn sync_broker_member_group(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> crate::ClientResult<()> {
        ConsumerAdmin::sync_broker_member_group(self.inner(), controller_addr, cluster_name, broker_name).await
    }

    async fn get_lite_group_info(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
    ) -> crate::ClientResult<GetLiteGroupInfoResponseBody> {
        ConsumerAdmin::get_lite_group_info(self.inner(), broker_addr, group, lite_topic, top_k).await
    }
}

impl BrokerAdmin for crate::admin::default_mq_admin_ext::DefaultMQAdminExt {
    async fn start(&mut self) -> crate::ClientResult<()> {
        BrokerAdmin::start(self.inner_mut()).await
    }

    async fn shutdown(&mut self) {
        BrokerAdmin::shutdown(self.inner_mut()).await
    }

    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::add_broker_to_container(self.inner(), broker_container_addr, broker_config).await
    }

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::remove_broker_from_container(
            self.inner(),
            broker_container_addr,
            cluster_name,
            broker_name,
            broker_id,
        )
        .await
    }

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::update_broker_config(self.inner(), broker_addr, properties).await
    }

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<HashMap<CheetahString, CheetahString>> {
        BrokerAdmin::get_broker_config(self.inner(), broker_addr).await
    }

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> crate::ClientResult<KVTable> {
        BrokerAdmin::fetch_broker_runtime_stats(self.inner(), broker_addr).await
    }

    async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        check_store_time: i64,
    ) -> crate::ClientResult<CheckRocksdbCqWriteResult> {
        BrokerAdmin::check_rocksdb_cq_write_progress(self.inner(), broker_addr, topic, check_store_time).await
    }

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> crate::ClientResult<i32> {
        BrokerAdmin::wipe_write_perm_of_broker(self.inner(), namesrv_addr, broker_name).await
    }

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> crate::ClientResult<i32> {
        BrokerAdmin::add_write_perm_of_broker(self.inner(), namesrv_addr, broker_name).await
    }

    async fn view_broker_stats_data(
        &self,
        broker_addr: CheetahString,
        stats_name: CheetahString,
        stats_key: CheetahString,
    ) -> crate::ClientResult<BrokerStatsData> {
        BrokerAdmin::view_broker_stats_data(self.inner(), broker_addr, stats_name, stats_key).await
    }

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::ClientResult<bool> {
        BrokerAdmin::resume_check_half_message(self.inner(), topic, msg_id).await
    }

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> crate::ClientResult<HARuntimeInfo> {
        BrokerAdmin::get_broker_ha_status(self.inner(), broker_addr).await
    }

    async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
    ) -> crate::ClientResult<BrokerReplicasInfo> {
        BrokerAdmin::get_in_sync_state_data(self.inner(), controller_address, brokers).await
    }

    async fn get_broker_epoch_cache(&self, broker_addr: CheetahString) -> crate::ClientResult<EpochEntryCache> {
        BrokerAdmin::get_broker_epoch_cache(self.inner(), broker_addr).await
    }

    async fn get_controller_meta_data(
        &self,
        controller_addr: CheetahString,
    ) -> crate::ClientResult<GetMetaDataResponseHeader> {
        BrokerAdmin::get_controller_meta_data(self.inner(), controller_addr).await
    }

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> crate::ClientResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        BrokerAdmin::get_controller_config(self.inner(), controller_servers).await
    }

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::update_controller_config(self.inner(), properties, controllers).await
    }

    async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
    ) -> crate::ClientResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        BrokerAdmin::elect_master(self.inner(), controller_addr, cluster_name, broker_name, broker_id).await
    }

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::clean_controller_broker_data(
            self.inner(),
            controller_addr,
            cluster_name,
            broker_name,
            broker_controller_ids_to_clean,
            is_clean_living_broker,
        )
        .await
    }

    async fn get_cold_data_flow_ctr_info(&self, broker_addr: CheetahString) -> crate::ClientResult<CheetahString> {
        BrokerAdmin::get_cold_data_flow_ctr_info(self.inner(), broker_addr).await
    }

    async fn notify_min_broker_id_changed(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        min_broker_id: u64,
        min_broker_addr: CheetahString,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::notify_min_broker_id_changed(
            self.inner(),
            cluster_name,
            broker_name,
            min_broker_id,
            min_broker_addr,
            offline_broker_addr,
            ha_broker_addr,
        )
        .await
    }

    async fn export_pop_records(&self, broker_addr: CheetahString, timeout: u64) -> crate::ClientResult<()> {
        BrokerAdmin::export_pop_records(self.inner(), broker_addr, timeout).await
    }

    async fn switch_timer_engine(
        &self,
        broker_addr: CheetahString,
        des_timer_engine: CheetahString,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::switch_timer_engine(self.inner(), broker_addr, des_timer_engine).await
    }

    async fn get_broker_lite_info(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<GetBrokerLiteInfoResponseBody> {
        BrokerAdmin::get_broker_lite_info(self.inner(), broker_addr).await
    }

    async fn get_lite_client_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> crate::ClientResult<GetLiteClientInfoResponseBody> {
        BrokerAdmin::get_lite_client_info(self.inner(), broker_addr, parent_topic, group, client_id).await
    }

    async fn trigger_lite_dispatch(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::trigger_lite_dispatch(self.inner(), broker_addr, group, client_id).await
    }

    async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
    ) -> crate::ClientResult<()> {
        BrokerAdmin::export_rocksdb_config_to_json(self.inner(), broker_addr, config_types).await
    }
}

impl AuthAdmin for crate::admin::default_mq_admin_ext::DefaultMQAdminExt {
    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> crate::ClientResult<()> {
        AuthAdmin::create_and_update_plain_access_config(self.inner(), addr, config).await
    }

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::delete_plain_access_config(self.inner(), addr, access_key).await
    }

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> crate::ClientResult<()> {
        AuthAdmin::update_global_white_addr_config(self.inner(), addr, global_white_addrs, acl_file_full_path).await
    }

    async fn examine_broker_cluster_acl_version_info(&self, addr: CheetahString) -> crate::ClientResult<CheetahString> {
        AuthAdmin::examine_broker_cluster_acl_version_info(self.inner(), addr).await
    }

    async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::ClientResult<SubscriptionGroupWrapper> {
        AuthAdmin::get_user_subscription_group(self.inner(), broker_addr, timeout_millis).await
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> crate::ClientResult<TopicConfigSerializeWrapper> {
        AuthAdmin::get_user_topic_config(self.inner(), broker_addr, special_topic, timeout_millis).await
    }

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::create_user(self.inner(), broker_addr, username, password, user_type).await
    }

    async fn create_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::create_user_with_info(self.inner(), broker_addr, username, password).await
    }

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::update_user(self.inner(), broker_addr, username, password, user_type, user_status).await
    }

    async fn update_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::update_user_with_info(self.inner(), broker_addr, username, password).await
    }

    async fn delete_user(&self, broker_addr: CheetahString, username: CheetahString) -> crate::ClientResult<()> {
        AuthAdmin::delete_user(self.inner(), broker_addr, username).await
    }

    async fn get_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> crate::ClientResult<Option<UserInfo>> {
        AuthAdmin::get_user(self.inner(), broker_addr, username).await
    }

    async fn list_users(
        &self,
        broker_addr: CheetahString,
        filter: CheetahString,
    ) -> crate::ClientResult<Vec<UserInfo>> {
        AuthAdmin::list_users(self.inner(), broker_addr, filter).await
    }

    async fn create_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::create_acl(
            self.inner(),
            broker_addr,
            subject,
            resources,
            actions,
            source_ips,
            decision,
        )
        .await
    }

    async fn create_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::create_acl_with_info(self.inner(), broker_addr, subject).await
    }

    async fn update_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::update_acl(
            self.inner(),
            broker_addr,
            subject,
            resources,
            actions,
            source_ips,
            decision,
        )
        .await
    }

    async fn update_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::update_acl_with_info(self.inner(), broker_addr, subject).await
    }

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> crate::ClientResult<()> {
        AuthAdmin::delete_acl(self.inner(), broker_addr, subject, resource).await
    }

    async fn get_acl(&self, broker_addr: CheetahString, subject: CheetahString) -> crate::ClientResult<AclInfo> {
        AuthAdmin::get_acl(self.inner(), broker_addr, subject).await
    }

    async fn list_acl(
        &self,
        broker_addr: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
    ) -> crate::ClientResult<Vec<AclInfo>> {
        AuthAdmin::list_acl(self.inner(), broker_addr, subject_filter, resource_filter).await
    }
}

impl OffsetAdmin for crate::admin::default_mq_admin_ext::DefaultMQAdminExt {
    async fn reset_offset_by_timestamp_old(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> crate::ClientResult<Vec<RollbackStats>> {
        OffsetAdmin::reset_offset_by_timestamp_old(self.inner(), cluster_name, consumer_group, topic, timestamp, force)
            .await
    }

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> crate::ClientResult<HashMap<MessageQueue, u64>> {
        OffsetAdmin::reset_offset_by_timestamp(self.inner(), cluster_name, topic, group, timestamp, is_force).await
    }

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::reset_offset_new(self.inner(), consumer_group, topic, timestamp).await
    }

    #[allow(deprecated)]
    async fn reset_offset_new_concurrent(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult> {
        OffsetAdmin::reset_offset_new_concurrent(self.inner(), group, topic, timestamp).await
    }

    async fn query_consume_time_span(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> crate::ClientResult<Vec<QueueTimeSpan>> {
        OffsetAdmin::query_consume_time_span(self.inner(), topic, group).await
    }

    async fn query_consume_time_span_concurrent(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>> {
        OffsetAdmin::query_consume_time_span_concurrent(self.inner(), topic, group).await
    }

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::ClientResult<bool> {
        OffsetAdmin::delete_expired_commit_log(self.inner(), cluster, addr).await
    }

    async fn delete_expired_commit_log_by_addr(&self, addr: CheetahString) -> crate::ClientResult<bool> {
        OffsetAdmin::delete_expired_commit_log_by_addr(self.inner(), addr).await
    }

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::clone_group_offset(self.inner(), src_group, dest_group, topic, is_offline).await
    }

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::update_consume_offset(self.inner(), broker_addr, consume_group, mq, offset).await
    }

    async fn reset_offset_by_queue_id(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        reset_offset: u64,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::reset_offset_by_queue_id(
            self.inner(),
            broker_addr,
            consumer_group,
            topic_name,
            queue_id,
            reset_offset,
        )
        .await
    }

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::reset_master_flush_offset(self.inner(), broker_addr, master_flush_offset).await
    }

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> crate::ClientResult<CheetahString> {
        OffsetAdmin::set_commit_log_read_ahead_mode(self.inner(), broker_addr, mode).await
    }

    async fn update_lite_pull_consumer_offset(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        group: CheetahString,
        queue_id: i32,
        offset: u64,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::update_lite_pull_consumer_offset(self.inner(), addr, topic, group, queue_id, offset).await
    }

    async fn export_rocksdb_consumer_offset_to_json(
        &self,
        broker_addr: CheetahString,
        file_path: CheetahString,
    ) -> crate::ClientResult<()> {
        OffsetAdmin::export_rocksdb_consumer_offset_to_json(self.inner(), broker_addr, file_path).await
    }

    async fn export_rocksdb_consumer_offset_from_memory(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<CheetahString> {
        OffsetAdmin::export_rocksdb_consumer_offset_from_memory(self.inner(), broker_addr).await
    }

    async fn search_offset(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        timestamp: u64,
        timeout_millis: u64,
    ) -> crate::ClientResult<u64> {
        OffsetAdmin::search_offset(
            self.inner(),
            broker_addr,
            topic_name,
            queue_id,
            timestamp,
            timeout_millis,
        )
        .await
    }

    async fn min_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> crate::ClientResult<i64> {
        OffsetAdmin::min_offset(self.inner(), broker_addr, message_queue, timeout_millis).await
    }

    async fn max_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> crate::ClientResult<i64> {
        OffsetAdmin::max_offset(self.inner(), broker_addr, message_queue, timeout_millis).await
    }
}
