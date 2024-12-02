/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::group_list::GroupList;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::admin::broker_operator_result::BrokerOperatorResult;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;

use crate::admin::common::admin_tool_result::AdminToolResult;
use crate::Result;

#[cfg(feature = "async")]
#[allow(dead_code)]
#[trait_variant::make(MQAdminExt: Send)]
pub trait MQAdminExtLocal: Sync {
    async fn start(&self) -> Result<()>;
    async fn shutdown(&self);
    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> Result<()>;

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> Result<()>;

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> Result<()>;

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> Result<HashMap<CheetahString, CheetahString>>;

    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> Result<()>;

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> Result<()>;

    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> Result<()>;

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> Result<()>;

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> Result<()>;

    async fn examine_broker_cluster_acl_version_info(
        &self,
        addr: CheetahString,
    ) -> Result<CheetahString>;

    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> Result<()>;

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> Result<()>;

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> Result<SubscriptionGroupConfig>;

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> Result<TopicStatsTable>;

    async fn examine_topic_stats_concurrent(
        &self,
        topic: CheetahString,
    ) -> AdminToolResult<TopicStatsTable>;

    async fn fetch_all_topic_list(&self) -> Result<TopicList>;

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> Result<TopicList>;

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> Result<KVTable>;

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> Result<ConsumeStats>;

    /*async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> Result<CheckRocksdbCqWriteProgressResponseBody>;*/

    async fn examine_broker_cluster_info(&self) -> Result<ClusterInfo>;

    async fn examine_topic_route_info(&self, topic: CheetahString) -> Result<TopicRouteData>;

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> Result<ConsumerConnection>;

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> Result<ProducerConnection>;

    /* async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> Result<ProducerTableInfo>;*/

    async fn get_name_server_address_list(&self) -> Vec<CheetahString>;

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> Result<i32>;

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> Result<i32>;

    async fn put_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    );

    async fn get_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> Result<CheetahString>;

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> Result<KVTable>;

    async fn delete_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> Result<()>;

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> Result<()>;

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
    ) -> Result<()>;

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> Result<()>;

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> Result<()>;

    async fn delete_kv_config(&self, namespace: CheetahString, key: CheetahString) -> Result<()>;

    async fn reset_offset_by_timestamp_old(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> Result<Vec<RollbackStats>>;

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> Result<HashMap<MessageQueue, u64>>;

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> Result<()>;

    async fn reset_offset_new_concurrent(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult>;

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> Result<HashMap<CheetahString, HashMap<MessageQueue, u64>>>;

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> Result<()>;

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> Result<GroupList>;

    async fn query_topics_by_consumer(&self, group: CheetahString) -> Result<TopicList>;

    async fn query_topics_by_consumer_concurrent(
        &self,
        group: CheetahString,
    ) -> AdminToolResult<TopicList>;

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> Result<SubscriptionData>;

    /*async fn query_consume_time_span(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> Result<Vec<QueueTimeSpan>>;

    async fn query_consume_time_span_concurrent(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>>;*/

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> Result<bool>;

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> Result<bool>;

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> Result<bool>;

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        metrics: Option<bool>,
    ) -> Result<ConsumerRunningInfo>;

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> Result<ConsumeMessageDirectlyResult>;

    async fn consume_message_directly_ext(
        &self,
        cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> Result<ConsumeMessageDirectlyResult>;

    /*async fn message_track_detail(
        &self,
        msg: MessageExt,
    ) -> Result<Vec<MessageTrack>>;

    async fn message_track_detail_concurrent(
        &self,
        msg: MessageExt,
    ) -> AdminToolResult<Vec<MessageTrack>>;*/

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> Result<()>;

    /*async fn view_broker_stats_data(
        &self,
        broker_addr: CheetahString,
        stats_name: CheetahString,
        stats_key: CheetahString,
    ) -> Result<BrokerStatsData>;*/

    async fn get_cluster_list(&self, topic: String) -> Result<HashSet<CheetahString>>;

    /*async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: CheetahString,
        is_order: bool,
        timeout_millis: u64,
    ) -> Result<ConsumeStatsList>;*/

    async fn get_topic_cluster_list(&self, topic: String) -> Result<HashSet<CheetahString>>;

    /*async fn get_all_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> Result<SubscriptionGroupWrapper>;*/

    /*async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> Result<SubscriptionGroupWrapper>;*/

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> Result<TopicConfigSerializeWrapper>;

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> Result<TopicConfigSerializeWrapper>;

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> Result<()>;

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        name_servers: Vec<CheetahString>,
    ) -> Result<()>;

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> Result<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>;

    /*async fn query_consume_queue(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: u64,
        count: i32,
        consumer_group: CheetahString,
    ) -> Result<QueryConsumeQueueResponseBody>;*/

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> Result<bool>;

    async fn set_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> Result<()>;

    async fn reset_offset_by_queue_id(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        reset_offset: u64,
    ) -> Result<()>;

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> Result<TopicConfig>;

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> Result<()>;

    /*async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        topic_name: CheetahString,
        readable: Option<bool>,
    ) -> Result<GroupForbidden>;

    async fn query_message(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> Result<MessageExt>;

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> Result<HARuntimeInfo>;

    async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
    ) -> Result<BrokerReplicasInfo>;

    async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
    ) -> Result<EpochEntryCache>;

    async fn get_controller_meta_data(
        &self,
        controller_addr: CheetahString,
    ) -> Result<GetMetaDataResponseHeader>;*/

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> Result<()>;

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> Result<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>;

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> Result<()>;

    /*async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
    ) -> Result<(ElectMasterResponseHeader, BrokerMemberGroup)>;*/

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> Result<()>;

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> Result<()>;

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> Result<()>;

    async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
    ) -> Result<CheetahString>;

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> Result<CheetahString>;

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> Result<()>;

    /*async fn create_user_with_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> Result<()>;*/

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> Result<()>;

    /* async fn update_user_with_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> Result<()>;*/

    async fn delete_user(&self, broker_addr: CheetahString, username: CheetahString) -> Result<()>;

    /*async fn get_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> Result<UserInfo>;*/

    /* async fn list_users(
        &self,
        broker_addr: CheetahString,
        filter: CheetahString,
    ) -> Result<Vec<UserInfo>>;*/

    async fn create_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> Result<()>;

    /*async fn create_acl_with_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> Result<()>;*/

    async fn update_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> Result<()>;

    /*async fn update_acl_with_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> Result<()>;*/

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> Result<()>;

    /*async fn get_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> Result<AclInfo>;*/

    /*async fn list_acl(
        &self,
        broker_addr: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
    ) -> Result<Vec<AclInfo>>;*/
}
