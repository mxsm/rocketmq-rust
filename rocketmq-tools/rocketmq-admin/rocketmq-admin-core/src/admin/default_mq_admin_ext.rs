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
use std::sync::Arc;
use std::time::Duration;

use crate::client_adapter::legacy::client_sdk::admin::default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
use crate::client_adapter::legacy::client_sdk::admin::mq_admin_ext_async::MQAdminExt;
use crate::client_adapter::legacy::client_sdk::base::client_config::ClientConfig;
use crate::client_adapter::legacy::client_sdk::common::admin_tool_result::AdminToolResult;
use crate::client_adapter::legacy::common_sdk::common::base::plain_access_config::PlainAccessConfig;
use crate::client_adapter::legacy::common_sdk::common::config::TopicConfig;
use crate::client_adapter::legacy::common_sdk::common::message::message_enum::MessageRequestMode;
use crate::client_adapter::legacy::common_sdk::common::message::message_ext::MessageExt;
use crate::client_adapter::legacy::common_sdk::common::message::message_queue::MessageQueue;
#[allow(deprecated)]
use crate::client_adapter::legacy::common_sdk::common::tools::broker_operator_result::BrokerOperatorResult;
#[allow(deprecated)]
use crate::client_adapter::legacy::common_sdk::common::tools::message_track::MessageTrack;
use crate::client_adapter::legacy::common_sdk::common::topic::TopicValidator;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_remoting::protocol::body::group_list::GroupList;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;

use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;

use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_remoting::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_remoting::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;

const ADMIN_EXT_GROUP: &str = "admin_ext_group";

pub struct DefaultMQAdminExt {
    admin_ext_group: CheetahString,
    create_topic_key: CheetahString,
    timeout_millis: Duration,
    default_mqadmin_ext_impl: DefaultMQAdminExtImpl,
}

impl DefaultMQAdminExt {
    fn build(
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
        timeout_millis: Duration,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let default_mqadmin_ext_impl =
            DefaultMQAdminExtImpl::new(rpc_hook, timeout_millis, client_config, admin_ext_group.clone());

        Self {
            default_mqadmin_ext_impl,
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis,
        }
    }

    pub fn set_namesrv_addr(&mut self, name_serv_addr: &str) {
        self.default_mqadmin_ext_impl
            .client_config_mut()
            .set_namesrv_addr(name_serv_addr.into());
    }
}

impl DefaultMQAdminExt {
    pub fn new() -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ClientConfig::new();
        Self::build(client_config, admin_ext_group, Duration::from_millis(5000), None)
    }

    pub fn with_timeout(timeout_millis: Duration) -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ClientConfig::new();
        Self::build(client_config, admin_ext_group, timeout_millis, None)
    }

    pub fn with_rpc_hook(rpc_hook: Arc<dyn RPCHook>) -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ClientConfig::new();
        Self::build(
            client_config,
            admin_ext_group,
            Duration::from_millis(5000),
            Some(rpc_hook),
        )
    }

    pub fn with_rpc_hook_and_timeout(rpc_hook: Arc<dyn RPCHook>, timeout_millis: Duration) -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ClientConfig::new();
        Self::build(client_config, admin_ext_group, timeout_millis, Some(rpc_hook))
    }

    pub fn with_admin_ext_group_and_rpc_hook(
        admin_ext_group: impl Into<CheetahString>,
        rpc_hook: Arc<dyn RPCHook>,
    ) -> Self {
        let admin_ext_group = admin_ext_group.into();
        let client_config = ClientConfig::new();
        Self::build(
            client_config,
            admin_ext_group,
            Duration::from_millis(5000),
            Some(rpc_hook),
        )
    }

    pub fn with_admin_ext_group(admin_ext_group: impl Into<CheetahString>) -> Self {
        let admin_ext_group = admin_ext_group.into();
        let client_config = ClientConfig::new();
        Self::build(client_config, admin_ext_group, Duration::from_millis(5000), None)
    }

    pub fn with_admin_ext_group_and_timeout(
        admin_ext_group: impl Into<CheetahString>,
        timeout_millis: Duration,
    ) -> Self {
        let admin_ext_group = admin_ext_group.into();
        let client_config = ClientConfig::new();
        Self::build(client_config, admin_ext_group, timeout_millis, None)
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        self.default_mqadmin_ext_impl.client_config()
    }

    #[inline]
    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        self.default_mqadmin_ext_impl.client_config_mut()
    }

    pub async fn create_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_acl_with_acl_info(broker_addr, acl_info)
            .await
    }

    pub async fn update_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_acl_with_acl_info(broker_addr, acl_info)
            .await
    }

    pub async fn create_user_with_user_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_user_with_user_info(broker_addr, user_info)
            .await
    }

    pub async fn update_user_with_user_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_user_with_user_info(broker_addr, user_info)
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
    ) -> rocketmq_error::RocketMQResult<crate::client_adapter::legacy::client_sdk::consumer::pull_result::PullResult>
    {
        self.default_mqadmin_ext_impl
            .pull_message_from_queue(broker_addr, mq, sub_expression, offset, max_nums, timeout_millis)
            .await
    }

    pub async fn query_message_by_key(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        key_type: CheetahString,
        last_key: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<crate::client_adapter::legacy::client_sdk::base::query_result::QueryResult>
    {
        self.default_mqadmin_ext_impl
            .query_message_by_key(
                cluster_name,
                topic,
                key,
                max_num,
                begin_timestamp,
                end_timestamp,
                key_type,
                last_key,
            )
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
    ) -> rocketmq_error::RocketMQResult<crate::client_adapter::legacy::client_sdk::base::query_result::QueryResult>
    {
        self.default_mqadmin_ext_impl
            .query_message_by_unique_key(cluster_name, topic, unique_key, max_num, begin_timestamp, end_timestamp)
            .await
    }
}

impl Default for DefaultMQAdminExt {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused_variables)]
#[allow(unused_mut)]
impl MQAdminExt for DefaultMQAdminExt {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        MQAdminExt::start(&mut self.default_mqadmin_ext_impl).await
    }

    async fn shutdown(&mut self) {
        MQAdminExt::shutdown(&mut self.default_mqadmin_ext_impl).await
    }

    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .add_broker_to_container(broker_container_addr, broker_config)
            .await
    }

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .remove_broker_from_container(broker_container_addr, cluster_name, broker_name, broker_id)
            .await
    }

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_broker_config(broker_addr, properties)
            .await
    }

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, CheetahString>> {
        self.default_mqadmin_ext_impl.get_broker_config(broker_addr).await
    }

    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_and_update_topic_config(addr, config)
            .await
    }

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_and_update_topic_config_list(addr, topic_config_list)
            .await
    }

    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_and_update_plain_access_config(addr, config)
            .await
    }

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .delete_plain_access_config(addr, access_key)
            .await
    }

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_global_white_addr_config(addr, global_white_addrs, acl_file_full_path)
            .await
    }

    async fn examine_broker_cluster_acl_version_info(
        &self,
        addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.default_mqadmin_ext_impl
            .examine_broker_cluster_acl_version_info(addr)
            .await
    }

    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_and_update_subscription_group_config(addr, config)
            .await
    }

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_and_update_subscription_group_config_list(broker_addr, configs)
            .await
    }

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfig> {
        self.default_mqadmin_ext_impl
            .examine_subscription_group_config(addr, group)
            .await
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        self.default_mqadmin_ext_impl
            .examine_topic_stats(topic, broker_addr)
            .await
    }

    async fn examine_topic_stats_concurrent(&self, topic: CheetahString) -> AdminToolResult<TopicStatsTable> {
        self.default_mqadmin_ext_impl
            .examine_topic_stats_concurrent(topic)
            .await
    }

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList> {
        self.default_mqadmin_ext_impl.fetch_all_topic_list().await
    }

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.default_mqadmin_ext_impl
            .fetch_topics_by_cluster(cluster_name)
            .await
    }

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.default_mqadmin_ext_impl
            .fetch_broker_runtime_stats(broker_addr)
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
        self.default_mqadmin_ext_impl
            .examine_consume_stats(consumer_group, topic, cluster_name, broker_addr, timeout_millis)
            .await
    }

    async fn examine_broker_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        self.default_mqadmin_ext_impl.examine_broker_cluster_info().await
    }

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.default_mqadmin_ext_impl.examine_topic_route_info(topic).await
    }

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection> {
        self.default_mqadmin_ext_impl
            .examine_consumer_connection_info(consumer_group, broker_addr)
            .await
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        self.default_mqadmin_ext_impl
            .examine_producer_connection_info(producer_group, topic)
            .await
    }

    async fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        self.default_mqadmin_ext_impl.get_name_server_address_list().await
    }

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> RocketMQResult<i32> {
        self.default_mqadmin_ext_impl
            .wipe_write_perm_of_broker(namesrv_addr, broker_name)
            .await
    }

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> RocketMQResult<i32> {
        self.default_mqadmin_ext_impl
            .add_write_perm_of_broker(namesrv_addr, broker_name)
            .await
    }

    async fn put_kv_config(&self, namespace: CheetahString, key: CheetahString, value: CheetahString) {
        self.default_mqadmin_ext_impl.put_kv_config(namespace, key, value).await
    }

    async fn get_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.default_mqadmin_ext_impl.get_kv_config(namespace, key).await
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.default_mqadmin_ext_impl.get_kv_list_by_namespace(namespace).await
    }

    async fn delete_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .delete_topic(topic_name, cluster_name)
            .await
    }

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl.delete_topic_in_broker(addrs, topic).await
    }

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .delete_topic_in_name_server(addrs, cluster_name, topic)
            .await
    }

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .delete_subscription_group(addr, group_name, remove_offset)
            .await
    }

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_and_update_kv_config(namespace, key, value)
            .await
    }

    async fn delete_kv_config(&self, namespace: CheetahString, key: CheetahString) -> RocketMQResult<()> {
        self.default_mqadmin_ext_impl.delete_kv_config(namespace, key).await
    }

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>> {
        self.default_mqadmin_ext_impl
            .reset_offset_by_timestamp(cluster_name, topic, group, timestamp, is_force)
            .await
    }

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .reset_offset_new(consumer_group, topic, timestamp)
            .await
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        self.default_mqadmin_ext_impl
            .get_consume_status(topic, group, client_addr)
            .await
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_or_update_order_conf(key, value, is_cluster)
            .await
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList> {
        self.default_mqadmin_ext_impl.query_topic_consume_by_who(topic).await
    }

    async fn query_topics_by_consumer(&self, group: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.default_mqadmin_ext_impl.query_topics_by_consumer(group).await
    }

    async fn query_topics_by_consumer_concurrent(&self, group: CheetahString) -> AdminToolResult<TopicList> {
        self.default_mqadmin_ext_impl
            .query_topics_by_consumer_concurrent(group)
            .await
    }

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionData> {
        self.default_mqadmin_ext_impl.query_subscription(group, topic).await
    }

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl
            .clean_expired_consumer_queue(cluster, addr)
            .await
    }

    async fn clean_expired_consumer_queue_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl
            .clean_expired_consumer_queue_by_addr(addr)
            .await
    }

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl
            .delete_expired_commit_log(cluster, addr)
            .await
    }

    async fn delete_expired_commit_log_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl
            .delete_expired_commit_log_by_addr(addr)
            .await
    }

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl.clean_unused_topic(cluster, addr).await
    }

    async fn clean_unused_topic_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl.clean_unused_topic_by_addr(addr).await
    }

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        metrics: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo> {
        self.default_mqadmin_ext_impl
            .get_consumer_running_info(consumer_group, client_id, jstack, metrics)
            .await
    }

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        self.default_mqadmin_ext_impl
            .consume_message_directly(consumer_group, client_id, topic, msg_id)
            .await
    }

    async fn consume_message_directly_ext(
        &self,
        cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        self.default_mqadmin_ext_impl
            .consume_message_directly_ext(cluster_name, consumer_group, client_id, topic, msg_id)
            .await
    }

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .clone_group_offset(src_group, dest_group, topic, is_offline)
            .await
    }

    async fn get_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        self.default_mqadmin_ext_impl.get_cluster_list(topic).await
    }

    async fn get_topic_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        return self.default_mqadmin_ext_impl.get_topic_cluster_list(topic).await;
    }

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        self.default_mqadmin_ext_impl
            .get_all_topic_config(broker_addr, timeout_millis)
            .await
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        self.default_mqadmin_ext_impl
            .get_user_topic_config(broker_addr, special_topic, timeout_millis)
            .await
    }

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_consume_offset(broker_addr, consume_group, mq, offset)
            .await
    }

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        special_server_list: Option<Vec<CheetahString>>,
    ) -> RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_name_server_config(properties, special_server_list)
            .await
    }

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        self.default_mqadmin_ext_impl.get_name_server_config(name_servers).await
    }

    async fn probe_name_server(&self, name_server: CheetahString) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl.probe_name_server(name_server).await
    }

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl
            .resume_check_half_message(topic, msg_id)
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
        MQAdminExt::set_message_request_mode(
            &self.default_mqadmin_ext_impl,
            broker_addr,
            topic,
            consumer_group,
            mode,
            pop_work_group_size,
            timeout_millis,
        )
        .await
    }

    async fn reset_offset_by_queue_id(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        reset_offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .reset_offset_by_queue_id(broker_addr, consumer_group, topic_name, queue_id, reset_offset)
            .await
    }

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.default_mqadmin_ext_impl.examine_topic_config(addr, topic).await
    }

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_static_topic(addr, default_topic, topic_config, mapping_detail, force)
            .await
    }

    async fn get_controller_meta_data(
        &self,
        controller_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetMetaDataResponseHeader> {
        self.default_mqadmin_ext_impl
            .get_controller_meta_data(controller_addr)
            .await
    }

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .reset_master_flush_offset(broker_addr, master_flush_offset)
            .await
    }

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        self.default_mqadmin_ext_impl
            .get_controller_config(controller_servers)
            .await
    }

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_controller_config(properties, controllers)
            .await
    }

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .clean_controller_broker_data(
                controller_addr,
                cluster_name,
                broker_name,
                broker_controller_ids_to_clean,
                is_clean_living_broker,
            )
            .await
    }

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_cold_data_flow_ctr_group_config(broker_addr, properties)
            .await
    }

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .remove_cold_data_flow_ctr_group_config(broker_addr, consumer_group)
            .await
    }

    async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.default_mqadmin_ext_impl
            .get_cold_data_flow_ctr_info(broker_addr)
            .await
    }

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.default_mqadmin_ext_impl
            .set_commit_log_read_ahead_mode(broker_addr, mode)
            .await
    }

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_user(broker_addr, username, password, user_type)
            .await
    }

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_user(broker_addr, username, password, user_type, user_status)
            .await
    }

    async fn delete_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl.delete_user(broker_addr, username).await
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
        self.default_mqadmin_ext_impl
            .create_acl(broker_addr, subject, resources, actions, source_ips, decision)
            .await
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
        self.default_mqadmin_ext_impl
            .update_acl(broker_addr, subject, resources, actions, source_ips, decision)
            .await
    }

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .delete_acl(broker_addr, subject, resource)
            .await
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
        self.default_mqadmin_ext_impl
            .create_lite_pull_topic(
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
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_lite_pull_topic(addr, topic, read_queue_nums, write_queue_nums)
            .await
    }

    async fn get_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.default_mqadmin_ext_impl.get_lite_pull_topic(addr, topic).await
    }

    async fn delete_lite_pull_topic(
        &self,
        addr: CheetahString,
        cluster_name: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .delete_lite_pull_topic(addr, cluster_name, topic)
            .await
    }

    async fn query_lite_pull_topic_list(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.default_mqadmin_ext_impl.query_lite_pull_topic_list(addr).await
    }

    async fn query_lite_pull_topic_by_cluster(
        &self,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        self.default_mqadmin_ext_impl
            .query_lite_pull_topic_by_cluster(cluster_name)
            .await
    }

    async fn query_lite_pull_subscription_list(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GroupList> {
        self.default_mqadmin_ext_impl
            .query_lite_pull_subscription_list(addr, topic)
            .await
    }

    async fn update_lite_pull_consumer_offset(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        group: CheetahString,
        queue_id: i32,
        offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_lite_pull_consumer_offset(addr, topic, group, queue_id, offset)
            .await
    }

    async fn examine_consume_stats_with_queue(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        queue_id: Option<i32>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        self.default_mqadmin_ext_impl
            .examine_consume_stats_with_queue(consumer_group, topic, queue_id)
            .await
    }

    async fn examine_consume_stats_concurrent(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        self.default_mqadmin_ext_impl
            .examine_consume_stats_concurrent(consumer_group, topic)
            .await
    }

    async fn examine_consume_stats_concurrent_with_cluster(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        self.default_mqadmin_ext_impl
            .examine_consume_stats_concurrent_with_cluster(consumer_group, topic, cluster_name)
            .await
    }

    async fn export_rocksdb_consumer_offset_to_json(
        &self,
        broker_addr: CheetahString,
        file_path: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .export_rocksdb_consumer_offset_to_json(broker_addr, file_path)
            .await
    }

    async fn export_rocksdb_consumer_offset_from_memory(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.default_mqadmin_ext_impl
            .export_rocksdb_consumer_offset_from_memory(broker_addr)
            .await
    }

    async fn sync_broker_member_group(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .sync_broker_member_group(controller_addr, cluster_name, broker_name)
            .await
    }

    async fn get_topic_config_by_topic_name(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.default_mqadmin_ext_impl
            .get_topic_config_by_topic_name(broker_addr, topic_name)
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
        self.default_mqadmin_ext_impl
            .notify_min_broker_id_changed(
                cluster_name,
                broker_name,
                min_broker_id,
                min_broker_addr,
                offline_broker_addr,
                ha_broker_addr,
            )
            .await
    }

    async fn get_topic_stats_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        self.default_mqadmin_ext_impl
            .get_topic_stats_info(broker_addr, topic)
            .await
    }

    async fn query_broker_has_topic(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.default_mqadmin_ext_impl
            .query_broker_has_topic(broker_addr, topic)
            .await
    }

    async fn get_system_topic_list_from_broker(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        self.default_mqadmin_ext_impl
            .get_system_topic_list_from_broker(broker_addr)
            .await
    }

    async fn examine_topic_route_info_with_timeout(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.default_mqadmin_ext_impl
            .examine_topic_route_info_with_timeout(topic, timeout_millis)
            .await
    }

    async fn export_pop_records(&self, broker_addr: CheetahString, timeout: u64) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .export_pop_records(broker_addr, timeout)
            .await
    }

    async fn switch_timer_engine(
        &self,
        broker_addr: CheetahString,
        des_timer_engine: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .switch_timer_engine(broker_addr, des_timer_engine)
            .await
    }

    async fn trigger_lite_dispatch(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .trigger_lite_dispatch(broker_addr, group, client_id)
            .await
    }

    async fn search_offset(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        timestamp: u64,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<u64> {
        self.default_mqadmin_ext_impl
            .search_offset(broker_addr, topic_name, queue_id, timestamp, timeout_millis)
            .await
    }

    async fn min_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.default_mqadmin_ext_impl
            .min_offset(broker_addr, message_queue, timeout_millis)
            .await
    }

    async fn max_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.default_mqadmin_ext_impl
            .max_offset(broker_addr, message_queue, timeout_millis)
            .await
    }

    async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        check_store_time: i64,
    ) -> rocketmq_error::RocketMQResult<CheckRocksdbCqWriteResult> {
        self.default_mqadmin_ext_impl
            .check_rocksdb_cq_write_progress(broker_addr, topic, check_store_time)
            .await
    }

    async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        self.default_mqadmin_ext_impl.get_all_producer_info(broker_addr).await
    }
    #[allow(deprecated)]
    async fn delete_topic_in_broker_concurrent(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> AdminToolResult<BrokerOperatorResult> {
        self.default_mqadmin_ext_impl
            .delete_topic_in_broker_concurrent(addrs, topic)
            .await
    }

    async fn reset_offset_by_timestamp_old(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        self.default_mqadmin_ext_impl
            .reset_offset_by_timestamp_old(cluster_name, consumer_group, topic, timestamp, force)
            .await
    }
    #[allow(deprecated)]
    async fn reset_offset_new_concurrent(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult> {
        self.default_mqadmin_ext_impl
            .reset_offset_new_concurrent(group, topic, timestamp)
            .await
    }

    async fn query_consume_time_span(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<QueueTimeSpan>> {
        self.default_mqadmin_ext_impl
            .query_consume_time_span(topic, group)
            .await
    }

    async fn query_consume_time_span_concurrent(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>> {
        self.default_mqadmin_ext_impl
            .query_consume_time_span_concurrent(topic, group)
            .await
    }
    #[allow(deprecated)]
    async fn message_track_detail(&self, msg: MessageExt) -> rocketmq_error::RocketMQResult<Vec<MessageTrack>> {
        self.default_mqadmin_ext_impl.message_track_detail(msg).await
    }
    #[allow(deprecated)]
    async fn message_track_detail_concurrent(&self, msg: MessageExt) -> AdminToolResult<Vec<MessageTrack>> {
        self.default_mqadmin_ext_impl.message_track_detail_concurrent(msg).await
    }

    async fn view_broker_stats_data(
        &self,
        broker_addr: CheetahString,
        stats_name: CheetahString,
        stats_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerStatsData> {
        self.default_mqadmin_ext_impl
            .view_broker_stats_data(broker_addr, stats_name, stats_key)
            .await
    }

    async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: CheetahString,
        is_order: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumeStatsList> {
        self.default_mqadmin_ext_impl
            .fetch_consume_stats_in_broker(broker_addr, is_order, timeout_millis)
            .await
    }

    async fn get_all_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        self.default_mqadmin_ext_impl
            .get_all_subscription_group(broker_addr, timeout_millis)
            .await
    }

    async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        self.default_mqadmin_ext_impl
            .get_user_subscription_group(broker_addr, timeout_millis)
            .await
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
        self.default_mqadmin_ext_impl
            .query_consume_queue(broker_addr, topic, queue_id, index, count, consumer_group)
            .await
    }

    async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        topic_name: CheetahString,
        readable: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<GroupForbidden> {
        self.default_mqadmin_ext_impl
            .update_and_get_group_read_forbidden(broker_addr, group_name, topic_name, readable)
            .await
    }

    async fn query_message(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        self.default_mqadmin_ext_impl
            .query_message(cluster_name, topic, msg_id)
            .await
    }

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<HARuntimeInfo> {
        self.default_mqadmin_ext_impl.get_broker_ha_status(broker_addr).await
    }

    async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerReplicasInfo> {
        self.default_mqadmin_ext_impl
            .get_in_sync_state_data(controller_address, brokers)
            .await
    }

    async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<EpochEntryCache> {
        self.default_mqadmin_ext_impl.get_broker_epoch_cache(broker_addr).await
    }

    async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        self.default_mqadmin_ext_impl
            .elect_master(controller_addr, cluster_name, broker_name, broker_id)
            .await
    }

    async fn create_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_user_with_info(broker_addr, username, password)
            .await
    }

    async fn update_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_user_with_info(broker_addr, username, password)
            .await
    }

    async fn get_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<UserInfo>> {
        self.default_mqadmin_ext_impl.get_user(broker_addr, username).await
    }

    async fn list_users(
        &self,
        broker_addr: CheetahString,
        filter: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<UserInfo>> {
        self.default_mqadmin_ext_impl.list_users(broker_addr, filter).await
    }

    async fn create_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .create_acl_with_info(broker_addr, subject)
            .await
    }

    async fn update_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .update_acl_with_info(broker_addr, subject)
            .await
    }

    async fn get_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<AclInfo> {
        self.default_mqadmin_ext_impl.get_acl(broker_addr, subject).await
    }

    async fn list_acl(
        &self,
        broker_addr: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<AclInfo>> {
        self.default_mqadmin_ext_impl
            .list_acl(broker_addr, subject_filter, resource_filter)
            .await
    }

    async fn get_broker_lite_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetBrokerLiteInfoResponseBody> {
        self.default_mqadmin_ext_impl.get_broker_lite_info(broker_addr).await
    }

    async fn get_parent_topic_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetParentTopicInfoResponseBody> {
        self.default_mqadmin_ext_impl
            .get_parent_topic_info(broker_addr, topic)
            .await
    }

    async fn get_lite_topic_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        lite_topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteTopicInfoResponseBody> {
        self.default_mqadmin_ext_impl
            .get_lite_topic_info(broker_addr, parent_topic, lite_topic)
            .await
    }

    async fn get_lite_client_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteClientInfoResponseBody> {
        self.default_mqadmin_ext_impl
            .get_lite_client_info(broker_addr, parent_topic, group, client_id)
            .await
    }

    async fn get_lite_group_info(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
    ) -> rocketmq_error::RocketMQResult<GetLiteGroupInfoResponseBody> {
        self.default_mqadmin_ext_impl
            .get_lite_group_info(broker_addr, group, lite_topic, top_k)
            .await
    }

    async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_mqadmin_ext_impl
            .export_rocksdb_config_to_json(broker_addr, config_types)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::client_adapter::legacy::client_sdk::admin::mq_admin_ext_async::MQAdminExt;
    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;

    use super::DefaultMQAdminExt;
    use std::time::Duration;

    #[test]
    fn admin_ext_builders_initialize_owned_impl() {
        let default_admin = DefaultMQAdminExt::new();
        assert!(default_admin.default_mqadmin_ext_impl.has_inner());

        let timed_admin = DefaultMQAdminExt::with_timeout(Duration::from_secs(3));
        assert!(timed_admin.default_mqadmin_ext_impl.has_inner());

        let grouped_admin = DefaultMQAdminExt::with_admin_ext_group("dashboard-test");
        assert!(grouped_admin.default_mqadmin_ext_impl.has_inner());

        let grouped_timed_admin =
            DefaultMQAdminExt::with_admin_ext_group_and_timeout("dashboard-test", Duration::from_secs(3));
        assert!(grouped_timed_admin.default_mqadmin_ext_impl.has_inner());
    }

    #[tokio::test]
    async fn query_message_delegates_to_inner_impl() {
        let admin = DefaultMQAdminExt::new();

        let error = admin
            .query_message(
                CheetahString::default(),
                CheetahString::from("TopicTest"),
                CheetahString::from("msg-id"),
            )
            .await
            .expect_err("unstarted admin should return an error instead of panicking");

        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn get_kv_config_delegates_to_inner_impl() {
        let admin = DefaultMQAdminExt::new();

        let error = admin
            .get_kv_config(CheetahString::from("namespace"), CheetahString::from("key"))
            .await
            .expect_err("unstarted admin should return an error instead of panicking");

        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn get_acl_delegates_to_inner_impl() {
        let admin = DefaultMQAdminExt::new();

        let error = admin
            .get_acl(
                CheetahString::from("127.0.0.1:10911"),
                CheetahString::from("user:alice"),
            )
            .await
            .expect_err("unstarted admin should return an error instead of panicking");

        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn by_addr_cleanup_delegates_to_inner_impl() {
        let admin = DefaultMQAdminExt::new();
        let broker_addr = CheetahString::from("127.0.0.1:10911");

        let error = admin
            .clean_expired_consumer_queue_by_addr(broker_addr.clone())
            .await
            .expect_err("unstarted admin should return an error instead of panicking");
        assert!(matches!(error, RocketMQError::ClientNotStarted));

        let error = admin
            .delete_expired_commit_log_by_addr(broker_addr.clone())
            .await
            .expect_err("unstarted admin should return an error instead of panicking");
        assert!(matches!(error, RocketMQError::ClientNotStarted));

        let error = admin
            .clean_unused_topic_by_addr(broker_addr)
            .await
            .expect_err("unstarted admin should return an error instead of panicking");
        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }

    #[tokio::test]
    async fn export_pop_records_delegates_to_inner_impl() {
        let admin = DefaultMQAdminExt::new();

        let error = admin
            .export_pop_records(CheetahString::from("127.0.0.1:10911"), 3000)
            .await
            .expect_err("unstarted admin should return an error instead of a fixed not-implemented error");

        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }
}
