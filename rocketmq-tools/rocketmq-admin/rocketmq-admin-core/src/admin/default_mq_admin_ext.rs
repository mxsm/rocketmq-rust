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

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::common::admin_tool_result::AdminToolResult;
use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
#[allow(deprecated)]
use rocketmq_common::common::tools::broker_operator_result::BrokerOperatorResult;
#[allow(deprecated)]
use rocketmq_common::common::tools::message_track::MessageTrack;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
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
use rocketmq_rust::ArcMut;

const ADMIN_EXT_GROUP: &str = "admin_ext_group";

pub struct DefaultMQAdminExt {
    client_config: ArcMut<ClientConfig>,
    admin_ext_group: CheetahString,
    create_topic_key: CheetahString,
    timeout_millis: Duration,
    default_mqadmin_ext_impl: ArcMut<DefaultMQAdminExtImpl>,
}

impl DefaultMQAdminExt {
    pub(crate) fn set_namesrv_addr(&mut self, name_serv_addr: &str) {
        self.client_config.set_namesrv_addr(name_serv_addr.into());
    }
}

impl DefaultMQAdminExt {
    pub fn new() -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ArcMut::new(ClientConfig::new());
        let mut default_mqadmin_ext_impl = ArcMut::new(DefaultMQAdminExtImpl::new(
            None,
            Duration::from_millis(5000),
            client_config.clone(),
            admin_ext_group.clone(),
        ));
        let inner = default_mqadmin_ext_impl.clone();
        default_mqadmin_ext_impl.set_inner(inner);
        Self {
            client_config,
            default_mqadmin_ext_impl,
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis: Duration::from_millis(5000),
        }
    }

    pub fn with_timeout(timeout_millis: Duration) -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ArcMut::new(ClientConfig::new());
        Self {
            client_config: client_config.clone(),
            default_mqadmin_ext_impl: ArcMut::new(DefaultMQAdminExtImpl::new(
                None,
                timeout_millis,
                client_config,
                admin_ext_group.clone(),
            )),
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis,
        }
    }

    pub fn with_rpc_hook(rpc_hook: Arc<dyn RPCHook>) -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ArcMut::new(ClientConfig::new());
        Self {
            client_config: client_config.clone(),
            default_mqadmin_ext_impl: ArcMut::new(DefaultMQAdminExtImpl::new(
                Some(rpc_hook),
                Duration::from_millis(5000),
                client_config,
                admin_ext_group.clone(),
            )),
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis: Duration::from_millis(5000),
        }
    }

    pub fn with_rpc_hook_and_timeout(rpc_hook: Arc<dyn RPCHook>, timeout_millis: Duration) -> Self {
        let admin_ext_group = CheetahString::from_static_str(ADMIN_EXT_GROUP);
        let client_config = ArcMut::new(ClientConfig::new());
        Self {
            client_config: client_config.clone(),
            default_mqadmin_ext_impl: ArcMut::new(DefaultMQAdminExtImpl::new(
                Some(rpc_hook),
                timeout_millis,
                client_config,
                admin_ext_group.clone(),
            )),
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis,
        }
    }

    pub fn with_admin_ext_group(admin_ext_group: impl Into<CheetahString>) -> Self {
        let admin_ext_group = admin_ext_group.into();
        let client_config = ArcMut::new(ClientConfig::new());
        Self {
            client_config: client_config.clone(),
            default_mqadmin_ext_impl: ArcMut::new(DefaultMQAdminExtImpl::new(
                None,
                Duration::from_millis(5000),
                client_config,
                admin_ext_group.clone(),
            )),
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis: Duration::from_millis(5000),
        }
    }

    pub fn with_admin_ext_group_and_timeout(
        admin_ext_group: impl Into<CheetahString>,
        timeout_millis: Duration,
    ) -> Self {
        let admin_ext_group = admin_ext_group.into();
        let client_config = ArcMut::new(ClientConfig::new());
        Self {
            client_config: client_config.clone(),
            default_mqadmin_ext_impl: ArcMut::new(DefaultMQAdminExtImpl::new(
                None,
                timeout_millis,
                client_config,
                admin_ext_group.clone(),
            )),
            admin_ext_group,
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            timeout_millis,
        }
    }

    #[inline]
    pub fn client_config(&self) -> &ArcMut<ClientConfig> {
        &self.client_config
    }

    #[inline]
    pub fn client_config_mut(&mut self) -> &mut ArcMut<ClientConfig> {
        &mut self.client_config
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
        MQAdminExt::start(self.default_mqadmin_ext_impl.as_mut()).await
    }

    async fn shutdown(&mut self) {
        MQAdminExt::shutdown(self.default_mqadmin_ext_impl.as_mut()).await
    }

    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, CheetahString>> {
        todo!()
    }

    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn examine_broker_cluster_acl_version_info(
        &self,
        addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        todo!()
    }

    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfig> {
        todo!()
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        todo!()
    }

    async fn examine_topic_stats_concurrent(&self, topic: CheetahString) -> AdminToolResult<TopicStatsTable> {
        todo!()
    }

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList> {
        todo!()
    }

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        todo!()
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
        todo!()
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
        todo!()
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        todo!()
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
        todo!()
    }

    async fn get_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        todo!()
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        todo!()
    }

    async fn delete_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
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
        todo!()
    }

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        todo!()
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList> {
        todo!()
    }

    async fn query_topics_by_consumer(&self, group: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        todo!()
    }

    async fn query_topics_by_consumer_concurrent(&self, group: CheetahString) -> AdminToolResult<TopicList> {
        todo!()
    }

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionData> {
        todo!()
    }

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        todo!()
    }

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        todo!()
    }

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        todo!()
    }

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        metrics: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo> {
        todo!()
    }

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        todo!()
    }

    async fn consume_message_directly_ext(
        &self,
        cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        todo!()
    }

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn get_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        todo!()
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
        todo!()
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

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        todo!()
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
            self.default_mqadmin_ext_impl.as_ref(),
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
        todo!()
    }

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        todo!()
    }

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
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
        todo!()
    }

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
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
        todo!()
    }

    async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        todo!()
    }

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        todo!()
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
        todo!()
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
        _addr: CheetahString,
        _topic: CheetahString,
        _queue_num: i32,
        _topic_sys_flag: i32,
        _read_queue_nums: i32,
        _write_queue_nums: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("create_lite_pull_topic not implemented yet")
    }

    async fn update_lite_pull_topic(
        &self,
        _addr: CheetahString,
        _topic: CheetahString,
        _read_queue_nums: i32,
        _write_queue_nums: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("update_lite_pull_topic not implemented yet")
    }

    async fn get_lite_pull_topic(
        &self,
        _addr: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        unimplemented!("get_lite_pull_topic not implemented yet")
    }

    async fn delete_lite_pull_topic(
        &self,
        _addr: CheetahString,
        _cluster_name: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("delete_lite_pull_topic not implemented yet")
    }

    async fn query_lite_pull_topic_list(&self, _addr: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        unimplemented!("query_lite_pull_topic_list not implemented yet")
    }

    async fn query_lite_pull_topic_by_cluster(
        &self,
        _cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        unimplemented!("query_lite_pull_topic_by_cluster not implemented yet")
    }

    async fn query_lite_pull_subscription_list(
        &self,
        _addr: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GroupList> {
        unimplemented!("query_lite_pull_subscription_list not implemented yet")
    }

    async fn update_lite_pull_consumer_offset(
        &self,
        _addr: CheetahString,
        _topic: CheetahString,
        _group: CheetahString,
        _queue_id: i32,
        _offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("update_lite_pull_consumer_offset not implemented yet")
    }

    async fn examine_consume_stats_with_queue(
        &self,
        _consumer_group: CheetahString,
        _topic: Option<CheetahString>,
        _queue_id: Option<i32>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        unimplemented!("examine_consume_stats_with_queue not implemented yet")
    }

    async fn examine_consume_stats_concurrent(
        &self,
        _consumer_group: CheetahString,
        _topic: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        unimplemented!("examine_consume_stats_concurrent not implemented yet")
    }

    async fn examine_consume_stats_concurrent_with_cluster(
        &self,
        _consumer_group: CheetahString,
        _topic: Option<CheetahString>,
        _cluster_name: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        unimplemented!("examine_consume_stats_concurrent_with_cluster not implemented yet")
    }

    async fn export_rocksdb_consumer_offset_to_json(
        &self,
        _broker_addr: CheetahString,
        _file_path: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("export_rocksdb_consumer_offset_to_json not implemented yet")
    }

    async fn export_rocksdb_consumer_offset_from_memory(
        &self,
        _broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        unimplemented!("export_rocksdb_consumer_offset_from_memory not implemented yet")
    }

    async fn sync_broker_member_group(
        &self,
        _controller_addr: CheetahString,
        _cluster_name: CheetahString,
        _broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("sync_broker_member_group not implemented yet")
    }

    async fn get_topic_config_by_topic_name(
        &self,
        _broker_addr: CheetahString,
        _topic_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        unimplemented!("get_topic_config_by_topic_name not implemented yet")
    }

    async fn notify_min_broker_id_changed(
        &self,
        _cluster_name: CheetahString,
        _broker_name: CheetahString,
        _min_broker_id: u64,
        _min_broker_addr: CheetahString,
        _offline_broker_addr: Option<CheetahString>,
        _ha_broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("notify_min_broker_id_changed not implemented yet")
    }

    async fn get_topic_stats_info(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        unimplemented!("get_topic_stats_info not implemented yet")
    }

    async fn query_broker_has_topic(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        unimplemented!("query_broker_has_topic not implemented yet")
    }

    async fn get_system_topic_list_from_broker(
        &self,
        _broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        unimplemented!("get_system_topic_list_from_broker not implemented yet")
    }

    async fn examine_topic_route_info_with_timeout(
        &self,
        _topic: CheetahString,
        _timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        unimplemented!("examine_topic_route_info_with_timeout not implemented yet")
    }

    async fn export_pop_records(
        &self,
        _broker_addr: CheetahString,
        _timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("export_pop_records not implemented yet")
    }

    async fn switch_timer_engine(
        &self,
        _broker_addr: CheetahString,
        _des_timer_engine: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("switch_timer_engine not implemented yet")
    }

    async fn trigger_lite_dispatch(
        &self,
        _broker_addr: CheetahString,
        _group: CheetahString,
        _client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("trigger_lite_dispatch not implemented yet")
    }

    async fn search_offset(
        &self,
        _broker_addr: CheetahString,
        _topic_name: CheetahString,
        _queue_id: i32,
        _timestamp: u64,
        _timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<u64> {
        unimplemented!("search_offset not implemented yet (deprecated)")
    }

    async fn check_rocksdb_cq_write_progress(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        unimplemented!("check_rocksdb_cq_write_progress not implemented yet")
    }

    async fn get_all_producer_info(
        &self,
        _broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        unimplemented!("get_all_producer_info not implemented yet")
    }
    #[allow(deprecated)]
    async fn delete_topic_in_broker_concurrent(
        &self,
        _addrs: HashSet<CheetahString>,
        _topic: CheetahString,
    ) -> AdminToolResult<BrokerOperatorResult> {
        unimplemented!("delete_topic_in_broker_concurrent not implemented yet")
    }

    async fn reset_offset_by_timestamp_old(
        &self,
        _consumer_group: CheetahString,
        _topic: CheetahString,
        _timestamp: u64,
        _force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        unimplemented!("reset_offset_by_timestamp_old not implemented yet")
    }
    #[allow(deprecated)]
    async fn reset_offset_new_concurrent(
        &self,
        _group: CheetahString,
        _topic: CheetahString,
        _timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult> {
        unimplemented!("reset_offset_new_concurrent not implemented yet")
    }

    async fn query_consume_time_span(
        &self,
        _topic: CheetahString,
        _group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<QueueTimeSpan>> {
        unimplemented!("query_consume_time_span not implemented yet")
    }

    async fn query_consume_time_span_concurrent(
        &self,
        _topic: CheetahString,
        _group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>> {
        unimplemented!("query_consume_time_span_concurrent not implemented yet")
    }
    #[allow(deprecated)]
    async fn message_track_detail(&self, _msg_id: CheetahString) -> rocketmq_error::RocketMQResult<Vec<MessageTrack>> {
        unimplemented!("message_track_detail not implemented yet")
    }
    #[allow(deprecated)]
    async fn message_track_detail_concurrent(&self, _msg_id: CheetahString) -> AdminToolResult<Vec<MessageTrack>> {
        unimplemented!("message_track_detail_concurrent not implemented yet")
    }

    async fn view_broker_stats_data(
        &self,
        _broker_addr: CheetahString,
        _stats_name: CheetahString,
        _stats_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerStatsData> {
        unimplemented!("view_broker_stats_data not implemented yet")
    }

    async fn fetch_consume_stats_in_broker(
        &self,
        _broker_addr: CheetahString,
        _is_order: bool,
        _timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumeStatsList> {
        unimplemented!("fetch_consume_stats_in_broker not implemented yet")
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
        _broker_addr: CheetahString,
        _topic: CheetahString,
        _queue_id: i32,
        _index: u64,
        _count: i32,
        _consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<QueryConsumeQueueResponseBody> {
        unimplemented!("query_consume_queue not implemented yet")
    }

    async fn update_and_get_group_read_forbidden(
        &self,
        _broker_addr: CheetahString,
        _group_name: CheetahString,
        _topic_name: CheetahString,
        _readable: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<GroupForbidden> {
        unimplemented!("update_and_get_group_read_forbidden not implemented yet")
    }

    async fn query_message(
        &self,
        _cluster_name: CheetahString,
        _topic: CheetahString,
        _msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        unimplemented!("query_message not implemented yet")
    }

    async fn get_broker_ha_status(&self, _broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<HARuntimeInfo> {
        unimplemented!("get_broker_ha_status not implemented yet")
    }

    async fn get_in_sync_state_data(
        &self,
        _controller_address: CheetahString,
        _brokers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerReplicasInfo> {
        unimplemented!("get_in_sync_state_data not implemented yet")
    }

    async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<EpochEntryCache> {
        self.default_mqadmin_ext_impl.get_broker_epoch_cache(broker_addr).await
    }

    async fn elect_master(
        &self,
        _controller_addr: CheetahString,
        _cluster_name: CheetahString,
        _broker_name: CheetahString,
        _broker_id: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        unimplemented!("elect_master not implemented yet")
    }

    async fn create_user_with_info(
        &self,
        _broker_addr: CheetahString,
        _username: CheetahString,
        _password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("create_user_with_info not implemented yet")
    }

    async fn update_user_with_info(
        &self,
        _broker_addr: CheetahString,
        _username: CheetahString,
        _password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("update_user_with_info not implemented yet")
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
        _broker_addr: CheetahString,
        _subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("create_acl_with_info not implemented yet")
    }

    async fn update_acl_with_info(
        &self,
        _broker_addr: CheetahString,
        _subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("update_acl_with_info not implemented yet")
    }

    async fn get_acl(
        &self,
        _broker_addr: CheetahString,
        _subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<AclInfo> {
        unimplemented!("get_acl not implemented yet")
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
        _broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetBrokerLiteInfoResponseBody> {
        unimplemented!("get_broker_lite_info not implemented yet")
    }

    async fn get_parent_topic_info(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetParentTopicInfoResponseBody> {
        unimplemented!("get_parent_topic_info not implemented yet")
    }

    async fn get_lite_topic_info(
        &self,
        _broker_addr: CheetahString,
        _parent_topic: CheetahString,
        _lite_topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteTopicInfoResponseBody> {
        unimplemented!("get_lite_topic_info not implemented yet")
    }

    async fn get_lite_client_info(
        &self,
        _broker_addr: CheetahString,
        _parent_topic: CheetahString,
        _group: CheetahString,
        _client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteClientInfoResponseBody> {
        unimplemented!("get_lite_client_info not implemented yet")
    }

    async fn get_lite_group_info(
        &self,
        _broker_addr: CheetahString,
        _group: CheetahString,
        _lite_topic: CheetahString,
        _top_k: i32,
    ) -> rocketmq_error::RocketMQResult<GetLiteGroupInfoResponseBody> {
        unimplemented!("get_lite_group_info not implemented yet")
    }

    async fn export_rocksdb_config_to_json(
        &self,
        _broker_addr: CheetahString,
        _config_types: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("export_rocksdb_config_to_json not implemented yet")
    }
}
