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

#![allow(dead_code)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use lazy_static::lazy_static;
use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::FAQUrl;
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
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::info;

use crate::admin::mq_admin_ext_async::MQAdminExt;
use crate::admin::mq_admin_ext_async_inner::MQAdminExtInnerImpl;
use crate::base::client_config::ClientConfig;
use crate::client_error::ClientErr;
use crate::client_error::MQClientError::MQClientErr;
use crate::common::admin_tool_result::AdminToolResult;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::implementation::mq_client_manager::MQClientManager;

lazy_static! {
    static ref SYSTEM_GROUP_SET: HashSet<CheetahString> = {
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
    };
}

const SOCKS_PROXY_JSON: &str = "socksProxyJson";
const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
pub struct DefaultMQAdminExtImpl {
    service_state: ServiceState,
    client_instance: Option<ArcMut<MQClientInstance>>,
    rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    timeout_millis: Duration,
    kv_namespace_to_delete_list: Vec<CheetahString>,
    client_config: ArcMut<ClientConfig>,
    admin_ext_group: CheetahString,
    inner: Option<ArcMut<DefaultMQAdminExtImpl>>,
}

impl DefaultMQAdminExtImpl {
    pub fn new(
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
        timeout_millis: Duration,
        client_config: ArcMut<ClientConfig>,
        admin_ext_group: CheetahString,
    ) -> Self {
        DefaultMQAdminExtImpl {
            service_state: ServiceState::CreateJust,
            client_instance: None,
            rpc_hook,
            timeout_millis,
            kv_namespace_to_delete_list: vec![CheetahString::from_static_str(
                NAMESPACE_ORDER_TOPIC_CONFIG,
            )],
            client_config,
            admin_ext_group,
            inner: None,
        }
    }

    pub fn set_inner(&mut self, inner: ArcMut<DefaultMQAdminExtImpl>) {
        self.inner = Some(inner);
    }
}

#[allow(unused_variables)]
#[allow(unused_mut)]
impl MQAdminExt for DefaultMQAdminExtImpl {
    async fn start(&mut self) -> crate::Result<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                self.client_config.change_instance_name_to_pid();
                if "{}".eq(&self.client_config.socks_proxy_config) {
                    self.client_config.socks_proxy_config = env::var(SOCKS_PROXY_JSON)
                        .unwrap_or_else(|_| "{}".to_string())
                        .into();
                }
                self.client_instance = Some(
                    MQClientManager::get_instance().get_or_create_mq_client_instance(
                        self.client_config.as_ref().clone(),
                        self.rpc_hook.clone(),
                    ),
                );

                let group = &self.admin_ext_group.clone();
                let register_ok = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .register_admin_ext(
                        group,
                        MQAdminExtInnerImpl {
                            inner: self.inner.as_ref().unwrap().clone(),
                        },
                    )
                    .await;
                if !register_ok {
                    self.service_state = ServiceState::StartFailed;
                    return Err(MQClientErr(ClientErr::new(format!(
                        "The adminExt group[{}] has created already, specified another name \
                         please.{}",
                        self.admin_ext_group,
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    ))));
                }
                let arc_mut = self.client_instance.clone().unwrap();
                self.client_instance
                    .as_mut()
                    .unwrap()
                    .start(arc_mut)
                    .await?;
                self.service_state = ServiceState::Running;
                info!("the adminExt [{}] start OK", self.admin_ext_group);
                Ok(())
            }
            ServiceState::Running | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                unimplemented!()
            }
        }
    }

    async fn shutdown(&mut self) {
        match self.service_state {
            ServiceState::CreateJust
            | ServiceState::ShutdownAlready
            | ServiceState::StartFailed => {
                // do nothing
            }
            ServiceState::Running => {
                let instance = self.client_instance.as_mut().unwrap();
                instance.unregister_admin_ext(&self.admin_ext_group).await;
                instance.shutdown().await;
                self.service_state = ServiceState::ShutdownAlready;
            }
        }
    }

    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> crate::Result<HashMap<CheetahString, CheetahString>> {
        todo!()
    }

    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn examine_broker_cluster_acl_version_info(
        &self,
        addr: CheetahString,
    ) -> crate::Result<CheetahString> {
        todo!()
    }

    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> crate::Result<SubscriptionGroupConfig> {
        todo!()
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> crate::Result<TopicStatsTable> {
        todo!()
    }

    async fn examine_topic_stats_concurrent(
        &self,
        topic: CheetahString,
    ) -> AdminToolResult<TopicStatsTable> {
        todo!()
    }

    async fn fetch_all_topic_list(&self) -> crate::Result<TopicList> {
        todo!()
    }

    async fn fetch_topics_by_cluster(
        &self,
        cluster_name: CheetahString,
    ) -> crate::Result<TopicList> {
        todo!()
    }

    async fn fetch_broker_runtime_stats(
        &self,
        broker_addr: CheetahString,
    ) -> crate::Result<KVTable> {
        todo!()
    }

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> crate::Result<ConsumeStats> {
        todo!()
    }

    async fn examine_broker_cluster_info(&self) -> crate::Result<ClusterInfo> {
        todo!()
    }

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> crate::Result<Option<TopicRouteData>> {
        self.client_instance
            .as_ref()
            .unwrap()
            .mq_client_api_impl
            .as_ref()
            .unwrap()
            .get_topic_route_info_from_name_server(&topic, self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> crate::Result<ConsumerConnection> {
        todo!()
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> crate::Result<ProducerConnection> {
        todo!()
    }

    async fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        todo!()
    }

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> crate::Result<i32> {
        todo!()
    }

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> crate::Result<i32> {
        todo!()
    }

    async fn put_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) {
        todo!()
    }

    async fn get_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> crate::Result<CheetahString> {
        todo!()
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> crate::Result<KVTable> {
        todo!()
    }

    async fn delete_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> crate::Result<HashMap<MessageQueue, u64>> {
        todo!()
    }

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> crate::Result<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        todo!()
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> crate::Result<GroupList> {
        todo!()
    }

    async fn query_topics_by_consumer(&self, group: CheetahString) -> crate::Result<TopicList> {
        todo!()
    }

    async fn query_topics_by_consumer_concurrent(
        &self,
        group: CheetahString,
    ) -> AdminToolResult<TopicList> {
        todo!()
    }

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> crate::Result<SubscriptionData> {
        todo!()
    }

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::Result<bool> {
        todo!()
    }

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::Result<bool> {
        todo!()
    }

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> crate::Result<bool> {
        todo!()
    }

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        metrics: Option<bool>,
    ) -> crate::Result<ConsumerRunningInfo> {
        todo!()
    }

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::Result<ConsumeMessageDirectlyResult> {
        todo!()
    }

    async fn consume_message_directly_ext(
        &self,
        cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::Result<ConsumeMessageDirectlyResult> {
        todo!()
    }

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get_cluster_list(&self, topic: String) -> crate::Result<HashSet<CheetahString>> {
        todo!()
    }

    async fn get_topic_cluster_list(&self, topic: String) -> crate::Result<HashSet<CheetahString>> {
        todo!()
    }

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> crate::Result<TopicConfigSerializeWrapper> {
        todo!()
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> crate::Result<TopicConfigSerializeWrapper> {
        todo!()
    }

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        name_servers: Vec<CheetahString>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> crate::Result<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        todo!()
    }

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> crate::Result<bool> {
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
    ) -> crate::Result<()> {
        let mut mq_client_api = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl();
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
    ) -> crate::Result<()> {
        todo!()
    }

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> crate::Result<TopicConfig> {
        todo!()
    }

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> crate::Result<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        todo!()
    }

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
    ) -> crate::Result<CheetahString> {
        todo!()
    }

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> crate::Result<CheetahString> {
        todo!()
    }

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> crate::Result<()> {
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
    ) -> crate::Result<()> {
        todo!()
    }

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> crate::Result<()> {
        todo!()
    }
}
