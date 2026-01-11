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

use cheetah_string::CheetahString;
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
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
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
use crate::common::admin_tool_result::AdminToolResult;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::implementation::mq_client_manager::MQClientManager;

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
pub struct DefaultMQAdminExtImpl {
    service_state: ServiceState,
    client_instance: Option<ArcMut<MQClientInstance>>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    timeout_millis: Duration,
    kv_namespace_to_delete_list: Vec<CheetahString>,
    client_config: ArcMut<ClientConfig>,
    admin_ext_group: CheetahString,
    inner: Option<ArcMut<DefaultMQAdminExtImpl>>,
}

impl DefaultMQAdminExtImpl {
    pub fn new(
        rpc_hook: Option<Arc<dyn RPCHook>>,
        timeout_millis: Duration,
        client_config: ArcMut<ClientConfig>,
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
                        .get_or_create_mq_client_instance(self.client_config.as_ref().clone(), self.rpc_hook.clone()),
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
                    return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                        "The adminExt group[{}] has created already, specified another name please.{}",
                        self.admin_ext_group,
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                let arc_mut = self.client_instance.clone().unwrap();
                self.client_instance.as_mut().unwrap().start(arc_mut).await?;
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
            ServiceState::CreateJust | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
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
        todo!()
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_broker_cluster_info(self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_name_server_address_list()
            .to_vec()
    }

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .wipe_write_perm_of_broker(namesrv_addr, broker_name, self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .add_write_perm_of_broker(namesrv_addr, broker_name, self.timeout_millis.as_millis() as u64)
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
        todo!()
    }

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .put_kvconfig_value(namespace, key, value, self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn delete_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .delete_kvconfig_value(namespace, key, self.timeout_millis.as_millis() as u64)
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
        let cluster_info = self.examine_broker_cluster_info().await?;
        let topic_route_data = self.examine_topic_route_info(topic.into()).await?.unwrap();
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
        todo!()
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        todo!()
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
        name_servers: Option<Vec<CheetahString>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .update_name_server_config(properties, name_servers, self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        Ok(self
            .client_instance
            .as_ref()
            .unwrap()
            .mq_client_api_impl
            .as_ref()
            .unwrap()
            .get_name_server_config(Some(name_servers), self.timeout_millis)
            .await?
            .unwrap_or_default())
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
        let mut mq_client_api = self.client_instance.as_ref().unwrap().get_mq_client_api_impl();
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
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()
                .get_controller_metadata(controller_addr, self.timeout_millis.as_millis() as u64)
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
        todo!()
    }

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        todo!()
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
        todo!()
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
        todo!()
    }

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn delete_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
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
        todo!()
    }

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }
}
