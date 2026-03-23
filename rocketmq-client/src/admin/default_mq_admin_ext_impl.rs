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
use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
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
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::protocol::admin::topic_offset::TopicOffset;
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
use rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_remoting::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_remoting::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader;
use rocketmq_remoting::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_remoting::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::info;

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

    pub fn has_inner(&self) -> bool {
        self.inner.is_some()
    }

    pub async fn create_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let subject = acl_info
            .subject
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("ACL subject is required".into()))?;

        if let Some(ref policies) = acl_info.policies {
            for policy in policies {
                if let Some(ref entries) = policy.entries {
                    for entry in entries {
                        let resources: Vec<CheetahString> =
                            entry.resource.as_ref().map(|r| vec![r.clone()]).unwrap_or_default();

                        let actions: Vec<CheetahString> = entry
                            .actions
                            .as_ref()
                            .map(|a| a.split(',').map(|s| CheetahString::from(s.trim())).collect())
                            .unwrap_or_default();

                        let source_ips: Vec<CheetahString> = entry.source_ips.clone().unwrap_or_default();

                        let decision: CheetahString = entry.decision.clone().unwrap_or_default();

                        self.create_acl(
                            broker_addr.clone(),
                            subject.clone(),
                            resources,
                            actions,
                            source_ips,
                            decision,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn update_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let subject = acl_info
            .subject
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("ACL subject is required".into()))?;

        if let Some(ref policies) = acl_info.policies {
            for policy in policies {
                if let Some(ref entries) = policy.entries {
                    for entry in entries {
                        let resources: Vec<CheetahString> =
                            entry.resource.as_ref().map(|r| vec![r.clone()]).unwrap_or_default();

                        let actions: Vec<CheetahString> = entry
                            .actions
                            .as_ref()
                            .map(|a| a.split(',').map(|s| CheetahString::from(s.trim())).collect())
                            .unwrap_or_default();

                        let source_ips: Vec<CheetahString> = entry.source_ips.clone().unwrap_or_default();

                        let decision: CheetahString = entry.decision.clone().unwrap_or_default();

                        self.update_acl(
                            broker_addr.clone(),
                            subject.clone(),
                            resources,
                            actions,
                            source_ips,
                            decision,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
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
            fn on_exception(&mut self, _e: Box<dyn std::error::Error + Send>) {}
        }

        let api_impl = self.client_instance.as_ref().unwrap().get_mq_client_api_impl();

        let mut result = MQClientAPIImpl::pull_message(
            api_impl,
            CheetahString::from(broker_addr),
            request_header,
            timeout_millis,
            CommunicationMode::Sync,
            NoopPullCallback,
        )
        .await?
        .ok_or_else(|| rocketmq_error::RocketMQError::Internal("pull_message returned None in sync mode".into()))?;

        if result.pull_result.pull_status == PullStatus::Found {
            if let Some(mut message_binary) = result.message_binary.take() {
                let msg_vec = message_decoder::decodes_batch(&mut message_binary, true, true);
                result.pull_result.msg_found_list = Some(msg_vec.into_iter().map(ArcMut::new).collect());
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
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::Internal(format!("Topic route not found for: {}", route_topic))
            })?;

        let mut message_list: Vec<MessageExt> = Vec::new();
        let mut index_last_update_timestamp: u64 = 0;

        let api_impl = self.client_instance.as_ref().unwrap().get_mq_client_api_impl();
        let timeout = self.timeout_millis.as_millis() as u64;

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
                    if response_header.index_last_update_timestamp as u64 > index_last_update_timestamp {
                        index_last_update_timestamp = response_header.index_last_update_timestamp as u64;
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
        let validator_input = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>();
        Validators::check_broker_config(&validator_input)?;

        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()
                .update_broker_config(&broker_addr, properties, self.timeout_millis.as_millis() as u64)
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
                .get_mq_client_api_impl()
                .get_broker_config(&broker_addr, self.timeout_millis.as_millis() as u64)
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
            topic_filter_type: config.topic_filter_type.to_string().into(),
            topic_sys_flag: Some(config.topic_sys_flag as i32),
            order: config.order,
            attributes,
            force: Some(false),
            topic_request_header: None,
        };

        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .update_or_create_topic(&addr, request_header, self.timeout_millis.as_millis() as u64)
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
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()
            .create_subscription_group(&addr, &config, self.timeout_millis.as_millis() as u64)
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_subscription_group_config(&addr, group, self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        let timeout = self.timeout_millis.as_millis() as u64;
        let request_header = GetTopicStatsInfoRequestHeader {
            topic: topic.clone(),
            topic_request_header: None,
        };
        if let Some(addr) = broker_addr {
            return self
                .client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .get_topic_stats_info(&addr, request_header, timeout)
                .await;
        }

        let topic_route = self.examine_topic_route_info(topic).await?;
        let mut result = TopicStatsTable::new();
        if let Some(route_data) = topic_route {
            for broker_data in &route_data.broker_datas {
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let stats = self
                        .client_instance
                        .as_ref()
                        .unwrap()
                        .get_mq_client_api_impl()
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_all_topic_list_from_name_server(self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        todo!()
    }

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_broker_runtime_info(&broker_addr, self.timeout_millis.as_millis() as u64)
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
        let timeout = timeout_millis.unwrap_or(self.timeout_millis.as_millis() as u64);
        let topic_str = topic.clone().unwrap_or_default();

        if let Some(addr) = broker_addr {
            let request_header = GetConsumeStatsRequestHeader {
                consumer_group,
                topic: topic_str,
                topic_request_header: None,
            };
            return self
                .client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .get_consume_stats(&addr, request_header, timeout)
                .await;
        }

        let retry_topic: CheetahString = rocketmq_common::common::mix_all::get_retry_topic(&consumer_group).into();
        let topic_route = self
            .client_instance
            .as_ref()
            .unwrap()
            .mq_client_api_impl
            .as_ref()
            .unwrap()
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
                        .client_instance
                        .as_ref()
                        .unwrap()
                        .get_mq_client_api_impl()
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .check_rocksdb_cq_write_progress(
                &broker_addr,
                topic,
                check_store_time,
                self.timeout_millis.as_millis() as u64,
            )
            .await
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
        let mut result = ConsumerConnection::new();
        let timeout = self.timeout_millis.as_millis() as u64;

        let selected_addr = if let Some(broker_addr) = broker_addr {
            Some(broker_addr)
        } else {
            let topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_group.as_str()));
            let topic_route_data = self
                .client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
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
                .client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
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
        let timeout = self.timeout_millis.as_millis() as u64;

        if let Some(topic_route_data) = self.examine_topic_route_info(topic).await? {
            let brokers = &topic_route_data.broker_datas;
            let selected_addr = brokers
                .choose(&mut rand::rng())
                .and_then(|broker_data| broker_data.select_broker_addr());
            if let Some(addr) = selected_addr {
                result = self
                    .client_instance
                    .as_ref()
                    .unwrap()
                    .get_mq_client_api_impl()
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_all_producer_info(broker_addr.as_str(), self.timeout_millis.as_millis() as u64)
            .await
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
        Ok(self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_kvconfig_value(namespace, key, self.timeout_millis.as_millis() as u64)
            .await?
            .unwrap_or_default())
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        todo!()
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
        let api = self.client_instance.as_ref().unwrap().get_mq_client_api_impl();
        let timeout = self.timeout_millis.as_millis() as u64;
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
        let api = self.client_instance.as_ref().unwrap().get_mq_client_api_impl();
        let timeout = self.timeout_millis.as_millis() as u64;
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .delete_subscription_group(
                &addr,
                group_name,
                remove_offset.unwrap_or(false),
                self.timeout_millis.as_millis() as u64,
            )
            .await
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
        let topic_route = self.examine_topic_route_info(topic.clone()).await?;
        let mut offset_table = HashMap::new();
        let timeout = self.timeout_millis.as_millis() as u64;

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
                        offset: None,
                        timestamp: timestamp as i64,
                        is_force,
                        topic_request_header: None,
                    };
                    let offsets = self
                        .client_instance
                        .as_ref()
                        .unwrap()
                        .get_mq_client_api_impl()
                        .invoke_broker_to_reset_offset(master_addr, request_header, timeout)
                        .await?;
                    offset_table.extend(offsets.into_iter().map(|(mq, offset)| (mq, offset as u64)));
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
        todo!()
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        let topic_route_data = self.examine_topic_route_info(topic.clone()).await?;
        if let Some(route_data) = topic_route_data {
            if !route_data.broker_datas.is_empty() {
                if let Some(addr) = route_data.broker_datas[0].select_broker_addr() {
                    let result = self
                        .client_instance
                        .as_ref()
                        .unwrap()
                        .get_mq_client_api_impl()
                        .invoke_broker_to_get_consumer_status(
                            addr.as_str(),
                            topic,
                            group,
                            client_addr,
                            self.timeout_millis.as_millis() as u64,
                        )
                        .await?;
                    let converted: HashMap<CheetahString, HashMap<MessageQueue, u64>> = result
                        .into_iter()
                        .map(|(k, v)| {
                            let inner: HashMap<MessageQueue, u64> =
                                v.into_iter().map(|(mq, off)| (mq, off as u64)).collect();
                            (k, inner)
                        })
                        .collect();
                    return Ok(converted);
                }
            }
        }
        Ok(HashMap::new())
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if is_cluster {
            return self
                .client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .put_kvconfig_value(
                    CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                    key,
                    value,
                    self.timeout_millis.as_millis() as u64,
                )
                .await;
        }

        let existing = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                key.clone(),
                self.timeout_millis.as_millis() as u64,
            )
            .await?
            .unwrap_or_default();

        let merged_order_conf = merge_order_conf_entries(existing.as_str(), value.as_str());

        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .put_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                key,
                merged_order_conf.into(),
                self.timeout_millis.as_millis() as u64,
            )
            .await
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList> {
        let topic_route = self
            .client_instance
            .as_ref()
            .unwrap()
            .mq_client_api_impl
            .as_ref()
            .unwrap()
            .get_topic_route_info_from_name_server(&topic, self.timeout_millis.as_millis() as u64)
            .await?;

        if let Some(route_data) = topic_route {
            for bd in &route_data.broker_datas {
                if let Some(master_addr) = bd.broker_addrs().get(&rocketmq_common::common::mix_all::MASTER_ID) {
                    let request_header = QueryTopicConsumeByWhoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    };
                    return self
                        .client_instance
                        .as_ref()
                        .unwrap()
                        .get_mq_client_api_impl()
                        .query_topic_consume_by_who(master_addr, request_header, self.timeout_millis.as_millis() as u64)
                        .await;
                }
            }
        }

        Ok(GroupList::default())
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
            .get_mq_client_api_impl()
            .get_consumer_running_info(
                &broker_addr,
                consumer_group,
                client_id,
                jstack,
                self.timeout_millis.as_millis() as u64,
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
            .get_mq_client_api_impl()
            .consume_message_directly(
                &client_addr,
                request_header,
                &message,
                self.timeout_millis.as_millis() as u64,
            )
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_all_topic_config(&broker_addr, timeout_millis)
            .await
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        let mut topic_config_wrapper = self.get_all_topic_config(broker_addr, timeout_millis).await?;

        if let Some(ref mut topic_table) = topic_config_wrapper.topic_config_table_mut() {
            topic_table.retain(|topic_name, topic_config| {
                if TopicValidator::is_system_topic(topic_name.as_str()) {
                    return false;
                }
                if !special_topic
                    && (topic_name.starts_with(RETRY_GROUP_TOPIC_PREFIX)
                        || topic_name.starts_with(DLQ_GROUP_TOPIC_PREFIX))
                {
                    return false;
                }
                if !PermName::is_valid(topic_config.perm) {
                    return false;
                }
                true
            });
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
        let request_header = GetTopicConfigRequestHeader {
            topic,
            topic_request_header: None,
        };
        let mapping: TopicConfigAndQueueMapping = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_topic_config(&addr, request_header, self.timeout_millis.as_millis() as u64)
            .await?;

        Ok(mapping.topic_config)
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
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()
                .reset_master_flush_offset(&broker_addr, master_flush_offset as i64)
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
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;

            for controller_addr in controller_servers {
                match mq_client_api
                    .get_controller_config(controller_addr.clone(), timeout_millis)
                    .await
                {
                    Ok(config) => {
                        result.insert(controller_addr, config);
                    }
                    Err(e) => {
                        eprintln!("Failed to get config from controller {}: {}", controller_addr, e);
                    }
                }
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .update_cold_data_flow_ctr_group_config(broker_addr, properties, self.timeout_millis.as_millis() as u64)
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
        let user_info = UserInfo {
            username: Some(username),
            user_type: Some(user_type),
            password: Some(password),
            user_status: None,
        };

        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;
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
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;
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
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;
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
        if let Some(ref client_instance) = self.client_instance {
            let mq_client_api = client_instance.get_mq_client_api_impl();
            mq_client_api
                .delete_acl(broker_addr, subject, resource, self.timeout_millis.as_millis() as u64)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
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
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let mut route_topic = topic.clone();
        if !topic.is_empty()
            && (mix_all::is_lmq(Some(topic.as_str()))
                || topic.as_str() == format!("{}wheel_timer", TopicValidator::SYSTEM_TOPIC_PREFIX))
            && cluster_name.as_ref().is_some_and(|name| !name.is_empty())
        {
            route_topic = cluster_name.unwrap();
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
                                timestamp as i64,
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .view_broker_stats_data(&broker_addr, request_header, self.timeout_millis.as_millis() as u64)
            .await
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_all_subscription_group_config(&broker_addr, timeout_millis)
            .await
    }

    async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        let subscription_group_wrapper = self.get_all_subscription_group(broker_addr, timeout_millis).await?;

        let system_group_set = get_system_group_set();
        let table = subscription_group_wrapper.get_subscription_group_table();
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .query_consume_queue(
                &broker_addr,
                topic,
                queue_id,
                index as i64,
                count,
                consumer_group,
                self.timeout_millis.as_millis() as u64,
            )
            .await
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
            .get_mq_client_api_impl()
            .view_message(&broker_addr, request_header, self.timeout_millis.as_millis() as u64)
            .await
    }

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<HARuntimeInfo> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()
                .get_broker_ha_status(broker_addr, self.timeout_millis.as_millis() as u64)
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
                .get_mq_client_api_impl()
                .get_in_sync_state_data(controller_address, brokers, self.timeout_millis.as_millis() as u64)
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
                .get_mq_client_api_impl()
                .get_broker_epoch_cache(broker_addr, self.timeout_millis.as_millis() as u64)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
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
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;
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
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;
            let result = mq_client_api.list_users(broker_addr, filter, timeout_millis).await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
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
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl();
            let timeout_millis = self.timeout_millis.as_millis() as u64;
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
                .get_mq_client_api_impl()
                .get_broker_lite_info(&broker_addr, self.timeout_millis.as_millis() as u64)
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
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_parent_topic_info(&broker_addr, topic, self.timeout_millis.as_millis() as u64)
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
                .get_mq_client_api_impl()
                .get_lite_topic_info(
                    &broker_addr,
                    &parent_topic,
                    &lite_topic,
                    self.timeout_millis.as_millis() as u64,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
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
        broker_addr: CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
    ) -> rocketmq_error::RocketMQResult<GetLiteGroupInfoResponseBody> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_lite_group_info(
                &broker_addr,
                group,
                lite_topic,
                top_k,
                self.timeout_millis.as_millis() as u64,
            )
            .await
    }

    async fn export_rocksdb_config_to_json(
        &self,
        _broker_addr: CheetahString,
        _config_types: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        unimplemented!("export_rocksdb_config_to_json not implemented yet")
    }

    async fn search_offset(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        timestamp: u64,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<u64> {
        let mq = MessageQueue::from_parts(&topic_name, "", queue_id);
        let offset = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .search_offset_by_timestamp(
                broker_addr.as_str(),
                &mq,
                timestamp as i64,
                rocketmq_common::common::boundary_type::BoundaryType::Lower,
                timeout_millis,
            )
            .await?;
        Ok(offset as u64)
    }

    async fn min_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_min_offset(broker_addr.as_str(), &message_queue, timeout_millis)
            .await
    }

    async fn max_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
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
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_consume_stats(
                &broker_addr,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: CheetahString::empty(),
                    topic_request_header: None,
                },
                self.timeout_millis.as_millis() as u64,
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
                .client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .get_topic_stats_info(
                    &broker_addr,
                    GetTopicStatsInfoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    },
                    self.timeout_millis.as_millis() as u64,
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
            self.client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .get_max_offset(broker_addr.as_str(), &queue, self.timeout_millis.as_millis() as u64)
                .await?
        } else {
            self.client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .search_offset_by_timestamp(
                    broker_addr.as_str(),
                    &queue,
                    timestamp,
                    rocketmq_common::common::boundary_type::BoundaryType::Lower,
                    self.timeout_millis.as_millis() as u64,
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
            self.client_instance
                .as_ref()
                .unwrap()
                .get_mq_client_api_impl()
                .update_consumer_offset(
                    &broker_addr,
                    UpdateConsumerOffsetRequestHeader {
                        consumer_group,
                        topic: queue.topic().clone(),
                        queue_id: queue.queue_id(),
                        commit_offset: reset_offset,
                        topic_request_header: None,
                    },
                    self.timeout_millis.as_millis() as u64,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_builder::MessageBuilder;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::mix_all;
    #[allow(deprecated)]
    use rocketmq_common::common::tools::track_type::TrackType;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
    use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_remoting::protocol::body::connection::Connection;
    use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
    use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    use super::encode_topic_attributes;
    use super::is_message_consumed;
    use super::merge_order_conf_entries;
    use super::parse_response_code_from_message;
    use super::resolve_consumed_track_type;
    use super::select_consumer_direct_connection;

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
    fn parse_response_code_from_message_reads_consumer_not_online_code() {
        let code = parse_response_code_from_message("CODE: 206 DESC: Not found the consumer group connection");

        assert_eq!(code, Some(ResponseCode::ConsumerNotOnline));
    }
}
