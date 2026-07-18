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
use std::future::Future;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use crate::base::client_config::ClientConfig;
use crate::base::mq_client_admin::MqClientAdminInner;
use crate::consumer::ack_callback::AckCallback;
use crate::consumer::ack_result::AckResult;
use crate::consumer::ack_status::AckStatus;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::notify_result::NotifyResult;
use crate::consumer::pop_callback::PopCallback;
use crate::consumer::pop_result::PopResult;
use crate::consumer::pop_status::PopStatus;
use crate::consumer::pull_callback::PullCallback;
use crate::consumer::pull_result::PullResult;
use crate::consumer::pull_status::PullStatus;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_context::SendMessageTraceSnapshot;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::communication_mode::CommunicationMode;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::runtime::spawn_client_blocking_io;
use crate::runtime::spawn_client_task;
use cheetah_string::CheetahString;

use crate::base::validators::Validators;
use rocketmq_common::common::attribute::attribute_parser::AttributeParser;
use rocketmq_common::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::file_readahead_mode;
use rocketmq_common::common::lite::LiteSubscriptionDTO;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::common::namesrv::default_top_addressing::DefaultTopAddressing;
use rocketmq_common::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use rocketmq_common::common::namesrv::top_addressing::TopAddressing;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::MessageDecoder;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::base::connection_net_event::ConnectionNetEvent;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::common::heartbeat_v2_result::HeartbeatV2Result;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::bodies::broker::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::protocol::body::check_client_request_body::CheckClientRequestBody;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_remoting::protocol::body::cluster_acl_version_info::ClusterAclVersionInfo;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::create_topic_list_request_body::CreateTopicListRequestBody;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_remoting::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::group_list::GroupList;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_remoting::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_remoting::protocol::body::query_consume_time_span_body::QueryConsumeTimeSpanBody;
use rocketmq_remoting::protocol::body::query_correction_offset_body::QueryCorrectionOffsetBody;
use rocketmq_remoting::protocol::body::query_subscription_response_body::QuerySubscriptionResponseBody;
use rocketmq_remoting::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::response::get_consumer_status_body::GetConsumerStatusBody;
use rocketmq_remoting::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_remoting::protocol::body::response::reset_offset_body::ResetOffsetBody;
use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_remoting::protocol::body::subscription_group_list::SubscriptionGroupList;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::add_broker_request_header::AddBrokerRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_remoting::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::clone_group_offset_request_header::CloneGroupOffsetRequestHeader;
use rocketmq_remoting::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_remoting::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_remoting::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::create_acl_request_header::CreateAclRequestHeader;
use rocketmq_remoting::protocol::header::create_topic_list_request_header::CreateTopicListRequestHeader;
use rocketmq_remoting::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_remoting::protocol::header::create_user_request_header::CreateUserRequestHeader;
use rocketmq_remoting::protocol::header::delete_acl_request_header::DeleteAclRequestHeader;
use rocketmq_remoting::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
use rocketmq_remoting::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_remoting::protocol::header::delete_user_request_header::DeleteUserRequestHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::header::get_acl_request_header::GetAclRequestHeader;
use rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_remoting::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
use rocketmq_remoting::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
use rocketmq_remoting::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_remoting::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_remoting::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_user_request_headers::GetUserRequestHeader;
use rocketmq_remoting::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_remoting::protocol::header::list_acl_request_header::ListAclRequestHeader;
use rocketmq_remoting::protocol::header::list_users_request_header::ListUsersRequestHeader;
use rocketmq_remoting::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::config_header::GetNamesrvConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader;
use rocketmq_remoting::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_remoting::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_remoting::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_remoting::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::query_correction_offset_header::QueryCorrectionOffsetHeader;
use rocketmq_remoting::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_remoting::protocol::header::query_message_response_header::QueryMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_remoting::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_remoting::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_remoting::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
use rocketmq_remoting::protocol::header::remove_broker_request_header::RemoveBrokerRequestHeader;
use rocketmq_remoting::protocol::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader;
use rocketmq_remoting::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_remoting::protocol::header::resume_check_half_message_request_header::ResumeCheckHalfMessageRequestHeader;

use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_remoting::protocol::header::trigger_lite_dispatch_request_header::TriggerLiteDispatchRequestHeader;
use rocketmq_remoting::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::header::update_acl_request_header::UpdateAclRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_global_white_addrs_config_request_header::UpdateGlobalWhiteAddrsConfigRequestHeader;
use rocketmq_remoting::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_remoting::protocol::header::update_user_request_header::UpdateUserRequestHeader;
use rocketmq_remoting::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_remoting::protocol::headers::client::GetConsumerConnectionListRequestHeader;
use rocketmq_remoting::protocol::headers::view::SearchOffsetRequestHeader;
use rocketmq_remoting::protocol::headers::view::SearchOffsetResponseHeader;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::LanguageCode;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_remoting::ConsumerConnection;
use rocketmq_rust::ArcMut;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
use tracing::warn;

static INIT_REMOTING_VERSION: OnceLock<()> = OnceLock::new();

static SEND_SMART_MSG: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("org.apache.rocketmq.client.sendSmartMsg")
        .unwrap_or("false".to_string())
        .parse()
        .unwrap_or(false)
});

fn java_long_to_u64_field(
    operation: &'static str,
    field: &'static str,
    value: i64,
) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(value).map_err(|_| {
        RocketMQError::illegal_argument(format!(
            "{operation} {field} is negative and cannot be represented as Rust u64"
        ))
    })
}

fn trace_on_from_ext_fields(ext_fields: Option<&HashMap<CheetahString, CheetahString>>) -> bool {
    ext_fields
        .and_then(|fields| fields.get(MessageConst::PROPERTY_TRACE_SWITCH))
        .is_none_or(|trace_on| trace_on.as_str() != "false")
}

fn duration_millis_to_u64(operation: &'static str, duration: Duration) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(duration.as_millis())
        .map_err(|_| RocketMQError::illegal_argument(format!("{operation} timeout exceeds Rust u64 millisecond range")))
}

#[derive(Clone)]
struct AsyncSendHookContext {
    producer_group: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    born_host: Option<CheetahString>,
    communication_mode: Option<CommunicationMode>,
    msg_type: Option<MessageType>,
    namespace: Option<CheetahString>,
    mq_trace_context: Option<Arc<Box<dyn std::any::Any + Send + Sync>>>,
    producer: Option<ArcMut<DefaultMQProducerImpl>>,
    mq: Option<MessageQueue>,
    message_trace_snapshot: Option<SendMessageTraceSnapshot>,
    trace_start_time: Option<u64>,
}

struct AsyncRetryRequest {
    template: Option<RemotingCommand>,
}

impl AsyncRetryRequest {
    fn new(mut request: RemotingCommand) -> Self {
        request.materialize_custom_header_to_ext_fields();
        Self {
            template: Some(request),
        }
    }

    fn next_attempt(&mut self, keep_template_for_retry: bool) -> RemotingCommand {
        if keep_template_for_retry {
            return self
                .template
                .as_ref()
                .expect("async retry request template should be available")
                .clone();
        }

        self.template
            .take()
            .expect("async retry final request should be available")
    }

    fn set_retry_opaque(&mut self, opaque: i32) {
        self.template
            .as_mut()
            .expect("async retry request template should be available")
            .set_opaque_mut(opaque);
    }

    #[cfg(test)]
    fn is_consumed(&self) -> bool {
        self.template.is_none()
    }
}

pub struct MQClientAPIImpl {
    remoting_client: Arc<RocketmqDefaultClient<ClientRemotingProcessor>>,
    top_addressing: Arc<Box<dyn TopAddressing>>,
    name_srv_addr: Option<String>,
    client_config: Arc<ClientConfig>,
    background_tasks: TaskTracker,
    background_shutdown: CancellationToken,
}

impl MQClientAPIImpl {
    pub(crate) async fn get_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<CheetahString>> {
        let request_header = GetKVConfigRequestHeader::new(namespace, key);
        let request = RemotingCommand::create_request_command(RequestCode::GetKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header = response
                        .decode_command_custom_header::<GetKVConfigResponseHeader>()
                        .map_err(|error| mq_client_err!(format!("decode GetKVConfigResponseHeader failed: {error}")))?;
                    return Ok(response_header.value);
                }
                ResponseCode::QueryNotFound => return Ok(None),
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }

        Ok(None)
    }

    pub(crate) async fn get_kv_config_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<CheetahString>> {
        self.get_kvconfig_value(namespace, key, timeout_millis).await
    }

    pub(crate) async fn delete_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = DeleteKVConfigRequestHeader::new(namespace, key);
        let request = RemotingCommand::create_request_command(RequestCode::DeleteKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn delete_kv_config_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.delete_kvconfig_value(namespace, key, timeout_millis).await
    }

    pub(crate) async fn get_kvlist_by_namespace(
        &self,
        namespace: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::kv_table::KVTable> {
        let request_header = GetKVListByNamespaceRequestHeader::new(namespace);
        let request = RemotingCommand::create_request_command(RequestCode::GetKvlistByNamespace, request_header);

        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::kv_table::KVTable::decode(body.as_ref());
            }
        }

        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_kv_list_by_namespace(
        &self,
        namespace: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::kv_table::KVTable> {
        self.get_kvlist_by_namespace(namespace, timeout_millis).await
    }

    pub(crate) async fn put_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = PutKVConfigRequestHeader::new(namespace, key, value);
        let request = RemotingCommand::create_request_command(RequestCode::PutKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn put_kv_config_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.put_kvconfig_value(namespace, key, value, timeout_millis).await
    }

    pub(crate) async fn create_user(
        &self,
        broker_address: CheetahString,
        user_info: &UserInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request_header = CreateUserRequestHeader::default();
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "username is required".to_string()))?;
        request_header.set_username(username);
        let mut request = RemotingCommand::create_request_command(RequestCode::AuthCreateUser, request_header);
        request = request.set_body(user_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request.clone(), timeout_millis)
            .await?;

        let mut err_response = None;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {}
            _ => err_response = Some(response),
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn update_user(
        &self,
        broker_address: CheetahString,
        user_info: &UserInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request_header = UpdateUserRequestHeader::default();
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "username is required".to_string()))?;
        request_header.set_username(username);
        let mut request = RemotingCommand::create_request_command(RequestCode::AuthUpdateUser, request_header);
        request = request.set_body(user_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request.clone(), timeout_millis)
            .await?;

        let mut err_response = None;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {}
            _ => err_response = Some(response),
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn create_acl(
        &self,
        broker_address: CheetahString,
        acl_info: &AclInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let subject = acl_info
            .subject
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "ACL subject is required".to_string()))?;
        let request_header = CreateAclRequestHeader { subject };
        let request = RemotingCommand::create_request_command(RequestCode::AuthCreateAcl, request_header)
            .set_body(acl_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn update_acl(
        &self,
        broker_address: CheetahString,
        acl_info: &AclInfo,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let subject = acl_info
            .subject
            .clone()
            .ok_or_else(|| mq_client_err!(-1, "ACL subject is required".to_string()))?;
        let request_header = UpdateAclRequestHeader { subject };
        let request = RemotingCommand::create_request_command(RequestCode::AuthUpdateAcl, request_header)
            .set_body(acl_info.encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn create_and_update_plain_access_config(
        &self,
        broker_address: CheetahString,
        plain_access_config: &PlainAccessConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = create_and_update_plain_access_config_request(plain_access_config)?;

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, ToString::to_string)
            )),
        }
    }

    pub(crate) async fn delete_plain_access_config(
        &self,
        broker_address: CheetahString,
        access_key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = delete_plain_access_config_request(&access_key);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, ToString::to_string)
            )),
        }
    }

    pub(crate) async fn update_global_white_addrs_config(
        &self,
        broker_address: CheetahString,
        global_white_addrs: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = UpdateGlobalWhiteAddrsConfigRequestHeader { global_white_addrs };
        let request =
            RemotingCommand::create_request_command(RequestCode::UpdateGlobalWhiteAddrsConfig, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn get_broker_cluster_acl_version_info(
        &self,
        broker_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<ClusterAclVersionInfo> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterAclInfo);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => decode_cluster_acl_version_info_response_body(response.body()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn delete_acl(
        &self,
        broker_address: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let resource_option = if resource.is_empty() { None } else { Some(resource) };
        let request_header = DeleteAclRequestHeader::new(subject, resource_option);
        let request = RemotingCommand::create_request_command(RequestCode::AuthDeleteAcl, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn list_acl(
        &self,
        broker_address: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<AclInfo>> {
        let request_header = ListAclRequestHeader {
            subject_filter,
            resource_filter,
        };
        let request = RemotingCommand::create_request_command(RequestCode::AuthListAcl, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    Vec::<AclInfo>::decode(body.as_ref())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn get_acl(
        &self,
        broker_address: CheetahString,
        subject: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<AclInfo> {
        let request = get_acl_request(subject);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response
                    .get_body()
                    .ok_or_else(|| mq_client_err!("get_acl response body is empty".to_string()))?;
                AclInfo::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn get_user(
        &self,
        broker_address: CheetahString,
        username: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<UserInfo>> {
        let request_header = GetUserRequestHeader {
            username: username.clone(),
        };
        let request = RemotingCommand::create_request_command(RequestCode::AuthGetUser, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response.get_body();
                if let Some(body) = response.get_body() {
                    let user_info = UserInfo::decode(body)?;
                    Ok(Some(user_info))
                } else {
                    Ok(None)
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map(|s| s.to_string()).unwrap_or_default()
            )),
        }
    }

    pub(crate) async fn list_users(
        &self,
        broker_address: CheetahString,
        filter: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<UserInfo>> {
        let request_header = ListUsersRequestHeader { filter };
        let request = RemotingCommand::create_request_command(RequestCode::AuthListUsers, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    Vec::<UserInfo>::decode(body.as_ref())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn list_user(
        &self,
        broker_address: CheetahString,
        filter: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<UserInfo>> {
        self.list_users(broker_address, filter, timeout_millis).await
    }

    pub(crate) async fn delete_user(
        &self,
        broker_address: CheetahString,
        username: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request_header = DeleteUserRequestHeader::default();
        request_header.set_username(username);
        let request = RemotingCommand::create_request_command(RequestCode::AuthDeleteUser, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&broker_address), request.clone(), timeout_millis)
            .await?;

        let mut err_response = None;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {}
            _ => err_response = Some(response),
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        special_name_servers: Option<Vec<CheetahString>>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }
        let invoke_name_servers = if let Some(name_servers) = special_name_servers {
            if !name_servers.is_empty() {
                name_servers
            } else {
                self.get_name_server_address_list()
            }
        } else {
            self.get_name_server_address_list()
        };
        if invoke_name_servers.is_empty() {
            return Ok(());
        }
        let empty_header = EmptyHeader {};
        let mut request = RemotingCommand::create_request_command(RequestCode::UpdateNamesrvConfig, empty_header);

        request = request.set_body(body.to_string());
        let mut err_response = None;
        for name_srv_addr in invoke_name_servers {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<i32> {
        let request_header = AddWritePermOfBrokerRequestHeader::new(broker_name);
        let request = RemotingCommand::create_request_command(RequestCode::AddWritePermOfBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&namesrv_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let request_header = response.decode_command_custom_header_fast::<AddWritePermOfBrokerResponseHeader>()?;
            return Ok(request_header.get_add_topic_count());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<i32> {
        let request_header = WipeWritePermOfBrokerRequestHeader::new(broker_name);
        let request = RemotingCommand::create_request_command(RequestCode::WipeWritePermOfBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&namesrv_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let request_header = response.decode_command_custom_header_fast::<WipeWritePermOfBrokerResponseHeader>()?;
            return Ok(request_header.get_wipe_topic_count());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_broker_cluster_info(&self, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerClusterInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ClusterInfo::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_broker_runtime_info(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::kv_table::KVTable> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerRuntimeInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::kv_table::KVTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn sync_broker_member_group(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        is_compatible_with_old_name_srv: bool,
    ) -> RocketMQResult<Option<BrokerMemberGroup>> {
        if is_compatible_with_old_name_srv {
            self.get_broker_member_group_compatible(cluster_name, broker_name).await
        } else {
            self.get_broker_member_group(cluster_name, broker_name).await
        }
    }

    async fn get_broker_member_group(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
    ) -> RocketMQResult<Option<BrokerMemberGroup>> {
        let request_header = GetBrokerMemberGroupRequestHeader::new(cluster_name.clone(), broker_name.clone());
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerMemberGroup, request_header);
        let mut response = self.remoting_client.invoke_request(None, request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                let response_body = GetBrokerMemberGroupResponseBody::decode(body.as_ref())?;
                return Ok(Some(
                    response_body
                        .broker_member_group
                        .unwrap_or_else(|| empty_broker_member_group(cluster_name, broker_name)),
                ));
            }
        }
        Ok(Some(empty_broker_member_group(cluster_name, broker_name)))
    }

    async fn get_broker_member_group_compatible(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
    ) -> RocketMQResult<Option<BrokerMemberGroup>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: CheetahString::from_string(format!(
                "{}{}",
                TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX,
                broker_name
            )),
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetRouteinfoByTopic, request_header);
        let mut response = self.remoting_client.invoke_request(None, request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                let topic_route_data = TopicRouteData::decode(body.as_ref())?;
                return Ok(Some(broker_member_group_from_route_data(
                    cluster_name,
                    broker_name,
                    &topic_route_data,
                )));
            }
        }
        Ok(Some(empty_broker_member_group(cluster_name, broker_name)))
    }

    pub(crate) async fn get_broker_lite_info(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetBrokerLiteInfoResponseBody> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetBrokerLiteInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn check_rocksdb_cq_write_progress(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        check_store_time: i64,
        timeout_millis: u64,
    ) -> RocketMQResult<CheckRocksdbCqWriteResult> {
        let request_header = CheckRocksdbCqWriteProgressRequestHeader {
            topic,
            check_store_time,
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::CheckRocksdbCqWriteProgress, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let result: CheckRocksdbCqWriteResult = serde_json::from_slice(body.as_ref()).map_err(|e| {
                    mq_client_err!(-1, format!("Failed to deserialize CheckRocksdbCqWriteResult: {}", e))
                })?;
                return Ok(result);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_consume_queue(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: i64,
        count: i32,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<QueryConsumeQueueResponseBody> {
        let request_header = QueryConsumeQueueRequestHeader {
            topic,
            queue_id,
            index,
            count,
            consumer_group: if consumer_group.is_empty() {
                None
            } else {
                Some(consumer_group)
            },
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumeQueue, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let result: QueryConsumeQueueResponseBody = serde_json::from_slice(body.as_ref()).map_err(|e| {
                    mq_client_err!(
                        -1,
                        format!("Failed to deserialize QueryConsumeQueueResponseBody: {}", e)
                    )
                })?;
                return Ok(result);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_lite_group_info(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
        timeout_millis: u64,
    ) -> RocketMQResult<GetLiteGroupInfoResponseBody> {
        let request_header = GetLiteGroupInfoRequestHeader {
            group,
            lite_topic,
            top_k,
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetLiteGroupInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_lite_client_info(
        &self,
        addr: &CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetLiteClientInfoResponseBody> {
        let request_header = GetLiteClientInfoRequestHeader {
            parent_topic: Some(parent_topic),
            group: Some(group),
            client_id: Some(client_id),
            max_count: 1000,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetLiteClientInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetLiteClientInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn trigger_lite_dispatch(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        client_id: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = TriggerLiteDispatchRequestHeader {
            group,
            client_id: if client_id.is_empty() { None } else { Some(client_id) },
        };
        let request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn sync_lite_subscription(
        &self,
        broker_addr: &CheetahString,
        lite_subscription_dto: LiteSubscriptionDTO,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = lite_subscription_ctl_request(lite_subscription_dto)?;
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn sync_lite_subscription_async(
        &self,
        broker_addr: &CheetahString,
        lite_subscription_dto: LiteSubscriptionDTO,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.sync_lite_subscription(broker_addr, lite_subscription_dto, timeout_millis)
            .await
    }

    pub(crate) async fn get_lite_topic_info(
        &self,
        addr: &CheetahString,
        parent_topic: &CheetahString,
        lite_topic: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetLiteTopicInfoResponseBody> {
        let request_header = GetLiteTopicInfoRequestHeader {
            parent_topic: parent_topic.clone(),
            lite_topic: lite_topic.clone(),
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetLiteTopicInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }
    pub(crate) async fn get_parent_topic_info(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetParentTopicInfoResponseBody> {
        let request_header = GetParentTopicInfoRequestHeader { topic, rpc: None };
        let request = RemotingCommand::create_request_command(RequestCode::GetParentTopicInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GetParentTopicInfoResponseBody::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub async fn delete_subscription_group(
        &self,
        addr: &CheetahString,
        group_name: CheetahString,
        clean_offset: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = DeleteSubscriptionGroupRequestHeader {
            group_name,
            clean_offset,
            rpc_request_header: None,
        };

        let request = RemotingCommand::create_request_command(RequestCode::DeleteSubscriptionGroup, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub async fn reset_master_flush_offset(
        &self,
        broker_addr: &CheetahString,
        master_flush_offset: i64,
    ) -> RocketMQResult<()> {
        let request_header = ResetMasterFlushOffsetHeader {
            master_flush_offset: Some(master_flush_offset),
        };

        let request = RemotingCommand::create_request_command(RequestCode::ResetMasterFlushOffset, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    async fn get_topic_list_from_name_server_by_code(
        &self,
        request_code: RequestCode,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let request = RemotingCommand::create_request_command(request_code, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_topic_list_from_name_server(&self, timeout_millis: u64) -> RocketMQResult<TopicList> {
        self.get_topic_list_from_name_server_by_code(RequestCode::GetAllTopicListFromNameserver, timeout_millis)
            .await
    }

    pub(crate) async fn get_all_topic_list_from_name_server(&self, timeout_millis: u64) -> RocketMQResult<TopicList> {
        self.get_topic_list_from_name_server(timeout_millis).await
    }

    pub(crate) async fn get_system_topic_list(&self, timeout_millis: u64) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetSystemTopicListFromNs, timeout_millis)
            .await?;
        merge_system_topic_list_from_broker(self, &mut topic_list, timeout_millis).await?;
        Ok(topic_list)
    }

    pub(crate) async fn get_unit_topic_list(
        &self,
        contain_retry: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetUnitTopicList, timeout_millis)
            .await?;
        filter_retry_topics_like_java(&mut topic_list, contain_retry);
        Ok(topic_list)
    }

    pub(crate) async fn get_has_unit_sub_topic_list(
        &self,
        contain_retry: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetHasUnitSubTopicList, timeout_millis)
            .await?;
        filter_retry_topics_like_java(&mut topic_list, contain_retry);
        Ok(topic_list)
    }

    pub(crate) async fn get_has_unit_sub_un_unit_topic_list(
        &self,
        contain_retry: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let mut topic_list = self
            .get_topic_list_from_name_server_by_code(RequestCode::GetHasUnitSubUnunitTopicList, timeout_millis)
            .await?;
        filter_retry_topics_like_java(&mut topic_list, contain_retry);
        Ok(topic_list)
    }

    pub(crate) async fn get_topics_by_cluster(
        &self,
        cluster: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::topic::topic_list::TopicList> {
        let request_header = GetTopicsByClusterRequestHeader::new(cluster);
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicsByCluster, request_header);
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::topic::topic_list::TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_cluster_list(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HashSet<CheetahString>> {
        let topic_route_data = self
            .get_topic_route_info_from_name_server(&topic, timeout_millis)
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for: {topic}")))?;
        let cluster_info = self.get_broker_cluster_info(timeout_millis).await?;
        Ok(cluster_names_for_topic_route(&cluster_info, &topic_route_data))
    }

    pub(crate) async fn get_system_topic_list_from_broker(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::topic::topic_list::TopicList> {
        let request =
            RemotingCommand::create_request_command(RequestCode::GetSystemTopicListFromBroker, EmptyHeader {});
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::topic::topic_list::TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_consume_stats(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumeStats, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_consume_time_span(
        &self,
        addr: &CheetahString,
        request_header: QueryConsumeTimeSpanRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<rocketmq_remoting::protocol::body::queue_time_span::QueueTimeSpan>> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumeTimeSpan, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let body: QueryConsumeTimeSpanBody = serde_json::from_slice(body.as_ref())?;
                return Ok(body.consume_time_span_set);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_topic_stats_info(
        &self,
        addr: &CheetahString,
        request_header: GetTopicStatsInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable> {
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicStatsInfo, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_topic_config(
        &self,
        addr: &CheetahString,
        request_header: GetTopicConfigRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicConfigAndQueueMapping> {
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicConfig, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return serde_json::from_slice(body.as_ref()).map_err(Into::into);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn create_static_topic(
        &self,
        addr: &CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let topic = topic_config
            .topic_name
            .clone()
            .filter(|topic| !topic.is_empty())
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("Topic name is required".into()))?;
        let read_queue_nums = i32::try_from(topic_config.read_queue_nums).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument("readQueueNums exceeds Java int range".into())
        })?;
        let write_queue_nums = i32::try_from(topic_config.write_queue_nums).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument("writeQueueNums exceeds Java int range".into())
        })?;
        let perm = i32::try_from(topic_config.perm)
            .map_err(|_| rocketmq_error::RocketMQError::IllegalArgument("perm exceeds Java int range".into()))?;
        let topic_sys_flag = i32::try_from(topic_config.topic_sys_flag).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument("topicSysFlag exceeds Java int range".into())
        })?;

        let request_header = CreateTopicRequestHeader {
            topic,
            default_topic,
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_filter_type: CheetahString::from_static_str(topic_config.topic_filter_type.as_str()),
            topic_sys_flag: Some(topic_sys_flag),
            order: topic_config.order,
            attributes: None,
            force: Some(force),
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateStaticTopic, request_header)
            .set_body(mapping_detail.encode()?);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub(crate) async fn query_topic_consume_by_who(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_remoting::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::group_list::GroupList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicConsumeByWho, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::group_list::GroupList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_topics_by_consumer(
        &self,
        addr: &CheetahString,
        request_header: QueryTopicsByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::topic::topic_list::TopicList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicsByConsumer, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::topic::topic_list::TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_subscription_by_consumer(
        &self,
        addr: &CheetahString,
        request_header: QuerySubscriptionByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionData> {
        let request = RemotingCommand::create_request_command(RequestCode::QuerySubscriptionByConsumer, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response
                .get_body()
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response body is empty".to_string()))?;
            let response_body: QuerySubscriptionResponseBody = serde_json::from_slice(body.as_ref())?;
            return response_body.subscription_data.ok_or_else(|| {
                mq_client_err!("query_subscription_by_consumer response subscriptionData is empty".to_string())
            });
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    async fn invoke_broker_success_request(
        &self,
        addr: &CheetahString,
        request_code: RequestCode,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        let request = RemotingCommand::create_request_command(request_code, EmptyHeader {});
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(true);
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn clean_expired_consume_queue(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        self.invoke_broker_success_request(addr, RequestCode::CleanExpiredConsumequeue, timeout_millis)
            .await
    }

    pub(crate) async fn delete_expired_commit_log(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        self.invoke_broker_success_request(addr, RequestCode::DeleteExpiredCommitlog, timeout_millis)
            .await
    }

    pub(crate) async fn clean_unused_topic(&self, addr: &CheetahString, timeout_millis: u64) -> RocketMQResult<bool> {
        self.invoke_broker_success_request(addr, RequestCode::CleanUnusedTopic, timeout_millis)
            .await
    }

    pub(crate) async fn clean_unused_topic_by_addr(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        self.clean_unused_topic(addr, timeout_millis).await
    }

    pub(crate) async fn create_topic(
        &self,
        addr: &CheetahString,
        default_topic: CheetahString,
        topic_config: &TopicConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = create_topic_request_header_like_java(default_topic, topic_config)?;
        self.update_or_create_topic(addr, request_header, timeout_millis).await
    }

    pub(crate) async fn create_topic_list(
        &self,
        address: &CheetahString,
        topic_config_list: Vec<TopicConfig>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = create_topic_list_request(topic_config_list)?;
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn update_or_create_topic(
        &self,
        addr: &CheetahString,
        request_header: CreateTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateTopic, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn create_subscription_group_list(
        &self,
        address: &CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = create_subscription_group_list_request(configs)?;
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, address.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn delete_topic_in_broker(
        &self,
        addr: &CheetahString,
        request_header: DeleteTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInBroker, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn delete_topic_in_name_server(
        &self,
        addr: &CheetahString,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.delete_topic_in_nameserver(
            addr,
            DeleteTopicFromNamesrvRequestHeader::new(topic, cluster_name),
            timeout_millis,
        )
        .await
    }

    pub(crate) async fn delete_topic_in_nameserver(
        &self,
        addr: &CheetahString,
        request_header: DeleteTopicFromNamesrvRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInNamesrv, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn invoke_broker_to_reset_offset(
        &self,
        addr: &CheetahString,
        request_header: ResetOffsetRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<MessageQueue, i64>> {
        let request = RemotingCommand::create_request_command(RequestCode::InvokeBrokerToResetOffset, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return reset_offset_table_from_response(&response);
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn query_correction_offset(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        group: CheetahString,
        filter_groups: Option<Vec<CheetahString>>,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<i32, i64>> {
        let request = query_correction_offset_request(topic, group, filter_groups);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return QueryCorrectionOffsetBody::decode(body.as_ref()).map(|body| body.correction_offsets);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn view_broker_stats_data(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_remoting::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData> {
        let request = RemotingCommand::create_request_command(RequestCode::ViewBrokerStatsData, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn fetch_consume_stats_in_broker(
        &self,
        addr: &CheetahString,
        request_header: rocketmq_remoting::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerConsumeStats, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList::decode(body.as_ref());
            }
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub(crate) async fn clone_group_offset(
        &self,
        addr: &CheetahString,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = CloneGroupOffsetRequestHeader {
            src_group,
            dest_group,
            topic: Some(topic),
            offline: is_offline,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::CloneGroupOffset, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }
}

impl NameServerUpdateCallback for MQClientAPIImpl {
    fn on_name_server_address_changed(&self, namesrv_address: Option<String>) -> String {
        namesrv_address.unwrap_or_default()
    }
}

impl MQClientAPIImpl {
    pub fn new(
        tokio_client_config: Arc<TokioClientConfig>,
        client_remoting_processor: ClientRemotingProcessor,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        client_config: Arc<ClientConfig>,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> Self {
        Self::init_remoting_version();

        let mut remoting_config = (*tokio_client_config).clone();
        remoting_config.use_tls = client_config.use_tls;
        remoting_config.tls_config = client_config.tls_config.clone();
        remoting_config.tls_config.enable = client_config.use_tls;
        let default_client =
            RocketmqDefaultClient::new_with_cl(Arc::new(remoting_config), client_remoting_processor, tx);
        if let Some(hook) = rpc_hook {
            default_client.register_rpc_hook(hook);
        }

        MQClientAPIImpl {
            remoting_client: Arc::new(default_client),
            top_addressing: Arc::new(Box::new(DefaultTopAddressing::new(
                mix_all::get_ws_addr().into(),
                client_config.unit_name.clone(),
            ))),
            //client_remoting_processor,
            name_srv_addr: None,
            client_config,
            background_tasks: TaskTracker::new(),
            background_shutdown: CancellationToken::new(),
        }
    }

    pub async fn start(&self) {
        let client = Arc::downgrade(&self.remoting_client);
        self.remoting_client.start(client).await;
    }

    pub fn shutdown(&mut self) {
        self.background_shutdown.cancel();
        self.background_tasks.close();
        self.remoting_client.shutdown();
    }

    pub async fn shutdown_background_tasks(&self, timeout: Duration) -> bool {
        self.background_shutdown.cancel();
        self.background_tasks.close();
        tokio::time::timeout(timeout, self.background_tasks.wait())
            .await
            .is_ok()
    }

    pub fn get_remoting_client(&self) -> Arc<RocketmqDefaultClient<ClientRemotingProcessor>> {
        self.remoting_client.clone()
    }

    #[inline]
    pub(crate) fn is_use_tls(&self) -> bool {
        self.remoting_client.is_use_tls()
    }

    pub async fn fetch_name_server_addr(&mut self) -> Option<String> {
        let top_addressing = self.top_addressing.clone();
        let addrs = spawn_client_blocking_io("client.fetch_name_server_addr", move || top_addressing.fetch_ns_addr())
            .await
            .unwrap_or_default();

        if let Some(addrs) = addrs.as_ref().filter(|addr| !addr.is_empty()) {
            let notify = self.name_srv_addr.as_deref() != Some(addrs.as_str());
            if notify {
                self.name_srv_addr = Some(addrs.clone());
                self.update_name_server_address_list(addrs.as_str()).await;
                return Some(addrs.clone());
            }
        }
        self.name_srv_addr.clone()
    }

    pub async fn on_name_server_address_change(&mut self, namesrv_address: Option<String>) -> String {
        if let Some(addrs) = namesrv_address.as_ref().filter(|addr| !addr.is_empty()) {
            let changed = self.name_srv_addr.as_deref() != Some(addrs.as_str());
            if changed {
                self.name_srv_addr = Some(addrs.clone());
                self.update_name_server_address_list(addrs.as_str()).await;
                return addrs.clone();
            }
        }
        self.name_srv_addr.clone().unwrap_or_default()
    }

    pub async fn update_name_server_address_list(&self, addrs: &str) {
        self.update_name_server_address_list_sync(addrs);
    }

    pub(crate) fn update_name_server_address_list_sync(&self, addrs: &str) {
        let addr_vec = addrs
            .split(";")
            .map(CheetahString::from_slice)
            .collect::<Vec<CheetahString>>();
        self.remoting_client.update_name_server_address_list_sync(addr_vec);
    }

    pub async fn invoke(
        &self,
        broker_addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<RemotingCommand> {
        self.remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await
    }

    pub async fn invoke_oneway(
        &self,
        broker_addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.remoting_client
            .invoke_request_oneway(broker_addr, request, timeout_millis)
            .await;
        Ok(())
    }

    #[inline]
    pub async fn get_default_topic_route_info_from_name_server(
        &self,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(
            TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            timeout_millis,
            false,
        )
        .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server(
        &self,
        topic: &str,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(topic, timeout_millis, true)
            .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server_detail(
        &self,
        topic: &str,
        timeout_millis: u64,
        allow_topic_not_exist: bool,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: CheetahString::from_slice(topic),
            accept_standard_json_only: None,
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetRouteinfoByTopic, request_header);
        let response = self.remoting_client.invoke_request(None, request, timeout_millis).await;
        match response {
            Ok(mut result) => {
                let code = result.code();
                let response_code = ResponseCode::from(code);
                match response_code {
                    ResponseCode::Success => {
                        let body = result.take_body();
                        if let Some(body_inner) = body {
                            let route_data = TopicRouteData::decode(body_inner.as_ref())?;
                            return Ok(Some(route_data));
                        }
                    }
                    ResponseCode::TopicNotExist => {
                        if allow_topic_not_exist {
                            warn!("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                        }
                    }
                    _ => {
                        return Err(mq_client_err!(
                            code,
                            result.remark().cloned().unwrap_or_default().to_string()
                        ))
                    }
                }
                Err(mq_client_err!(
                    code,
                    result.remark().cloned().unwrap_or_default().to_string()
                ))
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        self.remoting_client.get_name_server_address_list()
    }

    pub async fn send_message<T>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait,
    {
        let begin_start_time = Instant::now();
        let msg_type = msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TYPE));
        let is_reply = msg_type
            .as_ref()
            .is_some_and(|msg_type| msg_type.as_str() == mix_all::REPLY_MESSAGE_FLAG);
        let mut request = if is_reply {
            if *SEND_SMART_MSG {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
                RemotingCommand::create_request_command(RequestCode::SendReplyMessageV2, request_header_v2)
            } else {
                RemotingCommand::create_request_command(RequestCode::SendReplyMessage, request_header)
            }
        } else {
            let is_batch_message = msg.as_any().downcast_ref::<MessageBatch>().is_some();
            if *SEND_SMART_MSG || is_batch_message {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
                let request_code = if is_batch_message {
                    RequestCode::SendBatchMessage
                } else {
                    RequestCode::SendMessageV2
                };
                RemotingCommand::create_request_command(request_code, request_header_v2)
            } else {
                RemotingCommand::create_request_command(RequestCode::SendMessage, request_header)
            }
        };

        // Zero-copy optimization: Bytes is reference-counted, clone() only increments ref count
        // This is very cheap (~5ns) compared to deep copying the message body
        // For true zero-copy, we would need to restructure to pass &Bytes through the entire chain
        if let Some(compressed_body) = msg.get_compressed_body() {
            request.set_body_mut_ref(compressed_body.clone());
        } else if let Some(body) = msg.get_body() {
            request.set_body_mut_ref(body.clone());
        } else {
            return Err(mq_client_err!(-1, "Message body is None"));
        }
        match communication_mode {
            CommunicationMode::Sync => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(rocketmq_common::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: timeout_millis,
                    });
                }
                let result = self
                    .send_message_sync(addr, broker_name, msg, timeout_millis - cost_time_sync, request)
                    .await?;
                Ok(Some(result))
            }
            CommunicationMode::Async => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: timeout_millis,
                    });
                }
                self.send_message_async(
                    addr,
                    broker_name,
                    msg,
                    timeout_millis,
                    request,
                    send_callback,
                    topic_publish_info,
                    instance,
                    retry_times_when_send_failed,
                    context,
                    producer,
                )
                .await;
                Ok(None)
            }
            CommunicationMode::Oneway => {
                self.remoting_client
                    .invoke_request_oneway(addr, request, timeout_millis)
                    .await;
                Ok(None)
            }
        }
    }

    /// **High-Performance** unbounded oneway send without timeout control.
    ///
    /// This method provides **maximum throughput** by spawning background tasks immediately
    /// without waiting for network send completion, achieving near-zero latency overhead.
    ///
    /// # Performance Characteristics
    /// - **Latency**: < 10μs per send (tokio spawn overhead only)
    /// - **Throughput**: 100K+ messages/second per producer
    /// - **Memory**: ~1KB per spawned task
    /// - **Zero blocking**: Returns immediately after task spawn
    ///
    /// # When to Use
    /// Ideal for high-throughput scenarios where:
    /// - **Fire-and-forget** semantics are required
    /// - Message loss is acceptable (e.g., metrics, logs, telemetry)
    /// - **Maximum throughput** is the priority over reliability
    /// - Latency is critical (< 10μs send overhead)
    ///
    /// # Use Cases
    /// - Log collection and aggregation
    /// - Metrics reporting
    /// - Real-time telemetry
    /// - High-frequency event streaming
    pub async fn send_oneway_unbounded(
        &mut self,
        addr: &CheetahString,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.remoting_client.invoke_oneway_unbounded(addr.clone(), request);
        Ok(())
    }

    pub async fn send_message_simple<T>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait,
    {
        self.send_message(
            addr,
            broker_name,
            msg,
            request_header,
            timeout_millis,
            communication_mode,
            None,
            None,
            None,
            0,
            context,
            producer,
        )
        .await
    }

    async fn send_message_sync<T>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        T: MessageTrait,
    {
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        self.process_send_response(broker_name, msg, &response, addr)
    }

    async fn send_message_async<T: MessageTrait>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) {
        // Extract message metadata before spawning (msg cannot be moved)
        let msg_topic = msg.topic().clone();
        let is_batch_message = msg.as_any().downcast_ref::<MessageBatch>().is_some();

        // For MessageBatch, pre-compute combined uniq_id from all messages
        let msg_uniq_id = if is_batch_message {
            if let Some(batch) = msg.as_any().downcast_ref::<MessageBatch>() {
                let mut combined_id = String::new();
                for msg in &batch.messages {
                    if !combined_id.is_empty() {
                        combined_id.push(',');
                    }
                    if let Some(id) = MessageClientIDSetter::get_uniq_id(msg) {
                        combined_id.push_str(id.as_str());
                    }
                }
                if combined_id.is_empty() {
                    None
                } else {
                    Some(CheetahString::from_string(combined_id))
                }
            } else {
                None
            }
        } else {
            MessageClientIDSetter::get_uniq_id(msg)
        };

        // Clone all necessary data for background task
        let remoting_client = self.remoting_client.clone();
        let client_config = self.client_config.clone();
        let current_addr = addr.clone();
        let current_broker_name = broker_name.clone();
        let current_request = request;
        let topic_publish_info_cloned = topic_publish_info.cloned();
        let instance_cloned = instance.clone();
        let mq_fault_strategy = producer.mq_fault_strategy.clone();
        let callback_executor = producer.producer_config().callback_executor().cloned();

        // Clone the context data that we need for hook execution
        // We'll use the execute_send_message_hook_after method which requires a context
        let context_data = context.as_ref().map(|c| AsyncSendHookContext {
            producer_group: c.producer_group.as_ref().cloned(),
            broker_addr: c.broker_addr.as_ref().cloned(),
            born_host: c.born_host.as_ref().cloned(),
            communication_mode: c.communication_mode,
            msg_type: c.msg_type,
            namespace: c.namespace.as_ref().cloned(),
            mq_trace_context: c.mq_trace_context.clone(),
            producer: c.producer.clone(),
            mq: c.mq.cloned(),
            message_trace_snapshot: c.message_trace_snapshot.clone(),
            trace_start_time: c.trace_start_time,
        });

        Self::send_message_async_impl(
            remoting_client,
            client_config,
            mq_fault_strategy,
            current_addr,
            current_broker_name,
            msg_topic,
            msg_uniq_id,
            is_batch_message,
            timeout_millis,
            current_request,
            send_callback,
            topic_publish_info_cloned,
            instance_cloned,
            retry_times_when_send_failed,
            context_data,
            callback_executor,
        )
        .await;
    }

    /// Background task implementation for async message sending.
    #[allow(clippy::type_complexity)]
    async fn send_message_async_impl(
        remoting_client: Arc<RocketmqDefaultClient<ClientRemotingProcessor>>,
        client_config: Arc<ClientConfig>,
        mq_fault_strategy: ArcMut<MQFaultStrategy>,
        mut current_addr: CheetahString,
        mut current_broker_name: CheetahString,
        msg_topic: CheetahString,
        msg_uniq_id: Option<CheetahString>,
        is_batch_message: bool,
        timeout_millis: u64,
        current_request: RemotingCommand,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context_data: Option<AsyncSendHookContext>,
        callback_executor: Option<tokio::runtime::Handle>,
    ) {
        let begin_start_time_all = Instant::now();
        let mut retry_count = 0_u32;
        let mut retry_request = AsyncRetryRequest::new(current_request);

        loop {
            let elapsed = (Instant::now() - begin_start_time_all).as_millis() as u64;
            if elapsed >= timeout_millis {
                let err = rocketmq_error::RocketMQError::Timeout {
                    operation: "sendMessageAsync",
                    timeout_ms: timeout_millis,
                };
                Self::execute_async_send_hook_after(&context_data, None, Some(Self::context_error(err.to_string())));
                Self::notify_send_callback_exception(&send_callback, &callback_executor, &err);
                return;
            }

            let remaining_timeout = timeout_millis - elapsed;
            let begin_attempt_time = Instant::now();
            let keep_request_for_retry = retry_count < retry_times_when_send_failed;
            let attempt_request = retry_request.next_attempt(keep_request_for_retry);
            let result = remoting_client
                .invoke_request(Some(&current_addr), attempt_request, remaining_timeout)
                .await;
            let cost = (Instant::now() - begin_attempt_time).as_millis() as u64;

            match result {
                Ok(response) => {
                    // Determine send status
                    let response_code = ResponseCode::from(response.code());
                    let send_status = match response_code {
                        ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
                        ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
                        ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
                        ResponseCode::Success => SendStatus::SendOk,
                        _ => {
                            // Non-success response: update fault and call callback with an error
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, true, true)
                                .await;
                            let err_obj = mq_client_err!(
                                response.code(),
                                response.remark().map_or("".to_string(), |s| s.to_string())
                            );
                            Self::execute_async_send_hook_after(
                                &context_data,
                                None,
                                Some(Self::context_error(err_obj.to_string())),
                            );
                            Self::notify_send_callback_exception(&send_callback, &callback_executor, &err_obj);
                            return;
                        }
                    };

                    // Try to decode response header and build SendResult
                    match response.decode_command_custom_header_fast::<SendMessageResponseHeader>() {
                        Ok(response_header) => {
                            let mut topic = msg_topic.to_string();
                            if let Some(ns) = client_config.get_namespace_v2() {
                                if !ns.is_empty() {
                                    topic =
                                        NamespaceUtil::without_namespace_with_namespace(topic.as_str(), ns.as_str());
                                }
                            }
                            let message_queue = MessageQueue::from_parts(
                                topic.as_str(),
                                &current_broker_name,
                                response_header.queue_id(),
                            );
                            let region_id = response
                                .ext_fields()
                                .and_then(|m| m.get(MessageConst::PROPERTY_MSG_REGION).map(|s| s.to_string()))
                                .unwrap_or_else(|| mix_all::DEFAULT_TRACE_REGION_ID.to_string());
                            let trace_on = trace_on_from_ext_fields(response.ext_fields());
                            let queue_offset = match java_long_to_u64_field(
                                "sendMessage",
                                "queueOffset",
                                response_header.queue_offset(),
                            ) {
                                Ok(queue_offset) => queue_offset,
                                Err(err_obj) => {
                                    mq_fault_strategy
                                        .update_fault_item(current_broker_name.clone(), cost, true, true)
                                        .await;
                                    Self::execute_async_send_hook_after(
                                        &context_data,
                                        None,
                                        Some(Self::context_error(err_obj.to_string())),
                                    );
                                    Self::notify_send_callback_exception(&send_callback, &callback_executor, &err_obj);
                                    return;
                                }
                            };

                            let send_result = SendResult {
                                send_status,
                                msg_id: msg_uniq_id.clone(),
                                offset_msg_id: Some(response_header.msg_id().to_string()),
                                message_queue: Some(message_queue),
                                queue_offset,
                                transaction_id: response_header.transaction_id().map(|s| s.to_string()),
                                recall_handle: response_header.recall_handle().map(|s| s.to_string()),
                                region_id: Some(region_id),
                                trace_on,
                                ..Default::default()
                            };

                            // Success: update fault item and invoke callback
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, false, true)
                                .await;
                            Self::execute_async_send_hook_after(&context_data, Some(&send_result), None);
                            Self::notify_send_callback_success(&send_callback, &callback_executor, &send_result);
                            return;
                        }
                        Err(_) => {
                            mq_fault_strategy
                                .update_fault_item(current_broker_name.clone(), cost, true, true)
                                .await;
                            let err_obj = mq_client_err!("decode SendMessageResponseHeader failed".to_string());
                            Self::execute_async_send_hook_after(
                                &context_data,
                                None,
                                Some(Self::context_error(err_obj.to_string())),
                            );
                            Self::notify_send_callback_exception(&send_callback, &callback_executor, &err_obj);
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("send message async error: {:?}", e);
                    mq_fault_strategy
                        .update_fault_item(current_broker_name.clone(), cost, true, true)
                        .await;

                    let retry_elapsed = (Instant::now() - begin_start_time_all).as_millis() as u64;
                    let has_retry_budget = retry_count < retry_times_when_send_failed
                        && retry_elapsed < timeout_millis
                        && Self::should_retry_async_send_error(&e);
                    if has_retry_budget {
                        retry_count += 1;
                        if let Some((retry_addr, retry_broker_name)) = Self::select_async_retry_target(
                            &mq_fault_strategy,
                            topic_publish_info.as_ref(),
                            instance.as_ref(),
                            &current_broker_name,
                            &current_addr,
                        )
                        .await
                        {
                            warn!(
                                "async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}",
                                retry_count, msg_topic, retry_addr, retry_broker_name
                            );
                            current_addr = retry_addr;
                            current_broker_name = retry_broker_name;
                            retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
                            continue;
                        }
                    }

                    Self::execute_async_send_hook_after(&context_data, None, Some(Self::context_error(e.to_string())));
                    Self::notify_send_callback_exception(&send_callback, &callback_executor, &e);
                    return;
                }
            }
        }
    }

    fn select_async_retry_queue(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: Option<&TopicPublishInfo>,
        broker_name: &CheetahString,
    ) -> Option<MessageQueue> {
        topic_publish_info.and_then(|topic_publish_info| {
            mq_fault_strategy.select_one_message_queue(topic_publish_info, Some(broker_name), false)
        })
    }

    fn should_retry_async_send_error(error: &rocketmq_error::RocketMQError) -> bool {
        crate::common::retry_decision::should_retry_async_send_error(error)
    }

    async fn select_async_retry_target(
        mq_fault_strategy: &MQFaultStrategy,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<&ArcMut<MQClientInstance>>,
        broker_name: &CheetahString,
        current_addr: &CheetahString,
    ) -> Option<(CheetahString, CheetahString)> {
        let mut retry_broker_name = broker_name.clone();
        if let Some(mq_chosen) = Self::select_async_retry_queue(mq_fault_strategy, topic_publish_info, broker_name) {
            retry_broker_name = if let Some(instance) = instance {
                instance.get_broker_name_from_message_queue(&mq_chosen).await
            } else {
                mq_chosen.broker_name().clone()
            };
        }

        let retry_addr = instance
            .and_then(|instance| instance.find_broker_address_in_publish(retry_broker_name.as_ref()))
            .unwrap_or_else(|| current_addr.clone());
        Some((retry_addr, retry_broker_name))
    }

    fn execute_async_send_hook_after(
        context_data: &Option<AsyncSendHookContext>,
        send_result: Option<&SendResult>,
        exception: Option<Arc<RocketMQError>>,
    ) {
        let Some(context_data) = context_data.as_ref() else {
            return;
        };
        let Some(producer) = context_data.producer.as_ref() else {
            return;
        };

        let context = Some(SendMessageContext {
            producer_group: context_data.producer_group.clone(),
            broker_addr: context_data.broker_addr.clone(),
            born_host: context_data.born_host.clone(),
            communication_mode: context_data.communication_mode,
            send_result,
            exception,
            mq_trace_context: context_data.mq_trace_context.clone(),
            producer: Some(producer.clone()),
            msg_type: context_data.msg_type,
            namespace: context_data.namespace.clone(),
            mq: context_data.mq.as_ref(),
            message_trace_snapshot: context_data.message_trace_snapshot.clone(),
            trace_start_time: context_data.trace_start_time,
            ..Default::default()
        });
        producer.execute_send_message_hook_after(&context);
    }

    fn context_error(message: String) -> Arc<RocketMQError> {
        Arc::new(RocketMQError::response_process_failed("send_callback", message))
    }

    fn spawn_api_background_task<F>(
        thread_name: &'static str,
        tracker: &TaskTracker,
        shutdown_token: &CancellationToken,
        task: F,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        if shutdown_token.is_cancelled() {
            return;
        }

        let shutdown_token = shutdown_token.clone();
        let tracked_task = tracker.track_future(async move {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => {},
                _ = task => {},
            }
        });

        if let Err(error) = spawn_client_task(thread_name, tracked_task) {
            warn!("Failed to spawn {} background task: {}", thread_name, error);
        }
    }

    fn notify_send_callback_success(
        send_callback: &Option<ArcSendCallback>,
        callback_executor: &Option<tokio::runtime::Handle>,
        send_result: &SendResult,
    ) {
        let Some(callback) = send_callback.as_ref().cloned() else {
            return;
        };

        if let Some(executor) = callback_executor.as_ref() {
            let send_result = send_result.clone();
            executor.spawn(async move {
                callback.on_success(&send_result);
            });
        } else {
            callback.on_success(send_result);
        }
    }

    fn notify_send_callback_exception(
        send_callback: &Option<ArcSendCallback>,
        callback_executor: &Option<tokio::runtime::Handle>,
        error: &RocketMQError,
    ) {
        let Some(callback) = send_callback.as_ref().cloned() else {
            return;
        };

        if let Some(executor) = callback_executor.as_ref() {
            let message = error.to_string();
            executor.spawn(async move {
                let error = RocketMQError::response_process_failed("send_callback", message);
                callback.on_exception(&error);
            });
        } else {
            callback.on_exception(error);
        }
    }

    fn process_send_response<T>(
        &mut self,
        broker_name: &CheetahString,
        msg: &T,
        response: &RemotingCommand,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        T: MessageTrait,
    {
        let response_code = ResponseCode::from(response.code());
        let send_status = match response_code {
            ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
            ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
            ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
            ResponseCode::Success => SendStatus::SendOk,
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        };
        let response_header = response.decode_command_custom_header_fast::<SendMessageResponseHeader>()?;
        let mut topic = msg.topic().to_string();
        if let Some(ns) = self.client_config.get_namespace_v2() {
            if !ns.is_empty() {
                topic = NamespaceUtil::without_namespace_with_namespace(topic.as_str(), ns.as_str());
            }
        }
        let message_queue = MessageQueue::from_parts(topic.as_str(), broker_name, response_header.queue_id());
        let mut uniq_msg_id = MessageClientIDSetter::get_uniq_id(msg);
        let msgs = msg.as_any().downcast_ref::<MessageBatch>();

        if let (Some(msgs), true) = (msgs, response_header.batch_uniq_id().is_none()) {
            let mut sb = String::new();
            for msg in &msgs.messages {
                if let Some(uniq_id) = MessageClientIDSetter::get_uniq_id(msg) {
                    if !sb.is_empty() {
                        sb.push(',');
                    }
                    sb.push_str(uniq_id.as_str());
                } else {
                    warn!(
                        "skip empty uniq id while building batch send result for topic={}",
                        msg.topic()
                    );
                }
            }
            if !sb.is_empty() {
                uniq_msg_id = Some(CheetahString::from_string(sb));
            }
        }

        let region_id = response
            .ext_fields()
            .and_then(|fields| fields.get(MessageConst::PROPERTY_MSG_REGION))
            .map_or(mix_all::DEFAULT_TRACE_REGION_ID.to_string(), |s| s.to_string());
        let trace_on = trace_on_from_ext_fields(response.ext_fields());
        let queue_offset = java_long_to_u64_field("sendMessage", "queueOffset", response_header.queue_offset())?;
        let send_result = SendResult {
            send_status,
            msg_id: uniq_msg_id,
            offset_msg_id: Some(response_header.msg_id().to_string()),
            message_queue: Some(message_queue),
            queue_offset,
            transaction_id: response_header.transaction_id().map(|s| s.to_string()),
            recall_handle: response_header.recall_handle().map(|s| s.to_string()),
            region_id: Some(region_id),
            trace_on,
            ..Default::default()
        };

        Ok(send_result)
    }

    async fn prepare_retry<T: MessageTrait>(
        &self,
        broker_name: &CheetahString,
        msg: &T,
        request: &mut RemotingCommand,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<&ArcMut<MQClientInstance>>,
        producer: &DefaultMQProducerImpl,
    ) -> Option<(CheetahString, CheetahString)> {
        let mut retry_broker_name = broker_name.clone();

        if let Some(topic_publish_info) = topic_publish_info {
            let mq_chosen = producer.select_one_message_queue(topic_publish_info, Some(&retry_broker_name), false);
            let Some(mq_chosen) = mq_chosen.as_ref() else {
                warn!(
                    "prepare async retry failed: no message queue selected for topic={}",
                    msg.topic()
                );
                return None;
            };
            if let Some(instance) = instance {
                retry_broker_name = instance.get_broker_name_from_message_queue(mq_chosen).await;
            }
        }

        if let Some(instance) = instance {
            if let Some(addr) = instance.find_broker_address_in_publish(retry_broker_name.as_ref()) {
                return Some((addr, retry_broker_name));
            }
        }

        None
    }

    pub async fn send_heartbeat(
        &mut self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<(i32, Option<RemotingCommand>)> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok((response.version(), Some(response)));
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn send_heartbeat_async(
        &mut self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.send_heartbeat(addr, heartbeat_data, timeout_millis)
            .await
            .map(|(version, _)| version)
    }

    pub async fn send_heartbeat_oneway(
        &mut self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        self.remoting_client
            .invoke_request_oneway(addr, request, timeout_millis)
            .await;
        Ok(())
    }

    pub async fn register_client(
        &mut self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        Ok(ResponseCode::from(response.code()) == ResponseCode::Success)
    }

    pub async fn send_heartbeat_v2(
        &mut self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HeartbeatV2Result> {
        let request = heartbeat_request(heartbeat_data, self.client_config.language)?;
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(HeartbeatV2Result::from_response(&response));
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn check_client_in_broker(
        &mut self,
        broker_addr: &str,
        consumer_group: &str,
        client_id: &str,
        subscription_data: &SubscriptionData,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request = RemotingCommand::create_remoting_command(RequestCode::CheckClientConfig);
        let body = CheckClientRequestBody::new(
            client_id.to_string(),
            consumer_group.to_string(),
            subscription_data.clone(),
        );
        request.set_body_mut_ref(body.encode()?);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub async fn recall_message(
        &mut self,
        addr: &str,
        request_header: RecallMessageRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<String> {
        let request = RemotingCommand::create_request_command(RequestCode::RecallMessage, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(CheetahString::from_slice(addr).as_ref()), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                // Decode RecallMessageResponseHeader from response
                match response.decode_command_custom_header::<RecallMessageResponseHeader>() {
                    Ok(header) => Ok(header.msg_id().to_string()),
                    Err(_) => {
                        // Fallback to remark if header decode fails
                        Ok(response.remark().map_or(String::new(), |s| s.to_string()))
                    }
                }
            }
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or(String::new(), |s| s.to_string()),
                addr.to_string()
            )),
        }
    }

    pub async fn recall_message_async<F>(
        &self,
        addr: &CheetahString,
        request_header: RecallMessageRequestHeader,
        timeout_millis: u64,
        invoke_callback: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: FnOnce(rocketmq_error::RocketMQResult<RemotingCommand>) + Send,
    {
        let request = RemotingCommand::create_request_command(RequestCode::RecallMessage, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        invoke_callback(response);
        Ok(())
    }

    pub async fn notification(
        &self,
        broker_addr: &CheetahString,
        request_header: NotificationRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.notification_with_polling_stats(broker_addr, request_header, timeout_millis)
            .await
            .map(|result| result.is_has_msg())
    }

    pub async fn notification_with_polling_stats(
        &self,
        broker_addr: &CheetahString,
        request_header: NotificationRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<NotifyResult> {
        let request = notification_request(request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => notify_result_from_response(&response),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn get_consumer_id_list_by_group(
        &mut self,
        addr: &str,
        consumer_group: &str,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Vec<CheetahString>> {
        let request_header = GetConsumerListByGroupRequestHeader {
            consumer_group: CheetahString::from_slice(consumer_group),
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerListByGroup, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response.body();
                if let Some(body) = response.body() {
                    return match GetConsumerListByGroupResponseBody::decode(body) {
                        Ok(value) => Ok(value.consumer_id_list),
                        Err(e) => Err(mq_client_err!(response
                            .remark()
                            .map_or("".to_string(), |s| s.to_string()))),
                    };
                }
            }
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ));
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_consumer_connection_list(
        &mut self,
        addr: &str,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection>
    {
        let request_header = GetConsumerConnectionListRequestHeader {
            consumer_group,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerConnectionList, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    return ConsumerConnection::decode(body);
                }
            }
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ));
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_producer_connection_list(
        &mut self,
        addr: &str,
        producer_group: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        let request_header = GetProducerConnectionListRequestHeader {
            producer_group,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetProducerConnectionList, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    return ProducerConnection::decode(body);
                }
            }
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ));
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn invoke_broker_to_get_consumer_status(
        &mut self,
        addr: &str,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, i64>>> {
        let request_header = GetConsumerStatusRequestHeader {
            topic,
            group,
            client_addr: if client_addr.is_empty() {
                None
            } else {
                Some(client_addr)
            },
            rpc_request_header: None,
        };
        let request =
            RemotingCommand::create_request_command(RequestCode::InvokeBrokerToGetConsumerStatus, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    if let Some(status_body) = GetConsumerStatusBody::decode(body) {
                        return Ok(status_body.consumer_table);
                    }
                }
                Ok(HashMap::new())
            }
            _ => Err(mq_client_err!(
                response.code(),
                format!(
                    "invoke broker to get consumer status failed, remark={}",
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                )
            )),
        }
    }

    pub async fn get_all_producer_info(
        &mut self,
        addr: &str,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetAllProducerInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    return ProducerTableInfo::decode(body);
                }
            }
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ));
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn update_consumer_offset_oneway(
        &mut self,
        addr: &str,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateConsumerOffset, request_header);
        self.remoting_client
            .invoke_request_oneway(
                mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref(),
                request,
                timeout_millis,
            )
            .await;
        Ok(())
    }

    pub async fn update_consumer_offset_one_way(
        &mut self,
        addr: &str,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.update_consumer_offset_oneway(addr, request_header, timeout_millis)
            .await
    }

    pub async fn update_consumer_offset(
        &mut self,
        addr: &CheetahString,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateConsumerOffset, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        } else {
            Ok(())
        }
    }

    pub async fn update_consumer_offset_async(
        &mut self,
        addr: &CheetahString,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.update_consumer_offset(addr, request_header, timeout_millis).await
    }

    pub async fn query_consumer_offset(
        &mut self,
        addr: &str,
        request_header: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumerOffset, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response.decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()?;
                return response_header.offset.ok_or_else(|| {
                    client_broker_err!(
                        response.code(),
                        "QueryConsumerOffset response header missing offset".to_string(),
                        addr.to_string()
                    )
                });
            }
            ResponseCode::QueryNotFound => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
            _ => {}
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn query_consumer_offset_with_future(
        &mut self,
        addr: &str,
        request_header: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.query_consumer_offset(addr, request_header, timeout_millis).await
    }

    pub async fn query_message(
        this: &ArcMut<Self>,
        addr: &CheetahString,
        request_header: QueryMessageRequestHeader,
        unique_key_flag: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<(QueryMessageResponseHeader, Option<bytes::Bytes>)>> {
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, request_header);
        if unique_key_flag {
            request.ensure_ext_fields_initialized();
            request.add_ext_field(mix_all::UNIQUE_MSG_QUERY_FLAG, "true");
        }
        let response = this
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response
                    .decode_command_custom_header::<QueryMessageResponseHeader>()
                    .map_err(|e| {
                        RocketMQError::response_process_failed("decode QueryMessageResponseHeader", e.to_string())
                    })?;
                let body = response.body().cloned();
                Ok(Some((response_header, body)))
            }
            ResponseCode::QueryNotFound => Ok(None),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            )),
        }
    }

    pub async fn pull_message<PCB>(
        mut this: ArcMut<Self>,
        addr: CheetahString,
        request_header: PullMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        pull_callback: PCB,
    ) -> rocketmq_error::RocketMQResult<Option<PullResultExt>>
    where
        PCB: PullCallback + 'static,
    {
        let request = if PullSysFlag::has_lite_pull_flag(request_header.sys_flag as u32) {
            RemotingCommand::create_request_command(RequestCode::LitePullMessage, request_header)
        } else {
            RemotingCommand::create_request_command(RequestCode::PullMessage, request_header)
        };
        match communication_mode {
            CommunicationMode::Sync => {
                let result_ext = this.pull_message_sync(&addr, request, timeout_millis).await?;
                Ok(Some(result_ext))
            }
            CommunicationMode::Async => {
                let tracker = this.background_tasks.clone();
                let shutdown_token = this.background_shutdown.clone();
                Self::spawn_api_background_task(
                    "rocketmq-client-pull-message-async",
                    &tracker,
                    &shutdown_token,
                    async move {
                        let _ = this
                            .pull_message_async(&addr, request, timeout_millis, pull_callback)
                            .await;
                    },
                );
                Ok(None)
            }
            CommunicationMode::Oneway => Ok(None),
        }
    }

    async fn pull_message_sync(
        &mut self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<PullResultExt> {
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        self.process_pull_response(response, addr).await
    }

    async fn pull_message_async<PCB>(
        &mut self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
        mut pull_callback: PCB,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PCB: PullCallback,
    {
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let result = self.process_pull_response(response, addr).await;
                match result {
                    Ok(pull_result) => {
                        pull_callback.on_success(pull_result).await;
                    }
                    Err(error) => {
                        pull_callback.on_exception(error);
                    }
                }
            }
            Err(err) => {
                pull_callback.on_exception(err);
            }
        }
        Ok(())
    }

    async fn process_pull_response(
        &mut self,
        mut response: RemotingCommand,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<PullResultExt> {
        let pull_status = match ResponseCode::from(response.code()) {
            ResponseCode::Success => PullStatus::Found,
            ResponseCode::PullNotFound => PullStatus::NoNewMsg,
            ResponseCode::PullRetryImmediately => PullStatus::NoMatchedMsg,
            ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        };
        let response_header = response.decode_command_custom_header::<PullMessageResponseHeader>()?;
        let next_begin_offset =
            java_long_to_u64_field("pullMessage", "nextBeginOffset", response_header.next_begin_offset)?;
        let min_offset = java_long_to_u64_field("pullMessage", "minOffset", response_header.min_offset)?;
        let max_offset = java_long_to_u64_field("pullMessage", "maxOffset", response_header.max_offset)?;
        let pull_result = PullResultExt {
            pull_result: PullResult {
                pull_status,
                next_begin_offset,
                min_offset,
                max_offset,
                msg_found_list: Some(vec![]),
            },
            suggest_which_broker_id: response_header.suggest_which_broker_id,
            message_binary: response.take_body(),
            offset_delta: response_header.offset_delta,
        };
        Ok(pull_result)
    }

    pub async fn consumer_send_message_back(
        &mut self,
        addr: &str,
        broker_name: Option<&str>,
        msg: &MessageExt,
        consumer_group: &str,
        delay_level: i32,
        timeout_millis: u64,
        max_consume_retry_times: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let header = Self::consumer_send_message_back_request_header(
            msg,
            broker_name,
            consumer_group,
            delay_level,
            max_consume_retry_times,
        );

        let request_command = RemotingCommand::create_request_command(RequestCode::ConsumerSendMsgBack, header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request_command,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        }
    }

    fn consumer_send_message_back_request_header(
        msg: &MessageExt,
        broker_name: Option<&str>,
        consumer_group: &str,
        delay_level: i32,
        max_consume_retry_times: i32,
    ) -> ConsumerSendMsgBackRequestHeader {
        ConsumerSendMsgBackRequestHeader {
            offset: msg.commit_log_offset,
            group: CheetahString::from_slice(consumer_group),
            delay_level,
            origin_msg_id: Some(CheetahString::from_slice(msg.msg_id.as_str())),
            origin_topic: Some(CheetahString::from_slice(msg.topic())),
            unit_mode: false,
            max_reconsume_times: Some(max_consume_retry_times),
            rpc_request_header: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: broker_name.map(CheetahString::from_slice),
                oneway: None,
            }),
        }
    }

    pub async fn send_message_back_async(
        &mut self,
        addr: &CheetahString,
        request_header: ConsumerSendMsgBackRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request = RemotingCommand::create_request_command(RequestCode::ConsumerSendMsgBack, request_header);
        self.remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
    }

    pub async fn unregister_client(
        &mut self,
        addr: &CheetahString,
        client_id: CheetahString,
        producer_group: Option<CheetahString>,
        consumer_group: Option<CheetahString>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = UnregisterClientRequestHeader {
            client_id,
            producer_group,
            consumer_group,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::UnregisterClient, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        }
    }

    pub async fn unlock_batch_mq(
        &mut self,
        addr: &CheetahString,
        request_body: UnlockBatchRequestBody,
        timeout_millis: u64,
        oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, UnlockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode()?);
        if oneway {
            self.remoting_client
                .invoke_request_oneway(addr, request, timeout_millis)
                .await;
            Ok(())
        } else {
            let response = self
                .remoting_client
                .invoke_request(
                    Some(&mix_all::broker_vip_channel(
                        self.client_config.vip_channel_enabled,
                        addr,
                    )),
                    request,
                    timeout_millis,
                )
                .await?;
            if ResponseCode::from(response.code()) == ResponseCode::Success {
                Ok(())
            } else {
                Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        }
    }

    pub async fn unlock_batch_mq_oneway(
        &mut self,
        addr: &CheetahString,
        request_body: UnlockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.unlock_batch_mq(addr, request_body, timeout_millis, true).await
    }

    pub async fn lock_batch_mq(
        &mut self,
        addr: &str,
        request_body: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::LockBatchMq, LockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode()?);
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.body() {
                LockBatchResponseBody::decode(body.as_ref())
                    .map(|body| body.lock_ok_mq_set)
                    .map_err(|e| client_broker_err!(response.code(), e.to_string(), addr.to_string()))
            } else {
                Err(client_broker_err!(
                    response.code(),
                    "Response body is empty".to_string(),
                    addr.to_string()
                ))
            }
        } else {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        }
    }

    pub async fn lock_batch_mq_with_future(
        &mut self,
        addr: &str,
        request_body: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        self.lock_batch_mq(addr, request_body, timeout_millis).await
    }

    pub async fn end_transaction_oneway(
        &mut self,
        addr: &CheetahString,
        request_header: EndTransactionRequestHeader,
        remark: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::EndTransaction, request_header).set_remark(remark);

        self.remoting_client
            .invoke_request_oneway(addr, request, timeout_millis)
            .await;
        Ok(())
    }

    pub async fn get_max_offset(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from_slice(message_queue.topic_str()),
            queue_id: message_queue.queue_id(),
            committed: false,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.broker_name())),
                    ..Default::default()
                }),
                lo: None,
            }),
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetMaxOffset, request_header);

        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let response_header = response.decode_command_custom_header::<GetMaxOffsetResponseHeader>()?;
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_min_offset(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMinOffsetRequestHeader {
            topic: CheetahString::from_slice(message_queue.topic_str()),
            queue_id: message_queue.queue_id(),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.broker_name())),
                    ..Default::default()
                }),
                lo: None,
            }),
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetMinOffset, request_header);

        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let response_header = response.decode_command_custom_header::<GetMinOffsetResponseHeader>()?;
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_earliest_msg_store_time(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetEarliestMsgStoretimeRequestHeader {
            topic: CheetahString::from_slice(message_queue.topic_str()),
            queue_id: message_queue.queue_id(),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.broker_name())),
                    ..Default::default()
                }),
                lo: None,
            }),
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetEarliestMsgStoreTime, request_header);

        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let response_header = response.decode_command_custom_header::<GetEarliestMsgStoretimeResponseHeader>()?;
            return Ok(response_header.timestamp);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_earliest_msg_storetime(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.get_earliest_msg_store_time(addr, message_queue, timeout_millis)
            .await
    }

    /// Searches for the queue offset whose store timestamp is closest to `timestamp`.
    ///
    /// When `boundary_type` is [`BoundaryType::Lower`], the returned offset is the earliest one
    /// whose store timestamp is greater than or equal to `timestamp`.  When
    /// [`BoundaryType::Upper`], the latest such offset is returned.
    ///
    /// Mirrors `MQClientAPIImpl.searchOffset` in the Java implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker returns a non-success response code or is unreachable.
    pub async fn search_offset_by_timestamp(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timestamp: i64,
        boundary_type: BoundaryType,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = SearchOffsetRequestHeader {
            topic: message_queue.topic().clone(),
            lite_topic: None,
            queue_id: message_queue.queue_id(),
            timestamp,
            boundary_type,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(message_queue.broker_name().clone()),
                    ..Default::default()
                }),
                lo: None,
            }),
        };
        let request = RemotingCommand::create_request_command(RequestCode::SearchOffsetByTimestamp, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let response_header = response.decode_command_custom_header::<SearchOffsetResponseHeader>()?;
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn search_offset(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timestamp: i64,
        boundary_type: BoundaryType,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.search_offset_by_timestamp(addr, message_queue, timestamp, boundary_type, timeout_millis)
            .await
    }

    pub async fn set_message_request_mode(
        &mut self,
        broker_addr: &CheetahString,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        mode: MessageRequestMode,
        pop_share_queue_num: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let body = SetMessageRequestModeRequestBody {
            topic: topic.clone(),
            consumer_group: consumer_group.clone(),
            mode,
            pop_share_queue_num,
        };
        let request =
            RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestMode).set_body(body.encode()?);
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    broker_addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().cloned().unwrap_or_default().to_string()
            ));
        }
        Ok(())
    }

    pub async fn query_assignment(
        &mut self,
        addr: &CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        strategy_name: CheetahString,
        message_model: MessageModel,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<HashSet<MessageQueueAssignment>>> {
        let request_body = QueryAssignmentRequestBody {
            topic,
            consumer_group,
            client_id,
            strategy_name,
            message_model,
        };
        let request = RemotingCommand::new_request(RequestCode::QueryAssignment, request_body.encode()?);
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout,
            )
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response.body();
            if let Some(body) = body {
                let assignment = QueryAssignmentResponseBody::decode(body.as_ref());
                if let Ok(assignment) = assignment {
                    return Ok(Some(assignment.message_queue_assignments));
                }
            }
            return Ok(None);
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn change_invisible_time_async(
        &self,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: ChangeInvisibleTimeRequestHeader,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        let offset = request_header.offset;
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let request = RemotingCommand::create_request_command(RequestCode::ChangeMessageInvisibleTime, request_header);
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let response_header = response.decode_command_custom_header::<ChangeInvisibleTimeResponseHeader>()?;
                let ack_result = if ResponseCode::from(response.code()) == ResponseCode::Success {
                    AckResult {
                        status: AckStatus::Ok,
                        pop_time: response_header.pop_time as i64,
                        extra_info: CheetahString::from_string(format!(
                            "{}{}{}",
                            ExtraInfoUtil::build_extra_info(
                                offset,
                                response_header.pop_time as i64,
                                response_header.invisible_time,
                                response_header.revive_qid,
                                &topic,
                                broker_name,
                                queue_id,
                            ),
                            MessageConst::KEY_SEPARATOR,
                            offset
                        )),
                    }
                } else {
                    AckResult {
                        status: AckStatus::NotExist,
                        ..Default::default()
                    }
                };
                ack_callback.on_success(ack_result);
            }
            Err(e) => {
                ack_callback.on_exception(e);
            }
        };
        Ok(())
    }

    pub async fn pop_message_async<PC>(
        &self,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: PopMessageRequestHeader,
        timeout_millis: u64,
        mut pop_callback: PC,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let topic = request_header.topic.clone();
        let order = request_header.order.unwrap_or_default();
        let request = RemotingCommand::create_request_command(RequestCode::PopMessage, request_header);
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let result = self.process_pop_response(broker_name, response, &topic, order);
                match result {
                    Ok(pop_result) => {
                        pop_callback.on_success(pop_result).await;
                    }
                    Err(e) => {
                        pop_callback.on_error(e);
                    }
                }
            }
            Err(e) => {
                pop_callback.on_error(e);
            }
        }
        Ok(())
    }

    pub async fn pop_lite_message_async<PC>(
        &self,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: PopLiteMessageRequestHeader,
        timeout_millis: u64,
        mut pop_callback: PC,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let bind_topic = request_header.topic.clone();
        let request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, request_header);
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let result = self.process_pop_lite_response(broker_name, response, &bind_topic);
                match result {
                    Ok(pop_result) => {
                        pop_callback.on_success(pop_result).await;
                    }
                    Err(e) => {
                        pop_callback.on_error(e);
                    }
                }
            }
            Err(e) => {
                pop_callback.on_error(e);
            }
        }
        Ok(())
    }

    fn process_pop_response(
        &self,
        broker_name: &CheetahString,
        mut response: RemotingCommand,
        topic: &CheetahString,
        is_order: bool,
    ) -> rocketmq_error::RocketMQResult<PopResult> {
        let response_code = ResponseCode::from(response.code());
        let (pop_status, msg_found_list) = match response_code {
            ResponseCode::Success => {
                let raw_response_code = response.code();
                let body = response
                    .get_body_mut()
                    .ok_or_else(|| client_broker_err!(raw_response_code, "PopMessage response body is empty"))?;
                let messages = MessageDecoder::decodes_batch(
                    body,
                    self.client_config.decode_read_body,
                    self.client_config.decode_decompress_body,
                );
                (PopStatus::Found, messages)
            }
            ResponseCode::PollingFull => (PopStatus::PollingFull, vec![]),
            ResponseCode::PollingTimeout | ResponseCode::PullNotFound => (PopStatus::PollingNotFound, vec![]),
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().cloned().unwrap_or_default()
                ))
            }
        };
        let mut pop_result = PopResult {
            pop_status,
            msg_found_list: Some(msg_found_list),
            ..Default::default()
        };
        let response_header = response.decode_command_custom_header::<PopMessageResponseHeader>()?;
        pop_result.rest_num = response_header.rest_num;
        if pop_result.pop_status != PopStatus::Found {
            return Ok(pop_result);
        }
        // it is a pop command if pop time greater than 0, we should set the check point info to
        // extraInfo field
        pop_result.invisible_time = response_header.invisible_time;
        pop_result.pop_time = response_header.pop_time;
        let start_offset_info = ExtraInfoUtil::parse_start_offset_info(
            response_header
                .start_offset_info
                .as_ref()
                .unwrap_or(&CheetahString::from_slice("")),
        )?;
        let msg_offset_info = ExtraInfoUtil::parse_msg_offset_info(
            response_header
                .msg_offset_info
                .as_ref()
                .unwrap_or(&CheetahString::from_slice("")),
        )?;
        let order_count_info = ExtraInfoUtil::parse_order_count_info(
            response_header
                .order_count_info
                .as_ref()
                .unwrap_or(&CheetahString::from_slice("")),
        )?;
        let sort_map = Self::build_queue_offset_sorted_map(
            topic.as_str(),
            pop_result
                .msg_found_list
                .as_ref()
                .map_or(&[] as &[MessageExt], |v| v.as_slice()),
        )?;
        let mut map = HashMap::with_capacity(5);
        for message in pop_result.msg_found_list.as_mut().map_or(&mut vec![], |v| v) {
            if start_offset_info.is_empty() {
                let key = CheetahString::from_string(format!("{}{}", message.topic(), message.queue_id() as i64));
                if !map.contains_key(&key) {
                    let extra_info = ExtraInfoUtil::build_extra_info(
                        message.queue_offset(),
                        response_header.pop_time as i64,
                        response_header.invisible_time as i64,
                        response_header.revive_qid as i32,
                        message.topic(),
                        broker_name,
                        message.queue_id(),
                    );
                    map.insert(key.clone(), CheetahString::from_string(extra_info));
                }
                message.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                    CheetahString::from_string(format!(
                        "{}{}{}",
                        map.get(&key).cloned().unwrap_or_default(),
                        MessageConst::KEY_SEPARATOR,
                        message.queue_offset()
                    )),
                );
            } else {
                let ck = message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK));
                if ck.is_none() {
                    let dispatch = message
                        .property(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                        ))
                        .unwrap_or_default();
                    let (queue_offset_key, queue_id_key) = if mix_all::is_lmq(Some(topic.as_str()))
                        && !dispatch.is_empty()
                    {
                        let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let data = message
                            .property(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                            ))
                            .unwrap_or_default();
                        let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let Some(position) = queues.iter().position(|&q| q == topic.as_str()) else {
                            warn!(
                                "LMQ dispatch queue does not contain topic={}, dispatch={}",
                                topic, dispatch
                            );
                            continue;
                        };
                        let Some(offset_value) = queue_offsets.get(position) else {
                            warn!(
                                "LMQ queue offset is missing for topic={}, dispatch={}, offsets={}",
                                topic, dispatch, data
                            );
                            continue;
                        };
                        let Ok(offset) = offset_value.parse::<i64>() else {
                            warn!(
                                "LMQ queue offset is invalid for topic={}, offset={}",
                                topic, offset_value
                            );
                            continue;
                        };
                        let queue_id_key =
                            ExtraInfoUtil::get_start_offset_info_map_key(topic.as_str(), mix_all::LMQ_QUEUE_ID as i64);
                        let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                            topic.as_str(),
                            mix_all::LMQ_QUEUE_ID as i64,
                            offset,
                        );
                        if !sort_map.contains_key(&queue_id_key) {
                            warn!("LMQ start offset info missing for key={}", queue_id_key);
                            continue;
                        }
                        let Some(start_offset) = start_offset_info.get(&queue_id_key).copied() else {
                            warn!("LMQ start offset info missing for key={}", queue_id_key);
                            continue;
                        };
                        let Some(msg_queue_offset) =
                            pop_msg_queue_offset_for_index(&queue_id_key, offset, &sort_map, &msg_offset_info)
                        else {
                            warn!(
                                "LMQ msg offset info missing for key={}, offset={}",
                                queue_id_key, offset
                            );
                            continue;
                        };
                        if msg_queue_offset != offset {
                            warn!(
                                "Queue offset[{}] of msg is strange, not equal to the stored in msg, {:?}",
                                msg_queue_offset, message
                            );
                        }
                        let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
                            start_offset,
                            response_header.pop_time as i64,
                            response_header.invisible_time as i64,
                            response_header.revive_qid as i32,
                            message.topic(),
                            broker_name,
                            mix_all::LMQ_QUEUE_ID as i32,
                            msg_queue_offset,
                        );
                        message.put_property(
                            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                            CheetahString::from_string(extra_info),
                        );
                        (queue_offset_key, queue_id_key)
                    } else {
                        let queue_id_key =
                            ExtraInfoUtil::get_start_offset_info_map_key(message.topic(), message.queue_id() as i64);
                        let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                            message.topic(),
                            message.queue_id() as i64,
                            message.queue_offset(),
                        );
                        let queue_offset = message.queue_offset();
                        if !sort_map.contains_key(&queue_id_key) {
                            warn!("start offset info missing for key={}", queue_id_key);
                            continue;
                        }
                        let Some(start_offset) = start_offset_info.get(&queue_id_key).copied() else {
                            warn!("start offset info missing for key={}", queue_id_key);
                            continue;
                        };
                        let Some(msg_queue_offset) =
                            pop_msg_queue_offset_for_index(&queue_id_key, queue_offset, &sort_map, &msg_offset_info)
                        else {
                            warn!(
                                "msg offset info missing for key={}, offset={}",
                                queue_id_key, queue_offset
                            );
                            continue;
                        };
                        if msg_queue_offset != queue_offset {
                            warn!(
                                "Queue offset[{}] of msg is strange, not equal to the stored in msg, {:?}",
                                msg_queue_offset, message
                            );
                        }
                        let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
                            start_offset,
                            response_header.pop_time as i64,
                            response_header.invisible_time as i64,
                            response_header.revive_qid as i32,
                            message.topic(),
                            broker_name,
                            message.queue_id(),
                            msg_queue_offset,
                        );
                        message.put_property(
                            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                            CheetahString::from_string(extra_info),
                        );
                        (queue_offset_key, queue_id_key)
                    };
                    if is_order && !order_count_info.is_empty() {
                        let mut count = order_count_info.get(&queue_offset_key);
                        if count.is_none() {
                            count = order_count_info.get(&queue_id_key);
                        }
                        if let Some(ct) = count.filter(|ct| **ct > 0) {
                            message.set_reconsume_times(*ct);
                        }
                    }
                }
            }
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                CheetahString::from(response_header.pop_time.to_string()),
            );
            message.broker_name = broker_name.clone();
            message.set_topic(
                NamespaceUtil::without_namespace_with_namespace(
                    topic.as_str(),
                    self.client_config.namespace.clone().unwrap_or_default().as_str(),
                )
                .into(),
            )
        }
        Ok(pop_result)
    }

    fn process_pop_lite_response(
        &self,
        broker_name: &CheetahString,
        mut response: RemotingCommand,
        topic: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<PopResult> {
        let response_code = ResponseCode::from(response.code());
        let (pop_status, msg_found_list) = match response_code {
            ResponseCode::Success => {
                let raw_response_code = response.code();
                let body = response
                    .get_body_mut()
                    .ok_or_else(|| client_broker_err!(raw_response_code, "PopLiteMessage response body is empty"))?;
                let messages = MessageDecoder::decodes_batch(
                    body,
                    self.client_config.decode_read_body,
                    self.client_config.decode_decompress_body,
                );
                (PopStatus::Found, messages)
            }
            ResponseCode::PollingFull => (PopStatus::PollingFull, vec![]),
            ResponseCode::PollingTimeout | ResponseCode::PullNotFound => (PopStatus::PollingNotFound, vec![]),
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().cloned().unwrap_or_default()
                ))
            }
        };
        let mut pop_result = PopResult {
            pop_status,
            msg_found_list: Some(msg_found_list),
            ..Default::default()
        };
        let response_header = response.decode_command_custom_header::<PopLiteMessageResponseHeader>()?;
        if pop_result.pop_status != PopStatus::Found {
            return Ok(pop_result);
        }

        let Some(messages) = pop_result.msg_found_list.as_mut() else {
            return Ok(pop_result);
        };
        let order_count_list =
            parse_lite_order_count_info_like_java(response_header.order_count_info.as_ref(), messages.len());
        for (index, message) in messages.iter_mut().enumerate() {
            let dispatch = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                ))
                .unwrap_or_default();
            let queue_offsets = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                ))
                .unwrap_or_default();
            let queues = split_lite_dispatch_value(dispatch.as_str());
            let offsets = split_lite_dispatch_value(queue_offsets.as_str());
            if queues.len() != 1 || offsets.len() != 1 {
                continue;
            }
            let Ok(queue_offset) = offsets[0].parse::<i64>() else {
                continue;
            };
            let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
                0,
                response_header.pop_time,
                response_header.invisible_time,
                response_header.revive_qid,
                topic,
                broker_name,
                0,
                queue_offset,
            );
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                CheetahString::from_string(extra_info),
            );
            if message
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME))
                .is_none()
            {
                message.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                    CheetahString::from_string(response_header.pop_time.to_string()),
                );
            }
            message.broker_name = broker_name.clone();
            message.set_reconsume_times(
                order_count_list
                    .as_ref()
                    .and_then(|counts| counts.get(index))
                    .copied()
                    .unwrap_or_default(),
            );
            message.set_queue_offset(queue_offset);
        }
        Ok(pop_result)
    }

    pub async fn ack_message_async(
        &self,
        addr: &CheetahString,
        request_header: AckMessageRequestHeader,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.ack_message_async_inner(addr, Some(request_header), None, timeout_millis, ack_callback)
            .await
    }

    pub async fn ack_lite_message_async(
        &self,
        addr: &CheetahString,
        request_header: AckMessageRequestHeader,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.ack_message_async(addr, request_header, timeout_millis, ack_callback)
            .await
    }

    pub async fn batch_ack_message_async(
        &self,
        addr: &CheetahString,
        request_body: BatchAckMessageRequestBody,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.ack_message_async_inner(addr, None, Some(request_body), timeout_millis, ack_callback)
            .await
    }

    pub(self) async fn ack_message_async_inner(
        &self,
        addr: &CheetahString,
        request_header: Option<AckMessageRequestHeader>,
        request_body: Option<BatchAckMessageRequestBody>,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = if let Some(header) = request_header {
            RemotingCommand::create_request_command(RequestCode::AckMessage, header)
        } else {
            let body =
                request_body.ok_or_else(|| mq_client_err!("BatchAckMessage request body is required".to_string()))?;
            RemotingCommand::new_request(RequestCode::BatchAckMessage, body.encode()?)
        };
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let response_code = ResponseCode::from(response.code());
                let ack_result = if response_code == ResponseCode::Success {
                    AckResult {
                        status: AckStatus::Ok,
                        ..Default::default()
                    }
                } else {
                    AckResult {
                        status: AckStatus::NotExist,
                        ..Default::default()
                    }
                };
                ack_callback.on_success(ack_result);
            }
            Err(e) => {
                ack_callback.on_exception(e);
            }
        }
        Ok(())
    }

    pub(crate) async fn update_broker_config(
        &self,
        addr: &CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let validator_input = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>();
        crate::base::validators::Validators::check_broker_config(&validator_input)?;

        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }

        let request =
            RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig).set_body(body.to_string());
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn add_broker(
        &self,
        addr: &CheetahString,
        broker_config_path: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = AddBrokerRequestHeader {
            config_path: Some(broker_config_path),
        };
        let request = RemotingCommand::create_request_command(RequestCode::AddBroker, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn remove_broker(
        &self,
        addr: &CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = RemoveBrokerRequestHeader {
            broker_name,
            broker_cluster_name: cluster_name,
            broker_id,
        };
        let request = RemotingCommand::create_request_command(RequestCode::RemoveBroker, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn notify_min_broker_id_changed(
        &self,
        broker_addr: &CheetahString,
        request_header: NotifyMinBrokerIdChangeRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::NotifyMinBrokerIdChange, request_header);
        self.remoting_client
            .invoke_request_oneway(broker_addr, request, timeout_millis)
            .await;
        Ok(())
    }

    pub async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut config_type = config_types
            .into_iter()
            .map(|config_type| config_type.as_str().trim().to_string())
            .filter(|config_type| !config_type.is_empty())
            .collect::<Vec<_>>()
            .join(";");
        if !config_type.is_empty() {
            config_type.push(';');
        }

        let request_header = ExportRocksdbConfigToJsonRequestHeader {
            config_type: CheetahString::from(config_type),
        };
        let request = RemotingCommand::create_request_command(RequestCode::ExportRocksdbConfigToJson, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn export_rocks_db_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        self.export_rocksdb_config_to_json(broker_addr, config_types, timeout_millis)
            .await
    }

    pub(crate) async fn get_all_consumer_offset_json(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<CheetahString> {
        let request = get_all_consumer_offset_request();
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        consumer_offset_json_from_response(&response)
    }

    pub(crate) async fn get_broker_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response
                    .get_body()
                    .ok_or_else(|| mq_client_err!("Broker config response body is empty".to_string()))?;
                let body_str = String::from_utf8_lossy(body.as_ref());
                mix_all::string_to_properties(body_str.as_ref())
                    .ok_or_else(|| mq_client_err!("Failed to parse broker config response body".to_string()))
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn get_name_server_config(
        &self,
        name_servers: Option<Vec<CheetahString>>,
        timeout_millis: Duration,
    ) -> RocketMQResult<Option<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>> {
        // Determine which name servers to invoke
        let invoke_name_servers = match name_servers {
            Some(servers) if !servers.is_empty() => servers,
            _ => self.remoting_client.get_name_server_address_list().to_vec(),
        };

        if invoke_name_servers.is_empty() {
            return Ok(None);
        }

        // Create request command
        let request = RemotingCommand::create_remoting_command(RequestCode::GetNamesrvConfig);
        let timeout_millis = duration_millis_to_u64("getNameServerConfig", timeout_millis)?;
        let mut config_map = HashMap::with_capacity(4);
        // Iterate through each name server
        for name_server in invoke_name_servers {
            // Make synchronous call with timeout
            let response = self
                .remoting_client
                .invoke_request(Some(&name_server), request.clone(), timeout_millis)
                .await?;
            // Check response code
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    // Parse response body as properties
                    match response.get_body() {
                        Some(body) => {
                            let body_str = String::from_utf8_lossy(body.as_ref()).to_string();
                            // if body_str contains =, return from Java version
                            let properties = if body_str.contains('=') {
                                mix_all::string_to_properties(&body_str).unwrap_or_default()
                            } else {
                                SerdeJsonUtils::from_json_str::<HashMap<CheetahString, CheetahString>>(&body_str)
                                    .map_err(|e| mq_client_err!(format!("Failed to parse namesrv config JSON: {e}")))?
                            };

                            config_map.insert(name_server.clone(), properties);
                        }
                        None => return Err(mq_client_err!("Body is empty".to_string())),
                    }
                }
                code => {
                    return Err(mq_client_err!(
                        response.code(),
                        response.remark().map_or("".to_string(), |s| s.to_string())
                    ));
                }
            }
        }
        Ok(Some(config_map))
    }

    pub async fn probe_name_server(&self, name_server: &CheetahString, timeout_millis: Duration) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(
            RequestCode::GetNamesrvConfig,
            GetNamesrvConfigRequestHeader::for_probe(),
        );
        let timeout_millis = duration_millis_to_u64("probeNameServer", timeout_millis)?;
        let response = self
            .remoting_client
            .invoke_request(Some(name_server), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_controller_metadata(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetMetaDataResponseHeader> {
        let request = RemotingCommand::create_remoting_command(RequestCode::ControllerGetMetadataInfo);
        let response = self
            .remoting_client
            .invoke_request(Some(&controller_address), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => match response.decode_command_custom_header_fast::<GetMetaDataResponseHeader>() {
                Ok(header) => Ok(header),
                Err(_) => Err(mq_client_err!("Could not decode GetMetaDataResponseHeader".to_string())),
            },
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_controller_meta_data(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetMetaDataResponseHeader> {
        self.get_controller_metadata(controller_address, timeout_millis).await
    }

    pub async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<BrokerReplicasInfo> {
        let controller_meta_data = self.get_controller_metadata(controller_address, timeout_millis).await?;
        let leader_address = controller_leader_address(controller_meta_data)?;

        let request = RemotingCommand::create_remoting_command(RequestCode::ControllerGetSyncStateData);
        let body = serde_json::to_vec(&brokers)
            .map_err(|e| mq_client_err!(format!("Failed to serialize broker names: {}", e)))?;
        let request = request.set_body(body);
        let response = self
            .remoting_client
            .invoke_request(Some(&leader_address), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    serde_json::from_slice(body)
                        .map_err(|e| mq_client_err!(format!("Failed to decode BrokerReplicasInfo: {}", e)))
                } else {
                    Err(mq_client_err!(
                        "get_in_sync_state_data response body is empty".to_string()
                    ))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<EpochEntryCache> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerEpochCache);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    match EpochEntryCache::decode(body) {
                        Ok(value) => Ok(value),
                        Err(e) => Err(mq_client_err!(format!("decode EpochEntryCache failed: {}", e))),
                    }
                } else {
                    Err(mq_client_err!(
                        "get_broker_epoch_cache response body is empty".to_string()
                    ))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn get_broker_ha_status(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HARuntimeInfo> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerHaStatus);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    serde_json::from_slice(body)
                        .map_err(|e| mq_client_err!(format!("decode HARuntimeInfo failed: {}", e)))
                } else {
                    Err(mq_client_err!("get_broker_ha_status response body is empty".to_string()))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn update_and_get_group_forbidden(
        &self,
        broker_addr: &CheetahString,
        request_header: UpdateGroupForbiddenRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<GroupForbidden> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndGetGroupForbidden, request_header);
        let broker_addr_vip = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr_vip), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.body() else {
                    return Err(mq_client_err!(
                        "update_and_get_group_forbidden response body is empty".to_string()
                    ));
                };
                SerdeJsonUtils::from_json_slice(body.as_ref())
                    .map_err(|error| mq_client_err!(format!("decode GroupForbidden failed: {error}")))
            }
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn get_controller_config(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetControllerConfig);
        let response = self
            .remoting_client
            .invoke_request(Some(&controller_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response
                    .body()
                    .ok_or_else(|| mq_client_err!("Controller config response body is empty".to_string()))?;
                controller_config_from_response_body(body)
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() || controllers.is_empty() {
            return Ok(());
        }

        let request =
            RemotingCommand::create_remoting_command(RequestCode::UpdateControllerConfig).set_body(body.to_string());
        let mut err_response = None;
        for controller_addr in controllers {
            let response = self
                .remoting_client
                .invoke_request(Some(&controller_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response
                    .remark()
                    .map_or_else(String::new, |remark| remark.to_string())
            ));
        }
        Ok(())
    }

    pub async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
        timeout_millis: u64,
    ) -> RocketMQResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        let controller_meta_data = self.get_controller_metadata(controller_addr, timeout_millis).await?;
        let leader_address = controller_leader_address(controller_meta_data)?;
        let designate_elect = broker_id.is_some();
        let broker_id = match broker_id {
            Some(broker_id) => i64::try_from(broker_id)
                .map_err(|error| mq_client_err!(format!("brokerId is out of range for i64: {error}")))?,
            None => -1,
        };
        let request_header = ElectMasterRequestHeader::new(
            cluster_name,
            broker_name,
            broker_id,
            designate_elect,
            rocketmq_common::TimeUtils::current_millis(),
        );
        let request = RemotingCommand::create_request_command(RequestCode::ControllerElectMaster, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(&leader_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response
                    .decode_command_custom_header_fast::<ElectMasterResponseHeader>()
                    .map_err(|error| mq_client_err!(format!("Could not decode ElectMasterResponseHeader: {error}")))?;
                let broker_member_group = response
                    .body()
                    .ok_or_else(|| mq_client_err!("elect_master response body is empty".to_string()))
                    .and_then(|body| {
                        serde_json::from_slice::<BrokerMemberGroup>(body)
                            .map_err(|error| mq_client_err!(format!("decode BrokerMemberGroup failed: {error}")))
                    })?;
                Ok((response_header, broker_member_group))
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        clean_living_broker: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let controller_meta_data = self.get_controller_metadata(controller_addr, timeout_millis).await?;
        let leader_address = controller_leader_address(controller_meta_data)?;
        let request_header = CleanBrokerDataRequestHeader {
            cluster_name: if cluster_name.is_empty() {
                None
            } else {
                Some(cluster_name)
            },
            broker_name,
            broker_controller_ids_to_clean,
            clean_living_broker,
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::CleanBrokerData, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(&leader_address), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }

        let request = RemotingCommand::create_remoting_command(RequestCode::UpdateColdDataFlowCtrConfig);
        let request = request.set_body(body.to_string());
        let broker_addr =
            mix_all::broker_vip_channel(self.client_config.is_vip_channel_enabled(), broker_addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr).as_ref(), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map(|s| s.to_string()).unwrap_or_default()
            )),
        }
    }

    pub async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        if consumer_group.is_empty() {
            return Ok(());
        }

        let request = RemotingCommand::create_request_command(RequestCode::RemoveColdDataFlowCtrConfig, EmptyHeader {})
            .set_body(consumer_group.to_string());
        let broker_addr_vip =
            mix_all::broker_vip_channel(self.client_config.is_vip_channel_enabled(), broker_addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr_vip), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<CheetahString> {
        let request = RemotingCommand::create_request_command(RequestCode::GetColdDataFlowCtrInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(response
                .body()
                .filter(|body| !body.is_empty())
                .map(|body| CheetahString::from_string(String::from_utf8_lossy(body.as_ref()).into_owned()))
                .unwrap_or_default()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<CheetahString> {
        let mut request = RemotingCommand::create_request_command(RequestCode::SetCommitlogReadMode, EmptyHeader {});
        request.ensure_ext_fields_initialized();
        request.add_ext_field(file_readahead_mode::READ_AHEAD_MODE, mode);
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(response.remark().cloned().unwrap_or_default()),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn export_pop_record(&self, broker_addr: CheetahString, timeout_millis: u64) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::PopRollback, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string()),
            broker_addr.to_string()
        ))
    }

    pub fn init_remoting_version() {
        INIT_REMOTING_VERSION.get_or_init(|| {
            EnvUtils::put_property(
                remoting_command::REMOTING_VERSION_KEY,
                (CURRENT_VERSION as u32).to_string(),
            );
        });
    }

    pub(crate) async fn get_all_topic_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper> {
        let request = RemotingCommand::create_request_command(RequestCode::GetAllTopicConfig, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_all_subscription_group_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper> {
        let request =
            RemotingCommand::create_request_command(RequestCode::GetAllSubscriptionGroupConfig, EmptyHeader {});
        let mut response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_all_subscription_group(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper> {
        self.get_all_subscription_group_config(addr, timeout_millis).await
    }

    pub(crate) async fn get_subscription_group_config(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig>
    {
        let request = RemotingCommand::create_request_command(
            RequestCode::GetSubscriptionGroupConfig,
            rocketmq_remoting::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader {
                group,
                rpc_request_header: None,
            },
        );
        let mut response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::decode(
                    body.as_ref(),
                );
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn create_subscription_group(
        &self,
        addr: &CheetahString,
        config: &rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroup, EmptyHeader {})
                .set_body(config.encode()?);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn get_consumer_running_info(
        &self,
        addr: &CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo> {
        let request_header = GetConsumerRunningInfoRequestHeader {
            consumer_group,
            client_id,
            jstack_enable: jstack,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerRunningInfo, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let mut response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!(
                        "get_consumer_running_info response body is empty".to_string()
                    ));
                };
                rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::decode(&body).map_err(
                    |error| mq_client_err!(format!("decode get_consumer_running_info response failed: {error}")),
                )
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub(crate) async fn consume_message_directly(
        &self,
        client_addr: &CheetahString,
        request_header: ConsumeMessageDirectlyResultRequestHeader,
        message: &MessageExt,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult>
    {
        let body = message_decoder::encode(message, false)?;
        let request = RemotingCommand::create_request_command(RequestCode::ConsumeMessageDirectly, request_header)
            .set_body(body.to_vec());
        let mut response = self
            .remoting_client
            .invoke_request(Some(client_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.take_body() {
                    rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult::decode(
                        body.as_ref(),
                    )
                } else {
                    Err(mq_client_err!(
                        "consume_message_directly response body is empty".to_string()
                    ))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    pub async fn view_message(
        &mut self,
        addr: &CheetahString,
        request_header: ViewMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<MessageExt> {
        let request = RemotingCommand::create_request_command(RequestCode::ViewMessageById, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    let mut bytes = body.clone();
                    let body_len = bytes.len();
                    MessageDecoder::decode(&mut bytes, true, true, false, false, false).ok_or_else(|| {
                        mq_client_err!(format!(
                            "Failed to decode message from view_message response body: body_len={}, possible causes: \
                             CRC check failed or malformed message data",
                            body_len
                        ))
                    })
                } else {
                    Err(mq_client_err!("view_message response body is empty".to_string()))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }

    pub(crate) async fn resume_check_half_message(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<bool> {
        let request_header = ResumeCheckHalfMessageRequestHeader {
            topic,
            msg_id: Some(msg_id),
        };
        let request = RemotingCommand::create_request_command(RequestCode::ResumeCheckHalfMessage, request_header);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(true),
            _ => {
                error!(
                    "Failed to resume half message check logic. Remark={}",
                    response.remark().map_or("", |remark| remark)
                );
                Ok(false)
            }
        }
    }

    pub(crate) async fn switch_timer_engine(
        &self,
        broker_addr: &CheetahString,
        engine_type: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request = RemotingCommand::create_request_command(RequestCode::SwitchTimerEngine, EmptyHeader {});
        request.ensure_ext_fields_initialized();
        request.add_ext_field(MessageConst::TIMER_ENGINE_TYPE, engine_type);
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            broker_addr.to_string()
        ))
    }

    fn build_queue_offset_sorted_map(
        topic: &str,
        msg_found_list: &[MessageExt],
    ) -> RocketMQResult<HashMap<String, Vec<i64>>> {
        let mut sort_map: HashMap<String, Vec<i64>> = HashMap::with_capacity(16);
        for message_ext in msg_found_list {
            let key: String;
            let dispatch = message_ext
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                ))
                .unwrap_or_default();
            if mix_all::is_lmq(Some(topic)) && message_ext.reconsume_times() == 0 && !dispatch.is_empty() {
                // process LMQ
                let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                let data = message_ext
                    .property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                    ))
                    .unwrap_or_default();
                let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                // LMQ topic has only 1 queue, which queue id is 0
                key = ExtraInfoUtil::get_start_offset_info_map_key(topic, mix_all::LMQ_QUEUE_ID as i64);
                let Some(position) = queues.iter().position(|&q| q == topic) else {
                    warn!(
                        "LMQ dispatch queue does not contain topic={}, dispatch={}",
                        topic, dispatch
                    );
                    continue;
                };
                let Some(offset_value) = queue_offsets.get(position) else {
                    warn!(
                        "LMQ queue offset is missing for topic={}, dispatch={}, offsets={}",
                        topic, dispatch, data
                    );
                    continue;
                };
                let Ok(offset) = offset_value.parse::<i64>() else {
                    warn!(
                        "LMQ queue offset is invalid for topic={}, offset={}",
                        topic, offset_value
                    );
                    continue;
                };
                sort_map
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push(offset);
                continue;
            }
            // Value of POP_CK is used to determine whether it is a pop retry,
            // cause topic could be rewritten by broker.
            key = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck(
                message_ext.topic(),
                message_ext
                    .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
                    .clone()
                    .as_ref()
                    .map(|item| item.as_str()),
                message_ext.queue_id() as i64,
            )?;
            sort_map
                .entry(key)
                .or_insert_with(|| Vec::with_capacity(4))
                .push(message_ext.queue_offset());
        }
        Ok(sort_map)
    }
}

fn pop_msg_queue_offset_for_index(
    queue_id_key: &str,
    queue_offset: i64,
    sort_map: &HashMap<String, Vec<i64>>,
    msg_offset_info: &HashMap<String, Vec<i64>>,
) -> Option<i64> {
    let index = sort_map
        .get(queue_id_key)?
        .iter()
        .position(|&offset| offset == queue_offset)?;
    msg_offset_info
        .get(queue_id_key)
        .and_then(|offsets| offsets.get(index))
        .copied()
}

fn empty_broker_member_group(cluster_name: &CheetahString, broker_name: &CheetahString) -> BrokerMemberGroup {
    BrokerMemberGroup::new(cluster_name.clone(), broker_name.clone())
}

fn broker_member_group_from_route_data(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    topic_route_data: &TopicRouteData,
) -> BrokerMemberGroup {
    let mut broker_member_group = empty_broker_member_group(cluster_name, broker_name);
    if let Some(broker_data) = topic_route_data
        .broker_datas
        .iter()
        .find(|broker_data| broker_data.cluster() == cluster_name.as_str() && broker_data.broker_name() == broker_name)
    {
        broker_member_group
            .broker_addrs
            .extend(broker_data.broker_addrs().clone());
    }
    broker_member_group
}

fn cluster_names_for_topic_route(
    cluster_info: &ClusterInfo,
    topic_route_data: &TopicRouteData,
) -> HashSet<CheetahString> {
    let mut cluster_names = HashSet::new();
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return cluster_names;
    };

    for broker_data in &topic_route_data.broker_datas {
        cluster_names.extend(
            cluster_addr_table
                .iter()
                .filter(|(_, broker_names)| broker_names.contains(broker_data.broker_name()))
                .map(|(cluster_name, _)| cluster_name.clone()),
        );
    }

    cluster_names
}

fn lite_subscription_ctl_request(lite_subscription_dto: LiteSubscriptionDTO) -> RocketMQResult<RemotingCommand> {
    let mut request_body = LiteSubscriptionCtlRequestBody::new();
    request_body.set_subscription_set(vec![lite_subscription_dto]);
    let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {});
    request.set_body_mut_ref(request_body.encode()?);
    Ok(request)
}

fn notification_request(request_header: NotificationRequestHeader) -> RemotingCommand {
    RemotingCommand::create_request_command(RequestCode::Notification, request_header)
}

fn create_and_update_plain_access_config_request(
    plain_access_config: &PlainAccessConfig,
) -> RocketMQResult<RemotingCommand> {
    let mut request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateAclConfig, EmptyHeader {});
    request.set_body_mut_ref(plain_access_config.encode()?);
    Ok(request)
}

fn get_acl_request(subject: CheetahString) -> RemotingCommand {
    RemotingCommand::create_request_command(RequestCode::AuthGetAcl, GetAclRequestHeader { subject })
}

fn delete_plain_access_config_request(access_key: &CheetahString) -> RemotingCommand {
    RemotingCommand::create_request_command(RequestCode::DeleteAclConfig, EmptyHeader {})
        .set_body(access_key.as_str().as_bytes().to_vec())
}

fn heartbeat_request(heartbeat_data: &HeartbeatData, language: LanguageCode) -> RocketMQResult<RemotingCommand> {
    Ok(
        RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
            .set_language(language)
            .set_body(heartbeat_data.encode()?),
    )
}

fn get_all_consumer_offset_request() -> RemotingCommand {
    RemotingCommand::create_remoting_command(RequestCode::GetAllConsumerOffset)
}

fn consumer_offset_json_from_response(response: &RemotingCommand) -> RocketMQResult<CheetahString> {
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, ToString::to_string)
        ));
    }

    let body = response
        .body()
        .ok_or_else(|| mq_client_err!("get_all_consumer_offset response body is empty"))?;
    let json = std::str::from_utf8(body.as_ref())
        .map_err(|error| mq_client_err!(format!("decode get_all_consumer_offset response body failed: {error}")))?;
    Ok(CheetahString::from_string(json.to_owned()))
}

fn encode_topic_attributes_like_java(attributes: &HashMap<CheetahString, CheetahString>) -> Option<CheetahString> {
    if attributes.is_empty() {
        return None;
    }

    let encoded = AttributeParser::parse_to_string(
        &attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>(),
    );
    if encoded.is_empty() {
        None
    } else {
        Some(CheetahString::from_string(encoded))
    }
}

fn topic_config_u32_to_java_i32(field_name: &'static str, value: u32) -> RocketMQResult<i32> {
    i32::try_from(value).map_err(|_| mq_client_err!(format!("{field_name} value {value} exceeds Java int range")))
}

fn create_topic_request_header_like_java(
    default_topic: CheetahString,
    topic_config: &TopicConfig,
) -> RocketMQResult<CreateTopicRequestHeader> {
    Validators::check_topic_config(topic_config)?;
    let topic = topic_config
        .topic_name
        .clone()
        .ok_or_else(|| mq_client_err!("topicConfig.topicName is required".to_string()))?;

    Ok(CreateTopicRequestHeader {
        topic,
        default_topic,
        read_queue_nums: topic_config_u32_to_java_i32("readQueueNums", topic_config.read_queue_nums)?,
        write_queue_nums: topic_config_u32_to_java_i32("writeQueueNums", topic_config.write_queue_nums)?,
        perm: topic_config_u32_to_java_i32("perm", topic_config.perm)?,
        topic_filter_type: CheetahString::from_static_str(topic_config.topic_filter_type.as_str()),
        topic_sys_flag: Some(topic_config_u32_to_java_i32(
            "topicSysFlag",
            topic_config.topic_sys_flag,
        )?),
        order: topic_config.order,
        attributes: encode_topic_attributes_like_java(&topic_config.attributes),
        force: None,
        topic_request_header: None,
    })
}

fn create_topic_list_request(topic_config_list: Vec<TopicConfig>) -> RocketMQResult<RemotingCommand> {
    let body = CreateTopicListRequestBody { topic_config_list };
    Ok(RemotingCommand::create_request_command(
        RequestCode::UpdateAndCreateTopicList,
        CreateTopicListRequestHeader::default(),
    )
    .set_body(body.encode()?))
}

fn create_subscription_group_list_request(configs: Vec<SubscriptionGroupConfig>) -> RocketMQResult<RemotingCommand> {
    let body = SubscriptionGroupList {
        group_config_list: configs,
    };
    Ok(
        RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroupList, EmptyHeader {})
            .set_body(body.encode()?),
    )
}

fn query_correction_offset_request(
    topic: CheetahString,
    group: CheetahString,
    filter_groups: Option<Vec<CheetahString>>,
) -> RemotingCommand {
    let filter_groups = filter_groups.map(|groups| {
        CheetahString::from_string(
            groups
                .into_iter()
                .map(|group| group.to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
    });
    let request_header = QueryCorrectionOffsetHeader {
        filter_groups,
        compare_group: group,
        topic,
        topic_request_header: None,
    };
    RemotingCommand::create_request_command(RequestCode::QueryCorrectionOffset, request_header)
}

fn split_lite_dispatch_value(value: &str) -> Vec<&str> {
    value
        .split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER)
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn parse_lite_order_count_info_like_java(
    order_count_info: Option<&CheetahString>,
    msg_count: usize,
) -> Option<Vec<i32>> {
    let order_count_info = order_count_info?.as_str();
    if order_count_info.trim().is_empty() {
        return None;
    }
    let infos = order_count_info.split(';').collect::<Vec<_>>();
    if infos.len() != msg_count {
        return None;
    }
    Some(infos.into_iter().map(parse_lite_order_count_like_java).collect())
}

fn parse_lite_order_count_like_java(info: &str) -> i32 {
    if info.trim().is_empty() {
        return 0;
    }
    if !info.contains(MessageConst::KEY_SEPARATOR) {
        return info.parse::<i32>().unwrap_or_default();
    }
    let split = info.split(MessageConst::KEY_SEPARATOR).collect::<Vec<_>>();
    if split.len() != 3 {
        return 0;
    }
    split[2].parse::<i32>().unwrap_or_default()
}

fn notify_result_from_response(response: &RemotingCommand) -> RocketMQResult<NotifyResult> {
    let response_header = response.decode_command_custom_header::<NotificationResponseHeader>()?;
    Ok(NotifyResult::new(response_header.has_msg, response_header.polling_full))
}

fn filter_retry_topics_like_java(topic_list: &mut TopicList, contain_retry: bool) {
    if contain_retry {
        return;
    }
    topic_list
        .topic_list
        .retain(|topic| !topic.as_str().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX));
}

fn controller_leader_address(header: GetMetaDataResponseHeader) -> RocketMQResult<CheetahString> {
    header
        .controller_leader_address
        .ok_or_else(|| mq_client_err!("Controller leader address is not available".to_string()))
}

fn controller_config_from_response_body(body: &[u8]) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
    let body_str = String::from_utf8_lossy(body);
    mix_all::string_to_properties(body_str.as_ref())
        .ok_or_else(|| mq_client_err!("Failed to parse controller config response body".to_string()))
}

fn should_fetch_system_topic_list_from_broker(topic_list: &TopicList) -> bool {
    !topic_list.topic_list.is_empty()
        && topic_list
            .broker_addr
            .as_ref()
            .is_some_and(|broker_addr| !broker_addr.as_str().trim().is_empty())
}

fn append_system_topic_list_from_broker_like_java(topic_list: &mut TopicList, broker_topic_list: TopicList) {
    if !broker_topic_list.topic_list.is_empty() {
        topic_list.topic_list.extend(broker_topic_list.topic_list);
    }
}

async fn merge_system_topic_list_from_broker(
    api: &MQClientAPIImpl,
    topic_list: &mut TopicList,
    timeout_millis: u64,
) -> RocketMQResult<()> {
    if !should_fetch_system_topic_list_from_broker(topic_list) {
        return Ok(());
    }
    let Some(broker_addr) = topic_list.broker_addr.clone() else {
        return Ok(());
    };
    let broker_topic_list = api
        .get_system_topic_list_from_broker(&broker_addr, timeout_millis)
        .await?;
    append_system_topic_list_from_broker_like_java(topic_list, broker_topic_list);
    Ok(())
}

fn decode_cluster_acl_version_info_response_body(body: Option<&bytes::Bytes>) -> RocketMQResult<ClusterAclVersionInfo> {
    let body = body.ok_or_else(|| mq_client_err!("get_broker_cluster_acl_version_info response body is empty"))?;
    SerdeJsonUtils::from_json_slice(body.as_ref())
        .map_err(|error| mq_client_err!(format!("decode ClusterAclVersionInfo failed: {error}")))
}

fn reset_offset_table_from_response(response: &RemotingCommand) -> RocketMQResult<HashMap<MessageQueue, i64>> {
    if ResponseCode::from(response.code()) == ResponseCode::Success {
        let Some(body) = response.get_body() else {
            return Err(mq_client_err!(
                response.code(),
                response
                    .remark()
                    .map_or_else(|| "reset offset response body is empty".to_string(), |s| s.to_string())
            ));
        };
        let Some(reset_body) = ResetOffsetBody::decode(body.as_ref()) else {
            return Err(mq_client_err!(
                response.code(),
                response
                    .remark()
                    .map_or_else(|| "decode ResetOffsetBody failed".to_string(), |s| s.to_string())
            ));
        };
        return Ok(reset_body.offset_table);
    }

    Err(mq_client_err!(
        response.code(),
        response.remark().map_or("".to_string(), |s| s.to_string())
    ))
}

fn admin_message_matches_query(
    topic: &CheetahString,
    key: &CheetahString,
    msg: &MessageExt,
    unique_key_flag: bool,
) -> bool {
    if msg.topic().as_str() != topic.as_str() {
        return false;
    }

    if unique_key_flag {
        if let Some(uniq_id) = MessageClientIDSetter::get_uniq_id(msg) {
            if uniq_id.as_str() == key.as_str() {
                return true;
            }
        }
        return msg.msg_id.as_str() == key.as_str();
    }

    let Some(keys) = MessageTrait::get_keys(msg) else {
        return false;
    };

    keys.split(MessageConst::KEY_SEPARATOR)
        .any(|candidate| candidate == key.as_str())
}

impl MQClientAPIImpl {
    async fn invoke_admin_request(
        &self,
        address: &str,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<RemotingCommand> {
        let address = CheetahString::from_slice(address);
        self.remoting_client
            .invoke_request(Some(&address), request, timeout_millis)
            .await
    }
}

impl MqClientAdminInner for MQClientAPIImpl {
    async fn query_message(
        &self,
        address: &str,
        unique_key_flag: bool,
        decompress_body: bool,
        request_header: QueryMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<MessageExt>> {
        let topic = request_header.topic.clone();
        let key = request_header.key.clone();
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, request_header);
        request.ensure_ext_fields_initialized();
        request.add_ext_field(
            mix_all::UNIQUE_MSG_QUERY_FLAG,
            CheetahString::from_static_str(if unique_key_flag { "true" } else { "false" }),
        );

        let mut response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(mut body) = response.take_body() else {
                    return Err(mq_client_err!("query_message response body is empty"));
                };
                Ok(message_decoder::decodes_batch(&mut body, true, decompress_body)
                    .into_iter()
                    .filter(|msg| admin_message_matches_query(&topic, &key, msg, unique_key_flag))
                    .collect())
            }
            ResponseCode::QueryNotFound => Ok(Vec::new()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn get_topic_stats_info(
        &self,
        address: &str,
        request_header: GetTopicStatsInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicStatsTable> {
        let request = RemotingCommand::create_request_command(RequestCode::GetTopicStatsInfo, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicStatsTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_consume_time_span(
        &self,
        address: &str,
        request_header: QueryConsumeTimeSpanRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<QueueTimeSpan>> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumeTimeSpan, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let body: QueryConsumeTimeSpanBody = serde_json::from_slice(body.as_ref())?;
                return Ok(body.consume_time_span_set);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn update_or_create_topic(
        &self,
        address: &str,
        request_header: CreateTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateAndCreateTopic, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn update_or_create_subscription_group(
        &self,
        address: &str,
        config: SubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::UpdateAndCreateSubscriptionGroup, EmptyHeader {})
                .set_body(config.encode()?);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn delete_topic_in_broker(
        &self,
        address: &str,
        request_header: DeleteTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInBroker, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn delete_topic_in_nameserver(
        &self,
        address: &str,
        request_header: DeleteTopicFromNamesrvRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteTopicInNamesrv, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn delete_kv_config(
        &self,
        address: &str,
        request_header: DeleteKVConfigRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteKvConfig, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn delete_subscription_group(
        &self,
        address: &str,
        request_header: DeleteSubscriptionGroupRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::DeleteSubscriptionGroup, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn invoke_broker_to_reset_offset(
        &self,
        address: &str,
        request_header: ResetOffsetRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<MessageQueue, i64>> {
        let request = RemotingCommand::create_request_command(RequestCode::InvokeBrokerToResetOffset, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        reset_offset_table_from_response(&response)
    }

    async fn view_message(
        &self,
        address: &str,
        request_header: ViewMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<MessageExt> {
        let request = RemotingCommand::create_request_command(RequestCode::ViewMessageById, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    let mut bytes = body.clone();
                    MessageDecoder::decode(&mut bytes, true, true, false, false, false)
                        .ok_or_else(|| mq_client_err!("view_message response body decode failed"))
                } else {
                    Err(mq_client_err!("view_message response body is empty"))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn get_broker_cluster_info(&self, address: &str, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerClusterInfo, EmptyHeader {});
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ClusterInfo::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consumer_connection_list(
        &self,
        address: &str,
        request_header: GetConsumerConnectionListRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumerConnection> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerConnectionList, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ConsumerConnection::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_topics_by_consumer(
        &self,
        address: &str,
        request_header: QueryTopicsByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicsByConsumer, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_subscription_by_consumer(
        &self,
        address: &str,
        request_header: QuerySubscriptionByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionData> {
        let request = RemotingCommand::create_request_command(RequestCode::QuerySubscriptionByConsumer, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response
                .get_body()
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response body is empty"))?;
            let response_body: QuerySubscriptionResponseBody = serde_json::from_slice(body.as_ref())?;
            return response_body
                .subscription_data
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response subscriptionData is empty"));
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consume_stats(
        &self,
        address: &str,
        request_header: GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumeStats> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumeStats, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ConsumeStats::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_topic_consume_by_who(
        &self,
        address: &str,
        request_header: QueryTopicConsumeByWhoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<GroupList> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryTopicConsumeByWho, request_header);
        let response = self.invoke_admin_request(address, request, timeout_millis).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GroupList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consumer_running_info(
        &self,
        address: &str,
        request_header: GetConsumerRunningInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumerRunningInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerRunningInfo, request_header);
        let mut response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!("get_consumer_running_info response body is empty"));
                };
                ConsumerRunningInfo::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn consume_message_directly(
        &self,
        address: &str,
        request_header: ConsumeMessageDirectlyResultRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumeMessageDirectlyResult> {
        let request = RemotingCommand::create_request_command(RequestCode::ConsumeMessageDirectly, request_header);
        let mut response = self.invoke_admin_request(address, request, timeout_millis).await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!("consume_message_directly response body is empty"));
                };
                ConsumeMessageDirectlyResult::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering as AtomicOrdering;

    use rocketmq_common::common::lite::LiteSubscriptionAction;
    use rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader;
    use rocketmq_remoting::protocol::command_custom_header::FromMap;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    use super::*;

    fn retry_strategy() -> MQFaultStrategy {
        MQFaultStrategy::new(&ClientConfig::default())
    }

    fn topic_publish_info() -> TopicPublishInfo {
        let mut info = TopicPublishInfo::new();
        info.message_queue_list = vec![
            MessageQueue::from_parts("topicA", "broker-a", 0),
            MessageQueue::from_parts("topicA", "broker-b", 1),
        ];
        info
    }

    #[derive(Debug)]
    struct AsyncRetryTestHeader {
        retry_marker: CheetahString,
    }

    impl CommandCustomHeader for AsyncRetryTestHeader {
        fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
            Some(HashMap::from([(
                CheetahString::from_static_str("retryMarker"),
                self.retry_marker.clone(),
            )]))
        }
    }

    impl FromMap for AsyncRetryTestHeader {
        type Error = rocketmq_error::RocketMQError;
        type Target = Self;

        fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self, Self::Error> {
            let retry_marker = map
                .get(&CheetahString::from_static_str("retryMarker"))
                .cloned()
                .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("missing retryMarker test header"))?;
            Ok(Self { retry_marker })
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, AtomicOrdering::Release);
        }
    }

    #[test]
    fn java_long_to_u64_field_rejects_negative_protocol_values() {
        assert_eq!(
            java_long_to_u64_field("sendMessage", "queueOffset", 123).expect("positive Java long should convert"),
            123
        );

        let error = java_long_to_u64_field("pullMessage", "nextBeginOffset", -1)
            .expect_err("negative broker offset must not wrap");

        assert!(error
            .to_string()
            .contains("pullMessage nextBeginOffset is negative and cannot be represented as Rust u64"));
    }

    #[test]
    fn trace_on_from_ext_fields_matches_java_missing_default() {
        assert!(trace_on_from_ext_fields(None));
        assert!(trace_on_from_ext_fields(Some(&HashMap::new())));
    }

    #[test]
    fn trace_on_from_ext_fields_only_false_disables_trace_like_java() {
        let trace_switch = CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH);

        let mut fields = HashMap::new();
        fields.insert(trace_switch.clone(), CheetahString::from_static_str("false"));
        assert!(!trace_on_from_ext_fields(Some(&fields)));

        fields.insert(trace_switch.clone(), CheetahString::from_static_str("true"));
        assert!(trace_on_from_ext_fields(Some(&fields)));

        fields.insert(trace_switch.clone(), CheetahString::from_static_str("False"));
        assert!(trace_on_from_ext_fields(Some(&fields)));

        fields.insert(trace_switch, CheetahString::from_static_str("invalid"));
        assert!(trace_on_from_ext_fields(Some(&fields)));
    }

    #[test]
    fn duration_millis_to_u64_rejects_values_outside_rust_range() {
        assert_eq!(
            duration_millis_to_u64("probeNameServer", Duration::from_millis(u64::MAX))
                .expect("max u64 millis should convert"),
            u64::MAX
        );

        let error = duration_millis_to_u64("probeNameServer", Duration::from_secs(u64::MAX))
            .expect_err("duration larger than u64 millis should fail");

        assert!(error
            .to_string()
            .contains("probeNameServer timeout exceeds Rust u64 millisecond range"));
    }

    #[test]
    fn controller_leader_address_requires_controller_metadata_leader_like_java() {
        let leader_address = controller_leader_address(GetMetaDataResponseHeader {
            controller_leader_address: Some(CheetahString::from_static_str("127.0.0.1:9878")),
            ..Default::default()
        })
        .expect("leader address should be returned");

        assert_eq!(leader_address, CheetahString::from_static_str("127.0.0.1:9878"));

        let error = controller_leader_address(GetMetaDataResponseHeader::default())
            .expect_err("controller metadata without leader should be rejected");

        assert!(error.to_string().contains("Controller leader address"));
    }

    #[test]
    fn controller_config_from_response_body_uses_java_properties_rules() {
        let body = b"
            # comment
            controllerType:Raft
            notifyBrokerRoleChanged = true
            blankValue
        ";

        let config = controller_config_from_response_body(body).expect("controller config body should parse");

        assert_eq!(
            config.get(&CheetahString::from_static_str("controllerType")),
            Some(&CheetahString::from_static_str("Raft"))
        );
        assert_eq!(
            config.get(&CheetahString::from_static_str("notifyBrokerRoleChanged")),
            Some(&CheetahString::from_static_str("true"))
        );
        assert_eq!(
            config.get(&CheetahString::from_static_str("blankValue")),
            Some(&CheetahString::new())
        );
    }

    #[test]
    fn unit_topic_list_filters_retry_topics_like_java() {
        let mut topic_list = TopicList {
            topic_list: vec![
                CheetahString::from_static_str("TopicA"),
                CheetahString::from_static_str("%RETRY%GroupA"),
                CheetahString::from_static_str("TopicB"),
            ],
            broker_addr: None,
        };

        filter_retry_topics_like_java(&mut topic_list, false);

        assert_eq!(
            topic_list.topic_list,
            vec![
                CheetahString::from_static_str("TopicA"),
                CheetahString::from_static_str("TopicB")
            ]
        );
    }

    #[test]
    fn unit_topic_list_keeps_retry_topics_when_requested_like_java() {
        let mut topic_list = TopicList {
            topic_list: vec![
                CheetahString::from_static_str("%RETRY%GroupA"),
                CheetahString::from_static_str("TopicA"),
            ],
            broker_addr: None,
        };

        filter_retry_topics_like_java(&mut topic_list, true);

        assert_eq!(
            topic_list.topic_list,
            vec![
                CheetahString::from_static_str("%RETRY%GroupA"),
                CheetahString::from_static_str("TopicA")
            ]
        );
    }

    #[test]
    fn cluster_names_for_topic_route_maps_brokers_to_clusters() {
        let mut cluster_a_brokers = HashSet::new();
        cluster_a_brokers.insert(CheetahString::from_static_str("broker-a"));
        let mut cluster_b_brokers = HashSet::new();
        cluster_b_brokers.insert(CheetahString::from_static_str("broker-b"));

        let cluster_info = ClusterInfo::new(
            None,
            Some(HashMap::from([
                (CheetahString::from_static_str("cluster-a"), cluster_a_brokers),
                (CheetahString::from_static_str("cluster-b"), cluster_b_brokers),
            ])),
        );
        let topic_route_data = TopicRouteData {
            broker_datas: vec![
                BrokerData::new(
                    CheetahString::from_static_str("cluster-a"),
                    CheetahString::from_static_str("broker-a"),
                    HashMap::new(),
                    None,
                ),
                BrokerData::new(
                    CheetahString::from_static_str("cluster-b"),
                    CheetahString::from_static_str("broker-b"),
                    HashMap::new(),
                    None,
                ),
            ],
            ..Default::default()
        };

        let clusters = cluster_names_for_topic_route(&cluster_info, &topic_route_data);

        assert_eq!(clusters.len(), 2);
        assert!(clusters.contains(&CheetahString::from_static_str("cluster-a")));
        assert!(clusters.contains(&CheetahString::from_static_str("cluster-b")));
    }

    #[test]
    fn system_topic_list_fetch_predicate_matches_java() {
        assert!(!should_fetch_system_topic_list_from_broker(&TopicList {
            topic_list: vec![],
            broker_addr: Some(CheetahString::from_static_str("127.0.0.1:10911")),
        }));
        assert!(!should_fetch_system_topic_list_from_broker(&TopicList {
            topic_list: vec![CheetahString::from_static_str("TopicA")],
            broker_addr: None,
        }));
        assert!(!should_fetch_system_topic_list_from_broker(&TopicList {
            topic_list: vec![CheetahString::from_static_str("TopicA")],
            broker_addr: Some(CheetahString::from_static_str("   ")),
        }));
        assert!(should_fetch_system_topic_list_from_broker(&TopicList {
            topic_list: vec![CheetahString::from_static_str("TopicA")],
            broker_addr: Some(CheetahString::from_static_str("127.0.0.1:10911")),
        }));
    }

    #[test]
    fn system_topic_list_appends_broker_topics_without_dedup_like_java() {
        let mut topic_list = TopicList {
            topic_list: vec![CheetahString::from_static_str("TopicA")],
            broker_addr: Some(CheetahString::from_static_str("127.0.0.1:10911")),
        };
        let broker_topic_list = TopicList {
            topic_list: vec![
                CheetahString::from_static_str("TopicA"),
                CheetahString::from_static_str("BrokerTopic"),
            ],
            broker_addr: None,
        };

        append_system_topic_list_from_broker_like_java(&mut topic_list, broker_topic_list);

        assert_eq!(
            topic_list.topic_list,
            vec![
                CheetahString::from_static_str("TopicA"),
                CheetahString::from_static_str("TopicA"),
                CheetahString::from_static_str("BrokerTopic")
            ]
        );
    }

    #[test]
    fn create_topic_request_header_matches_java_topic_config_mapping() {
        let mut topic_config = TopicConfig::with_sys_flag("TopicA", 2, 4, 6, 8);
        topic_config.order = true;
        topic_config.attributes.insert(
            CheetahString::from_static_str("+cleanup.policy"),
            CheetahString::from_static_str("DELETE"),
        );

        let header = create_topic_request_header_like_java(CheetahString::from_static_str("TBW102"), &topic_config)
            .expect("valid topic config should map to request header");

        assert_eq!(header.topic.as_str(), "TopicA");
        assert_eq!(header.default_topic.as_str(), "TBW102");
        assert_eq!(header.read_queue_nums, 2);
        assert_eq!(header.write_queue_nums, 4);
        assert_eq!(header.perm, 6);
        assert_eq!(header.topic_filter_type.as_str(), "SINGLE_TAG");
        assert_eq!(header.topic_sys_flag, Some(8));
        assert!(header.order);
        assert_eq!(header.attributes.as_deref(), Some("+cleanup.policy=DELETE"));
        assert_eq!(header.force, None);
    }

    #[test]
    fn create_topic_request_header_rejects_values_outside_java_int_range() {
        let mut topic_config = TopicConfig::new("TopicA");
        topic_config.read_queue_nums = u32::MAX;

        let error = create_topic_request_header_like_java(CheetahString::from_static_str("TBW102"), &topic_config)
            .expect_err("Java int overflow should be rejected before encoding");

        assert!(error.to_string().contains("readQueueNums value"));
    }

    #[test]
    fn create_topic_list_request_matches_java_request_code_and_body() {
        let request = create_topic_list_request(vec![TopicConfig::new("TopicA"), TopicConfig::new("TopicB")])
            .expect("topic list request should encode");

        assert_eq!(request.code(), RequestCode::UpdateAndCreateTopicList as i32);
        let body = request.body().expect("topic list request body should be set");
        let decoded: CreateTopicListRequestBody =
            serde_json::from_slice(body.as_ref()).expect("topic list body should decode");
        assert_eq!(decoded.topic_config_list.len(), 2);
        assert_eq!(decoded.topic_config_list[0].topic_name.as_deref(), Some("TopicA"));
        assert_eq!(decoded.topic_config_list[1].topic_name.as_deref(), Some("TopicB"));
    }

    #[test]
    fn create_subscription_group_list_request_matches_java_request_code_and_body() {
        let request = create_subscription_group_list_request(vec![
            SubscriptionGroupConfig::new(CheetahString::from_static_str("GroupA")),
            SubscriptionGroupConfig::new(CheetahString::from_static_str("GroupB")),
        ])
        .expect("subscription group list request should encode");

        assert_eq!(request.code(), RequestCode::UpdateAndCreateSubscriptionGroupList as i32);
        let body = request
            .body()
            .expect("subscription group list request body should be set");
        let decoded: SubscriptionGroupList =
            serde_json::from_slice(body.as_ref()).expect("subscription group list body should decode");
        assert_eq!(decoded.group_config_list.len(), 2);
        assert_eq!(decoded.group_config_list[0].group_name().as_str(), "GroupA");
        assert_eq!(decoded.group_config_list[1].group_name().as_str(), "GroupB");
    }

    #[test]
    fn query_correction_offset_request_joins_filter_groups_like_java() {
        let request = query_correction_offset_request(
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("CompareGroup"),
            Some(vec![
                CheetahString::from_static_str("GroupA"),
                CheetahString::from_static_str("GroupB"),
            ]),
        );

        assert_eq!(request.code(), RequestCode::QueryCorrectionOffset as i32);
        let header = request
            .try_read_custom_header_ref::<QueryCorrectionOffsetHeader>()
            .expect("query correction header should be attached");
        assert_eq!(header.topic.as_str(), "TopicA");
        assert_eq!(header.compare_group.as_str(), "CompareGroup");
        assert_eq!(header.filter_groups.as_deref(), Some("GroupA,GroupB"));
    }

    #[test]
    fn parse_lite_order_count_info_matches_java_rules() {
        assert_eq!(
            parse_lite_order_count_info_like_java(Some(&CheetahString::from_static_str("1;0 7 3;bad")), 3),
            Some(vec![1, 3, 0])
        );
        assert_eq!(
            parse_lite_order_count_info_like_java(Some(&CheetahString::from_static_str("1;2")), 3),
            None
        );
        assert_eq!(
            parse_lite_order_count_info_like_java(Some(&CheetahString::from_static_str("")), 0),
            None
        );
    }

    #[test]
    fn split_lite_dispatch_value_drops_empty_segments_like_java_string_utils_split() {
        assert_eq!(split_lite_dispatch_value("q0,,q1,"), vec!["q0", "q1"]);
        assert!(split_lite_dispatch_value("").is_empty());
    }

    #[test]
    fn build_queue_offset_sorted_map_preserves_java_long_offsets() {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str("TopicA"));
        message.set_queue_id(3);
        message.set_queue_offset(-7);

        let sort_map = MQClientAPIImpl::build_queue_offset_sorted_map("TopicA", &[message])
            .expect("queue offset map should build");
        let key = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck("TopicA", None, 3)
            .expect("normal topic key should build");

        assert_eq!(sort_map.get(&key).map(Vec::as_slice), Some(&[-7][..]));
    }

    #[test]
    fn build_queue_offset_sorted_map_preserves_lmq_java_long_offsets() {
        const TOPIC: &str = "%LMQ%TopicA";
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str(TOPIC));
        message.set_queue_id(0);
        message.set_reconsume_times(0);
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
            CheetahString::from_static_str("Other,%LMQ%TopicA"),
        );
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_static_str("11,-9"),
        );

        let sort_map =
            MQClientAPIImpl::build_queue_offset_sorted_map(TOPIC, &[message]).expect("LMQ offset map should build");
        let key = ExtraInfoUtil::get_start_offset_info_map_key(TOPIC, mix_all::LMQ_QUEUE_ID as i64);

        assert_eq!(sort_map.get(&key).map(Vec::as_slice), Some(&[-9][..]));
    }

    #[test]
    fn pop_msg_queue_offset_lookup_uses_msg_offset_info_like_java() {
        let queue_id_key = ExtraInfoUtil::get_start_offset_info_map_key("TopicA", 1);
        let sort_map = HashMap::from([(queue_id_key.clone(), vec![10, 11])]);
        let msg_offset_info = HashMap::from([(queue_id_key.clone(), vec![100, 101])]);

        assert_eq!(
            pop_msg_queue_offset_for_index(&queue_id_key, 11, &sort_map, &msg_offset_info),
            Some(101)
        );
        assert_eq!(
            pop_msg_queue_offset_for_index(&queue_id_key, 12, &sort_map, &msg_offset_info),
            None
        );
    }

    #[test]
    fn pop_msg_queue_offset_lookup_preserves_negative_java_long_offsets() {
        let queue_id_key = ExtraInfoUtil::get_start_offset_info_map_key("TopicA", 1);
        let sort_map = HashMap::from([(queue_id_key.clone(), vec![-1])]);
        let msg_offset_info = HashMap::from([(queue_id_key.clone(), vec![-2])]);

        assert_eq!(
            pop_msg_queue_offset_for_index(&queue_id_key, -1, &sort_map, &msg_offset_info),
            Some(-2)
        );
    }

    #[test]
    fn admin_query_filter_matches_java_unique_key_rules() {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str("TopicA"));
        message.set_msg_id(CheetahString::from_static_str("OFFSET-MSG-1"));
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("UNIQ-MSG-1"),
        );

        assert!(admin_message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("UNIQ-MSG-1"),
            &message,
            true
        ));
        assert!(!admin_message_matches_query(
            &CheetahString::from_static_str("OtherTopic"),
            &CheetahString::from_static_str("UNIQ-MSG-1"),
            &message,
            true
        ));
        assert!(!admin_message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("MSG-2"),
            &message,
            true
        ));

        let mut fallback_message = MessageExt::default();
        fallback_message.set_topic(CheetahString::from_static_str("TopicA"));
        fallback_message.set_msg_id(CheetahString::from_static_str("OFFSET-MSG-2"));
        assert!(admin_message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("OFFSET-MSG-2"),
            &fallback_message,
            true
        ));
    }

    #[test]
    fn admin_query_filter_matches_java_key_separator_rules() {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str("TopicA"));
        message.set_keys(CheetahString::from_static_str("KeyA KeyB"));

        assert!(admin_message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("KeyB"),
            &message,
            false
        ));
        assert!(!admin_message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("Key"),
            &message,
            false
        ));
    }

    #[test]
    fn async_retry_queue_prefers_broker_different_from_failed_broker() {
        let strategy = retry_strategy();
        let info = topic_publish_info();

        let selected = MQClientAPIImpl::select_async_retry_queue(
            &strategy,
            Some(&info),
            &CheetahString::from_static_str("broker-a"),
        )
        .expect("retry queue should be selected");

        assert_eq!(selected.broker_name(), "broker-b");
    }

    #[test]
    fn async_retry_queue_without_topic_publish_info_returns_none() {
        let strategy = retry_strategy();

        let selected =
            MQClientAPIImpl::select_async_retry_queue(&strategy, None, &CheetahString::from_static_str("broker-a"));

        assert!(selected.is_none());
    }

    #[test]
    fn async_send_callback_success_uses_configured_callback_executor_like_java() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("rocketmq-callback-success")
            .enable_all()
            .build()
            .expect("callback runtime should build");
        let (tx, rx) = std::sync::mpsc::channel();
        let callback: ArcSendCallback = Arc::new(move |result: Option<&SendResult>, error: Option<&RocketMQError>| {
            tx.send((
                std::thread::current().name().unwrap_or_default().to_string(),
                result.is_some(),
                error.is_some(),
            ))
            .expect("test receiver should be alive");
        });

        MQClientAPIImpl::notify_send_callback_success(
            &Some(callback),
            &Some(runtime.handle().clone()),
            &SendResult::default(),
        );

        let (thread_name, has_result, has_error) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("callback should execute on configured runtime");
        assert_eq!(thread_name, "rocketmq-callback-success");
        assert!(has_result);
        assert!(!has_error);
    }

    #[test]
    fn async_send_callback_exception_uses_configured_callback_executor_like_java() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("rocketmq-callback-error")
            .enable_all()
            .build()
            .expect("callback runtime should build");
        let (tx, rx) = std::sync::mpsc::channel();
        let callback: ArcSendCallback = Arc::new(move |result: Option<&SendResult>, error: Option<&RocketMQError>| {
            tx.send((
                std::thread::current().name().unwrap_or_default().to_string(),
                result.is_some(),
                error.map(ToString::to_string),
            ))
            .expect("test receiver should be alive");
        });
        let error = RocketMQError::network_request_failed("broker-a", "callback failure");

        MQClientAPIImpl::notify_send_callback_exception(&Some(callback), &Some(runtime.handle().clone()), &error);

        let (thread_name, has_result, error) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("callback should execute on configured runtime");
        assert_eq!(thread_name, "rocketmq-callback-error");
        assert!(!has_result);
        assert_eq!(
            error.as_deref(),
            Some("Response send_callback failed: Send failed to broker-a: callback failure")
        );
    }

    #[tokio::test]
    async fn api_background_task_tracker_waits_for_completion() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();

        MQClientAPIImpl::spawn_api_background_task(
            "rocketmq-client-api-background-test",
            &tracker,
            &token,
            async move {
                completed_in_task.store(true, AtomicOrdering::Release);
            },
        );
        tracker.close();

        tokio::time::timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("tracked API background task should finish");

        assert!(completed.load(AtomicOrdering::Acquire));
    }

    #[tokio::test]
    async fn api_background_task_shutdown_token_cancels_pending_task() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();

        MQClientAPIImpl::spawn_api_background_task(
            "rocketmq-client-api-background-test",
            &tracker,
            &token,
            async move {
                let _drop_flag = DropFlag(dropped_in_task);
                pending::<()>().await;
            },
        );
        tracker.close();

        assert!(tokio::time::timeout(Duration::from_millis(20), tracker.wait())
            .await
            .is_err());

        token.cancel();

        tokio::time::timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("shutdown token should release pending API background task");

        assert!(dropped.load(AtomicOrdering::Acquire));
    }

    #[test]
    fn lite_subscription_ctl_request_matches_java_single_dto_body() {
        let lite_subscription_dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id(CheetahString::from_static_str("client-a"))
            .with_group(CheetahString::from_static_str("group-a"))
            .with_topic(CheetahString::from_static_str("topic-a"))
            .with_version(42);

        let request = lite_subscription_ctl_request(lite_subscription_dto.clone())
            .expect("lite subscription request should encode");

        assert_eq!(request.code(), RequestCode::LiteSubscriptionCtl as i32);
        let body = request.body().expect("request body should be set");
        let decoded = LiteSubscriptionCtlRequestBody::decode(body.as_ref()).expect("body should decode");
        assert_eq!(decoded.subscription_set(), &[lite_subscription_dto]);
    }

    #[test]
    fn notification_request_matches_java_header_fields() {
        let mut request = notification_request(NotificationRequestHeader {
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("topic-a"),
            queue_id: -1,
            poll_time: 3_000,
            born_time: 10,
            order: true,
            attempt_id: Some(CheetahString::from_static_str("attempt-a")),
            exp_type: Some(CheetahString::from_static_str("TAG")),
            exp: Some(CheetahString::from_static_str("tag-a")),
            topic_request_header: None,
        });

        assert_eq!(request.code(), RequestCode::Notification as i32);
        request.make_custom_header_to_net();
        let ext_fields = request.ext_fields().expect("notification request should encode header");
        assert_eq!(
            ext_fields.get("consumerGroup").map(|value| value.as_str()),
            Some("group-a")
        );
        assert_eq!(ext_fields.get("topic").map(|value| value.as_str()), Some("topic-a"));
        assert_eq!(ext_fields.get("queueId").map(|value| value.as_str()), Some("-1"));
        assert_eq!(ext_fields.get("pollTime").map(|value| value.as_str()), Some("3000"));
        assert_eq!(ext_fields.get("bornTime").map(|value| value.as_str()), Some("10"));
        assert_eq!(ext_fields.get("order").map(|value| value.as_str()), Some("true"));
        assert_eq!(
            ext_fields.get("attemptId").map(|value| value.as_str()),
            Some("attempt-a")
        );
        assert_eq!(ext_fields.get("expType").map(|value| value.as_str()), Some("TAG"));
        assert_eq!(ext_fields.get("exp").map(|value| value.as_str()), Some("tag-a"));
    }

    #[test]
    fn create_and_update_plain_access_config_request_matches_java_legacy_acl_body() {
        let config = PlainAccessConfig {
            access_key: Some(CheetahString::from_static_str("AK")),
            secret_key: Some(CheetahString::from_static_str("SK")),
            white_remote_address: Some(CheetahString::from_static_str("10.0.*.*")),
            admin: true,
            default_topic_perm: Some(CheetahString::from_static_str("DENY")),
            default_group_perm: Some(CheetahString::from_static_str("SUB")),
            topic_perms: vec![CheetahString::from_static_str("TopicA=PUB|SUB")],
            group_perms: vec![CheetahString::from_static_str("GroupA=SUB")],
        };

        let request =
            create_and_update_plain_access_config_request(&config).expect("plain access config request should encode");

        assert_eq!(request.code(), RequestCode::UpdateAndCreateAclConfig as i32);
        let body = std::str::from_utf8(request.body().expect("request body should be set").as_ref())
            .expect("body should be UTF-8 JSON");
        assert_eq!(
            body,
            r#"{"accessKey":"AK","secretKey":"SK","whiteRemoteAddress":"10.0.*.*","admin":true,"defaultTopicPerm":"DENY","defaultGroupPerm":"SUB","topicPerms":["TopicA=PUB|SUB"],"groupPerms":["GroupA=SUB"]}"#
        );
    }

    #[test]
    fn delete_plain_access_config_request_matches_java_legacy_acl_body() {
        let request = delete_plain_access_config_request(&CheetahString::from_static_str("AK"));

        assert_eq!(request.code(), RequestCode::DeleteAclConfig as i32);
        assert_eq!(request.body().expect("request body should be set").as_ref(), b"AK");
    }

    #[test]
    fn get_acl_request_matches_java_auth_get_acl_header() {
        let request = get_acl_request(CheetahString::from_static_str("User:alice"));

        assert_eq!(request.code(), RequestCode::AuthGetAcl as i32);
        let header = request
            .try_read_custom_header_ref::<GetAclRequestHeader>()
            .expect("get ACL header should be attached");
        assert_eq!(header.subject.as_str(), "User:alice");
    }

    #[test]
    fn heartbeat_request_matches_java_register_client_request() {
        let heartbeat_data = HeartbeatData::default();

        let request =
            heartbeat_request(&heartbeat_data, LanguageCode::RUST).expect("heartbeat request should encode body");

        assert_eq!(request.code(), RequestCode::HeartBeat as i32);
        assert!(request.body().is_some());
    }

    #[test]
    fn get_all_consumer_offset_request_uses_java_request_code() {
        let request = get_all_consumer_offset_request();

        assert_eq!(request.code(), RequestCode::GetAllConsumerOffset as i32);
    }

    #[test]
    fn consumer_offset_json_from_response_returns_raw_broker_json() {
        let body = r#"{"dataVersion":{"counter":1},"offsetTable":{"TopicA@GroupA":{"0":42}}}"#;
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_body(body);

        let json = consumer_offset_json_from_response(&response).expect("success response should decode body");

        assert_eq!(json.as_str(), body);
    }

    #[test]
    fn consumer_offset_json_from_response_rejects_success_without_body() {
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);

        let error = consumer_offset_json_from_response(&response)
            .expect_err("success response without body should be rejected");

        assert!(error
            .to_string()
            .contains("get_all_consumer_offset response body is empty"));
    }

    #[test]
    fn consumer_send_message_back_header_omits_broker_name_when_java_passes_null() {
        let mut message = MessageExt::default();
        message.set_commit_log_offset(42);
        message.msg_id = CheetahString::from_static_str("MSG_ID_A");
        message.set_topic(CheetahString::from_static_str("TopicA"));

        let header = MQClientAPIImpl::consumer_send_message_back_request_header(&message, None, "GroupA", 3, 16);
        let map = header.to_map().expect("header should encode");

        assert_eq!(header.offset, 42);
        assert_eq!(header.group.as_str(), "GroupA");
        assert_eq!(header.origin_msg_id.as_deref(), Some("MSG_ID_A"));
        assert_eq!(header.origin_topic.as_deref(), Some("TopicA"));
        assert!(header
            .rpc_request_header
            .as_ref()
            .expect("RPC header should be present")
            .broker_name
            .is_none());
        assert!(!map.contains_key(&CheetahString::from_static_str("brokerName")));
    }

    #[test]
    fn consumer_send_message_back_header_preserves_broker_name_when_java_passes_value() {
        let mut message = MessageExt::default();
        message.set_commit_log_offset(42);
        message.msg_id = CheetahString::from_static_str("MSG_ID_A");
        message.set_topic(CheetahString::from_static_str("TopicA"));

        let header =
            MQClientAPIImpl::consumer_send_message_back_request_header(&message, Some("broker-a"), "GroupA", 3, 16);
        let map = header.to_map().expect("header should encode");

        assert_eq!(
            header
                .rpc_request_header
                .as_ref()
                .and_then(|rpc| rpc.broker_name.as_deref()),
            Some("broker-a")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerName"))
                .map(CheetahString::as_str),
            Some("broker-a")
        );
    }

    #[test]
    fn notify_result_from_response_maps_polling_full_like_java() {
        let mut response = RemotingCommand::create_response_command_with_header(NotificationResponseHeader {
            has_msg: true,
            polling_full: true,
        });
        response.make_custom_header_to_net();

        let notify_result = notify_result_from_response(&response).expect("notification response should decode");

        assert!(notify_result.is_has_msg());
        assert!(notify_result.is_polling_full());
    }

    #[test]
    fn decode_cluster_acl_version_info_response_body_decodes_java_json() {
        let body = bytes::Bytes::from_static(
            br#"{
                "brokerName":"broker-a",
                "brokerAddr":"127.0.0.1:10911",
                "aclConfigDataVersion":null,
                "allAclConfigDataVersion":{},
                "clusterName":"DefaultCluster"
            }"#,
        );

        let version_info = decode_cluster_acl_version_info_response_body(Some(&body))
            .expect("cluster ACL version response should decode");

        assert_eq!(version_info.broker_name.as_str(), "broker-a");
        assert_eq!(version_info.broker_addr.as_str(), "127.0.0.1:10911");
        assert_eq!(version_info.cluster_name.as_str(), "DefaultCluster");
        assert!(version_info.acl_config_data_version.is_none());
        assert!(version_info.all_acl_config_data_version.is_empty());
    }

    #[test]
    fn decode_cluster_acl_version_info_response_body_rejects_success_without_body() {
        let error = decode_cluster_acl_version_info_response_body(None)
            .expect_err("SUCCESS cluster ACL version response must include a body");

        assert!(error
            .to_string()
            .contains("get_broker_cluster_acl_version_info response body is empty"));
    }

    #[test]
    fn reset_offset_table_from_response_rejects_success_without_body_like_java() {
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);

        let error = reset_offset_table_from_response(&response)
            .expect_err("Java invokeBrokerToResetOffset throws when SUCCESS has no body");

        assert!(error.to_string().contains("reset offset response body is empty"));
    }

    #[test]
    fn reset_offset_table_from_response_decodes_java_body() {
        let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let mut body = ResetOffsetBody::new();
        body.offset_table.insert(mq.clone(), 42);
        let response =
            RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_body(body.encode());

        let offset_table = reset_offset_table_from_response(&response).expect("valid reset offset body should decode");

        assert_eq!(offset_table.get(&mq), Some(&42));
    }

    #[test]
    fn async_send_retries_transient_network_failures_only() {
        let connection_failed =
            rocketmq_error::RocketMQError::network_connection_failed("broker-a:10911", "connection failed");
        let send_failed = rocketmq_error::RocketMQError::Network(rocketmq_error::NetworkError::send_failed(
            "broker-a:10911",
            "write failed",
        ));

        assert!(MQClientAPIImpl::should_retry_async_send_error(&connection_failed));
        assert!(MQClientAPIImpl::should_retry_async_send_error(&send_failed));
    }

    #[test]
    fn async_send_does_not_retry_timeout_backpressure_or_stopped_client() {
        let timeout = rocketmq_error::RocketMQError::Timeout {
            operation: "send_request",
            timeout_ms: 3_000,
        };
        let request_timeout = rocketmq_error::RocketMQError::Network(rocketmq_error::NetworkError::RequestTimeout {
            addr: "broker-a:10911".to_string(),
            timeout_ms: 3_000,
        });
        let too_many_requests = rocketmq_error::RocketMQError::Network(rocketmq_error::NetworkError::TooManyRequests {
            addr: "broker-a:10911".to_string(),
            limit: 1,
        });

        assert!(!MQClientAPIImpl::should_retry_async_send_error(&timeout));
        assert!(!MQClientAPIImpl::should_retry_async_send_error(&request_timeout));
        assert!(!MQClientAPIImpl::should_retry_async_send_error(&too_many_requests));
        assert!(!MQClientAPIImpl::should_retry_async_send_error(
            &rocketmq_error::RocketMQError::ClientShuttingDown,
        ));
    }

    #[test]
    fn async_retry_request_reuses_final_attempt_after_first_failure() {
        let retry_key = CheetahString::from_static_str("retry-key");
        let retry_value = CheetahString::from_static_str("retry-value");
        let mut request = RemotingCommand::create_request_command(RequestCode::SendMessageV2, EmptyHeader {})
            .set_body(bytes::Bytes::from_static(b"retry-body"))
            .set_ext_fields(HashMap::from([(retry_key.clone(), retry_value.clone())]));
        let initial_opaque = RemotingCommand::create_new_request_id();
        let retry_opaque = RemotingCommand::create_new_request_id();
        request.set_opaque_mut(initial_opaque);

        let mut retry_request = AsyncRetryRequest::new(request);
        let first_attempt = retry_request.next_attempt(true);

        assert_eq!(first_attempt.opaque(), initial_opaque);
        assert_eq!(
            first_attempt.body().map(bytes::Bytes::as_ref),
            Some(b"retry-body".as_slice())
        );
        assert_eq!(
            first_attempt
                .ext_fields()
                .and_then(|fields| fields.get(&retry_key))
                .map(CheetahString::as_str),
            Some(retry_value.as_str())
        );
        assert!(!retry_request.is_consumed());

        retry_request.set_retry_opaque(retry_opaque);
        let second_attempt = retry_request.next_attempt(false);

        assert_eq!(second_attempt.opaque(), retry_opaque);
        assert_eq!(
            second_attempt.body().map(bytes::Bytes::as_ref),
            Some(b"retry-body".as_slice())
        );
        assert_eq!(
            second_attempt
                .ext_fields()
                .and_then(|fields| fields.get(&retry_key))
                .map(CheetahString::as_str),
            Some(retry_value.as_str())
        );
        assert!(retry_request.is_consumed());
    }

    #[test]
    fn async_retry_request_consumes_immediate_final_attempt_without_clone_template() {
        let mut request = RemotingCommand::create_request_command(RequestCode::SendMessageV2, EmptyHeader {})
            .set_body(bytes::Bytes::from_static(b"single-attempt-body"));
        let initial_opaque = RemotingCommand::create_new_request_id();
        request.set_opaque_mut(initial_opaque);

        let mut retry_request = AsyncRetryRequest::new(request);
        let attempt = retry_request.next_attempt(false);

        assert_eq!(attempt.opaque(), initial_opaque);
        assert_eq!(
            attempt.body().map(bytes::Bytes::as_ref),
            Some(b"single-attempt-body".as_slice())
        );
        assert!(retry_request.is_consumed());
    }

    #[test]
    fn async_retry_request_materializes_custom_header_for_clone_and_final_attempt() {
        let header_value = CheetahString::from_static_str("retry-header-value");
        let request = RemotingCommand::create_request_command(
            RequestCode::SendMessageV2,
            AsyncRetryTestHeader {
                retry_marker: header_value.clone(),
            },
        );

        let mut retry_request = AsyncRetryRequest::new(request);
        let first_attempt = retry_request.next_attempt(true);

        assert_eq!(
            first_attempt
                .ext_fields()
                .and_then(|fields| fields.get(&CheetahString::from_static_str("retryMarker")))
                .map(CheetahString::as_str),
            Some(header_value.as_str())
        );
        let decoded_first = first_attempt
            .decode_command_custom_header::<AsyncRetryTestHeader>()
            .expect("materialized custom header should decode from cloned retry attempt");
        assert_eq!(decoded_first.retry_marker.as_str(), header_value.as_str());
        assert!(!retry_request.is_consumed());

        retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
        let final_attempt = retry_request.next_attempt(false);

        assert_eq!(
            final_attempt
                .ext_fields()
                .and_then(|fields| fields.get(&CheetahString::from_static_str("retryMarker")))
                .map(CheetahString::as_str),
            Some(header_value.as_str())
        );
        let decoded_final = final_attempt
            .decode_command_custom_header::<AsyncRetryTestHeader>()
            .expect("materialized custom header should decode from final retry attempt");
        assert_eq!(decoded_final.retry_marker.as_str(), header_value.as_str());
        assert!(retry_request.is_consumed());
    }
}
