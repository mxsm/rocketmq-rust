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
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::communication_mode::CommunicationMode;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::runtime::spawn_client_task_with_context;
use cheetah_string::CheetahString;

use crate::base::validators::Validators;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::attribute::attribute_parser::AttributeParser;
use rocketmq_model::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::constant::file_readahead_mode;
use rocketmq_model::common::lite::LiteSubscriptionDTO;
use rocketmq_model::common::message::message_batch::MessageBatch;
use rocketmq_model::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_enum::MessageType;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mq_version::CURRENT_VERSION;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::utils::env_utils::EnvUtils;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::bodies::broker::GetBrokerLiteInfoResponseBody;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_protocol::protocol::body::check_client_request_body::CheckClientRequestBody;
use rocketmq_protocol::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_protocol::protocol::body::cluster_acl_version_info::ClusterAclVersionInfo;
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_protocol::protocol::body::create_topic_list_request_body::CreateTopicListRequestBody;
use rocketmq_protocol::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_protocol::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_protocol::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::group_list::GroupList;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_protocol::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_protocol::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_protocol::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_protocol::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_protocol::protocol::body::query_consume_time_span_body::QueryConsumeTimeSpanBody;
use rocketmq_protocol::protocol::body::query_correction_offset_body::QueryCorrectionOffsetBody;
use rocketmq_protocol::protocol::body::query_subscription_response_body::QuerySubscriptionResponseBody;
use rocketmq_protocol::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::response::get_consumer_status_body::GetConsumerStatusBody;
use rocketmq_protocol::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_protocol::protocol::body::response::reset_offset_body::ResetOffsetBody;
use rocketmq_protocol::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_protocol::protocol::body::subscription_group_list::SubscriptionGroupList;
use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_protocol::protocol::header::add_broker_request_header::AddBrokerRequestHeader;
use rocketmq_protocol::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_protocol::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_protocol::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::header::clone_group_offset_request_header::CloneGroupOffsetRequestHeader;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_protocol::protocol::header::create_acl_request_header::CreateAclRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_list_request_header::CreateTopicListRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::create_user_request_header::CreateUserRequestHeader;
use rocketmq_protocol::protocol::header::delete_acl_request_header::DeleteAclRequestHeader;
use rocketmq_protocol::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
use rocketmq_protocol::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_protocol::protocol::header::delete_user_request_header::DeleteUserRequestHeader;
use rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
use rocketmq_protocol::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_protocol::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader;
use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_protocol::protocol::header::get_acl_request_header::GetAclRequestHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_user_request_headers::GetUserRequestHeader;
use rocketmq_protocol::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_protocol::protocol::header::list_acl_request_header::ListAclRequestHeader;
use rocketmq_protocol::protocol::header::list_users_request_header::ListUsersRequestHeader;
use rocketmq_protocol::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::config_header::GetNamesrvConfigRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_protocol::protocol::header::query_correction_offset_header::QueryCorrectionOffsetHeader;
use rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_message_response_header::QueryMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_protocol::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
use rocketmq_protocol::protocol::header::remove_broker_request_header::RemoveBrokerRequestHeader;
use rocketmq_protocol::protocol::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::header::resume_check_half_message_request_header::ResumeCheckHalfMessageRequestHeader;
use rocketmq_transport::ConnectionNetEvent;
use rocketmq_transport::DefaultTopAddressing;
use rocketmq_transport::HeartbeatV2Result;
use rocketmq_transport::NameServerUpdateCallback;
use rocketmq_transport::RemotingClient;
use rocketmq_transport::RocketmqDefaultClient;
use rocketmq_transport::TopAddressing;

use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::header::trigger_lite_dispatch_request_header::TriggerLiteDispatchRequestHeader;
use rocketmq_protocol::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_protocol::protocol::header::update_acl_request_header::UpdateAclRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_global_white_addrs_config_request_header::UpdateGlobalWhiteAddrsConfigRequestHeader;
use rocketmq_protocol::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_protocol::protocol::header::update_user_request_header::UpdateUserRequestHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::headers::client::GetConsumerConnectionListRequestHeader;
use rocketmq_protocol::protocol::headers::view::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::headers::view::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::remoting_command;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_protocol::protocol::subscription::group_forbidden::GroupForbidden;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_transport::RPCHook;
use rocketmq_transport::RemotingService;
use rocketmq_transport::RpcRequestHeader;
use rocketmq_transport::TokioClientConfig;
use rocketmq_transport::TopicRequestHeader;
use rocketmq_transport::TransportTelemetry;
use tokio::sync::RwLock;
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

#[derive(Clone, Default)]
struct AsyncSendHookContext {
    producer_group: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    born_host: Option<CheetahString>,
    communication_mode: Option<CommunicationMode>,
    msg_type: Option<MessageType>,
    namespace: Option<CheetahString>,
    mq_trace_context: Option<Arc<Box<dyn std::any::Any + Send + Sync>>>,
    hooks: Arc<[Arc<dyn SendMessageHook>]>,
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
    service_context: ChildServiceContext,
    remoting_client: Arc<RocketmqDefaultClient<ClientRemotingProcessor>>,
    top_addressing: Arc<Box<dyn TopAddressing>>,
    name_srv_addr: RwLock<Option<String>>,
    client_config: Arc<ClientConfig>,
    background_tasks: TaskTracker,
    background_shutdown: CancellationToken,
}

async fn update_cached_name_server_addr<F>(cache: &RwLock<Option<String>>, addrs: &str, update: F) -> bool
where
    F: FnOnce(&str),
{
    let mut current = cache.write().await;
    if current.as_deref() == Some(addrs) {
        return false;
    }
    update(addrs);
    *current = Some(addrs.to_owned());
    true
}

mod admin;
mod consumer;
mod producer;
mod request_builder;
mod response_decoder;
mod route;
mod transaction;
mod transport;

pub use admin::AdminClient;
pub use consumer::ConsumerClient;
pub use producer::ProducerClient;
pub use route::RouteClient;
pub use transaction::TransactionClient;

#[cfg(test)]
#[path = "../../tests/implementation/mq_client_api_impl/unit.rs"]
mod tests;
