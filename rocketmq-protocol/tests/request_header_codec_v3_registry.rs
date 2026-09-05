// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_model::boundary_type::BoundaryType;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_protocol::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_protocol::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
use rocketmq_protocol::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::header::clone_group_offset_request_header::CloneGroupOffsetRequestHeader;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_list_request_header::CreateTopicListRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
use rocketmq_protocol::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_protocol::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_request_header::GetTopicStatsRequestHeader;
use rocketmq_protocol::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_protocol::protocol::header::lite_subscription_ctl_request_header::LiteSubscriptionCtlRequestHeader;
use rocketmq_protocol::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as NamesrvTopicRequestHeader;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_protocol::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
use rocketmq_protocol::protocol::header::peek_message_request_header::PeekMessageRequestHeader;
use rocketmq_protocol::protocol::header::polling_info_request_header::PollingInfoRequestHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::query_correction_offset_header::QueryCorrectionOffsetHeader;
use rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_protocol::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_protocol::protocol::header::{
    ack_message_request_header::AckMessageRequestHeader, pop_message_request_header::PopMessageRequestHeader,
};
use rocketmq_protocol::protocol::header_codec::{
    AliasConflictPolicy, HeaderCodec, HeaderFieldSpec, HeaderFlattenSpec, HeaderPresence, HeaderRange, HeaderValueKind,
};
#[allow(deprecated, reason = "verifies the source-compatible legacy adapter delegates to V3")]
use rocketmq_protocol::protocol::FastCodesHeader;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_protocol::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_protocol::{
    CommandCustomHeader, FromMap, HeaderEncodeCapability, HeaderMap, ProtocolContractViolation, RemotingCommand,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JavaSchema {
    headers: Vec<JavaHeader>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JavaHeader {
    rust_type_id: String,
    rust_type: String,
    java_class: String,
    java_fast: bool,
    fields: Vec<JavaField>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JavaField {
    key: String,
    java_type: String,
    presence: String,
    default_semantic: String,
    declared_in: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaOverrides {
    defaults: Vec<DefaultOverride>,
    alias_conflict_policies: Vec<AliasOverride>,
    required_drift: Vec<RequiredOverride>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionAllowlist {
    extensions: Vec<ExtensionOverride>,
    rust_only_types: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionOverride {
    rust_type_id: String,
    fields: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MigrationManifest {
    entries: Vec<MigrationEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MigrationEntry {
    rust_type_id: String,
    current_codec: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DefaultOverride {
    rust_type: String,
    field: String,
    semantic: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequiredOverride {
    rust_type: String,
    field: String,
    java_presence: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AliasOverride {
    rust_type: String,
    canonical: String,
    aliases: Vec<String>,
    policy: String,
}

struct RegisteredSchema {
    type_id: &'static str,
    java_class: Option<&'static str>,
    direct_binary: bool,
    local_fields: &'static [HeaderFieldSpec],
    fields: Vec<HeaderFieldSpec>,
    flattens: Vec<HeaderFlattenSpec>,
}

static VALIDATION_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::SingleValidationHeader",
    crate = "rocketmq_protocol",
    validate = "Self::count_validation",
    fast
)]
struct SingleValidationHeader {
    #[header(required)]
    value: CheetahString,
}

impl SingleValidationHeader {
    fn count_validation(&self) -> Result<(), ProtocolContractViolation> {
        VALIDATION_CALLS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn register_value<T: HeaderCodec + CommandCustomHeader>(header: &T) -> RegisteredSchema {
    assert_eq!(
        header.encode_capability() == HeaderEncodeCapability::DirectBinary,
        T::FAST_ENABLED,
        "{} encode capability must follow its fast schema flag",
        T::TYPE_ID
    );
    assert_eq!(
        header.supports_direct_json_fields(),
        T::FAST_ENABLED,
        "{} direct JSON capability must follow its fast schema flag",
        T::TYPE_ID
    );
    let mut fields = Vec::new();
    T::visit_field_specs(&mut |field| fields.push(*field));
    let mut flattens = Vec::new();
    T::visit_flatten_specs(&mut |flatten| flattens.push(*flatten));
    RegisteredSchema {
        type_id: T::TYPE_ID,
        java_class: T::JAVA_CLASS,
        direct_binary: T::FAST_ENABLED,
        local_fields: T::LOCAL_FIELD_SPECS,
        fields,
        flattens,
    }
}

fn register<T: HeaderCodec + CommandCustomHeader + Default>() -> RegisteredSchema {
    register_value(&T::default())
}

fn registry() -> Vec<RegisteredSchema> {
    vec![
        register::<RpcRequestHeader>(),
        register::<TopicRequestHeader>(),
        register::<NamesrvTopicRequestHeader>(),
        register_value(&AckMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            extra_info: CheetahString::from_static_str("registry"),
            offset: 0,
            lite_topic: None,
            topic_request_header: None,
        }),
        register_value(&CheckTransactionStateRequestHeader {
            topic: None,
            tran_state_table_offset: 0,
            commit_log_offset: 0,
            msg_id: None,
            transaction_id: None,
            offset_msg_id: None,
            rpc_request_header: None,
        }),
        register_value(&CheckRocksdbCqWriteProgressRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            check_store_time: 0,
            rpc: None,
        }),
        register::<ChangeInvisibleTimeRequestHeader>(),
        register::<CloneGroupOffsetRequestHeader>(),
        register::<ConsumeMessageDirectlyResultRequestHeader>(),
        register_value(&ConsumerSendMsgBackRequestHeader {
            offset: 0,
            group: CheetahString::from_static_str("registry"),
            delay_level: 0,
            origin_msg_id: None,
            origin_topic: None,
            unit_mode: false,
            max_reconsume_times: None,
            rpc_request_header: None,
        }),
        register::<CleanBrokerDataRequestHeader>(),
        register::<CreateTopicListRequestHeader>(),
        register_value(&CreateTopicRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            default_topic: CheetahString::from_static_str("registry"),
            read_queue_nums: 0,
            write_queue_nums: 0,
            perm: 0,
            topic_filter_type: CheetahString::from_static_str("SINGLE_TAG"),
            topic_sys_flag: None,
            order: false,
            attributes: None,
            force: Some(false),
            topic_request_header: None,
        }),
        register::<DeleteSubscriptionGroupRequestHeader>(),
        register_value(&DeleteTopicRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&EndTransactionRequestHeader {
            topic: CheetahString::new(),
            producer_group: CheetahString::from_static_str("registry"),
            tran_state_table_offset: 0,
            commit_log_offset: 0,
            commit_or_rollback: MessageSysFlag::TRANSACTION_NOT_TYPE,
            from_transaction_check: false,
            msg_id: CheetahString::from_static_str("registry"),
            transaction_id: None,
            rpc_request_header: RpcRequestHeader::default(),
        }),
        register_value(&GetConsumerConnectionListRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            rpc_request_header: None,
        }),
        register_value(&GetConsumerListByGroupRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            rpc: None,
        }),
        register::<GetConsumerRunningInfoRequestHeader>(),
        register::<GetConsumerStatusRequestHeader>(),
        register_value(&GetConsumeStatsRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::new(),
            topic_list: None,
            topic_request_header: None,
        }),
        register_value(&GetEarliestMsgStoretimeRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            topic_request_header: None,
        }),
        register::<GetLiteClientInfoRequestHeader>(),
        register_value(&GetLiteGroupInfoRequestHeader {
            group: CheetahString::from_static_str("registry"),
            lite_topic: CheetahString::new(),
            top_k: 0,
            rpc: None,
        }),
        register_value(&GetParentTopicInfoRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            rpc: None,
        }),
        register_value(&GetMaxOffsetRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            committed: true,
            topic_request_header: None,
        }),
        register_value(&GetMinOffsetRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            topic_request_header: None,
        }),
        register_value(&GetRouteInfoRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            accept_standard_json_only: None,
            topic_request_header: None,
        }),
        register::<GetProducerConnectionListRequestHeader>(),
        register::<GetSubscriptionGroupConfigRequestHeader>(),
        register_value(&GetTopicConfigRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&GetTopicStatsInfoRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&GetTopicStatsRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register::<HeartbeatRequestHeader>(),
        register::<LiteSubscriptionCtlRequestHeader>(),
        register::<LockBatchMqRequestHeader>(),
        register::<SendMessageRequestHeader>(),
        register::<DeleteTopicFromNamesrvRequestHeader>(),
        register::<RegisterTopicRequestHeader>(),
        register::<QueryConsumeQueueRequestHeader>(),
        register_value(&QueryConsumeTimeSpanRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            group: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&QueryCorrectionOffsetHeader {
            filter_groups: None,
            compare_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&QueryMessageRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            key: CheetahString::from_static_str("registry"),
            max_num: 0,
            begin_timestamp: 0,
            end_timestamp: 0,
            index_type: None,
            last_key: None,
            topic_request_header: None,
        }),
        register_value(&QuerySubscriptionByConsumerRequestHeader {
            group: CheetahString::from_static_str("registry"),
            topic: CheetahString::new(),
            topic_request_header: None,
        }),
        register_value(&QueryTopicConsumeByWhoRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&RecallMessageRequestHeader {
            producer_group: None,
            topic: CheetahString::from_static_str("registry"),
            recall_handle: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register_value(&ReplyMessageRequestHeader {
            producer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            default_topic: CheetahString::from_static_str("registry"),
            default_topic_queue_nums: 0,
            queue_id: 0,
            sys_flag: 0,
            born_timestamp: 0,
            flag: 0,
            properties: None,
            reconsume_times: None,
            unit_mode: Some(false),
            born_host: CheetahString::from_static_str("registry"),
            store_host: CheetahString::from_static_str("registry"),
            store_timestamp: 0,
            topic_request: None,
        }),
        register::<ResetOffsetRequestHeader>(),
        register::<SearchOffsetRequestHeader>(),
        register::<SearchOffsetResponseHeader>(),
        register::<PullMessageRequestHeader>(),
        register::<PullMessageResponseHeader>(),
        register_value(&PeekMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            max_msg_nums: 0,
            topic_request_header: None,
        }),
        register_value(&PollingInfoRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            topic_request_header: None,
        }),
        register_value(&PopMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            max_msg_nums: 0,
            invisible_time: 0,
            poll_time: 0,
            born_time: 0,
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        }),
        register_value(&PopLiteMessageRequestHeader {
            client_id: CheetahString::from_static_str("registry"),
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            max_msg_num: 0,
            invisible_time: 0,
            poll_time: 0,
            born_time: 0,
            attempt_id: None,
            rpc: None,
        }),
        register::<SendMessageRequestHeaderV2>(),
        register::<SendMessageResponseHeader>(),
        register::<NotificationRequestHeader>(),
        register_value(&NotifyConsumerIdsChangedRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            rpc_request_header: None,
        }),
        register_value(&NotifyUnsubscribeLiteRequestHeader {
            lite_topic: CheetahString::from_static_str("registry"),
            consumer_group: CheetahString::from_static_str("registry"),
            client_id: CheetahString::from_static_str("registry"),
            rpc_request_header: None,
        }),
        register::<QueryTopicsByConsumerRequestHeader>(),
        register_value(&QueryConsumerOffsetRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            set_zero_if_not_found: None,
            topic_request_header: None,
        }),
        register::<UnlockBatchMqRequestHeader>(),
        register::<UnregisterClientRequestHeader>(),
        register_value(&UpdateConsumerOffsetRequestHeader {
            consumer_group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            queue_id: 0,
            commit_offset: 0,
            topic_request_header: None,
        }),
        register_value(&UpdateGroupForbiddenRequestHeader {
            group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            readable: None,
            topic_request_header: None,
        }),
        // Flat request and response headers migrated as one schema-governed cohort.
        register::<rocketmq_protocol::protocol::header::broker::broker_heartbeat_request_header::BrokerHeartbeatRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::check_transaction_state_response_header::CheckTransactionStateResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::elect_master_response_header::ElectMasterResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::exchange_ha_info_request_header::ExchangeHAInfoRequestHeader>(),
        register_value(
            &rocketmq_protocol::protocol::header::exchange_ha_info_response_header::ExchangeHaInfoResponseHeader {
                master_ha_address: None,
                master_flush_offset: None,
                master_address: None,
            },
        ),
        register::<rocketmq_protocol::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::get_all_subscription_group_request_header::GetAllSubscriptionGroupRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::get_all_subscription_group_request_header::GetAllSubscriptionGroupResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::get_all_topic_config_request_header::GetAllTopicConfigRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::get_all_topic_config_response_header::GetAllTopicConfigResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::get_broker_config_response_header::GetBrokerConfigResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader>(),
        register::<rocketmq_protocol::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::query_data_version_header::QueryDataVersionRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::query_data_version_header::QueryDataVersionResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader>(),
        register_value(
            &rocketmq_protocol::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader {
                master_address: None,
                master_epoch: None,
                sync_state_set_epoch: None,
                master_broker_id: None,
            },
        ),
        register::<rocketmq_protocol::protocol::header::polling_info_response_header::PollingInfoResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::query_message_response_header::QueryMessageResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::recall_message_response_header::RecallMessageResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::remove_broker_request_header::RemoveBrokerRequestHeader>(),
        register_value(
            &rocketmq_protocol::protocol::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader {
                master_flush_offset: None,
            },
        ),
        register_value(
            &rocketmq_protocol::protocol::header::trigger_lite_dispatch_request_header::TriggerLiteDispatchRequestHeader {
                group: CheetahString::from_static_str("registry"),
                client_id: None,
            },
        ),
        register_value(
            &rocketmq_protocol::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader {
                stats_name: CheetahString::from_static_str("registry"),
                stats_key: CheetahString::from_static_str("registry"),
            },
        ),
        register::<rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader>(),
        // Compatibility-sensitive ACL, user, controller, nameserver, and response schemas.
        register::<rocketmq_protocol::protocol::header::add_broker_request_header::AddBrokerRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::create_acl_request_header::CreateAclRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::create_user_request_header::CreateUserRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::delete_acl_request_header::DeleteAclRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::delete_user_request_header::DeleteUserRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::get_acl_request_header::GetAclRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::get_all_producer_info_request_header::GetAllProducerInfoRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::get_consumer_listby_group_response_header::GetConsumerListByGroupResponseHeader>(),
        register_value(
            &rocketmq_protocol::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader {
                parent_topic: CheetahString::from_static_str("registry"),
                lite_topic: CheetahString::from_static_str("registry"),
            },
        ),
        register::<rocketmq_protocol::protocol::header::get_user_request_headers::GetUserRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::list_acl_request_header::ListAclRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::list_users_request_header::ListUsersRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::resume_check_half_message_request_header::ResumeCheckHalfMessageRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::update_acl_request_header::UpdateAclRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::update_user_request_header::UpdateUserRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::view_message_response_header::ViewMessageResponseHeader>(),
        // Rust maintenance, probe, and compare-and-set configuration extensions.
        register_value(
            &rocketmq_protocol::protocol::header::maintenance_request_header::MaintenanceRequestHeader {
                operation_id: CheetahString::from_static_str("registry"),
                policy_version: 1,
                deadline_unix_millis: 1,
                fencing_token: 1,
            },
        ),
        register::<rocketmq_protocol::protocol::header::namesrv::config_header::GetNamesrvConfigRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::update_broker_config_request_header::UpdateBrokerConfigRequestHeader>(),
        register::<rocketmq_protocol::protocol::header::update_broker_config_response_header::UpdateBrokerConfigResponseHeader>(),
        register::<rocketmq_protocol::protocol::header::update_consumer_offset_conditional_header::UpdateConsumerOffsetConditionalHeader>(),
        register::<rocketmq_protocol::protocol::header::update_global_white_addrs_config_request_header::UpdateGlobalWhiteAddrsConfigRequestHeader>(),
        register_value(
            &rocketmq_protocol::protocol::header::update_subscription_group_config_cas_request_header::UpdateSubscriptionGroupConfigCasRequestHeader {
                group: CheetahString::from_static_str("registry"),
                expected_version: 1,
                ..Default::default()
            },
        ),
        register::<rocketmq_protocol::protocol::header::update_subscription_group_config_cas_response_header::UpdateSubscriptionGroupConfigCasResponseHeader>(),
        register_value(
            &rocketmq_protocol::protocol::header::update_topic_config_cas_request_header::UpdateTopicConfigCasRequestHeader {
                topic: CheetahString::from_static_str("registry"),
                expected_version: 1,
                ..Default::default()
            },
        ),
        register::<rocketmq_protocol::protocol::header::update_topic_config_cas_response_header::UpdateTopicConfigCasResponseHeader>(),
    ]
}

#[test]
fn typed_registry_contains_every_migrated_v3_header_exactly_once() {
    let migration: MigrationManifest =
        serde_json::from_str(include_str!("../../scripts/request-header-codec/migration.json"))
            .expect("checked-in migration manifest");
    let expected = migration
        .entries
        .iter()
        .filter(|entry| entry.current_codec == "v3")
        .map(|entry| entry.rust_type_id.as_str())
        .collect::<HashSet<_>>();
    let registered = registry();
    let actual = registered.iter().map(|schema| schema.type_id).collect::<HashSet<_>>();

    assert_eq!(
        actual.len(),
        registered.len(),
        "typed registry contains duplicate type IDs"
    );
    assert_eq!(
        actual, expected,
        "typed registry and migration manifest must cover the same V3 headers"
    );
}

#[test]
fn performance_corpus_headers_use_generated_direct_codecs() {
    const {
        assert!(<ConsumeMessageDirectlyResultRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<CleanBrokerDataRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<GetConsumerStatusRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<SendMessageRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<DeleteTopicFromNamesrvRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<QueryConsumeQueueRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<GetLiteClientInfoRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<SendMessageRequestHeaderV2 as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<SendMessageResponseHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<NotificationRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<PullMessageRequestHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
        assert!(<PullMessageResponseHeader as FromMap>::SUPPORTS_HEADER_FIELD_SOURCE);
    }

    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../scripts/request-header-codec/perf-corpus-v1.json"))
            .expect("checked-in performance corpus");
    let registered: HashMap<_, _> = registry()
        .into_iter()
        .map(|schema| (schema.type_id, schema.direct_binary))
        .collect();

    for case in corpus["cases"].as_array().expect("corpus cases") {
        let type_id = case["header"].as_str().expect("corpus header type ID");
        assert!(
            registered.contains_key(type_id),
            "{type_id} must use a generated direct source codec"
        );
    }

    for type_id in [
        ConsumeMessageDirectlyResultRequestHeader::TYPE_ID,
        CleanBrokerDataRequestHeader::TYPE_ID,
        GetConsumerStatusRequestHeader::TYPE_ID,
        SendMessageRequestHeader::TYPE_ID,
        DeleteTopicFromNamesrvRequestHeader::TYPE_ID,
        QueryConsumeQueueRequestHeader::TYPE_ID,
    ] {
        assert_eq!(
            registered.get(type_id),
            Some(&true),
            "{type_id} must use direct binary encoding"
        );
    }
}

fn rust_kind(kind: HeaderValueKind) -> &'static str {
    match kind {
        HeaderValueKind::String => "string",
        HeaderValueKind::Bool => "bool",
        HeaderValueKind::I32 | HeaderValueKind::U32 => "i32",
        HeaderValueKind::I64 | HeaderValueKind::U64 => "i64",
        HeaderValueKind::BoundaryType => "boundary",
    }
}

fn java_kind(java_type: &str) -> &'static str {
    match java_type {
        "java.lang.String" | "String" => "string",
        "boolean" | "java.lang.Boolean" | "Boolean" => "bool",
        "int" | "java.lang.Integer" | "Integer" => "i32",
        "long" | "java.lang.Long" | "Long" => "i64",
        value if value.ends_with(".BoundaryType") || value == "BoundaryType" => "boundary",
        value => panic!("unsupported registered Java field type {value}"),
    }
}

#[test]
fn registered_typed_schemas_match_the_pinned_java_contract() {
    let java: JavaSchema = serde_json::from_str(include_str!("fixtures/request_header_codec/java-schema.json"))
        .expect("pinned Java schema");
    let overrides: SchemaOverrides =
        serde_json::from_str(include_str!("../../scripts/request-header-codec/schema-overrides.json"))
            .expect("schema overrides");
    let extensions: ExtensionAllowlist = serde_json::from_str(include_str!(
        "../../scripts/request-header-codec/extension-allowlist.json"
    ))
    .expect("extension allowlist");
    let registered = registry();

    let mut type_ids = HashSet::new();
    for schema in &registered {
        assert!(
            type_ids.insert(schema.type_id),
            "duplicate typed schema ID {}",
            schema.type_id
        );
    }
    let by_type_id: HashMap<_, _> = registered.iter().map(|schema| (schema.type_id, schema)).collect();
    let rust_only_type_ids = extensions
        .rust_only_types
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let registered_rust_only_type_ids = registered
        .iter()
        .filter(|schema| schema.java_class.is_none())
        .map(|schema| schema.type_id)
        .collect::<HashSet<_>>();
    assert_eq!(
        registered_rust_only_type_ids, rust_only_type_ids,
        "registered schemas without Java peers must match the reviewed Rust-only allowlist"
    );

    for schema in &registered {
        assert!(
            schema.local_fields.iter().all(|field| field.java_type.is_none()),
            "{} must infer Java-compatible value kinds instead of repeating java_type metadata",
            schema.type_id
        );
        if rust_only_type_ids.contains(schema.type_id) {
            assert!(
                schema.fields.iter().all(|field| field.java_range.is_none()),
                "{} Rust-only fields must retain their native numeric domains",
                schema.type_id
            );
            for flatten in &schema.flattens {
                assert!(
                    by_type_id.contains_key(flatten.nested_type_id),
                    "{} flattens unregistered {}",
                    schema.type_id,
                    flatten.nested_type_id
                );
            }
            continue;
        }
        let java_header = java
            .headers
            .iter()
            .find(|header| header.rust_type_id == schema.type_id)
            .unwrap_or_else(|| panic!("missing pinned Java schema for {}", schema.type_id));
        assert_eq!(schema.java_class, Some(java_header.java_class.as_str()));
        assert!(
            !java_header.java_fast || schema.direct_binary,
            "{} must preserve Java fast encoding through generated direct binary",
            schema.type_id
        );
        let allowed_extension_fields = extensions
            .extensions
            .iter()
            .find(|entry| entry.rust_type_id == schema.type_id)
            .map(|entry| entry.fields.iter().map(String::as_str).collect::<HashSet<_>>())
            .unwrap_or_default();
        assert_eq!(
            schema.fields.len(),
            java_header.fields.len() + allowed_extension_fields.len(),
            "{} field count",
            schema.type_id
        );

        let mut seen_extension_fields = HashSet::new();
        for field in &schema.fields {
            let owner = by_type_id
                .get(field.declared_in)
                .unwrap_or_else(|| panic!("unregistered field owner {}", field.declared_in));
            let Some(java_field) = java_header.fields.iter().find(|candidate| candidate.key == field.key) else {
                assert!(
                    allowed_extension_fields.contains(field.key),
                    "unreviewed Rust extension field {}.{}",
                    schema.type_id,
                    field.key
                );
                assert!(
                    seen_extension_fields.insert(field.key),
                    "duplicate Rust extension field {}.{}",
                    schema.type_id,
                    field.key
                );
                continue;
            };
            assert_eq!(rust_kind(field.kind), java_kind(&java_field.java_type));
            match field.kind {
                HeaderValueKind::U32 => assert_eq!(field.java_range, Some(HeaderRange::I32)),
                HeaderValueKind::U64 => assert_eq!(field.java_range, Some(HeaderRange::I64)),
                _ => assert_eq!(field.java_range, None),
            }

            assert_eq!(owner.java_class, Some(java_field.declared_in.as_str()));

            match field.presence {
                HeaderPresence::Required | HeaderPresence::Optional => {
                    let rust_presence = match field.presence {
                        HeaderPresence::Required => "required",
                        HeaderPresence::Optional => "optional",
                        HeaderPresence::Default | HeaderPresence::DefaultWith(_) => unreachable!(),
                    };
                    if java_field.presence != rust_presence {
                        assert!(
                            overrides.required_drift.iter().any(|entry| {
                                entry.rust_type == java_header.rust_type
                                    && entry.field == field.key
                                    && entry.java_presence == java_field.presence
                            }),
                            "{}.{} presence {} != Java {} without review",
                            schema.type_id,
                            field.key,
                            rust_presence,
                            java_field.presence
                        );
                    }
                }
                HeaderPresence::Default | HeaderPresence::DefaultWith(_) => {
                    let expected = field.default_semantic.expect("default fields declare stable semantics");
                    let reviewed = overrides
                        .defaults
                        .iter()
                        .find(|entry| entry.rust_type == java_header.rust_type && entry.field == field.key)
                        .map(|entry| entry.semantic.as_str())
                        .or_else(|| {
                            java_field
                                .default_semantic
                                .starts_with("literal:")
                                .then_some(java_field.default_semantic.as_str())
                        });
                    assert_eq!(reviewed, Some(expected), "{}.{} default", schema.type_id, field.key);
                }
            }
        }
        assert_eq!(
            seen_extension_fields, allowed_extension_fields,
            "{} reviewed extension fields must be present exactly once",
            schema.type_id
        );

        for flatten in &schema.flattens {
            assert!(
                by_type_id.contains_key(flatten.nested_type_id),
                "{} flattens unregistered {}",
                schema.type_id,
                flatten.nested_type_id
            );
        }

        for field in schema
            .local_fields
            .iter()
            .filter(|field| field.alias_conflict == AliasConflictPolicy::PreferCanonical)
        {
            let reviewed = overrides.alias_conflict_policies.iter().any(|entry| {
                entry.rust_type == java_header.rust_type
                    && entry.canonical == field.key
                    && entry
                        .aliases
                        .iter()
                        .map(String::as_str)
                        .eq(field.aliases.iter().copied())
                    && entry.policy == "prefer_canonical"
            });
            assert!(
                reviewed,
                "unreviewed prefer_canonical policy for {}.{}",
                schema.type_id, field.key
            );
        }
    }
}

fn assert_rpc_envelope_contract<T>(
    local_fields: &[(&'static str, &'static str)],
    required_keys: &[&'static str],
    rpc: fn(&T) -> &Option<RpcRequestHeader>,
) where
    T: CommandCustomHeader + FromMap<Target = T> + HeaderCodec,
    <T as FromMap>::Error: std::fmt::Debug,
{
    let mut input = HeaderMap::from([
        ("ns".into(), "canonical-ns".into()),
        ("namespace".into(), "legacy-ns".into()),
        ("nsd".into(), "true".into()),
        ("namespaced".into(), "false".into()),
        ("bname".into(), "canonical-broker".into()),
        ("brokerName".into(), "legacy-broker".into()),
        ("oway".into(), "false".into()),
        ("oneway".into(), "true".into()),
    ]);
    for &(key, value) in local_fields {
        input.insert(key.into(), value.into());
    }
    let typed = <T as HeaderCodec>::decode_from_map(&input).expect("typed RPC envelope decode");
    let legacy = <T as FromMap>::from(&input).expect("legacy RPC envelope adapter");
    for decoded in [rpc(&typed), rpc(&legacy)] {
        let decoded = decoded
            .as_ref()
            .expect("Java inheritance is always present after decode");
        assert_eq!(decoded.namespace.as_deref(), Some("canonical-ns"));
        assert_eq!(decoded.namespaced, Some(true));
        assert_eq!(decoded.broker_name.as_deref(), Some("canonical-broker"));
        assert_eq!(decoded.oneway, Some(false));
    }

    let encoded = typed.to_map().expect("typed RPC envelope encode");
    let legacy_encoded = legacy.to_map().expect("legacy RPC envelope encode");
    for &(key, value) in local_fields {
        assert_eq!(encoded.get(key).map(CheetahString::as_str), Some(value));
        assert_eq!(legacy_encoded.get(key).map(CheetahString::as_str), Some(value));
    }
    assert_eq!(encoded.get("ns").map(CheetahString::as_str), Some("canonical-ns"));
    assert_eq!(encoded.get("nsd").map(CheetahString::as_str), Some("true"));
    assert_eq!(
        encoded.get("bname").map(CheetahString::as_str),
        Some("canonical-broker")
    );
    assert_eq!(encoded.get("oway").map(CheetahString::as_str), Some("false"));
    for alias in ["namespace", "namespaced", "brokerName", "oneway"] {
        assert!(
            !encoded.contains_key(alias),
            "legacy alias {alias} must remain decode-only"
        );
    }

    let mut parent_only = HeaderMap::new();
    for &(key, value) in local_fields {
        parent_only.insert(key.into(), value.into());
    }
    for &key in required_keys {
        let mut missing = parent_only.clone();
        missing.remove(key);
        let typed_missing = <T as HeaderCodec>::decode_from_map(&missing);
        assert!(
            matches!(typed_missing, Err(ProtocolContractViolation::Missing { key: actual, .. }) if actual == key),
            "typed decode must reject missing required field {key}"
        );
        assert!(
            <T as FromMap>::from(&missing).is_err(),
            "legacy adapter must reject missing required field {key}"
        );
    }
    let empty = <T as HeaderCodec>::decode_from_map(&parent_only).expect("inherited header without RPC fields");
    let empty_rpc = rpc(&empty)
        .as_ref()
        .expect("Java parent exists even when all fields are absent");
    assert_eq!(empty_rpc.namespace, None);
    assert_eq!(empty_rpc.namespaced, None);
    assert_eq!(empty_rpc.broker_name, None);
    assert_eq!(empty_rpc.oneway, None);
    assert_eq!(empty.encode_capability(), HeaderEncodeCapability::MapOnly);
}

struct TopicEnvelopeRef<'a> {
    lo: Option<bool>,
    rpc: &'a Option<RpcRequestHeader>,
}

fn assert_topic_envelope_contract<T>(
    local_fields: &[(&'static str, &'static str)],
    required_keys: &[&'static str],
    topic: for<'a> fn(&'a T) -> Option<TopicEnvelopeRef<'a>>,
) where
    T: CommandCustomHeader + FromMap<Target = T> + HeaderCodec,
    <T as FromMap>::Error: std::fmt::Debug,
{
    let mut input = HeaderMap::from([
        ("lo".into(), "true".into()),
        ("ns".into(), "canonical-ns".into()),
        ("namespace".into(), "legacy-ns".into()),
        ("nsd".into(), "true".into()),
        ("namespaced".into(), "false".into()),
        ("bname".into(), "canonical-broker".into()),
        ("brokerName".into(), "legacy-broker".into()),
        ("oway".into(), "false".into()),
        ("oneway".into(), "true".into()),
    ]);
    for &(key, value) in local_fields {
        input.insert(key.into(), value.into());
    }

    let typed = <T as HeaderCodec>::decode_from_map(&input).expect("typed Topic envelope decode");
    let legacy = <T as FromMap>::from(&input).expect("legacy Topic envelope adapter");
    for decoded in [&typed, &legacy] {
        let topic = topic(decoded).expect("Java Topic parent is always present after decode");
        assert_eq!(topic.lo, Some(true));
        let rpc = topic
            .rpc
            .as_ref()
            .expect("Java RPC parent is always present after decode");
        assert_eq!(rpc.namespace.as_deref(), Some("canonical-ns"));
        assert_eq!(rpc.namespaced, Some(true));
        assert_eq!(rpc.broker_name.as_deref(), Some("canonical-broker"));
        assert_eq!(rpc.oneway, Some(false));
    }

    for encoded in [typed.to_map().unwrap(), legacy.to_map().unwrap()] {
        for &(key, value) in local_fields {
            assert_eq!(encoded.get(key).map(CheetahString::as_str), Some(value));
        }
        assert_eq!(encoded.get("lo").map(CheetahString::as_str), Some("true"));
        assert_eq!(encoded.get("ns").map(CheetahString::as_str), Some("canonical-ns"));
        assert_eq!(encoded.get("nsd").map(CheetahString::as_str), Some("true"));
        assert_eq!(
            encoded.get("bname").map(CheetahString::as_str),
            Some("canonical-broker")
        );
        assert_eq!(encoded.get("oway").map(CheetahString::as_str), Some("false"));
        for alias in ["namespace", "namespaced", "brokerName", "oneway"] {
            assert!(
                !encoded.contains_key(alias),
                "legacy alias {alias} must remain decode-only"
            );
        }
    }

    let parent_only = HeaderMap::from_iter(local_fields.iter().map(|&(key, value)| (key.into(), value.into())));
    for &key in required_keys {
        let mut missing = parent_only.clone();
        missing.remove(key);
        assert!(matches!(
            <T as HeaderCodec>::decode_from_map(&missing),
            Err(ProtocolContractViolation::Missing { key: actual, .. }) if actual == key
        ));
        assert!(<T as FromMap>::from(&missing).is_err());
    }

    let empty = <T as HeaderCodec>::decode_from_map(&parent_only).expect("Topic header without parent fields");
    let empty_topic = topic(&empty).expect("Java Topic parent exists even when inherited fields are absent");
    assert!(empty_topic.lo.is_none());
    assert!(empty_topic.rpc.is_some());
    assert_eq!(empty.encode_capability(), HeaderEncodeCapability::MapOnly);
}

#[test]
fn topic_headers_preserve_nested_java_inheritance_and_defaults() {
    assert_topic_envelope_contract::<GetMaxOffsetRequestHeader>(
        &[
            ("topic", "topic-max"),
            ("queueId", "2147483647"),
            ("committed", "false"),
        ],
        &["topic", "queueId"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<GetMinOffsetRequestHeader>(
        &[("topic", "topic-min"), ("queueId", "-2147483648")],
        &["topic", "queueId"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<GetEarliestMsgStoretimeRequestHeader>(
        &[("topic", "topic-earliest"), ("queueId", "0")],
        &["topic", "queueId"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<GetTopicConfigRequestHeader>(&[("topic", "topic-config")], &["topic"], |header| {
        header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
            lo: topic.lo,
            rpc: &topic.rpc_request_header,
        })
    });
    assert_topic_envelope_contract::<GetTopicStatsInfoRequestHeader>(
        &[("topic", "topic-stats-info")],
        &["topic"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<GetTopicStatsRequestHeader>(&[("topic", "topic-stats")], &["topic"], |header| {
        header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
            lo: topic.lo,
            rpc: &topic.rpc_request_header,
        })
    });
    assert_topic_envelope_contract::<DeleteTopicRequestHeader>(&[("topic", "topic-delete")], &["topic"], |header| {
        header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
            lo: topic.lo,
            rpc: &topic.rpc_request_header,
        })
    });
    assert_topic_envelope_contract::<RegisterTopicRequestHeader>(
        &[("topic", "topic-register")],
        &["topic"],
        |header| {
            header.topic_request.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<QueryTopicConsumeByWhoRequestHeader>(
        &[("topic", "topic-consumers")],
        &["topic"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<GetConsumeStatsRequestHeader>(
        &[
            ("consumerGroup", "consumer-a"),
            ("topic", "topic-stats"),
            ("topicList", "topic-a;topic-b"),
        ],
        &["consumerGroup"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<QueryConsumeTimeSpanRequestHeader>(
        &[("topic", "topic-span"), ("group", "group-span")],
        &["topic", "group"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<QueryCorrectionOffsetHeader>(
        &[
            ("filterGroups", "group-a,group-b"),
            ("compareGroup", "group-c"),
            ("topic", "topic-correction"),
        ],
        &["compareGroup", "topic"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<QuerySubscriptionByConsumerRequestHeader>(
        &[("group", "group-subscription"), ("topic", "topic-subscription")],
        &["group"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<UpdateGroupForbiddenRequestHeader>(
        &[
            ("group", "group-forbidden"),
            ("topic", "topic-forbidden"),
            ("readable", "false"),
        ],
        &["group", "topic"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<QueryConsumerOffsetRequestHeader>(
        &[
            ("consumerGroup", "consumer-query"),
            ("topic", "topic-query"),
            ("queueId", "2147483647"),
            ("setZeroIfNotFound", "true"),
        ],
        &["consumerGroup", "topic", "queueId"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<UpdateConsumerOffsetRequestHeader>(
        &[
            ("consumerGroup", "consumer-update"),
            ("topic", "topic-update"),
            ("queueId", "-2147483648"),
            ("commitOffset", "9223372036854775807"),
        ],
        &["consumerGroup", "topic", "queueId", "commitOffset"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<PollingInfoRequestHeader>(
        &[
            ("consumerGroup", "consumer-polling"),
            ("topic", "topic-polling"),
            ("queueId", "0"),
        ],
        &["consumerGroup", "topic", "queueId"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<PeekMessageRequestHeader>(
        &[
            ("consumerGroup", "consumer-peek"),
            ("topic", "topic-peek"),
            ("queueId", "-1"),
            ("maxMsgNums", "2147483647"),
        ],
        &["consumerGroup", "topic", "queueId", "maxMsgNums"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<AckMessageRequestHeader>(
        &[
            ("consumerGroup", "consumer-ack"),
            ("topic", "topic-ack"),
            ("queueId", "2147483647"),
            ("extraInfo", "extra-ack"),
            ("offset", "-9223372036854775808"),
            ("liteTopic", "lite-ack"),
        ],
        &["consumerGroup", "topic", "queueId", "extraInfo", "offset"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<ChangeInvisibleTimeRequestHeader>(
        &[
            ("consumerGroup", "consumer-change"),
            ("topic", "topic-change"),
            ("queueId", "-2147483648"),
            ("extraInfo", "extra-change"),
            ("offset", "9223372036854775807"),
            ("invisibleTime", "-9223372036854775808"),
            ("liteTopic", "lite-change"),
            ("suspend", "true"),
        ],
        &[
            "consumerGroup",
            "topic",
            "queueId",
            "extraInfo",
            "offset",
            "invisibleTime",
        ],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<PopMessageRequestHeader>(
        &[
            ("consumerGroup", "consumer-pop"),
            ("topic", "topic-pop"),
            ("queueId", "2147483647"),
            ("maxMsgNums", "2147483647"),
            ("invisibleTime", "9223372036854775807"),
            ("pollTime", "9223372036854775807"),
            ("bornTime", "9223372036854775807"),
            ("initMode", "-2147483648"),
            ("expType", "TAG"),
            ("exp", "tag-a"),
            ("order", "true"),
            ("attemptId", "attempt-pop"),
        ],
        &[
            "consumerGroup",
            "topic",
            "queueId",
            "maxMsgNums",
            "invisibleTime",
            "pollTime",
            "bornTime",
            "initMode",
        ],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<GetRouteInfoRequestHeader>(
        &[("topic", "topic-route"), ("acceptStandardJsonOnly", "true")],
        &["topic"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<CreateTopicRequestHeader>(
        &[
            ("topic", "topic-create"),
            ("defaultTopic", "TBW102"),
            ("readQueueNums", "-2147483648"),
            ("writeQueueNums", "2147483647"),
            ("perm", "-2147483648"),
            ("topicFilterType", "MULTI_TAG"),
            ("topicSysFlag", "2147483647"),
            ("order", "true"),
            ("attributes", "+message.type=NORMAL"),
            ("force", "true"),
        ],
        &[
            "topic",
            "defaultTopic",
            "readQueueNums",
            "writeQueueNums",
            "perm",
            "topicFilterType",
            "order",
        ],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<QueryMessageRequestHeader>(
        &[
            ("topic", "topic-query-message"),
            ("key", "key-a"),
            ("maxNum", "2147483647"),
            ("beginTimestamp", "-9223372036854775808"),
            ("endTimestamp", "9223372036854775807"),
            ("indexType", "U"),
            ("lastKey", "last-a"),
        ],
        &["topic", "key", "maxNum", "beginTimestamp", "endTimestamp"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<RecallMessageRequestHeader>(
        &[
            ("producerGroup", "producer-recall"),
            ("topic", "topic-recall"),
            ("recallHandle", "handle-a"),
        ],
        &["topic", "recallHandle"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );
    assert_topic_envelope_contract::<ReplyMessageRequestHeader>(
        &[
            ("producerGroup", "producer-reply"),
            ("topic", "topic-reply"),
            ("defaultTopic", "TBW102"),
            ("defaultTopicQueueNums", "-2147483648"),
            ("queueId", "2147483647"),
            ("sysFlag", "-2147483648"),
            ("bornTimestamp", "-9223372036854775808"),
            ("flag", "2147483647"),
            ("properties", "KEYS=key-a"),
            ("reconsumeTimes", "-2147483648"),
            ("unitMode", "true"),
            ("bornHost", "127.0.0.1:1000"),
            ("storeHost", "127.0.0.1:2000"),
            ("storeTimestamp", "9223372036854775807"),
        ],
        &[
            "producerGroup",
            "topic",
            "defaultTopic",
            "defaultTopicQueueNums",
            "queueId",
            "sysFlag",
            "bornTimestamp",
            "flag",
            "bornHost",
            "storeHost",
            "storeTimestamp",
        ],
        |header| {
            header.topic_request.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc,
            })
        },
    );
    assert_topic_envelope_contract::<ResetOffsetRequestHeader>(
        &[
            ("topic", "topic-reset"),
            ("group", "group-reset"),
            ("queueId", "2147483647"),
            ("offset", "-9223372036854775808"),
            ("timestamp", "9223372036854775807"),
            ("isForce", "true"),
        ],
        &["topic", "group", "timestamp", "isForce"],
        |header| {
            header.topic_request_header.as_ref().map(|topic| TopicEnvelopeRef {
                lo: topic.lo,
                rpc: &topic.rpc_request_header,
            })
        },
    );

    let pop_required_only = HeaderMap::from([
        ("consumerGroup".into(), "consumer-pop".into()),
        ("topic".into(), "topic-pop".into()),
        ("queueId".into(), "0".into()),
        ("maxMsgNums".into(), "32".into()),
        ("invisibleTime".into(), "30000".into()),
        ("pollTime".into(), "15000".into()),
        ("bornTime".into(), "1720000000000".into()),
        ("initMode".into(), "0".into()),
    ]);
    let typed = <PopMessageRequestHeader as HeaderCodec>::decode_from_map(&pop_required_only)
        .expect("missing order uses the Java Boolean.FALSE initializer");
    let legacy = <PopMessageRequestHeader as FromMap>::from(&pop_required_only)
        .expect("legacy adapter uses the Java Boolean.FALSE initializer");
    assert_eq!(typed.order, Some(false));
    assert_eq!(legacy.order, Some(false));

    let mut malformed_order = pop_required_only.clone();
    malformed_order.insert("order".into(), "not-a-bool".into());
    assert!(matches!(
        <PopMessageRequestHeader as HeaderCodec>::decode_from_map(&malformed_order),
        Err(ProtocolContractViolation::InvalidValue { key: "order", .. })
    ));
    assert!(<PopMessageRequestHeader as FromMap>::from(&malformed_order).is_err());

    let mut malformed_suspend = HeaderMap::from([
        ("consumerGroup".into(), "consumer-change".into()),
        ("topic".into(), "topic-change".into()),
        ("queueId".into(), "0".into()),
        ("extraInfo".into(), "extra-change".into()),
        ("offset".into(), "0".into()),
        ("invisibleTime".into(), "1".into()),
    ]);
    let typed = <ChangeInvisibleTimeRequestHeader as HeaderCodec>::decode_from_map(&malformed_suspend)
        .expect("missing suspend uses the Java false initializer");
    let legacy = <ChangeInvisibleTimeRequestHeader as FromMap>::from(&malformed_suspend)
        .expect("legacy adapter uses the Java false initializer");
    assert!(!typed.suspend);
    assert!(!legacy.suspend);
    malformed_suspend.insert("suspend".into(), "not-a-bool".into());
    assert!(matches!(
        <ChangeInvisibleTimeRequestHeader as HeaderCodec>::decode_from_map(&malformed_suspend),
        Err(ProtocolContractViolation::InvalidValue { key: "suspend", .. })
    ));
    assert!(<ChangeInvisibleTimeRequestHeader as FromMap>::from(&malformed_suspend).is_err());

    let ack_signed_maximum = HeaderMap::from([
        ("consumerGroup".into(), "consumer-ack".into()),
        ("topic".into(), "topic-ack".into()),
        ("queueId".into(), "-2147483648".into()),
        ("extraInfo".into(), "extra-ack".into()),
        ("offset".into(), "9223372036854775807".into()),
    ]);
    let typed = <AckMessageRequestHeader as HeaderCodec>::decode_from_map(&ack_signed_maximum)
        .expect("signed Java extrema remain valid");
    let legacy = <AckMessageRequestHeader as FromMap>::from(&ack_signed_maximum)
        .expect("legacy adapter accepts signed Java extrema");
    assert_eq!(typed.queue_id, i32::MIN);
    assert_eq!(legacy.queue_id, i32::MIN);
    assert_eq!(typed.offset, i64::MAX);
    assert_eq!(legacy.offset, i64::MAX);

    let valid_pop = PopMessageRequestHeader {
        consumer_group: "consumer-pop".into(),
        topic: "topic-pop".into(),
        queue_id: 0,
        max_msg_nums: 32,
        invisible_time: 30_000,
        poll_time: 15_000,
        born_time: 1_720_000_000_000,
        init_mode: 0,
        exp_type: None,
        exp: None,
        order: Some(false),
        attempt_id: None,
        topic_request_header: None,
    };
    let mut overflow_cases = Vec::new();
    let mut max_msg_nums = valid_pop.clone();
    max_msg_nums.max_msg_nums = i32::MAX as u32 + 1;
    overflow_cases.push(("maxMsgNums", max_msg_nums));
    let mut invisible_time = valid_pop.clone();
    invisible_time.invisible_time = i64::MAX as u64 + 1;
    overflow_cases.push(("invisibleTime", invisible_time));
    let mut poll_time = valid_pop.clone();
    poll_time.poll_time = i64::MAX as u64 + 1;
    overflow_cases.push(("pollTime", poll_time));
    let mut born_time = valid_pop;
    born_time.born_time = i64::MAX as u64 + 1;
    overflow_cases.push(("bornTime", born_time));
    for (key, header) in overflow_cases {
        let mut map = HeaderMap::new();
        assert!(matches!(
            header.try_encode_into_map(&mut map),
            Err(ProtocolContractViolation::JavaRange { key: actual, .. }) if actual == key
        ));
    }

    let create_required_only = HeaderMap::from([
        ("topic".into(), "topic-create".into()),
        ("defaultTopic".into(), "TBW102".into()),
        ("readQueueNums".into(), "4".into()),
        ("writeQueueNums".into(), "4".into()),
        ("perm".into(), "6".into()),
        ("topicFilterType".into(), "SINGLE_TAG".into()),
        ("order".into(), "false".into()),
    ]);
    let typed = <CreateTopicRequestHeader as HeaderCodec>::decode_from_map(&create_required_only)
        .expect("missing force uses the Java Boolean.FALSE initializer");
    let legacy = <CreateTopicRequestHeader as FromMap>::from(&create_required_only)
        .expect("legacy adapter uses the Java Boolean.FALSE initializer");
    assert_eq!(typed.force, Some(false));
    assert_eq!(legacy.force, Some(false));

    let reply_required_only = HeaderMap::from([
        ("producerGroup".into(), "producer-reply".into()),
        ("topic".into(), "topic-reply".into()),
        ("defaultTopic".into(), "TBW102".into()),
        ("defaultTopicQueueNums".into(), "4".into()),
        ("queueId".into(), "0".into()),
        ("sysFlag".into(), "0".into()),
        ("bornTimestamp".into(), "1".into()),
        ("flag".into(), "0".into()),
        ("bornHost".into(), "127.0.0.1:1000".into()),
        ("storeHost".into(), "127.0.0.1:2000".into()),
        ("storeTimestamp".into(), "2".into()),
    ]);
    let typed = <ReplyMessageRequestHeader as HeaderCodec>::decode_from_map(&reply_required_only)
        .expect("missing unitMode uses the Java false initializer");
    let legacy = <ReplyMessageRequestHeader as FromMap>::from(&reply_required_only)
        .expect("legacy adapter uses the Java false initializer");
    assert_eq!(typed.unit_mode, Some(false));
    assert_eq!(legacy.unit_mode, Some(false));

    let reset_without_queue = HeaderMap::from([
        ("topic".into(), "topic-reset".into()),
        ("group".into(), "group-reset".into()),
        ("timestamp".into(), "0".into()),
        ("isForce".into(), "false".into()),
    ]);
    let typed = <ResetOffsetRequestHeader as HeaderCodec>::decode_from_map(&reset_without_queue)
        .expect("missing queueId uses the Java -1 initializer");
    let legacy = <ResetOffsetRequestHeader as FromMap>::from(&reset_without_queue)
        .expect("legacy adapter uses the Java -1 initializer");
    assert_eq!(typed.queue_id, -1);
    assert_eq!(legacy.queue_id, -1);

    let mut invalid_filter_type = create_required_only.clone();
    invalid_filter_type.insert("topicFilterType".into(), "SQL92".into());
    assert!(matches!(
        <CreateTopicRequestHeader as HeaderCodec>::decode_from_map(&invalid_filter_type),
        Err(ProtocolContractViolation::Validation {
            rule: "supported_topic_filter_type",
            ..
        })
    ));
    assert!(<CreateTopicRequestHeader as FromMap>::from(&invalid_filter_type).is_err());

    for (key, mut fields) in [
        ("force", create_required_only.clone()),
        ("order", create_required_only.clone()),
        ("unitMode", reply_required_only.clone()),
        ("isForce", reset_without_queue.clone()),
    ] {
        fields.insert(key.into(), "not-a-bool".into());
        let (typed_is_error, legacy_is_error) = match key {
            "force" | "order" => (
                <CreateTopicRequestHeader as HeaderCodec>::decode_from_map(&fields).is_err(),
                <CreateTopicRequestHeader as FromMap>::from(&fields).is_err(),
            ),
            "unitMode" => (
                <ReplyMessageRequestHeader as HeaderCodec>::decode_from_map(&fields).is_err(),
                <ReplyMessageRequestHeader as FromMap>::from(&fields).is_err(),
            ),
            "isForce" => (
                <ResetOffsetRequestHeader as HeaderCodec>::decode_from_map(&fields).is_err(),
                <ResetOffsetRequestHeader as FromMap>::from(&fields).is_err(),
            ),
            _ => unreachable!(),
        };
        assert!(typed_is_error, "typed decode must reject malformed {key}");
        assert!(legacy_is_error, "legacy decode must reject malformed {key}");
    }

    let malformed_accept = HeaderMap::from([
        ("topic".into(), "topic-route".into()),
        ("acceptStandardJsonOnly".into(), "not-a-bool".into()),
    ]);
    assert!(matches!(
        <GetRouteInfoRequestHeader as HeaderCodec>::decode_from_map(&malformed_accept),
        Err(ProtocolContractViolation::InvalidValue {
            key: "acceptStandardJsonOnly",
            ..
        })
    ));
    assert!(<GetRouteInfoRequestHeader as FromMap>::from(&malformed_accept).is_err());

    let update_signed_minimum = HeaderMap::from([
        ("consumerGroup".into(), "consumer-update".into()),
        ("topic".into(), "topic-update".into()),
        ("queueId".into(), "2147483647".into()),
        ("commitOffset".into(), "-9223372036854775808".into()),
    ]);
    let typed = <UpdateConsumerOffsetRequestHeader as HeaderCodec>::decode_from_map(&update_signed_minimum)
        .expect("signed Java Long and Integer minima remain valid");
    let legacy = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&update_signed_minimum)
        .expect("legacy adapter accepts signed Java extrema");
    assert_eq!(typed.queue_id, i32::MAX);
    assert_eq!(legacy.queue_id, i32::MAX);
    assert_eq!(typed.commit_offset, i64::MIN);
    assert_eq!(legacy.commit_offset, i64::MIN);

    let peek_signed_minimum = HeaderMap::from([
        ("consumerGroup".into(), "consumer-peek".into()),
        ("topic".into(), "topic-peek".into()),
        ("queueId".into(), "-2147483648".into()),
        ("maxMsgNums".into(), "-2147483648".into()),
    ]);
    let typed = <PeekMessageRequestHeader as HeaderCodec>::decode_from_map(&peek_signed_minimum)
        .expect("signed Java Integer minima remain valid");
    let legacy = <PeekMessageRequestHeader as FromMap>::from(&peek_signed_minimum)
        .expect("legacy adapter accepts signed Java Integer minima");
    assert_eq!(typed.queue_id, i32::MIN);
    assert_eq!(legacy.queue_id, i32::MIN);
    assert_eq!(typed.max_msg_nums, i32::MIN);
    assert_eq!(legacy.max_msg_nums, i32::MIN);

    let malformed_set_zero = HeaderMap::from([
        ("consumerGroup".into(), "consumer-query".into()),
        ("topic".into(), "topic-query".into()),
        ("queueId".into(), "0".into()),
        ("setZeroIfNotFound".into(), "not-a-bool".into()),
    ]);
    assert!(matches!(
        <QueryConsumerOffsetRequestHeader as HeaderCodec>::decode_from_map(&malformed_set_zero),
        Err(ProtocolContractViolation::InvalidValue {
            key: "setZeroIfNotFound",
            ..
        })
    ));
    assert!(<QueryConsumerOffsetRequestHeader as FromMap>::from(&malformed_set_zero).is_err());

    let consume_stats_minimum = HeaderMap::from([("consumerGroup".into(), "consumer-min".into())]);
    let typed = <GetConsumeStatsRequestHeader as HeaderCodec>::decode_from_map(&consume_stats_minimum)
        .expect("nullable Java topic decodes to the reviewed Rust empty-string default");
    let legacy = <GetConsumeStatsRequestHeader as FromMap>::from(&consume_stats_minimum)
        .expect("legacy adapter preserves the reviewed Rust empty-string default");
    assert!(typed.topic.is_empty());
    assert!(legacy.topic.is_empty());
    assert!(typed.topic_list.is_none());
    assert!(legacy.topic_list.is_none());

    let subscription_minimum = HeaderMap::from([("group".into(), "group-min".into())]);
    let typed = <QuerySubscriptionByConsumerRequestHeader as HeaderCodec>::decode_from_map(&subscription_minimum)
        .expect("nullable Java topic decodes to the reviewed Rust empty-string default");
    let legacy = <QuerySubscriptionByConsumerRequestHeader as FromMap>::from(&subscription_minimum)
        .expect("legacy adapter preserves the reviewed Rust empty-string default");
    assert!(typed.topic.is_empty());
    assert!(legacy.topic.is_empty());

    let malformed_readable = HeaderMap::from([
        ("group".into(), "group-forbidden".into()),
        ("topic".into(), "topic-forbidden".into()),
        ("readable".into(), "not-a-bool".into()),
    ]);
    assert!(matches!(
        <UpdateGroupForbiddenRequestHeader as HeaderCodec>::decode_from_map(&malformed_readable),
        Err(ProtocolContractViolation::InvalidValue { key: "readable", .. })
    ));
    assert!(<UpdateGroupForbiddenRequestHeader as FromMap>::from(&malformed_readable).is_err());

    let max_without_committed = HeaderMap::from([("topic".into(), "topic-max".into()), ("queueId".into(), "0".into())]);
    let typed = <GetMaxOffsetRequestHeader as HeaderCodec>::decode_from_map(&max_without_committed)
        .expect("missing committed uses the Java true default");
    let legacy = <GetMaxOffsetRequestHeader as FromMap>::from(&max_without_committed)
        .expect("legacy adapter uses the Java true default");
    assert!(typed.committed);
    assert!(legacy.committed);

    let mut malformed = max_without_committed;
    malformed.insert("committed".into(), "not-a-bool".into());
    assert!(matches!(
        <GetMaxOffsetRequestHeader as HeaderCodec>::decode_from_map(&malformed),
        Err(ProtocolContractViolation::InvalidValue { key: "committed", .. })
    ));
    assert!(<GetMaxOffsetRequestHeader as FromMap>::from(&malformed).is_err());
}

#[test]
fn rpc_envelope_headers_preserve_java_inheritance_and_legacy_aliases() {
    assert_rpc_envelope_contract::<CheckRocksdbCqWriteProgressRequestHeader>(
        &[("topic", "topic-a"), ("checkStoreTime", "9223372036854775807")],
        &["topic"],
        |header| &header.rpc,
    );
    assert_rpc_envelope_contract::<CheckTransactionStateRequestHeader>(
        &[("tranStateTableOffset", "-1"), ("commitLogOffset", "-2")],
        &["tranStateTableOffset", "commitLogOffset"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<CloneGroupOffsetRequestHeader>(
        &[("srcGroup", "src"), ("destGroup", "dest"), ("topic", "topic-a")],
        &["srcGroup", "destGroup"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<ConsumerSendMsgBackRequestHeader>(
        &[
            ("offset", "9223372036854775807"),
            ("group", "cg"),
            ("delayLevel", "2147483647"),
            ("originMsgId", "msg-a"),
            ("originTopic", "topic-a"),
            ("maxReconsumeTimes", "2147483647"),
        ],
        &["offset", "group", "delayLevel"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<CreateTopicListRequestHeader>(&[], &[], |header| &header.rpc_request_header);
    assert_rpc_envelope_contract::<DeleteSubscriptionGroupRequestHeader>(
        &[("groupName", "dg")],
        &["groupName"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<GetConsumerConnectionListRequestHeader>(
        &[("consumerGroup", "cg")],
        &["consumerGroup"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<GetConsumerListByGroupRequestHeader>(
        &[("consumerGroup", "cg")],
        &["consumerGroup"],
        |header| &header.rpc,
    );
    assert_rpc_envelope_contract::<GetConsumerRunningInfoRequestHeader>(
        &[("consumerGroup", "cg"), ("clientId", "ci")],
        &["consumerGroup", "clientId"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<GetLiteGroupInfoRequestHeader>(
        &[("group", "lg"), ("liteTopic", "lite-a"), ("topK", "2147483647")],
        &["group"],
        |header| &header.rpc,
    );
    assert_rpc_envelope_contract::<GetParentTopicInfoRequestHeader>(&[("topic", "parent-a")], &["topic"], |header| {
        &header.rpc
    });
    assert_rpc_envelope_contract::<GetProducerConnectionListRequestHeader>(
        &[("producerGroup", "pg")],
        &["producerGroup"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<GetSubscriptionGroupConfigRequestHeader>(&[("group", "sg")], &["group"], |header| {
        &header.rpc_request_header
    });
    assert_rpc_envelope_contract::<HeartbeatRequestHeader>(&[], &[], |header| &header.rpc_request);
    assert_rpc_envelope_contract::<LiteSubscriptionCtlRequestHeader>(&[], &[], |header| &header.rpc_request_header);
    assert_rpc_envelope_contract::<LockBatchMqRequestHeader>(&[], &[], |header| &header.rpc_request_header);
    assert_rpc_envelope_contract::<NotifyConsumerIdsChangedRequestHeader>(
        &[("consumerGroup", "ng")],
        &["consumerGroup"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<NotifyUnsubscribeLiteRequestHeader>(
        &[("liteTopic", "lt"), ("consumerGroup", "ng"), ("clientId", "ci")],
        &["liteTopic", "consumerGroup", "clientId"],
        |header| &header.rpc_request_header,
    );
    assert_rpc_envelope_contract::<PopLiteMessageRequestHeader>(
        &[
            ("clientId", "client-a"),
            ("consumerGroup", "cg"),
            ("topic", "topic-a"),
            ("maxMsgNum", "2147483647"),
            ("invisibleTime", "9223372036854775807"),
            ("pollTime", "9223372036854775807"),
            ("bornTime", "9223372036854775807"),
            ("attemptId", "attempt-a"),
        ],
        &[
            "clientId",
            "consumerGroup",
            "topic",
            "maxMsgNum",
            "invisibleTime",
            "pollTime",
            "bornTime",
        ],
        |header| &header.rpc,
    );
    assert_rpc_envelope_contract::<QueryTopicsByConsumerRequestHeader>(&[("group", "qg")], &["group"], |header| {
        &header.rpc_request_header
    });
    assert_rpc_envelope_contract::<UnlockBatchMqRequestHeader>(&[], &[], |header| &header.rpc_request_header);
    assert_rpc_envelope_contract::<UnregisterClientRequestHeader>(
        &[
            ("clientID", "canonical-client"),
            ("producerGroup", "pg"),
            ("consumerGroup", "cg"),
        ],
        &["clientID"],
        |header| &header.rpc_request_header,
    );

    let end_input = HeaderMap::from([
        ("producerGroup".into(), "pg".into()),
        ("tranStateTableOffset".into(), "-1".into()),
        ("commitLogOffset".into(), "-2".into()),
        (
            "commitOrRollback".into(),
            MessageSysFlag::TRANSACTION_COMMIT_TYPE.to_string().into(),
        ),
        ("msgId".into(), "msg-a".into()),
        ("ns".into(), "canonical-ns".into()),
        ("namespace".into(), "legacy-ns".into()),
        ("nsd".into(), "true".into()),
        ("namespaced".into(), "false".into()),
        ("bname".into(), "canonical-broker".into()),
        ("brokerName".into(), "legacy-broker".into()),
        ("oway".into(), "false".into()),
        ("oneway".into(), "true".into()),
    ]);
    let end_typed = <EndTransactionRequestHeader as HeaderCodec>::decode_from_map(&end_input)
        .expect("typed end transaction decode");
    let end_legacy =
        <EndTransactionRequestHeader as FromMap>::from(&end_input).expect("legacy end transaction adapter");
    for decoded in [&end_typed, &end_legacy] {
        assert_eq!(decoded.topic, "");
        assert_eq!(decoded.producer_group, "pg");
        assert_eq!(decoded.tran_state_table_offset, -1);
        assert_eq!(decoded.commit_log_offset, -2);
        assert!(!decoded.from_transaction_check);
        assert!(decoded.transaction_id.is_none());
        assert_eq!(decoded.rpc_request_header.namespace.as_deref(), Some("canonical-ns"));
        assert_eq!(decoded.rpc_request_header.namespaced, Some(true));
        assert_eq!(
            decoded.rpc_request_header.broker_name.as_deref(),
            Some("canonical-broker")
        );
        assert_eq!(decoded.rpc_request_header.oneway, Some(false));
    }
    let end_encoded = end_typed.to_map().expect("end transaction encode");
    assert_eq!(end_encoded.get("topic").map(CheetahString::as_str), Some(""));
    assert_eq!(
        end_encoded.get("fromTransactionCheck").map(CheetahString::as_str),
        Some("false")
    );
    for alias in ["namespace", "namespaced", "brokerName", "oneway"] {
        assert!(!end_encoded.contains_key(alias));
    }
    assert_eq!(end_typed.encode_capability(), HeaderEncodeCapability::MapOnly);

    for required in [
        "producerGroup",
        "tranStateTableOffset",
        "commitLogOffset",
        "commitOrRollback",
        "msgId",
    ] {
        let mut missing = end_input.clone();
        missing.remove(required);
        assert!(matches!(
            <EndTransactionRequestHeader as HeaderCodec>::decode_from_map(&missing),
            Err(ProtocolContractViolation::Missing { key, .. }) if key == required
        ));
        assert!(<EndTransactionRequestHeader as FromMap>::from(&missing).is_err());
    }

    let mut unsupported_state = end_input;
    unsupported_state.insert("commitOrRollback".into(), "999".into());
    assert!(matches!(
        <EndTransactionRequestHeader as HeaderCodec>::decode_from_map(&unsupported_state),
        Err(ProtocolContractViolation::Validation {
            rule: "supported_transaction_state",
            ..
        })
    ));
    assert!(<EndTransactionRequestHeader as FromMap>::from(&unsupported_state).is_err());

    let running = <GetConsumerRunningInfoRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([
        ("consumerGroup".into(), "cg".into()),
        ("clientId".into(), "ci".into()),
    ]))
    .expect("missing Java primitive boolean uses false");
    assert!(!running.jstack_enable);

    let deleted = <DeleteSubscriptionGroupRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([(
        "groupName".into(),
        "dg".into(),
    )]))
    .expect("missing Java primitive boolean uses false");
    assert!(!deleted.clean_offset);

    let cloned = <CloneGroupOffsetRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([
        ("srcGroup".into(), "src".into()),
        ("destGroup".into(), "dest".into()),
    ]))
    .expect("optional topic and primitive offline may be absent");
    assert!(cloned.topic.is_none());
    assert!(!cloned.offline);
    assert!(cloned.rpc_request_header.is_some());

    let checked = <CheckTransactionStateRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([
        ("tranStateTableOffset".into(), i64::MIN.to_string().into()),
        ("commitLogOffset".into(), i64::MAX.to_string().into()),
    ]))
    .expect("Java signed long extrema remain valid");
    assert_eq!(checked.tran_state_table_offset, i64::MIN);
    assert_eq!(checked.commit_log_offset, i64::MAX);
    assert!(checked.topic.is_none());
    assert!(checked.msg_id.is_none());
    assert!(checked.transaction_id.is_none());
    assert!(checked.offset_msg_id.is_none());
    assert!(checked.rpc_request_header.is_some());

    let rocksdb = <CheckRocksdbCqWriteProgressRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([(
        "topic".into(),
        "topic-a".into(),
    )]))
    .expect("missing Java primitive checkStoreTime uses zero");
    assert_eq!(rocksdb.check_store_time, 0);
    assert!(rocksdb.rpc.is_some());

    let lite_group = <GetLiteGroupInfoRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([(
        "group".into(),
        "lg".into(),
    )]))
    .expect("missing nullable liteTopic and primitive topK use reviewed defaults");
    assert_eq!(lite_group.lite_topic, "");
    assert_eq!(lite_group.top_k, 0);
    assert!(lite_group.rpc.is_some());

    let pop_lite = <PopLiteMessageRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([
        ("clientId".into(), "client-a".into()),
        ("consumerGroup".into(), "cg".into()),
        ("topic".into(), "topic-a".into()),
        ("maxMsgNum".into(), i32::MIN.to_string().into()),
        ("invisibleTime".into(), i64::MIN.to_string().into()),
        ("pollTime".into(), i64::MIN.to_string().into()),
        ("bornTime".into(), i64::MIN.to_string().into()),
    ]))
    .expect("Java signed minima and optional attemptId remain valid");
    assert_eq!(pop_lite.max_msg_num, i32::MIN);
    assert_eq!(pop_lite.invisible_time, i64::MIN);
    assert_eq!(pop_lite.poll_time, i64::MIN);
    assert_eq!(pop_lite.born_time, i64::MIN);
    assert!(pop_lite.attempt_id.is_none());
    assert!(pop_lite.rpc.is_some());

    let send_back = <ConsumerSendMsgBackRequestHeader as HeaderCodec>::decode_from_map(&HeaderMap::from([
        ("offset".into(), i64::MIN.to_string().into()),
        ("group".into(), "cg".into()),
        ("delayLevel".into(), i32::MIN.to_string().into()),
        ("maxReconsumeTimes".into(), i32::MIN.to_string().into()),
    ]))
    .expect("Java signed minima and missing unitMode remain valid");
    assert_eq!(send_back.offset, i64::MIN);
    assert_eq!(send_back.delay_level, i32::MIN);
    assert_eq!(send_back.max_reconsume_times, Some(i32::MIN));
    assert!(send_back.origin_msg_id.is_none());
    assert!(send_back.origin_topic.is_none());
    assert!(!send_back.unit_mode);
    assert!(send_back.rpc_request_header.is_some());

    let invalid_unit_mode = HeaderMap::from([
        ("offset".into(), "0".into()),
        ("group".into(), "cg".into()),
        ("delayLevel".into(), "0".into()),
        ("unitMode".into(), "invalid".into()),
    ]);
    assert!(<ConsumerSendMsgBackRequestHeader as HeaderCodec>::decode_from_map(&invalid_unit_mode).is_err());
    assert!(<ConsumerSendMsgBackRequestHeader as FromMap>::from(&invalid_unit_mode).is_err());

    let unregister_input = HeaderMap::from([
        ("clientID".into(), "canonical-client".into()),
        ("clientId".into(), "legacy-client".into()),
    ]);
    let unregister = <UnregisterClientRequestHeader as HeaderCodec>::decode_from_map(&unregister_input)
        .expect("reviewed alias conflict uses canonical input");
    let legacy_unregister = <UnregisterClientRequestHeader as FromMap>::from(&unregister_input)
        .expect("legacy adapter uses the same reviewed alias policy");
    assert_eq!(unregister.client_id, "canonical-client");
    assert_eq!(legacy_unregister.client_id, "canonical-client");
    assert!(unregister.producer_group.is_none());
    assert!(unregister.consumer_group.is_none());
    let encoded = unregister.to_map().expect("unregister header encodes");
    assert_eq!(
        encoded.get("clientID").map(CheetahString::as_str),
        Some("canonical-client")
    );
    assert!(!encoded.contains_key("clientId"));
}

#[test]
fn representative_headers_preserve_defaults_aliases_flattening_and_validation() {
    let empty = HeaderMap::new();
    let typed_default = <GetLiteClientInfoRequestHeader as HeaderCodec>::decode_from_map(&empty).unwrap();
    let legacy_default = <GetLiteClientInfoRequestHeader as FromMap>::from(&empty).unwrap();
    assert_eq!(typed_default.max_count, 1000);
    assert_eq!(legacy_default.max_count, 1000);

    for value in ["invalid", "0", "-1"] {
        let map = HeaderMap::from([("maxCount".into(), value.into())]);
        assert!(<GetLiteClientInfoRequestHeader as HeaderCodec>::decode_from_map(&map).is_err());
        assert!(<GetLiteClientInfoRequestHeader as FromMap>::from(&map).is_err());
    }

    let rpc_map = HeaderMap::from([
        ("ns".into(), "canonical".into()),
        ("namespace".into(), "legacy".into()),
        ("nsd".into(), "true".into()),
        ("bname".into(), "broker-a".into()),
        ("oway".into(), "false".into()),
    ]);
    let rpc = <RpcRequestHeader as HeaderCodec>::decode_from_map(&rpc_map).unwrap();
    assert_eq!(rpc.namespace.as_deref(), Some("canonical"));
    assert_eq!(rpc.namespaced, Some(true));

    let header = SearchOffsetRequestHeader {
        topic: CheetahString::from_static_str("topic-a"),
        lite_topic: Some(CheetahString::from_static_str("lite-a")),
        queue_id: 3,
        timestamp: 42,
        boundary_type: BoundaryType::Upper,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(rpc),
            lo: Some(true),
        }),
    };
    let map = header.to_map().expect("typed compatibility map");
    for key in [
        "topic",
        "liteTopic",
        "queueId",
        "timestamp",
        "boundaryType",
        "lo",
        "ns",
        "nsd",
        "bname",
        "oway",
    ] {
        assert!(map.contains_key(key), "missing flattened key {key}");
    }
    for legacy in ["namespace", "namespaced", "brokerName", "oneway"] {
        assert!(!map.contains_key(legacy));
    }
    let decoded = <SearchOffsetRequestHeader as HeaderCodec>::decode_from_map(&map).unwrap();
    assert_eq!(decoded.topic, "topic-a");
    assert_eq!(decoded.boundary_type, BoundaryType::Upper);
    assert_eq!(decoded.topic_request_header.unwrap().get_lo(), Some(&true));

    let mut lower_map = map;
    lower_map.remove("boundaryType");
    let lower = <SearchOffsetRequestHeader as HeaderCodec>::decode_from_map(&lower_map).unwrap();
    assert_eq!(lower.boundary_type, BoundaryType::Lower);

    let response = SearchOffsetResponseHeader { offset: 99 };
    let response_map = response.to_map().unwrap();
    assert_eq!(response_map.get("offset").map(CheetahString::as_str), Some("99"));
    assert_eq!(
        <SearchOffsetResponseHeader as HeaderCodec>::decode_from_map(&response_map)
            .unwrap()
            .offset,
        99
    );
}

#[test]
fn typed_validation_errors_remain_classified_and_redacted() {
    let invalid = GetLiteClientInfoRequestHeader {
        max_count: 0,
        ..Default::default()
    };
    let mut map = HeaderMap::new();
    assert!(matches!(
        invalid.try_encode_into_map(&mut map),
        Err(ProtocolContractViolation::Validation {
            rule: "max_count_positive",
            ..
        })
    ));
    assert!(map.is_empty());
}

#[test]
fn frame_encoding_validates_each_typed_header_once() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        VALIDATION_CALLS.store(0, Ordering::Relaxed);
        let header = SingleValidationHeader { value: "value".into() };
        let mut command = RemotingCommand::create_request_command(100, header).set_serialize_type(serialize_type);
        let mut encoded = BytesMut::new();

        command.try_fast_header_encode(&mut encoded).unwrap();

        assert!(!encoded.is_empty());
        assert_eq!(
            VALIDATION_CALLS.load(Ordering::Relaxed),
            1,
            "{serialize_type:?} must validate through its authoritative encoder exactly once"
        );
    }
}

fn decode_fast_fields(encoded: &[u8]) -> HeaderMap {
    decode_fast_pairs(encoded).into_iter().collect()
}

fn decode_fast_pairs(encoded: &[u8]) -> Vec<(CheetahString, CheetahString)> {
    let mut fields = Vec::new();
    let mut cursor = 0;
    while cursor < encoded.len() {
        let key_len = u16::from_be_bytes(encoded[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;
        let key = std::str::from_utf8(&encoded[cursor..cursor + key_len]).unwrap();
        cursor += key_len;
        let value_len = u32::from_be_bytes(encoded[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let value = std::str::from_utf8(&encoded[cursor..cursor + value_len]).unwrap();
        cursor += value_len;
        fields.push((key.into(), value.into()));
    }
    fields
}

fn assert_direct_binary_matches_typed_map<T>(header: &T, expected_keys: &[&str])
where
    T: CommandCustomHeader,
{
    assert_eq!(header.encode_capability(), HeaderEncodeCapability::DirectBinary);
    let expected = header.to_map().expect("typed compatibility map");
    let mut encoded = BytesMut::from(&b"prefix"[..]);
    header
        .encode_direct_binary(&mut encoded)
        .expect("typed direct binary encoding");
    assert_eq!(&encoded[..6], b"prefix");
    assert_eq!(encoded.len() - 6, header.encoded_len_hint());

    let pairs = decode_fast_pairs(&encoded[6..]);
    assert_eq!(
        pairs.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(),
        expected_keys
    );
    assert_eq!(pairs.into_iter().collect::<HeaderMap>(), expected);
}

#[test]
fn generated_fast_headers_write_canonical_binary_pairs_in_schema_order() {
    let rpc = RpcRequestHeader {
        namespace: Some("namespace-a".into()),
        namespaced: Some(true),
        broker_name: Some("broker-a".into()),
        oneway: Some(false),
    };
    let pull_request = PullMessageRequestHeader {
        consumer_group: "group-a".into(),
        topic: "topic-a".into(),
        lite_topic: Some("lite-a".into()),
        queue_id: -1,
        queue_offset: -42,
        max_msg_nums: 32,
        sys_flag: 1,
        commit_offset: 7,
        suspend_timeout_millis: 15_000,
        sub_version: 99,
        subscription: Some("tag-a".into()),
        expression_type: Some("TAG".into()),
        max_msg_bytes: Some(1024),
        request_source: Some(2),
        proxy_forward_client_id: Some("client-a".into()),
        topic_request: Some(NamesrvTopicRequestHeader {
            lo: Some(true),
            rpc: Some(rpc),
        }),
    };
    assert_direct_binary_matches_typed_map(
        &pull_request,
        &[
            "consumerGroup",
            "topic",
            "liteTopic",
            "queueId",
            "queueOffset",
            "maxMsgNums",
            "sysFlag",
            "commitOffset",
            "suspendTimeoutMillis",
            "subscription",
            "subVersion",
            "expressionType",
            "maxMsgBytes",
            "requestSource",
            "proxyFrowardClientId",
            "lo",
            "ns",
            "nsd",
            "bname",
            "oway",
        ],
    );

    let pull_response = PullMessageResponseHeader {
        suggest_which_broker_id: 1,
        next_begin_offset: -2,
        min_offset: -3,
        max_offset: 4,
        offset_delta: Some(-5),
        topic_sys_flag: Some(6),
        group_sys_flag: Some(7),
        forbidden_type: Some(8),
    };
    assert_direct_binary_matches_typed_map(
        &pull_response,
        &[
            "suggestWhichBrokerId",
            "nextBeginOffset",
            "minOffset",
            "maxOffset",
            "offsetDelta",
            "topicSysFlag",
            "groupSysFlag",
            "forbiddenType",
        ],
    );

    let send_response = SendMessageResponseHeader::new(
        "message-a".into(),
        -1,
        -42,
        Some("transaction-a".into()),
        Some("batch-a".into()),
        Some("recall-a".into()),
    );
    assert_direct_binary_matches_typed_map(
        &send_response,
        &[
            "msgId",
            "queueId",
            "queueOffset",
            "transactionId",
            "batchUniqId",
            "recallHandle",
        ],
    );

    let notification = NotificationRequestHeader {
        consumer_group: "group-a".into(),
        topic: "topic-a".into(),
        queue_id: 3,
        poll_time: 15_000,
        born_time: 1_720_000_000_000,
        order: false,
        attempt_id: Some("attempt-a".into()),
        exp_type: Some("TAG".into()),
        exp: Some("*".into()),
        is_lite_consumer: false,
        client_id: Some("client-a".into()),
        topic_request_header: Some(NamesrvTopicRequestHeader {
            lo: Some(false),
            rpc: Some(RpcRequestHeader {
                namespace: Some("tenant-a".into()),
                namespaced: Some(true),
                broker_name: Some("broker-a".into()),
                oneway: Some(false),
            }),
        }),
    };
    assert_direct_binary_matches_typed_map(
        &notification,
        &[
            "consumerGroup",
            "topic",
            "queueId",
            "pollTime",
            "bornTime",
            "order",
            "attemptId",
            "expType",
            "exp",
            "isLiteConsumer",
            "clientId",
            "lo",
            "ns",
            "nsd",
            "bname",
            "oway",
        ],
    );
}

#[test]
#[allow(deprecated, reason = "verifies the source-compatible legacy adapter delegates to V3")]
fn typed_schemas_preserve_java_send_fast_contracts() {
    let rpc = RpcRequestHeader {
        namespace: Some("namespace-a".into()),
        namespaced: Some(true),
        broker_name: Some("broker-a".into()),
        oneway: Some(false),
    };
    let request = SendMessageRequestHeaderV2 {
        a: "producer-a".into(),
        b: "topic-a".into(),
        c: "TBW102".into(),
        d: 4,
        e: 2,
        f: 0,
        g: 42,
        h: 1,
        i: Some("properties".into()),
        j: Some(3),
        k: Some(true),
        l: Some(5),
        m: Some(false),
        n: Some("broker-a".into()),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(rpc),
            lo: Some(true),
        }),
    };
    let typed_request_map = request.to_map().unwrap();
    let mut request_bytes = BytesMut::new();
    CommandCustomHeader::encode_direct_binary(&request, &mut request_bytes).unwrap();
    let direct_request_map = decode_fast_fields(&request_bytes);
    const JAVA_FAST_KEYS: [&str; 14] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"];
    let expected_direct_request_map = typed_request_map
        .iter()
        .filter(|(key, _)| JAVA_FAST_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HeaderMap>();
    assert_eq!(direct_request_map, expected_direct_request_map);
    for inherited in ["lo", "ns", "nsd", "bname", "oway"] {
        assert!(typed_request_map.contains_key(inherited));
        assert!(!direct_request_map.contains_key(inherited));
    }

    let typed_request = <SendMessageRequestHeaderV2 as HeaderCodec>::decode_from_map(&typed_request_map).unwrap();
    let legacy_request = <SendMessageRequestHeaderV2 as FromMap>::from(&typed_request_map).unwrap();
    let mut fast_request = SendMessageRequestHeaderV2::default();
    CommandCustomHeader::decode_fast(&mut fast_request, &typed_request_map).unwrap();
    assert_eq!(typed_request.to_map(), Some(typed_request_map.clone()));
    assert_eq!(legacy_request.to_map(), Some(typed_request_map.clone()));
    assert_eq!(fast_request.to_map(), Some(typed_request_map));

    let mut response = SendMessageResponseHeader::new(
        "message-a".into(),
        -1,
        -42,
        Some("transaction-a".into()),
        Some("batch-a".into()),
        Some("recall-a".into()),
    );
    let typed_response_map = response.to_map().unwrap();
    let mut legacy_response_bytes = BytesMut::new();
    FastCodesHeader::encode_fast(&mut response, &mut legacy_response_bytes);
    let mut typed_response_bytes = BytesMut::new();
    CommandCustomHeader::encode_direct_binary(&response, &mut typed_response_bytes).unwrap();
    assert_eq!(typed_response_bytes, legacy_response_bytes);
    assert_eq!(decode_fast_fields(&typed_response_bytes), typed_response_map);

    let mut fast_response = SendMessageResponseHeader::default();
    FastCodesHeader::decode_fast(&mut fast_response, &typed_response_map);
    assert_eq!(fast_response.to_map(), Some(typed_response_map));
}

fn assert_send_numeric_overflow_is_rejected<T>(base: &HeaderMap, cases: &[(&'static str, &'static str)])
where
    T: HeaderCodec + FromMap + std::fmt::Debug,
{
    for &(key, value) in cases {
        let mut overflow = base.clone();
        overflow.insert(key.into(), value.into());

        let error = <T as HeaderCodec>::decode_from_map(&overflow).unwrap_err();
        assert!(
            matches!(error, ProtocolContractViolation::InvalidValue { key: actual, .. } if actual == key),
            "{key} must reject a value above its Java/Rust signed limit: {error}"
        );
        assert!(
            <T as FromMap>::from(&overflow).is_err(),
            "the legacy adapter must reject the same overflow for {key}"
        );
    }
}

#[test]
fn send_headers_accept_signed_maxima_and_reject_limit_plus_one() {
    let v1 = SendMessageRequestHeader {
        producer_group: "producer-a".into(),
        topic: "topic-a".into(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: i32::MAX,
        queue_id: i32::MAX,
        sys_flag: i32::MAX,
        born_timestamp: i64::MAX,
        flag: i32::MAX,
        properties: None,
        reconsume_times: Some(i32::MAX),
        unit_mode: None,
        batch: None,
        max_reconsume_times: Some(i32::MAX),
        topic_request_header: None,
    };
    let v1_map = v1.to_map().unwrap();
    let decoded_v1 = <SendMessageRequestHeader as HeaderCodec>::decode_from_map(&v1_map).unwrap();
    assert_eq!(decoded_v1.to_map(), Some(v1_map.clone()));
    let mut v1_binary = BytesMut::new();
    CommandCustomHeader::encode_direct_binary(&v1, &mut v1_binary).unwrap();
    assert!(!v1_binary.is_empty());
    assert_send_numeric_overflow_is_rejected::<SendMessageRequestHeader>(
        &v1_map,
        &[
            ("defaultTopicQueueNums", "2147483648"),
            ("queueId", "2147483648"),
            ("sysFlag", "2147483648"),
            ("bornTimestamp", "9223372036854775808"),
            ("flag", "2147483648"),
            ("reconsumeTimes", "2147483648"),
            ("maxReconsumeTimes", "2147483648"),
        ],
    );

    let v2 = SendMessageRequestHeaderV2 {
        a: "producer-a".into(),
        b: "topic-a".into(),
        c: "TBW102".into(),
        d: i32::MAX,
        e: i32::MAX,
        f: i32::MAX,
        g: i64::MAX,
        h: i32::MAX,
        i: None,
        j: Some(i32::MAX),
        k: None,
        l: Some(i32::MAX),
        m: None,
        n: None,
        topic_request_header: None,
    };
    let v2_map = v2.to_map().unwrap();
    let decoded_v2 = <SendMessageRequestHeaderV2 as HeaderCodec>::decode_from_map(&v2_map).unwrap();
    assert_eq!(decoded_v2.to_map(), Some(v2_map.clone()));
    let mut v2_binary = BytesMut::new();
    CommandCustomHeader::encode_direct_binary(&v2, &mut v2_binary).unwrap();
    assert!(!v2_binary.is_empty());
    assert_send_numeric_overflow_is_rejected::<SendMessageRequestHeaderV2>(
        &v2_map,
        &[
            ("d", "2147483648"),
            ("e", "2147483648"),
            ("f", "2147483648"),
            ("g", "9223372036854775808"),
            ("h", "2147483648"),
            ("j", "2147483648"),
            ("l", "2147483648"),
        ],
    );
}

#[test]
fn unsigned_fast_header_fields_enforce_inferred_java_ranges() {
    let request = PullMessageRequestHeader {
        consumer_group: "group-a".into(),
        topic: "topic-a".into(),
        queue_id: 0,
        queue_offset: 0,
        max_msg_nums: 32,
        sys_flag: 0,
        commit_offset: 0,
        suspend_timeout_millis: i64::MAX as u64 + 1,
        sub_version: 1,
        ..Default::default()
    };
    let mut map = HeaderMap::new();
    assert!(matches!(
        request.try_encode_into_map(&mut map),
        Err(ProtocolContractViolation::JavaRange {
            key: "suspendTimeoutMillis",
            ..
        })
    ));
    let mut bytes = BytesMut::from(&b"prefix"[..]);
    assert!(matches!(
        request.encode_direct_binary(&mut bytes),
        Err(ProtocolContractViolation::JavaRange {
            key: "suspendTimeoutMillis",
            ..
        })
    ));
    assert_eq!(bytes.as_ref(), b"prefix");

    let response = PullMessageResponseHeader {
        suggest_which_broker_id: i64::MAX as u64 + 1,
        ..Default::default()
    };
    let mut map = HeaderMap::new();
    assert!(matches!(
        response.try_encode_into_map(&mut map),
        Err(ProtocolContractViolation::JavaRange {
            key: "suggestWhichBrokerId",
            ..
        })
    ));

    let mut bytes = BytesMut::from(&b"prefix"[..]);
    assert!(matches!(
        response.encode_direct_binary(&mut bytes),
        Err(ProtocolContractViolation::JavaRange {
            key: "suggestWhichBrokerId",
            ..
        })
    ));
    assert_eq!(bytes.as_ref(), b"prefix");
}
