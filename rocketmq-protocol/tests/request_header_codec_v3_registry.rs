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
use rocketmq_protocol::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
use rocketmq_protocol::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_protocol::protocol::header::clone_group_offset_request_header::CloneGroupOffsetRequestHeader;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_list_request_header::CreateTopicListRequestHeader;
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
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_protocol::protocol::header::query_correction_offset_header::QueryCorrectionOffsetHeader;
use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_protocol::protocol::header::update_group_forbidden_request_header::UpdateGroupForbiddenRequestHeader;
use rocketmq_protocol::protocol::header_codec::{
    AliasConflictPolicy, HeaderCodec, HeaderCodecError, HeaderFieldSpec, HeaderFlattenSpec, HeaderPresence,
    HeaderRange, HeaderValueKind,
};
#[allow(deprecated, reason = "verifies the source-compatible legacy adapter delegates to V3")]
use rocketmq_protocol::protocol::FastCodesHeader;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_protocol::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderEncodeCapability, HeaderMap, RemotingCommand};
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionAllowlist {
    extensions: Vec<ExtensionOverride>,
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
struct AliasOverride {
    rust_type: String,
    canonical: String,
    aliases: Vec<String>,
    policy: String,
}

struct RegisteredSchema {
    type_id: &'static str,
    java_class: &'static str,
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
    fn count_validation(&self) -> Result<(), HeaderCodecError> {
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
        java_class: T::JAVA_CLASS.expect("registered production headers declare a Java peer"),
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
        register_value(&QuerySubscriptionByConsumerRequestHeader {
            group: CheetahString::from_static_str("registry"),
            topic: CheetahString::new(),
            topic_request_header: None,
        }),
        register_value(&QueryTopicConsumeByWhoRequestHeader {
            topic: CheetahString::from_static_str("registry"),
            topic_request_header: None,
        }),
        register::<SearchOffsetRequestHeader>(),
        register::<SearchOffsetResponseHeader>(),
        register::<PullMessageRequestHeader>(),
        register::<PullMessageResponseHeader>(),
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
        register::<UnlockBatchMqRequestHeader>(),
        register::<UnregisterClientRequestHeader>(),
        register_value(&UpdateGroupForbiddenRequestHeader {
            group: CheetahString::from_static_str("registry"),
            topic: CheetahString::from_static_str("registry"),
            readable: None,
            topic_request_header: None,
        }),
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

    for schema in &registered {
        assert!(
            schema.local_fields.iter().all(|field| field.java_type.is_none()),
            "{} must infer Java-compatible value kinds instead of repeating java_type metadata",
            schema.type_id
        );
        let java_header = java
            .headers
            .iter()
            .find(|header| header.rust_type_id == schema.type_id)
            .unwrap_or_else(|| panic!("missing pinned Java schema for {}", schema.type_id));
        assert_eq!(schema.java_class, java_header.java_class);
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
                assert_ne!(
                    owner.type_id, schema.type_id,
                    "extension field {}.{} must come from a registered flattened owner",
                    schema.type_id, field.key
                );
                continue;
            };
            assert_eq!(rust_kind(field.kind), java_kind(&java_field.java_type));
            match field.kind {
                HeaderValueKind::U32 => assert_eq!(field.java_range, Some(HeaderRange::I32)),
                HeaderValueKind::U64 => assert_eq!(field.java_range, Some(HeaderRange::I64)),
                _ => assert_eq!(field.java_range, None),
            }

            assert_eq!(owner.java_class, java_field.declared_in);

            match field.presence {
                HeaderPresence::Required => assert_eq!(java_field.presence, "required"),
                HeaderPresence::Optional => assert_eq!(java_field.presence, "optional"),
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
            matches!(typed_missing, Err(HeaderCodecError::Missing { key: actual, .. }) if actual == key),
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
            Err(HeaderCodecError::Missing { key: actual, .. }) if actual == key
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
        Err(HeaderCodecError::InvalidValue { key: "readable", .. })
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
        Err(HeaderCodecError::InvalidValue { key: "committed", .. })
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
            Err(HeaderCodecError::Missing { key, .. }) if key == required
        ));
        assert!(<EndTransactionRequestHeader as FromMap>::from(&missing).is_err());
    }

    let mut unsupported_state = end_input;
    unsupported_state.insert("commitOrRollback".into(), "999".into());
    assert!(matches!(
        <EndTransactionRequestHeader as HeaderCodec>::decode_from_map(&unsupported_state),
        Err(HeaderCodecError::Validation {
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
        Err(HeaderCodecError::Validation {
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
            matches!(error, HeaderCodecError::InvalidValue { key: actual, .. } if actual == key),
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
        Err(HeaderCodecError::JavaRange {
            key: "suspendTimeoutMillis",
            ..
        })
    ));
    let mut bytes = BytesMut::from(&b"prefix"[..]);
    assert!(matches!(
        request.encode_direct_binary(&mut bytes),
        Err(HeaderCodecError::JavaRange {
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
        Err(HeaderCodecError::JavaRange {
            key: "suggestWhichBrokerId",
            ..
        })
    ));

    let mut bytes = BytesMut::from(&b"prefix"[..]);
    assert!(matches!(
        response.encode_direct_binary(&mut bytes),
        Err(HeaderCodecError::JavaRange {
            key: "suggestWhichBrokerId",
            ..
        })
    ));
    assert_eq!(bytes.as_ref(), b"prefix");
}
