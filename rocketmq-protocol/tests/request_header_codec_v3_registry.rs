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
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as NamesrvTopicRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
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
    fast: bool,
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

fn register<T: HeaderCodec + CommandCustomHeader + Default>() -> RegisteredSchema {
    assert_eq!(
        T::default().encode_capability() == HeaderEncodeCapability::DirectBinary,
        T::FAST_ENABLED,
        "{} encode capability must follow its fast schema flag",
        T::TYPE_ID
    );
    let mut fields = Vec::new();
    T::visit_field_specs(&mut |field| fields.push(*field));
    let mut flattens = Vec::new();
    T::visit_flatten_specs(&mut |flatten| flattens.push(*flatten));
    RegisteredSchema {
        type_id: T::TYPE_ID,
        java_class: T::JAVA_CLASS.expect("registered production headers declare a Java peer"),
        fast: T::FAST_ENABLED,
        local_fields: T::LOCAL_FIELD_SPECS,
        fields,
        flattens,
    }
}

fn registry() -> Vec<RegisteredSchema> {
    vec![
        register::<RpcRequestHeader>(),
        register::<TopicRequestHeader>(),
        register::<NamesrvTopicRequestHeader>(),
        register::<GetLiteClientInfoRequestHeader>(),
        register::<SearchOffsetRequestHeader>(),
        register::<SearchOffsetResponseHeader>(),
        register::<PullMessageRequestHeader>(),
        register::<PullMessageResponseHeader>(),
        register::<SendMessageRequestHeaderV2>(),
        register::<SendMessageResponseHeader>(),
    ]
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
        assert_eq!(schema.fast, java_header.java_fast);
        assert_eq!(
            schema.fields.len(),
            java_header.fields.len(),
            "{} field count",
            schema.type_id
        );

        for field in &schema.fields {
            let java_field = java_header
                .fields
                .iter()
                .find(|candidate| candidate.key == field.key)
                .unwrap_or_else(|| panic!("missing Java field {}.{}", schema.type_id, field.key));
            assert_eq!(rust_kind(field.kind), java_kind(&java_field.java_type));
            match field.kind {
                HeaderValueKind::U32 => assert_eq!(field.java_range, Some(HeaderRange::I32)),
                HeaderValueKind::U64 => assert_eq!(field.java_range, Some(HeaderRange::I64)),
                _ => assert_eq!(field.java_range, None),
            }

            let owner = by_type_id
                .get(field.declared_in)
                .unwrap_or_else(|| panic!("unregistered field owner {}", field.declared_in));
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
