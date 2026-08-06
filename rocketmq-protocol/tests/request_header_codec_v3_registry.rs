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

use cheetah_string::CheetahString;
use rocketmq_model::boundary_type::BoundaryType;
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::header_codec::{
    AliasConflictPolicy, HeaderCodec, HeaderCodecError, HeaderFieldSpec, HeaderFlattenSpec, HeaderPresence,
    HeaderValueKind,
};
use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_protocol::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderMap};
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

fn register<T: HeaderCodec>() -> RegisteredSchema {
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
        register::<GetLiteClientInfoRequestHeader>(),
        register::<SearchOffsetRequestHeader>(),
        register::<SearchOffsetResponseHeader>(),
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
