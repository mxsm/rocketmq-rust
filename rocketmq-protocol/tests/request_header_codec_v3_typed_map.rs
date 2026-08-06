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

use cheetah_string::CheetahString;
use rocketmq_macros::{RequestHeaderCodecV2, RequestHeaderCodecV3};
use rocketmq_protocol::protocol::header_codec::{
    AliasConflictPolicy, FlattenPresenceSpec, HeaderCodec, HeaderCodecError, HeaderPresence,
};
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderMap};

#[derive(Debug, PartialEq, RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::RoutingHeaderV3",
    java_class = "org.apache.rocketmq.fixtures.RoutingHeaderV3",
    crate = "rocketmq_protocol",
    lookup = "get"
)]
struct RoutingHeaderV3 {
    #[header(required, binary_order = 0)]
    topic: String,
    #[header(key = "routingQueueId", default, default_semantic = "literal:0", binary_order = 1)]
    queue_id: i32,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, PartialEq, RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::TypedMapHeaderV3",
    java_class = "org.apache.rocketmq.fixtures.TypedMapHeaderV3",
    crate = "rocketmq_protocol",
    validate = "Self::validate_header"
)]
struct TypedMapHeaderV3 {
    #[header(
        key = "requestId",
        alias = "legacyRequestId",
        alias_conflict = "prefer_canonical",
        required,
        binary_order = 0
    )]
    request_id: CheetahString,
    #[header(key = "traceId", alias = "legacyTraceId", alias_conflict = "error", binary_order = 1)]
    trace_id: Option<String>,
    #[header(default, default_semantic = "literal:0", binary_order = 2)]
    attempts: i32,
    #[header(required, java_type = "int", range = "i32", binary_order = 3)]
    queue_id: u32,
    #[header(
        default_with = "default_enabled",
        default_semantic = "literal:true",
        binary_order = 4
    )]
    enabled: bool,
    #[header(flatten, presence = "any", binary_order = 5)]
    routing: Option<RoutingHeaderV3>,
}

impl TypedMapHeaderV3 {
    fn validate_header(&self) -> Result<(), HeaderCodecError> {
        if self.attempts < 0 {
            Err(HeaderCodecError::Validation {
                header: <Self as HeaderCodec>::TYPE_ID,
                rule: "attempts_non_negative",
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, RequestHeaderCodecV3)]
#[header(type_id = "fixtures::DenseScanHeaderV3", crate = "rocketmq_protocol")]
struct DenseScanHeaderV3 {
    #[header(required)]
    first: i32,
    #[header(required)]
    second: i32,
    #[header(required)]
    third: i32,
    #[header(required)]
    fourth: i32,
}

#[derive(RequestHeaderCodecV2)]
struct HardenedV2Header {
    #[required]
    value: String,
}

fn sample_header() -> TypedMapHeaderV3 {
    TypedMapHeaderV3 {
        request_id: CheetahString::from_static_str("request-7"),
        trace_id: Some("trace-9".to_owned()),
        attempts: 2,
        queue_id: 3,
        enabled: false,
        routing: Some(RoutingHeaderV3 {
            topic: "topic-a".to_owned(),
            queue_id: 4,
        }),
    }
}

fn exact_ext_payload_len(map: &HeaderMap) -> usize {
    map.iter().map(|(key, value)| 2 + key.len() + 4 + value.len()).sum()
}

#[test]
fn typed_map_round_trip_uses_one_shared_destination_and_exact_hint() {
    let header = sample_header();
    let mut map = HeaderMap::new();
    map.insert(
        CheetahString::from_static_str("unrelated"),
        CheetahString::from_static_str("preserved"),
    );

    header.try_encode_into_map(&mut map).expect("typed map encode");
    assert_eq!(map.get("requestId").map(CheetahString::as_str), Some("request-7"));
    assert_eq!(map.get("topic").map(CheetahString::as_str), Some("topic-a"));
    assert_eq!(map.get("unrelated").map(CheetahString::as_str), Some("preserved"));

    let typed_only = header.to_map().expect("compatibility map");
    assert_eq!(
        HeaderCodec::encoded_len_hint(&header),
        exact_ext_payload_len(&typed_only)
    );
    assert_eq!(
        <TypedMapHeaderV3 as HeaderCodec>::decode_from_map(&map).unwrap(),
        header
    );
    assert_eq!(<TypedMapHeaderV3 as FromMap>::from(&map).unwrap(), header);
}

#[test]
fn alias_policy_is_deterministic_for_get_and_scan_plans() {
    let mut map = sample_header().to_map().unwrap();
    map.insert(
        CheetahString::from_static_str("legacyRequestId"),
        CheetahString::from_static_str("ignored-legacy"),
    );
    let decoded = <TypedMapHeaderV3 as HeaderCodec>::decode_from_map(&map).unwrap();
    assert_eq!(decoded.request_id.as_str(), "request-7");

    map.insert(
        CheetahString::from_static_str("legacyTraceId"),
        CheetahString::from_static_str("different"),
    );
    assert!(matches!(
        <TypedMapHeaderV3 as HeaderCodec>::decode_from_map(&map),
        Err(HeaderCodecError::Conflict {
            header: "fixtures::TypedMapHeaderV3",
            key: "traceId"
        })
    ));

    let dense = HeaderMap::from([
        (
            CheetahString::from_static_str("fourth"),
            CheetahString::from_static_str("4"),
        ),
        (
            CheetahString::from_static_str("second"),
            CheetahString::from_static_str("2"),
        ),
        (
            CheetahString::from_static_str("first"),
            CheetahString::from_static_str("1"),
        ),
        (
            CheetahString::from_static_str("third"),
            CheetahString::from_static_str("3"),
        ),
    ]);
    assert_eq!(
        <DenseScanHeaderV3 as HeaderCodec>::decode_from_map(&dense).unwrap(),
        DenseScanHeaderV3 {
            first: 1,
            second: 2,
            third: 3,
            fourth: 4,
        }
    );
}

#[test]
fn flatten_presence_schema_and_resolver_are_recursive() {
    let mut map = sample_header().to_map().unwrap();
    map.remove("topic");
    map.remove("routingQueueId");
    map.remove("attempts");
    map.remove("enabled");
    let decoded = <TypedMapHeaderV3 as HeaderCodec>::decode_from_map(&map).unwrap();
    assert!(decoded.routing.is_none());
    assert_eq!(decoded.attempts, 0);
    assert!(decoded.enabled);

    assert_eq!(<TypedMapHeaderV3 as HeaderCodec>::FIELD_COUNT_HINT, 7);
    assert_eq!(<TypedMapHeaderV3 as HeaderCodec>::LOCAL_FIELD_SPECS.len(), 5);
    assert_eq!(<TypedMapHeaderV3 as HeaderCodec>::LOCAL_FLATTEN_SPECS.len(), 1);
    assert_eq!(
        <TypedMapHeaderV3 as HeaderCodec>::LOCAL_FLATTEN_SPECS[0].presence,
        FlattenPresenceSpec::Any
    );
    assert_eq!(
        <TypedMapHeaderV3 as HeaderCodec>::LOCAL_FIELD_SPECS[0].presence,
        HeaderPresence::Required
    );

    let mut field_keys = Vec::new();
    <TypedMapHeaderV3 as HeaderCodec>::visit_field_specs(&mut |spec| field_keys.push(spec.key));
    assert_eq!(
        field_keys,
        [
            "requestId",
            "traceId",
            "attempts",
            "queueId",
            "enabled",
            "topic",
            "routingQueueId"
        ]
    );

    let alias = <TypedMapHeaderV3 as HeaderCodec>::resolve_wire_key("legacyRequestId").unwrap();
    assert_eq!(alias.canonical, "requestId");
    assert_eq!(alias.precedence, 1);
    assert_eq!(alias.alias_conflict, AliasConflictPolicy::PreferCanonical);
    let nested = <TypedMapHeaderV3 as HeaderCodec>::resolve_wire_key("topic").unwrap();
    assert_eq!(nested.owner_type_id, "fixtures::RoutingHeaderV3");
    assert_eq!(
        <TypedMapHeaderV3 as HeaderCodec>::canonical_wire_key("legacyTraceId"),
        Some("traceId")
    );
}

#[test]
fn validation_and_java_ranges_fail_before_map_values_are_exposed() {
    let mut empty_required = sample_header();
    empty_required.request_id = CheetahString::default();
    let mut out = HeaderMap::new();
    assert!(matches!(
        empty_required.try_encode_into_map(&mut out),
        Err(HeaderCodecError::Validation {
            rule: "required_non_empty:requestId",
            ..
        })
    ));
    assert!(out.is_empty());
    assert!(empty_required.to_map().is_none());

    let mut invalid_range = sample_header();
    invalid_range.queue_id = i32::MAX as u32 + 1;
    assert!(matches!(
        invalid_range.try_encode_into_map(&mut out),
        Err(HeaderCodecError::JavaRange {
            header: "fixtures::TypedMapHeaderV3",
            key: "queueId"
        })
    ));

    let mut invalid_custom = sample_header();
    invalid_custom.attempts = -1;
    assert!(matches!(
        invalid_custom.try_encode_into_map(&mut out),
        Err(HeaderCodecError::Validation {
            rule: "attempts_non_negative",
            ..
        })
    ));
}

#[test]
fn object_safe_and_v2_fallible_shims_preserve_classified_failures() {
    let header: Box<dyn CommandCustomHeader> = Box::new(sample_header());
    assert_eq!(header.canonical_wire_key("legacyRequestId"), Some("requestId"));
    assert!(header.contains_wire_key("topic"));
    assert!(header.encoded_len_hint() > 0);

    let v2 = HardenedV2Header { value: String::new() };
    let mut out = HeaderMap::new();
    assert!(matches!(
        v2.try_encode_into_map(&mut out),
        Err(HeaderCodecError::LegacyValidation { .. })
    ));
    assert!(out.is_empty());
}
