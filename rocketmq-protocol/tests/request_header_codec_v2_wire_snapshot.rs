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

#![allow(deprecated)]

use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderMap, ProtocolContractViolation};
use serde::{Deserialize, Serialize};

fn default_limit() -> i32 {
    32
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct BaseHeader {
    #[required]
    request_id: CheetahString,
    enabled: Option<bool>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct RoutingHeader {
    #[serde(flatten)]
    base: BaseHeader,
    #[serde(rename = "t", alias = "topic", alias = "legacyTopic")]
    topic_name: Option<CheetahString>,
    #[serde(default = "default_limit")]
    limit: i32,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct GenericHeader<T>
where
    T: Default + ToString + std::str::FromStr + 'static,
{
    value: T,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct OptionalLeaf {
    marker: Option<i64>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "rocketmq_protocol")]
struct OptionalFlattenHeader {
    #[serde(flatten)]
    leaf: Option<OptionalLeaf>,
    count: i32,
}

fn sorted(map: &HeaderMap) -> BTreeMap<String, String> {
    map.iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn supported_field_shapes_keep_the_frozen_wire_snapshot() {
    let header = RoutingHeader {
        base: BaseHeader {
            request_id: CheetahString::from_static_str("request-7"),
            enabled: Some(true),
        },
        topic_name: Some(CheetahString::from_static_str("orders")),
        limit: 64,
    };

    assert_eq!(
        sorted(&header.to_map().expect("header map")),
        BTreeMap::from([
            ("enabled".to_owned(), "true".to_owned()),
            ("limit".to_owned(), "64".to_owned()),
            ("requestId".to_owned(), "request-7".to_owned()),
            ("t".to_owned(), "orders".to_owned()),
        ])
    );

    let mut output = HeaderMap::new();
    output.insert(
        CheetahString::from_static_str("existing"),
        CheetahString::from_static_str("preserved"),
    );
    header.encode_into_map(&mut output);
    assert_eq!(output.len(), 5);
    assert_eq!(output.get("existing").map(CheetahString::as_str), Some("preserved"));
}

#[test]
fn canonical_alias_order_defaults_and_generics_are_deterministic() {
    let mut map = HeaderMap::from([
        (
            CheetahString::from_static_str("requestId"),
            CheetahString::from_static_str("request-8"),
        ),
        (
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("alias-topic"),
        ),
        (
            CheetahString::from_static_str("t"),
            CheetahString::from_static_str("canonical-topic"),
        ),
    ]);
    let decoded = <RoutingHeader as FromMap>::from(&map).expect("routing header");
    assert_eq!(decoded.topic_name.as_deref(), Some("canonical-topic"));
    assert_eq!(decoded.limit, 32);

    map = HeaderMap::from([(
        CheetahString::from_static_str("value"),
        CheetahString::from_static_str("41"),
    )]);
    let generic = <GenericHeader<u32> as FromMap>::from(&map).expect("generic header");
    assert_eq!(generic.value, 41);

    // The generated codec reports the failure through `request_header_error`, which discards the
    // free-form "Missing requestId field" detail; the stable contract is the header-invalid
    // descriptor.
    let error = <BaseHeader as FromMap>::from(&HeaderMap::new()).expect_err("requestId is required");
    assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);
}

#[test]
fn malformed_unknown_empty_and_optional_flatten_behavior_is_frozen() {
    let mut map = HeaderMap::from([
        (
            CheetahString::from_static_str("count"),
            CheetahString::from_static_str("9"),
        ),
        (
            CheetahString::from_static_str("unknown"),
            CheetahString::from_static_str("ignored"),
        ),
    ]);
    let decoded = <OptionalFlattenHeader as FromMap>::from(&map).expect("optional flatten header");
    assert_eq!(decoded.leaf, Some(OptionalLeaf::default()));
    assert_eq!(
        sorted(&decoded.to_map().expect("header map")),
        BTreeMap::from([("count".into(), "9".into())])
    );

    map.insert(
        CheetahString::from_static_str("count"),
        CheetahString::from_static_str("not-an-integer"),
    );
    // The generated codec drops the free-form "Parse count field error" detail; only the
    // header-invalid descriptor survives the canonical rendering.
    let error = <OptionalFlattenHeader as FromMap>::from(&map).expect_err("malformed count");
    assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);

    let empty_required = HeaderMap::from([(CheetahString::from_static_str("requestId"), CheetahString::new())]);
    let error = <BaseHeader as FromMap>::from(&empty_required).expect_err("empty requestId");
    assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);
    // The dropped "Required header field requestId must not be empty" detail comes from the
    // required-field validation hook, which keeps its classification through the typed codec
    // channel instead.
    let violation = BaseHeader {
        request_id: CheetahString::new(),
        enabled: None,
    }
    .try_encode_into_map(&mut HeaderMap::new())
    .expect_err("empty requestId should fail the header validation hook");
    assert!(matches!(violation, ProtocolContractViolation::LegacyValidation { .. }));
}
