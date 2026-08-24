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

use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderMap};
use serde::{Deserialize, Serialize};

// RequestHeaderCodec V1 expands these paths relative to its consuming crate.
pub mod protocol {
    pub mod command_custom_header {
        pub use rocketmq_protocol::{CommandCustomHeader, FromMap};
    }
}

#[allow(
    deprecated,
    reason = "freezes the legacy RequestHeaderCodec V1 nested-header expansion"
)]
mod legacy_nested_header {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodec)]
    pub struct LegacyNestedHeader {
        pub nested_flag: bool,
    }
}

use legacy_nested_header::LegacyNestedHeader;

#[allow(
    deprecated,
    reason = "freezes the legacy RequestHeaderCodec V1 wire and decode contract"
)]
mod legacy_header {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, RequestHeaderCodec)]
    pub struct LegacyHeader {
        #[required]
        pub request_name: String,
        #[required]
        pub request_token: CheetahString,
        pub attempt_count: Option<i32>,
        pub optional_label: Option<CheetahString>,
        pub retry_count: i32,
        #[serde(flatten)]
        pub nested: LegacyNestedHeader,
    }
}

use legacy_header::LegacyHeader;

fn sorted(map: &HeaderMap) -> BTreeMap<String, String> {
    map.iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn v1_named_struct_wire_map_and_decode_quirks_are_frozen() {
    let header = LegacyHeader {
        request_name: "request-name".to_owned(),
        request_token: CheetahString::from_static_str("token-7"),
        attempt_count: Some(7),
        optional_label: Some(CheetahString::from_static_str("label")),
        retry_count: 3,
        nested: LegacyNestedHeader { nested_flag: true },
    };

    assert_eq!(
        sorted(&header.to_map().expect("legacy header map")),
        BTreeMap::from([
            ("attemptCount".to_owned(), "7".to_owned()),
            ("nestedFlag".to_owned(), "true".to_owned()),
            ("optionalLabel".to_owned(), "label".to_owned()),
            ("requestName".to_owned(), "request-name".to_owned()),
            ("requestToken".to_owned(), "token-7".to_owned()),
            ("retryCount".to_owned(), "3".to_owned()),
        ])
    );

    let decoded = <LegacyHeader as FromMap>::from(&HeaderMap::from([
        (CheetahString::from_static_str("requestName"), "request-name".into()),
        (CheetahString::from_static_str("requestToken"), "token-7".into()),
        (CheetahString::from_static_str("attemptCount"), "invalid".into()),
        (CheetahString::from_static_str("optionalLabel"), "label".into()),
        (CheetahString::from_static_str("retryCount"), "invalid".into()),
        (CheetahString::from_static_str("nestedFlag"), "true".into()),
    ]))
    .expect("legacy decode");

    assert_eq!(decoded.request_name, "request-name");
    assert_eq!(decoded.request_token.as_str(), "token-7");
    assert_eq!(decoded.attempt_count, None);
    assert_eq!(decoded.optional_label.as_deref(), Some("label"));
    assert_eq!(decoded.retry_count, 0);
    assert!(decoded.nested.nested_flag);

    let missing_required = <LegacyHeader as FromMap>::from(&HeaderMap::new()).expect_err("missing requestName");
    assert!(missing_required.to_string().contains("Missing requestName field"));
}

#[test]
fn v1_required_decode_order_and_absent_optional_defaults_are_frozen() {
    let missing_request_token = <LegacyHeader as FromMap>::from(&HeaderMap::from([
        (CheetahString::from_static_str("requestName"), "request-name".into()),
        (CheetahString::from_static_str("nestedFlag"), "true".into()),
    ]))
    .expect_err("requestToken remains required after requestName and nested fields are present");
    assert_eq!(
        missing_request_token.to_string(),
        "Request header error: Missing requestToken field"
    );

    let decoded = <LegacyHeader as FromMap>::from(&HeaderMap::from([
        (CheetahString::from_static_str("requestName"), "request-name".into()),
        (CheetahString::from_static_str("requestToken"), "token-7".into()),
        (CheetahString::from_static_str("nestedFlag"), "true".into()),
    ]))
    .expect("optional fields and non-required scalar may be absent");

    assert_eq!(decoded.attempt_count, None);
    assert_eq!(decoded.optional_label, None);
    assert_eq!(decoded.retry_count, 0);
}

#[test]
fn v1_none_optionals_omit_their_wire_keys_with_a_literal_oracle() {
    let header = LegacyHeader {
        request_name: "request-name".to_owned(),
        request_token: CheetahString::from_static_str("token-7"),
        attempt_count: None,
        optional_label: None,
        retry_count: 0,
        nested: LegacyNestedHeader { nested_flag: true },
    };

    let map = header.to_map().expect("legacy header map");
    assert!(!map.contains_key("attemptCount"));
    assert!(!map.contains_key("optionalLabel"));
    assert_eq!(
        sorted(&map),
        BTreeMap::from([
            ("nestedFlag".to_owned(), "true".to_owned()),
            ("requestName".to_owned(), "request-name".to_owned()),
            ("requestToken".to_owned(), "token-7".to_owned()),
            ("retryCount".to_owned(), "0".to_owned()),
        ])
    );
}
