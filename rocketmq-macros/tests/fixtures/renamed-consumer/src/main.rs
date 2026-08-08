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

use protocol_api::{CommandCustomHeader, HeaderCodec};
use rocketmq_macros::{RequestHeaderCodecV2, RequestHeaderCodecV3};

#[derive(RequestHeaderCodecV2)]
struct RenamedConsumerHeader {
    queue_id: i32,
}

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::RenamedV3Header")]
struct RenamedV3Header {
    #[header(required)]
    queue_id: i32,
}

fn main() {
    let header = RenamedConsumerHeader { queue_id: 7 };
    let fields = header.to_map().expect("header map");
    assert_eq!(fields.get("queueId").map(|value| value.as_str()), Some("7"));

    let v3_header = RenamedV3Header { queue_id: 7 };
    let v3_fields = v3_header.to_map().expect("V3 header map");
    assert_eq!(v3_fields.get("queueId").map(|value| value.as_str()), Some("7"));
    assert_eq!(
        <RenamedV3Header as HeaderCodec>::canonical_wire_key("queueId"),
        Some("queueId")
    );
}
