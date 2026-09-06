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
use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_model::boundary_type::BoundaryType;

fn default_flag() -> bool {
    false
}

struct Nested<T> {
    value: T,
}

#[derive(RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::CompleteHeader",
    java_class = "org.apache.rocketmq.fixtures.CompleteHeader",
    validate = "Self::validate_header",
    lookup = "get",
    crate = "rocketmq_protocol",
    fast
)]
struct CompleteHeader<T> {
    #[header(key = "name", alias = "legacyName", alias_conflict = "prefer_canonical", required)]
    name: CheetahString,
    #[header(key = "description")]
    description: Option<String>,
    #[header(default_with = "default_flag", default_semantic = "literal:false")]
    enabled: bool,
    #[header(default, default_semantic = "literal:0")]
    attempts: i32,
    #[header(required)]
    timestamp: i64,
    #[header(required, range = "i32")]
    queue_id: u32,
    #[header(required, range = "i64")]
    offset: u64,
    #[header(default, default_semantic = "literal:LOWER")]
    boundary: BoundaryType,
    #[header(required)]
    generic: T,
    #[header(flatten, presence = "any")]
    nested: Option<Nested<T>>,
}

impl<T> CompleteHeader<T> {
    fn validate_header(&self) -> Result<(), rocketmq_protocol::ProtocolContractViolation> {
        Ok(())
    }
}

fn main() {}
