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

use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::MissingPolicies", crate = "rocketmq_protocol")]
struct MissingPolicies {
    #[header(required)]
    required_option: Option<String>,
    implicit_default: bool,
    #[header(required, default, default_semantic = "literal:false")]
    conflicting: bool,
    #[header(default)]
    missing_semantic: i32,
    #[header(required, default_semantic = "literal:0")]
    unexpected_semantic: i32,
    #[header(default, default_semantic = "unstable")]
    malformed_semantic: i32,
}

fn main() {}
