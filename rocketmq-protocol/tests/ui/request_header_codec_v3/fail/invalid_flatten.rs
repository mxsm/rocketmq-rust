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

struct Nested;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "fixtures::Flatten", crate = "rocketmq_protocol")]
struct InvalidFlatten {
    #[header(flatten)]
    optional_without_presence: Option<Nested>,
    #[header(flatten, presence = "any")]
    non_optional_any: Nested,
    #[header(flatten, key = "nested", required, java_type = "Nested")]
    conflicting_options: Nested,
    #[header(presence = "always", required)]
    presence_without_flatten: String,
    #[header(flatten, presence = "sometimes")]
    invalid_presence: Option<Nested>,
    #[header(flatten)]
    scalar: i32,
}

fn main() {}
