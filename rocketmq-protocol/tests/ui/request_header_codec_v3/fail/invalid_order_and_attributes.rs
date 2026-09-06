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
#[header(type_id = "fixtures::Attributes", crate = "rocketmq_protocol")]
struct InvalidAttributes {
    #[header(required, binary_order = 7)]
    first: String,
    #[header(required, binary_order = 7)]
    second: String,
    #[header(required, mystery = "value")]
    unknown: String,
    #[header(required = true)]
    valued_flag: String,
    #[required]
    #[header(required)]
    duplicate_required_forms: String,
}

fn main() {}
