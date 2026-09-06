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
#[header(type_id = "fixtures::Paths")]
#[header(type_id = "fixtures::DuplicatePaths")]
#[header(java_class = "not a java class")]
#[header(validate = "not a path!")]
#[header(crate = "not a path!")]
#[header(fast)]
#[header(fast)]
struct InvalidPathsAndDuplicates<T> {
    #[header(default_with = "not a path!", default_semantic = "dynamic:provider")]
    invalid_default_path: i32,
    #[header(required, binary_order = 65536)]
    order_overflow: i32,
    #[header(required)]
    unsupported_generic_container: Vec<T>,
}

fn main() {}
