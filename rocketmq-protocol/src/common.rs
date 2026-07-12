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

pub mod boundary_type;
pub mod compression;
pub mod consumer;
pub mod entity;
pub mod filter;
pub mod hasher;
pub mod key_builder;
pub mod lite;
pub mod message;
pub mod wire_constants {
    pub const KEY_SEPARATOR: &str = " ";
    pub const MASTER_ID: u64 = 0;
    pub const METADATA_SCOPE_GLOBAL: &str = "__global__";
    pub const RETRY_GROUP_TOPIC_PREFIX: &str = "%RETRY%";
}
pub mod sys_flag;
