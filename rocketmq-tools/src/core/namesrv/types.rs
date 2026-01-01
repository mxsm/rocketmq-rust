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

//! NameServer-related types

use serde::Deserialize;
use serde::Serialize;

/// NameServer configuration item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigItem {
    pub key: String,
    pub value: String,
    pub description: Option<String>,
}

/// Broker permission status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerPermission {
    pub broker_name: String,
    pub perm: i32,
    pub has_write_perm: bool,
}
