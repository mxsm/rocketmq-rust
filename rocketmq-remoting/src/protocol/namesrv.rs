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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::kv_table::KVTable;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegisterBrokerResult {
    #[serde(rename = "haServerAddr")]
    pub ha_server_addr: CheetahString,

    #[serde(rename = "masterAddr")]
    pub master_addr: CheetahString,

    #[serde(rename = "kvTable")]
    pub kv_table: KVTable,
}
