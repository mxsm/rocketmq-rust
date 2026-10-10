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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DiagnoseConsumerLagArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Exact Topic name. `rocketmq_list_topics` lists the names.
    pub topic: String,
    /// Exact Consumer Group name. `rocketmq_list_consumer_groups` lists the names.
    pub consumer_group: String,
}
