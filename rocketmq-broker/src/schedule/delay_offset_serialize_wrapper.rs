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

use std::collections::HashMap;

use rocketmq_remoting::protocol::DataVersion;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayOffsetSerializeWrapper {
    offset_table: Option<HashMap<i32 /* level */, i64 /* offset */>>,
    data_version: Option<DataVersion>,
}

impl DelayOffsetSerializeWrapper {
    pub fn offset_table(&self) -> Option<&HashMap<i32, i64>> {
        self.offset_table.as_ref()
    }

    pub fn data_version(&self) -> Option<&DataVersion> {
        self.data_version.as_ref()
    }

    pub fn new(offset_table: Option<HashMap<i32, i64>>, data_version: Option<DataVersion>) -> Self {
        Self {
            offset_table,
            data_version,
        }
    }
}
