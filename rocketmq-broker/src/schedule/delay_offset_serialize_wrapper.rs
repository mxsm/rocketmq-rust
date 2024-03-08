/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;

use rocketmq_remoting::protocol::DataVersion;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayOffsetSerializeWrapper {
    offset_table: HashMap<i32 /* level */, i64 /* offset */>,
    data_version: DataVersion,
}

impl DelayOffsetSerializeWrapper {
    pub fn offset_table(&self) -> &HashMap<i32, i64> {
        &self.offset_table
    }
    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }
}
