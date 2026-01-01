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
use std::sync::atomic::Ordering;

use rocketmq_remoting::protocol::RemotingSerializable;
use serde::Deserialize;
use serde::Serialize;

use crate::consumer::store::offset_serialize_wrapper::OffsetSerializeWrapper;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct OffsetSerialize {
    pub offset_table: HashMap<String, i64>,
}

impl From<OffsetSerializeWrapper> for OffsetSerialize {
    fn from(wrapper: OffsetSerializeWrapper) -> Self {
        let mut offset_table = HashMap::new();
        for (k, v) in wrapper.offset_table {
            let result = k
                .serialize_json()
                .expect("OffsetSerialize::from OffsetSerializeWrapper");
            offset_table.insert(result, v.load(Ordering::Relaxed));
        }
        OffsetSerialize { offset_table }
    }
}
