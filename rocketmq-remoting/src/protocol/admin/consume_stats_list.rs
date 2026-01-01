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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use super::consume_stats::ConsumeStats;

#[derive(Serialize, Deserialize)]
pub struct ConsumeStatsList {
    #[serde(rename = "consume_stats_list")]
    pub consume_stats_list: Vec<HashMap<CheetahString, Vec<ConsumeStats>>>,

    #[serde(rename = "brokerAddr")]
    pub broker_addr: Option<CheetahString>,

    #[serde(rename = "total_diff")]
    pub total_diff: i64,

    #[serde(rename = "total_inflight_diff")]
    pub total_inflight_diff: i64,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn consume_status_list_serialization() {
        let mut map = HashMap::new();
        let consume_stats_list = vec![ConsumeStats {
            offset_table: HashMap::new(),
            consume_tps: 1.2,
        }];
        map.insert(CheetahString::from("group1"), consume_stats_list);
        let consume_status_list = ConsumeStatsList {
            consume_stats_list: vec![map],
            broker_addr: Some(CheetahString::from("addr")),
            total_diff: 2,
            total_inflight_diff: 1,
        };
        let serialized = serde_json::to_string(&consume_status_list).unwrap();
        println!("{}", serialized);
        let deserialized: ConsumeStatsList = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.broker_addr.unwrap(), CheetahString::from("addr"));
        assert_eq!(deserialized.total_diff, 2);
        assert_eq!(deserialized.total_inflight_diff, 1);
        let a = deserialized.consume_stats_list[0]
            .get(&CheetahString::from("group1"))
            .unwrap();
        assert_eq!(a[0].consume_tps, 1.2);
    }
}
