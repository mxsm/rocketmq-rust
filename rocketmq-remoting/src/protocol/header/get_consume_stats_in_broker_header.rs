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

use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
pub struct GetConsumeStatsInBrokerHeader {
    #[required]
    #[serde(rename = "isOrder")]
    pub is_order: bool,
}

#[cfg(test)]
mod tests {
    use super::GetConsumeStatsInBrokerHeader;

    #[test]
    fn get_consume_stats_in_broker_header_round_trips() {
        let header = GetConsumeStatsInBrokerHeader { is_order: true };
        let json = serde_json::to_string(&header).expect("serialize get consume stats in broker header");
        assert_eq!(json, r#"{"isOrder":true}"#);

        let decoded: GetConsumeStatsInBrokerHeader =
            serde_json::from_str(&json).expect("deserialize get consume stats in broker header");
        assert!(decoded.is_order);
    }
}
