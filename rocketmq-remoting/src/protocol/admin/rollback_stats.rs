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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RollbackStats {
    pub broker_name: CheetahString,
    pub queue_id: i64,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub timestamp_offset: i64,
    pub rollback_offset: i64,
}

impl Display for RollbackStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RollbackStats [brokerName={}, queueId={}, brokerOffset={}, consumerOffset={}, timestampOffset={}, \
             rollbackOffset={}]",
            self.broker_name,
            self.queue_id,
            self.broker_offset,
            self.consumer_offset,
            self.timestamp_offset,
            self.rollback_offset
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn rollback_stats_default_values() {
        let stats: RollbackStats = Default::default();
        assert_eq!(stats.broker_name, CheetahString::from(""));
        assert_eq!(stats.queue_id, 0);
        assert_eq!(stats.broker_offset, 0);
        assert_eq!(stats.consumer_offset, 0);
        assert_eq!(stats.timestamp_offset, 0);
        assert_eq!(stats.rollback_offset, 0);
    }

    #[test]
    fn rollback_stats_serialization() {
        let stats = RollbackStats {
            broker_name: CheetahString::from("broker1"),
            queue_id: 1,
            broker_offset: 100,
            consumer_offset: 200,
            timestamp_offset: 300,
            rollback_offset: 400,
        };
        let serialized = serde_json::to_string(&stats).unwrap();
        assert_eq!(
            serialized,
            r#"{"brokerName":"broker1","queueId":1,"brokerOffset":100,"consumerOffset":200,"timestampOffset":300,"rollbackOffset":400}"#
        );
    }

    #[test]
    fn rollback_stats_deserialization() {
        let json = r#"{"brokerName":"broker1","queueId":1,"brokerOffset":100,"consumerOffset":200,"timestampOffset":300,"rollbackOffset":400}"#;
        let deserialized: RollbackStats = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.broker_name, CheetahString::from("broker1"));
        assert_eq!(deserialized.queue_id, 1);
        assert_eq!(deserialized.broker_offset, 100);
        assert_eq!(deserialized.consumer_offset, 200);
        assert_eq!(deserialized.timestamp_offset, 300);
        assert_eq!(deserialized.rollback_offset, 400);
    }

    #[test]
    fn rollback_stats_display_format() {
        let stats = RollbackStats {
            broker_name: CheetahString::from("broker1"),
            queue_id: 1,
            broker_offset: 100,
            consumer_offset: 200,
            timestamp_offset: 300,
            rollback_offset: 400,
        };
        let display = format!("{}", stats);
        assert_eq!(
            display,
            "RollbackStats [brokerName=broker1, queueId=1, brokerOffset=100, consumerOffset=200, timestampOffset=300, \
             rollbackOffset=400]"
        );
    }
}
