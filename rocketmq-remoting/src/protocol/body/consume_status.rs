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

use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsumeStatus {
    #[serde(rename = "pullRT")]
    pub pull_rt: f64,

    #[serde(rename = "pullTPS")]
    pub pull_tps: f64,

    #[serde(rename = "consumeRT")]
    pub consume_rt: f64,

    #[serde(rename = "consumeOKTPS")]
    pub consume_ok_tps: f64,

    #[serde(rename = "consumeFailedTPS")]
    pub consume_failed_tps: f64,

    #[serde(rename = "consumeFailedMsgs")]
    pub consume_failed_msgs: i64,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn consume_status_serialization() {
        let consume_status = ConsumeStatus {
            pull_rt: 1.1,
            pull_tps: 1.2,
            consume_rt: 1.3,
            consume_ok_tps: 1.4,
            consume_failed_tps: 1.5,
            consume_failed_msgs: 6,
        };
        let serialized = serde_json::to_string(&consume_status).unwrap();
        let deserialized: ConsumeStatus = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.pull_rt, 1.1);
        assert_eq!(deserialized.pull_tps, 1.2);
        assert_eq!(deserialized.consume_rt, 1.3);
        assert_eq!(deserialized.consume_ok_tps, 1.4);
        assert_eq!(deserialized.consume_failed_tps, 1.5);
        assert_eq!(deserialized.consume_failed_msgs, 6);
    }

    #[test]
    fn consume_status_deserialization() {
        let serialized = r#"{"pullRT":1.1,"pullTPS":1.2,"consumeRT":1.3,"consumeOKTPS":1.4,"consumeFailedTPS":1.5,"consumeFailedMsgs":6}"#;
        let deserialized: ConsumeStatus = serde_json::from_str(serialized).unwrap();
        assert_eq!(deserialized.pull_rt, 1.1);
        assert_eq!(deserialized.pull_tps, 1.2);
        assert_eq!(deserialized.consume_rt, 1.3);
        assert_eq!(deserialized.consume_ok_tps, 1.4);
        assert_eq!(deserialized.consume_failed_tps, 1.5);
        assert_eq!(deserialized.consume_failed_msgs, 6);
    }
}
