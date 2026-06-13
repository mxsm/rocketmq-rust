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

use rocketmq_common::common::mix_all;

use crate::protocol::remoting_command::RemotingCommand;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeartbeatV2Result {
    version: i32,
    sub_change: bool,
    support_v2: bool,
}

impl HeartbeatV2Result {
    pub fn new(version: i32, sub_change: bool, support_v2: bool) -> Self {
        Self {
            version,
            sub_change,
            support_v2,
        }
    }

    pub fn from_response(response: &RemotingCommand) -> Self {
        let ext_fields = response.ext_fields();
        Self {
            version: response.version(),
            sub_change: ext_fields
                .and_then(|fields| fields.get(mix_all::IS_SUB_CHANGE))
                .is_some_and(|value| value.as_str().eq_ignore_ascii_case("true")),
            support_v2: ext_fields
                .and_then(|fields| fields.get(mix_all::IS_SUPPORT_HEART_BEAT_V2))
                .is_some_and(|value| value.as_str().eq_ignore_ascii_case("true")),
        }
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn is_sub_change(&self) -> bool {
        self.sub_change
    }

    pub fn is_support_v2(&self) -> bool {
        self.support_v2
    }

    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }

    pub fn set_sub_change(&mut self, sub_change: bool) {
        self.sub_change = sub_change;
    }

    pub fn set_support_v2(&mut self, support_v2: bool) {
        self.support_v2 = support_v2;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn heartbeat_v2_result_from_response_matches_java_ext_fields() {
        let response = RemotingCommand::default()
            .set_version(456)
            .set_ext_fields(HashMap::from([
                (
                    CheetahString::from_static_str(mix_all::IS_SUB_CHANGE),
                    CheetahString::from_static_str("TrUe"),
                ),
                (
                    CheetahString::from_static_str(mix_all::IS_SUPPORT_HEART_BEAT_V2),
                    CheetahString::from_static_str("true"),
                ),
            ]));

        let result = HeartbeatV2Result::from_response(&response);

        assert_eq!(result.version(), 456);
        assert!(result.is_sub_change());
        assert!(result.is_support_v2());
    }

    #[test]
    fn heartbeat_v2_result_from_response_defaults_false_without_ext_fields_like_java() {
        let response = RemotingCommand::default().set_version(123);

        let result = HeartbeatV2Result::from_response(&response);

        assert_eq!(result.version(), 123);
        assert!(!result.is_sub_change());
        assert!(!result.is_support_v2());
    }
}
