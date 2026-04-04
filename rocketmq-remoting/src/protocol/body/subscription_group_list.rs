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

use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupList {
    pub group_config_list: Vec<SubscriptionGroupConfig>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn subscription_group_list_round_trips() {
        let body = SubscriptionGroupList {
            group_config_list: vec![SubscriptionGroupConfig::new(CheetahString::from_static_str("group-a"))],
        };

        let json = serde_json::to_string(&body).expect("serialize subscription group list");
        assert!(json.contains("\"groupConfigList\""));
        assert!(json.contains("\"groupName\":\"group-a\""));

        let decoded: SubscriptionGroupList = serde_json::from_str(&json).expect("deserialize subscription group list");
        assert_eq!(decoded.group_config_list.len(), 1);
        assert_eq!(decoded.group_config_list[0].group_name(), "group-a");
    }
}
