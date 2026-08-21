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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

/// Java-compatible body for deleting multiple subscription groups in one request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteSubscriptionGroupListRequestBody {
    pub group_name_list: Vec<CheetahString>,
    #[serde(default)]
    pub clean_offset: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_group_body_defaults_clean_offset_to_false() {
        let body: DeleteSubscriptionGroupListRequestBody =
            serde_json::from_str(r#"{"groupNameList":["GroupA","GroupB"]}"#).expect("decode group list");

        assert_eq!(
            vec![
                CheetahString::from_static_str("GroupA"),
                CheetahString::from_static_str("GroupB")
            ],
            body.group_name_list
        );
        assert!(!body.clean_offset);
        assert_eq!(
            r#"{"groupNameList":["GroupA","GroupB"],"cleanOffset":false}"#,
            serde_json::to_string(&body).expect("encode group list")
        );
    }
}
