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

use std::ops::Deref;
use std::sync::LazyLock;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::subscription::customized_retry_policy::CustomizedRetryPolicy;
use crate::protocol::subscription::exponential_retry_policy::ExponentialRetryPolicy;
use crate::protocol::subscription::group_retry_policy_type::GroupRetryPolicyType;
use crate::protocol::subscription::retry_policy::RetryPolicy;

static DEFAULT_RETRY_POLICY: LazyLock<CustomizedRetryPolicy> = LazyLock::new(CustomizedRetryPolicy::default);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRetryPolicy {
    #[serde(rename = "type")]
    type_: GroupRetryPolicyType,
    exponential_retry_policy: Option<ExponentialRetryPolicy>,
    customized_retry_policy: Option<CustomizedRetryPolicy>,
}

impl Default for GroupRetryPolicy {
    fn default() -> Self {
        GroupRetryPolicy {
            type_: GroupRetryPolicyType::Customized,
            exponential_retry_policy: None,
            customized_retry_policy: None,
        }
    }
}

impl GroupRetryPolicy {
    pub fn type_(&self) -> GroupRetryPolicyType {
        self.type_
    }

    pub fn exponential_retry_policy(&self) -> Option<&ExponentialRetryPolicy> {
        self.exponential_retry_policy.as_ref()
    }

    pub fn customized_retry_policy(&self) -> Option<&CustomizedRetryPolicy> {
        self.customized_retry_policy.as_ref()
    }

    pub fn set_type_(&mut self, type_: GroupRetryPolicyType) {
        self.type_ = type_;
    }

    pub fn set_exponential_retry_policy(&mut self, exponential_retry_policy: Option<ExponentialRetryPolicy>) {
        self.exponential_retry_policy = exponential_retry_policy;
    }

    pub fn set_customized_retry_policy(&mut self, customized_retry_policy: Option<CustomizedRetryPolicy>) {
        self.customized_retry_policy = customized_retry_policy;
    }

    pub fn get_retry_policy(&self) -> &dyn RetryPolicy {
        match self.type_ {
            GroupRetryPolicyType::Exponential => self
                .exponential_retry_policy
                .as_ref()
                .map(|p| p as &dyn RetryPolicy)
                .unwrap_or(DEFAULT_RETRY_POLICY.deref() as &dyn RetryPolicy),
            GroupRetryPolicyType::Customized => self
                .customized_retry_policy
                .as_ref()
                .map(|p| p as &dyn RetryPolicy)
                .unwrap_or(DEFAULT_RETRY_POLICY.deref() as &dyn RetryPolicy),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_preserves_retry_policy_configuration() {
        let mut policy = GroupRetryPolicy::default();
        let customized: CustomizedRetryPolicy = serde_json::from_str(r#"{"next":[1,2,42]}"#).unwrap();
        policy.set_type_(GroupRetryPolicyType::Exponential);
        policy.set_exponential_retry_policy(Some(ExponentialRetryPolicy::new(2_000, 30_000, 3)));
        policy.set_customized_retry_policy(Some(customized));

        let json = serde_json::to_value(&policy).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "EXPONENTIAL",
                "exponentialRetryPolicy": {"initial": 2_000, "max": 30_000, "multiplier": 3},
                "customizedRetryPolicy": {"next": [1, 2, 42]},
            })
        );

        let decoded: GroupRetryPolicy = serde_json::from_value(json).unwrap();

        assert_eq!(decoded.type_(), GroupRetryPolicyType::Exponential);
        assert_eq!(
            (
                decoded.exponential_retry_policy().unwrap().initial(),
                decoded.exponential_retry_policy().unwrap().max(),
                decoded.exponential_retry_policy().unwrap().multiplier(),
            ),
            (2_000, 30_000, 3)
        );
        assert_eq!(decoded.customized_retry_policy().unwrap().next(), [1, 2, 42]);
    }

    #[test]
    fn get_retry_policy_selects_configured_policy_or_default() {
        let mut policy = GroupRetryPolicy::default();
        assert_eq!(
            policy.get_retry_policy().next_delay_duration(0),
            DEFAULT_RETRY_POLICY.next_delay_duration(0)
        );

        policy.set_type_(GroupRetryPolicyType::Exponential);
        policy.set_exponential_retry_policy(Some(ExponentialRetryPolicy::new(2_000, 30_000, 3)));
        assert_eq!(policy.get_retry_policy().next_delay_duration(1), 6_000);

        policy.set_exponential_retry_policy(None);
        assert_eq!(
            policy.get_retry_policy().next_delay_duration(0),
            DEFAULT_RETRY_POLICY.next_delay_duration(0)
        );

        let customized: CustomizedRetryPolicy = serde_json::from_str(r#"{"next":[1,2,42]}"#).unwrap();
        policy.set_type_(GroupRetryPolicyType::Customized);
        policy.set_customized_retry_policy(Some(customized));
        assert_eq!(policy.get_retry_policy().next_delay_duration(0), 42);
    }
}
