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
    //default_retry_policy: CustomizedRetryPolicy,
}

impl Default for GroupRetryPolicy {
    fn default() -> Self {
        GroupRetryPolicy {
            type_: GroupRetryPolicyType::Customized,
            exponential_retry_policy: None,
            customized_retry_policy: None,
            //default_retry_policy: CustomizedRetryPolicy::default(),
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
