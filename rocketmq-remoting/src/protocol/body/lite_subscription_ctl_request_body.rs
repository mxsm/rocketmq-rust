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

use rocketmq_common::common::lite::LiteSubscriptionDTO;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LiteSubscriptionCtlRequestBody {
    #[serde(default)]
    subscription_set: Vec<LiteSubscriptionDTO>,
}

impl LiteSubscriptionCtlRequestBody {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn subscription_set(&self) -> &[LiteSubscriptionDTO] {
        &self.subscription_set
    }

    pub fn set_subscription_set(&mut self, subscription_set: Vec<LiteSubscriptionDTO>) {
        self.subscription_set = subscription_set;
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::lite::LiteSubscriptionAction;

    use super::*;

    #[test]
    fn lite_subscription_ctl_request_body_round_trip() {
        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id("client".into())
            .with_group("group".into())
            .with_topic("topic".into());
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto.clone()]);

        let json = serde_json::to_string(&body).unwrap();
        let decoded: LiteSubscriptionCtlRequestBody = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.subscription_set(), &[dto]);
    }
}
