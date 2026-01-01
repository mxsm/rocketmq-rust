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

use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CheckClientRequestBody {
    pub client_id: String,
    pub group: String,
    pub subscription_data: SubscriptionData,
    pub namespace: Option<String>,
}

impl CheckClientRequestBody {
    pub fn new(client_id: String, group: String, subscription_data: SubscriptionData) -> Self {
        Self {
            client_id,
            group,
            subscription_data,
            namespace: None,
        }
    }

    pub fn get_client_id(&self) -> &String {
        &self.client_id
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.client_id = client_id;
    }

    pub fn get_group(&self) -> &String {
        &self.group
    }

    pub fn set_group(&mut self, group: String) {
        self.group = group;
    }

    pub fn get_subscription_data(&self) -> &SubscriptionData {
        &self.subscription_data
    }

    pub fn set_subscription_data(&mut self, subscription_data: SubscriptionData) {
        self.subscription_data = subscription_data;
    }
}
