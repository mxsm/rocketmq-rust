// Copyright 2025 The RocketMQ Rust Authors
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

//! Business logic and service layer

use crate::api::DashboardClient;
use crate::models::*;
use anyhow::Result;
use std::sync::Arc;

/// Dashboard service for business logic
pub struct DashboardService {
    client: Arc<dyn DashboardClient>,
}

impl DashboardService {
    pub fn new(client: Arc<dyn DashboardClient>) -> Self {
        Self { client }
    }

    pub async fn fetch_broker_list(&self) -> Result<Vec<BrokerInfo>> {
        self.client.get_brokers().await
    }

    pub async fn fetch_topic_list(&self) -> Result<Vec<TopicInfo>> {
        self.client.get_topics().await
    }

    pub async fn fetch_consumer_group_list(&self) -> Result<Vec<ConsumerGroupInfo>> {
        self.client.get_consumer_groups().await
    }
}
