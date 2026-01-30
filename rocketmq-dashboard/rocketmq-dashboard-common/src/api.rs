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

//! API definitions and client interfaces

use crate::models::*;
use anyhow::Result;

/// Dashboard API client trait
#[async_trait::async_trait]
pub trait DashboardClient: Send + Sync {
    /// Get list of brokers
    async fn get_brokers(&self) -> Result<Vec<BrokerInfo>>;

    /// Get list of topics
    async fn get_topics(&self) -> Result<Vec<TopicInfo>>;

    /// Get list of consumer groups
    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>>;
}
