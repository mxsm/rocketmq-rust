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

//! Data models shared across dashboard implementations

use serde::Deserialize;
use serde::Serialize;

/// Broker information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub broker_name: String,
    pub broker_addr: String,
    pub cluster: String,
}

/// Topic information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub topic_name: String,
    pub broker_name: String,
    pub queue_count: i32,
}

/// Consumer group information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub group_name: String,
    pub consume_type: String,
    pub message_model: String,
}
