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

//! Dashboard-specific Consumer patch contract. The smaller supervised patch keeps its existing limits.

use super::{AdminError, AdminFuture, AdminResult};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigValue {
    pub version: u64,
    pub retry_max_times: i32,
    pub retry_queue_nums: u32,
    pub consume_timeout_minutes: u32,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerRetryPolicy {
    #[default]
    DefaultCustomized,
    Customized {
        delays_ms: Vec<i64>,
    },
    Exponential {
        initial_ms: u64,
        maximum_ms: u64,
        multiplier: u64,
    },
    DefaultFallback,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConfigDetails {
    pub retry_policy: ConsumerRetryPolicy,
    pub subscribed_topics: Vec<String>,
    pub attributes: Vec<(String, String)>,
    pub truncated: bool,
}

impl std::fmt::Debug for ConsumerConfigDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerConfigDetails")
            .field("topic_count", &self.subscribed_topics.len())
            .field("attribute_count", &self.attributes.len())
            .field("truncated", &self.truncated)
            .finish_non_exhaustive()
    }
}

/// Omitted fields retain the authoritative per-Broker value. Retry policy, subscriptions,
/// attributes, notifications and the system flag have no writable slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerWorkspaceConfigPatch {
    pub retry_max_times: Option<i32>,
    pub retry_queue_nums: Option<u32>,
    pub consume_timeout_minutes: Option<u32>,
    pub consume_enable: Option<bool>,
    pub consume_from_min_enable: Option<bool>,
    pub consume_broadcast_enable: Option<bool>,
    pub consume_message_orderly: Option<bool>,
    pub broker_id: Option<u64>,
    pub which_broker_when_consume_slowly: Option<u64>,
}

impl ConsumerWorkspaceConfigPatch {
    pub fn validate(self) -> AdminResult<Self> {
        if self == Self::default() {
            return Err(AdminError::invalid_argument("patch", "must not be empty"));
        }
        if self.retry_max_times.is_some_and(|value| value < -1)
            || self.retry_queue_nums.is_some_and(|value| value > i32::MAX as u32)
            || self
                .consume_timeout_minutes
                .is_some_and(|value| !(1..=i32::MAX as u32).contains(&value))
        {
            return Err(AdminError::invalid_argument(
                "patch",
                "Consumer limits are outside the Broker range",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchConsumerWorkspaceConfigRequest {
    pub broker_addr: String,
    pub group: String,
    pub expected_version: u64,
    pub patch: ConsumerWorkspaceConfigPatch,
}

impl PatchConsumerWorkspaceConfigRequest {
    pub fn try_new(
        broker_addr: impl Into<String>,
        group: impl Into<String>,
        expected_version: u64,
        patch: ConsumerWorkspaceConfigPatch,
    ) -> AdminResult<Self> {
        let broker_addr = broker_addr.into().trim().to_owned();
        let group = group.into().trim().to_owned();
        if broker_addr.is_empty() || group.is_empty() {
            return Err(AdminError::invalid_argument("identity", "must not be empty"));
        }
        Ok(Self {
            broker_addr,
            group,
            expected_version,
            patch: patch.validate()?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerConfigPersistence {
    Failed,
    Unconfirmed,
}

/// Versioned outcome keeping confirmed in-memory application separate from persistence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerWorkspaceConfigPatchResult {
    Applied {
        previous_version: u64,
        version: u64,
    },
    VersionConflict {
        expected_version: u64,
        actual_version: u64,
    },
    PersistenceIncomplete {
        /// Whether this command was confirmed applied in memory, independently of durability.
        applied: bool,
        previous_version: u64,
        version: u64,
        persistence: ConsumerConfigPersistence,
    },
}

pub trait ConsumerWorkspaceMutationAdmin: Send {
    fn patch_consumer_workspace_config<'a>(
        &'a mut self,
        request: &'a PatchConsumerWorkspaceConfigRequest,
    ) -> AdminFuture<'a, ConsumerWorkspaceConfigPatchResult>;
}
