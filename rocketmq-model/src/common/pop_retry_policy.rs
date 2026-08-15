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
use thiserror::Error;

use super::key_builder::KeyBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PopRetryTopicVersion {
    V1,
    V2,
}

impl PopRetryTopicVersion {
    pub const fn number(self) -> i32 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    pub const fn from_number(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PopRetryMigrationState {
    V1Only,
    DualReadV1Write,
    DualReadV2Write,
    V2Only,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PopRetryPolicy {
    pub write_version: PopRetryTopicVersion,
    pub read_fallback_order: Vec<PopRetryTopicVersion>,
    pub accept_v1: bool,
    pub accept_v2: bool,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PopRetryPolicyError {
    #[error("POP retry policy does not describe one of the four supported migration states")]
    InvalidState,
    #[error("POP retry policy transition from {from:?} to {to:?} is not safe")]
    UnsafeTransition {
        from: PopRetryMigrationState,
        to: PopRetryMigrationState,
    },
    #[error("POP retry policy generation must advance beyond {current}, got {next}")]
    StaleGeneration { current: u64, next: u64 },
}

impl PopRetryPolicy {
    pub fn v1_only(generation: u64) -> Self {
        Self::new(
            PopRetryTopicVersion::V1,
            vec![PopRetryTopicVersion::V1],
            true,
            false,
            generation,
        )
    }

    pub fn dual_read_v1_write(generation: u64) -> Self {
        Self::new(
            PopRetryTopicVersion::V1,
            vec![PopRetryTopicVersion::V1, PopRetryTopicVersion::V2],
            true,
            true,
            generation,
        )
    }

    pub fn dual_read_v2_write(generation: u64) -> Self {
        Self::new(
            PopRetryTopicVersion::V2,
            vec![PopRetryTopicVersion::V2, PopRetryTopicVersion::V1],
            true,
            true,
            generation,
        )
    }

    pub fn v2_only(generation: u64) -> Self {
        Self::new(
            PopRetryTopicVersion::V2,
            vec![PopRetryTopicVersion::V2],
            false,
            true,
            generation,
        )
    }

    pub fn from_legacy_flags(enable_v2: bool, retrieve_v1: bool, generation: u64) -> Self {
        match (enable_v2, retrieve_v1) {
            (false, _) => Self::v1_only(generation),
            (true, true) => Self::dual_read_v2_write(generation),
            (true, false) => Self::v2_only(generation),
        }
    }

    pub fn state(&self) -> Result<PopRetryMigrationState, PopRetryPolicyError> {
        let state = match (
            self.write_version,
            self.read_fallback_order.as_slice(),
            self.accept_v1,
            self.accept_v2,
        ) {
            (PopRetryTopicVersion::V1, [PopRetryTopicVersion::V1], true, false) => PopRetryMigrationState::V1Only,
            (PopRetryTopicVersion::V1, [PopRetryTopicVersion::V1, PopRetryTopicVersion::V2], true, true) => {
                PopRetryMigrationState::DualReadV1Write
            }
            (PopRetryTopicVersion::V2, [PopRetryTopicVersion::V2, PopRetryTopicVersion::V1], true, true) => {
                PopRetryMigrationState::DualReadV2Write
            }
            (PopRetryTopicVersion::V2, [PopRetryTopicVersion::V2], false, true) => PopRetryMigrationState::V2Only,
            _ => return Err(PopRetryPolicyError::InvalidState),
        };
        Ok(state)
    }

    pub fn transition_to(&self, next: PopRetryMigrationState, generation: u64) -> Result<Self, PopRetryPolicyError> {
        if generation <= self.generation {
            return Err(PopRetryPolicyError::StaleGeneration {
                current: self.generation,
                next: generation,
            });
        }
        let current = self.state()?;
        let allowed = matches!(
            (current, next),
            (PopRetryMigrationState::V1Only, PopRetryMigrationState::DualReadV1Write)
                | (
                    PopRetryMigrationState::DualReadV1Write,
                    PopRetryMigrationState::DualReadV2Write
                )
                | (PopRetryMigrationState::DualReadV2Write, PopRetryMigrationState::V2Only)
                | (
                    PopRetryMigrationState::DualReadV2Write,
                    PopRetryMigrationState::DualReadV1Write
                )
        );
        if !allowed {
            return Err(PopRetryPolicyError::UnsafeTransition {
                from: current,
                to: next,
            });
        }
        Ok(Self::for_state(next, generation))
    }

    pub fn write_topic(&self, topic: &str, consumer_group: &str) -> String {
        KeyBuilder::build_pop_retry_topic_for_version(topic, consumer_group, self.write_version)
    }

    pub fn read_topics(&self, topic: &str, consumer_group: &str) -> Vec<String> {
        self.read_fallback_order
            .iter()
            .map(|version| KeyBuilder::build_pop_retry_topic_for_version(topic, consumer_group, *version))
            .collect()
    }

    pub const fn accepts(&self, version: PopRetryTopicVersion) -> bool {
        match version {
            PopRetryTopicVersion::V1 => self.accept_v1,
            PopRetryTopicVersion::V2 => self.accept_v2,
        }
    }

    pub fn for_state(state: PopRetryMigrationState, generation: u64) -> Self {
        match state {
            PopRetryMigrationState::V1Only => Self::v1_only(generation),
            PopRetryMigrationState::DualReadV1Write => Self::dual_read_v1_write(generation),
            PopRetryMigrationState::DualReadV2Write => Self::dual_read_v2_write(generation),
            PopRetryMigrationState::V2Only => Self::v2_only(generation),
        }
    }

    fn new(
        write_version: PopRetryTopicVersion,
        read_fallback_order: Vec<PopRetryTopicVersion>,
        accept_v1: bool,
        accept_v2: bool,
        generation: u64,
    ) -> Self {
        Self {
            write_version,
            read_fallback_order,
            accept_v1,
            accept_v2,
            generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_sequence_and_dual_reader_rollback_are_explicit() {
        let v1 = PopRetryPolicy::v1_only(1);
        let dual_v1 = v1
            .transition_to(PopRetryMigrationState::DualReadV1Write, 2)
            .expect("v1 should enter dual-read before switching writes");
        let dual_v2 = dual_v1
            .transition_to(PopRetryMigrationState::DualReadV2Write, 3)
            .expect("dual reader should switch writes to v2");
        let rolled_back = dual_v2
            .transition_to(PopRetryMigrationState::DualReadV1Write, 4)
            .expect("dual reader should roll writes back to v1");

        assert_eq!(rolled_back.state(), Ok(PopRetryMigrationState::DualReadV1Write));
        assert!(rolled_back.accepts(PopRetryTopicVersion::V2));
        assert_eq!(rolled_back.write_version, PopRetryTopicVersion::V1);
    }

    #[test]
    fn policy_rejects_skipped_or_stale_transitions() {
        let v1 = PopRetryPolicy::v1_only(4);
        assert!(matches!(
            v1.transition_to(PopRetryMigrationState::DualReadV1Write, 4),
            Err(PopRetryPolicyError::StaleGeneration { .. })
        ));
        assert!(matches!(
            v1.transition_to(PopRetryMigrationState::DualReadV2Write, 5),
            Err(PopRetryPolicyError::UnsafeTransition { .. })
        ));
    }

    #[test]
    fn dual_read_topics_follow_write_first_fallback_order() {
        let policy = PopRetryPolicy::dual_read_v2_write(9);
        assert_eq!(
            policy.read_topics("orders", "consumer-a"),
            vec!["%RETRY%consumer-a+orders", "%RETRY%consumer-a_orders"]
        );
        assert_eq!(policy.write_topic("orders", "consumer-a"), "%RETRY%consumer-a+orders");
    }
}
