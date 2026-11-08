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

//! Typed editable Subscription Group values and safe, display-only configuration facts.

use super::{ConsumerConfiguration, ConsumerValidationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerConfigEntries {
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

impl Default for ConsumerConfigEntries {
    fn default() -> Self {
        Self {
            retry_max_times: 16,
            retry_queue_nums: 1,
            consume_timeout_minutes: 15,
            consume_enable: true,
            consume_from_min_enable: false,
            consume_broadcast_enable: false,
            consume_message_orderly: false,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
        }
    }
}

impl ConsumerConfigEntries {
    pub fn validate(self) -> Result<Self, ConsumerValidationError> {
        if self.retry_max_times < -1
            || self.retry_queue_nums > i32::MAX as u32
            || !(1..=i32::MAX as u32).contains(&self.consume_timeout_minutes)
        {
            return Err(ConsumerValidationError::InvalidConfigValue);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerConfigPersistence {
    Failed,
    Unconfirmed,
}

/// Only fields exposed as writable by the Consumer form. Omitted fields retain each Broker's value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumerConfigPatch {
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

impl ConsumerConfigPatch {
    pub fn validate(self) -> Result<Self, ConsumerValidationError> {
        if self == Self::default() {
            return Err(ConsumerValidationError::EmptyPatch);
        }
        let mut entries = ConsumerConfigEntries::default();
        self.apply_to(&mut entries);
        entries.validate()?;
        Ok(self)
    }

    pub fn apply_to(self, entries: &mut ConsumerConfigEntries) {
        macro_rules! apply { ($($field:ident),+ $(,)?) => { $(if let Some(value) = self.$field { entries.$field = value; })+ }; }
        apply!(
            retry_max_times,
            retry_queue_nums,
            consume_timeout_minutes,
            consume_enable,
            consume_from_min_enable,
            consume_broadcast_enable,
            consume_message_orderly,
            broker_id,
            which_broker_when_consume_slowly
        );
    }

    #[must_use]
    pub fn between(before: ConsumerConfigEntries, after: ConsumerConfigEntries) -> Self {
        macro_rules! changed {
            ($field:ident) => {
                (before.$field != after.$field).then_some(after.$field)
            };
        }
        Self {
            retry_max_times: changed!(retry_max_times),
            retry_queue_nums: changed!(retry_queue_nums),
            consume_timeout_minutes: changed!(consume_timeout_minutes),
            consume_enable: changed!(consume_enable),
            consume_from_min_enable: changed!(consume_from_min_enable),
            consume_broadcast_enable: changed!(consume_broadcast_enable),
            consume_message_orderly: changed!(consume_message_orderly),
            broker_id: changed!(broker_id),
            which_broker_when_consume_slowly: changed!(which_broker_when_consume_slowly),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
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
    /// The Broker selects exponential retry but supplies no exponential settings.
    DefaultFallback,
}

/// Read-only facts never become a write payload or an audit/configuration log value.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ConsumerConfigReadOnly {
    pub retry_policy: ConsumerRetryPolicy,
    pub subscribed_topics: Vec<String>,
    pub attributes: Vec<(String, String)>,
    pub truncated: bool,
}

impl std::fmt::Debug for ConsumerConfigReadOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerConfigReadOnly")
            .field("topic_count", &self.subscribed_topics.len())
            .field("attribute_count", &self.attributes.len())
            .field("truncated", &self.truncated)
            .finish_non_exhaustive()
    }
}

impl ConsumerConfiguration {
    /// Names of typed fields that differ among successful Broker observations. Failures remain separate.
    #[must_use]
    pub fn differing_fields(&self) -> Vec<&'static str> {
        let Some(first) = self.snapshots.first() else {
            return Vec::new();
        };
        let mut fields = Vec::new();
        macro_rules! compare { ($($field:ident),+ $(,)?) => { $(if self.snapshots.iter().skip(1).any(|snapshot|
            snapshot.entries.$field != first.entries.$field) { fields.push(stringify!($field)); })+ }; }
        compare!(
            retry_max_times,
            retry_queue_nums,
            consume_timeout_minutes,
            consume_enable,
            consume_from_min_enable,
            consume_broadcast_enable,
            consume_message_orderly,
            broker_id,
            which_broker_when_consume_slowly,
            notify_consumer_ids_changed_enable,
            group_sys_flag
        );
        if self
            .snapshots
            .iter()
            .skip(1)
            .any(|snapshot| snapshot.read_only.retry_policy != first.read_only.retry_policy)
        {
            fields.push("retry_policy");
        }
        if self
            .snapshots
            .iter()
            .skip(1)
            .any(|snapshot| snapshot.read_only.subscribed_topics != first.read_only.subscribed_topics)
        {
            fields.push("subscribed_topics");
        }
        if self
            .snapshots
            .iter()
            .skip(1)
            .any(|snapshot| snapshot.read_only.attributes != first.read_only.attributes)
        {
            fields.push("attributes");
        }
        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConsumerConfigIdentity, ConsumerConfigSnapshot, ConsumerIdentity, ConsumerObservationState,
        ConsumerTargetIdentity,
    };
    #[test]
    fn consumer_multifield_patch_decreases_retries_and_preserves_unselected_values() {
        let original = ConsumerConfigEntries {
            notify_consumer_ids_changed_enable: false,
            group_sys_flag: 37,
            ..Default::default()
        };
        let edited = ConsumerConfigEntries {
            retry_max_times: 3,
            retry_queue_nums: 0,
            consume_enable: false,
            broker_id: 9,
            ..original
        };
        let patch = ConsumerConfigPatch::between(original, edited).validate().unwrap();
        assert_eq!(patch.retry_max_times, Some(3));
        assert_eq!(patch.retry_queue_nums, Some(0));
        assert_eq!(patch.consume_from_min_enable, None);
        let mut applied = original;
        patch.apply_to(&mut applied);
        assert_eq!(applied, edited);
        assert!(ConsumerConfigPatch {
            retry_max_times: Some(-1),
            ..Default::default()
        }
        .validate()
        .is_ok());
        assert!(ConsumerConfigPatch {
            retry_max_times: Some(-2),
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    #[test]
    fn consumer_heterogeneous_configs_report_readonly_and_editable_differences_without_an_effective_default() {
        let group = ConsumerIdentity::parse("orders").unwrap();
        let first = ConsumerConfigSnapshot {
            identity: ConsumerConfigIdentity {
                group: group.clone(),
                target: ConsumerTargetIdentity::parse("cluster", "a", "a:10911").unwrap(),
            },
            generation: 1,
            entries: Default::default(),
            read_only: Default::default(),
        };
        let mut second = first.clone();
        second.identity.target = ConsumerTargetIdentity::parse("cluster", "b", "b:10911").unwrap();
        second.entries.retry_max_times = 4;
        second.read_only.subscribed_topics = vec!["orders".into()];
        second.read_only.retry_policy = ConsumerRetryPolicy::Exponential {
            initial_ms: 1000,
            maximum_ms: 5000,
            multiplier: 2,
        };
        let config = ConsumerConfiguration {
            group,
            snapshots: vec![first, second],
            observation: ConsumerObservationState::Partial,
            failures: Vec::new(),
        };
        assert_eq!(
            config.differing_fields(),
            ["retry_max_times", "retry_policy", "subscribed_topics"]
        );
        assert_eq!(config.observation, ConsumerObservationState::Partial);
    }
}
