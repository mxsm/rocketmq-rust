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

use std::collections::HashMap;

use cheetah_string::CheetahString;

use super::error::BrokerConfigError;
use super::validated::ConfigGeneration;
use super::validated::ValidatedBrokerConfig;

/// A fully validated candidate tied to the runtime generation it was based on.
#[derive(Debug)]
pub struct ConfigUpdateTransaction {
    expected_generation: ConfigGeneration,
    candidate: ValidatedBrokerConfig,
    patch: Option<RuntimeBrokerConfigPatch>,
}

/// The complete, closed set of Broker properties that can be changed without
/// restarting the process.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeBrokerConfigPatch {
    pub(crate) auto_create_topic_enable: Option<bool>,
    pub(crate) auto_create_subscription_group: Option<bool>,
    pub(crate) broker_permission: Option<u32>,
    pub(crate) default_topic_queue_nums: Option<u32>,
    pub(crate) message_index_enable: Option<bool>,
    pub(crate) trace_topic_enable: Option<bool>,
}

impl ConfigUpdateTransaction {
    pub fn from_broker_patch(
        expected_generation: ConfigGeneration,
        current: &ValidatedBrokerConfig,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<Self, BrokerConfigError> {
        let patch = RuntimeBrokerConfigPatch::parse(current, properties)?.only_changes(current);
        let mut broker = current.broker().clone();
        let mut store = current.store().clone();
        if let Some(value) = patch.auto_create_topic_enable {
            broker.auto_create_topic_enable = value;
        }
        if let Some(value) = patch.auto_create_subscription_group {
            broker.auto_create_subscription_group = value;
        }
        if let Some(value) = patch.broker_permission {
            broker.broker_permission = value;
        }
        if let Some(value) = patch.default_topic_queue_nums {
            broker.topic_queue_config.default_topic_queue_nums = value;
        }
        if let Some(value) = patch.message_index_enable {
            store.message_index_enable = value;
        }
        if let Some(value) = patch.trace_topic_enable {
            broker.trace_topic_enable = value;
        }

        let candidate = current.with_candidates(broker, store)?;
        Ok(Self {
            expected_generation,
            candidate,
            patch: Some(patch),
        })
    }

    pub(crate) fn replacement(expected_generation: ConfigGeneration, candidate: ValidatedBrokerConfig) -> Self {
        Self {
            expected_generation,
            candidate,
            patch: None,
        }
    }

    #[must_use]
    pub const fn expected_generation(&self) -> ConfigGeneration {
        self.expected_generation
    }

    pub(crate) fn into_candidate(self) -> ValidatedBrokerConfig {
        self.candidate
    }

    pub(crate) const fn patch(&self) -> Option<RuntimeBrokerConfigPatch> {
        self.patch
    }

    pub(crate) const fn candidate(&self) -> &ValidatedBrokerConfig {
        &self.candidate
    }
}

impl RuntimeBrokerConfigPatch {
    fn only_changes(mut self, current: &ValidatedBrokerConfig) -> Self {
        let broker = current.broker();
        let store = current.store();
        if self.auto_create_topic_enable == Some(broker.auto_create_topic_enable) {
            self.auto_create_topic_enable = None;
        }
        if self.auto_create_subscription_group == Some(broker.auto_create_subscription_group) {
            self.auto_create_subscription_group = None;
        }
        if self.broker_permission == Some(broker.broker_permission) {
            self.broker_permission = None;
        }
        if self.default_topic_queue_nums == Some(broker.topic_queue_config.default_topic_queue_nums) {
            self.default_topic_queue_nums = None;
        }
        if self.message_index_enable == Some(store.message_index_enable) {
            self.message_index_enable = None;
        }
        if self.trace_topic_enable == Some(broker.trace_topic_enable) {
            self.trace_topic_enable = None;
        }
        self
    }

    pub(crate) const fn affects_topic_registration(self) -> bool {
        self.auto_create_topic_enable.is_some()
            || self.broker_permission.is_some()
            || self.default_topic_queue_nums.is_some()
            || self.trace_topic_enable.is_some()
    }

    fn parse(
        current: &ValidatedBrokerConfig,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<Self, BrokerConfigError> {
        let broker_properties = current.broker().get_properties();
        let store_properties = current.store().get_properties();
        let mut patch = Self::default();
        let mut restart_required = Vec::new();
        let mut unsupported = Vec::new();

        for (key, value) in properties {
            match key.as_str() {
                "autoCreateTopicEnable" => patch.auto_create_topic_enable = Some(parse_bool(key, value)?),
                "autoCreateSubscriptionGroup" => {
                    patch.auto_create_subscription_group = Some(parse_bool(key, value)?);
                }
                "brokerPermission" => patch.broker_permission = Some(parse_broker_permission(key, value)?),
                "defaultTopicQueueNums" => {
                    patch.default_topic_queue_nums = Some(parse_default_topic_queue_nums(key, value)?);
                }
                "messageIndexEnable" => patch.message_index_enable = Some(parse_bool(key, value)?),
                "traceTopicEnable" => patch.trace_topic_enable = Some(parse_bool(key, value)?),
                key if broker_properties.contains_key(key) || store_properties.contains_key(key) => {
                    restart_required.push(key.to_owned());
                }
                _ => unsupported.push(key.to_string()),
            }
        }

        if !restart_required.is_empty() {
            return Err(BrokerConfigError::restart_required(restart_required));
        }
        if !unsupported.is_empty() {
            return Err(BrokerConfigError::unsupported_keys(unsupported));
        }
        Ok(patch)
    }
}

fn parse_bool(key: &CheetahString, value: &CheetahString) -> Result<bool, BrokerConfigError> {
    match value.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(BrokerConfigError::InvalidProperty {
            key: key.to_string(),
            value: value.to_string(),
            expected: "the canonical boolean `true` or `false`",
        }),
    }
}

fn parse_canonical_u32(
    key: &CheetahString,
    value: &CheetahString,
    expected: &'static str,
) -> Result<u32, BrokerConfigError> {
    let parsed = value.parse::<u32>().map_err(|_| BrokerConfigError::InvalidProperty {
        key: key.to_string(),
        value: value.to_string(),
        expected,
    })?;
    if parsed.to_string() != value.as_str() {
        return Err(BrokerConfigError::InvalidProperty {
            key: key.to_string(),
            value: value.to_string(),
            expected,
        });
    }
    Ok(parsed)
}

fn parse_broker_permission(key: &CheetahString, value: &CheetahString) -> Result<u32, BrokerConfigError> {
    const EXPECTED: &str = "a canonical integer from 1 through 7 with read or write permission";
    let parsed = parse_canonical_u32(key, value, EXPECTED)?;
    if !(1..=7).contains(&parsed) || parsed & 0b110 == 0 {
        return Err(BrokerConfigError::InvalidProperty {
            key: key.to_string(),
            value: value.to_string(),
            expected: EXPECTED,
        });
    }
    Ok(parsed)
}

fn parse_default_topic_queue_nums(key: &CheetahString, value: &CheetahString) -> Result<u32, BrokerConfigError> {
    const EXPECTED: &str = "a canonical integer from 1 through 128";
    let parsed = parse_canonical_u32(key, value, EXPECTED)?;
    if !(1..=128).contains(&parsed) {
        return Err(BrokerConfigError::InvalidProperty {
            key: key.to_string(),
            value: value.to_string(),
            expected: EXPECTED,
        });
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(entries: &[(&str, &str)]) -> HashMap<CheetahString, CheetahString> {
        entries
            .iter()
            .map(|(key, value)| (CheetahString::from(*key), CheetahString::from(*value)))
            .collect()
    }

    #[test]
    fn six_reviewed_properties_build_complete_typed_candidates() {
        let cases = [
            ("autoCreateTopicEnable", "false"),
            ("autoCreateSubscriptionGroup", "false"),
            ("brokerPermission", "4"),
            ("defaultTopicQueueNums", "16"),
            ("messageIndexEnable", "false"),
            ("traceTopicEnable", "true"),
        ];

        for (key, value) in cases {
            let current = ValidatedBrokerConfig::default();
            let transaction = ConfigUpdateTransaction::from_broker_patch(
                ConfigGeneration::INITIAL,
                &current,
                &properties(&[(key, value)]),
            )
            .unwrap_or_else(|error| panic!("{key} should be runtime-updatable: {error}"));
            let candidate = transaction.into_candidate();
            match key {
                "autoCreateTopicEnable" => assert!(!candidate.broker().auto_create_topic_enable),
                "autoCreateSubscriptionGroup" => {
                    assert!(!candidate.broker().auto_create_subscription_group);
                }
                "brokerPermission" => assert_eq!(candidate.broker().broker_permission, 4),
                "defaultTopicQueueNums" => {
                    assert_eq!(candidate.broker().topic_queue_config.default_topic_queue_nums, 16);
                }
                "messageIndexEnable" => assert!(!candidate.store().message_index_enable),
                "traceTopicEnable" => assert!(candidate.broker().trace_topic_enable),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn reviewed_properties_require_canonical_bounded_values() {
        let invalid = [
            ("autoCreateTopicEnable", "False"),
            ("autoCreateSubscriptionGroup", "TRUE"),
            ("messageIndexEnable", "1"),
            ("traceTopicEnable", " true"),
            ("brokerPermission", "0"),
            ("brokerPermission", "1"),
            ("brokerPermission", "08"),
            ("defaultTopicQueueNums", "0"),
            ("defaultTopicQueueNums", "008"),
            ("defaultTopicQueueNums", "129"),
        ];

        for (key, value) in invalid {
            let current = ValidatedBrokerConfig::default();
            let error = ConfigUpdateTransaction::from_broker_patch(
                ConfigGeneration::INITIAL,
                &current,
                &properties(&[(key, value)]),
            )
            .expect_err("non-canonical or out-of-range value must be rejected");
            assert!(
                matches!(error, BrokerConfigError::InvalidProperty { .. }),
                "{key}={value}"
            );
        }
    }

    #[test]
    fn mixed_invalid_patch_never_changes_the_source_generation() {
        let current = ValidatedBrokerConfig::default();
        let before_broker = current.broker().clone();
        let before_store = current.store().clone();
        let patch = properties(&[("autoCreateTopicEnable", "false"), ("defaultTopicQueueNums", "129")]);

        assert!(ConfigUpdateTransaction::from_broker_patch(ConfigGeneration::INITIAL, &current, &patch).is_err());
        assert_eq!(
            current.broker().auto_create_topic_enable,
            before_broker.auto_create_topic_enable
        );
        assert_eq!(
            current.broker().topic_queue_config.default_topic_queue_nums,
            before_broker.topic_queue_config.default_topic_queue_nums
        );
        assert_eq!(current.store().message_index_enable, before_store.message_index_enable);
    }

    #[test]
    fn all_other_known_properties_require_restart_and_unknown_keys_stay_unsupported() {
        let current = ValidatedBrokerConfig::default();
        let restart = ConfigUpdateTransaction::from_broker_patch(
            ConfigGeneration::INITIAL,
            &current,
            &properties(&[("maxClientEventCount", "101")]),
        )
        .expect_err("a known non-reviewed property must require restart");
        assert!(matches!(restart, BrokerConfigError::RestartRequired { .. }));

        let unsupported = ConfigUpdateTransaction::from_broker_patch(
            ConfigGeneration::INITIAL,
            &current,
            &properties(&[("arbitraryRuntimeSetting", "true")]),
        )
        .expect_err("an unknown property must remain unsupported");
        assert!(matches!(unsupported, BrokerConfigError::UnsupportedKeys { .. }));
    }
}
