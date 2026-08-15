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
pub struct ConfigUpdateTransaction {
    expected_generation: ConfigGeneration,
    candidate: ValidatedBrokerConfig,
}

impl ConfigUpdateTransaction {
    pub fn from_broker_patch(
        expected_generation: ConfigGeneration,
        current: &ValidatedBrokerConfig,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<Self, BrokerConfigError> {
        let mut broker = current.broker().clone();
        let broker_properties = current.broker().get_properties();
        let store_properties = current.store().get_properties();
        let mut restart_required = Vec::new();
        let mut unsupported = Vec::new();

        for (key, value) in properties {
            match key.as_str() {
                "enableLiteEventMode" => {
                    broker.enable_lite_event_mode = parse_bool(key, value)?;
                }
                "liteEventCheckInterval" => {
                    broker.lite_event_check_interval = parse_u64(key, value)?;
                }
                "liteTtlCheckInterval" => {
                    broker.lite_ttl_check_interval = parse_u64(key, value)?;
                }
                "liteSubscriptionCheckInterval" => {
                    broker.lite_subscription_check_interval = parse_u64(key, value)?;
                }
                "liteSubscriptionCheckTimeoutMills" => {
                    broker.lite_subscription_check_timeout_mills = parse_u64(key, value)?;
                }
                "maxLiteSubscriptionCount" => {
                    broker.max_lite_subscription_count = parse_positive_u64(key, value)?;
                }
                "enableLitePopLog" => {
                    broker.enable_lite_pop_log = parse_bool(key, value)?;
                }
                "maxClientEventCount" => {
                    broker.max_client_event_count = parse_positive_i32(key, value)?;
                }
                "liteEventFullDispatchDelayTime" => {
                    broker.lite_event_full_dispatch_delay_time = parse_u64(key, value)?;
                }
                "liteEventFullDispatchDelayTimeForWildcardGroup" => {
                    broker.lite_event_full_dispatch_delay_time_for_wildcard_group = parse_u64(key, value)?;
                }
                "liteLagLatencyCollectEnable" => {
                    broker.lite_lag_latency_collect_enable = parse_bool(key, value)?;
                }
                "liteLagLatencyMetricsEnable" => {
                    broker.lite_lag_latency_metrics_enable = parse_bool(key, value)?;
                }
                "liteLagCountMetricsEnable" => {
                    broker.lite_lag_count_metrics_enable = parse_bool(key, value)?;
                }
                "liteLagLatencyTopK" => {
                    broker.lite_lag_latency_top_k = parse_positive_i32(key, value)?;
                }
                "validateSystemTopicWhenUpdateTopic" => {
                    broker.validate_system_topic_when_update_topic = parse_bool(key, value)?;
                }
                "enableMixedMessageType" => {
                    broker.enable_mixed_message_type = parse_bool(key, value)?;
                }
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

        let candidate = current.with_broker_candidate(broker)?;
        Ok(Self {
            expected_generation,
            candidate,
        })
    }

    pub(crate) fn replacement(expected_generation: ConfigGeneration, candidate: ValidatedBrokerConfig) -> Self {
        Self {
            expected_generation,
            candidate,
        }
    }

    #[must_use]
    pub const fn expected_generation(&self) -> ConfigGeneration {
        self.expected_generation
    }

    pub(crate) fn into_candidate(self) -> ValidatedBrokerConfig {
        self.candidate
    }
}

fn parse_bool(key: &CheetahString, value: &CheetahString) -> Result<bool, BrokerConfigError> {
    value.parse::<bool>().map_err(|_| BrokerConfigError::InvalidProperty {
        key: key.to_string(),
        value: value.to_string(),
        expected: "a boolean",
    })
}

fn parse_u64(key: &CheetahString, value: &CheetahString) -> Result<u64, BrokerConfigError> {
    value.parse::<u64>().map_err(|_| BrokerConfigError::InvalidProperty {
        key: key.to_string(),
        value: value.to_string(),
        expected: "an unsigned integer",
    })
}

fn parse_positive_u64(key: &CheetahString, value: &CheetahString) -> Result<u64, BrokerConfigError> {
    let parsed = parse_u64(key, value)?;
    if parsed == 0 {
        return Err(BrokerConfigError::InvalidProperty {
            key: key.to_string(),
            value: value.to_string(),
            expected: "a positive integer",
        });
    }
    Ok(parsed)
}

fn parse_positive_i32(key: &CheetahString, value: &CheetahString) -> Result<i32, BrokerConfigError> {
    let parsed = value.parse::<i32>().map_err(|_| BrokerConfigError::InvalidProperty {
        key: key.to_string(),
        value: value.to_string(),
        expected: "an integer",
    })?;
    if parsed <= 0 {
        return Err(BrokerConfigError::InvalidProperty {
            key: key.to_string(),
            value: value.to_string(),
            expected: "a positive integer",
        });
    }
    Ok(parsed)
}
