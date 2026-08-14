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

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::contracts::ProxyServiceFuture;
use crate::proto::v2;
use crate::ProxyContext;

/// Proxy-owned defaults used to construct authoritative client settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct SettingsConfig {
    pub validate_message_type: bool,
    pub producer_max_attempts: i32,
    pub producer_backoff_initial_ms: u64,
    pub producer_backoff_max_ms: u64,
    pub producer_backoff_multiplier: u64,
    pub consumer_receive_batch_size: i32,
    pub max_lite_topic_size: i32,
}

impl Default for SettingsConfig {
    fn default() -> Self {
        Self {
            validate_message_type: true,
            producer_max_attempts: 3,
            producer_backoff_initial_ms: 10,
            producer_backoff_max_ms: 1_000,
            producer_backoff_multiplier: 2,
            consumer_receive_batch_size: 32,
            max_lite_topic_size: 64,
        }
    }
}

/// Retry strategy captured in one immutable settings generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsBackoffPolicy {
    Exponential {
        initial: Duration,
        max: Duration,
        multiplier: u64,
    },
    Customized {
        next: Vec<Duration>,
    },
}

impl SettingsBackoffPolicy {
    pub fn delay_for_attempt(&self, attempt: i32) -> Duration {
        match self {
            Self::Exponential {
                initial,
                max,
                multiplier,
            } => {
                let exponent = u32::try_from(attempt.max(0)).unwrap_or(u32::MAX).min(32);
                let factor = multiplier.saturating_pow(exponent);
                initial
                    .saturating_mul(u32::try_from(factor).unwrap_or(u32::MAX))
                    .min(*max)
            }
            Self::Customized { next } => {
                if next.is_empty() {
                    return Duration::ZERO;
                }
                let index = usize::try_from(attempt.max(0))
                    .unwrap_or(usize::MAX)
                    .saturating_add(2)
                    .min(next.len() - 1);
                next[index]
            }
        }
    }

    fn to_proto(&self) -> v2::retry_policy::Strategy {
        match self {
            Self::Exponential {
                initial,
                max,
                multiplier,
            } => v2::retry_policy::Strategy::ExponentialBackoff(v2::ExponentialBackoff {
                initial: Some(duration_to_proto(*initial)),
                max: Some(duration_to_proto(*max)),
                multiplier: *multiplier as f32,
            }),
            Self::Customized { next } => v2::retry_policy::Strategy::CustomizedBackoff(v2::CustomizedBackoff {
                next: next.iter().copied().map(duration_to_proto).collect(),
            }),
        }
    }
}

impl Default for SettingsBackoffPolicy {
    fn default() -> Self {
        Self::Customized {
            next: default_consumer_backoff(),
        }
    }
}

/// User-visible values owned by one server policy snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsPolicyValues {
    pub max_body_size: i32,
    pub validate_message_type: bool,
    pub retry_max_attempts: i32,
    pub retry_backoff: SettingsBackoffPolicy,
    pub receive_batch_size: i32,
    pub long_polling_timeout: Duration,
    pub fifo: bool,
    pub lite_subscription_quota: i32,
    pub max_lite_topic_size: i32,
}

impl Default for SettingsPolicyValues {
    fn default() -> Self {
        Self {
            max_body_size: 4 * 1024 * 1024,
            validate_message_type: true,
            retry_max_attempts: 17,
            retry_backoff: SettingsBackoffPolicy::default(),
            receive_batch_size: 32,
            long_polling_timeout: Duration::from_secs(20),
            fifo: false,
            lite_subscription_quota: 2_000,
            max_lite_topic_size: 64,
        }
    }
}

/// Immutable settings snapshot selected once for an inbound telemetry command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSettingsPolicy {
    generation: u64,
    values: SettingsPolicyValues,
}

impl ServerSettingsPolicy {
    pub fn new(generation: u64, values: SettingsPolicyValues) -> Self {
        Self { generation, values }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn values(&self) -> &SettingsPolicyValues {
        &self.values
    }

    pub fn has_same_values(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

/// Resolves one versioned policy for the complete lifetime of a request.
pub trait SettingsPolicyProvider: Send + Sync {
    fn policy_for<'a>(
        &'a self,
        context: &'a ProxyContext,
        client_settings: &'a v2::Settings,
    ) -> ProxyServiceFuture<'a, Arc<ServerSettingsPolicy>>;
}

/// Applies all server-owned values in one place while preserving client-owned identity and subscriptions.
pub fn effective_settings(client: &v2::Settings, policy: &ServerSettingsPolicy) -> v2::Settings {
    let mut effective = client.clone();
    let values = policy.values();
    match effective.pub_sub.as_mut() {
        Some(v2::settings::PubSub::Publishing(publishing)) => {
            publishing.max_body_size = values.max_body_size;
            publishing.validate_message_type = values.validate_message_type;
        }
        Some(v2::settings::PubSub::Subscription(subscription)) => {
            subscription.fifo = Some(values.fifo);
            subscription.receive_batch_size = Some(values.receive_batch_size);
            subscription.long_polling_timeout = Some(duration_to_proto(values.long_polling_timeout));
            if is_lite_client(effective.client_type) {
                subscription.lite_subscription_quota = Some(values.lite_subscription_quota);
                subscription.max_lite_topic_size = Some(values.max_lite_topic_size);
            } else {
                subscription.lite_subscription_quota = None;
                subscription.max_lite_topic_size = None;
            }
        }
        None => return effective,
    }
    effective.backoff_policy = Some(v2::RetryPolicy {
        max_attempts: values.retry_max_attempts,
        strategy: Some(values.retry_backoff.to_proto()),
    });
    effective
}

pub fn default_consumer_backoff() -> Vec<Duration> {
    [
        1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000, 480_000, 540_000,
        600_000, 1_200_000, 1_800_000, 3_600_000, 7_200_000,
    ]
    .into_iter()
    .map(Duration::from_millis)
    .collect()
}

fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs().try_into().unwrap_or(i64::MAX),
        nanos: duration.subsec_nanos().try_into().unwrap_or_default(),
    }
}

fn is_lite_client(client_type: Option<i32>) -> bool {
    matches!(
        client_type.and_then(|value| v2::ClientType::try_from(value).ok()),
        Some(v2::ClientType::LitePushConsumer | v2::ClientType::LiteSimpleConsumer)
    )
}
