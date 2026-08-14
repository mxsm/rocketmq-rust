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

use std::time::Duration;

use rocketmq_protocol::protocol::subscription::exponential_retry_policy::ExponentialRetryPolicy;
use rocketmq_protocol::protocol::subscription::group_retry_policy::GroupRetryPolicy;
use rocketmq_protocol::protocol::subscription::group_retry_policy_type::GroupRetryPolicyType;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_proxy_core::effective_settings;
use rocketmq_proxy_core::proto::v2;
use rocketmq_proxy_core::ServerSettingsPolicy;
use rocketmq_proxy_core::SettingsBackoffPolicy;
use rocketmq_proxy_core::SettingsPolicyValues;
use rocketmq_proxy_core::SubscriptionGroupMetadata;

fn duration(millis: u64) -> prost_types::Duration {
    prost_types::Duration {
        seconds: (millis / 1_000) as i64,
        nanos: ((millis % 1_000) * 1_000_000) as i32,
    }
}

#[test]
fn server_policy_overrides_client_attempts_batch_body_quota_and_fifo() {
    let client = v2::Settings {
        client_type: Some(v2::ClientType::LitePushConsumer as i32),
        backoff_policy: Some(v2::RetryPolicy {
            max_attempts: 999,
            strategy: Some(v2::retry_policy::Strategy::CustomizedBackoff(v2::CustomizedBackoff {
                next: vec![duration(1)],
            })),
        }),
        pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
            fifo: Some(false),
            receive_batch_size: Some(999),
            long_polling_timeout: Some(duration(120_000)),
            lite_subscription_quota: Some(99_999),
            max_lite_topic_size: Some(4_096),
            ..Default::default()
        })),
        ..Default::default()
    };
    let policy = ServerSettingsPolicy::new(
        7,
        SettingsPolicyValues {
            max_body_size: 4 * 1024 * 1024,
            validate_message_type: true,
            retry_max_attempts: 9,
            retry_backoff: SettingsBackoffPolicy::Exponential {
                initial: Duration::from_secs(5),
                max: Duration::from_secs(60),
                multiplier: 3,
            },
            receive_batch_size: 16,
            long_polling_timeout: Duration::from_secs(20),
            fifo: true,
            lite_subscription_quota: 23,
            max_lite_topic_size: 64,
        },
    );

    let effective = effective_settings(&client, &policy);
    let subscription = match effective.pub_sub.expect("subscription") {
        v2::settings::PubSub::Subscription(subscription) => subscription,
        _ => panic!("expected subscription settings"),
    };
    assert_eq!(subscription.fifo, Some(true));
    assert_eq!(subscription.receive_batch_size, Some(16));
    assert_eq!(subscription.long_polling_timeout, Some(duration(20_000)));
    assert_eq!(subscription.lite_subscription_quota, Some(23));
    assert_eq!(subscription.max_lite_topic_size, Some(64));
    let retry = effective.backoff_policy.expect("retry policy");
    assert_eq!(retry.max_attempts, 9);
    assert!(matches!(
        retry.strategy,
        Some(v2::retry_policy::Strategy::ExponentialBackoff(_))
    ));
    assert_eq!(policy.generation(), 7);
}

#[test]
fn publishing_policy_overrides_client_body_validation_and_retry() {
    let client = v2::Settings {
        backoff_policy: Some(v2::RetryPolicy {
            max_attempts: 200,
            strategy: None,
        }),
        pub_sub: Some(v2::settings::PubSub::Publishing(v2::Publishing {
            max_body_size: i32::MAX,
            validate_message_type: false,
            ..Default::default()
        })),
        ..Default::default()
    };
    let policy = ServerSettingsPolicy::new(
        3,
        SettingsPolicyValues {
            max_body_size: 1024,
            validate_message_type: true,
            retry_max_attempts: 3,
            retry_backoff: SettingsBackoffPolicy::Customized {
                next: vec![Duration::from_millis(10), Duration::from_millis(20)],
            },
            ..SettingsPolicyValues::default()
        },
    );

    let effective = effective_settings(&client, &policy);
    let publishing = match effective.pub_sub.expect("publishing") {
        v2::settings::PubSub::Publishing(publishing) => publishing,
        _ => panic!("expected publishing settings"),
    };
    assert_eq!(publishing.max_body_size, 1024);
    assert!(publishing.validate_message_type);
    assert_eq!(effective.backoff_policy.expect("retry").max_attempts, 3);
}

#[test]
fn field_ownership_inventory_has_unique_wire_fields() {
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../settings-field-ownership.json")).expect("valid ownership inventory");
    let fields = inventory["fields"].as_array().expect("fields array");
    assert!(fields.len() >= 10, "all authoritative settings fields must be owned");
    let mut wire_fields = std::collections::BTreeSet::new();
    for field in fields {
        let wire = field["wire"].as_str().expect("wire field");
        assert!(wire_fields.insert(wire), "duplicate wire field {wire}");
        assert!(field["owner"].is_string());
        assert!(field["merge"].is_string());
        assert!(field["consumers"].as_array().is_some_and(|value| !value.is_empty()));
    }
}

#[test]
fn shared_group_conversion_keeps_local_and_cluster_policy_inputs_identical() {
    let mut retry = GroupRetryPolicy::default();
    retry.set_type_(GroupRetryPolicyType::Exponential);
    retry.set_exponential_retry_policy(Some(ExponentialRetryPolicy::new(2_000, 30_000, 3)));
    let mut group = SubscriptionGroupConfig::default();
    group.set_consume_message_orderly(true);
    group.set_retry_max_times(8);
    group.set_group_retry_policy(retry);

    let metadata = SubscriptionGroupMetadata::from(&group);
    assert!(metadata.consume_message_orderly);
    assert_eq!(metadata.retry_max_times, 8);
    assert!(matches!(
        metadata.retry_backoff,
        SettingsBackoffPolicy::Exponential {
            initial,
            max,
            multiplier: 3
        } if initial == Duration::from_secs(2) && max == Duration::from_secs(30)
    ));
}

#[test]
fn customized_consumer_backoff_preserves_java_reconsume_offset() {
    let policy = SettingsBackoffPolicy::Customized {
        next: vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(10),
            Duration::from_secs(30),
        ],
    };

    assert_eq!(policy.delay_for_attempt(0), Duration::from_secs(10));
    assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(30));
    assert_eq!(policy.delay_for_attempt(20), Duration::from_secs(30));
}
