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

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::subscription::subscription_group_config::{
    validate_subscription_group_configs, validate_subscription_group_name, SubscriptionGroupConfig,
    SUBSCRIPTION_GROUP_NAME_MAX_LENGTH,
};
use rocketmq_protocol::ProtocolContractViolation;

#[test]
fn java_compatible_group_name_rules_are_shared() {
    assert_eq!(
        validate_subscription_group_name("   "),
        Err(ProtocolContractViolation::BlankSubscriptionGroup)
    );
    assert_eq!(
        validate_subscription_group_name("invalid.group"),
        Err(ProtocolContractViolation::SubscriptionGroupIllegalCharacters)
    );
    assert_eq!(
        validate_subscription_group_name(&"a".repeat(SUBSCRIPTION_GROUP_NAME_MAX_LENGTH + 1)),
        Err(ProtocolContractViolation::SubscriptionGroupTooLong {
            max_length: SUBSCRIPTION_GROUP_NAME_MAX_LENGTH,
        })
    );
    assert!(validate_subscription_group_name("valid_%|group-1").is_ok());
}

#[test]
fn batch_validation_rejects_the_entire_list_before_mutation() {
    let configs = vec![
        SubscriptionGroupConfig::new(CheetahString::from_static_str("valid-group")),
        SubscriptionGroupConfig::new(CheetahString::from_static_str("invalid.group")),
    ];
    assert_eq!(
        validate_subscription_group_configs(&configs),
        Err(ProtocolContractViolation::SubscriptionGroupIllegalCharacters)
    );

    let duplicate = vec![
        SubscriptionGroupConfig::new(CheetahString::from_static_str("same-group")),
        SubscriptionGroupConfig::new(CheetahString::from_static_str("same-group")),
    ];
    assert_eq!(
        validate_subscription_group_configs(&duplicate),
        Err(ProtocolContractViolation::DuplicateSubscriptionGroup)
    );
}
