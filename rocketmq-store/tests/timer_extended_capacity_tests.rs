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

#![cfg(feature = "extended_timeline")]

use rocketmq_store::TimerStoreConfig;
use rocketmq_store_api::StoreContractViolation;

#[test]
fn timer_capacity_configuration_supports_a_year_but_fails_closed_outside_bounds() {
    for days in [180, 366, 400] {
        let config = TimerStoreConfig {
            horizon_days: days,
            ..TimerStoreConfig::default()
        };
        assert!(config.validate().is_ok(), "{days}-day format horizon");
    }

    for days in [179, 401] {
        let config = TimerStoreConfig {
            horizon_days: days,
            ..TimerStoreConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(StoreContractViolation::TimerConfigurationOutOfRange {
                field: "horizonDays",
                actual: i128::from(days),
                minimum: 180,
                maximum: 400,
            })
        );
    }
}

#[test]
fn byte_and_bucket_quotas_cannot_be_disabled_or_exceed_the_global_budget() {
    let defaults = TimerStoreConfig::default();
    let config = TimerStoreConfig {
        max_topic_pending_bytes: defaults.max_pending_bytes.saturating_add(1),
        ..defaults
    };
    assert_eq!(
        config.validate(),
        Err(StoreContractViolation::TimerConfigurationOutOfRange {
            field: "maxTopicPendingBytes",
            actual: i128::from(config.max_topic_pending_bytes),
            minimum: 1,
            maximum: i128::from(config.max_pending_bytes),
        })
    );

    let config = TimerStoreConfig {
        max_bucket_messages: 0,
        ..TimerStoreConfig::default()
    };
    assert_eq!(
        config.validate(),
        Err(StoreContractViolation::TimerConfigurationOutOfRange {
            field: "maxBucketMessages",
            actual: 0,
            minimum: 1,
            maximum: i128::MAX,
        })
    );

    let config = TimerStoreConfig {
        minimum_free_ratio_basis_points: 10_000,
        ..TimerStoreConfig::default()
    };
    assert_eq!(
        config.validate(),
        Err(StoreContractViolation::TimerConfigurationOutOfRange {
            field: "minimumFreeRatioBasisPoints",
            actual: 10_000,
            minimum: 1,
            maximum: 9_999,
        })
    );
}
