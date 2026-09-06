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

use std::collections::TryReserveError;
use std::error::Error;
use std::num::NonZeroUsize;
use std::time::Duration;

use cheetah_string::CheetahString;

use crate::config::broker_config::BrokerConfig;
use crate::long_polling::pop_lite_deferred::data::PopLiteDeferredPolicy;
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadline;
use crate::long_polling::pop_lite_deferred::deadline::PopLiteWaitDeadlineOutcome;
use crate::long_polling::pop_lite_deferred::index::PopLiteCriteriaIndex;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexLimits;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexOperationalError;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexReserveOutcome;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexReserveRejection;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test limit is non-zero")
}

macro_rules! expect_reserved {
    ($result:expr, $message:literal) => {
        match $result {
            Ok(PopLiteIndexReserveOutcome::Reserved(reservation)) => reservation,
            Ok(PopLiteIndexReserveOutcome::Rejected(rejection)) => panic!("{}: {rejection:?}", $message),
            Err(error) => panic!("{}: {error:?}", $message),
        }
    };
}

fn deadline(base: tokio::time::Instant, end_millis: i64) -> PopLiteWaitDeadline {
    match PopLiteWaitDeadline::checked(0, end_millis + 49, 0, base, Duration::from_secs(300))
        .expect("test deadline should not overflow")
    {
        PopLiteWaitDeadlineOutcome::Pending(deadline) => deadline,
        PopLiteWaitDeadlineOutcome::Rejected(_) => panic!("test deadline should be pending"),
    }
}

#[test]
fn pop_lite_deferred_index_selects_earliest_expiry_then_oldest_sequence() {
    let base = tokio::time::Instant::now();
    let index = PopLiteCriteriaIndex::<u64>::new(PopLiteIndexLimits::new(nonzero(4), nonzero(2), nonzero(4)));
    let client = CheetahString::from_static_str("client-a");
    let late =
        expect_reserved!(index.reserve(client.clone(), base), "late reservation").publish(1, deadline(base, 200));
    let first_equal = expect_reserved!(index.reserve(client.clone(), base), "first equal reservation")
        .publish(2, deadline(base, 100));
    let second_equal = expect_reserved!(index.reserve(client.clone(), base), "second equal reservation")
        .publish(3, deadline(base, 100));

    let first = index.reserve_oldest(&client).expect("earliest candidate");
    assert_eq!(first.id(), 2);
    drop(first);
    let first_again = index
        .reserve_oldest(&client)
        .expect("released candidate restores order");
    assert_eq!(first_again.id(), 2);
    drop((first_again, late, first_equal, second_equal));
    assert_eq!(index.snapshot().live, 0);
    assert_eq!(index.snapshot().clients, 0);
}

#[test]
fn pop_lite_deferred_index_maps_global_client_and_per_client_capacities() {
    let base = tokio::time::Instant::now();
    let client_a = CheetahString::from_static_str("client-a");
    let client_b = CheetahString::from_static_str("client-b");
    let index = PopLiteCriteriaIndex::<u64>::new(PopLiteIndexLimits::new(nonzero(2), nonzero(1), nonzero(1)));
    let first = expect_reserved!(index.reserve(client_a.clone(), base), "first reservation");

    assert_eq!(
        match index.reserve(client_a, base) {
            Ok(PopLiteIndexReserveOutcome::Rejected(rejection)) => rejection,
            Ok(PopLiteIndexReserveOutcome::Reserved(_)) | Err(_) => panic!("per-client full"),
        },
        PopLiteIndexReserveRejection::PerClient
    );
    assert_eq!(
        match index.reserve(client_b, base) {
            Ok(PopLiteIndexReserveOutcome::Rejected(rejection)) => rejection,
            Ok(PopLiteIndexReserveOutcome::Reserved(_)) | Err(_) => panic!("distinct client full"),
        },
        PopLiteIndexReserveRejection::Client
    );
    drop(first);
    assert_eq!(index.snapshot().reserved, 0);
    assert_eq!(index.snapshot().clients, 0);
}

#[test]
fn pop_lite_index_allocation_failure_keeps_the_typed_source() {
    let source = Vec::<u8>::new()
        .try_reserve(usize::MAX)
        .expect_err("unrepresentable allocation must fail");
    let error = PopLiteIndexOperationalError::Allocation(source);

    assert!(error
        .source()
        .and_then(|source| source.downcast_ref::<TryReserveError>())
        .is_some());
}

#[test]
fn pop_lite_deferred_policy_preserves_legacy_capacity_mapping() {
    let config = BrokerConfig {
        max_pop_polling_size: 17,
        pop_polling_map_size: 5,
        pop_polling_size: 3,
        ..Default::default()
    };
    let policy = PopLiteDeferredPolicy::from_config(&config).expect("positive legacy limits");

    assert_eq!(policy.index_limits.max_entries.get(), 17);
    assert_eq!(policy.index_limits.max_clients.get(), 5);
    assert_eq!(policy.index_limits.max_entries_per_client.get(), 3);
    assert_eq!(policy.max_age, Duration::from_secs(30));
}
