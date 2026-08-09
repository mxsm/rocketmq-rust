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

use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerPayloadLocator;
use rocketmq_store_api::TimerStoreMode;
use rocketmq_store_api::JAVA_COMPAT_TIMER_FORMAT_VERSION;

#[test]
fn java_compat_is_the_default_store_mode() {
    assert_eq!(TimerStoreMode::default(), TimerStoreMode::JavaCompat);
}

#[test]
fn engine_ids_are_stable_and_unknown_values_fail_closed() {
    assert_eq!(TimerEngineId::JavaCompat.as_str(), "F");
    assert_eq!(TimerEngineId::ExtendedTimeline.as_str(), "R");
    assert_eq!(TimerEngineId::parse("F"), Ok(TimerEngineId::JavaCompat));
    assert!(TimerEngineId::parse("future-engine").is_err());
}

#[test]
fn persisted_route_round_trips_without_recomputing_owner_or_token() {
    let route = PersistedTimerRoute::try_new(
        TimerEngineId::JavaCompat,
        JAVA_COMPAT_TIMER_FORMAT_VERSION,
        0x5a5a,
        TimerGeneration::new(7),
        "F:1:source:42:7",
    )
    .expect("valid route");
    let encoded = serde_json::to_vec(&route).expect("encode route");
    let decoded: PersistedTimerRoute = serde_json::from_slice(&encoded).expect("decode route");

    assert_eq!(decoded, route);
    assert_eq!(decoded.delivery_token(), "F:1:source:42:7");
}

#[test]
fn payload_locator_rejects_values_that_cannot_reference_a_record() {
    assert!(TimerPayloadLocator::try_new(-1, 1).is_err());
    assert!(TimerPayloadLocator::try_new(0, 0).is_err());
    assert_eq!(
        TimerPayloadLocator::try_new(9, 17)
            .expect("valid locator")
            .commit_log_offset(),
        9
    );
}
