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

#[path = "../src/failover/escape_target_resolver.rs"]
mod escape_target_resolver;

use cheetah_string::CheetahString;
use escape_target_resolver::EscapeTargetError;
use escape_target_resolver::EscapeTargetPreference;
use escape_target_resolver::EscapeTargetResolver;
use rocketmq_model::common::message::message_queue::MessageQueue;

fn queue(broker: &'static str, queue_id: i32) -> MessageQueue {
    MessageQueue::from_parts("escape-topic", broker, queue_id)
}

#[test]
fn missing_and_self_only_routes_fail_closed_without_panicking() {
    let resolver = EscapeTargetResolver::default();
    let local = CheetahString::from_static_str("broker-a");

    assert_eq!(
        resolver.resolve(&[], &local, EscapeTargetPreference::Any),
        Err(EscapeTargetError::RouteUnavailable)
    );
    assert_eq!(
        resolver.resolve(&[queue("broker-a", 0)], &local, EscapeTargetPreference::Any),
        Err(EscapeTargetError::SelfOnlyRoute)
    );
}

#[test]
fn all_send_modes_share_remote_only_eligibility() {
    let resolver = EscapeTargetResolver::default();
    let local = CheetahString::from_static_str("broker-a");
    let remote = CheetahString::from_static_str("broker-b");
    let queues = [queue("broker-a", 0), queue("broker-b", 1), queue("broker-c", 2)];

    let sync = resolver
        .resolve(&queues, &local, EscapeTargetPreference::Any)
        .expect("sync target");
    let asynchronous = resolver
        .resolve(&queues, &local, EscapeTargetPreference::Any)
        .expect("async target");
    let requested = resolver
        .resolve(&queues, &local, EscapeTargetPreference::Broker(&remote))
        .expect("requested target");
    let stable = resolver
        .resolve(&queues, &local, EscapeTargetPreference::Stable(17))
        .expect("stable target");

    for selected in [&sync, &asynchronous, &requested, &stable] {
        assert_ne!(selected.broker_name(), &local);
    }
    assert_eq!(requested.broker_name(), &remote);
    assert_eq!(
        stable,
        resolver
            .resolve(&queues, &local, EscapeTargetPreference::Stable(17))
            .expect("repeat stable target")
    );
}

#[test]
fn requested_local_or_missing_broker_is_rejected() {
    let resolver = EscapeTargetResolver::default();
    let local = CheetahString::from_static_str("broker-a");
    let missing = CheetahString::from_static_str("broker-z");
    let queues = [queue("broker-a", 0), queue("broker-b", 1)];

    assert_eq!(
        resolver.resolve(&queues, &local, EscapeTargetPreference::Broker(&local)),
        Err(EscapeTargetError::SelfTarget)
    );
    assert_eq!(
        resolver.resolve(&queues, &local, EscapeTargetPreference::Broker(&missing)),
        Err(EscapeTargetError::RequestedBrokerUnavailable)
    );
}
