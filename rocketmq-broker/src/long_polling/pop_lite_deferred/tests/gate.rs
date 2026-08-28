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

use crate::long_polling::pop_lite_deferred::gate::PopLiteEventGate;

#[test]
fn pop_lite_deferred_gate_is_single_flight_per_client_and_parallel_across_clients() {
    let gate = PopLiteEventGate::default();
    let client_a = CheetahString::from_static_str("client-a");
    let client_b = CheetahString::from_static_str("client-b");
    let first = gate.try_reserve(&client_a).expect("first client-a gate");
    let other = gate.try_reserve(&client_b).expect("client-b gate is independent");

    assert!(gate.try_reserve(&client_a).is_none());
    assert_eq!(gate.active_count(), 2);
    drop(first);
    assert!(gate.try_reserve(&client_a).is_some());
    drop(other);
}
