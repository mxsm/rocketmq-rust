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

#![recursion_limit = "256"]

use rocketmq_client_rust::test_support::run_lite_pull_concurrency_contract_probe;

mod support;

#[tokio::test]
async fn ten_assignments_observe_the_configured_two_pull_rpc_limit() {
    let probe = run_lite_pull_concurrency_contract_probe(support::client_runtime("lite-pull-concurrency"), 2, 10).await;

    assert_eq!(probe.configured_limit, 2, "{probe:?}");
    assert_eq!(probe.queues, 10, "{probe:?}");
    assert_eq!(probe.peak_inflight, 2, "{probe:?}");
    assert!(probe.cancelled_waiter_released, "{probe:?}");
    assert!(probe.runtime_mutation_rejected, "{probe:?}");
}
