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

#![cfg(all(feature = "nameserver-dns-discovery", feature = "test-support"))]

use rocketmq_client_rust::test_support::run_nameserver_discovery_lifecycle_probe;

mod support;

#[tokio::test]
async fn dns_discovery_initializes_and_cancels_inflight_refresh_without_leaking_tasks() {
    let runtime = support::client_runtime("nameserver-dns-discovery");
    let probe = run_nameserver_discovery_lifecycle_probe(runtime.component("discovery")).await;

    assert_eq!(probe.initial_generation, 1);
    assert!(probe.initial_fresh);
    assert_eq!(probe.initial_task_count, 1);
    assert_eq!(probe.publish_count, 1);
    assert_eq!(probe.task_count_after_shutdown, 0);
}
