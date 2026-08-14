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

use rocketmq_client_rust::test_support::run_lite_pull_suspend_timeout_probe;

#[test]
fn suspended_pull_uses_suspend_timeout_for_header_and_rpc() {
    let captured = run_lite_pull_suspend_timeout_probe(true, 20_000, 30_000, 10_000);

    assert_eq!(captured.header_suspend_timeout_millis, 20_000);
    assert_eq!(captured.invoke_timeout_millis, 30_000);
    assert!(captured.invoke_timeout_millis >= captured.header_suspend_timeout_millis);
}

#[test]
fn ordinary_pull_keeps_the_regular_rpc_timeout() {
    let captured = run_lite_pull_suspend_timeout_probe(false, 20_000, 30_000, 10_000);

    assert_eq!(captured.header_suspend_timeout_millis, 20_000);
    assert_eq!(captured.invoke_timeout_millis, 10_000);
}
