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

#![recursion_limit = "256"]

use rocketmq_client_rust::ClientConfig;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQClientException;

#[test]
fn client_consumers_use_only_intentional_root_exports() {
    let source = include_str!("../src/lib.rs");
    for module in [
        "admin",
        "base",
        "common",
        "config_support",
        "consumer",
        "exception",
        "factory",
        "hook",
        "implementation",
        "latency",
        "legacy",
        "lock",
        "producer",
        "runtime",
        "stat",
        "trace",
        "types",
        "utils",
    ] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-client-rust` implementation module `{module}` must remain private"
        );
    }

    let _ = ClientConfig::default();
    let _ = ClientRuntimeConfig::default();
    let _: Option<DefaultMQProducer> = None;
    let _: Option<DefaultMQPushConsumer> = None;
    let _: Option<MQClientException> = None;
}
