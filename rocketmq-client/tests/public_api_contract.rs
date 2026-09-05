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
use rocketmq_client_rust::ClientOptions;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQClientException;
use rocketmq_client_rust::NameServerDiscoveryConfig;
use rocketmq_client_rust::NameServerSource;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_client_rust::TransactionMQProducer;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

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
    let discovery =
        NameServerDiscoveryConfig::new(NameServerSource::dns("namesrv.default.svc", 9876).expect("public DNS source"));
    let options = ClientOptions::legacy(ClientConfig {
        namesrv_addr: None,
        ..Default::default()
    })
    .with_nameserver_discovery(discovery.clone());
    assert!(options.nameserver_discovery().is_some());

    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("public-api-contract"))
        .expect("test runtime configuration is valid")
        .build()
        .unwrap();
    let runtime = ClientRuntime::try_new(
        owner.root_context().component("client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .unwrap();
    let _ = DefaultMQProducer::builder(runtime.clone()).client_options(options.clone());
    let _ = TransactionMQProducer::builder(runtime.clone()).nameserver_discovery(discovery.clone());
    let _ = DefaultMQPushConsumer::builder(runtime.clone()).client_options(options.clone());
    let _ = DefaultLitePullConsumer::builder(runtime.clone()).nameserver_discovery(discovery);
    owner.block_on(async {
        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner.shutdown_runtime_blocking().unwrap();

    let _: Option<DefaultMQProducer> = None;
    let _: Option<DefaultMQPushConsumer> = None;
    let _: Option<MQClientException> = None;
}
