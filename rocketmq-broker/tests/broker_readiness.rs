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

use std::net::TcpListener;

use rocketmq_broker::Builder;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_runtime::RuntimeContext;
use rocketmq_store::config::message_store_config::MessageStoreConfig;

fn available_listener_pair() -> u16 {
    for _ in 0..100 {
        let normal = TcpListener::bind("127.0.0.1:0").expect("probe normal listener");
        let normal_port = normal.local_addr().expect("normal listener address").port();
        let Some(fast_port) = normal_port.checked_sub(2) else {
            continue;
        };
        if let Ok(fast) = TcpListener::bind(("127.0.0.1", fast_port)) {
            drop(fast);
            drop(normal);
            return normal_port;
        }
    }
    panic!("unable to find broker listener pair");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_state_is_created_only_with_complete_readiness_evidence() {
    let root = tempfile::tempdir().expect("create broker test root");
    let root_path = root.path().to_string_lossy().into_owned();
    let normal_port = available_listener_pair();
    let mut broker_config = BrokerConfig {
        broker_ip1: "127.0.0.1".into(),
        listen_port: normal_port as u32,
        store_path_root_dir: root_path.clone().into(),
        namesrv_addr: None,
        ..BrokerConfig::default()
    };
    broker_config.broker_server_config.bind_address = "127.0.0.1".to_owned();
    broker_config.broker_server_config.listen_port = normal_port as u32;
    let message_store_config = MessageStoreConfig {
        store_path_root_dir: root_path.into(),
        ..MessageStoreConfig::default()
    };
    let runtime_context = RuntimeContext::from_current("broker-readiness-test");
    let initialized = Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .set_service_context(runtime_context.service_context("broker-under-test"))
        .build()
        .initialize()
        .await
        .expect("broker initialization should succeed");

    let running = initialized.start().await.expect("broker should become ready");
    let readiness = running.readiness();
    assert!(readiness.store_writable());
    assert_eq!(
        readiness.normal_listener().map(|address| address.port()),
        Some(normal_port)
    );
    assert_eq!(
        readiness.fast_listener().map(|address| address.port()),
        Some(normal_port - 2)
    );
    assert!(readiness.processors_started());
    assert!(readiness.security_ready());
    assert!(readiness.registration_ready());

    running.shutdown().await;
}
