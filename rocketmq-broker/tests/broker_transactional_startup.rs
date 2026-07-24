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

use rocketmq_broker::BrokerStartupError;
use rocketmq_broker::Builder;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_runtime::RuntimeContext;
use rocketmq_store::config::message_store_config::MessageStoreConfig;

static BROKER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn reserve_listener_pair() -> (TcpListener, u16) {
    for _ in 0..100 {
        let normal = TcpListener::bind("127.0.0.1:0").expect("reserve normal listener");
        let normal_port = normal.local_addr().expect("reserved listener address").port();
        let Some(fast_port) = normal_port.checked_sub(2) else {
            continue;
        };
        if let Ok(fast) = TcpListener::bind(("127.0.0.1", fast_port)) {
            drop(fast);
            return (normal, normal_port);
        }
    }
    panic!("unable to reserve broker listener pair");
}

fn broker_configs(root: &std::path::Path, normal_port: u16) -> (BrokerConfig, MessageStoreConfig) {
    let root = root.to_string_lossy().into_owned();
    let mut broker_config = BrokerConfig {
        broker_ip1: "127.0.0.1".into(),
        listen_port: normal_port as u32,
        store_path_root_dir: root.clone().into(),
        namesrv_addr: None,
        ..BrokerConfig::default()
    };
    broker_config.broker_server_config.bind_address = "127.0.0.1".to_owned();
    broker_config.broker_server_config.listen_port = normal_port as u32;
    let message_store_config = MessageStoreConfig {
        store_path_root_dir: root.into(),
        ..MessageStoreConfig::default()
    };
    (broker_config, message_store_config)
}

#[tokio::test]
async fn invalid_broker_address_returns_typed_initialization_error() {
    let _test_guard = BROKER_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("create broker test root");
    let (normal_probe, normal_port) = reserve_listener_pair();
    drop(normal_probe);
    let (mut broker_config, message_store_config) = broker_configs(root.path(), normal_port);
    broker_config.broker_ip1 = "not-an-ip-address".into();

    let initialization = Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .build()
        .initialize()
        .await;
    let error = match initialization {
        Ok(running) => {
            running
                .start()
                .await
                .expect("unexpected initialized broker should start")
                .shutdown()
                .await;
            panic!("invalid broker address must fail without panicking");
        }
        Err(error) => error,
    };
    let BrokerStartupError::RolledBack { cause, .. } = error else {
        panic!("configuration failure should include rollback evidence");
    };
    assert!(matches!(
        *cause,
        BrokerStartupError::Initialization {
            component: "broker_configuration",
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_bind_failure_rolls_back_already_started_components() {
    let _test_guard = BROKER_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("create broker test root");
    let (reserved_normal_listener, normal_port) = reserve_listener_pair();
    let fast_port = normal_port - 2;
    let (broker_config, message_store_config) = broker_configs(root.path(), normal_port);
    let runtime_context = RuntimeContext::from_current("broker-transactional-startup-test");
    let initialized = Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .set_service_context(runtime_context.service_context("broker-under-test"))
        .build()
        .initialize()
        .await
        .expect("broker initialization should succeed before bind fault injection");

    let error = match initialized.start().await {
        Ok(running) => {
            running.shutdown().await;
            panic!("reserved normal listener must make startup fail");
        }
        Err(error) => error,
    };
    let BrokerStartupError::RolledBack {
        cause,
        unhealthy_components,
    } = error
    else {
        panic!("startup failure should include rollback evidence");
    };
    assert!(
        matches!(
            cause.as_ref(),
            BrokerStartupError::ListenerStartup { listener: "normal", .. }
        ),
        "unexpected startup failure: {cause:?}"
    );
    assert!(
        unhealthy_components.is_empty(),
        "rollback should be healthy: {unhealthy_components:?}"
    );

    drop(reserved_normal_listener);
    TcpListener::bind(("127.0.0.1", normal_port)).expect("normal listener should be released after rollback");
    TcpListener::bind(("127.0.0.1", fast_port)).expect("fast listener should be released after rollback");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_cold_data_hold_capability_fails_before_listener_startup() {
    let _test_guard = BROKER_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("create broker test root");
    let (normal_probe, normal_port) = reserve_listener_pair();
    drop(normal_probe);
    let (broker_config, mut message_store_config) = broker_configs(root.path(), normal_port);
    message_store_config.cold_data_flow_control_enable = true;
    let runtime_context = RuntimeContext::from_current("broker-unsupported-capability-test");
    let initialized = Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .set_service_context(runtime_context.service_context("broker-under-test"))
        .build()
        .initialize()
        .await
        .expect("metadata initialization should remain available for configuration diagnostics");

    let error = match initialized.start().await {
        Ok(running) => {
            running.shutdown().await;
            panic!("unsupported cold-data suspension must fail closed");
        }
        Err(error) => error,
    };
    let BrokerStartupError::RolledBack { cause, .. } = error else {
        panic!("unsupported capability should be reported through transactional rollback");
    };
    assert!(matches!(
        *cause,
        BrokerStartupError::UnsupportedCapability {
            capability: "cold_data_flow_control",
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_but_unreachable_name_server_prevents_readiness() {
    let _test_guard = BROKER_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("create broker test root");
    let (normal_probe, normal_port) = reserve_listener_pair();
    drop(normal_probe);
    let (mut broker_config, message_store_config) = broker_configs(root.path(), normal_port);
    let unavailable_namesrv = TcpListener::bind("127.0.0.1:0").expect("reserve unavailable NameServer address");
    let unavailable_namesrv_addr = unavailable_namesrv
        .local_addr()
        .expect("unavailable NameServer address");
    drop(unavailable_namesrv);
    broker_config.namesrv_addr = Some(unavailable_namesrv_addr.to_string().into());
    broker_config.register_broker_timeout_mills = 250;
    let runtime_context = RuntimeContext::from_current("broker-registration-readiness-test");
    let initialized = Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .set_service_context(runtime_context.service_context("broker-under-test"))
        .build()
        .initialize()
        .await
        .expect("broker initialization should succeed before registration");

    let error = match initialized.start().await {
        Ok(running) => {
            running.shutdown().await;
            panic!("unreachable configured NameServer must prevent readiness");
        }
        Err(error) => error,
    };
    let BrokerStartupError::RolledBack { cause, .. } = error else {
        panic!("registration failure should trigger transactional rollback");
    };
    assert!(matches!(
        *cause,
        BrokerStartupError::ComponentStart {
            component: "broker_registration",
            ..
        }
    ));
}
