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

#![cfg(feature = "tls")]

use std::time::Duration;

use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_transport::config::TlsConfig;
use rocketmq_transport::tls::TlsServerRuntime;

#[tokio::test]
async fn reload_filesystem_work_queues_through_the_injected_blocking_executor() {
    let policy = BlockingPoolPolicy {
        max_concurrency: 1,
        queue_timeout: Duration::from_secs(1),
        task_timeout: Duration::from_secs(1),
        ..BlockingPoolPolicy::default()
    };
    let context = RuntimeContext::new_with_blocking_policy(
        RuntimeHandle::new(tokio::runtime::Handle::current()),
        "tls-blocking-test",
        policy,
    )
    .unwrap();
    let service = context.service_context("tls-service");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocking = service.blocking().clone();
    let occupying = tokio::spawn(async move {
        blocking
            .spawn_io("occupy-blocking-slot", move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
            .await
            .unwrap();
    });
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();

    let tls = TlsServerRuntime::new_with_service_context(
        TlsConfig {
            test_mode_enable: true,
            ..TlsConfig::default()
        },
        &service,
    );
    let reload = tokio::spawn(async move { tls.reload_now().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.blocking().snapshot().queued == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("TLS reload should queue behind the injected executor");
    assert!(!reload.is_finished());

    release_tx.send(()).unwrap();
    occupying.await.unwrap();
    reload.await.unwrap().unwrap();
    let report = context.shutdown_tasks(Duration::from_secs(1)).await;
    report.assert_no_task_leak().unwrap();
}
