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

#![cfg(feature = "test-support")]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::TlsConfig;
use rocketmq_transport::api::v1::TlsServerRuntime;
use rocketmq_transport::test_support::connect_with_config;
use rocketmq_transport::test_support::ConnectionHandler;
use rocketmq_transport::test_support::SessionHandle;
use rocketmq_transport::test_support::TransportListener;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

struct CountConnections(AtomicUsize);

impl ConnectionHandler for CountConnections {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
        })
    }

    fn command(
        &self,
        _session: SessionHandle,
        _command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

#[tokio::test]
async fn canonical_client_connect_builds_the_framed_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

    let connected = connect_with_config(
        &address.to_string(),
        &TlsConfig::default(),
        FrameLimits::legacy_compatibility(),
        RequestDeadline::after(Duration::from_secs(1)),
    )
    .await
    .unwrap();

    assert_eq!(connected.remote_addr(), address);
    drop(connected);
    accept.await.unwrap();
}

#[tokio::test]
async fn canonical_listener_uses_idle_timeout_for_a_silent_tls_peek() {
    let runtime = RuntimeContext::from_current("transport-listener-test");
    let service = runtime.service_context("listener");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let tls = TlsServerRuntime::initialize_with_service_context(TlsConfig::default(), &service)
        .await
        .unwrap();
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let handled = Arc::new(CountConnections(AtomicUsize::new(0)));
    let runtime_listener = TransportListener::new(
        listener,
        service.task_group().clone(),
        tls,
        admission.clone(),
        Duration::from_millis(25),
    )
    .with_idle_timeout(Duration::from_millis(100));
    let handler = handled.clone();
    service
        .spawn_service("listener.run", async move {
            let _ = runtime_listener.run(handler).await;
        })
        .unwrap();

    let mut silent = tokio::net::TcpStream::connect(address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;

    assert_eq!(handled.0.load(Ordering::SeqCst), 0);
    assert_eq!(admission.snapshot().connections.current_count, 1);

    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_millis(500), silent.read(&mut byte))
        .await
        .expect("silent connection should reach the idle timeout")
        .expect("read idle close");
    assert_eq!(read, 0);
    assert_eq!(admission.snapshot().connections.current_count, 0);

    let mut active = tokio::net::TcpStream::connect(address).await.unwrap();
    active.write_all(&[0]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while handled.0.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a connection that sends its first byte should be delegated");

    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    report.assert_no_task_leak().unwrap();
}
