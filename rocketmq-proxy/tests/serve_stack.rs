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

//! The Proxy process entrypoint polls `ProxyRuntime::serve_with_lifecycle` on
//! its main thread, and Windows gives that thread 1 MiB of stack. Unoptimized
//! builds keep a copy of every child future a poll frame builds, so this probe
//! serves both listeners, and accepts a connection on each, from a thread of
//! that size.

use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_proxy::v2;
use rocketmq_proxy::v2::messaging_service_client::MessagingServiceClient;
use rocketmq_proxy::ClusterServiceManager;
use rocketmq_proxy::DefaultAssignmentService;
use rocketmq_proxy::DefaultConsumerService;
use rocketmq_proxy::DefaultMessageService;
use rocketmq_proxy::DefaultMetadataService;
use rocketmq_proxy::DefaultRouteService;
use rocketmq_proxy::DefaultTransactionService;
use rocketmq_proxy::GrpcConfig;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_proxy::RemotingConfig;
use rocketmq_runtime::heap_future;
use rocketmq_runtime::DrainRequestMethods;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleConfig;
use rocketmq_runtime::ShutdownReason;
use rocketmq_runtime::TaskKind;
use rocketmq_transport::test_support::Connection;

const CHILD_MODE: &str = "ROCKETMQ_PROXY_SERVE_STACK_TEST_CHILD";
const PROBE_NAME: &str = "serving_both_listeners_fits_on_a_one_mib_thread";
/// The stack Windows gives a process main thread.
const MAIN_THREAD_STACK_SIZE: usize = 1024 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_ATTEMPTS: usize = 200;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(25);

#[test]
fn serving_both_listeners_fits_on_a_one_mib_thread() {
    if std::env::var_os(CHILD_MODE).is_none() {
        // A stack overflow aborts the process; keep it out of the test harness.
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", PROBE_NAME, "--nocapture"])
            .env(CHILD_MODE, "1")
            .output()
            .expect("run Proxy serve stack probe");
        assert!(
            output.status.success(),
            "Proxy serve stack probe failed: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    std::thread::Builder::new()
        .name("proxy-serve-stack-probe".to_owned())
        .stack_size(MAIN_THREAD_STACK_SIZE)
        .spawn(serve_both_listeners_until_drained)
        .expect("start the probe thread")
        .join()
        .expect("probe thread completed");
}

fn serve_both_listeners_until_drained() {
    let owner = RuntimeOwner::plan(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .expect("valid runtime configuration")
    .build()
    .expect("runtime owner");
    let (grpc_addr, remoting_addr) = free_listener_addrs();
    let service_manager = Arc::new(ClusterServiceManager::with_services(
        Arc::new(DefaultRouteService::default()),
        Arc::new(DefaultMetadataService),
        Arc::new(DefaultAssignmentService),
        Arc::new(DefaultMessageService),
        Arc::new(DefaultConsumerService),
        Arc::new(DefaultTransactionService),
    ));
    let runtime = ProxyRuntime::builder(
        ProxyConfig {
            grpc: GrpcConfig {
                listen_addr: grpc_addr.to_string(),
                ..GrpcConfig::default()
            },
            remoting: RemotingConfig {
                enabled: true,
                listen_addr: remoting_addr.to_string(),
                ..RemotingConfig::default()
            },
            ..ProxyConfig::default()
        },
        owner.root_context().component("proxy"),
        rocketmq_observability::TelemetryHandle::noop(),
    )
    .with_service_manager(service_manager)
    .build()
    .expect("build the Proxy runtime");
    let lifecycle = ServiceLifecycle::new(ServiceLifecycleConfig {
        service_name: Arc::from("proxy-serve-stack-probe"),
        probe_bind_addr: None,
        shutdown_timeout: Duration::from_secs(10),
        liveness_stale_after: Duration::from_secs(30),
        drain_request_methods: DrainRequestMethods::PostOnly,
    });

    // The client runs on the runtime's workers while this thread polls the server.
    let client_lifecycle = lifecycle.clone();
    let client_context = owner.root_context().component("client");
    let (client_result_tx, client_result_rx) = std::sync::mpsc::sync_channel(1);
    let (_, client) = client_context
        .task_group()
        .spawn_with_handle("proxy-serve-stack-client", TaskKind::Worker, async move {
            let result = tokio::time::timeout(CLIENT_TIMEOUT, exercise_listeners(grpc_addr, remoting_addr))
                .await
                .unwrap_or_else(|_| Err("the listeners did not answer in time".to_owned()));
            client_lifecycle.request_shutdown(ShutdownReason::PreStop);
            let _ = client_result_tx.send(result);
        })
        .expect("start the client");

    // Like the process entrypoint, poll the root future from the heap.
    let serve_result = owner.block_on(heap_future(|| runtime.serve_with_lifecycle(lifecycle.clone())));
    if let Some(bytes) = rocketmq_runtime::current_thread_committed_stack_bytes() {
        println!("Proxy serve probe stack peak: {} KiB", bytes / 1024);
    }
    owner.block_on(client).expect("join the client");
    assert!(serve_result.is_ok(), "Proxy serve failed: {serve_result:?}");
    client_result_rx
        .recv()
        .expect("the client reports its result")
        .expect("both listeners should answer");
    let report = owner.shutdown_runtime_blocking().expect("stop the runtime");
    assert!(report.is_healthy(), "{}", report.to_json());
}

/// Sends one request to each listener, so each accepts and runs a session.
async fn exercise_listeners(grpc_addr: SocketAddr, remoting_addr: SocketAddr) -> Result<(), String> {
    let stream = connect_with_retry(remoting_addr).await?;
    let mut remoting = Connection::new(stream);
    let request = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new("TopicA", None),
    );
    let opaque = request.opaque();
    remoting
        .send_command(request)
        .await
        .map_err(|error| format!("send the Remoting request: {error}"))?;
    loop {
        match remoting.receive_command().await {
            Some(Ok(response)) if response.opaque() == opaque => break,
            Some(Ok(_)) => continue,
            Some(Err(error)) => return Err(format!("decode the Remoting response: {error}")),
            None => return Err("the Remoting session closed before it answered".to_owned()),
        }
    }

    let mut grpc = MessagingServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .map_err(|error| format!("connect to the gRPC listener: {error}"))?;
    grpc.query_route(v2::QueryRouteRequest {
        topic: Some(v2::Resource {
            resource_namespace: String::new(),
            name: "TopicA".to_owned(),
        }),
        endpoints: None,
    })
    .await
    .map_err(|status| format!("query the gRPC listener: {status}"))?;
    Ok(())
}

async fn connect_with_retry(addr: SocketAddr) -> Result<tokio::net::TcpStream, String> {
    let mut last_error = None;
    for _ in 0..CONNECT_ATTEMPTS {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(CONNECT_RETRY_DELAY).await;
    }
    Err(format!("connect to {addr}: {last_error:?}"))
}

fn free_listener_addrs() -> (SocketAddr, SocketAddr) {
    let grpc = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a gRPC port");
    let remoting = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a Remoting port");
    let addrs = (
        grpc.local_addr().expect("gRPC listener address"),
        remoting.local_addr().expect("Remoting listener address"),
    );
    drop((grpc, remoting));
    addrs
}
