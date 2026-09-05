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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use super::RequestProcessor;
use super::ServerConfig;
use super::TransportServer;
use crate::error::TransportError;

pub(super) struct TestRuntime {
    owner: RuntimeOwner,
    server_context: ChildServiceContext,
    runner_context: ChildServiceContext,
    task_name: String,
}

pub(super) fn loopback_server_config() -> Arc<ServerConfig> {
    Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    })
}

impl TestRuntime {
    pub(super) fn new(name: &'static str) -> Self {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default(name))
            .expect("test runtime configuration is valid")
            .build()
            .expect("test runtime owner");
        let server_context = owner.root_context().component(
            rocketmq_runtime::ScopeId::try_new(format!("{name}.server"))
                .expect("the server scope has a fixed nonblank suffix"),
        );
        let runner_context = owner.root_context().component(
            rocketmq_runtime::ScopeId::try_new(format!("{name}.runner"))
                .expect("the runner scope has a fixed nonblank suffix"),
        );
        Self {
            owner,
            server_context,
            runner_context,
            task_name: format!("{name}.serve"),
        }
    }

    pub(super) fn service_context(&self) -> ChildServiceContext {
        self.server_context.clone()
    }

    pub(super) async fn finish(self) {
        finish_owner(self.owner).await;
    }
}

pub(super) struct RunningServer {
    owner: RuntimeOwner,
    shutdown: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<Result<ShutdownReport, TransportError>>,
}

impl RunningServer {
    pub(super) fn begin_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    pub(super) async fn finish(mut self) -> ShutdownReport {
        self.begin_shutdown();
        let report = tokio::time::timeout(Duration::from_secs(2), &mut self.result)
            .await
            .expect("owned server shutdown deadline")
            .expect("owned server result channel")
            .expect("owned server shutdown report");
        assert_clean_shutdown("server", &report);
        finish_owner(self.owner).await;
        report
    }

    async fn finish_start_error(mut self) -> TransportError {
        self.shutdown.take();
        let error = self
            .result
            .await
            .expect("failed-start result channel")
            .expect_err("server startup must fail");
        finish_owner(self.owner).await;
        error
    }
}

pub(super) async fn start_server<P>(
    runtime: TestRuntime,
    server: TransportServer<P>,
) -> (crate::connection::Connection, SocketAddr, RunningServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let (startup, running) = spawn_server(runtime, server, None);
    let address = startup
        .await
        .expect("owned startup result channel")
        .expect("owned startup succeeds");
    let client =
        crate::connection::Connection::new(TcpStream::connect(address).await.expect("connect owned test client"));
    (client, address, running)
}

pub(super) async fn start_server_with_shutdown_observer<P>(
    runtime: TestRuntime,
    server: TransportServer<P>,
) -> (
    crate::connection::Connection,
    SocketAddr,
    RunningServer,
    oneshot::Receiver<()>,
)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let (shutdown_seen, shutdown_observed) = oneshot::channel();
    let (startup, running) = spawn_server(runtime, server, Some(shutdown_seen));
    let address = startup
        .await
        .expect("observed startup result channel")
        .expect("observed startup succeeds");
    let client =
        crate::connection::Connection::new(TcpStream::connect(address).await.expect("connect observed test client"));
    (client, address, running, shutdown_observed)
}

pub(super) async fn expect_start_error<P>(runtime: TestRuntime, server: TransportServer<P>) -> TransportError
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let (startup, running) = spawn_server(runtime, server, None);
    let startup_error = startup
        .await
        .expect("failed-start startup channel")
        .expect_err("startup must fail");
    let result_error = running.finish_start_error().await;
    assert_eq!(startup_error.code(), result_error.code());
    let startup_source = std::error::Error::source(&startup_error).expect("startup publication retains typed source");
    let result_source = std::error::Error::source(&result_error).expect("startup return retains typed source");
    assert!(
        std::ptr::eq(startup_source, result_source),
        "readiness and returned failures must share one original typed source"
    );
    result_error
}

fn spawn_server<P>(
    runtime: TestRuntime,
    server: TransportServer<P>,
    shutdown_seen: Option<oneshot::Sender<()>>,
) -> (oneshot::Receiver<Result<SocketAddr, TransportError>>, RunningServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let TestRuntime {
        owner,
        server_context: _,
        runner_context,
        task_name,
    } = runtime;
    let (shutdown, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service(task_name, async move {
            let result = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                        if let Some(shutdown_seen) = shutdown_seen {
                            let _ = shutdown_seen.send(());
                        }
                    },
                    startup_tx,
                )
                .await;
            let _ = result_tx.send(result);
        })
        .expect("spawn server in owned TaskGroup");
    (
        startup_rx,
        RunningServer {
            owner,
            shutdown: Some(shutdown),
            result: result_rx,
        },
    )
}

async fn finish_owner(owner: RuntimeOwner) {
    let report = owner.shutdown_tasks().await;
    assert_clean_shutdown("test runtime", &report);
    let final_report = owner.shutdown_background();
    assert_clean_shutdown("test runtime finalization", &final_report);
}

fn assert_clean_shutdown(owner: &str, report: &ShutdownReport) {
    assert!(report.is_healthy(), "{owner}: {}", report.to_json());
    assert_eq!(report.aborted, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.panicked, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.timed_out, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.leaked, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.blocking_still_running, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.detached_still_running, 0, "{owner}: {}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{owner}: {}", report.to_json());
    for child in &report.children {
        assert_clean_shutdown(owner, child);
    }
}
