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

use std::sync::Arc;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::SessionRegistry;
use rocketmq_transport::api::TransportServer;
use rocketmq_transport::test_support::Connection;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

pub(crate) struct RunningProcessorServer {
    address: std::net::SocketAddr,
    owner: RuntimeOwner,
    stop: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningProcessorServer {
    pub(crate) async fn connect(&self) -> Connection {
        Connection::new(
            TcpStream::connect(self.address)
                .await
                .expect("connect processor client"),
        )
    }

    pub(crate) async fn finish(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let report = self.result.await.expect("processor server shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let tasks = self.owner.shutdown_tasks().await;
        assert!(tasks.is_healthy(), "{}", tasks.to_json());
        let background = self.owner.shutdown_background();
        assert!(background.is_healthy(), "{}", background.to_json());
    }

    pub(crate) async fn finish_and_collect(mut self, mut connection: Connection) -> Vec<RemotingCommand> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let shutdown = async move {
            let report = self.result.await.expect("processor server shutdown report");
            assert!(report.is_healthy(), "{}", report.to_json());
            let tasks = self.owner.shutdown_tasks().await;
            assert!(tasks.is_healthy(), "{}", tasks.to_json());
            let background = self.owner.shutdown_background();
            assert!(background.is_healthy(), "{}", background.to_json());
        };
        let collect = async move {
            let mut responses = Vec::new();
            while let Some(response) = connection.receive_command().await {
                responses.push(response.expect("receive processor response"));
            }
            responses
        };
        let ((), responses) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(shutdown, collect)
        })
        .await
        .expect("processor server and client should terminate deterministically");
        responses
    }

    pub(crate) async fn receive_one_then_finish_and_collect(self, mut connection: Connection) -> Vec<RemotingCommand> {
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), connection.receive_command())
            .await
            .expect("processor response should arrive before shutdown")
            .expect("processor connection should remain open for its response")
            .expect("receive processor response");
        let mut responses = vec![first];
        responses.extend(self.finish_and_collect(connection).await);
        responses
    }
}

pub(crate) async fn start_processor_server<P>(
    label: &'static str,
    processor: P,
    controller: Arc<AdmissionController>,
) -> (Connection, RunningProcessorServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    start_processor_server_with_registry(label, processor, controller, None).await
}

pub(crate) async fn start_processor_server_with_session_registry<P>(
    label: &'static str,
    processor: P,
    controller: Arc<AdmissionController>,
    session_registry: Arc<SessionRegistry>,
) -> (Connection, RunningProcessorServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    start_processor_server_with_registry(label, processor, controller, Some(session_registry)).await
}

async fn start_processor_server_with_registry<P>(
    label: &'static str,
    processor: P,
    controller: Arc<AdmissionController>,
    session_registry: Option<Arc<SessionRegistry>>,
) -> (Connection, RunningProcessorServer)
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let mut runtime_config = RuntimeConfig::server_default(label);
    runtime_config.thread_stack_size = Some(16 * 1024 * 1024);
    let owner = RuntimeOwner::new(runtime_config).expect("processor runtime owner");
    let server_context = owner.root_context().component(format!("{label}.server"));
    let runner_context = owner.root_context().component(format!("{label}.runner"));
    let server = TransportServer::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        }),
        server_context,
        processor,
    )
    .with_admission_controller(controller);
    let server = match session_registry {
        Some(session_registry) => server.with_session_registry(session_registry),
        None => server,
    };
    let (stop_tx, stop_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service(format!("{label}.serve"), async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = stop_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("processor server report");
            let _ = result_tx.send(report);
        })
        .expect("start processor server");
    let address = startup_rx
        .await
        .expect("processor startup result")
        .expect("processor startup address");
    let connection = Connection::new(TcpStream::connect(address).await.expect("connect processor client"));
    (
        connection,
        RunningProcessorServer {
            address,
            owner,
            stop: Some(stop_tx),
            result: result_rx,
        },
    )
}
