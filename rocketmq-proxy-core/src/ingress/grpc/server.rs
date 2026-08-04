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

use std::future::Future;
use std::net::SocketAddr;

// gRPC listener lifecycle owned by the Proxy composition root.

use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::ingress::grpc::service::GrpcHousekeepingRunReport;
use crate::GrpcConfig;
use crate::ProxyError;
use crate::ProxyResult;

#[derive(Debug, Clone)]
pub struct GrpcServerShutdownReport {
    pub task_group: ShutdownReport,
    pub housekeeping: Option<GrpcHousekeepingRunReport>,
}

impl GrpcServerShutdownReport {
    pub fn is_healthy(&self) -> bool {
        self.task_group.is_healthy()
            && self
                .housekeeping
                .as_ref()
                .is_none_or(|report| report.shutdown_report.is_healthy())
    }
}

/// Owns listener binding and shutdown coordination while the facade supplies
/// the generated tonic service and the housekeeping callback.
pub async fn serve_with_lifecycle<F, H, HFut, S, SFut>(
    config: &GrpcConfig,
    shutdown: F,
    task_group: TaskGroup,
    housekeeping: H,
    serve: S,
) -> ProxyResult<GrpcServerShutdownReport>
where
    F: Future<Output = ShutdownDeadline> + Send,
    H: FnOnce(watch::Receiver<bool>, TaskGroup) -> HFut + Send + 'static,
    HFut: Future<Output = GrpcHousekeepingRunReport> + Send + 'static,
    S: FnOnce(TcpListener, SocketAddr, watch::Receiver<bool>) -> SFut,
    SFut: Future<Output = ProxyResult<()>> + Send,
{
    serve_with_lifecycle_and_ready(config, shutdown, task_group, || Ok(()), housekeeping, serve).await
}

/// Owns listener binding and publishes readiness only after the listener and housekeeping task
/// have both been initialized.
pub async fn serve_with_lifecycle_and_ready<F, R, H, HFut, S, SFut>(
    config: &GrpcConfig,
    shutdown: F,
    task_group: TaskGroup,
    ready: R,
    housekeeping: H,
    serve: S,
) -> ProxyResult<GrpcServerShutdownReport>
where
    F: Future<Output = ShutdownDeadline> + Send,
    R: FnOnce() -> ProxyResult<()>,
    H: FnOnce(watch::Receiver<bool>, TaskGroup) -> HFut + Send + 'static,
    HFut: Future<Output = GrpcHousekeepingRunReport> + Send + 'static,
    S: FnOnce(TcpListener, SocketAddr, watch::Receiver<bool>) -> SFut,
    SFut: Future<Output = ProxyResult<()>> + Send,
{
    let addr = config.socket_addr()?;
    let listener = TcpListener::bind(addr).await.map_err(|error| ProxyError::Transport {
        message: format!("proxy gRPC server failed to bind {addr}: {error}"),
    })?;
    let local_addr = listener.local_addr().map_err(|error| ProxyError::Transport {
        message: format!("proxy gRPC server failed to resolve local address for {addr}: {error}"),
    })?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let housekeeping_future = housekeeping(shutdown_rx.clone(), task_group.clone());

    let serve_future = serve(listener, local_addr, shutdown_tx.subscribe());
    ready()?;
    tokio::pin!(serve_future);
    tokio::pin!(housekeeping_future);
    tokio::pin!(shutdown);
    let mut serve_result = None;
    let mut housekeeping_report = None;
    let shutdown_deadline = tokio::select! {
        result = &mut serve_future => {
            serve_result = Some(result);
            ShutdownDeadline::after(config.shutdown_timeout())
        },
        report = &mut housekeeping_future => {
            housekeeping_report = Some(report);
            ShutdownDeadline::after(config.shutdown_timeout())
        },
        deadline = &mut shutdown => deadline,
    };
    let _ = shutdown_tx.send(true);
    let serve_completion = async {
        match serve_result {
            Some(result) => result,
            None => serve_future.await,
        }
    };
    let housekeeping_completion = async {
        match housekeeping_report {
            Some(report) => report,
            None => housekeeping_future.await,
        }
    };
    let (serve_result, housekeeping_report) = tokio::join!(serve_completion, housekeeping_completion);
    let task_group_report = task_group.shutdown_until(shutdown_deadline).await;
    serve_result?;
    Ok(GrpcServerShutdownReport {
        task_group: task_group_report,
        housekeeping: Some(housekeeping_report),
    })
}
