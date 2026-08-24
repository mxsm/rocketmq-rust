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

use super::connection_listener::ConnectionListener;
use super::*;

pub(super) async fn shutdown_remoting_server<RP>(
    listener: ConnectionListener<RP>,
    task_group: TaskGroup,
    lifecycle_shutdown: CancellationToken,
    shutdown_complete_rx: &mut mpsc::Receiver<()>,
) -> ShutdownReport {
    let ConnectionListener {
        shutdown_complete_tx,
        tls_runtime,
        lifecycle_dispatcher_task,
        ..
    } = listener;
    let deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
    task_group.cancel();
    drop(shutdown_complete_tx);
    let _ = tokio::time::timeout(deadline.remaining(), shutdown_complete_rx.recv()).await;

    lifecycle_shutdown.cancel();
    if let Some(task_id) = lifecycle_dispatcher_task {
        if !task_group.wait_task(task_id, deadline.remaining()).await {
            warn!(
                task_id = task_id.as_u64(),
                "Remoting lifecycle event dispatcher did not drain before shutdown deadline"
            );
        }
    }

    let tls_report = tls_runtime
        .shutdown_gracefully(deadline.remaining().min(Duration::from_secs(3)))
        .await;
    if let Some(report) = tls_report.as_ref() {
        report.log_if_unhealthy();
    }
    let mut report = task_group.shutdown_until(deadline).await;
    if let Some(tls_report) = tls_report {
        report.children.push(tls_report);
    }
    report.log_if_unhealthy();
    report
}

pub(super) fn new_remoting_server_context(context: &ChildServiceContext) -> ChildServiceContext {
    context.component("rocketmq.remoting.server")
}

#[cfg(test)]
pub(super) fn new_remoting_server_task_group_with_service_context(context: &ChildServiceContext) -> TaskGroup {
    new_remoting_server_context(context).task_group().clone()
}
