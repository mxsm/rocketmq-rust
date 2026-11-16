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

//! Failure-only projection of the already completed owner shutdown report.

use rocketmq_runtime::blocking::BlockingTaskState;
use rocketmq_runtime::task_group::TaskState;
use rocketmq_runtime::{BlockingKind, ShutdownReport, TaskKind};
use serde::Serialize;

const REPORT_LIMIT: usize = 64;
const DETAIL_LIMIT: usize = 64;

#[derive(Serialize)]
struct Diagnostic {
    schema_version: &'static str,
    lifecycle_failed: bool,
    owner_outcome: &'static str,
    reports_scanned: usize,
    truncated: bool,
    scopes: Vec<Scope>,
    tasks: Vec<Task>,
    blocking_tasks: Vec<BlockingTask>,
}

#[derive(Serialize)]
struct Scope {
    class: &'static str,
    parent_index: Option<usize>,
    elapsed_ms: u64,
    completed: usize,
    cancelled: usize,
    aborted: usize,
    failed: usize,
    panicked: usize,
    timed_out: usize,
    leaked: usize,
    blocking_still_running: usize,
    remaining_tasks: usize,
    remaining_tasks_omitted: usize,
}

#[derive(Serialize)]
struct Task {
    scope_index: usize,
    class: &'static str,
    kind: TaskKind,
    state: TaskState,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct BlockingTask {
    scope_index: usize,
    class: &'static str,
    kind: BlockingKind,
    state: BlockingTaskState,
    elapsed_ms: u64,
}

fn scope_class(name: &str) -> &'static str {
    match name {
        "rocketmq-broker" | "rocketmq-broker-runtime" | "broker" => "broker",
        "rocketmq-transport.tls" => "transport_tls",
        "rocketmq.observability.prometheus" => "prometheus",
        "broker.schedule-message" => "schedule_message",
        "broker.metadata-io" | "metadata-io" => "metadata_io",
        "broker.outer-api" => "broker_outer_api",
        "rocketmq-transport.client" | "rocketmq-transport.client.workers" | "rocketmq.transport.client.connection" => {
            "transport_client"
        }
        "broker.store" => "store",
        "broker.deferred-producers" => "deferred_producers",
        "broker.topic-config" => "topic_config",
        "broker.scheduled" => "scheduled_tasks",
        "broker.statistics" => "statistics",
        "broker.auth" | "auth.metadata-io" | "auth.acl-file-watcher" => "auth",
        "broker.pop-revive" | "broker.pop-buffer-merge" | "broker.pop-deferred" => "pop_services",
        "rocketmq-broker.remoting-server"
        | "rocketmq-broker.request-processor"
        | "broker.remoting-server.normal"
        | "broker.remoting-server.fast"
        | "rocketmq.remoting.server" => "request_pipeline",
        "runtime-diagnostics" => "runtime_diagnostics",
        _ => "unclassified",
    }
}

fn task_class(name: &str) -> &'static str {
    match name {
        "remoting.tls.reload" | "transport.tls.reload" | "transport.tls.reload.snapshot" => "tls_reload",
        "broker.log-filter-ttl" => "log_filter_ttl",
        "broker.pop-revive.scan" | "broker.pop-buffer-merge.scan" | "broker.pop-deferred.sweep" => "pop_services",
        "prometheus.accept" | "prometheus.connection" => "prometheus",
        "runtime-diagnostics.endpoint" | "runtime-diagnostics.sample" => "runtime_diagnostics",
        _ => "unclassified",
    }
}

fn millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn project(report: Option<&ShutdownReport>) -> Diagnostic {
    let mut result = Diagnostic {
        schema_version: "rocketmq.broker-shutdown-diagnostic.v1",
        lifecycle_failed: true,
        owner_outcome: "unavailable",
        reports_scanned: 0,
        truncated: false,
        scopes: Vec::new(),
        tasks: Vec::new(),
        blocking_tasks: Vec::new(),
    };
    let Some(report) = report else { return result };
    let mut pending = vec![(report, None)];
    let mut unhealthy = false;
    while let Some((report, parent_index)) = pending.pop() {
        let index = result.scopes.len();
        result.reports_scanned += 1;
        unhealthy |= report.failed > 0
            || report.panicked > 0
            || report.timed_out > 0
            || report.leaked > 0
            || report.blocking_still_running > 0;
        result.scopes.push(Scope {
            class: scope_class(&report.name),
            parent_index,
            elapsed_ms: millis(report.elapsed),
            completed: report.completed,
            cancelled: report.cancelled,
            aborted: report.aborted,
            failed: report.failed,
            panicked: report.panicked,
            timed_out: report.timed_out,
            leaked: report.leaked,
            blocking_still_running: report.blocking_still_running,
            remaining_tasks: report.remaining_tasks.len(),
            remaining_tasks_omitted: report.remaining_tasks_omitted,
        });
        let previous_details = result.tasks.len() + result.blocking_tasks.len();
        let detail_budget = DETAIL_LIMIT.saturating_sub(previous_details);
        for task in report.remaining_tasks.iter().take(detail_budget) {
            result.tasks.push(Task {
                scope_index: index,
                class: task_class(&task.name),
                kind: task.kind,
                state: task.state,
                elapsed_ms: millis(task.elapsed),
            });
        }
        let detail_budget = DETAIL_LIMIT.saturating_sub(result.tasks.len() + result.blocking_tasks.len());
        for task in report.blocking_tasks.iter().take(detail_budget) {
            result.blocking_tasks.push(BlockingTask {
                scope_index: index,
                class: task_class(&task.name),
                kind: task.kind,
                state: task.state,
                elapsed_ms: millis(task.elapsed),
            });
        }
        let emitted_details = result.tasks.len() + result.blocking_tasks.len() - previous_details;
        result.truncated |= report.remaining_tasks_omitted > 0
            || report.remaining_tasks.len().saturating_add(report.blocking_tasks.len()) > emitted_details;
        let available = REPORT_LIMIT.saturating_sub(result.scopes.len() + pending.len());
        result.truncated |= report.children.len() > available;
        pending.extend(
            report
                .children
                .iter()
                .take(available)
                .rev()
                .map(|child| (child, Some(index))),
        );
    }
    result.owner_outcome = if unhealthy {
        "unhealthy_observed"
    } else if result.truncated {
        "partial_unknown"
    } else {
        "healthy_observed"
    };
    result
}

pub(super) fn record(report: Option<&ShutdownReport>) {
    let diagnostic = serde_json::to_string(&project(report)).unwrap_or_else(|_| {
        "{\"schema_version\":\"rocketmq.broker-shutdown-diagnostic.v1\",\"serialization_failed\":true}".to_owned()
    });
    // The failed run may already have dropped its telemetry guard. Keep this
    // bounded, name-free observation available through the CLI's stderr sink.
    eprintln!("Broker runtime observation after lifecycle failure: {diagnostic}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_runtime::ShutdownAnnotation;
    use std::time::Duration;

    #[test]
    fn failure_projection_preserves_owner_counts_without_reflecting_private_names_or_annotations() {
        let canary = "private-endpoint-key-and-config-canary";
        let mut report = ShutdownReport::new(canary, Duration::from_millis(45));
        report.annotations.push(ShutdownAnnotation::new(canary));
        let mut child = ShutdownReport::new("rocketmq-transport.tls", Duration::from_millis(44));
        child.timed_out = 1;
        child.leaked = 2;
        report.children.push(child);
        let projected = project(Some(&report));
        let encoded = serde_json::to_string(&projected).unwrap();
        assert!(!encoded.contains(canary));
        assert_eq!(projected.owner_outcome, "unhealthy_observed");
        assert_eq!(projected.scopes[0].class, "unclassified");
        assert_eq!(projected.scopes[1].class, "transport_tls");
        assert_eq!(projected.scopes[1].parent_index, Some(0));
        assert_eq!((projected.scopes[1].timed_out, projected.scopes[1].leaked), (1, 2));
        assert_eq!(task_class(canary), "unclassified");
    }

    #[test]
    fn bounded_failure_projection_never_calls_a_partial_tree_healthy() {
        let mut report = ShutdownReport::new("broker", Duration::ZERO);
        report.children = (0..200)
            .map(|_| ShutdownReport::new("private-child", Duration::ZERO))
            .collect();
        let projected = project(Some(&report));
        assert_eq!(projected.reports_scanned, REPORT_LIMIT);
        assert_eq!(projected.scopes.len(), REPORT_LIMIT);
        assert!(projected.truncated);
        assert_eq!(projected.owner_outcome, "partial_unknown");
        assert!(serde_json::to_vec(&projected).unwrap().len() < 64 * 1024);
        assert_eq!(project(None).owner_outcome, "unavailable");
    }

    #[tokio::test]
    async fn task_and_blocking_details_share_a_budget_and_never_reflect_private_labels() {
        const CANARY: &str = "private-task-endpoint-key-and-path-canary";
        let context = rocketmq_runtime::RuntimeContext::from_current("diagnostic-test");
        let service = context.service_context("private-owner-canary");
        let task_id = service.spawn_service("diagnostic.seed", async {}).unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let work = service.metadata_io().spawn_io(CANARY, move || {
            let _ = started_tx.send(());
            release_rx.recv_timeout(Duration::from_secs(3)).is_ok()
        });
        tokio::pin!(work);
        tokio::select! {
            result = started_rx => result.expect("blocking work started"),
            _ = &mut work => panic!("work must remain owned until released"),
        }
        let blocking = service
            .metadata_io()
            .snapshot()
            .tasks
            .into_iter()
            .find(|task| task.name == CANARY)
            .unwrap();
        release_tx.send(()).unwrap();
        assert!(work.await.unwrap());
        assert!(context.shutdown_tasks(Duration::from_secs(3)).await.is_healthy());

        let task = rocketmq_runtime::TaskSnapshot {
            id: task_id,
            name: CANARY.to_owned(),
            group_id: task_id.group_id(),
            group_name: CANARY.to_owned(),
            kind: TaskKind::Service,
            state: TaskState::Running,
            elapsed: Duration::from_secs(1),
        };
        let mut report = ShutdownReport::new("broker", Duration::ZERO);
        report.remaining_tasks = vec![task; 40];
        report.blocking_tasks = vec![blocking; 40];
        report.leaked = 40;
        report.blocking_still_running = 40;
        let projected = project(Some(&report));
        let encoded = serde_json::to_string(&projected).unwrap();
        assert_eq!(projected.tasks.len(), 40);
        assert_eq!(projected.blocking_tasks.len(), DETAIL_LIMIT - 40);
        assert!(projected.truncated);
        assert_eq!(projected.owner_outcome, "unhealthy_observed");
        assert!(!encoded.contains(CANARY));
        assert!(!encoded.contains("group_id"));
        assert!(!encoded.contains("\"id\":"));
        assert_eq!(projected.scopes[0].remaining_tasks, 40);
        assert_eq!(projected.scopes[0].blocking_still_running, 40);
    }
}
