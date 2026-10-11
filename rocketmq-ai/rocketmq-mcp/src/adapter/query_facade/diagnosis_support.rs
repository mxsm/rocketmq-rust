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

//! What every diagnosis workflow needs to collect evidence from one session.
//!
//! A diagnosis reads several sources and reports on what it got. No read fails it: a failed read
//! is missing evidence in the report and a source failure in the result.

use std::future::Future;

use super::completeness_for_error;
use super::WorkflowControl;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::admin_session::SessionConsumerLag;
use crate::adapter::admin_session::SessionTopicRoute;
use crate::adapter::admin_session::SessionTopicStats;
use crate::model::contract::observed_at;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QuerySource;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::diagnosis_collector::ConsumeDelay;
use crate::service::diagnosis_collector::ConsumerLagSummary;
use crate::service::diagnosis_collector::QueueLagBreakdown;
use crate::service::diagnosis_collector::TopicRouteSummary;
use crate::service::diagnosis_evidence::TopicActivity;
pub(super) use crate::service::diagnosis_evidence::DEAD_LETTER_TOPIC_PREFIX;
pub(super) use crate::service::diagnosis_evidence::RETRY_TOPIC_PREFIX;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// The time the evidence that explains a finding may take.
///
/// It ends before the request does. A slow source then costs the conclusions that need it, and the
/// report is still written from what was collected. Without it, one Broker that answers slowly
/// would turn a diagnosis that already knows its main fact into a timeout.
pub(super) struct SupportingBudget {
    deadline: tokio::time::Instant,
    /// The request budget, which a read that runs out of time reports.
    request_budget_ms: u64,
    spent: bool,
}

impl SupportingBudget {
    /// Leaves a tenth of the request budget for writing the report and closing the session.
    pub(super) fn new(control: &WorkflowControl) -> Self {
        let deadline = control.deadline();
        Self {
            deadline: deadline.checked_sub(control.budget / 10).unwrap_or(deadline),
            request_budget_ms: control.budget.as_millis().try_into().unwrap_or(u64::MAX),
            spent: false,
        }
    }

    /// Runs one read. Once the budget is spent, this and every later read fail as timed out.
    ///
    /// A read that runs out of time is dropped where it stands, so the session is not asked for
    /// anything else afterwards: it is only closed.
    pub(super) async fn read<T>(
        &mut self,
        read: impl Future<Output = Result<T, ToolFailure>>,
    ) -> Result<T, ToolFailure> {
        if !self.spent && tokio::time::Instant::now() < self.deadline {
            if let Ok(result) = tokio::time::timeout_at(self.deadline, read).await {
                return result;
            }
        }
        self.spent = true;
        Err(ToolFailure::Rejected(ToolRejection::TimedOut {
            timeout_ms: self.request_budget_ms,
        }))
    }
}

/// Records how complete one evidence read was: the source failures inside a result, or the failure
/// of the read itself.
pub(super) fn note<T>(
    completeness: &mut QueryCompleteness,
    source: QuerySource,
    logical_target: &str,
    result: Result<QueryPayload<T>, ToolFailure>,
) -> Result<QueryPayload<T>, ToolFailure> {
    match &result {
        Ok(payload) => completeness.merge(payload.completeness()),
        Err(error) => completeness.merge(completeness_for_error(source, logical_target, error)),
    }
    result
}

/// Wraps a finished report: a report with missing evidence makes the result partial.
pub(super) fn wrap_report(
    mut completeness: QueryCompleteness,
    report: DiagnosisReport,
) -> QueryPayload<DiagnosisReport> {
    if report.partial {
        completeness.partial = true;
        completeness.warnings.push("diagnosis_evidence_incomplete".to_string());
    }
    completeness.wrap(report)
}

/// Summarizes the statistics of a retry or dead-letter Topic.
///
/// Such a Topic exists only once a message was written to it, so a lookup that finds none is a
/// completed observation and not a failed source.
pub(super) fn system_topic_activity(
    completeness: &mut QueryCompleteness,
    cluster: &ResolvedCluster,
    topic: String,
    stats: Result<QueryPayload<SessionTopicStats>, ToolFailure>,
    now_millis: i64,
) -> Result<TopicActivity, ToolFailure> {
    let stats = match stats {
        Err(error) if error.not_found_entity() == Some(NotFoundEntity::Topic) => {
            return Ok(TopicActivity::absent(&cluster.name, topic));
        }
        stats => note(completeness, QuerySource::TopicStats, &topic, stats)?.data,
    };
    Ok(TopicActivity::from_stats(
        &cluster.name,
        topic,
        stats.queue_count,
        stats.total_message_count,
        &stats.queues,
        now_millis,
    ))
}

/// Summarizes the lag of every queue of one Topic as diagnosis evidence.
pub(super) fn consumer_lag_summary(
    cluster: &ResolvedCluster,
    topic: &str,
    consumer_group: &str,
    lag: SessionConsumerLag,
    now_millis: i64,
) -> ConsumerLagSummary {
    ConsumerLagSummary {
        cluster: cluster.name.clone(),
        topic: topic.to_string(),
        consumer_group: consumer_group.to_string(),
        total_lag: lag.total_lag,
        consume_tps: lag.consume_tps,
        inflight_total: lag.inflight_total,
        delay: ConsumeDelay::measure(&lag.queues, now_millis),
        queues: QueueLagBreakdown::from_queues(lag.queues),
        generated_at: observed_at(),
    }
}

/// Keeps every route row of a Topic as diagnosis evidence.
pub(super) fn topic_route_summary(
    cluster: &ResolvedCluster,
    topic: &str,
    route: SessionTopicRoute,
) -> TopicRouteSummary {
    TopicRouteSummary {
        cluster: cluster.name.clone(),
        topic: topic.to_string(),
        read_queue_count: route.queues.iter().map(|queue| queue.read_queue_nums).sum(),
        write_queue_count: route.queues.iter().map(|queue| queue.write_queue_nums).sum(),
        brokers: route.brokers,
        queues: route.queues,
        generated_at: observed_at(),
    }
}
