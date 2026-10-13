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

//! The Consumer Group diagnosis: connections, configuration, progress and retries of one group,
//! read from one session inside one request budget.

use std::time::Duration;

use super::diagnosis_support::note;
use super::diagnosis_support::system_topic_activity;
use super::diagnosis_support::wrap_report;
use super::diagnosis_support::SupportingBudget;
use super::diagnosis_support::DEAD_LETTER_TOPIC_PREFIX;
use super::diagnosis_support::RETRY_TOPIC_PREFIX;
use super::normalized_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::admin_session::SessionConsumerProgress;
use crate::model::contract::now_millis;
use crate::model::contract::observed_at;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::consumer_group_diagnosis;
use crate::service::consumer_group_diagnosis::ConsumerGroupEvidence;
use crate::service::consumer_group_diagnosis::ConsumerProgressSummary;
use crate::service::consumer_group_diagnosis::TopicLag;
use crate::service::diagnosis_collector::ConsumeDelay;
use crate::service::diagnosis_collector::QueueLagBreakdown;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_rules::ConsumerLagPolicy;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::diagnosis_tools::DiagnoseConsumerGroupArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn diagnose_consumer_group(
        &self,
        mut args: DiagnoseConsumerGroupArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolFailure> {
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key(
            "diagnose_consumer_group",
            &cluster.name,
            &format!("group={}", args.consumer_group),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        let policy = ConsumerLagPolicy::from(&self.config.diagnosis);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                move || async {
                    let budget = SupportingBudget::new(&self.control);
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(diagnose_in_session(session, cluster, args, policy, budget))
                    })
                    .await
                },
            )
            .await
    }
}

/// Collects the evidence and evaluates the rules on it.
///
/// Whether the group can consume and how far behind it is are what the diagnosis is about, so the
/// group details and the progress may take the whole request budget. The retry and dead-letter
/// Topics share the supporting budget. No read fails the diagnosis.
async fn diagnose_in_session<S>(
    session: &mut S,
    cluster: &ResolvedCluster,
    args: DiagnoseConsumerGroupArgs,
    policy: ConsumerLagPolicy,
    mut budget: SupportingBudget,
) -> Result<QueryPayload<DiagnosisReport>, ToolFailure>
where
    S: AdminSession,
{
    let mut completeness = QueryCompleteness::default();
    let now = now_millis();
    let group_name = args.consumer_group.as_str();

    let group = note(
        &mut completeness,
        QuerySource::ConsumerGroupConfig,
        group_name,
        session.consumer_group_details(group_name).await,
    )
    .map(|details| {
        let sources_complete = !details.partial;
        ConsumerGroupState::from_details(details.data, sources_complete)
    });
    let group_is_absent =
        group.as_ref().err().and_then(ToolFailure::not_found_entity) == Some(NotFoundEntity::ConsumerGroup);
    let progress = if group_is_absent {
        // No Broker holds the configuration of the group, so none holds progress for it either.
        Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup))
    } else {
        note(
            &mut completeness,
            QuerySource::ConsumerStatistics,
            group_name,
            session.consumer_progress(group_name).await,
        )
        .map(|progress| progress_summary(cluster, group_name, progress.data, now))
    };

    let retry_topic = format!("{RETRY_TOPIC_PREFIX}{group_name}");
    let retry = budget.read(session.topic_stats(&retry_topic)).await;
    let retry = system_topic_activity(&mut completeness, cluster, retry_topic, retry, now);
    let dead_letter_topic = format!("{DEAD_LETTER_TOPIC_PREFIX}{group_name}");
    let dead_letter = budget.read(session.topic_stats(&dead_letter_topic)).await;
    let dead_letter = system_topic_activity(&mut completeness, cluster, dead_letter_topic, dead_letter, now);

    let report = consumer_group_diagnosis::evaluate(
        &args,
        ConsumerGroupEvidence {
            group,
            progress,
            retry,
            dead_letter,
        },
        &policy,
    );
    Ok(wrap_report(completeness, report))
}

/// Summarizes the progress of a group over every Topic as diagnosis evidence.
fn progress_summary(
    cluster: &ResolvedCluster,
    consumer_group: &str,
    progress: SessionConsumerProgress,
    now_millis: i64,
) -> ConsumerProgressSummary {
    let saturating = |count: u64| i64::try_from(count).unwrap_or(i64::MAX);
    let queues = progress
        .queues
        .into_iter()
        .map(|row| QueueLag {
            topic: row.topic,
            broker_name: row.broker_name,
            queue_id: row.queue_id,
            broker_offset: row.broker_offset,
            consumer_offset: row.consumer_offset,
            lag: saturating(row.lag),
            inflight: saturating(row.inflight),
            last_observed_at: row.last_observed_at,
            client_ip: None,
        })
        .collect::<Vec<_>>();
    let delay = ConsumeDelay::measure(&queues, now_millis);
    let topics = TopicLag::worst(&queues);
    let mut breakdown = QueueLagBreakdown::from_queues(queues);
    // The totals of the source cover every queue, also those beyond the rows it returned.
    breakdown.queue_count = progress.queue_count;
    breakdown.max_queue_lag = saturating(progress.max_queue_lag);
    ConsumerProgressSummary {
        cluster: cluster.name.clone(),
        consumer_group: consumer_group.to_string(),
        state: progress.state,
        topic_count: progress.topic_count,
        total_lag: saturating(progress.total_lag),
        consume_tps: progress.consume_tps,
        inflight_total: saturating(progress.total_inflight),
        delay,
        queues: breakdown,
        topics,
        truncated: progress.truncated,
        generated_at: observed_at(),
    }
}
