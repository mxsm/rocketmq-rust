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

//! The consumer lag diagnosis: all of its evidence from one session, inside one request budget.

use std::time::Duration;

use super::completeness_for_error;
use super::describe_broker_in_session;
use super::diagnosis_support::consumer_lag_summary;
use super::diagnosis_support::note;
use super::diagnosis_support::system_topic_activity;
use super::diagnosis_support::topic_route_summary;
use super::diagnosis_support::wrap_report;
use super::diagnosis_support::SupportingBudget;
use super::diagnosis_support::DEAD_LETTER_TOPIC_PREFIX;
use super::diagnosis_support::RETRY_TOPIC_PREFIX;
use super::normalized_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::ResolvedCluster;
use crate::model::contract::now_millis;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::diagnosis_collector::ConsumerLagEvidence;
use crate::service::diagnosis_collector::ConsumerLagSummary;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_evidence::TopicConfigSummary;
use crate::service::diagnosis_rules;
use crate::service::diagnosis_rules::ConsumerLagPolicy;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn diagnose_consumer_lag(
        &self,
        mut args: DiagnoseConsumerLagArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolFailure> {
        args.topic = normalized_identifier("topic", &args.topic)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key(
            "diagnose_consumer_lag",
            &cluster.name,
            &format!("topic={}|group={}", args.topic, args.consumer_group),
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
/// Consumer lag and the Topic route are read first and may take the whole request budget: they
/// say whether there is a backlog, and whether the Topic and the group exist at all. Everything
/// else explains a backlog and shares the supporting budget, the evidence most rules need first.
/// No read fails the diagnosis; a failed read is missing evidence in the report.
async fn diagnose_in_session<S>(
    session: &mut S,
    cluster: &ResolvedCluster,
    args: DiagnoseConsumerLagArgs,
    policy: ConsumerLagPolicy,
    mut budget: SupportingBudget,
) -> Result<QueryPayload<DiagnosisReport>, ToolFailure>
where
    S: AdminSession,
{
    let mut completeness = QueryCompleteness::default();
    let now = now_millis();

    let lag = note(
        &mut completeness,
        QuerySource::ConsumerStatistics,
        &args.consumer_group,
        session.consumer_lag(&args.topic, &args.consumer_group).await,
    )
    .map(|lag| consumer_lag_summary(cluster, &args.topic, &args.consumer_group, lag.data, now));
    let route = session
        .topic_route(&args.topic)
        .await
        .map(|route| topic_route_summary(cluster, &args.topic, route))
        .inspect_err(|error| {
            completeness.merge(completeness_for_error(QuerySource::TopicRoute, &args.topic, error));
        });

    let group = note(
        &mut completeness,
        QuerySource::ConsumerGroupConfig,
        &args.consumer_group,
        budget.read(session.consumer_group_details(&args.consumer_group)).await,
    )
    .map(|details| {
        let sources_complete = !details.partial;
        ConsumerGroupState::from_details(details.data, sources_complete)
    });
    let topic_config = note(
        &mut completeness,
        QuerySource::TopicConfig,
        &args.topic,
        budget.read(session.topic_config(&args.topic)).await,
    )
    .map(|config| TopicConfigSummary::from_config(config.data));
    let broker = match lag.as_ref().ok().and_then(ConsumerLagSummary::top_lag_broker) {
        Some(broker_name) => Some(
            note(
                &mut completeness,
                QuerySource::BrokerRuntime,
                broker_name,
                budget
                    .read(describe_broker_in_session(session, cluster, broker_name.to_string()))
                    .await,
            )
            .map(|broker| broker.data),
        ),
        None => None,
    };
    let retry_topic = format!("{RETRY_TOPIC_PREFIX}{}", args.consumer_group);
    let retry = budget.read(session.topic_stats(&retry_topic)).await;
    let retry = system_topic_activity(&mut completeness, cluster, retry_topic, retry, now);
    let dead_letter_topic = format!("{DEAD_LETTER_TOPIC_PREFIX}{}", args.consumer_group);
    let dead_letter = budget.read(session.topic_stats(&dead_letter_topic)).await;
    let dead_letter = system_topic_activity(&mut completeness, cluster, dead_letter_topic, dead_letter, now);

    let report = diagnosis_rules::evaluate(
        &args,
        ConsumerLagEvidence {
            lag,
            route,
            broker,
            group,
            topic_config,
            retry,
            dead_letter,
        },
        &policy,
    );
    Ok(wrap_report(completeness, report))
}
