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

//! The message delivery diagnosis: the path from one Topic to one Consumer Group, and optionally
//! one message on it, read from one session inside one request budget.

use std::time::Duration;

use super::alias_error;
use super::completeness_for_error;
use super::diagnosis_support::consumer_lag_summary;
use super::diagnosis_support::note;
use super::diagnosis_support::topic_route_summary;
use super::diagnosis_support::wrap_report;
use super::diagnosis_support::SupportingBudget;
use super::message_metadata_output;
use super::normalized_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::identifier_alias::IdentifierAliaser;
use crate::model::contract::now_millis;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::model::diagnosis::DiagnosisReport;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::service::diagnosis_rules::ConsumerLagPolicy;
use crate::service::message_delivery_diagnosis;
use crate::service::message_delivery_diagnosis::DeliveryTarget;
use crate::service::message_delivery_diagnosis::MessageDeliveryEvidence;
use crate::service::message_delivery_diagnosis::MessageObservation;
use crate::tools::diagnosis_tools::DiagnoseMessageDeliveryArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// What one delivery diagnosis collects evidence for.
///
/// It is the only place that holds the identifier of the message as the caller sent it. That
/// identifier is used for the lookup and for nothing else, so this type is not `Debug`.
struct DeliveryRequest {
    target: DeliveryTarget,
    message_id: Option<String>,
    aliases: IdentifierAliaser,
    policy: ConsumerLagPolicy,
}

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn diagnose_message_delivery(
        &self,
        args: DiagnoseMessageDeliveryArgs,
    ) -> Result<QueryResult<DiagnosisReport>, ToolFailure> {
        let topic = normalized_identifier("topic", &args.topic)?;
        let consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let message_id = args
            .message_id
            .as_deref()
            .map(|message_id| normalized_identifier("message_id", message_id))
            .transpose()?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        // The cache key and the report name the message by its pseudonym.
        let message_alias = message_id
            .as_deref()
            .map(|message_id| self.aliases.message_alias(message_id))
            .transpose()
            .map_err(alias_error)?;
        let key = self.cache_key(
            "diagnose_message_delivery",
            &cluster.name,
            &format!(
                "topic={topic}|group={consumer_group}|message={}",
                message_alias.as_deref().unwrap_or("-")
            ),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        let request = DeliveryRequest {
            target: DeliveryTarget {
                cluster: cluster.name.clone(),
                topic,
                consumer_group,
                message_alias,
            },
            message_id,
            aliases: self.aliases.clone(),
            policy: ConsumerLagPolicy::from(&self.config.diagnosis),
        };
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                move || async {
                    let budget = SupportingBudget::new(&self.control);
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(diagnose_in_session(session, cluster, request, budget))
                    })
                    .await
                },
            )
            .await
    }
}

/// Whether a completed lookup established that `entity` does not exist.
fn is_absent<T>(result: &Result<T, ToolFailure>, entity: NotFoundEntity) -> bool {
    result.as_ref().err().and_then(ToolFailure::not_found_entity) == Some(entity)
}

/// Collects the evidence and evaluates the rules on it.
///
/// The Topic route says whether the Topic exists and who may use it, so it may take the whole
/// request budget. Everything else shares the supporting budget, in the order the rules need it:
/// the group, its progress on the Topic, the message when one was asked about, and what the Topic
/// holds. What does not exist is not asked about. No read fails the diagnosis.
async fn diagnose_in_session<S>(
    session: &mut S,
    cluster: &ResolvedCluster,
    request: DeliveryRequest,
    mut budget: SupportingBudget,
) -> Result<QueryPayload<DiagnosisReport>, ToolFailure>
where
    S: AdminSession,
{
    let DeliveryRequest {
        target,
        message_id,
        aliases,
        policy,
    } = request;
    let mut completeness = QueryCompleteness::default();
    let now = now_millis();
    let topic = target.topic.as_str();
    let group_name = target.consumer_group.as_str();

    let route = session
        .topic_route(topic)
        .await
        .map(|route| topic_route_summary(cluster, topic, route))
        .inspect_err(|error| {
            completeness.merge(completeness_for_error(QuerySource::TopicRoute, topic, error));
        });
    let topic_is_absent = is_absent(&route, NotFoundEntity::Topic);

    let group = note(
        &mut completeness,
        QuerySource::ConsumerGroupConfig,
        group_name,
        budget.read(session.consumer_group_details(group_name)).await,
    )
    .map(|details| {
        let sources_complete = !details.partial;
        ConsumerGroupState::from_details(details.data, sources_complete)
    });
    let lag = if topic_is_absent {
        Err(ToolFailure::not_found(NotFoundEntity::Topic))
    } else if is_absent(&group, NotFoundEntity::ConsumerGroup) {
        Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup))
    } else {
        note(
            &mut completeness,
            QuerySource::ConsumerStatistics,
            group_name,
            budget.read(session.consumer_lag(topic, group_name)).await,
        )
    };
    let message = match &message_id {
        Some(message_id) => Some(
            budget
                .read(session.message_metadata(message_id))
                .await
                .and_then(|metadata| message_metadata_output(&aliases, cluster, metadata))
                .map(|metadata| {
                    // Offsets that a Broker did not report could be those of the queue that holds
                    // the message, so only complete offsets say where the message stands.
                    let group_queues = lag
                        .as_ref()
                        .ok()
                        .filter(|lag| !lag.partial)
                        .map(|lag| lag.data.queues.as_slice());
                    MessageObservation::observe(metadata, topic, group_queues, now)
                }),
        ),
        None => None,
    };
    let lag = lag.map(|lag| consumer_lag_summary(cluster, topic, group_name, lag.data, now));
    let activity = if topic_is_absent {
        Err(ToolFailure::not_found(NotFoundEntity::Topic))
    } else {
        note(
            &mut completeness,
            QuerySource::TopicStats,
            topic,
            budget.read(session.topic_stats(topic)).await,
        )
        .map(|stats| {
            TopicActivity::from_stats(
                &cluster.name,
                topic.to_string(),
                stats.data.queue_count,
                stats.data.total_message_count,
                &stats.data.queues,
                now,
            )
        })
    };

    let report = message_delivery_diagnosis::evaluate(
        &target,
        MessageDeliveryEvidence {
            route,
            group,
            lag,
            activity,
            message,
        },
        &policy,
    );
    Ok(wrap_report(completeness, report))
}
