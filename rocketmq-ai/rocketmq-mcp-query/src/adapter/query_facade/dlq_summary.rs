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

//! How many messages of a Consumer Group were dead-lettered, and when the last one was.
//!
//! The summary is what the statistics of the dead-letter Topic say. No message is read.

use std::collections::BTreeMap;
use std::time::Duration;

use super::completeness_for_error;
use super::diagnosis_support::DEAD_LETTER_TOPIC_PREFIX;
use super::normalized_identifier;
use super::normalized_logical_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::SessionTopicStats;
use crate::infrastructure::snapshot::RetainedSize;
use crate::model::contract::millis_from_observed_at;
use crate::model::contract::now_millis;
use crate::model::contract::observed_at;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::model::contract::QuerySource;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::tools::consumer_tools::DlqBrokerSummary;
use crate::tools::consumer_tools::GetDlqSummaryArgs;
use crate::tools::consumer_tools::GetDlqSummaryOutput;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl RetainedSize for DlqBrokerSummary {
    fn retained_heap_size(&self) -> usize {
        self.broker_name
            .retained_heap_size()
            .saturating_add(self.last_message_at.retained_heap_size())
    }
}

impl RetainedSize for GetDlqSummaryOutput {
    fn retained_heap_size(&self) -> usize {
        self.cluster
            .retained_heap_size()
            .saturating_add(self.consumer_group.retained_heap_size())
            .saturating_add(self.dlq_topic.retained_heap_size())
            .saturating_add(self.last_message_at.retained_heap_size())
            .saturating_add(self.brokers.retained_heap_size())
            .saturating_add(self.generated_at.retained_heap_size())
    }
}

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Reads the statistics of the dead-letter Topic of a group.
    ///
    /// The Topic exists only once a message was dead-lettered, so a lookup that finds none is an
    /// answer: zero dead letters. A name that is no Consumer Group has no such Topic either, and
    /// the summary would then say "zero" about nothing. The group is therefore looked up when
    /// the Topic is absent, and only then.
    pub(crate) async fn dlq_summary(
        &self,
        mut args: GetDlqSummaryArgs,
    ) -> Result<QueryResult<GetDlqSummaryOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key(
            "dlq_summary",
            &cluster.name,
            &format!("consumer_group={}", args.consumer_group),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let consumer_group = args.consumer_group;
                            let dlq_topic = format!("{DEAD_LETTER_TOPIC_PREFIX}{consumer_group}");
                            match session.topic_stats(&dlq_topic).await {
                                Ok(stats) => {
                                    Ok(stats.map(|stats| summarize(&cluster.name, consumer_group, dlq_topic, &stats)))
                                }
                                Err(error) if error.not_found_entity() == Some(NotFoundEntity::Topic) => {
                                    let absent = absent(&cluster.name, consumer_group.clone(), dlq_topic);
                                    match session.consumer_group_details(&consumer_group).await {
                                        Ok(_) => Ok(QueryPayload::complete(absent)),
                                        Err(error) if error.not_found_entity().is_some() => Err(error),
                                        // The Topic is known to be absent. Whether the group
                                        // exists is the part that could not be read.
                                        Err(error) => Ok(completeness_for_error(
                                            QuerySource::ConsumerGroupConfig,
                                            &consumer_group,
                                            &error,
                                        )
                                        .wrap(absent)),
                                    }
                                }
                                Err(error) => Err(error),
                            }
                        })
                    })
                    .await
                },
            )
            .await
    }
}

fn absent(cluster: &str, consumer_group: String, dlq_topic: String) -> GetDlqSummaryOutput {
    GetDlqSummaryOutput {
        cluster: cluster.to_string(),
        consumer_group,
        dlq_topic,
        exists: false,
        queue_count: 0,
        message_count: 0,
        last_message_at: None,
        last_message_age_ms: None,
        brokers: Vec::new(),
        truncated: false,
        generated_at: observed_at(),
    }
}

fn summarize(
    cluster: &str,
    consumer_group: String,
    dlq_topic: String,
    stats: &SessionTopicStats,
) -> GetDlqSummaryOutput {
    let total = TopicActivity::from_stats(
        cluster,
        dlq_topic,
        stats.queue_count,
        stats.total_message_count,
        &stats.queues,
        now_millis(),
    );
    // The newest store time of a Broker, next to the text it was reported as.
    let mut brokers = BTreeMap::<&str, (DlqBrokerSummary, i64)>::new();
    for queue in &stats.queues {
        let (broker, newest) = brokers.entry(queue.broker_name.as_str()).or_insert_with(|| {
            (
                DlqBrokerSummary {
                    broker_name: queue.broker_name.clone(),
                    queue_count: 0,
                    message_count: 0,
                    last_message_at: None,
                },
                0,
            )
        });
        broker.queue_count += 1;
        broker.message_count = broker.message_count.saturating_add(queue.message_count);
        // A queue that never held a message reports the Unix epoch, which is not a store time.
        let stored_at = queue
            .last_update_at
            .as_deref()
            .and_then(|text| Some((millis_from_observed_at(text)?, text)));
        if let Some((millis, text)) = stored_at.filter(|(millis, _)| *millis > *newest) {
            *newest = millis;
            broker.last_message_at = Some(text.to_string());
        }
    }
    GetDlqSummaryOutput {
        cluster: total.cluster,
        consumer_group,
        dlq_topic: total.topic,
        exists: true,
        queue_count: total.queue_count,
        message_count: total.message_count,
        last_message_at: total.last_message_at,
        last_message_age_ms: total.last_message_age_ms,
        brokers: brokers.into_values().map(|(broker, _)| broker).collect(),
        truncated: stats.truncated,
        generated_at: total.generated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::super::observation_fixture::config;
    use super::super::observation_fixture::StubFactory;
    use super::*;
    use crate::model::contract::CacheStatus;
    use crate::model::contract::SourceFailureCode;
    use crate::tools::consumer_tools::GetConsumerGroupDetailsOutput;
    use crate::tools::executor::ToolExecutionError;
    use crate::tools::topic_tools::TopicStatsQueueRow;

    fn args(consumer_group: &str) -> GetDlqSummaryArgs {
        GetDlqSummaryArgs {
            cluster: "local-dev".to_string(),
            consumer_group: consumer_group.to_string(),
        }
    }

    fn queue(broker_name: &str, queue_id: i32, message_count: u64, last_update_at: &str) -> TopicStatsQueueRow {
        TopicStatsQueueRow {
            broker_name: broker_name.to_string(),
            queue_id,
            min_offset: 0,
            max_offset: i64::try_from(message_count).unwrap(),
            message_count,
            last_update_at: Some(last_update_at.to_string()),
        }
    }

    fn stats() -> SessionTopicStats {
        SessionTopicStats {
            total_message_count: 12,
            queue_count: 3,
            queues: vec![
                queue("broker-b", 0, 5, "2026-10-01T00:00:05.000Z"),
                queue("broker-a", 0, 7, "2026-10-01T00:00:01.000Z"),
                // A queue that never held a message reports the Unix epoch.
                queue("broker-a", 1, 0, "1970-01-01T00:00:00.000Z"),
            ],
            truncated: false,
        }
    }

    fn details() -> GetConsumerGroupDetailsOutput {
        GetConsumerGroupDetailsOutput {
            cluster: "local-dev".to_string(),
            consumer_group: "workers".to_string(),
            total_connection_count: 0,
            brokers: Vec::new(),
            generated_at: "1".to_string(),
        }
    }

    fn topic_not_found() -> ToolFailure {
        ToolFailure::not_found(NotFoundEntity::Topic)
    }

    #[tokio::test]
    async fn the_summary_counts_what_the_dead_letter_topic_holds_and_reads_no_message() {
        let factory = StubFactory {
            topic_stats: Some(Ok(QueryPayload::complete(stats()))),
            ..Default::default()
        };
        let facade = QueryFacade::with_factory(config(), factory.clone());

        let first = facade.dlq_summary(args(" workers ")).await.unwrap();
        let second = facade.dlq_summary(args("workers")).await.unwrap();

        let summary = &first.data;
        assert_eq!(summary.dlq_topic, "%DLQ%workers");
        assert_eq!(*factory.asked_topics.lock().unwrap(), ["%DLQ%workers"]);
        assert!(summary.exists && !summary.truncated);
        assert_eq!((summary.queue_count, summary.message_count), (3, 12));
        assert_eq!(summary.last_message_at.as_deref(), Some("2026-10-01T00:00:05.000Z"));
        assert!(summary.last_message_age_ms.is_some());
        assert_eq!(
            summary.brokers,
            [
                DlqBrokerSummary {
                    broker_name: "broker-a".to_string(),
                    queue_count: 2,
                    message_count: 7,
                    last_message_at: Some("2026-10-01T00:00:01.000Z".to_string()),
                },
                DlqBrokerSummary {
                    broker_name: "broker-b".to_string(),
                    queue_count: 1,
                    message_count: 5,
                    last_message_at: Some("2026-10-01T00:00:05.000Z".to_string()),
                },
            ]
        );
        // The Topic was found, so nothing else was read, and the answer is kept for a while.
        assert_eq!(factory.used(), (1, 1, 1));
        assert_eq!(
            (first.cache_status, second.cache_status),
            (CacheStatus::Miss, CacheStatus::Hit)
        );
    }

    #[tokio::test]
    async fn a_group_without_a_dead_letter_topic_has_zero_dead_letters() {
        let factory = StubFactory {
            topic_stats: Some(Err(topic_not_found())),
            consumer_group_details: Some(Ok(QueryPayload::complete(details()))),
            ..Default::default()
        };
        let facade = QueryFacade::with_factory(config(), factory.clone());

        let result = facade.dlq_summary(args("workers")).await.unwrap();

        assert!(!result.data.exists && !result.partial);
        assert_eq!((result.data.queue_count, result.data.message_count), (0, 0));
        assert!(result.data.brokers.is_empty());
        assert_eq!(result.data.last_message_at, None);
        assert_eq!(factory.used(), (1, 1, 2));
    }

    #[tokio::test]
    async fn a_name_that_is_no_group_is_not_reported_as_zero_dead_letters() {
        let unknown = StubFactory {
            topic_stats: Some(Err(topic_not_found())),
            consumer_group_details: Some(Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup))),
            ..Default::default()
        };
        let error = QueryFacade::with_factory(config(), unknown)
            .dlq_summary(args("wrokers"))
            .await
            .unwrap_err();
        assert_eq!(error.not_found_entity(), Some(NotFoundEntity::ConsumerGroup));

        // When the group could not be looked up, the answer says so instead of claiming it.
        let unreadable = StubFactory {
            topic_stats: Some(Err(topic_not_found())),
            consumer_group_details: Some(Err(ToolFailure::Operational(ToolExecutionError::Backend(None)))),
            ..Default::default()
        };
        let result = QueryFacade::with_factory(config(), unreadable)
            .dlq_summary(args("workers"))
            .await
            .unwrap();
        assert!(!result.data.exists && result.partial);
        assert_eq!(result.source_failures.len(), 1);
        assert_eq!(result.source_failures[0].source, QuerySource::ConsumerGroupConfig);
        assert_eq!(result.source_failures[0].code, SourceFailureCode::SourceUnavailable);
    }

    #[tokio::test]
    async fn statistics_that_cannot_be_read_are_an_error_and_partial_ones_stay_partial() {
        let down = StubFactory::default();
        let error = QueryFacade::with_factory(config(), down.clone())
            .dlq_summary(args("workers"))
            .await
            .unwrap_err();
        assert_eq!(error.code(), "source_unavailable");
        // The group is not looked up when the Topic was not proven absent.
        assert_eq!(down.used(), (1, 1, 1));

        let failure = crate::model::contract::SourceFailure::new(
            QuerySource::TopicStats,
            SourceFailureCode::Timeout,
            true,
            "broker-b",
        );
        let partial = StubFactory {
            topic_stats: Some(Ok(QueryPayload::new(stats(), true, Vec::new(), vec![failure.clone()]))),
            ..Default::default()
        };
        let result = QueryFacade::with_factory(config(), partial)
            .dlq_summary(args("workers"))
            .await
            .unwrap();
        assert!(result.partial && result.data.exists);
        assert_eq!(result.source_failures, [failure]);
    }
}
