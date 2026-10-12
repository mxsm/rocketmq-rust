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

//! The Consumer Groups that consume one Topic, a page at a time.

use super::normalized_identifier;
use super::normalized_logical_identifier;
use super::query_result_from_snapshot;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::SessionTopicConsumers;
use crate::infrastructure::snapshot::SnapshotKind;
use crate::infrastructure::snapshot::SnapshotRequest;
use crate::infrastructure::snapshot::SnapshotSelectionMode;
use crate::infrastructure::snapshot::SnapshotWeight;
use crate::model::contract::Page;
use crate::model::contract::QueryResult;
use crate::tools::executor::ToolFailure;
use crate::tools::topic_tools::ListTopicConsumersArgs;
use crate::tools::topic_tools::ListTopicConsumersOutput;
use crate::tools::topic_tools::TopicConsumerGroup;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Reads the consumers of a Topic once and pages that one observation, so a continuation
    /// never mixes two reads. The group names change with the clients, like a lag, and are
    /// current for as long.
    pub(crate) async fn topic_consumers(
        &self,
        mut args: ListTopicConsumersArgs,
    ) -> Result<QueryResult<ListTopicConsumersOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let request = SnapshotRequest::try_new_with_selection(
            SnapshotKind::TopicConsumers,
            cluster.name.clone(),
            format!("topic={}", args.topic),
            SnapshotSelectionMode::ExactIdentifier,
            &args.page,
            self.visibility_class.as_str(),
        )?;
        let topic = args.topic.clone();
        let snapshot = self
            .snapshots
            .get_or_load(
                request,
                args.page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |consumers: &SessionTopicConsumers| SnapshotWeight::detail(consumers.groups.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster.clone(), move |session, _| {
                        Box::pin(async move { session.topic_consumers(&topic).await })
                    })
                },
            )
            .await?;
        let consumers = &snapshot.payload.data;
        let names = self.snapshots.page(&snapshot, &consumers.groups)?;
        let page = Page {
            count: names.count,
            total_count: names.total_count,
            has_more: names.has_more,
            next_cursor: names.next_cursor,
            items: names
                .items
                .into_iter()
                .map(|group| TopicConsumerGroup { group })
                .collect(),
        };
        let payload = snapshot.payload.completeness().wrap(ListTopicConsumersOutput {
            cluster: cluster.name,
            topic: args.topic,
            truncated: consumers.truncated,
            page,
            generated_at: snapshot.observed_at.clone(),
        });
        Ok(query_result_from_snapshot(&snapshot, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::super::observation_fixture::config;
    use super::super::observation_fixture::StubFactory;
    use super::*;
    use crate::model::contract::PageRequest;
    use crate::model::contract::QueryPayload;
    use crate::model::contract::QuerySource;
    use crate::model::contract::SourceFailure;
    use crate::model::contract::SourceFailureCode;

    fn args(topic: &str, limit: u32, cursor: Option<String>) -> ListTopicConsumersArgs {
        ListTopicConsumersArgs {
            cluster: "local-dev".to_string(),
            topic: topic.to_string(),
            page: PageRequest {
                limit: Some(limit),
                cursor,
            },
        }
    }

    fn consumers(truncated: bool) -> SessionTopicConsumers {
        SessionTopicConsumers {
            groups: ["audit", "billing", "workers"].map(str::to_string).to_vec(),
            truncated,
        }
    }

    #[tokio::test]
    async fn pages_of_one_topic_share_one_read_and_carry_its_evidence() {
        let failure = SourceFailure::new(
            QuerySource::SubscriptionGroups,
            SourceFailureCode::Timeout,
            true,
            "broker-b",
        );
        let factory = StubFactory {
            topic_consumers: Some(QueryPayload::new(
                consumers(true),
                true,
                vec!["topic_subscribers_truncated".to_string()],
                vec![failure.clone()],
            )),
            ..Default::default()
        };
        let facade = QueryFacade::with_factory(config(), factory.clone());

        let first = facade.topic_consumers(args(" orders ", 2, None)).await.unwrap();
        let cursor = first.data.page.next_cursor.clone().unwrap();
        let second = facade
            .topic_consumers(args("orders", 2, Some(cursor.clone())))
            .await
            .unwrap();

        assert_eq!(first.data.cluster, "local-dev");
        assert_eq!(first.data.topic, "orders");
        let names =
            |page: &Page<TopicConsumerGroup>| page.items.iter().map(|item| item.group.clone()).collect::<Vec<_>>();
        assert_eq!(names(&first.data.page), ["audit", "billing"]);
        assert_eq!(names(&second.data.page), ["workers"]);
        assert_eq!(first.data.page.total_count, 3);
        assert!(first.data.page.has_more && !second.data.page.has_more);
        // Both pages say what the one read could not see.
        for page in [&first, &second] {
            assert!(page.data.truncated && page.partial);
            assert_eq!(
                page.warnings,
                ["source_failures_present", "topic_subscribers_truncated"]
            );
            assert_eq!(page.source_failures, std::slice::from_ref(&failure));
        }
        assert_eq!(factory.used(), (1, 1, 1));

        // A cursor continues the Topic it was issued for and no other.
        assert!(facade.topic_consumers(args("payments", 2, Some(cursor))).await.is_err());
        assert_eq!(factory.used(), (1, 1, 1));
    }

    #[tokio::test]
    async fn a_topic_nobody_consumes_is_an_empty_page_and_a_blank_name_reads_nothing() {
        let factory = StubFactory {
            topic_consumers: Some(QueryPayload::complete(SessionTopicConsumers {
                groups: Vec::new(),
                truncated: false,
            })),
            ..Default::default()
        };
        let facade = QueryFacade::with_factory(config(), factory.clone());

        let empty = facade.topic_consumers(args("orders", 10, None)).await.unwrap();
        assert!(empty.data.page.items.is_empty());
        assert_eq!(empty.data.page.total_count, 0);
        assert!(!empty.partial && !empty.data.truncated);
        assert_eq!(factory.used(), (1, 1, 1));

        for (cluster, topic) in [
            ("local-dev", "  "),
            ("unconfigured", "orders"),
            ("127.0.0.1:9876", "orders"),
        ] {
            let mut rejected = args(topic, 10, None);
            rejected.cluster = cluster.to_string();
            assert!(facade.topic_consumers(rejected).await.is_err(), "{cluster} {topic}");
        }
        assert_eq!(factory.used(), (1, 1, 1));
    }

    #[tokio::test]
    async fn a_read_that_fails_is_an_error_and_closes_its_session() {
        let factory = StubFactory::default();
        let facade = QueryFacade::with_factory(config(), factory.clone());

        assert!(facade.topic_consumers(args("orders", 10, None)).await.is_err());
        assert_eq!(factory.used(), (1, 1, 1));
    }
}
