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

//! What one connected consumer client reports about itself, named by its pseudonym.

use super::normalized_identifier;
use super::normalized_logical_identifier;
use super::query_result_from_snapshot;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::SessionConsumerClientRuntime;
use crate::infrastructure::snapshot::RetainedSize;
use crate::infrastructure::snapshot::SnapshotKind;
use crate::infrastructure::snapshot::SnapshotRequest;
use crate::infrastructure::snapshot::SnapshotSelectionMode;
use crate::infrastructure::snapshot::SnapshotWeight;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::tools::consumer_tools::is_client_alias;
use crate::tools::consumer_tools::GetConsumerRunningInfoArgs;
use crate::tools::consumer_tools::GetConsumerRunningInfoOutput;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolExecutionError;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// One client as it was found and asked: the Broker that relayed the request, and the answer.
#[derive(Debug, Clone, PartialEq)]
struct ObservedClient {
    broker_name: String,
    runtime: SessionConsumerClientRuntime,
}

impl RetainedSize for ObservedClient {
    fn retained_heap_size(&self) -> usize {
        self.broker_name
            .retained_heap_size()
            .saturating_add(self.runtime.retained_heap_size())
    }
}

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Resolves the pseudonym against the connections of the group and asks that one client.
    ///
    /// A caller never names a client by its identifier: the pseudonym is matched here, in the
    /// session that then asks the client, and the identifier does not leave that session. The
    /// queue rows of one answer are paged, so a continuation never mixes two answers.
    pub(crate) async fn consumer_running_info(
        &self,
        mut args: GetConsumerRunningInfoArgs,
    ) -> Result<QueryResult<GetConsumerRunningInfoOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        if !is_client_alias(&args.client_alias) {
            return Err(ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None }));
        }
        let cluster = self.resolve_cluster(&args.cluster)?;
        let request = SnapshotRequest::try_new_with_selection(
            SnapshotKind::ConsumerRunningInfo,
            cluster.name.clone(),
            format!("consumer_group={}|client={}", args.consumer_group, args.client_alias),
            SnapshotSelectionMode::ExactIdentifier,
            &args.page,
            self.visibility_class.as_str(),
        )?;
        let consumer_group = args.consumer_group.clone();
        let client_alias = args.client_alias.clone();
        let aliases = self.aliases.clone();
        let snapshot = self
            .snapshots
            .get_or_load(
                request,
                args.page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |client: &ObservedClient| SnapshotWeight::detail(client.runtime.queues.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster.clone(), move |session, _| {
                        Box::pin(async move {
                            let connections = session.consumer_connections(&consumer_group).await?;
                            let connected = connections.data.rows.iter().find(|row| {
                                aliases
                                    .client_alias(&row.client_id, &row.client_addr)
                                    .is_ok_and(|alias| alias == client_alias)
                            });
                            let Some(connected) = connected else {
                                // A Broker that did not answer, or rows beyond the bound of one
                                // read, may hold the client: only a complete list proves it gone.
                                let inconclusive = connections.partial || connections.data.truncated;
                                return Err(if inconclusive {
                                    ToolFailure::Operational(ToolExecutionError::Backend(None))
                                } else {
                                    ToolFailure::not_found(NotFoundEntity::Client)
                                });
                            };
                            let broker_name = connected.broker_name.clone();
                            let client_id = connected.client_id.clone();
                            let runtime = session
                                .consumer_client_runtime(&consumer_group, &broker_name, &client_id)
                                .await?;
                            Ok(QueryPayload::complete(ObservedClient { broker_name, runtime }))
                        })
                    })
                },
            )
            .await?;
        let client = &snapshot.payload.data;
        let page = self.snapshots.page(&snapshot, &client.runtime.queues)?;
        let payload = snapshot.payload.completeness().wrap(GetConsumerRunningInfoOutput {
            cluster: cluster.name,
            consumer_group: args.consumer_group,
            client_alias: args.client_alias,
            broker_name: client.broker_name.clone(),
            consume_type: client.runtime.consume_type.clone(),
            consume_orderly: client.runtime.consume_orderly,
            thread_pool_core_size: client.runtime.thread_pool_core_size,
            client_version: client.runtime.client_version.clone(),
            started_at: client.runtime.started_at.clone(),
            subscriptions: client.runtime.subscriptions.clone(),
            topic_status: client.runtime.topic_status.clone(),
            queue_count: client.runtime.queue_count,
            truncated: client.runtime.truncated,
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
    use crate::adapter::admin_session::SessionConnectionRow;
    use crate::adapter::admin_session::SessionConnections;
    use crate::adapter::identifier_alias::IdentifierAliaser;
    use crate::model::contract::PageRequest;
    use crate::model::contract::QuerySource;
    use crate::model::contract::SourceFailure;
    use crate::model::contract::SourceFailureCode;
    use crate::tools::consumer_tools::ConsumerClientQueueMode;
    use crate::tools::consumer_tools::ConsumerClientQueueRow;

    const KEY: &[u8] = b"consumer-running-info-test-key";
    const CLIENT_ID: &str = "consumer@10.0.0.7@4242";
    const CLIENT_ADDR: &str = "10.0.0.7:52144";

    fn alias_of(client_id: &str) -> String {
        IdentifierAliaser::with_key(KEY)
            .unwrap()
            .client_alias(client_id, CLIENT_ADDR)
            .unwrap()
    }

    fn connections(client_ids: &[&str], truncated: bool) -> SessionConnections {
        SessionConnections {
            rows: client_ids
                .iter()
                .map(|client_id| SessionConnectionRow {
                    broker_name: "broker-a".to_string(),
                    client_id: (*client_id).to_string(),
                    client_addr: CLIENT_ADDR.to_string(),
                    language: "JAVA".to_string(),
                    version: 453,
                    last_update_timestamp: None,
                })
                .collect(),
            queried_broker_count: 1,
            truncated,
        }
    }

    fn queue(queue_id: i32) -> ConsumerClientQueueRow {
        ConsumerClientQueueRow {
            topic: "orders".to_string(),
            broker_name: "broker-a".to_string(),
            queue_id,
            mode: ConsumerClientQueueMode::Pull,
            dropped: false,
            last_pull_at: None,
            commit_offset: Some(i64::from(queue_id) * 10),
            cached_message_count: Some(0),
            cached_message_size_mib: Some(0),
            locked: Some(false),
            try_unlock_times: Some(0),
            last_lock_at: None,
            last_consume_at: None,
            wait_ack_count: None,
        }
    }

    fn runtime() -> SessionConsumerClientRuntime {
        SessionConsumerClientRuntime {
            consume_type: Some("CONSUME_PASSIVELY".to_string()),
            consume_orderly: Some(false),
            thread_pool_core_size: Some(20),
            client_version: Some("V5_3_0".to_string()),
            started_at: Some("2023-11-14T22:13:20.000Z".to_string()),
            subscriptions: Vec::new(),
            topic_status: Vec::new(),
            queues: (0..3).map(queue).collect(),
            queue_count: 3,
            truncated: false,
        }
    }

    fn args(alias: &str, limit: u32, cursor: Option<String>) -> GetConsumerRunningInfoArgs {
        GetConsumerRunningInfoArgs {
            cluster: "local-dev".to_string(),
            consumer_group: "workers".to_string(),
            client_alias: alias.to_string(),
            page: PageRequest {
                limit: Some(limit),
                cursor,
            },
        }
    }

    fn facade(factory: &StubFactory) -> QueryFacade<StubFactory> {
        QueryFacade::with_factory(config(), factory.clone())
            .with_identifier_aliases(IdentifierAliaser::with_key(KEY).unwrap())
    }

    #[tokio::test]
    async fn the_pseudonym_selects_one_client_and_its_queue_rows_are_paged_from_one_answer() {
        let factory = StubFactory {
            consumer_connections: Some(QueryPayload::complete(connections(
                &["consumer@10.0.0.8@1", CLIENT_ID],
                false,
            ))),
            consumer_client_runtime: Some(runtime()),
            ..Default::default()
        };
        let facade = facade(&factory);
        let alias = alias_of(CLIENT_ID);

        let first = facade.consumer_running_info(args(&alias, 2, None)).await.unwrap();
        let cursor = first.data.page.next_cursor.clone().unwrap();
        let second = facade
            .consumer_running_info(args(&alias, 2, Some(cursor)))
            .await
            .unwrap();

        assert_eq!(first.data.client_alias, alias);
        assert_eq!(first.data.broker_name, "broker-a");
        assert_eq!(first.data.client_version.as_deref(), Some("V5_3_0"));
        assert_eq!(first.data.queue_count, 3);
        let queue_ids = |output: &GetConsumerRunningInfoOutput| {
            output.page.items.iter().map(|row| row.queue_id).collect::<Vec<_>>()
        };
        assert_eq!(queue_ids(&first.data), [0, 1]);
        assert_eq!(queue_ids(&second.data), [2]);
        // The client that was asked is the one the pseudonym names, in one session.
        assert_eq!(
            *factory.asked_clients.lock().unwrap(),
            [("workers".to_string(), "broker-a".to_string(), CLIENT_ID.to_string())]
        );
        assert_eq!(factory.used(), (1, 1, 2));
        // The identifier and the address of the client stay in the session.
        let published = serde_json::to_string(&first.data).unwrap();
        assert!(!published.contains(CLIENT_ID) && !published.contains("10.0.0.7"));
    }

    #[tokio::test]
    async fn a_pseudonym_no_connection_has_is_not_found_only_when_the_list_is_complete() {
        let unknown = alias_of("consumer@10.0.0.9@7");
        let complete = StubFactory {
            consumer_connections: Some(QueryPayload::complete(connections(&[CLIENT_ID], false))),
            consumer_client_runtime: Some(runtime()),
            ..Default::default()
        };
        let error = facade(&complete)
            .consumer_running_info(args(&unknown, 10, None))
            .await
            .unwrap_err();
        assert_eq!(error.not_found_entity(), Some(NotFoundEntity::Client));
        assert!(complete.asked_clients.lock().unwrap().is_empty());

        let failure = SourceFailure::new(
            QuerySource::ConsumerConnection,
            SourceFailureCode::Timeout,
            true,
            "broker-b",
        );
        for inconclusive in [
            QueryPayload::new(connections(&[CLIENT_ID], false), true, Vec::new(), vec![failure]),
            QueryPayload::complete(connections(&[CLIENT_ID], true)),
        ] {
            let factory = StubFactory {
                consumer_connections: Some(inconclusive),
                consumer_client_runtime: Some(runtime()),
                ..Default::default()
            };
            let error = facade(&factory)
                .consumer_running_info(args(&unknown, 10, None))
                .await
                .unwrap_err();
            assert_eq!(error.not_found_entity(), None);
            assert_eq!(error.code(), "source_unavailable");
        }
    }

    #[tokio::test]
    async fn anything_but_a_pseudonym_is_rejected_before_a_session_and_a_failed_relay_is_an_error() {
        let factory = StubFactory {
            consumer_connections: Some(QueryPayload::complete(connections(&[CLIENT_ID], false))),
            ..Default::default()
        };
        let facade = facade(&factory);
        for not_an_alias in [CLIENT_ID, "", "client-", "CLIENT-0123456789abcdef0123456789abcdef"] {
            assert!(
                facade
                    .consumer_running_info(args(not_an_alias, 10, None))
                    .await
                    .is_err(),
                "{not_an_alias}"
            );
        }
        assert_eq!(factory.used(), (0, 0, 0));

        // The client is connected, and the Broker could not relay the request to it.
        let error = facade
            .consumer_running_info(args(&alias_of(CLIENT_ID), 10, None))
            .await
            .unwrap_err();
        assert_eq!(error.code(), "source_unavailable");
        assert_eq!(factory.used(), (1, 1, 2));
    }
}
