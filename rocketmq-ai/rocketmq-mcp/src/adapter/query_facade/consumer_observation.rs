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

use std::time::Duration;

use super::normalized_identifier;
use super::normalized_logical_identifier;
use super::query_result_from_snapshot;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::SessionConsumerProgress;
use crate::infrastructure::snapshot::SnapshotKind;
use crate::infrastructure::snapshot::SnapshotRequest;
use crate::infrastructure::snapshot::SnapshotSelectionMode;
use crate::infrastructure::snapshot::SnapshotWeight;
use crate::model::contract::QueryResult;
use crate::tools::consumer_tools::GetConsumerGroupDetailsArgs;
use crate::tools::consumer_tools::GetConsumerGroupDetailsOutput;
use crate::tools::consumer_tools::GetConsumerProgressArgs;
use crate::tools::consumer_tools::GetConsumerProgressOutput;
use crate::tools::executor::ToolExecutionError;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn consumer_group_details(
        &self,
        mut args: GetConsumerGroupDetailsArgs,
    ) -> Result<QueryResult<GetConsumerGroupDetailsOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key(
            "consumer_group_details",
            &cluster.name,
            &format!("consumer_group={}", args.consumer_group),
        );
        let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.consumer_group_details(&args.consumer_group).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn consumer_progress(
        &self,
        mut args: GetConsumerProgressArgs,
    ) -> Result<QueryResult<GetConsumerProgressOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.consumer_group = normalized_identifier("consumer_group", &args.consumer_group)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let request = SnapshotRequest::try_new_with_selection(
            SnapshotKind::ConsumerProgress,
            cluster.name.clone(),
            format!("consumer_group={}", args.consumer_group),
            SnapshotSelectionMode::ExactIdentifier,
            &args.page,
            self.visibility_class.as_str(),
        )?;
        let consumer_group = args.consumer_group.clone();
        let snapshot = self
            .snapshots
            .get_or_load(
                request,
                args.page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |progress: &SessionConsumerProgress| SnapshotWeight::detail(progress.queues.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster.clone(), move |session, _| {
                        Box::pin(async move { session.consumer_progress(&consumer_group).await })
                    })
                },
            )
            .await?;
        let progress = &snapshot.payload.data;
        let page = self.snapshots.page(&snapshot, &progress.queues)?;
        let payload = snapshot.payload.completeness().wrap(GetConsumerProgressOutput {
            cluster: cluster.name,
            consumer_group: args.consumer_group,
            state: progress.state.clone(),
            topic_count: progress.topic_count,
            queue_count: progress.queue_count,
            total_lag: progress.total_lag,
            max_queue_lag: progress.max_queue_lag,
            total_inflight: progress.total_inflight,
            consume_tps: progress.consume_tps,
            truncated: progress.truncated,
            page,
            generated_at: snapshot.observed_at.clone(),
        });
        Ok(query_result_from_snapshot(&snapshot, payload))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rocketmq_admin_core::core::broker::BrokerRuntimeTargetStatus;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::adapter::admin_session::AdminSession;
    use crate::adapter::admin_session::ResolvedCluster;
    use crate::adapter::admin_session::SessionConsumerLag;
    use crate::adapter::admin_session::SessionTopicRoute;
    use crate::config::McpConfig;
    use crate::guard::context::VisibilityClass;
    use crate::model::contract::CacheStatus;
    use crate::model::contract::PageRequest;
    use crate::model::contract::QueryPayload;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::consumer_tools::ConsumerConnectionState;
    use crate::tools::consumer_tools::ConsumerGroupConfigPresence;
    use crate::tools::consumer_tools::ConsumerGroupDetailsBrokerRow;
    use crate::tools::consumer_tools::ConsumerGroupSummary;
    use crate::tools::consumer_tools::ConsumerProgressQueueRow;
    use crate::tools::consumer_tools::ConsumerProgressState;

    #[derive(Debug, Default)]
    struct Counters {
        starts: AtomicUsize,
        shutdowns: AtomicUsize,
        details: AtomicUsize,
        progress: AtomicUsize,
        progress_entered: AtomicBool,
    }

    #[derive(Clone, Default)]
    struct Factory {
        counters: Arc<Counters>,
        hang_progress: bool,
    }

    impl AdminSessionFactory for Factory {
        type Session = Session;

        async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(Session {
                cluster,
                counters: self.counters.clone(),
                hang_progress: self.hang_progress,
            })
        }
    }

    struct Session {
        cluster: ResolvedCluster,
        counters: Arc<Counters>,
        hang_progress: bool,
    }

    impl AdminSession for Session {
        async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolExecutionError> {
            Ok(QueryPayload::complete(Vec::new()))
        }

        async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolExecutionError> {
            Ok(Vec::new())
        }

        async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
            Ok(SessionTopicRoute {
                brokers: Vec::new(),
                queues: Vec::new(),
            })
        }

        async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError> {
            Ok(QueryPayload::complete(Vec::new()))
        }

        async fn consumer_lag(
            &mut self,
            _topic: &str,
            _consumer_group: &str,
        ) -> Result<QueryPayload<SessionConsumerLag>, ToolExecutionError> {
            Ok(QueryPayload::complete(SessionConsumerLag {
                queues: Vec::new(),
                total_lag: 0,
                consume_tps: 0.0,
                inflight_total: 0,
            }))
        }

        async fn consumer_group_details(
            &mut self,
            consumer_group: &str,
        ) -> Result<QueryPayload<GetConsumerGroupDetailsOutput>, ToolExecutionError> {
            self.counters.details.fetch_add(1, Ordering::SeqCst);
            Ok(QueryPayload::complete(GetConsumerGroupDetailsOutput {
                cluster: self.cluster.name.clone(),
                consumer_group: consumer_group.to_string(),
                total_connection_count: 0,
                brokers: vec![ConsumerGroupDetailsBrokerRow {
                    broker_name: "broker-a".to_string(),
                    config_state: ConsumerGroupConfigPresence::Present,
                    config_version: Some(1),
                    consume_enable: Some(true),
                    consume_from_min_enable: Some(false),
                    consume_broadcast_enable: Some(false),
                    consume_message_orderly: Some(false),
                    retry_queue_nums: Some(1),
                    retry_max_times: Some(1),
                    notify_consumer_ids_changed_enable: Some(true),
                    consume_timeout_minutes: Some(1),
                    connection_state: Some(ConsumerConnectionState::Offline),
                    connection_count: 0,
                    consume_type: None,
                    message_model: None,
                    consume_from_where: None,
                }],
                generated_at: "session-time".to_string(),
            }))
        }

        async fn consumer_progress(
            &mut self,
            _consumer_group: &str,
        ) -> Result<QueryPayload<SessionConsumerProgress>, ToolExecutionError> {
            self.counters.progress.fetch_add(1, Ordering::SeqCst);
            self.counters.progress_entered.store(true, Ordering::SeqCst);
            if self.hang_progress {
                std::future::pending::<()>().await;
            }
            Ok(QueryPayload::complete(SessionConsumerProgress {
                state: ConsumerProgressState::Observed,
                topic_count: 1,
                queue_count: 3,
                total_lag: 3,
                max_queue_lag: 1,
                total_inflight: 0,
                consume_tps: 1.0,
                queues: (0..3)
                    .map(|queue_id| ConsumerProgressQueueRow {
                        topic: "orders".to_string(),
                        broker_name: "broker-a".to_string(),
                        queue_id,
                        broker_offset: i64::from(queue_id) + 1,
                        consumer_offset: i64::from(queue_id),
                        pull_offset: i64::from(queue_id),
                        lag: 1,
                        inflight: 0,
                        last_observed_at: None,
                    })
                    .collect(),
                truncated: false,
            }))
        }

        async fn probe_broker_runtime_target(
            &mut self,
            _broker_name: &str,
        ) -> Result<BrokerRuntimeTargetStatus, ToolExecutionError> {
            Ok(BrokerRuntimeTargetStatus::NotFound)
        }

        async fn shutdown(self) -> Result<(), ToolExecutionError> {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn config() -> McpConfig {
        McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .unwrap()
    }

    fn progress_args(group: &str, limit: u32, cursor: Option<String>) -> GetConsumerProgressArgs {
        GetConsumerProgressArgs {
            cluster: "local-dev".to_string(),
            consumer_group: group.to_string(),
            page: PageRequest {
                limit: Some(limit),
                cursor,
            },
        }
    }

    #[tokio::test]
    async fn continuation_is_zero_rpc_and_binds_group_limit_and_visibility() {
        let factory = Factory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        let first = facade
            .consumer_progress(progress_args("group-a", 1, None))
            .await
            .unwrap();
        let cursor = first.data.page.next_cursor.unwrap();
        let second = facade
            .consumer_progress(progress_args("group-a", 1, Some(cursor.clone())))
            .await
            .unwrap();
        assert_eq!((first.data.topic_count, first.data.queue_count), (1, 3));
        assert_eq!((first.data.total_lag, first.data.max_queue_lag), (3, 1));
        assert_eq!(first.data.page.items[0].queue_id, 0);
        assert_eq!(second.data.page.items[0].queue_id, 1);
        assert_eq!(counters.progress.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert!(facade
            .consumer_progress(progress_args("group-b", 1, Some(cursor.clone())))
            .await
            .is_err());
        assert!(facade
            .consumer_progress(progress_args("group-a", 2, Some(cursor.clone())))
            .await
            .is_err());
        assert!(facade
            .clone()
            .with_visibility_class(VisibilityClass::Sensitive)
            .consumer_progress(progress_args("group-a", 1, Some(cursor)))
            .await
            .is_err());
        assert_eq!(counters.progress.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cache_disabled_reloads_first_pages_but_keeps_existing_cursor() {
        let mut configuration = config();
        configuration.cache.enabled = false;
        let factory = Factory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(configuration, factory);
        let first = facade
            .consumer_progress(progress_args("group-a", 1, None))
            .await
            .unwrap();
        facade
            .consumer_progress(progress_args("group-a", 1, first.data.page.next_cursor))
            .await
            .unwrap();
        facade
            .consumer_progress(progress_args("group-a", 1, None))
            .await
            .unwrap();
        let details = GetConsumerGroupDetailsArgs {
            cluster: "local-dev".to_string(),
            consumer_group: "group-a".to_string(),
        };
        facade.consumer_group_details(details.clone()).await.unwrap();
        facade.consumer_group_details(details).await.unwrap();
        assert_eq!(counters.progress.load(Ordering::SeqCst), 2);
        assert_eq!(counters.details.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn details_cache_hits_and_output_has_no_sensitive_shapes() {
        let factory = Factory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        let args = GetConsumerGroupDetailsArgs {
            cluster: "local-dev".to_string(),
            consumer_group: "group-a".to_string(),
        };
        let first = facade.consumer_group_details(args.clone()).await.unwrap();
        let second = facade.consumer_group_details(args).await.unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(counters.details.load(Ordering::SeqCst), 1);
        let wire = serde_json::to_string(&first.data).unwrap();
        for forbidden in ["client_id", "client_addr", "subscription", "attributes", "raw_error"] {
            assert!(!wire.contains(forbidden));
        }
    }

    #[tokio::test]
    async fn cancellation_and_timeout_shutdown_progress_sessions() {
        let factory = Factory {
            hang_progress: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let cancellation = CancellationToken::new();
        let facade = QueryFacade::with_factory(config(), factory).with_cancellation(cancellation.clone());
        let query = facade.consumer_progress(progress_args("group-a", 1, None));
        tokio::pin!(query);
        loop {
            tokio::select! {
                result = &mut query => panic!("query completed before cancellation: {result:?}"),
                () = tokio::task::yield_now() => {
                    if counters.progress_entered.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }
        cancellation.cancel();
        assert!(matches!(query.await, Err(ToolExecutionError::Cancelled)));
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);

        let factory = Factory {
            hang_progress: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let control = super::super::WorkflowControl::new(Duration::from_millis(10), CancellationToken::new());
        let facade = QueryFacade::with_factory_and_control(config(), factory, control);
        assert!(matches!(
            facade.consumer_progress(progress_args("group-a", 1, None)).await,
            Err(ToolExecutionError::TimedOut { timeout_ms: 10 })
        ));
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }
}
