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
use crate::adapter::admin_session::SessionTopicStats;
use crate::infrastructure::snapshot::SnapshotKind;
use crate::infrastructure::snapshot::SnapshotRequest;
use crate::infrastructure::snapshot::SnapshotSelectionMode;
use crate::infrastructure::snapshot::SnapshotWeight;
use crate::model::contract::QueryResult;
use crate::tools::config_tools::GetTopicConfigArgs;
use crate::tools::config_tools::GetTopicConfigOutput;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::GetTopicStatsArgs;
use crate::tools::topic_tools::GetTopicStatsOutput;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn topic_stats(
        &self,
        mut args: GetTopicStatsArgs,
    ) -> Result<QueryResult<GetTopicStatsOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let request = SnapshotRequest::try_new_with_selection(
            SnapshotKind::TopicStats,
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
                self.snapshot_response_ttl(self.config.cache.topic_list_ttl_ms),
                |stats: &SessionTopicStats| SnapshotWeight::detail(stats.queues.len()),
                &self.control.cancellation,
                || {
                    self.run_workflow(cluster.clone(), move |session, _| {
                        Box::pin(async move { session.topic_stats(&topic).await })
                    })
                },
            )
            .await?;
        let page = self.snapshots.page(&snapshot, &snapshot.payload.data.queues)?;
        let stats = &snapshot.payload.data;
        let payload = snapshot.payload.completeness().wrap(GetTopicStatsOutput {
            cluster: cluster.name,
            topic: args.topic,
            total_message_count: stats.total_message_count,
            queue_count: stats.queue_count,
            truncated: stats.truncated,
            page,
            generated_at: snapshot.observed_at.clone(),
        });
        Ok(query_result_from_snapshot(&snapshot, payload))
    }

    pub(crate) async fn topic_config(
        &self,
        mut args: GetTopicConfigArgs,
    ) -> Result<QueryResult<GetTopicConfigOutput>, ToolExecutionError> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.topic = normalized_identifier("topic", &args.topic)?;
        let cluster = self.resolve_required_cluster(&args.cluster)?;
        let key = self.cache_key("topic_config", &cluster.name, &format!("topic={}", args.topic));
        let ttl = Duration::from_millis(self.config.cache.topic_list_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.topic_config(&args.topic).await })
                    })
                    .await
                },
            )
            .await
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
    use crate::model::contract::QuerySource;
    use crate::model::contract::SourceFailure;
    use crate::model::contract::SourceFailureCode;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::config_tools::TopicConfigDifferenceField;
    use crate::tools::config_tools::TopicConfigObservationRow;
    use crate::tools::consumer_tools::ConsumerGroupSummary;
    use crate::tools::topic_tools::TopicStatsQueueRow;

    #[derive(Debug, Default)]
    struct Counters {
        starts: AtomicUsize,
        shutdowns: AtomicUsize,
        stats: AtomicUsize,
        configs: AtomicUsize,
        stats_entered: AtomicBool,
    }

    #[derive(Clone, Default)]
    struct Factory {
        counters: Arc<Counters>,
        partial: bool,
        hang_stats: bool,
        hang_config: bool,
    }

    impl AdminSessionFactory for Factory {
        type Session = Session;

        async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(Session {
                cluster,
                counters: self.counters.clone(),
                partial: self.partial,
                hang_stats: self.hang_stats,
                hang_config: self.hang_config,
            })
        }
    }

    struct Session {
        cluster: ResolvedCluster,
        counters: Arc<Counters>,
        partial: bool,
        hang_stats: bool,
        hang_config: bool,
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

        async fn topic_stats(&mut self, _topic: &str) -> Result<QueryPayload<SessionTopicStats>, ToolExecutionError> {
            self.counters.stats.fetch_add(1, Ordering::SeqCst);
            self.counters.stats_entered.store(true, Ordering::SeqCst);
            if self.hang_stats {
                std::future::pending::<()>().await;
            }
            let data = SessionTopicStats {
                total_message_count: 60,
                queue_count: 3,
                queues: (0..3)
                    .map(|queue_id| TopicStatsQueueRow {
                        broker_name: "broker-a".to_string(),
                        queue_id,
                        min_offset: i64::from(queue_id) * 10,
                        max_offset: i64::from(queue_id) * 10 + 20,
                        message_count: 20,
                        last_update_at: Some("1970-01-01T00:00:01.000Z".to_string()),
                    })
                    .collect(),
                truncated: false,
            };
            Ok(if self.partial {
                QueryPayload::new(
                    data,
                    true,
                    Vec::new(),
                    vec![SourceFailure::new(
                        QuerySource::TopicStats,
                        SourceFailureCode::Timeout,
                        true,
                        "broker-b",
                    )],
                )
            } else {
                QueryPayload::complete(data)
            })
        }

        async fn topic_config(
            &mut self,
            topic: &str,
        ) -> Result<QueryPayload<GetTopicConfigOutput>, ToolExecutionError> {
            self.counters.configs.fetch_add(1, Ordering::SeqCst);
            if self.hang_config {
                std::future::pending::<()>().await;
            }
            let data = GetTopicConfigOutput {
                cluster: self.cluster.name.clone(),
                topic: topic.to_string(),
                brokers: vec![TopicConfigObservationRow {
                    broker_name: "broker-a".to_string(),
                    version: 9,
                    read_queue_nums: 8,
                    write_queue_nums: 8,
                    perm: 6,
                    order: false,
                    message_type: "NORMAL".to_string(),
                }],
                inconsistent_fields: vec![TopicConfigDifferenceField::ReadQueueNums],
                generated_at: "1970-01-01T00:00:01.000Z".to_string(),
            };
            Ok(if self.partial {
                QueryPayload::new(
                    data,
                    true,
                    Vec::new(),
                    vec![SourceFailure::new(
                        QuerySource::TopicConfig,
                        SourceFailureCode::Timeout,
                        true,
                        "broker-b",
                    )],
                )
            } else {
                QueryPayload::complete(data)
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

    fn stats_args(topic: &str, limit: u32, cursor: Option<String>) -> GetTopicStatsArgs {
        GetTopicStatsArgs {
            cluster: "local-dev".to_string(),
            topic: topic.to_string(),
            page: PageRequest {
                limit: Some(limit),
                cursor,
            },
        }
    }

    #[tokio::test]
    async fn continuation_pages_use_one_snapshot_and_bind_all_request_context() {
        let factory = Factory {
            partial: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        let first = facade.topic_stats(stats_args("orders", 1, None)).await.unwrap();
        let cursor = first.data.page.next_cursor.clone().unwrap();
        let second = facade
            .topic_stats(stats_args("orders", 1, Some(cursor.clone())))
            .await
            .unwrap();

        assert_eq!(first.data.page.items[0].queue_id, 0);
        assert_eq!(second.data.page.items[0].queue_id, 1);
        assert_eq!(first.partial, second.partial);
        assert_eq!(first.warnings, second.warnings);
        assert_eq!(first.source_failures, second.source_failures);
        assert_eq!(counters.stats.load(Ordering::SeqCst), 1);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);

        assert!(facade
            .topic_stats(stats_args("payments", 1, Some(cursor.clone())))
            .await
            .is_err());
        assert!(facade
            .topic_stats(stats_args("orders", 2, Some(cursor.clone())))
            .await
            .is_err());
        let sensitive = facade.clone().with_visibility_class(VisibilityClass::Sensitive);
        assert!(sensitive
            .topic_stats(stats_args("orders", 1, Some(cursor)))
            .await
            .is_err());
        assert_eq!(counters.stats.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disabled_response_cache_reloads_first_page_but_preserves_continuations() {
        let mut config = config();
        config.cache.enabled = false;
        let factory = Factory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config, factory);
        let first = facade.topic_stats(stats_args("orders", 1, None)).await.unwrap();
        facade
            .topic_stats(stats_args("orders", 1, first.data.page.next_cursor.clone()))
            .await
            .unwrap();
        facade.topic_stats(stats_args("orders", 1, None)).await.unwrap();
        assert_eq!(counters.stats.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn configuration_cache_hit_preserves_safe_data_and_completeness() {
        let factory = Factory {
            partial: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        let args = GetTopicConfigArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
        };
        let first = facade.topic_config(args.clone()).await.unwrap();
        let second = facade.topic_config(args).await.unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(first.data, second.data);
        assert_eq!(first.partial, second.partial);
        assert_eq!(first.warnings, second.warnings);
        assert_eq!(first.source_failures, second.source_failures);
        assert_eq!(counters.configs.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        let json = serde_json::to_string(&first.data).unwrap();
        assert!(!json.contains("addr"));
        assert!(!json.contains("attribute"));
    }

    #[tokio::test]
    async fn cancellation_still_shuts_down_the_admin_session() {
        let factory = Factory {
            hang_stats: true,
            ..Default::default()
        };
        let counters = factory.counters.clone();
        let cancellation = CancellationToken::new();
        let facade = QueryFacade::with_factory(config(), factory).with_cancellation(cancellation.clone());
        let task = tokio::spawn(async move { facade.topic_stats(stats_args("orders", 1, None)).await });
        for _ in 0..10_000 {
            if counters.stats_entered.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(counters.stats_entered.load(Ordering::SeqCst));
        cancellation.cancel();
        assert!(matches!(task.await.unwrap(), Err(ToolExecutionError::Cancelled)));
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn both_topic_observation_timeouts_shutdown_the_started_session() {
        let stats_factory = Factory {
            hang_stats: true,
            ..Default::default()
        };
        let stats_counters = stats_factory.counters.clone();
        let control = super::super::WorkflowControl::new(Duration::from_millis(10), CancellationToken::new());
        let stats_facade = QueryFacade::with_factory_and_control(config(), stats_factory, control);
        assert!(matches!(
            stats_facade.topic_stats(stats_args("orders", 1, None)).await,
            Err(ToolExecutionError::TimedOut { timeout_ms: 10 })
        ));
        assert_eq!(stats_counters.shutdowns.load(Ordering::SeqCst), 1);

        let config_factory = Factory {
            hang_config: true,
            ..Default::default()
        };
        let config_counters = config_factory.counters.clone();
        let control = super::super::WorkflowControl::new(Duration::from_millis(10), CancellationToken::new());
        let config_facade = QueryFacade::with_factory_and_control(config(), config_factory, control);
        assert!(matches!(
            config_facade
                .topic_config(GetTopicConfigArgs {
                    cluster: "local-dev".to_string(),
                    topic: "orders".to_string(),
                })
                .await,
            Err(ToolExecutionError::TimedOut { timeout_ms: 10 })
        ));
        assert_eq!(config_counters.shutdowns.load(Ordering::SeqCst), 1);
    }
}
