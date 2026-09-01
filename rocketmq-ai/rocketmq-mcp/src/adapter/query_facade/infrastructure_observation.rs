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

use rocketmq_admin_core::core::infrastructure_observation::QueryControllerMetadataRequest;
use rocketmq_admin_core::core::infrastructure_observation::QueryHaStatusRequest;
use rocketmq_admin_core::core::infrastructure_observation::QueryNameserverConfigSummaryRequest;

use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::model::contract::QueryResult;
use crate::tools::executor::ToolExecutionError;
use crate::tools::infrastructure_tools::GetControllerMetadataArgs;
use crate::tools::infrastructure_tools::GetControllerMetadataOutput;
use crate::tools::infrastructure_tools::GetHaStatusArgs;
use crate::tools::infrastructure_tools::GetHaStatusOutput;
use crate::tools::infrastructure_tools::GetNameserverConfigSummaryArgs;
use crate::tools::infrastructure_tools::GetNameserverConfigSummaryOutput;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    pub(crate) async fn ha_status(
        &self,
        args: GetHaStatusArgs,
    ) -> Result<QueryResult<GetHaStatusOutput>, ToolExecutionError> {
        let request = QueryHaStatusRequest::try_new(
            args.cluster,
            args.broker_names,
            args.include_sync_state,
            args.controller_names,
        )
        .map_err(|_| ToolExecutionError::InvalidArguments("invalid HA observation selectors".to_string()))?;
        let cluster = self.resolve_required_cluster(&request.cluster)?;
        let key = self.cache_key(
            "ha_status",
            &cluster.name,
            &format!(
                "brokers={}|sync={}|controllers={}",
                request.broker_names.join(","),
                request.include_sync_state,
                request.controller_names.join(",")
            ),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move {
                            session
                                .ha_status(
                                    &request.broker_names,
                                    request.include_sync_state,
                                    &request.controller_names,
                                )
                                .await
                        })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn controller_metadata(
        &self,
        args: GetControllerMetadataArgs,
    ) -> Result<QueryResult<GetControllerMetadataOutput>, ToolExecutionError> {
        let request = QueryControllerMetadataRequest::try_new(args.cluster, args.controller_names)
            .map_err(|_| ToolExecutionError::InvalidArguments("invalid Controller selectors".to_string()))?;
        let cluster = self.resolve_required_cluster(&request.cluster)?;
        let key = self.cache_key(
            "controller_metadata",
            &cluster.name,
            &format!("controllers={}", request.controller_names.join(",")),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.controller_metadata(&request.controller_names).await })
                    })
                    .await
                },
            )
            .await
    }

    pub(crate) async fn nameserver_config_summary(
        &self,
        args: GetNameserverConfigSummaryArgs,
    ) -> Result<QueryResult<GetNameserverConfigSummaryOutput>, ToolExecutionError> {
        let request = QueryNameserverConfigSummaryRequest::try_new(args.cluster)
            .map_err(|_| ToolExecutionError::InvalidArguments("invalid cluster selector".to_string()))?;
        let cluster = self.resolve_required_cluster(&request.cluster)?;
        let key = self.cache_key("nameserver_config_summary", &cluster.name, "");
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolExecutionError::Cancelled,
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move { session.nameserver_config_summary().await })
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

    use super::*;
    use crate::adapter::admin_session::AdminSession;
    use crate::adapter::admin_session::ResolvedCluster;
    use crate::adapter::admin_session::SessionConsumerLag;
    use crate::adapter::admin_session::SessionTopicRoute;
    use crate::config::McpConfig;
    use crate::guard::context::VisibilityClass;
    use crate::model::contract::CacheStatus;
    use crate::model::contract::QueryPayload;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::consumer_tools::ConsumerGroupSummary;
    use crate::tools::infrastructure_tools::BrokerHaObservation;
    use crate::tools::infrastructure_tools::ControllerMetadataObservation;
    use crate::tools::infrastructure_tools::HaConnectionObservation;
    use crate::tools::infrastructure_tools::LogicalBrokerInstance;
    use crate::tools::infrastructure_tools::NameserverConfigObservation;
    use crate::tools::infrastructure_tools::NameserverConfigValues;

    #[derive(Debug, Default)]
    struct Counters {
        starts: AtomicUsize,
        shutdowns: AtomicUsize,
        ha: AtomicUsize,
        controller: AtomicUsize,
        nameserver: AtomicUsize,
        oversized: AtomicBool,
    }

    #[derive(Clone, Default)]
    struct Factory {
        counters: Arc<Counters>,
    }

    impl AdminSessionFactory for Factory {
        type Session = Session;

        async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(Session {
                cluster,
                counters: self.counters.clone(),
            })
        }
    }

    struct Session {
        cluster: ResolvedCluster,
        counters: Arc<Counters>,
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

        async fn probe_broker_runtime_target(
            &mut self,
            _broker_name: &str,
        ) -> Result<BrokerRuntimeTargetStatus, ToolExecutionError> {
            Ok(BrokerRuntimeTargetStatus::NotFound)
        }

        async fn ha_status(
            &mut self,
            broker_names: &[String],
            _include_sync_state: bool,
            _controller_names: &[String],
        ) -> Result<QueryPayload<GetHaStatusOutput>, ToolExecutionError> {
            self.counters.ha.fetch_add(1, Ordering::SeqCst);
            let brokers = if broker_names == ["bounded"] {
                bounded_ha_brokers()
            } else {
                self.counters
                    .oversized
                    .load(Ordering::SeqCst)
                    .then(|| BrokerHaObservation {
                        broker_name: "x".repeat(4 * 1024 * 1024),
                        broker_id: 0,
                        master_commit_log_max_offset: 0,
                        in_sync_slave_count: 0,
                        pending_group_transfer_request_count: 0,
                        pending_group_transfer_oldest_wait_millis: 0,
                        group_transfer_ack_notify_count: 0,
                        connections: Vec::new(),
                    })
                    .into_iter()
                    .collect()
            };
            Ok(QueryPayload::complete(GetHaStatusOutput {
                cluster: self.cluster.name.clone(),
                brokers,
                controller_sync_states: Vec::new(),
            }))
        }

        async fn controller_metadata(
            &mut self,
            _controller_names: &[String],
        ) -> Result<QueryPayload<GetControllerMetadataOutput>, ToolExecutionError> {
            self.counters.controller.fetch_add(1, Ordering::SeqCst);
            let controllers = self
                .counters
                .oversized
                .load(Ordering::SeqCst)
                .then(|| ControllerMetadataObservation {
                    controller_name: "x".repeat(4 * 1024 * 1024),
                    group: None,
                    leader_id: None,
                    is_leader: None,
                    peer_count: None,
                    last_log_index: None,
                    committed_log_index: None,
                    applied_log_index: None,
                })
                .into_iter()
                .collect();
            Ok(QueryPayload::complete(GetControllerMetadataOutput {
                cluster: self.cluster.name.clone(),
                controllers,
            }))
        }

        async fn nameserver_config_summary(
            &mut self,
        ) -> Result<QueryPayload<GetNameserverConfigSummaryOutput>, ToolExecutionError> {
            self.counters.nameserver.fetch_add(1, Ordering::SeqCst);
            let nameservers = self
                .counters
                .oversized
                .load(Ordering::SeqCst)
                .then(|| NameserverConfigObservation {
                    nameserver_name: "x".repeat(4 * 1024 * 1024),
                    values: NameserverConfigValues::default(),
                })
                .into_iter()
                .collect();
            Ok(QueryPayload::complete(GetNameserverConfigSummaryOutput {
                cluster: self.cluster.name.clone(),
                nameservers,
                inconsistent_fields: Vec::new(),
            }))
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

    fn bounded_ha_brokers() -> Vec<BrokerHaObservation> {
        (0..16)
            .map(|broker_index| {
                let connection_count = if broker_index == 15 { 39 } else { 63 };
                BrokerHaObservation {
                    broker_name: format!("broker-{broker_index:02}"),
                    broker_id: 0,
                    master_commit_log_max_offset: 100,
                    in_sync_slave_count: u32::try_from(connection_count).unwrap(),
                    pending_group_transfer_request_count: 0,
                    pending_group_transfer_oldest_wait_millis: 0,
                    group_transfer_ack_notify_count: 0,
                    connections: (1..=connection_count)
                        .map(|replica_index| HaConnectionObservation {
                            replica: LogicalBrokerInstance {
                                broker_name: format!("broker-{broker_index:02}"),
                                broker_id: u64::try_from(replica_index).unwrap(),
                            },
                            slave_ack_offset: 90,
                            diff: 10,
                            in_sync: true,
                            transferred_bytes_per_second: 1,
                            transfer_from_where: 80,
                        })
                        .collect(),
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn normalized_selectors_share_cache_within_but_not_across_visibility_and_shutdown() {
        let factory = Factory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        let first = facade
            .ha_status(GetHaStatusArgs {
                cluster: "local-dev".to_string(),
                broker_names: vec!["broker-b".to_string(), "broker-a".to_string(), "broker-b".to_string()],
                include_sync_state: true,
                controller_names: vec!["controller-b".to_string(), "controller-a".to_string()],
            })
            .await
            .unwrap();
        let second = facade
            .ha_status(GetHaStatusArgs {
                cluster: "local-dev".to_string(),
                broker_names: vec!["broker-a".to_string(), "broker-b".to_string()],
                include_sync_state: true,
                controller_names: vec!["controller-a".to_string(), "controller-b".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        facade
            .clone()
            .with_visibility_class(VisibilityClass::Sensitive)
            .ha_status(GetHaStatusArgs {
                cluster: "local-dev".to_string(),
                broker_names: vec!["broker-a".to_string(), "broker-b".to_string()],
                include_sync_state: true,
                controller_names: vec!["controller-a".to_string(), "controller-b".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(counters.ha.load(Ordering::SeqCst), 2);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 2);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);

        assert!(facade
            .controller_metadata(GetControllerMetadataArgs {
                cluster: "local-dev".to_string(),
                controller_names: vec!["controller-a".to_string(), "controller-a".to_string()],
            })
            .await
            .is_err());
        assert_eq!(
            counters.starts.load(Ordering::SeqCst),
            2,
            "invalid selectors must be zero-session"
        );
    }

    #[tokio::test]
    async fn every_infrastructure_cache_entry_rejects_oversized_payloads_before_retention() {
        let factory = Factory::default();
        factory.counters.oversized.store(true, Ordering::SeqCst);
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        for _ in 0..2 {
            assert_eq!(
                facade
                    .ha_status(GetHaStatusArgs {
                        cluster: "local-dev".to_string(),
                        broker_names: Vec::new(),
                        include_sync_state: false,
                        controller_names: Vec::new(),
                    })
                    .await
                    .unwrap()
                    .cache_status,
                CacheStatus::Miss
            );
            assert_eq!(
                facade
                    .controller_metadata(GetControllerMetadataArgs {
                        cluster: "local-dev".to_string(),
                        controller_names: Vec::new(),
                    })
                    .await
                    .unwrap()
                    .cache_status,
                CacheStatus::Miss
            );
            assert_eq!(
                facade
                    .nameserver_config_summary(GetNameserverConfigSummaryArgs {
                        cluster: "local-dev".to_string(),
                    })
                    .await
                    .unwrap()
                    .cache_status,
                CacheStatus::Miss
            );
        }
        assert_eq!(counters.ha.load(Ordering::SeqCst), 2);
        assert_eq!(counters.controller.load(Ordering::SeqCst), 2);
        assert_eq!(counters.nameserver.load(Ordering::SeqCst), 2);
        assert_eq!(facade.cache_metrics().hits, 0);
    }

    #[tokio::test]
    async fn bounded_nested_infrastructure_result_is_cached_before_executor_policy() {
        let factory = Factory::default();
        let counters = factory.counters.clone();
        let facade = QueryFacade::with_factory(config(), factory);
        let args = GetHaStatusArgs {
            cluster: "local-dev".to_string(),
            broker_names: vec!["bounded".to_string()],
            include_sync_state: false,
            controller_names: Vec::new(),
        };
        let first = facade.ha_status(args.clone()).await.unwrap();
        let second = facade.ha_status(args).await.unwrap();
        let rows = first
            .data
            .brokers
            .iter()
            .fold(0usize, |rows, broker| rows + 1 + broker.connections.len());
        assert_eq!(rows, 1_000);
        assert!(serde_json::to_vec(&first.data).unwrap().len() < 4 * 1024 * 1024);
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(counters.ha.load(Ordering::SeqCst), 1);
    }
}
