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

//! The Broker rows of one cluster, read without the Topic and Consumer Group inventories.

use std::time::Duration;

use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::model::contract::observed_at;
use crate::model::contract::QueryResult;
use crate::tools::broker_tools::ListBrokersArgs;
use crate::tools::broker_tools::ListBrokersOutput;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Lists the Broker instances of one cluster.
    ///
    /// The cluster overview reads the same rows and then sizes two inventories. This workflow
    /// stops at the rows, so listing Brokers costs one session and one read however many Topics
    /// and Consumer Groups the cluster has.
    pub(crate) async fn list_brokers(
        &self,
        args: ListBrokersArgs,
    ) -> Result<QueryResult<ListBrokersOutput>, ToolFailure> {
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key("list_brokers", &cluster.name, "");
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let brokers = session.broker_rows().await?;
                            Ok(brokers.completeness().wrap(ListBrokersOutput {
                                cluster: cluster.name.clone(),
                                brokers: brokers.data,
                                generated_at: observed_at(),
                            }))
                        })
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
    use crate::adapter::admin_session::ResolvedCluster;
    use crate::adapter::admin_session::SessionConsumerLag;
    use crate::adapter::admin_session::SessionTopicRoute;
    use crate::adapter::query_facade::ReadOnlyQuery;
    use crate::config::McpConfig;
    use crate::model::contract::CacheStatus;
    use crate::model::contract::QueryPayload;
    use crate::model::contract::QuerySource;
    use crate::model::contract::SourceFailure;
    use crate::model::contract::SourceFailureCode;
    use crate::resources::reader;
    use crate::resources::uri::ResourceKind;
    use crate::resources::uri::RocketmqResourceUri;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::cluster_tools::ClusterOverviewArgs;
    use crate::tools::consumer_tools::ConsumerGroupSummary;

    /// How often each read of a session was made, across every session of one factory.
    #[derive(Debug, Default)]
    struct Counters {
        starts: AtomicUsize,
        shutdowns: AtomicUsize,
        broker_rows: AtomicUsize,
        topic_inventory: AtomicUsize,
        topic_route: AtomicUsize,
        consumer_groups: AtomicUsize,
        consumer_group_inventory: AtomicUsize,
        consumer_lag: AtomicUsize,
        probes: AtomicUsize,
        /// Makes the Broker rows partial: one Broker did not answer.
        partial: AtomicBool,
    }

    impl Counters {
        /// The reads that size or page the Topic and Consumer Group inventories.
        fn inventory_reads(&self) -> usize {
            self.topic_inventory.load(Ordering::SeqCst)
                + self.consumer_groups.load(Ordering::SeqCst)
                + self.consumer_group_inventory.load(Ordering::SeqCst)
        }

        /// Every read other than the Broker rows.
        fn other_reads(&self) -> usize {
            self.inventory_reads()
                + self.topic_route.load(Ordering::SeqCst)
                + self.consumer_lag.load(Ordering::SeqCst)
                + self.probes.load(Ordering::SeqCst)
        }
    }

    #[derive(Clone, Default)]
    struct Factory {
        counters: Arc<Counters>,
    }

    impl AdminSessionFactory for Factory {
        type Session = Session;

        async fn start(&self, _cluster: ResolvedCluster) -> Result<Self::Session, ToolFailure> {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            Ok(Session {
                counters: self.counters.clone(),
            })
        }
    }

    struct Session {
        counters: Arc<Counters>,
    }

    fn broker(broker_name: &str, broker_id: u64) -> BrokerSummary {
        BrokerSummary {
            cluster: "DefaultCluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_id,
            broker_addr: "10.11.12.13:10911".to_string(),
            version: "V5_3_0".to_string(),
            in_tps: "1.00".to_string(),
            out_tps: "2.00".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "1.00".to_string(),
            space: "0.10".to_string(),
            broker_active: true,
        }
    }

    impl AdminSession for Session {
        async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolFailure> {
            self.counters.broker_rows.fetch_add(1, Ordering::SeqCst);
            let rows = vec![broker("broker-a", 0), broker("broker-a", 1)];
            if !self.counters.partial.load(Ordering::SeqCst) {
                return Ok(QueryPayload::complete(rows));
            }
            Ok(QueryPayload::new(
                rows,
                true,
                vec!["broker_runtime_unavailable".to_string()],
                vec![SourceFailure {
                    source: QuerySource::BrokerRuntime,
                    code: SourceFailureCode::Timeout,
                    retryable: true,
                    logical_target: "broker-b".to_string(),
                }],
            ))
        }

        async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolFailure> {
            self.counters.topic_inventory.fetch_add(1, Ordering::SeqCst);
            Ok(vec!["orders".to_string()])
        }

        async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolFailure> {
            self.counters.topic_route.fetch_add(1, Ordering::SeqCst);
            Ok(SessionTopicRoute {
                brokers: Vec::new(),
                queues: Vec::new(),
            })
        }

        async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolFailure> {
            self.counters.consumer_groups.fetch_add(1, Ordering::SeqCst);
            Ok(QueryPayload::complete(Vec::new()))
        }

        async fn consumer_group_inventory(&mut self) -> Result<QueryPayload<Vec<String>>, ToolFailure> {
            self.counters.consumer_group_inventory.fetch_add(1, Ordering::SeqCst);
            Ok(QueryPayload::complete(vec!["order-consumers".to_string()]))
        }

        async fn consumer_lag(
            &mut self,
            _topic: &str,
            _consumer_group: &str,
        ) -> Result<QueryPayload<SessionConsumerLag>, ToolFailure> {
            self.counters.consumer_lag.fetch_add(1, Ordering::SeqCst);
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
        ) -> Result<BrokerRuntimeTargetStatus, ToolFailure> {
            self.counters.probes.fetch_add(1, Ordering::SeqCst);
            Ok(BrokerRuntimeTargetStatus::NotFound)
        }

        async fn shutdown(self) -> Result<(), ToolFailure> {
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

    fn facade() -> (QueryFacade<Factory>, Arc<Counters>) {
        let factory = Factory::default();
        let counters = factory.counters.clone();
        (QueryFacade::with_factory(config(), factory), counters)
    }

    fn arguments() -> ListBrokersArgs {
        ListBrokersArgs {
            cluster: "local-dev".to_string(),
        }
    }

    /// Reads the `brokers` Resource of the cluster the way a client does.
    async fn read_brokers_resource(facade: &QueryFacade<Factory>) -> serde_json::Value {
        let uri = RocketmqResourceUri::new("local-dev".to_string(), ResourceKind::Brokers).as_string();
        let result = reader::read_resource(facade, &uri).await.unwrap();
        match &result.contents[0] {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => serde_json::from_str(text).unwrap(),
            contents => panic!("expected text resource contents, got {contents:?}"),
        }
    }

    #[tokio::test]
    async fn list_brokers_uses_one_workflow_and_no_inventory_queries() {
        let (facade, counters) = facade();

        let listed = facade.list_brokers(arguments()).await.unwrap();
        assert_eq!(listed.cache_status, CacheStatus::Miss);
        assert!(!listed.partial);
        assert_eq!(listed.data.cluster, "local-dev");
        assert_eq!(
            listed
                .data
                .brokers
                .iter()
                .map(|broker| (broker.broker_name.as_str(), broker.broker_id))
                .collect::<Vec<_>>(),
            [("broker-a", 0), ("broker-a", 1)]
        );
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_rows.load(Ordering::SeqCst), 1);
        assert_eq!(counters.other_reads(), 0, "{counters:?}");

        // A repeat within the freshness window is served without a session.
        let repeated = facade.list_brokers(arguments()).await.unwrap();
        assert_eq!(repeated.cache_status, CacheStatus::Hit);
        assert_eq!(repeated.data.brokers, listed.data.brokers);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_rows.load(Ordering::SeqCst), 1);

        // The overview is the workflow this one replaces for a Broker list: it sizes both inventories.
        facade
            .cluster_overview(ClusterOverviewArgs {
                cluster: "local-dev".to_string(),
            })
            .await
            .unwrap();
        assert!(counters.inventory_reads() >= 2, "{counters:?}");
    }

    #[tokio::test]
    async fn brokers_resource_no_longer_loads_inventories() {
        let (facade, counters) = facade();

        let payload = read_brokers_resource(&facade).await;
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_rows.load(Ordering::SeqCst), 1);
        assert_eq!(counters.other_reads(), 0, "{counters:?}");

        // The Resource and the Tool publish the same rows, from one read.
        let listed = ReadOnlyQuery::list_brokers(&facade, arguments()).await.unwrap();
        assert_eq!(listed.cache_status, CacheStatus::Hit);
        assert_eq!(payload["brokers"], serde_json::to_value(&listed.data.brokers).unwrap());
        assert_eq!(payload["brokers"].as_array().unwrap().len(), 2);
        assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
        assert_eq!(counters.broker_rows.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn list_brokers_propagates_partial_evidence() {
        let (facade, counters) = facade();
        counters.partial.store(true, Ordering::SeqCst);

        let listed = facade.list_brokers(arguments()).await.unwrap();
        assert!(listed.partial);
        assert_eq!(listed.data.brokers.len(), 2, "the rows that did answer are kept");
        assert!(listed.warnings.contains(&"broker_runtime_unavailable".to_string()));
        assert!(listed.warnings.contains(&"source_failures_present".to_string()));
        assert_eq!(
            listed.source_failures,
            [SourceFailure {
                source: QuerySource::BrokerRuntime,
                code: SourceFailureCode::Timeout,
                retryable: true,
                logical_target: "broker-b".to_string(),
            }]
        );

        // The Resource reports the same incompleteness instead of a shorter list that looks whole.
        let payload = read_brokers_resource(&facade).await;
        assert_eq!(payload["partial"], true);
        assert_eq!(payload["source_failures"][0]["logical_target"], "broker-b");
    }
}
