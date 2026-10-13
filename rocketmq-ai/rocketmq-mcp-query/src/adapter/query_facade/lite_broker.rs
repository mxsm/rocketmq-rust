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

//! What one Broker holds for Lite Topics.

use std::time::Duration;

use super::normalized_logical_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::infrastructure::snapshot::RetainedSize;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::tools::broker_tools::GetLiteBrokerInfoArgs;
use crate::tools::broker_tools::GetLiteBrokerInfoOutput;
use crate::tools::broker_tools::LiteTopicSummary;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl RetainedSize for LiteTopicSummary {
    fn retained_heap_size(&self) -> usize {
        self.topic
            .retained_heap_size()
            .saturating_add(self.bound_groups.retained_heap_size())
    }
}

impl RetainedSize for GetLiteBrokerInfoOutput {
    fn retained_heap_size(&self) -> usize {
        self.cluster
            .retained_heap_size()
            .saturating_add(self.broker_name.retained_heap_size())
            .saturating_add(self.store_type.retained_heap_size())
            .saturating_add(self.lite_topics.retained_heap_size())
            .saturating_add(self.generated_at.retained_heap_size())
    }
}

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Reads the Lite Topic state of the master of one Broker. The counters move with the
    /// traffic, so the answer is current for as long as other Broker metrics are.
    pub(crate) async fn lite_broker_info(
        &self,
        mut args: GetLiteBrokerInfoArgs,
    ) -> Result<QueryResult<GetLiteBrokerInfoOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.broker_name = normalized_logical_identifier("broker_name", &args.broker_name)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let key = self.cache_key(
            "lite_broker_info",
            &cluster.name,
            &format!("broker={}", args.broker_name),
        );
        let ttl = Duration::from_millis(self.config.cache.broker_metrics_ttl_ms);
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                || async {
                    self.run_workflow(cluster, move |session, _| {
                        Box::pin(async move {
                            session
                                .lite_broker_info(&args.broker_name)
                                .await
                                .map(QueryPayload::complete)
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
    use super::super::observation_fixture::config;
    use super::super::observation_fixture::StubFactory;
    use super::*;
    use crate::model::contract::CacheStatus;

    fn args(broker_name: &str) -> GetLiteBrokerInfoArgs {
        GetLiteBrokerInfoArgs {
            cluster: "local-dev".to_string(),
            broker_name: broker_name.to_string(),
        }
    }

    fn observed() -> GetLiteBrokerInfoOutput {
        GetLiteBrokerInfoOutput {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
            store_type: Some("RocksDB".to_string()),
            max_lmq_count: 1_000,
            current_lmq_count: 42,
            lite_subscription_count: 7,
            order_info_count: 3,
            consume_queue_count: 50,
            offset_count: 60,
            event_count: 5,
            lite_topic_count: 1,
            lite_topics: vec![LiteTopicSummary {
                topic: "lite-orders".to_string(),
                expiration_minutes: 30,
                bound_group_count: 1,
                bound_groups: vec!["workers".to_string()],
            }],
            truncated: false,
            generated_at: "1".to_string(),
        }
    }

    #[tokio::test]
    async fn the_lite_state_of_a_broker_is_read_once_and_kept_for_a_while() {
        let factory = StubFactory {
            lite_broker_info: Some(observed()),
            ..Default::default()
        };
        let facade = QueryFacade::with_factory(config(), factory.clone());

        let first = facade.lite_broker_info(args(" broker-a ")).await.unwrap();
        let second = facade.lite_broker_info(args("broker-a")).await.unwrap();

        assert_eq!(first.data, observed());
        assert_eq!(
            (first.cache_status, second.cache_status),
            (CacheStatus::Miss, CacheStatus::Hit)
        );
        assert!(!first.partial);
        assert_eq!(*factory.asked_brokers.lock().unwrap(), ["broker-a"]);
        assert_eq!(factory.used(), (1, 1, 1));
    }

    #[tokio::test]
    async fn an_address_is_no_broker_name_and_a_failed_read_is_an_error() {
        let factory = StubFactory::default();
        let facade = QueryFacade::with_factory(config(), factory.clone());

        for not_a_name in ["127.0.0.1:10911", "broker a", " ", "broker/a"] {
            assert!(facade.lite_broker_info(args(not_a_name)).await.is_err(), "{not_a_name}");
        }
        assert_eq!(factory.used(), (0, 0, 0));

        let error = facade.lite_broker_info(args("broker-a")).await.unwrap_err();
        assert_eq!(error.code(), "source_unavailable");
        assert_eq!(factory.used(), (1, 1, 1));
    }
}
