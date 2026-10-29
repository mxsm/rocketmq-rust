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

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
};

use cheetah_string::CheetahString;
use rocketmq_client_rust::{
    BrokerAdmin as _, DefaultMQAdminExt, MQAdminReadExt as _, MQAdminTopicSubscriberReadExt as _,
};
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData;

use super::lifecycle::AdminSession;
use crate::core::{
    dashboard_metrics::{
        DashboardConsumerSummary, DashboardMetricsAdmin, DashboardTopicRates, DASHBOARD_RATE_CANDIDATES,
    },
    AdminError, AdminFuture,
};

impl DashboardMetricsAdmin for AdminSession {
    fn dashboard_topic_names(&self) -> AdminFuture<'_, Vec<String>> {
        Box::pin(async move {
            self.ensure_open()?;
            let response = self.inner.fetch_all_topic_list().await.map_err(|error| {
                AdminError::from_error(
                    "dashboard_topic_names",
                    crate::IntoCanonicalError::into_canonical_error(error),
                )
            })?;
            let mut topics = response
                .topic_list
                .into_iter()
                .map(|topic| topic.to_string())
                .collect::<Vec<_>>();
            topics.sort();
            topics.dedup();
            Ok(topics)
        })
    }

    fn dashboard_topic_rates<'a>(&'a self, topics: &'a [String]) -> AdminFuture<'a, Vec<DashboardTopicRates>> {
        Box::pin(async move {
            self.ensure_open()?;
            Ok(collect_rates(&self.inner, topics).await)
        })
    }

    fn dashboard_consumer_summary(&self) -> AdminFuture<'_, DashboardConsumerSummary> {
        Box::pin(super::consumer::dashboard_consumer_summary(self))
    }
}

// This seam tests the actual sampling loop and its RPC budget, including high-fanout Topics.
trait TopicRateSource: Sync {
    fn masters<'a>(&'a self, topic: &'a str) -> impl Future<Output = Option<Vec<Option<CheetahString>>>> + Send + 'a;
    fn groups<'a>(
        &'a self,
        address: CheetahString,
        topic: &'a str,
    ) -> impl Future<Output = Option<Vec<CheetahString>>> + Send + 'a;
    fn stats<'a>(
        &'a self,
        address: CheetahString,
        metric: &'static str,
        key: CheetahString,
    ) -> impl Future<Output = Option<BrokerStatsData>> + Send + 'a;
}

impl TopicRateSource for DefaultMQAdminExt {
    async fn masters(&self, topic: &str) -> Option<Vec<Option<CheetahString>>> {
        self.examine_topic_route_info(topic.into())
            .await
            .ok()
            .flatten()
            .map(|route| {
                route
                    .broker_datas
                    .into_iter()
                    .map(|broker| broker.broker_addrs().get(&MASTER_ID).cloned())
                    .collect()
            })
    }
    async fn groups(&self, address: CheetahString, topic: &str) -> Option<Vec<CheetahString>> {
        // This exact-target capability performs one Broker RPC and no route rediscovery.
        self.topic_consumer_groups_at(address, topic.into())
            .await
            .ok()
            .map(|groups| groups.group_list.into_iter().collect())
    }
    async fn stats(&self, address: CheetahString, metric: &'static str, key: CheetahString) -> Option<BrokerStatsData> {
        self.view_broker_stats_data(address, metric.into(), key).await.ok()
    }
}

async fn collect_rates(source: &impl TopicRateSource, topics: &[String]) -> Vec<DashboardTopicRates> {
    let mut remaining_calls = 2_000usize;
    let mut result = Vec::new();
    for topic in topics.iter().take(DASHBOARD_RATE_CANDIDATES) {
        let mut observation = DashboardTopicRates {
            topic: topic.clone(),
            produced_messages_24h: None,
            produce_tps: None,
            consume_tps: None,
            failed_targets: 0,
        };
        let masters = if take_call(&mut remaining_calls) {
            source.masters(topic).await
        } else {
            None
        };
        let Some(masters) = masters.filter(|masters| !masters.is_empty()) else {
            observation.failed_targets = 1;
            result.push(observation);
            continue;
        };
        let mut production = RateTotal::default();
        for address in &masters {
            let stats = match address {
                Some(address) if take_call(&mut remaining_calls) => {
                    source
                        .stats(address.clone(), "TOPIC_PUT_NUMS", topic.as_str().into())
                        .await
                }
                _ => None,
            };
            production.observe(stats.as_ref());
        }
        observation.produce_tps = production.rate();
        observation.produced_messages_24h = production.count();
        observation.failed_targets += production.failed;
        let mut consumption = RateTotal::default();
        let mut group_targets = BTreeMap::<CheetahString, BTreeSet<CheetahString>>::new();
        for address in &masters {
            let groups = match address {
                Some(address) if take_call(&mut remaining_calls) => source.groups(address.clone(), topic).await,
                _ => None,
            };
            match (address, groups) {
                (Some(address), Some(groups)) => {
                    for group in groups {
                        group_targets.entry(group).or_default().insert(address.clone());
                    }
                }
                _ => consumption.failed += 1,
            }
        }
        for (group, addresses) in group_targets {
            for address in addresses {
                let stats = if take_call(&mut remaining_calls) {
                    source
                        .stats(address, "GROUP_GET_NUMS", format!("{topic}@{group}").into())
                        .await
                } else {
                    None
                };
                consumption.observe(stats.as_ref());
            }
        }
        // Zero is established only after every master reports a complete empty group set.
        observation.consume_tps = consumption.rate();
        observation.failed_targets += consumption.failed;
        result.push(observation);
    }
    result
}

fn take_call(remaining: &mut usize) -> bool {
    if *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    true
}

#[derive(Default)]
struct RateTotal {
    tps: f64,
    messages: u64,
    failed: usize,
}
impl RateTotal {
    fn observe(&mut self, stats: Option<&BrokerStatsData>) {
        let Some(stats) = stats else {
            self.failed += 1;
            return;
        };
        let rate = stats.get_stats_minute().get_tps();
        if !rate.is_finite() || rate < 0.0 {
            self.failed += 1;
            return;
        }
        self.tps += rate;
        let count = if stats.get_stats_day().get_sum() != 0 {
            stats.get_stats_day().get_sum()
        } else if stats.get_stats_hour().get_sum() != 0 {
            stats.get_stats_hour().get_sum()
        } else {
            stats.get_stats_minute().get_sum()
        };
        match self.messages.checked_add(count) {
            Some(total) => self.messages = total,
            None => self.failed += 1,
        }
    }
    fn rate(&self) -> Option<f64> {
        (self.failed == 0 && self.tps.is_finite()).then_some(self.tps)
    }
    fn count(&self) -> Option<u64> {
        (self.failed == 0).then_some(self.messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::body::broker_stats_item::BrokerStatsItem;
    use std::sync::Mutex;

    struct Source {
        calls: Mutex<Vec<String>>,
        fanout: usize,
        last_master_only: bool,
    }
    impl Source {
        fn new(fanout: usize) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fanout,
                last_master_only: false,
            }
        }

        fn call(&self, value: String) {
            self.calls.lock().unwrap().push(value);
        }
    }
    impl TopicRateSource for Source {
        async fn masters(&self, topic: &str) -> Option<Vec<Option<CheetahString>>> {
            self.call(format!("route:{topic}"));
            Some(
                (0..self.fanout)
                    .map(|index| Some(format!("broker-{index}").into()))
                    .collect(),
            )
        }
        async fn groups(&self, address: CheetahString, topic: &str) -> Option<Vec<CheetahString>> {
            self.call(format!("groups:{address}:{topic}"));
            let last_master = format!("broker-{}", self.fanout - 1);
            if topic == "group-failure" && address.as_str() == last_master {
                return None;
            }
            if self.last_master_only && address.as_str() != last_master {
                return Some(Vec::new());
            }
            Some(vec!["group".into(), "group".into()])
        }
        async fn stats(
            &self,
            address: CheetahString,
            metric: &'static str,
            key: CheetahString,
        ) -> Option<BrokerStatsData> {
            self.call(format!("stats:{address}:{metric}:{key}"));
            if key.starts_with("failed") {
                return None;
            }
            let tps = if key.starts_with("zero") { 0.0 } else { 0.5 };
            let item = BrokerStatsItem::new(30, tps, 0.0);
            Some(BrokerStatsData::new(item.clone(), item.clone(), item))
        }
    }

    #[tokio::test]
    async fn dashboard_rates_preserve_fractional_zero_and_independent_topic_failures() {
        let source = Source::new(1);
        let rates = collect_rates(&source, &["fractional".into(), "failed".into(), "zero".into()]).await;
        assert_eq!(rates[0].produce_tps, Some(0.5));
        assert_eq!(rates[0].consume_tps, Some(0.5));
        assert_eq!(rates[1].produce_tps, None);
        assert_eq!(rates[1].consume_tps, None);
        assert!(rates[1].failed_targets > 0);
        assert_eq!(rates[2].produce_tps, Some(0.0));
    }

    #[tokio::test]
    async fn dashboard_consume_rates_discover_every_exact_master_and_reject_partial_discovery() {
        let mut source = Source::new(2);
        source.last_master_only = true;
        let rates = collect_rates(&source, &["orders".into(), "group-failure".into()]).await;
        assert_eq!(rates[0].produce_tps, Some(1.0));
        assert_eq!(rates[0].consume_tps, Some(0.5));
        assert_eq!(rates[0].failed_targets, 0);
        assert_eq!(rates[1].produce_tps, Some(1.0));
        assert_eq!(rates[1].consume_tps, None);
        assert!(rates[1].failed_targets > 0);
        let calls = source.calls.lock().unwrap();
        assert!(calls.contains(&"groups:broker-0:orders".to_string()));
        assert!(calls.contains(&"groups:broker-1:orders".to_string()));
        assert!(calls.contains(&"stats:broker-1:GROUP_GET_NUMS:orders@group".to_string()));
        assert!(!calls.contains(&"stats:broker-0:GROUP_GET_NUMS:orders@group".to_string()));
        assert_eq!(calls.iter().filter(|call| call.starts_with("route:")).count(), 2);
    }

    #[tokio::test]
    async fn dashboard_rate_rpc_budget_stops_high_fanout_and_keeps_coverage_unknown() {
        let source = Source::new(100);
        let topics = (0..205).map(|index| format!("topic-{index:03}")).collect::<Vec<_>>();
        let rates = collect_rates(&source, &topics).await;
        assert_eq!(source.calls.lock().unwrap().len(), 2_000);
        let calls = source.calls.lock().unwrap();
        assert_eq!(calls.iter().filter(|call| call.starts_with("route:")).count(), 7);
        assert_eq!(calls.iter().filter(|call| call.starts_with("groups:")).count(), 693);
        assert_eq!(
            calls.iter().filter(|call| call.contains(":TOPIC_PUT_NUMS:")).count(),
            700
        );
        assert_eq!(
            calls.iter().filter(|call| call.contains(":GROUP_GET_NUMS:")).count(),
            600
        );
        drop(calls);
        assert_eq!(rates.len(), 200);
        assert_eq!(rates[0].produce_tps, Some(50.0));
        assert_eq!(rates.last().unwrap().produce_tps, None);
        assert!(rates.last().unwrap().failed_targets > 0);
        assert!(!source
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.contains("topic-200")));
    }
}
