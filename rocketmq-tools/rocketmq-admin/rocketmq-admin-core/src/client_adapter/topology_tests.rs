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

use super::*;
use crate::core::AdminError;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
struct FakeSource {
    targets: usize,
    groups: usize,
    fail_producer: bool,
    fail_routes: bool,
    topic_names: Option<usize>,
    calls: Mutex<Vec<String>>,
}
impl FakeSource {
    fn call(&self, stage: &str) {
        self.calls.lock().unwrap().push(stage.into());
    }
}
impl TopologySource for FakeSource {
    async fn cluster(&self) -> AdminResult<ClusterInfo> {
        self.call("cluster");
        let table = (0..self.targets)
            .map(|index| {
                let name = format!("broker-{index:02}");
                (
                    name.clone().into(),
                    BrokerData::new(
                        "cluster-a".into(),
                        name.into(),
                        HashMap::from([(0, format!("broker-{index:02}:10911").into())]),
                        None,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(ClusterInfo::new(
            Some(table.clone()),
            Some(HashMap::from([("cluster-a".into(), table.keys().cloned().collect())])),
        ))
    }
    async fn names(&self) -> AdminResult<Vec<String>> {
        self.call("names");
        Ok((0..self.topic_names.unwrap_or(150))
            .map(|index| format!("topic-{index:03}"))
            .collect())
    }
    async fn runtime(&self, _: &TopologyBrokerKey) -> AdminResult<BTreeMap<String, String>> {
        self.call("runtime");
        Ok(BTreeMap::from([("putTps".into(), "0.5 0.0 0.0".into())]))
    }
    async fn route(&self, _: &str) -> AdminResult<Option<TopicRouteData>> {
        self.call("route");
        if self.fail_routes {
            Err(AdminError::operation_failed("route", "unavailable"))
        } else {
            Ok(Some(TopicRouteData::default()))
        }
    }
    async fn producers(&self, _: &TopologyBrokerKey) -> AdminResult<Vec<(String, Vec<String>)>> {
        self.call("producer");
        if self.fail_producer {
            Err(AdminError::operation_failed("producer", "unavailable"))
        } else {
            Ok((0..self.groups)
                .map(|index| (format!("group-{index:03}"), vec!["same-client".into()]))
                .collect())
        }
    }
    async fn groups(&self, _: &TopologyBrokerKey) -> AdminResult<Vec<String>> {
        self.call("groups");
        Ok((0..self.groups).map(|index| format!("group-{index:03}")).collect())
    }
    async fn connections(&self, _: &TopologyBrokerKey, _: &str) -> AdminResult<Vec<String>> {
        self.call("connections");
        Ok(vec!["same-client".into()])
    }
    async fn progress(&self, _: &TopologyBrokerKey, _: &str) -> AdminResult<Option<(i64, f64)>> {
        self.call("progress");
        Ok(Some((3, 0.5)))
    }
    async fn config(&self, _: &TopologyBrokerKey) -> AdminResult<BTreeMap<String, String>> {
        self.call("config");
        Ok(BTreeMap::new())
    }
}

#[tokio::test]
async fn topology_budgets_bound_actual_source_calls_and_retain_counts_before_drawing() {
    let source = FakeSource {
        targets: 40,
        groups: 80,
        ..Default::default()
    };
    let read = brokers(&source).await.unwrap();
    assert_eq!(read.quality.total, 40);
    assert_eq!(read.items.len(), 32);
    assert_eq!(read.items[0].produce_tps, Some(0.5));
    assert_eq!(read.items[0].consume_tps, None);
    assert!(read.quality.truncated && !read.quality.inventory_complete);
    let topics = topics(&source).await.unwrap();
    assert_eq!(topics.quality.total, 150);
    assert_eq!(topics.items.len(), 128);
    let producers = producers(&source).await.unwrap();
    assert_eq!(producers.quality.total, 80);
    assert!(!producers.quality.total_complete);
    assert_eq!(producers.items.len(), 50);
    assert!(producers.items.iter().all(|row| row.connections.is_none()));
    let calls = source.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|call| *call == "runtime").count(), 32);
    assert_eq!(calls.iter().filter(|call| *call == "route").count(), 128);
    assert_eq!(calls.iter().filter(|call| *call == "producer").count(), 16);
}

#[tokio::test]
async fn topology_consumer_enrichment_spends_one_global_pair_budget_and_never_fills_missing_metrics_with_zero() {
    let source = FakeSource {
        targets: 16,
        groups: 80,
        ..Default::default()
    };
    let read = consumers(&source).await.unwrap();
    assert_eq!(read.quality.total, 80);
    assert!(read.quality.total_complete);
    assert!(read.quality.truncated);
    assert_eq!(read.items.len(), 50);
    assert_eq!(read.items[0].connections, Some(1));
    assert_eq!(read.items[0].backlog_messages, Some(48));
    assert_eq!(read.items[0].consume_tps, Some(8.));
    assert!(read.items[4..]
        .iter()
        .all(|row| row.connections.is_none() && row.backlog_messages.is_none() && row.consume_tps.is_none()));
    let calls = source.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|call| *call == "groups").count(), 16);
    assert_eq!(calls.iter().filter(|call| *call == "connections").count(), 64);
    assert_eq!(calls.iter().filter(|call| *call == "progress").count(), 64);
}

#[test]
fn topology_controller_mode_requires_actual_boolean_evidence_and_preserves_unknown() {
    assert_eq!(
        replication::controller_settings(&BTreeMap::new()).0,
        TopologyMasterElection::Unknown
    );
    let entries = |value: &str| {
        BTreeMap::from([
            ("enableControllerMode".into(), value.into()),
            ("controllerAddr".into(), " b:9878;a:9878;a:9878; ".into()),
        ])
    };
    assert_eq!(
        replication::controller_settings(&entries("false")),
        (TopologyMasterElection::Configured, Vec::new())
    );
    assert_eq!(
        replication::controller_settings(&entries("TRUE")),
        (
            TopologyMasterElection::Controller,
            vec!["a:9878".into(), "b:9878".into()]
        )
    );
    assert_eq!(
        replication::controller_settings(&entries("invalid")).0,
        TopologyMasterElection::Unknown
    );
}

#[tokio::test]
async fn topology_failed_route_keeps_the_known_topic_total_but_cannot_establish_route_absence() {
    let source = FakeSource {
        fail_routes: true,
        topic_names: Some(1),
        ..Default::default()
    };
    let read = topics(&source).await.unwrap();
    assert_eq!(read.quality.total, 1);
    assert!(read.quality.total_complete && !read.quality.truncated);
    assert!(!read.quality.inventory_complete && read.quality.failures > 0 && read.quality.successful_reads > 0);
    assert_eq!(read.items[0].topic, "topic-000");
    assert!(!read.items[0].route_observed);
    assert_eq!(*source.calls.lock().unwrap(), vec!["names", "route"]);
}
