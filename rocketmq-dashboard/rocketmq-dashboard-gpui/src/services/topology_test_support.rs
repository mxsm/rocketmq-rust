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
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};
use tokio::sync::oneshot;

type Gate = (oneshot::Sender<()>, oneshot::Receiver<()>, oneshot::Sender<()>);
#[derive(Default)]
pub(crate) struct FakeTopologyBackend {
    calls: Mutex<Vec<TopologyReadRequest>>,
    failures: Mutex<BTreeMap<TopologySource, VecDeque<UiError>>>,
    gates: Mutex<BTreeMap<TopologySource, Gate>>,
}
struct Finished(Option<oneshot::Sender<()>>);
impl Drop for Finished {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
impl FakeTopologyBackend {
    pub fn calls(&self) -> Vec<TopologyReadRequest> {
        self.calls.lock().unwrap().clone()
    }
    pub fn fail(&self, source: TopologySource, error: UiError) {
        self.failures
            .lock()
            .unwrap()
            .entry(source)
            .or_default()
            .push_back(error);
    }
    pub fn hold(&self, source: TopologySource) -> (oneshot::Receiver<()>, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (start, started) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        self.gates.lock().unwrap().insert(source, (start, wait, finish));
        (started, release, finished)
    }
}
impl TopologyBackend for FakeTopologyBackend {
    fn layer(&self, request: TopologyReadRequest) -> ServiceFuture<'_, Result<TopologyLayer, UiError>> {
        self.calls.lock().unwrap().push(request.clone());
        let failure = self
            .failures
            .lock()
            .unwrap()
            .get_mut(&request.source)
            .and_then(VecDeque::pop_front);
        let gate = self.gates.lock().unwrap().remove(&request.source);
        Box::pin(async move {
            if let Some((start, release, finish)) = gate {
                let _finished = Finished(Some(finish));
                let _ = start.send(());
                let _ = release.await;
            }
            match failure {
                Some(error) => Err(error),
                None => Ok(layer(request.source, request.observed_at_ms)),
            }
        })
    }
}
pub fn broker(cluster: &str) -> BrokerIdentity {
    BrokerIdentity {
        cluster: cluster.into(),
        broker_name: format!("broker-{cluster}"),
        broker_id: 0,
        address: format!("broker-{cluster}:10911"),
    }
}
pub fn layer(source: TopologySource, now: u64) -> TopologyLayer {
    let mut layer = TopologyLayer::empty(source, now);
    match source {
        TopologySource::Config => {}
        TopologySource::Nameservers => layer.nodes.push(TopologyNode::new(
            TopologyNodeId::NameServer("one:9876".into()),
            "one:9876".into(),
            source,
            now,
        )),
        TopologySource::Brokers => {
            for cluster in ["a", "b"] {
                let identity = broker(cluster);
                let mut group = TopologyNode::new(TopologyNodeId::Cluster(cluster.into()), cluster.into(), source, now);
                group.clusters.insert(cluster.into());
                layer.nodes.push(group);
                let mut node = TopologyNode::new(
                    TopologyNodeId::Broker(identity.clone()),
                    identity.broker_name,
                    source,
                    now,
                );
                node.clusters.insert(cluster.into());
                node.availability = EndpointAvailability::Available;
                node.traffic.produce_tps = Some(0.5);
                layer.edges.push(TopologyEdge::new(
                    TopologyNodeId::Cluster(cluster.into()),
                    node.id.clone(),
                    TopologyEdgeKind::Registration,
                    source,
                    now,
                ));
                layer.nodes.push(node);
            }
        }
        TopologySource::Topics => {
            for (topic, cluster) in [("orders", "a"), ("audit", "b")] {
                let mut node = TopologyNode::new(TopologyNodeId::Topic(topic.into()), topic.into(), source, now);
                node.clusters.insert(cluster.into());
                layer.edges.push(TopologyEdge::new(
                    TopologyNodeId::Broker(broker(cluster)),
                    node.id.clone(),
                    TopologyEdgeKind::TopicRoute,
                    source,
                    now,
                ));
                layer.nodes.push(node);
            }
        }
        TopologySource::Producers | TopologySource::Consumers => {
            let id = if source == TopologySource::Producers {
                TopologyNodeId::Producer("producer-a".into())
            } else {
                TopologyNodeId::Consumer("consumer-a".into())
            };
            let mut node = TopologyNode::new(
                id,
                if source == TopologySource::Producers {
                    "producer-a".into()
                } else {
                    "consumer-a".into()
                },
                source,
                now,
            );
            node.clusters.insert("a".into());
            node.traffic.connections = Some(1);
            let (from, to, kind) = if source == TopologySource::Producers {
                (
                    node.id.clone(),
                    TopologyNodeId::Broker(broker("a")),
                    TopologyEdgeKind::Produce,
                )
            } else {
                (
                    TopologyNodeId::Broker(broker("a")),
                    node.id.clone(),
                    TopologyEdgeKind::Consume,
                )
            };
            layer.edges.push(TopologyEdge::new(from, to, kind, source, now));
            layer.nodes.push(node);
        }
        TopologySource::Replication => {}
    }
    layer.total = layer.nodes.len();
    layer.total_edges = layer.edges.len();
    layer
}
