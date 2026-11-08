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

fn context(revision: u64) -> TopologyContext {
    TopologyContext {
        environment: None,
        revision,
        scope: ConnectionScope::NameServer,
    }
}
fn topic(name: &str, cluster: &str, now: u64) -> TopologyNode {
    let mut node = TopologyNode::new(
        TopologyNodeId::Topic(name.into()),
        name.into(),
        TopologySource::Topics,
        now,
    );
    node.clusters.insert(cluster.into());
    node
}
fn observed(nodes: Vec<TopologyNode>, now: u64) -> TopologyLayer {
    TopologyLayer {
        total: nodes.len(),
        nodes,
        ..TopologyLayer::empty(TopologySource::Topics, now)
    }
}

#[test]
fn topology_partial_failure_keeps_other_layers_and_never_claims_an_unknown_deletion() {
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![observed(vec![topic("orders", "a", 100)], 100)], 100);
    let mut partial = observed(vec![topic("audit", "a", 200)], 200);
    partial.state = TopologySourceState::Partial;
    partial.inventory_complete = false;
    let diff = cache.apply(context(1), vec![partial], 200);
    assert!(diff.removed.is_empty());
    assert_eq!(cache.snapshot().unwrap().nodes().len(), 2);
    let layout = layout_topology(cache.snapshot().unwrap(), None, 1.);
    assert!(
        layout
            .nodes
            .iter()
            .find(|node| node.node.label == "orders")
            .unwrap()
            .stale
    );
    assert!(
        !layout
            .nodes
            .iter()
            .find(|node| node.node.label == "audit")
            .unwrap()
            .stale
    );
    assert!(cache.snapshot().unwrap().layers[&TopologySource::Topics].is_stale());
    let diff = cache.apply(
        context(1),
        vec![TopologyLayer::failed(
            TopologySource::Topics,
            300,
            TopologyFailureCode::Connection,
        )],
        300,
    );
    assert!(diff.removed.is_empty());
    let snapshot = cache.snapshot().unwrap();
    assert!(snapshot.all_remote_failed());
    assert_eq!(snapshot.layers[&TopologySource::Topics].observed_at_ms, Some(200));
    assert_eq!(snapshot.layers[&TopologySource::Topics].total, 1);
    assert!(snapshot.layers[&TopologySource::Topics].total_complete);
    assert_eq!(snapshot.nodes().len(), 2);
    assert!(layout_topology(snapshot, None, 1.).nodes.iter().all(|node| node.stale));
}

#[test]
fn topology_expiry_and_complete_absence_are_distinct_and_context_clears_old_selection_data() {
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![observed(vec![topic("orders", "a", 100)], 100)], 100);
    let now = 100 + TOPOLOGY_CACHE_TTL_MS + 1;
    let diff = cache.apply(
        context(1),
        vec![TopologyLayer::failed(
            TopologySource::Topics,
            now,
            TopologyFailureCode::Connection,
        )],
        now,
    );
    assert!(diff.removed.is_empty());
    assert_eq!(diff.unconfirmed_missing, 1);
    assert!(cache.snapshot().unwrap().layers[&TopologySource::Topics].expired);
    cache.apply(context(1), vec![observed(vec![topic("orders", "a", now)], now)], now);
    let diff = cache.apply(context(1), vec![observed(Vec::new(), now + 1)], now + 1);
    assert_eq!(diff.removed, vec![TopologyNodeId::Topic("orders".into())]);
    cache.apply(
        context(2),
        vec![TopologyLayer::failed(
            TopologySource::Topics,
            now + 2,
            TopologyFailureCode::Connection,
        )],
        now + 2,
    );
    assert!(cache.snapshot().unwrap().nodes().is_empty());
}

#[test]
fn topology_layout_is_stable_clips_cross_cluster_edges_and_compares_every_broker_identity_field() {
    let a = topic("same", "a", 100);
    let b = topic("other", "b", 100);
    let mut layer = observed(vec![b.clone(), a.clone()], 100);
    layer.edges.push(TopologyEdge::new(
        a.id.clone(),
        b.id.clone(),
        TopologyEdgeKind::TopicRoute,
        TopologySource::Topics,
        100,
    ));
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![layer.clone()], 100);
    let first = layout_topology(cache.snapshot().unwrap(), Some("a"), 1.);
    assert_eq!(first.nodes.len(), 1);
    assert!(first.edges.is_empty());
    layer.nodes.reverse();
    let diff = cache.apply(context(1), vec![layer], 100);
    assert!(diff.added.is_empty() && diff.changed.is_empty() && diff.removed.is_empty());
    assert_eq!(first, layout_topology(cache.snapshot().unwrap(), Some("a"), 1.));
    let broker = BrokerIdentity {
        cluster: "a".into(),
        broker_name: "same".into(),
        broker_id: 0,
        address: "one:10911".into(),
    };
    assert_ne!(
        TopologyNodeId::Broker(broker.clone()),
        TopologyNodeId::Broker(BrokerIdentity {
            address: "two:10911".into(),
            ..broker
        })
    );
}

#[test]
fn topology_display_and_cache_are_bounded_and_keep_pretruncation_counts() {
    let nodes = (0..TOPOLOGY_NODE_LIMIT + 20)
        .map(|index| topic(&format!("topic-{index:04}"), "a", 100))
        .collect::<Vec<_>>();
    let total = nodes.len();
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![observed(nodes, 100)], 100);
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.layers[&TopologySource::Topics].total, total);
    assert!(snapshot.layers[&TopologySource::Topics].truncated);
    assert_eq!(snapshot.nodes().len(), TOPOLOGY_NODE_LIMIT);
    let layout = layout_topology(snapshot, None, 0.75);
    assert_eq!(layout.nodes.len(), TOPOLOGY_NODE_LIMIT);
    assert!(layout.height.is_finite() && layout.width.is_finite());
}

#[test]
fn topology_resume_expiry_clears_old_edges_without_a_new_observation_or_false_removal() {
    let mut layer = observed(vec![topic("orders", "a", 100), topic("audit", "a", 100)], 100);
    layer.edges.push(TopologyEdge::new(
        layer.nodes[0].id.clone(),
        layer.nodes[1].id.clone(),
        TopologyEdgeKind::TopicRoute,
        TopologySource::Topics,
        100,
    ));
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![layer], 100);
    let diff = cache.expire(100 + TOPOLOGY_CACHE_TTL_MS + 1);
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.generated_at_ms, 100);
    assert_eq!(snapshot.layers[&TopologySource::Topics].observed_at_ms, Some(100));
    assert!(snapshot.nodes().is_empty() && snapshot.layers[&TopologySource::Topics].edges.is_empty());
    assert!(diff.removed.is_empty());
    assert_eq!(diff.unconfirmed_missing, 2);
    assert!(snapshot.all_remote_failed());
}

#[test]
fn topology_group_display_limits_keep_totals_and_do_not_draw_edges_to_omitted_groups() {
    let mut producers = TopologyLayer::empty(TopologySource::Producers, 100);
    let mut consumers = TopologyLayer::empty(TopologySource::Consumers, 100);
    for index in 0..70 {
        let producer = TopologyNode::new(
            TopologyNodeId::Producer(format!("producer-{index:03}")),
            format!("producer-{index:03}"),
            producers.source,
            100,
        );
        let consumer = TopologyNode::new(
            TopologyNodeId::Consumer(format!("consumer-{index:03}")),
            format!("consumer-{index:03}"),
            consumers.source,
            100,
        );
        producers.edges.push(TopologyEdge::new(
            producer.id.clone(),
            consumer.id.clone(),
            TopologyEdgeKind::Produce,
            producers.source,
            100,
        ));
        producers.nodes.push(producer);
        consumers.nodes.push(consumer);
    }
    producers.total = 70;
    consumers.total = 70;
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![producers, consumers], 100);
    let snapshot = cache.snapshot().unwrap();
    let layout = layout_topology(snapshot, None, 1.);
    assert_eq!(snapshot.layers[&TopologySource::Producers].total, 70);
    assert_eq!(snapshot.layers[&TopologySource::Consumers].total, 70);
    assert_eq!(
        layout
            .nodes
            .iter()
            .filter(|placed| matches!(placed.node.id, TopologyNodeId::Producer(_)))
            .count(),
        50
    );
    assert_eq!(
        layout
            .nodes
            .iter()
            .filter(|placed| matches!(placed.node.id, TopologyNodeId::Consumer(_)))
            .count(),
        50
    );
    assert_eq!(layout.omitted_nodes, 40);
    assert_eq!(layout.paths.len(), 50);
    let visible = layout.nodes.iter().map(|node| &node.node.id).collect::<BTreeSet<_>>();
    assert!(layout
        .edges
        .iter()
        .all(|edge| visible.contains(&edge.id.from) && visible.contains(&edge.id.to)));
}

fn replication_group(name: &str, observed: u64) -> TopologyReplicationGroup {
    TopologyReplicationGroup {
        cluster: "a".into(),
        broker_name: name.into(),
        observed_at_ms: observed,
        election: TopologyElection::Controller,
        controllers: vec!["controller-a:9878".into()],
        replicas: vec![TopologyReplica {
            identity: BrokerIdentity {
                cluster: "a".into(),
                broker_name: name.into(),
                broker_id: 1,
                address: "replica:10911".into(),
            },
            lag_bytes: Some(64),
            in_sync: Some(true),
            bytes_per_second: Some(32),
        }],
        sync_state: Some(TopologySyncState {
            master_broker_id: 0,
            master_epoch: 7,
            sync_state_set_epoch: 8,
            in_sync_broker_ids: vec![1],
            out_of_sync_broker_ids: Vec::new(),
        }),
        master_commit_log_max_offset: Some(1024),
        in_sync_slave_count: Some(1),
        config_observed_at_ms: Some(observed),
        ha_observed_at_ms: Some(observed),
        sync_observed_at_ms: Some(observed),
    }
}
fn controller(address: &str, observed: Option<u64>) -> TopologyControllerInfo {
    TopologyControllerInfo {
        address: address.into(),
        observed_at_ms: observed,
        group: observed.map(|_| "raft".into()),
        leader_id: observed.map(|_| "n0".into()),
        is_leader: observed.map(|_| true),
        peer_count: observed.map(|_| 3),
        last_log_index: observed.map(|_| 20),
        committed_log_index: observed.map(|_| 19),
        applied_log_index: observed.map(|_| 18),
    }
}

#[test]
fn topology_partial_refresh_expires_each_controller_and_replication_fact_without_poisoning_fresh_peers() {
    let mut cache = TopologyCache::default();
    let mut first = TopologyLayer::empty(TopologySource::Replication, 100);
    for address in ["controller-a:9878", "controller-b:9878"] {
        first.nodes.push(TopologyNode::new(
            TopologyNodeId::Controller(address.into()),
            address.into(),
            first.source,
            100,
        ));
        first.controllers.push(controller(address, Some(100)));
    }
    first.replication = vec![replication_group("old", 100), replication_group("fresh", 100)];
    cache.apply(context(1), vec![first], 100);
    for now in [1_000, 100 + TOPOLOGY_CACHE_TTL_MS + 1] {
        let mut partial = TopologyLayer::empty(TopologySource::Replication, now);
        partial.state = TopologySourceState::Partial;
        partial.inventory_complete = false;
        for address in ["controller-a:9878", "controller-b:9878"] {
            partial.nodes.push(TopologyNode::new(
                TopologyNodeId::Controller(address.into()),
                address.into(),
                partial.source,
                now,
            ));
        }
        partial.controllers = vec![
            controller("controller-a:9878", None),
            controller("controller-b:9878", Some(now)),
        ];
        let mut fresh = replication_group("fresh", now);
        fresh.config_observed_at_ms = Some(100);
        fresh.ha_observed_at_ms = Some(100);
        partial.replication = vec![fresh];
        let diff = cache.apply(context(1), vec![partial], now);
        assert!(diff.removed.is_empty());
        if now == 1_000 {
            assert_eq!(
                cache.snapshot().unwrap().layers[&TopologySource::Replication].controllers[0].observed_at_ms,
                Some(100)
            );
        }
    }
    let snapshot = cache.snapshot().unwrap();
    let source = &snapshot.layers[&TopologySource::Replication];
    let old = source
        .controllers
        .iter()
        .find(|controller| controller.address == "controller-a:9878")
        .unwrap();
    assert!(
        old.observed_at_ms.is_none()
            && old.group.is_none()
            && old.leader_id.is_none()
            && old.is_leader.is_none()
            && old.peer_count.is_none()
            && old.last_log_index.is_none()
            && old.committed_log_index.is_none()
            && old.applied_log_index.is_none()
    );
    assert_eq!(
        source
            .controllers
            .iter()
            .find(|controller| controller.address == "controller-b:9878")
            .unwrap()
            .observed_at_ms,
        source.observed_at_ms
    );
    assert_eq!(source.replication.len(), 1);
    let fresh = &source.replication[0];
    assert_eq!(fresh.broker_name, "fresh");
    assert_eq!(fresh.election, TopologyElection::Unknown);
    assert!(
        fresh.controllers.is_empty()
            && fresh.master_commit_log_max_offset.is_none()
            && fresh.in_sync_slave_count.is_none()
    );
    assert!(fresh
        .replicas
        .iter()
        .all(|replica| replica.lag_bytes.is_none() && replica.in_sync.is_none() && replica.bytes_per_second.is_none()));
    assert!(fresh.sync_state.is_some());
    assert!(!source.expired && !snapshot.all_remote_failed());
    assert!(layout_topology(snapshot, None, 1.)
        .nodes
        .iter()
        .all(|placed| !placed.stale));
}

#[test]
fn topology_expiring_an_old_route_does_not_expire_a_fresh_topic_observation() {
    let mut cache = TopologyCache::default();
    cache.apply(context(1), vec![observed(vec![topic("old", "a", 100)], 100)], 100);
    let now = 100 + TOPOLOGY_CACHE_TTL_MS + 1;
    let mut partial = observed(vec![topic("fresh", "a", now)], now);
    partial.state = TopologySourceState::Partial;
    partial.inventory_complete = false;
    let diff = cache.apply(context(1), vec![partial], now);
    let snapshot = cache.snapshot().unwrap();
    assert!(diff.removed.is_empty());
    assert_eq!(diff.unconfirmed_missing, 1);
    assert!(!snapshot.layers[&TopologySource::Topics].expired && !snapshot.all_remote_failed());
    let layout = layout_topology(snapshot, Some("a"), 1.);
    assert_eq!(layout.nodes.len(), 1);
    assert_eq!(layout.nodes[0].node.label, "fresh");
    assert!(!layout.nodes[0].stale);
}

#[test]
fn topology_replication_enrichment_cache_is_bounded_across_partial_refreshes() {
    let mut cache = TopologyCache::default();
    for page in 0..22 {
        let now = 100 + page;
        let mut layer = TopologyLayer::empty(TopologySource::Replication, now);
        layer.state = TopologySourceState::Partial;
        layer.inventory_complete = false;
        layer.total = 352;
        layer.replication = (0..16)
            .map(|index| replication_group(&format!("group-{:03}", page * 16 + index), now))
            .collect();
        cache.apply(context(1), vec![layer], now);
    }
    let layer = &cache.snapshot().unwrap().layers[&TopologySource::Replication];
    assert_eq!(layer.total, 352);
    assert_eq!(layer.replication.len(), TOPOLOGY_NODE_LIMIT);
    assert!(layer.truncated);
    assert!(layer.replication.iter().any(|group| group.broker_name == "group-351"));
    assert!(!layer.replication.iter().any(|group| group.broker_name == "group-000"));
}
