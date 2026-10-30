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

#[test]
fn topology_route_failure_retains_previous_cluster_membership_and_edges_with_original_time() {
    use rocketmq_dashboard_common::{TopologyCache, TopologyContext, TopologyNodeId, layout_topology};
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        let context = TopologyContext {
            environment: None,
            revision: 1,
            scope: ConnectionScope::NameServer,
        };
        let mut cache = TopologyCache::default();
        for (now, route_observed) in [(100, true), (200, false)] {
            let mut quality = TopologyReadQuality::complete(1);
            quality.successful_reads = 1;
            quality.failures = usize::from(!route_observed);
            quality.inventory_complete = route_observed;
            controls
                .topology_replies
                .lock()
                .unwrap()
                .push_back(TopologyAdminData::Topics(TopologyRead {
                    quality,
                    items: vec![TopologyTopicRow {
                        topic: "orders".into(),
                        brokers: if route_observed { vec![key("a", 0)] } else { Vec::new() },
                        route_observed,
                    }],
                }));
            let layer = provider.topology_layer(1, TopologySource::Topics, now).await.unwrap();
            assert_eq!(layer.total, 1);
            let diff = cache.apply(context.clone(), vec![layer], now);
            assert!(diff.removed.is_empty());
        }
        let snapshot = cache.snapshot().unwrap();
        let source = &snapshot.layers[&TopologySource::Topics];
        assert_eq!(source.state, TopologySourceState::Partial);
        assert_eq!(source.nodes[0].id, TopologyNodeId::Topic("orders".into()));
        assert_eq!(source.nodes[0].observed_at_ms, 100);
        assert_eq!(source.edges[0].observed_at_ms, 100);
        let layout = layout_topology(snapshot, Some("a"), 1.);
        assert_eq!(layout.nodes.len(), 1);
        assert!(layout.nodes[0].stale);
        provider.shutdown().await;
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
