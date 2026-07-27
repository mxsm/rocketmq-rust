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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

/// Bounded undirected projection of the latest resource topology.
///
/// Direction remains available in the source inventory. Correlation uses the
/// undirected projection because an alert can originate at either end of a
/// known dependency.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceGraph {
    adjacency: BTreeMap<String, BTreeSet<String>>,
}

impl ResourceGraph {
    /// Adds one sanitized topology relationship.
    pub fn add_edge(&mut self, from: impl Into<String>, to: impl Into<String>) {
        let from = from.into();
        let to = to.into();
        if from == to {
            return;
        }
        self.adjacency.entry(from.clone()).or_default().insert(to.clone());
        self.adjacency.entry(to).or_default().insert(from);
    }

    /// Returns the shortest distance when two resources are connected within
    /// the caller-provided hop bound.
    #[must_use]
    pub fn distance_within(&self, from: &str, to: &str, max_hops: u8) -> Option<u8> {
        if from == to {
            return Some(0);
        }
        if max_hops == 0 {
            return None;
        }
        let mut visited = BTreeSet::from([from.to_owned()]);
        let mut queue = VecDeque::from([(from.to_owned(), 0_u8)]);
        while let Some((current, distance)) = queue.pop_front() {
            if distance >= max_hops {
                continue;
            }
            let Some(neighbors) = self.adjacency.get(&current) else {
                continue;
            };
            for neighbor in neighbors {
                let next_distance = distance.saturating_add(1);
                if neighbor == to {
                    return Some(next_distance);
                }
                if visited.insert(neighbor.clone()) {
                    queue.push_back((neighbor.clone(), next_distance));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlates_all_required_cross_component_chains_with_a_bound() {
        let mut graph = ResourceGraph::default();
        graph.add_edge("topic:orders", "queue:orders-0");
        graph.add_edge("queue:orders-0", "broker:broker-a");
        graph.add_edge("broker:broker-a", "store:broker-a");
        graph.add_edge("broker:broker-a", "controller:controller-a");
        graph.add_edge("controller:controller-a", "node:node-a");
        graph.add_edge("node:node-a", "pod:broker-a-0");
        graph.add_edge("consumer_group:billing", "connection:client-42");
        graph.add_edge("connection:client-42", "broker:broker-a");

        assert_eq!(graph.distance_within("topic:orders", "store:broker-a", 3), Some(3));
        assert_eq!(graph.distance_within("broker:broker-a", "pod:broker-a-0", 3), Some(3));
        assert_eq!(
            graph.distance_within("consumer_group:billing", "broker:broker-a", 2),
            Some(2)
        );
        assert_eq!(graph.distance_within("topic:orders", "pod:broker-a-0", 3), None);
    }
}
