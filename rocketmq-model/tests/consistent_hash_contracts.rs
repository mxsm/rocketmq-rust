// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::Arc;

use rocketmq_model::consistent_hash::{ConsistentHashRouter, HashFunction, Node};

#[derive(Clone)]
struct TestNode(&'static str);

impl Node for TestNode {
    fn get_key(&self) -> &str {
        self.0
    }
}

struct Positions;

impl HashFunction for Positions {
    fn hash(&self, key: &str) -> i64 {
        match key {
            "a-0" | "at10" => 10,
            "b-0" | "at20" => 20,
            "a-1" | "at30" => 30,
            "a-2" | "at40" => 40,
            "below" => 5,
            "between" => 15,
            "above" => 25,
            "wrap" => 50,
            _ => panic!("unexpected fixture key: {key}"),
        }
    }
}

fn assert_routes(router: &ConsistentHashRouter<TestNode>, cases: &[(&str, Option<&str>)]) {
    for (query, expected) in cases {
        assert_eq!(
            router.route_node_ref(query).map(Node::get_key),
            *expected,
            "query: {query}"
        );
        let owned = router.route_node(query);
        assert_eq!(owned.as_ref().map(Node::get_key), *expected, "query: {query}");
    }
}

#[test]
fn exact_positions_successors_and_wraparound_select_expected_nodes() {
    let router =
        ConsistentHashRouter::new_with_hash_function(vec![TestNode("a"), TestNode("b")], 1, Arc::new(Positions));
    assert_routes(
        &router,
        &[
            ("below", Some("a")),
            ("at10", Some("a")),
            ("between", Some("b")),
            ("at20", Some("b")),
            ("above", Some("a")),
        ],
    );
}

#[test]
fn adding_replicas_and_removing_nodes_updates_routes_and_counts() {
    let mut router =
        ConsistentHashRouter::new_with_hash_function(vec![TestNode("a"), TestNode("b")], 1, Arc::new(Positions));
    router.try_add_node(TestNode("a"), 2).unwrap();
    assert_eq!(router.get_existing_replicas(&TestNode("a")), 3);
    assert_eq!(router.get_existing_replicas(&TestNode("b")), 1);
    assert_eq!(router.size(), 4);
    assert_routes(
        &router,
        &[
            ("at20", Some("b")),
            ("above", Some("a")),
            ("at30", Some("a")),
            ("at40", Some("a")),
            ("wrap", Some("a")),
        ],
    );
    router.remove_node(&TestNode("a"));
    assert_eq!(router.get_existing_replicas(&TestNode("a")), 0);
    assert_eq!(router.get_existing_replicas(&TestNode("b")), 1);
    assert_eq!(router.size(), 1);
    router.remove_node(&TestNode("absent"));
    assert_eq!(router.size(), 1);
    assert_routes(
        &router,
        &[
            ("below", Some("b")),
            ("at20", Some("b")),
            ("at30", Some("b")),
            ("wrap", Some("b")),
        ],
    );
    router.remove_node(&TestNode("b"));
    assert!(router.is_empty());
    assert_routes(&router, &[("at10", None), ("wrap", None)]);
}

#[test]
fn zero_and_negative_replica_counts_preserve_ring_contracts() {
    let empty = ConsistentHashRouter::new_with_hash_function(vec![TestNode("a")], 0, Arc::new(Positions));
    assert!(empty.is_empty());
    assert_eq!(empty.get_existing_replicas(&TestNode("a")), 0);
    assert_routes(&empty, &[("below", None), ("at20", None)]);
    assert!(ConsistentHashRouter::try_new_with_hash_function(vec![TestNode("a")], -1, Arc::new(Positions)).is_err());
    let mut router =
        ConsistentHashRouter::new_with_hash_function(vec![TestNode("a"), TestNode("b")], 1, Arc::new(Positions));
    assert!(router.try_add_node(TestNode("a"), -1).is_err());
    assert_eq!(router.size(), 2);
    assert_eq!(router.get_existing_replicas(&TestNode("a")), 1);
    assert_routes(
        &router,
        &[("at10", Some("a")), ("between", Some("b")), ("above", Some("a"))],
    );
}
