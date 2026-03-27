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

//! Multi-node cluster integration tests

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use openraft::ServerState;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::openraft::GrpcRaftService;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;
use rocketmq_controller::typ::RaftMetrics;
use rocketmq_rust::ArcMut;
use tonic::transport::Server;

fn cluster_peers(node_count: u64, base_port: u16) -> Vec<RaftPeer> {
    (1..=node_count)
        .map(|node_id| RaftPeer {
            id: node_id,
            addr: format!("127.0.0.1:{}", base_port + node_id as u16).parse().unwrap(),
        })
        .collect()
}

fn raft_node(node_id: u64, base_port: u16) -> Node {
    Node {
        node_id,
        rpc_addr: format!("127.0.0.1:{}", base_port + node_id as u16),
    }
}

fn membership_voters(metrics: &RaftMetrics) -> BTreeSet<u64> {
    metrics
        .membership_config
        .membership()
        .get_joint_config()
        .iter()
        .flat_map(|members| members.iter().copied())
        .collect()
}

fn snapshot_metrics(nodes: &[(u64, Arc<RaftNodeManager>)]) -> Vec<(u64, RaftMetrics)> {
    nodes
        .iter()
        .map(|(node_id, node)| (*node_id, node.raft().metrics().borrow_watched().clone()))
        .collect()
}

async fn start_node(
    node_id: u64,
    base_port: u16,
    all_peers: &[RaftPeer],
    seed_known_peers: bool,
) -> (u64, Arc<RaftNodeManager>) {
    let addr = format!("127.0.0.1:{}", base_port + node_id as u16).parse().unwrap();
    let peers = if seed_known_peers {
        all_peers.iter().filter(|p| p.id != node_id).cloned().collect()
    } else {
        Vec::new()
    };

    let config = ControllerConfig::default()
        .with_node_info(node_id, addr)
        .with_election_timeout_ms(1000)
        .with_heartbeat_interval_ms(300)
        .with_raft_peers(peers);

    let node = Arc::new(RaftNodeManager::new(ArcMut::new(config)).await.unwrap());

    if !seed_known_peers {
        node.raft().runtime_config().heartbeat(false);
        node.raft().runtime_config().elect(false);
        node.raft().runtime_config().tick(false);
    }

    let service = GrpcRaftService::new(node.raft());
    let server = Server::builder()
        .add_service(OpenRaftServiceServer::new(service))
        .serve(addr);

    tokio::spawn(async move {
        if let Err(e) = server.await {
            eprintln!("gRPC server error for node {}: {}", node_id, e);
        }
    });

    (node_id, node)
}

async fn commit_bootstrap_write(node: &RaftNodeManager) {
    node.client_write(ControllerRequest::ApplyBrokerId {
        cluster_name: "bootstrap-cluster".to_string(),
        broker_name: "bootstrap-broker".to_string(),
        broker_address: "127.0.0.1:0".to_string(),
        applied_broker_id: 0,
        register_check_code: "bootstrap-broker-check-code".to_string(),
    })
    .await
    .expect("commit bootstrap controller write");
}

async fn wait_for_stable_voters(nodes: &[(u64, Arc<RaftNodeManager>)], expected_voters: &BTreeSet<u64>) {
    for attempt in 1..=80 {
        let metrics = snapshot_metrics(nodes);
        let leader_ids = metrics
            .iter()
            .filter_map(|(node_id, metrics)| {
                (metrics.state == ServerState::Leader && metrics.current_leader == Some(*node_id)).then_some(*node_id)
            })
            .collect::<Vec<_>>();
        let leader_id = leader_ids.first().copied();
        let all_voters = metrics.iter().all(|(_, metrics)| metrics.state != ServerState::Learner);
        let all_memberships_match = metrics
            .iter()
            .all(|(_, metrics)| membership_voters(metrics) == *expected_voters);
        let all_follow_leader =
            leader_id.is_some() && metrics.iter().all(|(_, metrics)| metrics.current_leader == leader_id);
        let all_applied = metrics.iter().all(|(_, metrics)| metrics.last_applied.is_some());

        if leader_ids.len() == 1 && all_voters && all_memberships_match && all_follow_leader && all_applied {
            return;
        }

        if attempt % 10 == 0 {
            println!(
                "Waiting for stable voter membership {:?}, current snapshot: {:?}",
                expected_voters,
                metrics
                    .iter()
                    .map(|(node_id, metrics)| {
                        (
                            *node_id,
                            metrics.state,
                            metrics.current_leader,
                            membership_voters(metrics),
                        )
                    })
                    .collect::<Vec<_>>()
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("Cluster failed to converge to stable voter membership {expected_voters:?}");
}

async fn wait_for_learner_readiness(nodes: &[(u64, Arc<RaftNodeManager>)], learner_id: u64, leader_id: u64) {
    for _ in 1..=50 {
        let metrics = snapshot_metrics(nodes);
        if let Some((_, metrics)) = metrics.iter().find(|(node_id, _)| *node_id == learner_id) {
            if metrics.state == ServerState::Learner
                && metrics.current_leader == Some(leader_id)
                && metrics.last_applied.is_some()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("Learner node {learner_id} failed to become ready under leader {leader_id}");
}

/// Bootstrap a stable multi-voter cluster by delaying follower startup until the
/// initial leader has a committed write in its current term.
async fn bootstrap_cluster(node_count: u64, base_port: u16) -> Vec<(u64, Arc<RaftNodeManager>)> {
    let all_peers = cluster_peers(node_count, base_port);
    let mut nodes = vec![start_node(all_peers[0].id, base_port, &all_peers, true).await];
    tokio::time::sleep(Duration::from_secs(1)).await;

    let first_node_id = nodes[0].0;
    let first_node = nodes[0].1.clone();

    let mut single_node_cluster = BTreeMap::new();
    single_node_cluster.insert(first_node_id, raft_node(first_node_id, base_port));

    println!("Initializing node {} as single-node cluster", first_node_id);
    first_node.initialize_cluster(single_node_cluster).await.unwrap();

    // Wait for the first node to become leader and commit the bootstrap membership log.
    for attempt in 1..=50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if matches!(first_node.is_leader().await, Ok(true)) && first_node.has_committed_log() {
            println!(
                "Node {} became leader with committed bootstrap log after {} attempts",
                first_node_id, attempt
            );
            break;
        }
        if attempt == 50 {
            panic!("First node failed to become leader after initialization");
        }
    }

    commit_bootstrap_write(&first_node).await;

    for peer in all_peers.iter().skip(1) {
        nodes.push(start_node(peer.id, base_port, &all_peers, false).await);
        tokio::time::sleep(Duration::from_millis(200)).await;
        println!("Adding node {} as learner", peer.id);
        first_node
            .add_learner(peer.id, raft_node(peer.id, base_port), true)
            .await
            .unwrap();
        wait_for_learner_readiness(&nodes, peer.id, first_node_id).await;
    }

    let expected_voters = all_peers.iter().map(|peer| peer.id).collect::<BTreeSet<_>>();
    println!("Promoting cluster membership to voters: {:?}", expected_voters);
    first_node
        .change_membership(expected_voters.clone(), false)
        .await
        .unwrap();
    wait_for_stable_voters(&nodes, &expected_voters).await;

    for (_, node) in nodes.iter().skip(1) {
        node.raft().runtime_config().tick(true);
        node.raft().runtime_config().heartbeat(true);
        node.raft().runtime_config().elect(true);
    }

    println!("Multi-voter cluster initialized with membership: {:?}", expected_voters);
    nodes
}

#[tokio::test]
async fn test_three_node_cluster_formation() {
    const BASE_PORT: u16 = 50000;

    // Create 3 nodes
    let nodes = bootstrap_cluster(3, BASE_PORT).await;

    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check that we have exactly one leader
    let mut leader_count = 0;
    let mut leader_id = None;
    let mut learner_count = 0;

    for (node_id, node) in &nodes {
        let is_leader = node.is_leader().await.unwrap_or(false);
        let metrics = node.raft().metrics().borrow_watched().clone();
        if metrics.state == ServerState::Learner {
            learner_count += 1;
        }

        if is_leader {
            leader_count += 1;
            leader_id = Some(*node_id);
            println!("Node {} is leader", node_id);
        } else {
            println!("Node {} is {:?}", node_id, metrics.state);
        }
    }

    assert_eq!(
        leader_count, 1,
        "Should have exactly one leader, found {}",
        leader_count
    );
    assert!(leader_id.is_some(), "Should have a leader");
    assert_eq!(learner_count, 0, "All nodes should be promoted to voters");

    println!(" Cluster formed with leader: {:?}", leader_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_client_write() {
    const BASE_PORT: u16 = 51000;

    // Create and initialize 3-node cluster
    let nodes = bootstrap_cluster(3, BASE_PORT).await;

    // Wait longer for leader election and vote commit
    println!("Waiting for leader election and vote commit...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Find the leader and check cluster state
    let mut leader = None;
    let mut learner_count = 0;
    let mut voter_count = 0;

    for (node_id, node) in &nodes {
        let is_leader = node.is_leader().await.unwrap_or(false);
        let metrics = node.raft().metrics().borrow_watched().clone();

        if metrics.state == openraft::ServerState::Learner {
            learner_count += 1;
        } else {
            voter_count += 1;
        }

        println!(
            "Node {}: is_leader={}, state={:?}, term={}, current_leader={:?}",
            node_id, is_leader, metrics.state, metrics.current_term, metrics.current_leader
        );

        if is_leader {
            leader = Some((*node_id, node));
        }
    }
    let (leader_id, leader_node) = leader.expect("Should have a leader");

    println!(
        "Leader is node {} (voters: {}, learners: {})",
        leader_id, voter_count, learner_count
    );
    assert_eq!(learner_count, 0, "All controller nodes should be voters");
    assert_eq!(voter_count, 3, "All controller nodes should participate in quorum");

    // Write data through the leader
    let request = ControllerRequest::ApplyBrokerId {
        cluster_name: "test-cluster".to_string(),
        broker_name: "broker-a".to_string(),
        broker_address: "127.0.0.1:10911".to_string(),
        applied_broker_id: 1,
        register_check_code: "broker-a-check-code".to_string(),
    };

    println!(" All nodes are voters, sending write request to leader...");
    let write_result = tokio::time::timeout(Duration::from_secs(5), leader_node.client_write(request)).await;

    match write_result {
        Ok(Ok(_)) => {
            println!("✓ Write completed successfully");

            // Wait for replication
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Verify data is replicated (metrics check)
            let leader_metrics = leader_node.raft().metrics().borrow_watched().clone();
            assert!(
                leader_metrics.last_applied.is_some(),
                "Leader should apply the client write"
            );
            for (node_id, node) in &nodes {
                let metrics = node.raft().metrics().borrow_watched().clone();
                assert_eq!(
                    membership_voters(&metrics),
                    BTreeSet::from([1, 2, 3]),
                    "Node {} should report stable three-voter membership",
                    node_id
                );
                assert!(
                    metrics.last_applied.is_some(),
                    "Node {} should apply replicated data",
                    node_id
                );
            }
            println!(" Data replicated across all controller voters");
        }
        Ok(Err(e)) => {
            println!(" Write failed: {:?}", e);
            panic!("Write should succeed with all voters, but got error: {:?}", e);
        }
        Err(_) => {
            println!(" Write operation timed out after 10 seconds");
            for (node_id, node) in &nodes {
                use openraft::async_runtime::WatchReceiver;
                let metrics = node.raft().metrics().borrow_watched().clone();
                println!(
                    "  Node {}: state={:?}, term={}, leader={:?}",
                    node_id, metrics.state, metrics.current_term, metrics.current_leader
                );
            }
            panic!("Write operation timed out");
        }
    }

    println!("Test completed");
}

#[tokio::test]
async fn test_cluster_follower_redirect() {
    const BASE_PORT: u16 = 52000;

    // Create and initialize 3-node cluster
    let nodes = bootstrap_cluster(3, BASE_PORT).await;

    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Find a follower
    let mut follower = None;
    for (node_id, node) in &nodes {
        if !node.is_leader().await.unwrap_or(false) {
            follower = Some((*node_id, node));
            break;
        }
    }
    let (follower_id, follower_node) = follower.expect("Should have followers");

    println!("Testing write to follower (node {})", follower_id);

    // Try to write through a follower
    let request = ControllerRequest::ApplyBrokerId {
        cluster_name: "test-cluster".to_string(),
        broker_name: "broker-b".to_string(),
        broker_address: "127.0.0.1:10912".to_string(),
        applied_broker_id: 2,
        register_check_code: "broker-b-check-code".to_string(),
    };

    let result = follower_node.client_write(request).await;

    // In OpenRaft, writes to followers are forwarded to leader or rejected
    // The behavior depends on configuration
    println!("Write to follower result: {:?}", result);

    // We expect either success (forwarded) or specific error
    if result.is_ok() {
        println!("Write was forwarded to leader successfully");
    } else {
        println!("Write to follower rejected as expected");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_five_node_cluster() {
    const BASE_PORT: u16 = 53000;

    // Create 5 nodes for better fault tolerance
    let nodes = bootstrap_cluster(5, BASE_PORT).await;

    // Wait for leader election and vote commit - increased timeout for 5-node cluster
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify cluster formation
    let mut leader_count = 0;

    for (node_id, node) in &nodes {
        if node.is_leader().await.unwrap_or(false) {
            leader_count += 1;
            println!("Node {} is leader", node_id);
        }
    }

    assert_eq!(leader_count, 1, "Should have exactly one leader");

    // Test multiple writes
    let mut leader = None;
    for (node_id, node) in &nodes {
        if node.is_leader().await.unwrap_or(false) {
            leader = Some((*node_id, node));
            break;
        }
    }
    let (_leader_id, leader_node) = leader.unwrap();

    for i in 0..5 {
        let request = ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: format!("broker-{}", i),
            broker_address: format!("127.0.0.1:{}", 10911 + i),
            applied_broker_id: i + 1,
            register_check_code: format!("broker-{}-check-code", i),
        };

        let write_result = tokio::time::timeout(Duration::from_secs(10), leader_node.client_write(request)).await;

        match write_result {
            Ok(result) => {
                assert!(result.is_ok(), "Write {} should succeed: {:?}", i, result.err());
                println!("Write {} completed", i);
            }
            Err(_) => {
                panic!("Write {} timed out after 10 seconds", i);
            }
        }
    }

    println!("Completed write tests to 5-node cluster");

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    use openraft::async_runtime::WatchReceiver;
    let metrics = leader_node.raft().metrics().borrow_watched().clone();
    println!("Final metrics: last_applied={:?}", metrics.last_applied);
}

#[tokio::test]
async fn test_cluster_metrics() {
    const BASE_PORT: u16 = 54000;

    // Create 3-node cluster
    let nodes = bootstrap_cluster(3, BASE_PORT).await;

    // Wait for leader election and cluster stabilization
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Check metrics for all nodes
    for (node_id, node) in &nodes {
        use openraft::async_runtime::WatchReceiver;
        let metrics = node.raft().metrics().borrow_watched().clone();

        println!("Node {} metrics:", node_id);
        println!("  State: {:?}", metrics.state);
        println!("  Current term: {}", metrics.current_term);
        println!("  Current leader: {:?}", metrics.current_leader);
        println!("  Last log index: {:?}", metrics.last_log_index);
        println!("  Last applied: {:?}", metrics.last_applied);

        assert_ne!(
            metrics.state,
            ServerState::Learner,
            "Node {} should be promoted to a voter",
            node_id
        );
        if matches!(
            metrics.state,
            ServerState::Leader | ServerState::Follower | ServerState::Candidate
        ) {
            assert!(
                metrics.current_term > 0,
                "Node {} in state {:?} should have non-zero term",
                node_id,
                metrics.state
            );
        }
    }

    println!("All nodes have valid metrics");
}
