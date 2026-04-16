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
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use openraft::ServerState;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::config::StorageBackendType;
use rocketmq_controller::openraft::GrpcRaftService;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::ControllerResponseHeader;
use rocketmq_controller::typ::Node;
use rocketmq_controller::typ::RaftMetrics;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_rust::ArcMut;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
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

struct ManagedNode {
    node_id: u64,
    node: Arc<RaftNodeManager>,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: Option<JoinHandle<()>>,
}

impl ManagedNode {
    fn to_ref(&self) -> (u64, Arc<RaftNodeManager>) {
        (self.node_id, self.node.clone())
    }

    fn enable_runtime(&self) {
        self.node.raft().runtime_config().tick(true);
        self.node.raft().runtime_config().heartbeat(true);
        self.node.raft().runtime_config().elect(true);
    }

    fn disable_runtime(&self) {
        self.node.raft().runtime_config().tick(false);
        self.node.raft().runtime_config().heartbeat(false);
        self.node.raft().runtime_config().elect(false);
    }

    async fn shutdown(&mut self) {
        self.node.shutdown().await.expect("shutdown raft node");

        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.server_handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
    }
}

fn persistent_config(
    node_id: u64,
    base_port: u16,
    all_peers: &[RaftPeer],
    storage_root: &Path,
    seed_known_peers: bool,
) -> ControllerConfig {
    let peers = if seed_known_peers {
        all_peers.iter().filter(|peer| peer.id != node_id).cloned().collect()
    } else {
        Vec::new()
    };
    let storage_path = storage_root.join(format!("node-{node_id}"));

    ControllerConfig::default()
        .with_node_info(
            node_id,
            format!("127.0.0.1:{}", base_port + node_id as u16).parse().unwrap(),
        )
        .with_election_timeout_ms(1000)
        .with_heartbeat_interval_ms(300)
        .with_raft_peers(peers)
        .with_storage_backend(StorageBackendType::File)
        .with_storage_path(storage_path.to_string_lossy().into_owned())
}

fn managed_refs(nodes: &[ManagedNode]) -> Vec<(u64, Arc<RaftNodeManager>)> {
    nodes.iter().map(ManagedNode::to_ref).collect()
}

async fn start_managed_node(config: ControllerConfig, enable_runtime: bool) -> ManagedNode {
    let node_id = config.node_id;
    let addr = config.listen_addr;
    let node = Arc::new(RaftNodeManager::new(ArcMut::new(config)).await.unwrap());
    let service = GrpcRaftService::new(node.raft());
    let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(async move {
        let result = Server::builder()
            .add_service(OpenRaftServiceServer::new(service))
            .serve_with_shutdown(addr, async {
                let _ = server_shutdown_rx.await;
            })
            .await;

        if let Err(error) = result {
            eprintln!("gRPC server error for node {}: {}", node_id, error);
        }
    });

    let managed = ManagedNode {
        node_id,
        node,
        server_shutdown_tx: Some(server_shutdown_tx),
        server_handle: Some(server_handle),
    };

    if enable_runtime {
        managed.enable_runtime();
    } else {
        managed.disable_runtime();
    }

    managed
}

async fn bootstrap_persistent_cluster(node_count: u64, base_port: u16, storage_root: &Path) -> Vec<ManagedNode> {
    let all_peers = cluster_peers(node_count, base_port);
    let mut nodes = vec![
        start_managed_node(
            persistent_config(all_peers[0].id, base_port, &all_peers, storage_root, true),
            true,
        )
        .await,
    ];
    tokio::time::sleep(Duration::from_secs(1)).await;

    let first_node_id = nodes[0].node_id;
    let first_node = nodes[0].node.clone();

    let mut single_node_cluster = BTreeMap::new();
    single_node_cluster.insert(first_node_id, raft_node(first_node_id, base_port));

    println!("Initializing persistent node {} as single-node cluster", first_node_id);
    first_node.initialize_cluster(single_node_cluster).await.unwrap();

    for attempt in 1..=50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if matches!(first_node.is_leader().await, Ok(true)) && first_node.has_committed_log() {
            println!(
                "Persistent node {} became leader with committed bootstrap log after {} attempts",
                first_node_id, attempt
            );
            break;
        }
        if attempt == 50 {
            panic!("Persistent first node failed to become leader after initialization");
        }
    }

    commit_bootstrap_write(&first_node).await;

    for peer in all_peers.iter().skip(1) {
        nodes.push(
            start_managed_node(
                persistent_config(peer.id, base_port, &all_peers, storage_root, false),
                false,
            )
            .await,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        println!("Adding persistent node {} as learner", peer.id);
        first_node
            .add_learner(peer.id, raft_node(peer.id, base_port), true)
            .await
            .unwrap();
        let node_refs = managed_refs(&nodes);
        wait_for_learner_readiness(&node_refs, peer.id, first_node_id).await;
    }

    let expected_voters = all_peers.iter().map(|peer| peer.id).collect::<BTreeSet<_>>();
    println!(
        "Promoting persistent cluster membership to voters: {:?}",
        expected_voters
    );
    first_node
        .change_membership(expected_voters.clone(), false)
        .await
        .unwrap();
    let node_refs = managed_refs(&nodes);
    wait_for_stable_voters(&node_refs, &expected_voters).await;

    for node in nodes.iter().skip(1) {
        node.enable_runtime();
    }

    nodes
}

async fn seed_replica_group_state(
    node: &RaftNodeManager,
    broker_name: &str,
    master_address: &str,
    replica_address: &str,
) {
    let apply_master = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_address: master_address.to_string(),
            applied_broker_id: 1,
            register_check_code: format!("{broker_name}-master-check-code"),
        })
        .await
        .expect("apply master broker id");
    assert_eq!(apply_master.data.response_code, ResponseCode::Success as i32);

    let apply_replica = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_address: replica_address.to_string(),
            applied_broker_id: 2,
            register_check_code: format!("{broker_name}-replica-check-code"),
        })
        .await
        .expect("apply replica broker id");
    assert_eq!(apply_replica.data.response_code, ResponseCode::Success as i32);

    let alive_broker_ids = HashSet::from([1_u64, 2_u64]);
    let register_master = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_address: master_address.to_string(),
            broker_id: 1,
            alive_broker_ids: alive_broker_ids.clone(),
        })
        .await
        .expect("register master broker");
    assert_eq!(register_master.data.response_code, ResponseCode::Success as i32);

    let register_replica = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_address: replica_address.to_string(),
            broker_id: 2,
            alive_broker_ids: alive_broker_ids.clone(),
        })
        .await
        .expect("register replica broker");
    assert_eq!(register_replica.data.response_code, ResponseCode::Success as i32);

    let elect_master = node
        .client_write(ControllerRequest::ElectMaster {
            cluster_name: "test-cluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_id: Some(1),
            designate_elect: false,
            alive_broker_ids: alive_broker_ids.clone(),
            live_broker_infos: Default::default(),
        })
        .await
        .expect("elect master");
    assert_eq!(elect_master.data.response_code, ResponseCode::Success as i32);

    let elect_header = match elect_master.data.header {
        Some(ControllerResponseHeader::ElectMaster(header)) => header,
        _ => panic!("elect master should return elect-master response header"),
    };

    let alter_sync_state_set = node
        .client_write(ControllerRequest::AlterSyncStateSet {
            cluster_name: "test-cluster".to_string(),
            broker_name: broker_name.to_string(),
            master_broker_id: 1,
            master_epoch: elect_header.master_epoch.expect("master epoch"),
            new_sync_state_set: HashSet::from([1_u64, 2_u64]),
            sync_state_set_epoch: elect_header.sync_state_set_epoch.expect("sync state set epoch"),
            alive_broker_ids,
        })
        .await
        .expect("alter sync state set");
    assert_eq!(alter_sync_state_set.data.response_code, ResponseCode::Success as i32);
}

fn assert_replica_group_state(node: &RaftNodeManager, broker_name: &str, master_address: &str) {
    let replicas_info_manager = node.store().state_machine.replicas_info_manager();
    assert_eq!(
        replicas_info_manager.cluster_name(broker_name).as_deref(),
        Some("test-cluster")
    );
    assert_eq!(
        replicas_info_manager.broker_ids(broker_name),
        HashSet::from([1_u64, 2_u64])
    );

    let replica_info = replicas_info_manager.get_replica_info(broker_name);
    assert!(replica_info.is_success(), "replica group state should exist");
    let header = replica_info.response().expect("replica info response");
    assert_eq!(header.master_broker_id, Some(1));
    assert_eq!(header.master_address.as_deref(), Some(master_address));
    assert_eq!(header.master_epoch, Some(1));

    let sync_state_set = SyncStateSet::decode(replica_info.body().expect("sync state set body")).expect("decode body");
    assert_eq!(
        sync_state_set.get_sync_state_set().cloned().unwrap_or_default(),
        HashSet::from([1_i64, 2_i64])
    );
}

async fn wait_for_applied_broker_id(node: &RaftNodeManager, broker_name: &str, expected_next_id: u64) {
    for _ in 0..50 {
        let next_broker_id = node
            .store()
            .state_machine
            .replicas_info_manager()
            .get_next_broker_id("test-cluster", broker_name)
            .response()
            .and_then(|header| header.next_broker_id);

        if next_broker_id == Some(expected_next_id) {
            return;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "broker {} did not reach expected next broker id {}",
        broker_name, expected_next_id
    );
}

async fn find_leader_node(nodes: &[ManagedNode]) -> Arc<RaftNodeManager> {
    for node in nodes {
        if node.node.is_leader().await.unwrap_or(false) {
            return node.node.clone();
        }
    }

    panic!("cluster should have a leader");
}

async fn find_leader_index(nodes: &[ManagedNode]) -> usize {
    for (index, node) in nodes.iter().enumerate() {
        if node.node.is_leader().await.unwrap_or(false) {
            return index;
        }
    }

    panic!("cluster should have a leader");
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
    for attempt in 1..=100 {
        let metrics = snapshot_metrics(nodes);
        if let Some((_, metrics)) = metrics.iter().find(|(node_id, _)| *node_id == learner_id) {
            if metrics.state == ServerState::Learner
                && metrics.current_leader == Some(leader_id)
                && metrics.last_applied.is_some()
            {
                return;
            }
        }
        if attempt % 25 == 0 {
            println!(
                "Waiting for learner {} readiness (attempt {}), current state: {:?}",
                learner_id,
                attempt,
                metrics
                    .iter()
                    .find(|(node_id, _)| *node_id == learner_id)
                    .map(|(_, m)| &m.state)
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("Learner node {learner_id} failed to become ready under leader {leader_id}");
}

async fn wait_for_failover_leader(
    nodes: &[(u64, Arc<RaftNodeManager>)],
    excluded_node_id: u64,
) -> (u64, Arc<RaftNodeManager>) {
    for attempt in 1..=80 {
        let metrics = snapshot_metrics(nodes);
        let leaders = metrics
            .iter()
            .filter(|(node_id, _)| *node_id != excluded_node_id)
            .filter_map(|(node_id, metrics)| {
                (metrics.state == ServerState::Leader && metrics.current_leader == Some(*node_id)).then_some(*node_id)
            })
            .collect::<Vec<_>>();

        if leaders.len() == 1 {
            let leader_id = leaders[0];
            let all_survivors_follow = metrics
                .iter()
                .filter(|(node_id, _)| *node_id != excluded_node_id)
                .all(|(_, metrics)| metrics.current_leader == Some(leader_id));

            if all_survivors_follow {
                let leader_node = nodes
                    .iter()
                    .find(|(node_id, _)| *node_id == leader_id)
                    .map(|(_, node)| node.clone())
                    .expect("leader node should be present");
                return (leader_id, leader_node);
            }
        }

        if attempt % 10 == 0 {
            println!(
                "Waiting for failover leader excluding node {}, current snapshot: {:?}",
                excluded_node_id,
                metrics
                    .iter()
                    .map(|(node_id, metrics)| (*node_id, metrics.state, metrics.current_leader))
                    .collect::<Vec<_>>()
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("Cluster failed to elect a failover leader after excluding node {excluded_node_id}");
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

#[tokio::test(flavor = "multi_thread")]
async fn test_three_node_cluster_re_elects_after_leader_shutdown() {
    const BASE_PORT: u16 = 55000;

    let nodes = bootstrap_cluster(3, BASE_PORT).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut old_leader = None;
    for (node_id, node) in &nodes {
        if node.is_leader().await.unwrap_or(false) {
            old_leader = Some((*node_id, node.clone()));
            break;
        }
    }
    let (old_leader_id, old_leader_node) = old_leader.expect("cluster should elect an initial leader");

    let pre_failover_write = old_leader_node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-before-failover".to_string(),
            broker_address: "127.0.0.1:10921".to_string(),
            applied_broker_id: 1,
            register_check_code: "broker-before-failover-check-code".to_string(),
        })
        .await
        .expect("pre-failover write should succeed");
    assert_eq!(
        pre_failover_write.data.response_code,
        rocketmq_remoting::code::response_code::ResponseCode::Success as i32,
        "pre-failover controller write should succeed"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    old_leader_node.shutdown().await.expect("shutdown old leader");

    let (new_leader_id, new_leader_node) = wait_for_failover_leader(&nodes, old_leader_id).await;
    assert_ne!(
        new_leader_id, old_leader_id,
        "cluster should elect a different leader after shutdown"
    );

    let post_failover_write = new_leader_node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-after-failover".to_string(),
            broker_address: "127.0.0.1:10922".to_string(),
            applied_broker_id: 1,
            register_check_code: "broker-after-failover-check-code".to_string(),
        })
        .await
        .expect("post-failover write should succeed");
    assert_eq!(
        post_failover_write.data.response_code,
        rocketmq_remoting::code::response_code::ResponseCode::Success as i32,
        "post-failover controller write should succeed"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    for (node_id, node) in nodes.iter().filter(|(node_id, _)| *node_id != old_leader_id) {
        let metrics = node.raft().metrics().borrow_watched().clone();
        assert_eq!(
            metrics.current_leader,
            Some(new_leader_id),
            "surviving node {} should follow the new leader",
            node_id
        );

        let replicas_info_manager = node.store().state_machine.replicas_info_manager();
        let next_before = replicas_info_manager
            .get_next_broker_id("test-cluster", "broker-before-failover")
            .response()
            .and_then(|header| header.next_broker_id)
            .expect("pre-failover broker id should remain visible");
        let next_after = replicas_info_manager
            .get_next_broker_id("test-cluster", "broker-after-failover")
            .response()
            .and_then(|header| header.next_broker_id)
            .expect("post-failover broker id should replicate");

        assert_eq!(next_before, 2, "node {} should retain pre-failover state", node_id);
        assert_eq!(next_after, 2, "node {} should apply post-failover state", node_id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_three_node_cluster_persistent_restart_recovers_controller_state() {
    const BASE_PORT: u16 = 58000;

    let storage_root = tempfile::tempdir().expect("create persistent multi-node storage root");
    let mut nodes = bootstrap_persistent_cluster(3, BASE_PORT, storage_root.path()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_node = find_leader_node(&nodes).await;

    seed_replica_group_state(
        &leader_node,
        "persistent-broker-set",
        "127.0.0.1:10931",
        "127.0.0.1:10932",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    for node in &nodes {
        assert_replica_group_state(&node.node, "persistent-broker-set", "127.0.0.1:10931");
    }

    for node in &mut nodes {
        node.shutdown().await;
    }

    let all_peers = cluster_peers(3, BASE_PORT);
    let mut restarted_nodes = Vec::new();
    for peer in &all_peers {
        restarted_nodes.push(
            start_managed_node(
                persistent_config(peer.id, BASE_PORT, &all_peers, storage_root.path(), true),
                true,
            )
            .await,
        );
    }

    let expected_voters = BTreeSet::from([1_u64, 2_u64, 3_u64]);
    let restarted_refs = managed_refs(&restarted_nodes);
    wait_for_stable_voters(&restarted_refs, &expected_voters).await;

    for node in &restarted_nodes {
        assert_replica_group_state(&node.node, "persistent-broker-set", "127.0.0.1:10931");
    }

    let restarted_leader = find_leader_node(&restarted_nodes).await;
    let post_restart_write = restarted_leader
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "persistent-post-restart".to_string(),
            broker_address: "127.0.0.1:10933".to_string(),
            applied_broker_id: 1,
            register_check_code: "persistent-post-restart-check-code".to_string(),
        })
        .await
        .expect("post-restart write should succeed");
    assert_eq!(post_restart_write.data.response_code, ResponseCode::Success as i32);

    for node in &restarted_nodes {
        wait_for_applied_broker_id(&node.node, "persistent-post-restart", 2).await;
    }

    for node in &mut restarted_nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persistent_follower_restart_rejoins_and_catches_up() {
    const BASE_PORT: u16 = 57000;

    let storage_root = tempfile::tempdir().expect("create node rejoin storage root");
    let all_peers = cluster_peers(3, BASE_PORT);
    let mut nodes = bootstrap_persistent_cluster(3, BASE_PORT, storage_root.path()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_index = find_leader_index(&nodes).await;
    let leader_node = nodes[leader_index].node.clone();
    seed_replica_group_state(
        &leader_node,
        "rejoin-seeded-broker",
        "127.0.0.1:10941",
        "127.0.0.1:10942",
    )
    .await;

    let follower_index = nodes
        .iter()
        .position(|node| node.node_id != nodes[leader_index].node_id)
        .expect("persistent cluster should have a follower");
    let follower_id = nodes[follower_index].node_id;
    nodes[follower_index].shutdown().await;

    let gap_write = leader_node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "rejoin-gap-broker".to_string(),
            broker_address: "127.0.0.1:10943".to_string(),
            applied_broker_id: 1,
            register_check_code: "rejoin-gap-broker-check-code".to_string(),
        })
        .await
        .expect("leader should accept writes while follower is down");
    assert_eq!(gap_write.data.response_code, ResponseCode::Success as i32);

    let restarted_follower = start_managed_node(
        persistent_config(follower_id, BASE_PORT, &all_peers, storage_root.path(), true),
        true,
    )
    .await;
    nodes[follower_index] = restarted_follower;

    let node_refs = managed_refs(&nodes);
    let expected_voters = BTreeSet::from([1_u64, 2_u64, 3_u64]);
    wait_for_stable_voters(&node_refs, &expected_voters).await;

    assert_replica_group_state(&nodes[follower_index].node, "rejoin-seeded-broker", "127.0.0.1:10941");
    wait_for_applied_broker_id(&nodes[follower_index].node, "rejoin-gap-broker", 2).await;

    for node in &mut nodes {
        node.shutdown().await;
    }
}
