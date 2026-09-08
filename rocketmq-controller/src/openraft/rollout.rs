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

use std::collections::BTreeSet;
use std::time::Duration;

use openraft::Instant;
use openraft::TokioInstant;
pub use rocketmq_protocol::protocol::body::controller_rollout::RolloutQuorumStatus;

use crate::error::{consensus_failed, consensus_timed_out, request_invalid};
use crate::typ::{LogId, RaftMetrics};
use crate::ControllerResult;

use super::RaftNodeManager;

const CHECK_TIMEOUT: Duration = Duration::from_secs(3);

impl RaftNodeManager {
    /// Checks actual replication acknowledgements after a ReadIndex barrier.
    ///
    /// # Errors
    ///
    /// Rejects followers, unknown targets, joint membership, changed leadership or
    /// membership, and a missing quorum. The entire check has one three-second deadline.
    pub async fn check_rollout_quorum(&self, target_node_id: u64) -> ControllerResult<RolloutQuorumStatus> {
        tokio::time::timeout(CHECK_TIMEOUT, self.check_rollout_quorum_inner(target_node_id))
            .await
            .map_err(|_| consensus_timed_out("check Controller rollout quorum", 3_000))?
    }

    async fn check_rollout_quorum_inner(&self, target: u64) -> ControllerResult<RolloutQuorumStatus> {
        let initial = self.raft_metrics();
        let voters = stable_voters(&initial, target)?;
        let membership = initial.membership_config.clone();
        let term = initial.current_term;
        let barrier = self.ensure_linearizable_read().await?;
        if barrier.is_none() {
            return Err(request_invalid("rollout requires committed Controller state"));
        }
        // Metrics from before this request cannot establish that a peer is still reachable.
        let started = TokioInstant::now();
        let raft = self.raft();
        raft.trigger()
            .heartbeat()
            .await
            .map_err(|error| consensus_failed("probe Controller rollout peers", error))?;
        let observed = raft
            .wait(Some(CHECK_TIMEOUT))
            .metrics(
                |metrics| {
                    metrics.current_leader != Some(initial.id)
                        || metrics.current_term != term
                        || metrics.membership_config != membership
                        || ready_voters(metrics, target, &voters, barrier, started).len() > voters.len() / 2
                },
                "remaining Controller voters acknowledge rollout probe",
            )
            .await
            .map_err(|error| consensus_failed("wait for Controller rollout quorum", error))?;
        if observed.current_term != term || observed.membership_config != membership {
            return Err(request_invalid(
                "Controller membership or leadership changed during rollout check",
            ));
        }
        stable_voters(&observed, target)?;
        let remaining = ready_voters(&observed, target, &voters, barrier, started);
        Ok(RolloutQuorumStatus {
            schema_version: 1,
            target_node_id: target,
            leader_id: initial.id,
            allowed: remaining.len() > voters.len() / 2,
            peer_endpoints: observed
                .membership_config
                .nodes()
                .filter(|(id, _)| voters.contains(id))
                .map(|(id, node)| (*id, node.rpc_addr.clone()))
                .collect(),
            voters: voters.into_iter().collect(),
            remaining_ready_voters: remaining,
        })
    }
}

fn stable_voters(metrics: &RaftMetrics, target: u64) -> ControllerResult<BTreeSet<u64>> {
    if metrics.running_state.is_err() || metrics.current_leader != Some(metrics.id) {
        return Err(request_invalid("rollout check requires the current Controller leader"));
    }
    let membership = metrics.membership_config.membership();
    let voters = membership.voter_ids().collect::<BTreeSet<_>>();
    if membership.get_joint_config().len() != 1 || !voters.contains(&target) || voters.len() < 3 {
        return Err(request_invalid(
            "rollout requires a target in a stable Controller quorum",
        ));
    }
    Ok(voters)
}

fn ready_voters(
    metrics: &RaftMetrics,
    target: u64,
    voters: &BTreeSet<u64>,
    barrier: Option<LogId>,
    started: TokioInstant,
) -> Vec<u64> {
    voters
        .iter()
        .copied()
        .filter(|id| {
            if *id == target {
                return false;
            }
            if *id == metrics.id {
                return metrics.last_applied >= barrier;
            }
            let fresh = metrics
                .heartbeat
                .as_ref()
                .and_then(|heartbeats| heartbeats.get(id))
                .and_then(|acked| *acked)
                .is_some_and(|acked| acked.into_inner() >= started);
            let caught_up = metrics
                .replication
                .as_ref()
                .and_then(|replication| replication.get(id))
                .is_some_and(|matched| matched.is_some() && *matched >= barrier);
            fresh && caught_up
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::protobuf::openraft::open_raft_service_server::OpenRaftServiceServer;
    use crate::{ControllerConfig, ControllerConfigReader, Node, StorageBackendType};

    #[tokio::test]
    async fn rollout_requires_fresh_quorum_without_the_target_voter() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("controller-rollout-test");
        let scope = runtime.service_context("raft-nodes");
        let mut nodes = Vec::new();
        let mut groups = Vec::new();
        let mut membership = BTreeMap::new();
        for id in 1..=3 {
            let listener = tokio::net::TcpListener::bind("localhost:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let config = ControllerConfig::default()
                .with_node_info(id, addr)
                .with_storage_backend(StorageBackendType::Memory)
                .with_heartbeat_interval_ms(100)
                .with_election_timeout_ms(1_000);
            let node = RaftNodeManager::new(ControllerConfigReader::new(config), scope.storage_io().clone())
                .await
                .unwrap();
            if id != 1 {
                node.set_runtime_elect_enabled(false);
            }
            let group = scope
                .component(["rpc-1", "rpc-2", "rpc-3"][(id - 1) as usize])
                .task_group()
                .clone();
            let cancellation = group.cancellation_token();
            let service = node.grpc_service();
            group
                .spawn_service("raft-rpc", async move {
                    Server::builder()
                        .add_service(OpenRaftServiceServer::new(service))
                        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), cancellation.cancelled_owned())
                        .await
                        .unwrap();
                })
                .unwrap();
            membership.insert(
                id,
                Node {
                    node_id: id,
                    rpc_addr: format!("localhost:{}", addr.port()),
                },
            );
            nodes.push(node);
            groups.push(group);
        }
        nodes[0].initialize_cluster(membership).await.unwrap();
        nodes[0]
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "bootstrap leader")
            .await
            .unwrap();

        let before = nodes[0].check_rollout_quorum(2).await.unwrap();
        assert!(before.allowed);
        assert_eq!(before.remaining_ready_voters, vec![1, 3]);
        assert!(nodes[0].check_rollout_quorum(1).await.unwrap().allowed);
        assert!(
            nodes[1].check_rollout_quorum(3).await.is_err(),
            "followers cannot grant a rollout"
        );
        assert!(
            nodes[0].check_rollout_quorum(4).await.is_err(),
            "target must be a voter"
        );

        assert!(groups[2].shutdown(Duration::from_secs(5)).await.is_healthy());
        nodes[2].shutdown().await.unwrap();
        // The failed peer's old matched offset remains in leader metrics. Its stale
        // acknowledgement must not permit restarting the other healthy follower.
        assert!(nodes[0].check_rollout_quorum(2).await.is_err());
        assert!(nodes[0].check_rollout_quorum(1).await.is_err());
        assert!(
            nodes[0].check_rollout_quorum(3).await.unwrap().allowed,
            "replacing the failed voter retains the two healthy voters"
        );

        for index in 0..2 {
            assert!(groups[index].shutdown(Duration::from_secs(5)).await.is_healthy());
            nodes[index].shutdown().await.unwrap();
        }
        assert_eq!(scope.task_group().task_count(), 0);
    }
}
