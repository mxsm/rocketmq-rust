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

//! OpenRaft adapter for the project-owned membership port.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_error::Result;

use crate::error::consensus_failed;
use crate::openraft::RaftNodeManager;
use crate::typ::Node;

use super::ConsensusMembership;
use super::ConsensusMembershipPort;
use super::ConsensusNode;

impl ConsensusMembershipPort for RaftNodeManager {
    async fn current_membership(&self) -> Result<ConsensusMembership> {
        use openraft::async_runtime::WatchReceiver;

        let metrics = self.raft().metrics().borrow_watched().clone();
        let stored = metrics.membership_config;
        let voters = stored.voter_ids().collect::<BTreeSet<_>>();
        let learners = stored.membership().learner_ids().collect::<BTreeSet<_>>();
        let nodes = stored
            .nodes()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    ConsensusNode {
                        node_id: *node_id,
                        rpc_addr: node.rpc_addr.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut caught_up = BTreeSet::new();
        if metrics.current_leader == Some(metrics.id) {
            caught_up.insert(metrics.id);
        }
        if let Some(replication) = metrics.replication {
            for (node_id, matched) in replication {
                let reached_commit = match (metrics.committed, matched) {
                    (Some(committed), Some(matched)) => matched >= committed,
                    (None, Some(_)) => true,
                    _ => false,
                };
                if reached_commit {
                    caught_up.insert(node_id);
                }
            }
        }
        Ok(ConsensusMembership::new(
            stored.log_id().map_or(0, |log_id| log_id.index),
            metrics.current_leader,
            voters,
            learners,
            nodes,
            caught_up,
        ))
    }

    async fn add_caught_up_learner(&self, node: &ConsensusNode) -> Result<()> {
        RaftNodeManager::add_learner(
            self,
            node.node_id,
            Node {
                node_id: node.node_id,
                rpc_addr: node.rpc_addr.clone(),
            },
            true,
        )
        .await
    }

    async fn change_voters(&self, voters: BTreeSet<u64>) -> Result<()> {
        RaftNodeManager::change_membership(self, voters, false).await
    }

    async fn remove_learner(&self, node_id: u64) -> Result<()> {
        self.raft()
            .change_membership(
                openraft::ChangeMembers::<u64, Node>::RemoveNodes(BTreeSet::from([node_id])),
                false,
            )
            .await
            .map_err(|error| consensus_failed("remove Raft learner", error))?;
        Ok(())
    }
}
