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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Fresh voter observations returned only for an explicit Controller rollout check.
/// Operators must recheck before each restart; this observation reserves no resources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RolloutQuorumStatus {
    pub schema_version: u32,
    pub target_node_id: u64,
    pub leader_id: u64,
    pub voters: Vec<u64>,
    /// Advertised Raft endpoints of current voters, used to identify the deployment.
    pub peer_endpoints: BTreeMap<u64, String>,
    pub remaining_ready_voters: Vec<u64>,
    pub allowed: bool,
}

impl RolloutQuorumStatus {
    /// Checks the response version, target, and remaining majority before an operator acts.
    pub fn permits(&self, target: u64) -> bool {
        let voters = self.voters.iter().copied().collect::<BTreeSet<_>>();
        let remaining = self.remaining_ready_voters.iter().copied().collect::<BTreeSet<_>>();
        self.schema_version == 1
            && self.allowed
            && target != 0
            && self.target_node_id == target
            && voters.len() >= 3
            && voters.len() == self.voters.len()
            && !voters.contains(&0)
            && voters.contains(&target)
            && voters.contains(&self.leader_id)
            && self.peer_endpoints.keys().copied().collect::<BTreeSet<_>>() == voters
            && self.peer_endpoints.values().all(|endpoint| !endpoint.is_empty())
            && remaining.len() == self.remaining_ready_voters.len()
            && !remaining.contains(&target)
            && remaining.is_subset(&voters)
            && remaining.len() > voters.len() / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollout_observation_rejects_unsupported_malformed_or_insufficient_majorities() {
        let good = RolloutQuorumStatus {
            schema_version: 1,
            target_node_id: 2,
            leader_id: 1,
            voters: vec![1, 2, 3],
            peer_endpoints: (1..=3).map(|id| (id, format!("node-{id}:9879"))).collect(),
            remaining_ready_voters: vec![1, 3],
            allowed: true,
        };
        assert!(good.permits(2));
        assert!(!good.permits(1));
        let mut bad = good.clone();
        bad.schema_version = 2;
        assert!(!bad.permits(2));
        bad = good.clone();
        bad.remaining_ready_voters = vec![1, 1];
        assert!(!bad.permits(2));
        bad = good.clone();
        bad.remaining_ready_voters = vec![1, 4];
        assert!(!bad.permits(2));
        bad = good.clone();
        bad.remaining_ready_voters = vec![1, 2];
        assert!(!bad.permits(2));
        bad = good.clone();
        bad.allowed = false;
        assert!(!bad.permits(2));
        bad = good.clone();
        bad.voters.push(3);
        assert!(!bad.permits(2));
        assert!(serde_json::from_str::<RolloutQuorumStatus>("{}").is_err());
        assert_eq!(
            serde_json::from_slice::<RolloutQuorumStatus>(&serde_json::to_vec(&good).unwrap()).unwrap(),
            good
        );
    }
}
