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

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use rocketmq_protocol::protocol::body::controller_write_lease::ControllerWriteLeaseGrant;

const LEADER_AND_HANDOVER_WAIT: Duration = Duration::from_millis(
    ControllerWriteLeaseGrant::DEFAULT_LEASE_DURATION_MILLIS + ControllerWriteLeaseGrant::DEFAULT_SAFETY_MARGIN_MILLIS,
);

/// Leader-local monotonic gate for the first lease of a term, and of an authority in each Broker
/// group. Groups hand over independently, so each one waits only for a change of its own master.
#[derive(Debug, Default)]
pub(crate) struct WriteLeaseGrantGate {
    leader_term: Option<u64>,
    leader_since: Option<Instant>,
    /// Per Broker group, the committed authority last seen and since when it holds.
    authorities: HashMap<String, (Option<(u64, i32)>, Instant)>,
}

impl WriteLeaseGrantGate {
    /// Whether the master of Broker group `broker_name` in `cluster_name`, whose committed
    /// authority is `committed_authority`, may get a lease from the leader of `leader_term`.
    pub(crate) fn allow(
        &mut self,
        leader_term: u64,
        cluster_name: &str,
        broker_name: &str,
        committed_authority: Option<(u64, i32)>,
        now: Instant,
    ) -> bool {
        if self.leader_term != Some(leader_term) {
            self.leader_term = Some(leader_term);
            self.leader_since = Some(now);
            self.authorities.clear();
        }

        let (authority, authority_since) = self
            .authorities
            .entry(format!("{cluster_name}\0{broker_name}"))
            .or_insert((committed_authority, now));
        if *authority != committed_authority {
            *authority = committed_authority;
            *authority_since = now;
            return false;
        }

        committed_authority.is_some()
            && now.duration_since(*authority_since) >= LEADER_AND_HANDOVER_WAIT
            && self
                .leader_since
                .is_some_and(|since| now.duration_since(since) >= LEADER_AND_HANDOVER_WAIT)
    }

    pub(crate) fn observe_not_leader(&mut self) {
        self.leader_term = None;
        self.leader_since = None;
        self.authorities.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_and_authority_changes_each_restart_the_monotonic_wait() {
        let start = Instant::now();
        let mut gate = WriteLeaseGrantGate::default();
        assert!(!gate.allow(4, "c", "a", Some((0, 1)), start));
        assert!(!gate.allow(
            4,
            "c",
            "a",
            Some((0, 1)),
            start + LEADER_AND_HANDOVER_WAIT - Duration::from_millis(1)
        ));
        assert!(gate.allow(4, "c", "a", Some((0, 1)), start + LEADER_AND_HANDOVER_WAIT));

        let handover = start + LEADER_AND_HANDOVER_WAIT;
        assert!(!gate.allow(4, "c", "a", Some((1, 2)), handover));
        assert!(gate.allow(4, "c", "a", Some((1, 2)), handover + LEADER_AND_HANDOVER_WAIT));

        let next_term = handover + LEADER_AND_HANDOVER_WAIT;
        assert!(!gate.allow(5, "c", "a", Some((1, 2)), next_term));
        assert!(gate.allow(5, "c", "a", Some((1, 2)), next_term + LEADER_AND_HANDOVER_WAIT));
    }

    #[test]
    fn broker_groups_with_different_masters_each_get_their_lease() {
        let start = Instant::now();
        let mut gate = WriteLeaseGrantGate::default();
        // Their heartbeats alternate; neither group's master changes.
        let mut now = start;
        while now < start + LEADER_AND_HANDOVER_WAIT {
            gate.allow(4, "c", "a", Some((1, 1)), now);
            gate.allow(4, "c", "b", Some((2, 2)), now);
            now += Duration::from_millis(500);
        }

        assert!(gate.allow(4, "c", "a", Some((1, 1)), now));
        assert!(gate.allow(4, "c", "b", Some((2, 2)), now));

        // A handover in one group holds back only that group.
        assert!(!gate.allow(4, "c", "b", Some((1, 3)), now));
        assert!(gate.allow(4, "c", "a", Some((1, 1)), now));
    }

    #[test]
    fn losing_leadership_invalidates_the_local_wait() {
        let start = Instant::now();
        let mut gate = WriteLeaseGrantGate::default();
        assert!(!gate.allow(8, "c", "a", Some((0, 3)), start));
        gate.observe_not_leader();
        assert!(!gate.allow(8, "c", "a", Some((0, 3)), start + LEADER_AND_HANDOVER_WAIT));
    }
}
