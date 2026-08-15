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

use std::time::Duration;
use std::time::Instant;

use rocketmq_protocol::protocol::body::controller_write_lease::ControllerWriteLeaseGrant;

const LEADER_AND_HANDOVER_WAIT: Duration = Duration::from_millis(
    ControllerWriteLeaseGrant::DEFAULT_LEASE_DURATION_MILLIS + ControllerWriteLeaseGrant::DEFAULT_SAFETY_MARGIN_MILLIS,
);

/// Leader-local monotonic gate for the first lease of a term or authority.
#[derive(Debug, Default)]
pub(crate) struct WriteLeaseGrantGate {
    leader_term: Option<u64>,
    leader_since: Option<Instant>,
    authority: Option<(u64, i32)>,
    authority_since: Option<Instant>,
}

impl WriteLeaseGrantGate {
    pub(crate) fn allow(&mut self, leader_term: u64, committed_authority: Option<(u64, i32)>, now: Instant) -> bool {
        if self.leader_term != Some(leader_term) {
            self.leader_term = Some(leader_term);
            self.leader_since = Some(now);
            self.authority = committed_authority;
            self.authority_since = Some(now);
            return false;
        }

        if self.authority != committed_authority {
            self.authority = committed_authority;
            self.authority_since = Some(now);
            return false;
        }

        committed_authority.is_some()
            && self
                .leader_since
                .is_some_and(|since| now.duration_since(since) >= LEADER_AND_HANDOVER_WAIT)
            && self
                .authority_since
                .is_some_and(|since| now.duration_since(since) >= LEADER_AND_HANDOVER_WAIT)
    }

    pub(crate) fn observe_not_leader(&mut self) {
        self.leader_term = None;
        self.leader_since = None;
        self.authority = None;
        self.authority_since = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_and_authority_changes_each_restart_the_monotonic_wait() {
        let start = Instant::now();
        let mut gate = WriteLeaseGrantGate::default();
        assert!(!gate.allow(4, Some((0, 1)), start));
        assert!(!gate.allow(
            4,
            Some((0, 1)),
            start + LEADER_AND_HANDOVER_WAIT - Duration::from_millis(1)
        ));
        assert!(gate.allow(4, Some((0, 1)), start + LEADER_AND_HANDOVER_WAIT));

        let handover = start + LEADER_AND_HANDOVER_WAIT;
        assert!(!gate.allow(4, Some((1, 2)), handover));
        assert!(gate.allow(4, Some((1, 2)), handover + LEADER_AND_HANDOVER_WAIT));

        let next_term = handover + LEADER_AND_HANDOVER_WAIT;
        assert!(!gate.allow(5, Some((1, 2)), next_term));
        assert!(gate.allow(5, Some((1, 2)), next_term + LEADER_AND_HANDOVER_WAIT));
    }

    #[test]
    fn losing_leadership_invalidates_the_local_wait() {
        let start = Instant::now();
        let mut gate = WriteLeaseGrantGate::default();
        assert!(!gate.allow(8, Some((0, 3)), start));
        gate.observe_not_leader();
        assert!(!gate.allow(8, Some((0, 3)), start + LEADER_AND_HANDOVER_WAIT));
    }
}
