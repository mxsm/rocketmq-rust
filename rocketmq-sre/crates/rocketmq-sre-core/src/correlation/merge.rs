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

/// Deterministic incident candidate independent of storage ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationCandidate {
    pub incident_key: String,
    pub exact_key: bool,
    pub topology_distance: Option<u8>,
    pub last_occurred_at_epoch: i64,
    pub terminal: bool,
}

/// Chooses the best non-terminal candidate. Exact-key matches win, followed by
/// the nearest topology relation, most recent occurrence, then stable key.
#[must_use]
pub fn select_candidate(candidates: &[CorrelationCandidate]) -> Option<&CorrelationCandidate> {
    candidates
        .iter()
        .filter(|candidate| !candidate.terminal)
        .min_by(|left, right| {
            (!left.exact_key)
                .cmp(&(!right.exact_key))
                .then_with(|| {
                    left.topology_distance
                        .unwrap_or(u8::MAX)
                        .cmp(&right.topology_distance.unwrap_or(u8::MAX))
                })
                .then_with(|| right.last_occurred_at_epoch.cmp(&left.last_occurred_at_epoch))
                .then_with(|| left.incident_key.cmp(&right.incident_key))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_non_terminal_candidate_wins_independent_of_input_order() {
        let topology = CorrelationCandidate {
            incident_key: "a".into(),
            exact_key: false,
            topology_distance: Some(1),
            last_occurred_at_epoch: 20,
            terminal: false,
        };
        let exact = CorrelationCandidate {
            incident_key: "b".into(),
            exact_key: true,
            topology_distance: Some(0),
            last_occurred_at_epoch: 10,
            terminal: false,
        };
        let terminal = CorrelationCandidate {
            incident_key: "c".into(),
            exact_key: true,
            topology_distance: Some(0),
            last_occurred_at_epoch: 30,
            terminal: true,
        };

        assert_eq!(
            select_candidate(&[topology.clone(), terminal.clone(), exact.clone()]),
            Some(&exact)
        );
        assert_eq!(select_candidate(&[terminal]), None);
    }
}
