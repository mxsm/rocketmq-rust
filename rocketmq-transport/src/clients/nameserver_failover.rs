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

use std::collections::HashSet;

use cheetah_string::CheetahString;

use crate::clients::nameserver_selector::LatencyTracker;

pub(crate) const MAX_NAMESERVER_CONNECT_ATTEMPTS: usize = 3;

pub(crate) fn build_nameserver_failover_candidates<F>(
    configured: &[CheetahString],
    available: &HashSet<CheetahString>,
    latency_tracker: &LatencyTracker,
    mut admitted_by_circuit: F,
) -> Vec<CheetahString>
where
    F: FnMut(&CheetahString) -> bool,
{
    let mut eligible = configured
        .iter()
        .filter(|address| available.is_empty() || available.contains(*address))
        .filter(|address| admitted_by_circuit(address))
        .cloned()
        .collect::<Vec<_>>();
    let mut ordered = Vec::with_capacity(eligible.len().min(MAX_NAMESERVER_CONNECT_ATTEMPTS));

    while ordered.len() < MAX_NAMESERVER_CONNECT_ATTEMPTS {
        let Some(selected) = latency_tracker.select_best(&eligible).cloned() else {
            break;
        };
        let Some(index) = eligible.iter().position(|address| address == &selected) else {
            break;
        };
        ordered.push(eligible.remove(index));
    }

    ordered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_unhealthy_first_candidate_and_keeps_cold_backup() {
        let unhealthy = CheetahString::from_static_str("ns-a:9876");
        let cold = CheetahString::from_static_str("ns-b:9876");
        let tracker = LatencyTracker::new();
        tracker.record_error(&unhealthy);
        tracker.record_error(&unhealthy);
        tracker.record_error(&unhealthy);

        let candidates =
            build_nameserver_failover_candidates(&[unhealthy.clone(), cold.clone()], &HashSet::new(), &tracker, |_| {
                true
            });

        assert_eq!(candidates, vec![cold, unhealthy]);
    }

    #[test]
    fn applies_availability_and_circuit_admission_before_ranking() {
        let first = CheetahString::from_static_str("ns-a:9876");
        let admitted = CheetahString::from_static_str("ns-b:9876");
        let unavailable = CheetahString::from_static_str("ns-c:9876");
        let available = HashSet::from([first.clone(), admitted.clone()]);
        let tracker = LatencyTracker::new();

        let candidates = build_nameserver_failover_candidates(
            &[first.clone(), admitted.clone(), unavailable],
            &available,
            &tracker,
            |address| address != &first,
        );

        assert_eq!(candidates, vec![admitted]);
    }

    #[test]
    fn caps_connection_attempts_without_dropping_health_ordering() {
        let configured = (0..8)
            .map(|index| CheetahString::from_string(format!("ns-{index}:9876")))
            .collect::<Vec<_>>();
        let tracker = LatencyTracker::new();

        let candidates = build_nameserver_failover_candidates(&configured, &HashSet::new(), &tracker, |_| true);

        assert_eq!(candidates.len(), MAX_NAMESERVER_CONNECT_ATTEMPTS);
        assert_eq!(candidates, configured[..MAX_NAMESERVER_CONNECT_ATTEMPTS]);
    }
}
