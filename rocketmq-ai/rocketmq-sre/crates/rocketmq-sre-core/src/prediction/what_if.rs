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

use rocketmq_sre_contracts::SimulationKind;
use rocketmq_sre_contracts::SimulationStatus;
use rocketmq_sre_contracts::WhatIfSimulationRequest;

/// Deterministic result before transport-specific JSON projection.
#[derive(Clone, Debug, PartialEq)]
pub struct SimulationProjection {
    pub status: SimulationStatus,
    pub projected_utilization: Option<f64>,
    pub assumptions: Vec<String>,
    pub bottlenecks: Vec<String>,
    pub blast_radius: Vec<String>,
    pub missing_assumptions: Vec<String>,
}

/// Runs a bounded what-if scenario. The function has no execution dependency
/// and cannot create an action or mutate a cluster.
#[must_use]
pub fn simulate(request: &WhatIfSimulationRequest) -> SimulationProjection {
    let mut projection = SimulationProjection {
        status: SimulationStatus::Completed,
        projected_utilization: None,
        assumptions: Vec::new(),
        bottlenecks: Vec::new(),
        blast_radius: request.affected_resource_keys.iter().take(128).cloned().collect(),
        missing_assumptions: Vec::new(),
    };
    let utilization = request
        .current_utilization
        .filter(|value| value.is_finite() && (0.0..=2.0).contains(value));
    if utilization.is_none() {
        projection.missing_assumptions.push("current_utilization".to_owned());
    }
    if projection.blast_radius.is_empty() {
        projection.missing_assumptions.push("dependency_graph".to_owned());
    }
    projection.projected_utilization = match request.kind {
        SimulationKind::BrokerOffline | SimulationKind::ProxyOffline => {
            let instances = positive(request.current_instances, "current_instances", &mut projection);
            match (utilization, instances) {
                (Some(value), Some(instances)) if instances > 1 => {
                    projection.assumptions.push("load_is_evenly_distributed".to_owned());
                    Some(value * f64::from(instances) / f64::from(instances - 1))
                }
                (_, Some(_)) => {
                    projection.missing_assumptions.push("at_least_two_instances".to_owned());
                    None
                }
                _ => None,
            }
        }
        SimulationKind::TrafficIncrease => {
            let percent = request
                .traffic_increase_percent
                .filter(|value| [25, 50, 100].contains(value));
            if percent.is_none() {
                projection
                    .missing_assumptions
                    .push("traffic_increase_percent_25_50_or_100".to_owned());
            }
            match (utilization, percent) {
                (Some(value), Some(percent)) => {
                    projection.assumptions.push("capacity_remains_constant".to_owned());
                    Some(value * (1.0 + f64::from(percent) / 100.0))
                }
                _ => None,
            }
        }
        SimulationKind::BrokerScaleOut | SimulationKind::ProxyScaleOut => {
            let instances = positive(request.current_instances, "current_instances", &mut projection);
            let delta = positive(request.instance_delta, "positive_instance_delta", &mut projection);
            match (utilization, instances, delta) {
                (Some(value), Some(instances), Some(delta)) => {
                    projection
                        .assumptions
                        .push("new_instances_share_load_evenly".to_owned());
                    Some(value * f64::from(instances) / f64::from(instances.saturating_add(delta)))
                }
                _ => None,
            }
        }
        SimulationKind::TopicQueueExpand => {
            let queues = positive(request.current_queue_count, "current_queue_count", &mut projection);
            let delta = positive(request.queue_delta, "positive_queue_delta", &mut projection);
            match (utilization, queues, delta) {
                (Some(value), Some(queues), Some(delta)) => {
                    projection.assumptions.push("queue_expansion_is_one_way".to_owned());
                    projection.assumptions.push("producers_rebalance_evenly".to_owned());
                    Some(value * f64::from(queues) / f64::from(queues.saturating_add(delta)))
                }
                _ => None,
            }
        }
        SimulationKind::VersionUpgrade => {
            if request
                .target_version
                .as_deref()
                .map(str::trim)
                .is_none_or(str::is_empty)
            {
                projection.missing_assumptions.push("target_version".to_owned());
            }
            projection
                .assumptions
                .push("version_change_does_not_improve_capacity_without_evidence".to_owned());
            utilization
        }
        SimulationKind::ConfigurationDiff => {
            if request.configuration_changes.is_empty() || request.configuration_changes.len() > 64 {
                projection
                    .missing_assumptions
                    .push("bounded_configuration_changes".to_owned());
            }
            projection
                .assumptions
                .push("unknown_configuration_effect_is_not_invented".to_owned());
            utilization
        }
    };
    if projection.projected_utilization.is_some_and(|value| value >= 1.0) {
        projection.bottlenecks.push("projected_capacity_exhaustion".to_owned());
    } else if projection.projected_utilization.is_some_and(|value| value >= 0.8) {
        projection.bottlenecks.push("projected_high_utilization".to_owned());
    }
    projection.status = if projection.projected_utilization.is_none()
        || projection
            .missing_assumptions
            .iter()
            .any(|value| value != "dependency_graph")
    {
        SimulationStatus::InsufficientData
    } else {
        SimulationStatus::Completed
    };
    projection
}

fn positive(value: Option<u32>, name: &str, projection: &mut SimulationProjection) -> Option<u32> {
    let value = value.filter(|value| *value > 0);
    if value.is_none() {
        projection.missing_assumptions.push(name.to_owned());
    }
    value
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::ClusterId;

    use super::*;

    fn request(kind: SimulationKind) -> WhatIfSimulationRequest {
        WhatIfSimulationRequest {
            cluster_id: ClusterId::new(),
            kind,
            current_utilization: Some(0.6),
            current_instances: Some(3),
            traffic_increase_percent: None,
            instance_delta: None,
            current_queue_count: None,
            queue_delta: None,
            target_version: None,
            configuration_changes: Vec::new(),
            affected_resource_keys: vec!["topic/orders".to_owned()],
            evidence_ids: Vec::new(),
        }
    }

    #[test]
    fn broker_offline_fixture_projects_remaining_capacity() {
        let projection = simulate(&request(SimulationKind::BrokerOffline));

        assert_eq!(projection.status, SimulationStatus::Completed);
        assert!(
            projection
                .projected_utilization
                .is_some_and(|value| (value - 0.9).abs() < f64::EPSILON)
        );
        assert_eq!(projection.blast_radius, ["topic/orders"]);
    }

    #[test]
    fn traffic_growth_fixture_accepts_only_bounded_percentages() {
        let mut input = request(SimulationKind::TrafficIncrease);
        input.traffic_increase_percent = Some(50);
        let projection = simulate(&input);
        assert!(
            projection
                .projected_utilization
                .is_some_and(|value| (value - 0.9).abs() < f64::EPSILON)
        );

        input.traffic_increase_percent = Some(60);
        assert_eq!(simulate(&input).status, SimulationStatus::InsufficientData);
    }

    #[test]
    fn queue_scenario_is_expand_only() {
        let mut input = request(SimulationKind::TopicQueueExpand);
        input.current_queue_count = Some(8);
        input.queue_delta = Some(0);
        assert_eq!(simulate(&input).status, SimulationStatus::InsufficientData);
    }
}
