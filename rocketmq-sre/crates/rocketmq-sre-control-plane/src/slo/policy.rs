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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use rocketmq_sre_contracts::BurnRateSeverity;
use rocketmq_sre_contracts::HealthOperationalState;
use rocketmq_sre_contracts::SloDimension;
use rocketmq_sre_core::health::HealthScorePolicy;
use rocketmq_sre_core::slo::BurnWindowPolicy;
use rocketmq_sre_core::slo::SliPolicy;
use rocketmq_sre_core::slo::SloPolicy;
use serde::Deserialize;

use crate::ControlPlaneError;

const SLO_CONFIG: &str = include_str!("../../../../config/slo/rocketmq-slo.yaml");
const SCHEMA_VERSION: &str = "rocketmq-sre.slo-config.v1";
const EXPECTED_RECORDING_METRIC: &str = "rocketmq_sre_sli_burn_rate";
const EXPECTED_SLI_COUNT: usize = 18;
#[cfg(test)]
const BURN_RATE_RULES: &str = include_str!("../../../../deploy/helm/rocketmq-sre/rules/rocketmq-burn-rate.yaml");

/// Parsed, validated deterministic SLO and health-score configuration.
#[derive(Clone, Debug)]
pub(crate) struct SloConfiguration {
    pub(crate) algorithm_version: String,
    pub(crate) recording_metric: String,
    pub(crate) slo_policy: SloPolicy,
    pub(crate) score_policy: HealthScorePolicy,
    pub(crate) worker_interval: Duration,
    pub(crate) query_timeout: Duration,
    maintenance_environments: BTreeSet<String>,
    fault_drill_environments: BTreeSet<String>,
}

impl SloConfiguration {
    pub(crate) fn embedded() -> Result<Self, ControlPlaneError> {
        Self::parse(SLO_CONFIG)
    }

    fn parse(input: &str) -> Result<Self, ControlPlaneError> {
        let document: SloConfigurationDocument = serde_yaml::from_str(input).map_err(|error| {
            ControlPlaneError::configuration(format!("embedded SLO configuration cannot be parsed: {error}"))
        })?;
        if document.schema_version != SCHEMA_VERSION
            || document.algorithm_version.trim().is_empty()
            || document.recording_metric != EXPECTED_RECORDING_METRIC
            || document.slis.len() != EXPECTED_SLI_COUNT
            || document.worker_interval_seconds == 0
            || document.query_timeout_seconds == 0
            || document.query_timeout_seconds >= document.worker_interval_seconds
        {
            return Err(ControlPlaneError::configuration(
                "embedded SLO configuration has an unsupported shape",
            ));
        }
        let slo_policy = SloPolicy {
            windows: document
                .windows
                .into_iter()
                .map(|window| BurnWindowPolicy {
                    id: window.id,
                    short_window_seconds: window.short_window_seconds,
                    long_window_seconds: window.long_window_seconds,
                    threshold: window.threshold,
                    severity: window.severity,
                })
                .collect(),
            slis: document
                .slis
                .into_iter()
                .map(|sli| SliPolicy {
                    id: sli.id,
                    display_name: sli.display_name,
                    dimension: sli.dimension,
                    objective: sli.objective,
                    weight: sli.weight,
                })
                .collect(),
            freshness_seconds: document.freshness_seconds,
        };
        slo_policy
            .validate()
            .map_err(|reason| ControlPlaneError::configuration(format!("embedded SLO policy is invalid: {reason}")))?;
        let score_policy = HealthScorePolicy {
            dimension_weights: document.dimensions,
            partial_score_cap: document.score_caps.partial,
            missing_score_cap: document.score_caps.missing,
        };
        score_policy.validate().map_err(|reason| {
            ControlPlaneError::configuration(format!("embedded health score policy is invalid: {reason}"))
        })?;
        validate_sli_dimension_coverage(&slo_policy)?;
        let maintenance_environments = normalized_environments(document.operational_context.maintenance_environments)?;
        let fault_drill_environments = normalized_environments(document.operational_context.fault_drill_environments)?;
        if !maintenance_environments.is_disjoint(&fault_drill_environments) {
            return Err(ControlPlaneError::configuration(
                "maintenance and fault-drill environments must be disjoint",
            ));
        }
        Ok(Self {
            algorithm_version: document.algorithm_version,
            recording_metric: document.recording_metric,
            slo_policy,
            score_policy,
            worker_interval: Duration::from_secs(document.worker_interval_seconds),
            query_timeout: Duration::from_secs(document.query_timeout_seconds),
            maintenance_environments,
            fault_drill_environments,
        })
    }

    pub(crate) fn resource(&self) -> String {
        format!("instant/{}", self.recording_metric)
    }

    pub(crate) fn operational_state(&self, environment: &str) -> HealthOperationalState {
        let environment = environment.trim().to_ascii_lowercase();
        if self.maintenance_environments.contains(&environment) {
            HealthOperationalState::Maintenance
        } else if self.fault_drill_environments.contains(&environment) {
            HealthOperationalState::FaultDrill
        } else {
            HealthOperationalState::Normal
        }
    }
}

#[derive(Deserialize)]
struct SloConfigurationDocument {
    schema_version: String,
    algorithm_version: String,
    recording_metric: String,
    freshness_seconds: u64,
    worker_interval_seconds: u64,
    query_timeout_seconds: u64,
    dimensions: BTreeMap<SloDimension, u8>,
    score_caps: ScoreCaps,
    windows: Vec<BurnWindowDocument>,
    operational_context: OperationalContext,
    slis: Vec<SliDocument>,
}

#[derive(Deserialize)]
struct ScoreCaps {
    partial: u8,
    missing: u8,
}

#[derive(Deserialize)]
struct BurnWindowDocument {
    id: String,
    short_window_seconds: u64,
    long_window_seconds: u64,
    threshold: f64,
    severity: BurnRateSeverity,
}

#[derive(Deserialize)]
struct OperationalContext {
    #[serde(default)]
    maintenance_environments: Vec<String>,
    #[serde(default)]
    fault_drill_environments: Vec<String>,
}

#[derive(Deserialize)]
struct SliDocument {
    id: String,
    display_name: String,
    dimension: SloDimension,
    objective: f64,
    weight: u8,
}

fn validate_sli_dimension_coverage(policy: &SloPolicy) -> Result<(), ControlPlaneError> {
    let coverage = policy.slis.iter().map(|sli| sli.dimension).collect::<BTreeSet<_>>();
    if coverage
        != BTreeSet::from([
            SloDimension::Traffic,
            SloDimension::Consumer,
            SloDimension::Broker,
            SloDimension::Store,
            SloDimension::HaController,
            SloDimension::RoutingProxy,
            SloDimension::Security,
            SloDimension::Platform,
        ])
    {
        return Err(ControlPlaneError::configuration(
            "embedded SLO policy must cover all eight health dimensions",
        ));
    }
    Ok(())
}

fn normalized_environments(values: Vec<String>) -> Result<BTreeSet<String>, ControlPlaneError> {
    let values = values
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if values
        .iter()
        .any(|value| value.is_empty() || value.len() > 64 || value.chars().any(char::is_control))
    {
        return Err(ControlPlaneError::configuration(
            "SLO operational environment name is invalid",
        ));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_policy_has_three_windows_eighteen_slis_and_eight_dimensions() {
        let config = SloConfiguration::embedded().expect("embedded SLO configuration");

        assert_eq!(config.slo_policy.windows.len(), 3);
        assert_eq!(config.slo_policy.slis.len(), 18);
        assert_eq!(config.score_policy.dimension_weights.len(), 8);
        assert_eq!(
            config
                .score_policy
                .dimension_weights
                .values()
                .copied()
                .map(u16::from)
                .sum::<u16>(),
            100
        );
        assert_eq!(config.recording_metric, EXPECTED_RECORDING_METRIC);
    }

    #[test]
    fn operational_context_is_explicit_and_does_not_change_score_policy() {
        let config = SloConfiguration::embedded().expect("embedded SLO configuration");

        assert_eq!(
            config.operational_state("maintenance"),
            HealthOperationalState::Maintenance
        );
        assert_eq!(config.operational_state("chaos"), HealthOperationalState::FaultDrill);
        assert_eq!(config.operational_state("production"), HealthOperationalState::Normal);
    }

    #[test]
    fn recording_rules_cover_every_configured_sli() {
        let config = SloConfiguration::embedded().expect("embedded SLO configuration");
        let rules: serde_yaml::Value = serde_yaml::from_str(BURN_RATE_RULES).expect("PrometheusRule YAML");
        let groups = rules["spec"]["groups"].as_sequence().expect("rule groups");
        let covered = groups
            .iter()
            .filter(|group| {
                matches!(
                    group["name"].as_str(),
                    Some("rocketmq-sre.sli-error-ratios" | "rocketmq-sre.sli-objectives")
                )
            })
            .flat_map(|group| group["rules"].as_sequence().expect("group rules"))
            .filter_map(|rule| rule["labels"]["sli"].as_str())
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let configured = config
            .slo_policy
            .slis
            .iter()
            .map(|sli| sli.id.clone())
            .collect::<BTreeSet<_>>();

        assert_eq!(covered, configured);
    }
}
