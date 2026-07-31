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

use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::SloDimension;

use crate::slo::SliEvaluation;

const ALL_DIMENSIONS: [SloDimension; 8] = [
    SloDimension::Traffic,
    SloDimension::Consumer,
    SloDimension::Broker,
    SloDimension::Store,
    SloDimension::HaController,
    SloDimension::RoutingProxy,
    SloDimension::Security,
    SloDimension::Platform,
];

/// Fixed weights and caps used by the deterministic health score.
#[derive(Clone, Debug, PartialEq)]
pub struct HealthScorePolicy {
    pub dimension_weights: BTreeMap<SloDimension, u8>,
    pub partial_score_cap: u8,
    pub missing_score_cap: u8,
}

impl HealthScorePolicy {
    /// Validates exact eight-dimension coverage and a total weight of 100.
    ///
    /// # Errors
    ///
    /// Returns a stable reason code for an invalid policy.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.dimension_weights.len() != ALL_DIMENSIONS.len()
            || ALL_DIMENSIONS
                .iter()
                .any(|dimension| !self.dimension_weights.contains_key(dimension))
            || self
                .dimension_weights
                .values()
                .map(|weight| u16::from(*weight))
                .sum::<u16>()
                != 100
            || self.partial_score_cap > 100
            || self.missing_score_cap > self.partial_score_cap
        {
            return Err("invalid_health_score_policy");
        }
        Ok(())
    }
}

/// Explainable score for one fixed health dimension.
#[derive(Clone, Debug, PartialEq)]
pub struct DimensionScoreEvaluation {
    pub dimension: SloDimension,
    pub weight: u8,
    pub score: Option<u8>,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub triggered_sli_ids: Vec<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub reason_codes: Vec<String>,
}

/// Deterministic cluster score. No model-produced value is accepted.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterScoreEvaluation {
    pub score: Option<u8>,
    pub status: HealthStatus,
    pub data_quality: HealthDataQuality,
    pub dimensions: Vec<DimensionScoreEvaluation>,
    pub triggered_sli_ids: Vec<String>,
    pub evidence_ids: Vec<EvidenceId>,
}

/// Computes eight dimension scores and a fixed weighted cluster score.
///
/// Missing dimensions produce an unknown score instead of a fabricated zero.
/// A critical known dimension always dominates unknown or degraded data.
///
/// # Errors
///
/// Returns a stable reason code when policy or SLI ownership is invalid.
pub fn evaluate_health_score(
    policy: &HealthScorePolicy,
    slis: &[SliEvaluation],
) -> Result<ClusterScoreEvaluation, &'static str> {
    policy.validate()?;
    if slis.is_empty() {
        return Err("missing_sli_evaluations");
    }
    let mut ids = BTreeSet::new();
    if slis.iter().any(|sli| !ids.insert(sli.id.as_str())) {
        return Err("duplicate_sli_evaluation");
    }
    let dimensions = ALL_DIMENSIONS
        .into_iter()
        .map(|dimension| evaluate_dimension(policy, dimension, slis))
        .collect::<Vec<_>>();
    let any_critical = dimensions
        .iter()
        .any(|dimension| dimension.status == HealthStatus::Critical);
    let missing_dimension = dimensions.iter().any(|dimension| dimension.score.is_none());
    let score = if missing_dimension {
        None
    } else {
        let weighted = dimensions
            .iter()
            .map(|dimension| u32::from(dimension.score.unwrap_or_default()) * u32::from(dimension.weight))
            .sum::<u32>();
        Some(u8::try_from((weighted + 50) / 100).unwrap_or(100).min(100))
    };
    let data_quality = aggregate_quality(dimensions.iter().map(|dimension| dimension.data_quality));
    let status = if any_critical {
        HealthStatus::Critical
    } else if missing_dimension || matches!(data_quality, HealthDataQuality::Missing | HealthDataQuality::Stale) {
        HealthStatus::Unknown
    } else if dimensions
        .iter()
        .any(|dimension| dimension.status == HealthStatus::Degraded)
        || data_quality == HealthDataQuality::Partial
    {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    let triggered_sli_ids = dimensions
        .iter()
        .flat_map(|dimension| dimension.triggered_sli_ids.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let evidence_ids = dimensions
        .iter()
        .flat_map(|dimension| dimension.evidence_ids.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(ClusterScoreEvaluation {
        score,
        status,
        data_quality,
        dimensions,
        triggered_sli_ids,
        evidence_ids,
    })
}

fn evaluate_dimension(
    policy: &HealthScorePolicy,
    dimension: SloDimension,
    slis: &[SliEvaluation],
) -> DimensionScoreEvaluation {
    let owned = slis.iter().filter(|sli| sli.dimension == dimension).collect::<Vec<_>>();
    let weight = policy.dimension_weights[&dimension];
    if owned.is_empty() {
        return DimensionScoreEvaluation {
            dimension,
            weight,
            score: None,
            status: HealthStatus::Unknown,
            data_quality: HealthDataQuality::Missing,
            triggered_sli_ids: Vec::new(),
            evidence_ids: Vec::new(),
            reason_codes: vec!["dimension_sli_missing".to_owned()],
        };
    }
    let total_sli_weight = owned.iter().map(|sli| u32::from(sli.weight)).sum::<u32>();
    let all_unusable = owned
        .iter()
        .all(|sli| matches!(sli.data_quality, HealthDataQuality::Missing | HealthDataQuality::Stale));
    let data_quality = aggregate_quality(owned.iter().map(|sli| sli.data_quality));
    let mut score = if all_unusable {
        None
    } else {
        let penalty = owned
            .iter()
            .map(|sli| u32::from(status_penalty(sli.status)) * u32::from(sli.weight))
            .sum::<u32>();
        let raw = 100_u32.saturating_sub((penalty + total_sli_weight / 2) / total_sli_weight.max(1));
        Some(u8::try_from(raw).unwrap_or_default())
    };
    if let Some(value) = &mut score {
        *value = match data_quality {
            HealthDataQuality::Complete => *value,
            HealthDataQuality::Partial => (*value).min(policy.partial_score_cap),
            HealthDataQuality::Missing | HealthDataQuality::Stale => (*value).min(policy.missing_score_cap),
        };
    }
    let any_critical = owned.iter().any(|sli| sli.status == HealthStatus::Critical);
    let status = if any_critical {
        HealthStatus::Critical
    } else if all_unusable {
        HealthStatus::Unknown
    } else if owned.iter().any(|sli| sli.status == HealthStatus::Degraded)
        || data_quality != HealthDataQuality::Complete
    {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    let triggered_sli_ids = owned
        .iter()
        .filter(|sli| matches!(sli.status, HealthStatus::Critical | HealthStatus::Degraded))
        .map(|sli| sli.id.clone())
        .collect();
    let evidence_ids = owned
        .iter()
        .flat_map(|sli| sli.evidence_ids.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let reason_codes = owned
        .iter()
        .flat_map(|sli| sli.reason_codes.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    DimensionScoreEvaluation {
        dimension,
        weight,
        score,
        status,
        data_quality,
        triggered_sli_ids,
        evidence_ids,
        reason_codes,
    }
}

const fn status_penalty(status: HealthStatus) -> u8 {
    match status {
        HealthStatus::Healthy => 0,
        HealthStatus::Degraded => 35,
        HealthStatus::Critical => 75,
        HealthStatus::Unknown => 0,
    }
}

fn aggregate_quality(values: impl IntoIterator<Item = HealthDataQuality>) -> HealthDataQuality {
    values
        .into_iter()
        .max_by_key(|quality| match quality {
            HealthDataQuality::Complete => 0,
            HealthDataQuality::Partial => 1,
            HealthDataQuality::Stale => 2,
            HealthDataQuality::Missing => 3,
        })
        .unwrap_or(HealthDataQuality::Missing)
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::BurnRateSeverity;

    use super::*;
    use crate::slo::WindowEvaluation;

    fn policy() -> HealthScorePolicy {
        HealthScorePolicy {
            dimension_weights: BTreeMap::from([
                (SloDimension::Traffic, 15),
                (SloDimension::Consumer, 15),
                (SloDimension::Broker, 12),
                (SloDimension::Store, 15),
                (SloDimension::HaController, 13),
                (SloDimension::RoutingProxy, 10),
                (SloDimension::Security, 10),
                (SloDimension::Platform, 10),
            ]),
            partial_score_cap: 80,
            missing_score_cap: 60,
        }
    }

    fn sli(dimension: SloDimension, status: HealthStatus, quality: HealthDataQuality) -> SliEvaluation {
        let id = format!("{dimension:?}").to_ascii_lowercase();
        SliEvaluation {
            id,
            display_name: format!("{dimension:?}"),
            dimension,
            objective: 0.99,
            weight: 100,
            status,
            data_quality: quality,
            windows: vec![WindowEvaluation {
                window_id: "fast".into(),
                short_window_seconds: 300,
                long_window_seconds: 3_600,
                short_burn_rate: Some(1.0),
                long_burn_rate: Some(1.0),
                threshold: 14.4,
                severity: BurnRateSeverity::Critical,
                triggered: status == HealthStatus::Critical,
                status,
                data_quality: quality,
                observed_epoch_seconds: Some(1_000),
                evidence_ids: vec![EvidenceId::new()],
                reason_codes: Vec::new(),
            }],
            evidence_ids: vec![EvidenceId::new()],
            reason_codes: Vec::new(),
        }
    }

    fn complete() -> Vec<SliEvaluation> {
        ALL_DIMENSIONS
            .into_iter()
            .map(|dimension| sli(dimension, HealthStatus::Healthy, HealthDataQuality::Complete))
            .collect()
    }

    #[test]
    fn all_healthy_dimensions_score_one_hundred() {
        let result = evaluate_health_score(&policy(), &complete()).expect("health score");

        assert_eq!(result.score, Some(100));
        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.dimensions.len(), 8);
    }

    #[test]
    fn critical_cluster_is_not_masked_by_healthy_dimensions() {
        let mut slis = complete();
        slis[3] = sli(SloDimension::Store, HealthStatus::Critical, HealthDataQuality::Complete);
        let result = evaluate_health_score(&policy(), &slis).expect("health score");

        assert_eq!(result.status, HealthStatus::Critical);
        assert!(result.score.is_some_and(|score| score < 100));
        assert_eq!(result.triggered_sli_ids, vec!["store"]);
    }

    #[test]
    fn missing_dimension_never_fabricates_a_score() {
        let mut slis = complete();
        slis.retain(|sli| sli.dimension != SloDimension::Security);
        let result = evaluate_health_score(&policy(), &slis).expect("health score");

        assert_eq!(result.score, None);
        assert_eq!(result.status, HealthStatus::Unknown);
        assert_eq!(result.data_quality, HealthDataQuality::Missing);
    }

    #[test]
    fn critical_known_dimension_still_dominates_missing_data() {
        let mut slis = complete();
        slis.retain(|sli| sli.dimension != SloDimension::Security);
        slis[0] = sli(
            SloDimension::Traffic,
            HealthStatus::Critical,
            HealthDataQuality::Complete,
        );
        let result = evaluate_health_score(&policy(), &slis).expect("health score");

        assert_eq!(result.score, None);
        assert_eq!(result.status, HealthStatus::Critical);
    }
}
