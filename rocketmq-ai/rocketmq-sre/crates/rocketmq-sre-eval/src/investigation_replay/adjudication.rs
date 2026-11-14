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

use rocketmq_sre_contracts::{HypothesisAssessmentConclusion, InvestigationRunStatus, canonical_sha256};

use super::validation::{json_len, text};
use super::*;

/// Applies separately supplied verdicts to exact observed results. Missing
/// verdicts stay unadjudicated; synthetic judgments never become external ones.
///
/// # Errors
/// Rejects duplicate/conflicting annotations, stale observations, malformed
/// provenance and a reasonable-inconclusive verdict on a strong conclusion.
pub fn adjudicate_pair(
    pair: &InvestigationReplayPair,
    annotations: &[SuppliedReplayAdjudication],
) -> Result<AdjudicatedReplayPair, InvestigationReplayRejection> {
    use InvestigationReplayRejection::InvalidAdjudication as Invalid;
    if annotations.len() > 2 {
        return Err(Invalid);
    }
    json_len(&annotations, 32 * 1024).map_err(|_| Invalid)?;
    let mut baseline = ReplayAdjudication::Unadjudicated {};
    let mut multi_pack = ReplayAdjudication::Unadjudicated {};
    for annotation in annotations {
        let (observed, target) = match annotation.arm {
            ReplayArm::SinglePack => (&pair.baseline, &mut baseline),
            ReplayArm::MultiPack => (&pair.multi_pack, &mut multi_pack),
        };
        if !matches!(target, ReplayAdjudication::Unadjudicated {})
            || annotation.case_id != pair.case_id
            || annotation.case_digest != pair.case_digest
            || annotation.arm != observed.arm
            || annotation.observation != observed.observation
            || annotation.observation_digest != observed.observation_digest
            || canonical_sha256(&annotation.observation).map_err(|_| Invalid)? != annotation.observation_digest
            || !text(&annotation.rationale, 4096)
        {
            return Err(Invalid);
        }
        let provenance_valid = match &annotation.provenance {
            AdjudicationProvenance::SyntheticFixture { fixture_ref } => text(fixture_ref, 512),
            AdjudicationProvenance::ExternallySupplied {
                adjudicator_ref,
                source_ref,
            } => text(adjudicator_ref, 512) && text(source_ref, 512),
        };
        if !provenance_valid {
            return Err(Invalid);
        }
        let inconclusive = match observed.observation.conclusion {
            Some(HypothesisAssessmentConclusion::Inconclusive { .. }) => true,
            None => {
                observed.observation.status == InvestigationRunStatus::NeedsEvidence
                    && observed.observation.stop_reason.is_some()
            }
            Some(HypothesisAssessmentConclusion::RootCause { .. } | HypothesisAssessmentConclusion::Healthy { .. }) => {
                false
            }
        };
        if annotation.verdict == ReplayVerdict::ReasonableInconclusive && !inconclusive {
            return Err(Invalid);
        }
        *target = ReplayAdjudication::Supplied {
            verdict: annotation.verdict,
            rationale: annotation.rationale.clone(),
            provenance: annotation.provenance.clone(),
        };
    }
    Ok(AdjudicatedReplayPair {
        case_id: pair.case_id.clone(),
        baseline,
        multi_pack,
    })
}
