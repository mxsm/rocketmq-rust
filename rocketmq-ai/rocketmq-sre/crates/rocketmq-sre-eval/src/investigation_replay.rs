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

//! Paired replay of the production investigation planner over immutable saved
//! Evidence. No connector, provider or target transport is available here.
//! Synthetic expectations and supplied adjudications are separate from execution.

mod adjudication;
mod files;
mod model;
mod runner;
mod validation;

pub use adjudication::adjudicate_pair;
pub use files::{InvestigationReplayFileRejection, replay_files};
pub use model::*;

/// Runs the explicit single-pack baseline and multi-pack arm with the same
/// Evidence fixture, clock, budget, deadline and model policy.
///
/// # Errors
/// Rejects malformed, oversized, ambiguous or out-of-scope fixtures and any
/// violation of the production planner/admission/settlement invariants.
pub fn replay_pair(case: &InvestigationReplayCase) -> Result<InvestigationReplayPair, InvestigationReplayRejection> {
    validation::validate_case(case)?;
    let baseline = runner::run(case, ReplayArm::SinglePack)?;
    let multi_pack = runner::run(case, ReplayArm::MultiPack)?;
    Ok(InvestigationReplayPair {
        schema_version: INVESTIGATION_REPLAY_SCHEMA_VERSION.to_owned(),
        case_id: case.case_id.clone(),
        case_digest: rocketmq_sre_contracts::canonical_sha256(case)
            .map_err(|_| InvestigationReplayRejection::InvalidCase)?,
        provenance: case.provenance.clone(),
        baseline,
        multi_pack,
    })
}

#[cfg(test)]
mod tests;
