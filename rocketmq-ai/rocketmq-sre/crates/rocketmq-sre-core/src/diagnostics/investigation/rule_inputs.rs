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

use rocketmq_sre_contracts::{
    DiagnosticEvidenceQuality, EvidenceSnapshot, InvestigationPackRef, InvestigationRunFailure,
};

use super::{InvestigationPlanningInput, planning, quality};
use crate::diagnostics::DiagnosticPackRegistry;

/// Selects exact current rule inputs for both the dispatcher and paired replay.
/// A compiled pack can admit validated context only while its own missing-fact
/// contract prevents a whole-pack conclusive result. No serialized flag opts in.
///
/// # Errors
/// Rejects invalid run state, scope, bindings or pack versions. Stale, malformed
/// and unbound snapshots are excluded rather than borrowed from another scope.
pub fn investigation_rule_inputs(
    registry: &DiagnosticPackRegistry,
    reference: &InvestigationPackRef,
    input: &InvestigationPlanningInput<'_>,
) -> Result<Vec<EvidenceSnapshot>, InvestigationRunFailure> {
    input.run.validate()?;
    planning::validate_inputs(registry, input)?;
    if !input.run.effective_packs.contains(reference) {
        return Err(InvestigationRunFailure::PackNotAllowed);
    }
    let pack = quality::exact_pack(registry, reference)?;
    let requirements = quality::requirements(pack, reference, input);
    let mut selected = Vec::new();
    for snapshot in input.evidence {
        let accepted = requirements.iter().any(|requirement| {
            if !requirement.evidence_ids.contains(&snapshot.evidence_id) {
                return false;
            }
            match requirement.quality {
                DiagnosticEvidenceQuality::Available => true,
                DiagnosticEvidenceQuality::Partial => {
                    !pack.missing_evidence_facts(&requirement.id, snapshot).is_empty()
                        && pack.accepts_partial_context(&requirement.id, snapshot, input.as_of_unix_ms)
                }
                DiagnosticEvidenceQuality::Missing
                | DiagnosticEvidenceQuality::Stale
                | DiagnosticEvidenceQuality::Unknown
                | DiagnosticEvidenceQuality::Unsupported => false,
            }
        });
        if accepted {
            selected.push(snapshot.clone());
        }
    }
    Ok(selected)
}
