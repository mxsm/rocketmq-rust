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

use super::{InvestigationPlanningInput, InvestigationReadBinding};
use crate::diagnostics::packs::{NAMESRV_ROUTE_SCHEMA, namesrv_route_domain};
use rocketmq_sre_contracts::{
    EvidenceContent, EvidenceId, InvestigationRun, InvestigationStepKind, InvestigationStepOutcome,
};
use std::collections::BTreeSet;

/// Metadata continuity is independent of business-evidence freshness. A lost
/// reference or changed key domain cannot be presented as new route progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamesrvDomainContinuity {
    NotApplicable,
    Unanchored,
    Matched,
    MissingAnchor,
    InvalidAnchor,
    ChangedDomain,
}
impl NamesrvDomainContinuity {
    #[must_use]
    pub const fn blocks_progress(self) -> bool {
        matches!(self, Self::MissingAnchor | Self::InvalidAnchor | Self::ChangedDomain)
    }
}

fn active<'a>(run: &InvestigationRun, bindings: &'a [InvestigationReadBinding]) -> Vec<&'a InvestigationReadBinding> {
    if !run
        .effective_packs
        .iter()
        .any(|pack| pack.pack_id == "namesrv-route.v2" && pack.pack_version == "2.0.0")
    {
        return vec![];
    }
    bindings
        .iter()
        .filter(|binding| {
            binding.requirement.pack.pack_id == "namesrv-route.v2"
                && binding.requirement.pack.pack_version == "2.0.0"
                && binding.requirement.requirement_id == "namesrv-route"
                && binding.source == "admin-query"
                && binding.resource.starts_with("namesrv-route/")
                && binding.scope == run.effective_scope
        })
        .collect()
}

/// Original references needed only for a bounded, service-authorized domain
/// metadata pool. This is not a request to re-use their historical business
/// facts. Supplied IDs are inspected before their source can be determined;
/// missing metadata cannot silently erase a possible original anchor.
#[must_use]
pub fn investigation_namesrv_anchor_ids(
    run: &InvestigationRun,
    bindings: &[InvestigationReadBinding],
) -> Vec<EvidenceId> {
    let active = active(run, bindings);
    if active.is_empty() {
        return vec![];
    }
    let mut ids: BTreeSet<_> = run.supplied_evidence_ids.iter().copied().collect();
    for step in &run.steps {
        if step.spec.pack.pack_id != "namesrv-route.v2"
            || step.spec.pack.pack_version != "2.0.0"
            || !matches!(&step.spec.work,InvestigationStepKind::CollectEvidence {requirement_id} if requirement_id=="namesrv-route")
            || !bindings.iter().any(|binding| {
                binding.requirement.pack == step.spec.pack
                    && binding.scope == step.spec.scope
                    && active
                        .iter()
                        .any(|current| current.source == binding.source && current.resource == binding.resource)
            })
        {
            continue;
        }
        for result in step.attempts.iter().filter_map(|attempt| attempt.result.as_ref()) {
            if result.outcome == InvestigationStepOutcome::Succeeded {
                ids.extend(&result.evidence_ids);
            }
        }
    }
    ids.into_iter().collect()
}

/// Checks immutable original references using their original read scope, even
/// after Steer narrows the effective window. Callers must have reauthorized
/// retention, tenant/cluster and original read provenance before supplying the
/// pool. No business TTL is applied to this metadata-only operation.
#[must_use]
pub fn investigation_namesrv_domain_continuity(input: &InvestigationPlanningInput<'_>) -> NamesrvDomainContinuity {
    let bindings = active(input.run, input.bindings);
    if bindings.is_empty() {
        return NamesrvDomainContinuity::NotApplicable;
    }
    let ids = investigation_namesrv_anchor_ids(input.run, input.bindings);
    let mut domains = BTreeSet::new();
    for id in ids {
        let Some(snapshot) = input.evidence.iter().find(|snapshot| snapshot.evidence_id == id) else {
            return NamesrvDomainContinuity::MissingAnchor;
        };
        if !bindings
            .iter()
            .any(|binding| snapshot.source == binding.source && snapshot.resource == binding.resource)
        {
            continue;
        }
        if snapshot.tenant_id != input.run.initial_scope.tenant_id
            || snapshot.cluster_id != input.run.initial_scope.cluster_id
            || snapshot.time_range.start.timestamp_micros()
                < input.run.initial_scope.time_range.start.timestamp_micros()
            || snapshot.time_range.end.timestamp_micros() > input.run.initial_scope.time_range.end.timestamp_micros()
            || snapshot.verify_content_hash().is_err()
        {
            return NamesrvDomainContinuity::InvalidAnchor;
        }
        let collected_by_run = input
            .run
            .steps
            .iter()
            .filter(|step| matches!(step.spec.work, InvestigationStepKind::CollectEvidence { .. }))
            .filter(|step| {
                step.attempts.iter().any(|attempt| {
                    attempt
                        .result
                        .as_ref()
                        .is_some_and(|result| result.evidence_ids.contains(&id))
                })
            })
            .collect::<Vec<_>>();
        if !collected_by_run.is_empty()
            && !collected_by_run.iter().any(|step| {
                step.spec.scope.tenant_id == snapshot.tenant_id
                    && step.spec.scope.cluster_id == snapshot.cluster_id
                    && step.spec.scope.time_range.start.timestamp_micros()
                        == snapshot.time_range.start.timestamp_micros()
                    && step.spec.scope.time_range.end.timestamp_micros() == snapshot.time_range.end.timestamp_micros()
            })
        {
            return NamesrvDomainContinuity::InvalidAnchor;
        }
        // An honestly unconfigured/missing collector response establishes no
        // domain. A recognized profile with invalid metadata cannot be skipped.
        if !matches!(&snapshot.content,EvidenceContent::Inline(content) if content["schema_version"].as_str()==Some(NAMESRV_ROUTE_SCHEMA))
        {
            continue;
        }
        match namesrv_route_domain(snapshot) {
            Ok(domain) => {
                domains.insert(domain);
            }
            Err(_) => return NamesrvDomainContinuity::InvalidAnchor,
        }
    }
    if domains.len() > 1 {
        NamesrvDomainContinuity::ChangedDomain
    } else if domains.is_empty() {
        NamesrvDomainContinuity::Unanchored
    } else {
        NamesrvDomainContinuity::Matched
    }
}
