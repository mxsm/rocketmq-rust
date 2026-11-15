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

use super::*;

pub(super) fn catalog_context(
    service: &InvestigationRunService,
    auth: &AuthContext,
    run: &InvestigationRun,
) -> InvestigationRunContext {
    let mut allowed_packs = Vec::new();
    let mut allowed_requirements = Vec::new();
    for pack in service.catalog.active_packs() {
        let reference = InvestigationPackRef {
            pack_id: pack.qualified_id(),
            pack_version: pack.version().to_string(),
        };
        for requirement in pack.required_evidence().iter().chain(pack.optional_evidence()) {
            allowed_requirements.push(InvestigationRequirementRef {
                pack: reference.clone(),
                requirement_id: requirement.key.to_owned(),
            });
        }
        allowed_packs.push(reference);
    }
    // A queued job pins its accepted compiled version, even after the active
    // major-qualified alias points at a newer compiled implementation.
    for reference in &run.initial_packs {
        if !allowed_packs.contains(reference)
            && let Ok(pack) = super::super::bindings::exact_pack(&service.catalog, reference)
        {
            allowed_packs.push(reference.clone());
            for requirement in pack.required_evidence().iter().chain(pack.optional_evidence()) {
                allowed_requirements.push(InvestigationRequirementRef {
                    pack: reference.clone(),
                    requirement_id: requirement.key.to_owned(),
                });
            }
        }
    }
    InvestigationRunContext {
        actor_subject: auth.subject.clone(),
        permission: InvestigationRunPermission::Control,
        authorized_scope: run.initial_scope.clone(),
        allowed_packs,
        allowed_requirements,
        visible_evidence: vec![],
    }
}

impl RunDispatcher {
    pub(super) async fn planning_snapshots(
        &self,
        service: &InvestigationRunService,
        auth: &AuthContext,
        run: &InvestigationRun,
        bindings: &[InvestigationReadBinding],
        active: &[EvidenceSnapshot],
    ) -> Result<Vec<EvidenceSnapshot>, ControlPlaneRequestFailure> {
        let ids = investigation_namesrv_anchor_ids(run, bindings);
        let mut pool = active.to_vec();
        if ids.is_empty() {
            return Ok(pool);
        }
        let proofs = service.repository.investigation_run_read_proofs(auth, run).await?;
        let parent = super::super::scope::authorize_saved_scope(&service.repository, auth, &run.initial_scope).await?;
        for id in ids {
            if pool.iter().any(|snapshot| snapshot.evidence_id == id) {
                continue;
            }
            if pool.len() >= 256 {
                return Err(contract_failure(InvestigationRunFailure::InvalidInput));
            }
            let Some(snapshot) = self.snapshots(auth, &[id]).await?.pop() else {
                return Err(ControlPlaneRequestFailure::not_found());
            };
            if snapshot.tenant_id != auth.tenant_id || snapshot.cluster_id != run.initial_scope.cluster_id {
                return Err(contract_failure(InvestigationRunFailure::ScopeMismatch));
            }
            if snapshot.source == "admin-query" && snapshot.resource.starts_with("namesrv-route/") {
                let from_read = proofs.iter().any(|proof| {
                    proof.query.query_id == snapshot.query_id
                        && proof.query.correlation_id == snapshot.correlation_id
                        && proof.query.source == snapshot.source
                        && proof.query.resource == snapshot.resource
                        && proof.query.time_range.start.timestamp_micros()
                            == snapshot.time_range.start.timestamp_micros()
                        && proof.query.time_range.end.timestamp_micros() == snapshot.time_range.end.timestamp_micros()
                        && proof.scope.narrows(&run.initial_scope)
                });
                if !from_read {
                    if !run.supplied_evidence_ids.contains(&id) {
                        return Err(contract_failure(InvestigationRunFailure::ScopeMismatch));
                    }
                    // Original saved references are checked against their
                    // immutable initial scope, not a later Steer window.
                    let mut original = run.initial_scope.clone();
                    original.time_range.start =
                        chrono::DateTime::from_timestamp_micros(original.time_range.start.timestamp_micros())
                            .ok_or_else(|| contract_failure(InvestigationRunFailure::InvalidInput))?;
                    original.time_range.end =
                        chrono::DateTime::from_timestamp_micros(original.time_range.end.timestamp_micros())
                            .ok_or_else(|| contract_failure(InvestigationRunFailure::InvalidInput))?;
                    super::super::scope::evidence_context(&service.repository, auth, &original, parent.as_ref(), &[id])
                        .await?;
                }
            }
            pool.push(snapshot);
        }
        Ok(pool)
    }

    pub(super) async fn snapshots(
        &self,
        auth: &AuthContext,
        ids: &[EvidenceId],
    ) -> Result<Vec<EvidenceSnapshot>, ControlPlaneRequestFailure> {
        if ids.len() > 256 {
            return Err(super::super::contract_failure(InvestigationRunFailure::InvalidInput));
        }
        let mut snapshots = Vec::new();
        for id in ids {
            let mut snapshot = self.evidence.get(auth, *id).await?;
            snapshot.verify_content_hash().map_err(|_| {
                ControlPlaneRequestFailure::state(
                    "invalid_content_hash",
                    "stored run evidence failed integrity validation",
                )
            })?;
            if matches!(snapshot.content, EvidenceContent::Reference(_)) {
                let bytes = self.evidence.content(auth, *id).await?;
                if bytes.len() > crate::connector_channel::MAX_RESPONSE_BYTES {
                    return Err(ControlPlaneRequestFailure::state(
                        "output_too_large",
                        "stored run evidence exceeds the bounded read size",
                    ));
                }
                snapshot.content = EvidenceContent::Inline(serde_json::from_slice(&bytes).map_err(|_| {
                    ControlPlaneRequestFailure::state("invalid_evidence", "stored evidence is not bounded JSON")
                })?);
                snapshot.content_hash = snapshot.compute_content_hash().map_err(|_| {
                    ControlPlaneRequestFailure::state("invalid_content_hash", "cannot materialize stored evidence")
                })?;
            }
            snapshots.push(snapshot);
        }
        Ok(snapshots)
    }

    pub(super) async fn context(
        &self,
        service: &InvestigationRunService,
        auth: &AuthContext,
        run: &InvestigationRun,
        extra: &[EvidenceSnapshot],
    ) -> Result<InvestigationRunContext, ControlPlaneRequestFailure> {
        super::super::scope::authorize_saved_scope(&service.repository, auth, &run.initial_scope).await?;
        let mut context = catalog_context(service, auth, run);
        let proofs = service.repository.investigation_run_read_proofs(auth, run).await?;
        let mut snapshots = self.snapshots(auth, &run.evidence_ids).await?;
        snapshots.extend_from_slice(extra);
        for snapshot in snapshots {
            if snapshot.tenant_id != auth.tenant_id
                || snapshot.cluster_id != run.initial_scope.cluster_id
                || snapshot.observed_at > Utc::now()
            {
                return Err(super::super::contract_failure(InvestigationRunFailure::ScopeMismatch));
            }
            if let Some(proof) = proofs.iter().find(|proof| {
                proof.query.query_id == snapshot.query_id
                    && proof.query.correlation_id == snapshot.correlation_id
                    && proof.query.source == snapshot.source
                    && proof.query.resource == snapshot.resource
                    && proof.query.time_range.start.timestamp_micros() == snapshot.time_range.start.timestamp_micros()
                    && proof.query.time_range.end.timestamp_micros() == snapshot.time_range.end.timestamp_micros()
            }) {
                if !context
                    .visible_evidence
                    .iter()
                    .any(|item| item.evidence_id == snapshot.evidence_id)
                {
                    context.visible_evidence.push(InvestigationEvidenceContext {
                        evidence_id: snapshot.evidence_id,
                        scope: proof.scope.clone(),
                    });
                }
            } else if run.supplied_evidence_ids.contains(&snapshot.evidence_id) {
                let parent =
                    super::super::scope::authorize_saved_scope(&service.repository, auth, &run.effective_scope).await?;
                let visible = super::super::scope::evidence_context(
                    &service.repository,
                    auth,
                    &run.effective_scope,
                    parent.as_ref(),
                    &[snapshot.evidence_id],
                )
                .await?;
                context.visible_evidence.extend(visible);
            } else {
                return Err(super::super::contract_failure(InvestigationRunFailure::ScopeMismatch));
            }
        }
        Ok(context)
    }
}
