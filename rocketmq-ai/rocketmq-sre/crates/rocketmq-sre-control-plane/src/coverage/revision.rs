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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::{ClusterId, DiagnosisRevisionId, EvidenceId, EvidenceSnapshot, InvestigationId};

use super::CoverageQuery;
use crate::{ControlPlaneFailure, ControlPlaneRequestFailure, api::AppState, auth::AuthContext};

pub(super) struct RevisionEvidence {
    pub(super) investigation_id: InvestigationId,
    pub(super) revision_id: DiagnosisRevisionId,
    pub(super) cluster_id: ClusterId,
    ids: Vec<EvidenceId>,
}

pub(super) async fn load(
    state: &AppState,
    auth: &AuthContext,
    query: &CoverageQuery,
) -> Result<Option<RevisionEvidence>, ControlPlaneRequestFailure> {
    let (investigation, revision) = match (&query.investigation_id, &query.revision_id) {
        (None, None) => return Ok(None),
        (Some(investigation), Some(revision)) => (investigation, revision),
        _ => {
            return Err(ControlPlaneRequestFailure::validation(
                "invalid_request",
                "investigation_id and revision_id must be supplied together",
            ));
        }
    };
    let investigation_id = investigation.parse::<InvestigationId>().map_err(|_| {
        ControlPlaneRequestFailure::validation("invalid_request", "investigation identifier must be a UUID")
    })?;
    let revision_id = revision
        .parse::<DiagnosisRevisionId>()
        .map_err(|_| ControlPlaneRequestFailure::validation("invalid_request", "revision identifier must be a UUID"))?;
    let view = state.workflow.investigation(auth, investigation_id).await?;
    // A saved child must agree with its authorized parent even if a corrupt
    // stored row carries another cluster. The legacy view omits this column.
    let scoped_revision = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM investigation_diagnosis_revisions
         WHERE id=$1 AND investigation_id=$2 AND tenant_id=$3 AND cluster_id=$4)",
    )
    .bind(revision_id.as_uuid())
    .bind(investigation_id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(view.investigation.cluster_id.as_uuid())
    .fetch_one(&state.repository.pool)
    .await?;
    if !scoped_revision {
        return Err(ControlPlaneRequestFailure::not_found());
    }
    let revision = view
        .diagnosis_revisions
        .iter()
        .find(|item| item.id == revision_id && item.investigation_id == investigation_id)
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
    Ok(Some(RevisionEvidence {
        investigation_id,
        revision_id,
        cluster_id: view.investigation.cluster_id,
        ids: revision.evidence_ids.clone(),
    }))
}

impl RevisionEvidence {
    pub(super) async fn evidence(
        &self,
        state: &AppState,
        auth: &AuthContext,
    ) -> Result<(Vec<EvidenceSnapshot>, bool), ControlPlaneRequestFailure> {
        let ids = self.ids.iter().copied().collect::<BTreeSet<_>>();
        let mut evidence = Vec::with_capacity(ids.len().min(64));
        for id in ids.iter().take(64) {
            match state.evidence.get(auth, *id).await {
                Ok(snapshot) if snapshot.cluster_id == self.cluster_id && snapshot.tenant_id == auth.tenant_id => {
                    evidence.push(snapshot)
                }
                Ok(_) => {
                    return Err(ControlPlaneRequestFailure::forbidden(
                        "cluster_not_allowed",
                        "revision evidence is outside the selected scope",
                    ));
                }
                Err(error) if error.failure() == ControlPlaneFailure::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok((evidence, ids.len() > 64))
    }
}
