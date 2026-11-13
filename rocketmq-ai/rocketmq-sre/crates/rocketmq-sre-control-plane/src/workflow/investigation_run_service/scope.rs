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

use chrono::{DateTime, Utc};
use rocketmq_sre_contracts::{
    ClusterId, EvidenceId, InvestigationEvidenceContext, InvestigationResource, InvestigationRunScope, ResourceKind,
};
use sqlx::Row;

use super::contract_failure;
use crate::{ControlPlaneRequestFailure, PostgresRepository, auth::AuthContext};

pub(super) async fn authorize_saved_scope(
    repository: &PostgresRepository,
    auth: &AuthContext,
    scope: &InvestigationRunScope,
) -> Result<Option<InvestigationResource>, ControlPlaneRequestFailure> {
    scope.validate().map_err(contract_failure)?;
    if scope.tenant_id != auth.tenant_id || !auth.clusters.contains(&scope.cluster_id) {
        return Err(scope_mismatch());
    }
    let row = sqlx::query("SELECT cluster_id, resource FROM investigations WHERE id=$1 AND tenant_id=$2")
        .bind(scope.investigation_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&repository.pool)
        .await?
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
    if row.try_get::<uuid::Uuid, _>("cluster_id")? != scope.cluster_id.as_uuid() {
        return Err(scope_mismatch());
    }
    let saved: Option<String> = row.try_get("resource")?;
    match saved.as_deref() {
        None | Some("cluster/overview") => Ok(None),
        Some(saved) => {
            let subject = resource_subject(saved, scope.cluster_id).ok_or_else(scope_mismatch)?;
            if scope.resource.as_ref() != Some(&subject) {
                return Err(scope_mismatch());
            }
            Ok(Some(subject))
        }
    }
}

/// Importing an ID is not proof of its relationship to this investigation.
/// Only immutable saved revisions establish related-resource provenance.
pub(super) async fn evidence_context(
    repository: &PostgresRepository,
    auth: &AuthContext,
    scope: &InvestigationRunScope,
    parent_subject: Option<&InvestigationResource>,
    ids: &[EvidenceId],
) -> Result<Vec<InvestigationEvidenceContext>, ControlPlaneRequestFailure> {
    let distinct = ids.iter().copied().collect::<BTreeSet<_>>();
    if ids.len() > 256 || distinct.len() != ids.len() || ids.iter().any(|id| id.as_uuid().is_nil()) {
        return Err(ControlPlaneRequestFailure::validation(
            "invalid_input",
            "invalid Evidence references",
        ));
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT e.id, e.cluster_id, e.resource, e.time_range_start, e.time_range_end, e.observed_at,
                EXISTS (SELECT 1 FROM investigation_diagnosis_revisions r
                        WHERE r.tenant_id=e.tenant_id AND r.cluster_id=e.cluster_id
                          AND r.investigation_id=$3 AND e.id=ANY(r.evidence_ids)) AS saved_reference
         FROM evidence_snapshots e
         WHERE e.tenant_id=$1 AND e.cluster_id=$2 AND e.id=ANY($4)",
    )
    .bind(auth.tenant_id.as_uuid())
    .bind(scope.cluster_id.as_uuid())
    .bind(scope.investigation_id.as_uuid())
    .bind(ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
    .fetch_all(&repository.pool)
    .await?;
    if rows.len() != ids.len() {
        return Err(ControlPlaneRequestFailure::not_found());
    }
    let mut proofs = Vec::with_capacity(rows.len());
    let as_of = Utc::now();
    for row in rows {
        let saved_reference: bool = row.try_get("saved_reference")?;
        let resource: String = row.try_get("resource")?;
        let start: DateTime<Utc> = row.try_get("time_range_start")?;
        let end: DateTime<Utc> = row.try_get("time_range_end")?;
        let observed: DateTime<Utc> = row.try_get("observed_at")?;
        if !saved_reference || !binding_matches(scope, parent_subject, &resource, start, end, observed, as_of) {
            return Err(scope_mismatch());
        }
        proofs.push(InvestigationEvidenceContext {
            evidence_id: EvidenceId::from_uuid(row.try_get("id")?),
            scope: scope.clone(),
        });
    }
    Ok(proofs)
}

fn binding_matches(
    scope: &InvestigationRunScope,
    parent_subject: Option<&InvestigationResource>,
    resource: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    observed: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> bool {
    if start > end
        || start < scope.time_range.start
        || end > scope.time_range.end
        // observed_at is collection time, not the requested history's deadline.
        || observed > as_of
    {
        return false;
    }
    scope.resource.as_ref().is_none_or(|subject| {
        resource_subject(resource, scope.cluster_id).as_ref() == Some(subject)
            // The caller has separately checked an actual saved revision's refs.
            // A broad parent narrowed to a new subject cannot confer this proof.
            || parent_subject == Some(subject)
    })
}

fn resource_subject(resource: &str, cluster_id: ClusterId) -> Option<InvestigationResource> {
    let (prefix, suffix) = resource.split_once('/')?;
    if suffix.is_empty() {
        return None;
    }
    let (kind, key) = match prefix {
        "cluster" if suffix == "overview" => (ResourceKind::Cluster, cluster_id.to_string()),
        "topics" | "topic" | "namesrv-route" => (ResourceKind::Topic, suffix.to_owned()),
        "brokers" | "broker" | "broker-runtime" | "broker-health" | "broker-ha" => {
            (ResourceKind::Broker, suffix.to_owned())
        }
        "consumer-groups" | "consumer-lag" | "consumer_group" => (ResourceKind::ConsumerGroup, suffix.to_owned()),
        "store-pressure" | "store" => (ResourceKind::Store, suffix.to_owned()),
        "metrics" | "telemetry" => (ResourceKind::Telemetry, suffix.to_owned()),
        "instant" | "range" => (ResourceKind::Telemetry, resource.to_owned()),
        _ => (
            serde_json::from_value::<ResourceKind>(serde_json::Value::String(prefix.to_owned())).ok()?,
            suffix.to_owned(),
        ),
    };
    Some(InvestigationResource { kind, key })
}

fn scope_mismatch() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::forbidden("scope_mismatch", "saved scope does not match current authority")
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use rocketmq_sre_contracts::{InvestigationId, TenantId, TimeRange};

    use super::*;

    #[test]
    fn resource_identity_is_exact_and_related_evidence_requires_a_bound_parent() {
        let now = Utc::now();
        let subject = InvestigationResource {
            kind: ResourceKind::Broker,
            key: "broker-a".to_owned(),
        };
        let scope = InvestigationRunScope {
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            investigation_id: InvestigationId::new(),
            resource: Some(subject.clone()),
            time_range: TimeRange::new(now - Duration::minutes(5), now).unwrap(),
        };
        let start = scope.time_range.start;
        let collected = now + Duration::minutes(10);
        assert!(binding_matches(
            &scope,
            None,
            "broker-runtime/broker-a",
            start,
            now,
            collected,
            collected
        ));
        assert!(!binding_matches(
            &scope,
            None,
            "broker-runtime/broker-ab",
            start,
            now,
            collected,
            collected
        ));
        assert!(!binding_matches(
            &scope,
            None,
            "store-pressure/broker-a",
            start,
            now,
            collected,
            collected
        ));
        assert!(binding_matches(
            &scope,
            Some(&subject),
            "store-pressure/broker-a",
            start,
            now,
            collected,
            collected
        ));
        assert!(!binding_matches(
            &scope,
            Some(&subject),
            "store-pressure/broker-a",
            start - Duration::seconds(1),
            now,
            collected,
            collected
        ));
        assert!(!binding_matches(
            &scope,
            Some(&subject),
            "store-pressure/broker-a",
            start,
            now,
            collected + Duration::seconds(1),
            collected
        ));
    }
}
