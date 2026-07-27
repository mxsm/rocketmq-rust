// Copyright 2023 The RocketMQ Rust Authors
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

use bytes::Bytes;
use chrono::DateTime;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceReference;
use rocketmq_sre_contracts::EvidenceSnapshot;
use sha2::Digest;
use sha2::Sha256;

use super::EvidenceBlobStore;
use super::EvidenceListQuery;
use super::EvidencePage;
use super::PersistEvidenceRequest;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const MAX_CONTENT_DOWNLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Canonical evidence persistence and bounded object-content access.
#[derive(Clone)]
pub(crate) struct EvidenceService {
    repository: PostgresRepository,
    blobs: EvidenceBlobStore,
}

impl EvidenceService {
    pub(crate) fn new(repository: PostgresRepository, blobs: EvidenceBlobStore) -> Self {
        Self { repository, blobs }
    }

    pub(crate) async fn persist(
        &self,
        auth: &AuthContext,
        request: PersistEvidenceRequest,
    ) -> Result<EvidenceSnapshot, ControlPlaneError> {
        request.validate()?;
        if request.evidence.tenant_id != auth.tenant_id || !auth.clusters.contains(&request.evidence.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "evidence scope differs from the authenticated connector scope",
            ));
        }
        let snapshot = normalize_for_persistence(request.evidence)?;
        let (snapshot, content_digest) = self.externalize_if_needed(snapshot).await?;
        self.repository
            .persist_evidence(
                auth,
                &snapshot,
                request.investigation_id,
                request.incident_id,
                &content_digest,
            )
            .await
    }

    pub(crate) async fn get(&self, auth: &AuthContext, id: EvidenceId) -> Result<EvidenceSnapshot, ControlPlaneError> {
        self.repository.evidence(auth, id).await
    }

    pub(crate) async fn list(
        &self,
        auth: &AuthContext,
        query: &EvidenceListQuery,
    ) -> Result<EvidencePage, ControlPlaneError> {
        self.repository.list_evidence(auth, query).await
    }

    pub(crate) async fn content(&self, auth: &AuthContext, id: EvidenceId) -> Result<Bytes, ControlPlaneError> {
        let snapshot = self.repository.evidence(auth, id).await?;
        let expected_digest = self.repository.evidence_content_digest(auth, id).await?;
        let content = match snapshot.content {
            EvidenceContent::Inline(value) => serde_json::to_vec(&value).map(Bytes::from).map_err(|_| {
                ControlPlaneError::validation("source_unavailable", "evidence content cannot be encoded")
            })?,
            EvidenceContent::Reference(reference) => {
                let bytes = self.blobs.get(&reference.uri, MAX_CONTENT_DOWNLOAD_BYTES).await?;
                if reference.digest != expected_digest {
                    return Err(ControlPlaneError::validation(
                        "invalid_content_hash",
                        "external evidence reference does not match its stored digest",
                    ));
                }
                bytes
            }
        };
        verify_content_digest(&content, &expected_digest)?;
        Ok(content)
    }

    async fn externalize_if_needed(
        &self,
        mut snapshot: EvidenceSnapshot,
    ) -> Result<(EvidenceSnapshot, String), ControlPlaneError> {
        let value = match &snapshot.content {
            EvidenceContent::Inline(value) => value,
            EvidenceContent::Reference(_) => {
                return Err(ControlPlaneError::validation(
                    "invalid_request",
                    "connector evidence must not supply an external content reference",
                ));
            }
        };
        let encoded = serde_json::to_vec(value)
            .map_err(|_| ControlPlaneError::validation("invalid_request", "evidence content cannot be encoded"))?;
        let content_digest = format!("sha256:{:x}", Sha256::digest(&encoded));
        if encoded.len() <= self.blobs.max_inline_bytes() {
            return Ok((snapshot, content_digest));
        }
        let path = format!(
            "evidence/{}/{}/{}.json",
            snapshot.tenant_id, snapshot.cluster_id, snapshot.evidence_id
        );
        let uri = self.blobs.put(&path, encoded.clone()).await?;
        snapshot.content = EvidenceContent::Reference(EvidenceReference {
            uri,
            digest: content_digest.clone(),
            media_type: "application/json".to_owned(),
            size_bytes: u64::try_from(encoded.len()).map_err(|_| {
                ControlPlaneError::validation("output_too_large", "evidence content exceeds the supported size")
            })?,
        });
        snapshot.content_hash = snapshot.compute_content_hash().map_err(|_| {
            ControlPlaneError::validation("invalid_content_hash", "evidence reference cannot be sealed")
        })?;
        Ok((snapshot, content_digest))
    }
}

fn normalize_for_persistence(mut snapshot: EvidenceSnapshot) -> Result<EvidenceSnapshot, ControlPlaneError> {
    snapshot.time_range.start = postgres_timestamp(snapshot.time_range.start)?;
    snapshot.time_range.end = postgres_timestamp(snapshot.time_range.end)?;
    snapshot.observed_at = postgres_timestamp(snapshot.observed_at)?;
    snapshot.content_hash = snapshot.compute_content_hash().map_err(|_| {
        ControlPlaneError::validation(
            "invalid_content_hash",
            "evidence cannot be sealed for persistent storage",
        )
    })?;
    Ok(snapshot)
}

fn postgres_timestamp(value: DateTime<Utc>) -> Result<DateTime<Utc>, ControlPlaneError> {
    value
        .with_nanosecond((value.nanosecond() / 1_000) * 1_000)
        .ok_or_else(|| {
            ControlPlaneError::validation("invalid_request", "evidence timestamp is outside the supported range")
        })
}

fn verify_content_digest(content: &[u8], expected_digest: &str) -> Result<(), ControlPlaneError> {
    let actual_digest = format!("sha256:{:x}", Sha256::digest(content));
    if actual_digest != expected_digest {
        return Err(ControlPlaneError::validation(
            "invalid_content_hash",
            "evidence content does not match its stored digest",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::DateTime;
    use chrono::Timelike;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::EvidenceQuery;
    use rocketmq_sre_contracts::InvestigationId;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::TimeRange;
    use rocketmq_sre_contracts::current_evidence_schema;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn large_inline_content_is_externalized_and_resealed() {
        let blobs = EvidenceBlobStore::in_memory(1_024);
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let at = Utc::now();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id,
            cluster_id,
            source: "prometheus".to_owned(),
            resource: "metrics/query".to_owned(),
            time_range: TimeRange::new(at, at).expect("time"),
        };
        let snapshot = EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(json!({"series": "x".repeat(2_048)})),
        )
        .expect("snapshot");
        let service = ExternalizationHarness { blobs };
        let (snapshot, digest) = service.externalize(snapshot).await.expect("externalize");
        assert!(matches!(snapshot.content, EvidenceContent::Reference(_)));
        assert!(digest.starts_with("sha256:"));
        snapshot.verify_content_hash().expect("sealed");
    }

    #[test]
    fn persistence_normalizes_submicrosecond_timestamps_and_reseals() {
        let at = DateTime::parse_from_rfc3339("2026-07-27T04:07:34.873014822Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            source: "rocketmq-mcp".to_owned(),
            resource: "consumer-lag/group/topic".to_owned(),
            time_range: TimeRange::new(at, at).expect("time"),
        };
        let snapshot = EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(json!({"lag": 1})),
        )
        .expect("snapshot");
        snapshot.verify_content_hash().expect("inbound hash");
        let inbound_hash = snapshot.content_hash.clone();

        let persisted = normalize_for_persistence(snapshot).expect("normalize");

        assert_eq!(persisted.time_range.start.nanosecond(), 873_014_000);
        assert_eq!(persisted.time_range.end.nanosecond(), 873_014_000);
        assert_eq!(persisted.observed_at.nanosecond(), 873_014_000);
        assert_ne!(persisted.content_hash, inbound_hash);
        persisted.verify_content_hash().expect("persisted hash");
    }

    #[tokio::test]
    async fn object_content_tampering_fails_digest_verification() {
        let blobs = EvidenceBlobStore::in_memory(1_024);
        let path = "evidence/tenant/cluster/tamper.json";
        let uri = blobs.put(path, b"original".to_vec()).await.expect("put original");
        let expected_digest = format!("sha256:{:x}", Sha256::digest(b"original"));
        blobs.put(path, b"tampered".to_vec()).await.expect("replace object");

        let content = blobs.get(&uri, 128).await.expect("get tampered object");
        let error = verify_content_digest(&content, &expected_digest).expect_err("tampering must fail closed");

        assert!(matches!(
            error,
            ControlPlaneError::Validation {
                code: "invalid_content_hash",
                ..
            }
        ));
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_round_trip_normalizes_time_and_detects_object_tampering() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let investigation_id = InvestigationId::new();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'local', 'test', 'test', 'evidence-roundtrip-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("evidence-roundtrip-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
        sqlx::query(
            "INSERT INTO investigations (
                id, tenant_id, cluster_id, title, symptom_family, fingerprint,
                status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'Evidence roundtrip', 'test', $4,
                'collecting', 'evidence-roundtrip-test', $5, $5
             )",
        )
        .bind(investigation_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(format!("evidence-roundtrip-{investigation_id}"))
        .bind(now)
        .execute(&repository.pool)
        .await
        .expect("test investigation");
        let auth = AuthContext {
            tenant_id,
            subject: "evidence-roundtrip-test".to_owned(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::new(),
        };
        let at = DateTime::parse_from_rfc3339("2026-07-27T04:07:34.873014822Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id,
            cluster_id,
            source: "rocketmq-mcp".to_owned(),
            resource: "consumer-lag/group/topic".to_owned(),
            time_range: TimeRange::new(at, at).expect("time"),
        };
        let evidence = EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(json!({"series": "x".repeat(2_048)})),
        )
        .expect("snapshot");
        let blobs = EvidenceBlobStore::in_memory(1_024);
        let service = EvidenceService::new(repository, blobs.clone());
        let persisted = service
            .persist(
                &auth,
                PersistEvidenceRequest {
                    investigation_id: Some(investigation_id),
                    incident_id: None,
                    evidence,
                },
            )
            .await
            .expect("persist evidence");

        let reloaded = service
            .get(&auth, persisted.evidence_id)
            .await
            .expect("reload evidence");
        assert_eq!(reloaded.time_range.start.nanosecond() % 1_000, 0);
        assert_eq!(reloaded.time_range.end.nanosecond() % 1_000, 0);
        assert_eq!(reloaded.observed_at.nanosecond() % 1_000, 0);
        reloaded.verify_content_hash().expect("database roundtrip hash");
        assert_eq!(
            service
                .content(&auth, persisted.evidence_id)
                .await
                .expect("original object")
                .len(),
            2_061
        );

        let path = format!("evidence/{tenant_id}/{cluster_id}/{}.json", persisted.evidence_id);
        blobs.put(&path, b"tampered".to_vec()).await.expect("replace object");
        let error = service
            .content(&auth, persisted.evidence_id)
            .await
            .expect_err("tampered object must fail closed");
        assert!(matches!(
            error,
            ControlPlaneError::Validation {
                code: "invalid_content_hash",
                ..
            }
        ));
    }

    struct ExternalizationHarness {
        blobs: EvidenceBlobStore,
    }

    impl ExternalizationHarness {
        async fn externalize(
            &self,
            mut snapshot: EvidenceSnapshot,
        ) -> Result<(EvidenceSnapshot, String), ControlPlaneError> {
            let encoded = match &snapshot.content {
                EvidenceContent::Inline(value) => serde_json::to_vec(value).expect("json"),
                EvidenceContent::Reference(_) => Vec::new(),
            };
            let digest = format!("sha256:{:x}", Sha256::digest(&encoded));
            let uri = self.blobs.put("evidence/test.json", encoded.clone()).await?;
            snapshot.content = EvidenceContent::Reference(EvidenceReference {
                uri,
                digest: digest.clone(),
                media_type: "application/json".to_owned(),
                size_bytes: encoded.len() as u64,
            });
            snapshot.content_hash = snapshot.compute_content_hash().expect("hash");
            Ok((snapshot, digest))
        }
    }
}
