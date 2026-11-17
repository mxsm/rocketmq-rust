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

use rocketmq_sre_contracts::KnowledgeItem;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};

use crate::{ControlPlaneError, ControlPlaneRequestFailure};

/// Serializes only one immutable declared source identity across both writers.
/// Hash collisions merely serialize extra work; a lock grants no permission.
pub(crate) async fn lock_knowledge_source(
    transaction: &mut Transaction<'_, Postgres>,
    item: &KnowledgeItem,
) -> Result<(), ControlPlaneRequestFailure> {
    let material = serde_json::to_vec(&(
        "rocketmq-sre.knowledge-source-lock.v1",
        item.tenant_id,
        item.cluster_id,
        &item.source_uri,
        &item.source_version,
    ))
    .map_err(ControlPlaneError::configuration_source)?;
    let digest = Sha256::digest(material);
    let mut key = [0_u8; 8];
    key.copy_from_slice(&digest[..8]);
    sqlx::query("SET LOCAL lock_timeout = '3s'")
        .execute(&mut **transaction)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(i64::from_be_bytes(key))
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

pub(crate) async fn record_source_conflict(
    transaction: &mut Transaction<'_, Postgres>,
    item: &KnowledgeItem,
) -> Result<bool, ControlPlaneRequestFailure> {
    let conflict: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM knowledge_items WHERE tenant_id=$1
           AND cluster_id IS NOT DISTINCT FROM $2 AND source_uri=$3 AND source_version=$4
           AND id<>$5 AND content_hash<>$6 AND review_status NOT IN ('deprecated','expired'))",
    )
    .bind(item.tenant_id.as_uuid())
    .bind(item.cluster_id.map(|id| id.as_uuid()))
    .bind(&item.source_uri)
    .bind(&item.source_version)
    .bind(item.id.as_uuid())
    .bind(&item.content_hash)
    .fetch_one(&mut **transaction)
    .await?;
    if conflict {
        sqlx::query(
            "UPDATE knowledge_items SET conflict=TRUE, updated_at=$7 WHERE tenant_id=$1
               AND cluster_id IS NOT DISTINCT FROM $2 AND source_uri=$3 AND source_version=$4
               AND (id=$5 OR (content_hash<>$6 AND review_status NOT IN ('deprecated','expired')))",
        )
        .bind(item.tenant_id.as_uuid())
        .bind(item.cluster_id.map(|id| id.as_uuid()))
        .bind(&item.source_uri)
        .bind(&item.source_version)
        .bind(item.id.as_uuid())
        .bind(&item.content_hash)
        .bind(chrono::Utc::now())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(conflict)
}

/// Safe display is a declaration, never a validated link or publication proof.
pub(super) fn source_uri_display(value: &str) -> Option<String> {
    if value.len() > 2_000 || value.chars().any(char::is_control) {
        return None;
    }
    let mut url = url::Url::parse(value).ok()?;
    match url.scheme() {
        "http" | "https" if url.host_str().is_some() => {}
        "rocketmq-sre" if url.host_str() == Some("postmortems") => {}
        _ => return None,
    }
    url.set_username("").ok()?;
    url.set_password(None).ok()?;
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_display_never_reflects_uri_credentials_or_unsupported_schemes() {
        assert_eq!(
            source_uri_display("https://user:PRIVATE@example.invalid/source?key=PRIVATE#PRIVATE").as_deref(),
            Some("https://example.invalid/source")
        );
        for value in [
            "javascript:PRIVATE",
            "file:///PRIVATE",
            "invalid PRIVATE",
            "https://example.invalid/\nPRIVATE",
        ] {
            assert_eq!(source_uri_display(value), None);
        }
    }
}
