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

use rocketmq_sre_contracts::AssetSnapshotId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimelineEventId;
use rocketmq_sre_contracts::TopologyEdgeId;
use serde_json::Value;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::AssetKey;
use super::AssetListQuery;
use super::AssetPage;
use super::AssetSource;
use super::IngestInventoryRequest;
use super::InventorySnapshot;
use super::NormalizedAsset;
use super::NormalizedTopologyEdge;
use super::TopologyDiff;
use super::TopologyDiffEntry;
use super::calculate_diff;
use super::invalid_stored_inventory;
use super::materialize_snapshot;
use super::verify_diff;
use super::verify_snapshot;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

impl PostgresRepository {
    pub(crate) async fn persist_inventory_snapshot(
        &self,
        auth: &AuthContext,
        request: &IngestInventoryRequest,
    ) -> Result<(InventorySnapshot, TopologyDiff), ControlPlaneError> {
        enforce_scope(auth, auth.tenant_id, request.cluster_id)?;
        let snapshot = materialize_snapshot(auth.tenant_id, request)?;
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth, request.cluster_id).await?;

        let previous_id = sqlx::query(
            "SELECT id
             FROM asset_inventory_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(request.cluster_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| row.try_get::<Uuid, _>("id"))
        .transpose()?;
        let previous = match previous_id {
            Some(id) => Some(snapshot_in_transaction(&mut transaction, auth.tenant_id, id).await?),
            None => None,
        };
        let diff = calculate_diff(previous.as_ref(), &snapshot)?;

        insert_snapshot(&mut transaction, &snapshot).await?;
        insert_diff(&mut transaction, &diff).await?;
        transaction.commit().await?;
        Ok((snapshot, diff))
    }

    pub(crate) async fn inventory_snapshot(
        &self,
        auth: &AuthContext,
        snapshot_id: Uuid,
    ) -> Result<InventorySnapshot, ControlPlaneError> {
        let snapshot = snapshot_from_pool(self, auth.tenant_id, snapshot_id).await?;
        enforce_scope(auth, snapshot.tenant_id, snapshot.cluster_id)?;
        Ok(snapshot)
    }

    pub(crate) async fn latest_inventory_snapshot(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Option<InventorySnapshot>, ControlPlaneError> {
        enforce_scope(auth, auth.tenant_id, cluster_id)?;
        let row = sqlx::query(
            "SELECT id
             FROM asset_inventory_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY observed_at DESC, created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let snapshot_id = row.map(|row| row.try_get::<Uuid, _>("id")).transpose()?;
        match snapshot_id {
            Some(id) => snapshot_from_pool(self, auth.tenant_id, id).await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn list_latest_assets(
        &self,
        auth: &AuthContext,
        query: &AssetListQuery,
    ) -> Result<AssetPage, ControlPlaneError> {
        enforce_scope(auth, auth.tenant_id, query.cluster_id)?;
        let limit = query.bounded_limit()?;
        let cursor = query.cursor.as_deref().map(AssetKey::parse_canonical).transpose()?;
        let metadata = sqlx::query(
            "SELECT id, observed_at, partial
             FROM asset_inventory_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY observed_at DESC, created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(metadata) = metadata else {
            return Ok(AssetPage {
                snapshot_id: None,
                observed_at: None,
                items: Vec::new(),
                next_cursor: None,
                partial: false,
            });
        };
        let snapshot_id: Uuid = metadata.try_get("id")?;
        let rows = sqlx::query(
            "SELECT id, kind, external_key, display_name, source, attributes,
                    observed_at, freshness_seconds, partial, content_hash
             FROM asset_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2 AND inventory_snapshot_id = $3
               AND ($4::TEXT IS NULL OR kind = $4)
               AND (
                    $5::TEXT IS NULL
                    OR (kind, external_key) > ($5::TEXT, $6::TEXT)
               )
             ORDER BY kind, external_key
             LIMIT $7",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(snapshot_id)
        .bind(query.kind.map(|kind| kind.as_str()))
        .bind(cursor.as_ref().map(|key| key.kind.as_str()))
        .bind(cursor.as_ref().map(|key| key.external_key.as_str()))
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let items = rows
            .iter()
            .take(limit as usize)
            .map(asset_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = has_more
            .then(|| items.last().map(|asset| asset.key.canonical()))
            .flatten();
        let partial = metadata.try_get::<bool, _>("partial")? || items.iter().any(|asset| asset.partial);
        Ok(AssetPage {
            snapshot_id: Some(snapshot_id),
            observed_at: Some(metadata.try_get("observed_at")?),
            items,
            next_cursor,
            partial,
        })
    }

    pub(crate) async fn latest_topology_diff(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Option<TopologyDiff>, ControlPlaneError> {
        enforce_scope(auth, auth.tenant_id, cluster_id)?;
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, previous_snapshot_id, current_snapshot_id,
                    previous_observed_at, current_observed_at, additions, removals, changes,
                    partial, suppressed_removals, content_hash, created_at
             FROM topology_diffs
             WHERE tenant_id = $1 AND cluster_id = $2 AND current_snapshot_id IS NOT NULL
             ORDER BY current_observed_at DESC, created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(diff_from_row).transpose()
    }

    pub(crate) async fn link_topology_diff(
        &self,
        auth: &AuthContext,
        diff: &TopologyDiff,
    ) -> Result<(), ControlPlaneError> {
        enforce_scope(auth, diff.tenant_id, diff.cluster_id)?;
        if diff.previous_snapshot_id.is_none()
            || (diff.additions.is_empty() && diff.removals.is_empty() && diff.changes.is_empty())
        {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT 'investigation' AS aggregate_type, id AS aggregate_id
             FROM investigations
             WHERE tenant_id = $1 AND cluster_id = $2
               AND status IN ('open', 'collecting', 'diagnosing', 'needs_evidence', 'monitoring')
             UNION ALL
             SELECT 'incident' AS aggregate_type, id AS aggregate_id
             FROM sre_incidents
             WHERE tenant_id = $1 AND cluster_id = $2
               AND status IN ('new', 'collecting', 'diagnosing', 'needs_evidence', 'monitoring')
             ORDER BY aggregate_type, aggregate_id
             LIMIT 100",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(diff.cluster_id.as_uuid())
        .fetch_all(&mut *transaction)
        .await?;
        let correlation_id = CorrelationId::new();
        let occurred_at = chrono::Utc::now();
        let details = serde_json::json!({
            "diff_id": diff.id,
            "previous_snapshot_id": diff.previous_snapshot_id,
            "current_snapshot_id": diff.current_snapshot_id,
            "additions": diff.additions.len(),
            "removals": diff.removals.len(),
            "changes": diff.changes.len(),
            "partial": diff.partial,
            "suppressed_removals": diff.suppressed_removals,
        });
        for row in rows {
            let aggregate_type: String = row.try_get("aggregate_type")?;
            let aggregate_id: Uuid = row.try_get("aggregate_id")?;
            let (investigation_id, incident_id) = match aggregate_type.as_str() {
                "investigation" => (Some(aggregate_id), None),
                "incident" => (None, Some(aggregate_id)),
                _ => return Err(invalid_stored_inventory("topology diff aggregate type")),
            };
            sqlx::query(
                "INSERT INTO incident_timeline (
                    event_id, tenant_id, cluster_id, investigation_id, incident_id,
                    event_type, summary, details, correlation_id, actor_subject, occurred_at
                 ) VALUES ($1, $2, $3, $4, $5, 'topology_diff_linked',
                           'A recent asset or topology change was linked to this workflow',
                           $6, $7, $8, $9)",
            )
            .bind(TimelineEventId::new().as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .bind(diff.cluster_id.as_uuid())
            .bind(investigation_id)
            .bind(incident_id)
            .bind(&details)
            .bind(correlation_id.as_uuid())
            .bind(&auth.subject)
            .bind(occurred_at)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO workflow_events (
                    event_id, tenant_id, cluster_id, aggregate_type, aggregate_id,
                    event_type, event_payload, correlation_id, occurred_at
                 ) VALUES ($1, $2, $3, $4, $5, 'topology_diff_linked', $6, $7, $8)",
            )
            .bind(Uuid::new_v4())
            .bind(auth.tenant_id.as_uuid())
            .bind(diff.cluster_id.as_uuid())
            .bind(&aggregate_type)
            .bind(aggregate_id)
            .bind(&details)
            .bind(correlation_id.as_uuid())
            .bind(occurred_at)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

async fn insert_snapshot(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot: &InventorySnapshot,
) -> Result<(), ControlPlaneError> {
    let sources = serde_json::to_value(&snapshot.sources)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "inventory sources cannot be serialized"))?;
    sqlx::query(
        "INSERT INTO asset_inventory_snapshots (
            id, tenant_id, cluster_id, sources, observed_at, freshness_seconds,
            partial, content_hash
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(snapshot.id)
    .bind(snapshot.tenant_id.as_uuid())
    .bind(snapshot.cluster_id.as_uuid())
    .bind(sources)
    .bind(snapshot.observed_at)
    .bind(to_i64(snapshot.freshness_seconds, "snapshot freshness")?)
    .bind(snapshot.partial)
    .bind(&snapshot.content_hash)
    .execute(&mut **transaction)
    .await?;
    for asset in &snapshot.assets {
        sqlx::query(
            "INSERT INTO asset_snapshots (
                id, tenant_id, cluster_id, kind, external_key, display_name,
                source, attributes, observed_at, freshness_seconds, partial,
                content_hash, inventory_snapshot_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(asset.id.as_uuid())
        .bind(snapshot.tenant_id.as_uuid())
        .bind(snapshot.cluster_id.as_uuid())
        .bind(asset.key.kind.as_str())
        .bind(&asset.key.external_key)
        .bind(&asset.display_name)
        .bind(asset.source.as_str())
        .bind(&asset.attributes)
        .bind(asset.observed_at)
        .bind(to_i64(asset.freshness_seconds, "asset freshness")?)
        .bind(asset.partial)
        .bind(&asset.content_hash)
        .bind(snapshot.id)
        .execute(&mut **transaction)
        .await?;
    }
    for edge in &snapshot.edges {
        sqlx::query(
            "INSERT INTO topology_edges (
                id, tenant_id, cluster_id, from_key, to_key, relation, source,
                observed_at, freshness_seconds, partial, content_hash,
                inventory_snapshot_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(edge.id.as_uuid())
        .bind(snapshot.tenant_id.as_uuid())
        .bind(snapshot.cluster_id.as_uuid())
        .bind(edge.from.canonical())
        .bind(edge.to.canonical())
        .bind(edge.relation.as_str())
        .bind(edge.source.as_str())
        .bind(edge.observed_at)
        .bind(to_i64(edge.freshness_seconds, "edge freshness")?)
        .bind(edge.partial)
        .bind(&edge.content_hash)
        .bind(snapshot.id)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn insert_diff(
    transaction: &mut Transaction<'_, Postgres>,
    diff: &TopologyDiff,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO topology_diffs (
            id, tenant_id, cluster_id, previous_observed_at, current_observed_at,
            additions, removals, changes, created_at, previous_snapshot_id,
            current_snapshot_id, partial, suppressed_removals, content_hash
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
         )",
    )
    .bind(diff.id)
    .bind(diff.tenant_id.as_uuid())
    .bind(diff.cluster_id.as_uuid())
    .bind(diff.previous_observed_at)
    .bind(diff.current_observed_at)
    .bind(json_value(&diff.additions, "diff additions")?)
    .bind(json_value(&diff.removals, "diff removals")?)
    .bind(json_value(&diff.changes, "diff changes")?)
    .bind(diff.created_at)
    .bind(diff.previous_snapshot_id)
    .bind(diff.current_snapshot_id)
    .bind(diff.partial)
    .bind(i32::try_from(diff.suppressed_removals).map_err(|_| {
        ControlPlaneError::validation(
            "output_too_large",
            "suppressed removal count exceeds the supported range",
        )
    })?)
    .bind(&diff.content_hash)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn ensure_cluster_scope(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    cluster_id: ClusterId,
) -> Result<(), ControlPlaneError> {
    let row = sqlx::query(
        "SELECT tenant_id, onboarding_state
         FROM clusters
         WHERE id = $1
         FOR SHARE",
    )
    .bind(cluster_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ControlPlaneError::NotFound)?;
    let stored_tenant: String = row.try_get("tenant_id")?;
    if stored_tenant != auth.tenant_id.to_string() {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "cluster tenant differs from the authenticated tenant",
        ));
    }
    if row.try_get::<String, _>("onboarding_state")? == "offboarded" {
        return Err(ControlPlaneError::conflict(
            "offboarded clusters cannot accept inventory snapshots",
        ));
    }
    Ok(())
}

pub(crate) fn enforce_scope(
    auth: &AuthContext,
    tenant_id: TenantId,
    cluster_id: ClusterId,
) -> Result<(), ControlPlaneError> {
    if tenant_id != auth.tenant_id {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "inventory tenant differs from the authenticated tenant",
        ));
    }
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "inventory cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

async fn snapshot_from_pool(
    repository: &PostgresRepository,
    tenant_id: TenantId,
    snapshot_id: Uuid,
) -> Result<InventorySnapshot, ControlPlaneError> {
    let metadata = sqlx::query(SNAPSHOT_METADATA)
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .fetch_optional(&repository.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
    let assets = sqlx::query(ASSET_ROWS)
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .fetch_all(&repository.pool)
        .await?;
    let edges = sqlx::query(EDGE_ROWS)
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .fetch_all(&repository.pool)
        .await?;
    snapshot_from_rows(&metadata, &assets, &edges)
}

async fn snapshot_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    snapshot_id: Uuid,
) -> Result<InventorySnapshot, ControlPlaneError> {
    let metadata = sqlx::query(SNAPSHOT_METADATA)
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
    let assets = sqlx::query(ASSET_ROWS)
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .fetch_all(&mut **transaction)
        .await?;
    let edges = sqlx::query(EDGE_ROWS)
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .fetch_all(&mut **transaction)
        .await?;
    snapshot_from_rows(&metadata, &assets, &edges)
}

fn snapshot_from_rows(
    metadata: &PgRow,
    asset_rows: &[PgRow],
    edge_rows: &[PgRow],
) -> Result<InventorySnapshot, ControlPlaneError> {
    let sources: Vec<AssetSource> = serde_json::from_value(metadata.try_get::<Value, _>("sources")?)
        .map_err(|_| invalid_stored_inventory("sources"))?;
    let snapshot = InventorySnapshot {
        id: metadata.try_get("id")?,
        tenant_id: TenantId::from_uuid(metadata.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(metadata.try_get("cluster_id")?),
        sources,
        observed_at: metadata.try_get("observed_at")?,
        freshness_seconds: to_u64(metadata.try_get("freshness_seconds")?, "snapshot freshness")?,
        partial: metadata.try_get("partial")?,
        content_hash: metadata.try_get("content_hash")?,
        assets: asset_rows.iter().map(asset_from_row).collect::<Result<Vec<_>, _>>()?,
        edges: edge_rows.iter().map(edge_from_row).collect::<Result<Vec<_>, _>>()?,
    };
    verify_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn asset_from_row(row: &PgRow) -> Result<NormalizedAsset, ControlPlaneError> {
    let kind = row.try_get::<String, _>("kind")?.parse()?;
    let external_key = row.try_get::<String, _>("external_key")?;
    let asset = NormalizedAsset {
        id: AssetSnapshotId::from_uuid(row.try_get("id")?),
        key: AssetKey::new(kind, external_key)?,
        display_name: row.try_get("display_name")?,
        source: row.try_get::<String, _>("source")?.parse()?,
        attributes: row.try_get("attributes")?,
        observed_at: row.try_get("observed_at")?,
        freshness_seconds: to_u64(row.try_get("freshness_seconds")?, "asset freshness")?,
        partial: row.try_get("partial")?,
        content_hash: row.try_get("content_hash")?,
    };
    Ok(asset)
}

fn edge_from_row(row: &PgRow) -> Result<NormalizedTopologyEdge, ControlPlaneError> {
    Ok(NormalizedTopologyEdge {
        id: TopologyEdgeId::from_uuid(row.try_get("id")?),
        from: AssetKey::parse_canonical(&row.try_get::<String, _>("from_key")?)?,
        to: AssetKey::parse_canonical(&row.try_get::<String, _>("to_key")?)?,
        relation: row.try_get::<String, _>("relation")?.parse()?,
        source: row.try_get::<String, _>("source")?.parse()?,
        observed_at: row.try_get("observed_at")?,
        freshness_seconds: to_u64(row.try_get("freshness_seconds")?, "edge freshness")?,
        partial: row.try_get("partial")?,
        content_hash: row.try_get("content_hash")?,
    })
}

fn diff_from_row(row: &PgRow) -> Result<TopologyDiff, ControlPlaneError> {
    let content_hash = row
        .try_get::<Option<String>, _>("content_hash")?
        .ok_or_else(|| invalid_stored_inventory("topology diff content hash"))?;
    let diff = TopologyDiff {
        id: row.try_get("id")?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        previous_snapshot_id: row.try_get("previous_snapshot_id")?,
        current_snapshot_id: row.try_get("current_snapshot_id")?,
        previous_observed_at: row.try_get("previous_observed_at")?,
        current_observed_at: row.try_get("current_observed_at")?,
        additions: parse_diff_entries(row.try_get("additions")?)?,
        removals: parse_diff_entries(row.try_get("removals")?)?,
        changes: parse_diff_entries(row.try_get("changes")?)?,
        partial: row.try_get("partial")?,
        suppressed_removals: u32::try_from(row.try_get::<i32, _>("suppressed_removals")?)
            .map_err(|_| invalid_stored_inventory("suppressed removal count"))?,
        content_hash,
        created_at: row.try_get("created_at")?,
    };
    verify_diff(&diff)?;
    Ok(diff)
}

fn parse_diff_entries(value: Value) -> Result<Vec<TopologyDiffEntry>, ControlPlaneError> {
    serde_json::from_value(value).map_err(|_| invalid_stored_inventory("topology diff entries"))
}

fn json_value<T: serde::Serialize>(value: &T, field: &str) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", format!("{field} cannot be serialized")))
}

fn to_i64(value: u64, field: &str) -> Result<i64, ControlPlaneError> {
    i64::try_from(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", format!("{field} exceeds the supported range")))
}

fn to_u64(value: i64, field: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| invalid_stored_inventory(field))
}

const SNAPSHOT_METADATA: &str = "SELECT id, tenant_id, cluster_id, sources, observed_at,
    freshness_seconds, partial, content_hash
    FROM asset_inventory_snapshots
    WHERE id = $1 AND tenant_id = $2";

const ASSET_ROWS: &str = "SELECT id, kind, external_key, display_name, source, attributes,
    observed_at, freshness_seconds, partial, content_hash
    FROM asset_snapshots
    WHERE inventory_snapshot_id = $1 AND tenant_id = $2
    ORDER BY kind, external_key";

const EDGE_ROWS: &str = "SELECT id, from_key, to_key, relation, source, observed_at,
    freshness_seconds, partial, content_hash
    FROM topology_edges
    WHERE inventory_snapshot_id = $1 AND tenant_id = $2
    ORDER BY from_key, to_key, relation";

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::assets::AssetKind;
    use crate::assets::AssetObservation;
    use crate::assets::TopologyObservation;
    use crate::assets::TopologyRelation;

    #[test]
    fn scope_rejects_tenant_and_cross_cluster_access() {
        let tenant = TenantId::new();
        let allowed = ClusterId::new();
        let auth = AuthContext {
            tenant_id: tenant,
            subject: "operator".to_owned(),
            clusters: BTreeSet::from([allowed]),
            roles: BTreeSet::new(),
        };
        assert!(enforce_scope(&auth, tenant, allowed).is_ok());
        assert!(matches!(
            enforce_scope(&auth, tenant, ClusterId::new()),
            Err(ControlPlaneError::Forbidden {
                code: "cluster_not_allowed",
                ..
            })
        ));
        assert!(matches!(
            enforce_scope(&auth, TenantId::new(), allowed),
            Err(ControlPlaneError::Forbidden {
                code: "tenant_mismatch",
                ..
            })
        ));
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_inventory_round_trip_and_diff() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'local', 'test', 'test', 'asset-topology-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster.as_uuid())
        .bind(tenant.to_string())
        .bind(format!("asset-topology-{cluster}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
        let auth = AuthContext {
            tenant_id: tenant,
            subject: "asset-topology-test".to_owned(),
            clusters: BTreeSet::from([cluster]),
            roles: BTreeSet::new(),
        };
        let observed_at = Utc::now();
        let cluster_asset = AssetObservation {
            kind: AssetKind::Cluster,
            external_key: cluster.to_string(),
            display_name: "Test cluster".to_owned(),
            source: AssetSource::Topology,
            attributes: json!({"mode": "controller"}),
            observed_at,
            freshness_seconds: 0,
            partial: false,
        };
        let broker_asset = AssetObservation {
            kind: AssetKind::Broker,
            external_key: "broker-a".to_owned(),
            display_name: "Broker A".to_owned(),
            source: AssetSource::Admin,
            attributes: json!({"role": "master"}),
            observed_at,
            freshness_seconds: 1,
            partial: false,
        };
        let first_request = IngestInventoryRequest {
            cluster_id: cluster,
            observed_at,
            partial: false,
            assets: vec![cluster_asset.clone(), broker_asset.clone()],
            edges: vec![TopologyObservation {
                from: AssetKey::new(AssetKind::Cluster, cluster.to_string()).expect("cluster key"),
                to: AssetKey::new(AssetKind::Broker, "broker-a").expect("broker key"),
                relation: TopologyRelation::Contains,
                source: AssetSource::Topology,
                observed_at,
                freshness_seconds: 1,
                partial: false,
            }],
        };
        let (first, first_diff) = repository
            .persist_inventory_snapshot(&auth, &first_request)
            .await
            .expect("first snapshot");
        assert_eq!(first_diff.additions.len(), 3);
        let loaded = repository
            .inventory_snapshot(&auth, first.id)
            .await
            .expect("snapshot round trip");
        assert_eq!(loaded.content_hash, first.content_hash);

        let mut changed_broker = broker_asset;
        changed_broker.attributes = json!({"role": "replica"});
        let topic_asset = AssetObservation {
            kind: AssetKind::Topic,
            external_key: "orders".to_owned(),
            display_name: "orders".to_owned(),
            source: AssetSource::Mcp,
            attributes: json!({"queues": 8}),
            observed_at,
            freshness_seconds: 2,
            partial: false,
        };
        let second_request = IngestInventoryRequest {
            cluster_id: cluster,
            observed_at,
            partial: false,
            assets: vec![cluster_asset, changed_broker, topic_asset],
            edges: Vec::new(),
        };
        let (_, second_diff) = repository
            .persist_inventory_snapshot(&auth, &second_request)
            .await
            .expect("second snapshot");
        assert_eq!(second_diff.additions.len(), 1);
        assert_eq!(second_diff.removals.len(), 1);
        assert_eq!(second_diff.changes.len(), 1);

        for table in [
            "topology_diffs",
            "topology_edges",
            "asset_snapshots",
            "asset_inventory_snapshots",
        ] {
            sqlx::query(&format!("DELETE FROM {table} WHERE tenant_id = $1 AND cluster_id = $2"))
                .bind(tenant.as_uuid())
                .bind(cluster.as_uuid())
                .execute(&repository.pool)
                .await
                .expect("test data cleanup");
        }
        sqlx::query("DELETE FROM clusters WHERE id = $1")
            .bind(cluster.as_uuid())
            .execute(&repository.pool)
            .await
            .expect("test cluster cleanup");
    }
}
