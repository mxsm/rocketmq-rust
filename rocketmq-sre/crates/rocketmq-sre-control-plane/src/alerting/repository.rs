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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::AlertEvent;
use rocketmq_sre_contracts::AlertStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentRelationId;
use rocketmq_sre_contracts::NotificationChannel;
use rocketmq_sre_contracts::NotificationDeliveryId;
use rocketmq_sre_contracts::NotificationTargetId;
use rocketmq_sre_contracts::ResourceRef;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimelineEvent;
use rocketmq_sre_contracts::TimelineEventId;
use rocketmq_sre_contracts::WorkflowActor;
use rocketmq_sre_core::correlation::CorrelationCandidate;
use rocketmq_sre_core::correlation::ResourceGraph;
use rocketmq_sre_core::correlation::select_candidate;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::model::ClusterIncidentHealth;
use super::model::CorrelationResult;
use super::model::IncidentTopologyEdge;
use super::model::IncidentTopologyNode;
use super::model::IncidentTopologyView;
use super::model::NotificationClaim;
use super::model::notification_summary;
use super::model::resource_kind_name;
use super::model::severity_name;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const MAX_TOPOLOGY_EDGES: i64 = 2_001;
const MAX_TOPOLOGY_NODES: i64 = 1_001;
const MAX_CHANGE_EVENTS: i64 = 64;
const MAX_NOTIFICATION_TARGETS: i64 = 8;
const NOTIFICATION_MAX_ATTEMPTS: u16 = 4;

impl PostgresRepository {
    pub(super) async fn correlate_alert(
        &self,
        auth: &AuthContext,
        event: &AlertEvent,
        persisted_alert_id: rocketmq_sre_contracts::AlertEventId,
        correlation_id: CorrelationId,
        public_base_url: &str,
    ) -> Result<CorrelationResult, ControlPlaneError> {
        enforce_auth_scope(auth, event.tenant_id, event.cluster_id)?;
        let mut transaction = self.pool.begin().await?;
        ensure_cluster_scope(&mut transaction, auth.tenant_id, event.cluster_id).await?;

        let exact_key = correlation_key_digest(event);
        let lock_scope = format!(
            "{}:{}:{}:{}",
            event.tenant_id,
            event.cluster_id,
            event.symptom_family.as_str(),
            event.correlation_key.window_start.timestamp()
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_scope)
            .execute(&mut *transaction)
            .await?;

        let latest_link = linked_incident(&mut transaction, event, persisted_alert_id).await?;
        let exact_incident = exact_incident(&mut transaction, event, &exact_key).await?;
        let mut recurrence_from = None;
        let selected = match latest_link.or(exact_incident) {
            Some(candidate) if candidate.terminal && event.status == AlertStatus::Firing => {
                recurrence_from = Some(candidate.id);
                None
            }
            Some(candidate) => Some(candidate),
            None => topology_candidate(&mut transaction, event).await?,
        };

        let owner_resolution = resolve_owner(&mut transaction, event).await?;
        let (incident_id, created) = match selected {
            Some(candidate) => (candidate.id, false),
            None => {
                let id = IncidentId::new();
                let correlation_key = if recurrence_from.is_some() {
                    format!("{exact_key}:recurrence:{}", event.id)
                } else {
                    exact_key.clone()
                };
                insert_correlated_incident(
                    &mut transaction,
                    auth,
                    event,
                    id,
                    &correlation_key,
                    &owner_resolution.owner,
                    recurrence_from,
                    correlation_id,
                )
                .await?;
                (id, true)
            }
        };

        sqlx::query(
            "INSERT INTO incident_alerts (
                incident_id, alert_id, tenant_id, cluster_id, linked_at
             ) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (incident_id, alert_id) DO NOTHING",
        )
        .bind(incident_id.as_uuid())
        .bind(persisted_alert_id.as_uuid())
        .bind(event.tenant_id.as_uuid())
        .bind(event.cluster_id.as_uuid())
        .bind(event.received_at)
        .execute(&mut *transaction)
        .await?;

        let timeline_id = deterministic_uuid(&format!(
            "alert-occurrence:{}:{}:{}",
            persisted_alert_id, event.sequence, incident_id
        ));
        append_alert_timeline(
            &mut transaction,
            auth,
            event,
            incident_id,
            persisted_alert_id,
            TimelineEventId::from_uuid(timeline_id),
            correlation_id,
        )
        .await?;
        append_recent_changes(&mut transaction, auth, event, incident_id, correlation_id).await?;

        let occurrence_count = count_incident_occurrences(&mut transaction, incident_id).await?;
        update_incident_aggregate(
            &mut transaction,
            event,
            incident_id,
            &owner_resolution.owner,
            occurrence_count,
        )
        .await?;

        if let Some(previous) = recurrence_from {
            insert_recurrence_relation(&mut transaction, event, previous, incident_id, auth, correlation_id).await?;
        }

        enqueue_incident_notifications(
            &mut transaction,
            event,
            incident_id,
            &owner_resolution.target_ids,
            public_base_url,
        )
        .await?;
        transaction.commit().await?;

        Ok(CorrelationResult {
            incident_id,
            created,
            recurrence: recurrence_from.is_some(),
            occurrence_count,
            owner: owner_resolution.owner,
            severity: event.severity,
        })
    }

    pub(super) async fn incident_timeline_for_alerting(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<Vec<TimelineEvent>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT event_id, tenant_id, cluster_id, investigation_id, incident_id,
                    event_type, summary, details, correlation_id, actor_subject,
                    actor_display_name, occurred_at
             FROM incident_timeline
             WHERE tenant_id = $1 AND incident_id = $2
             ORDER BY sequence_id",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(incident_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        let timeline = rows
            .iter()
            .map(|row| {
                let cluster_id = ClusterId::from_uuid(row.try_get("cluster_id")?);
                if !auth.clusters.contains(&cluster_id) {
                    return Err(ControlPlaneError::forbidden(
                        "cluster_not_allowed",
                        "incident is outside the authenticated cluster scope",
                    ));
                }
                Ok(TimelineEvent {
                    id: TimelineEventId::from_uuid(row.try_get("event_id")?),
                    tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
                    cluster_id,
                    investigation_id: row
                        .try_get::<Option<Uuid>, _>("investigation_id")?
                        .map(rocketmq_sre_contracts::InvestigationId::from_uuid),
                    incident_id: row
                        .try_get::<Option<Uuid>, _>("incident_id")?
                        .map(IncidentId::from_uuid),
                    event_type: row.try_get("event_type")?,
                    summary: row.try_get("summary")?,
                    details: row.try_get("details")?,
                    correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?),
                    actor: WorkflowActor {
                        subject: row.try_get("actor_subject")?,
                        display_name: row.try_get("actor_display_name")?,
                    },
                    occurred_at: row.try_get("occurred_at")?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if timeline.is_empty() {
            ensure_incident_visible(&self.pool, auth, incident_id).await?;
        }
        Ok(timeline)
    }

    pub(super) async fn append_incident_note(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        note: &str,
        correlation_id: CorrelationId,
    ) -> Result<TimelineEvent, ControlPlaneError> {
        let cluster_id = ensure_incident_visible(&self.pool, auth, incident_id).await?;
        let event_id = TimelineEventId::new();
        let occurred_at = Utc::now();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO incident_timeline (
                event_id, tenant_id, cluster_id, incident_id, event_type,
                summary, details, correlation_id, actor_subject, occurred_at
             ) VALUES (
                $1, $2, $3, $4, 'operator_note', $5, '{}', $6, $7, $8
             )",
        )
        .bind(event_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(note)
        .bind(correlation_id.as_uuid())
        .bind(&auth.subject)
        .bind(occurred_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO workflow_events (
                event_id, tenant_id, cluster_id, aggregate_type, aggregate_id,
                event_type, event_payload, correlation_id, occurred_at
             ) VALUES (
                $1, $2, $3, 'incident', $4, 'incident_note_added',
                $5, $6, $7
             )",
        )
        .bind(event_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(json!({"timeline_event_id": event_id}))
        .bind(correlation_id.as_uuid())
        .bind(occurred_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(TimelineEvent {
            id: event_id,
            tenant_id: auth.tenant_id,
            cluster_id,
            investigation_id: None,
            incident_id: Some(incident_id),
            event_type: "operator_note".into(),
            summary: note.to_owned(),
            details: json!({}),
            correlation_id,
            actor: WorkflowActor {
                subject: auth.subject.clone(),
                display_name: None,
            },
            occurred_at,
        })
    }

    pub(super) async fn incident_topology_for_alerting(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<IncidentTopologyView, ControlPlaneError> {
        let cluster_id = ensure_incident_visible(&self.pool, auth, incident_id).await?;
        let alert_rows = sqlx::query(
            "SELECT a.affected_resource
             FROM alert_events a
             JOIN incident_alerts ia ON ia.alert_id = a.id
             WHERE ia.incident_id = $1 AND ia.tenant_id = $2 AND ia.cluster_id = $3",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        let mut alert_counts = BTreeMap::<String, u32>::new();
        for row in alert_rows {
            let resource: ResourceRef = serde_json::from_value(row.try_get("affected_resource")?)
                .map_err(|_| ControlPlaneError::configuration("stored alert resource is invalid"))?;
            *alert_counts.entry(canonical_resource(&resource)).or_default() += 1;
        }

        let snapshot_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM asset_inventory_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2
             ORDER BY observed_at DESC, id DESC LIMIT 1",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(snapshot_id) = snapshot_id else {
            return Ok(IncidentTopologyView {
                schema_version: "rocketmq-sre.incident-topology.v1",
                incident_id,
                nodes: alert_counts
                    .iter()
                    .map(|(key, count)| IncidentTopologyNode {
                        key: key.clone(),
                        kind: key.split_once(':').map_or("unknown", |value| value.0).to_owned(),
                        display_name: key.clone(),
                        alert_count: *count,
                    })
                    .collect(),
                edges: Vec::new(),
                partial: true,
                warnings: vec!["topology_snapshot_missing".to_owned()],
            });
        };

        let edge_rows = sqlx::query(
            "SELECT from_key, to_key, relation
             FROM topology_edges
             WHERE tenant_id = $1 AND cluster_id = $2 AND inventory_snapshot_id = $3
             ORDER BY from_key, to_key, relation
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(snapshot_id)
        .bind(MAX_TOPOLOGY_EDGES)
        .fetch_all(&self.pool)
        .await?;
        let mut graph = ResourceGraph::default();
        let mut all_edges = Vec::new();
        for row in &edge_rows {
            let from: String = row.try_get("from_key")?;
            let to: String = row.try_get("to_key")?;
            graph.add_edge(from.clone(), to.clone());
            all_edges.push(IncidentTopologyEdge {
                from,
                to,
                relation: row.try_get("relation")?,
            });
        }

        let node_rows = sqlx::query(
            "SELECT kind, external_key, display_name
             FROM asset_snapshots
             WHERE tenant_id = $1 AND cluster_id = $2 AND inventory_snapshot_id = $3
             ORDER BY kind, external_key
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(snapshot_id)
        .bind(MAX_TOPOLOGY_NODES)
        .fetch_all(&self.pool)
        .await?;
        let mut included = BTreeSet::new();
        let mut nodes = Vec::new();
        for row in &node_rows {
            let kind: String = row.try_get("kind")?;
            let external_key: String = row.try_get("external_key")?;
            let key = format!("{kind}:{external_key}");
            let near_alert = alert_counts
                .keys()
                .any(|alert| graph.distance_within(alert, &key, 3).is_some());
            if near_alert {
                included.insert(key.clone());
                nodes.push(IncidentTopologyNode {
                    alert_count: alert_counts.get(&key).copied().unwrap_or(0),
                    key,
                    kind,
                    display_name: row.try_get("display_name")?,
                });
            }
        }
        for (key, count) in alert_counts {
            if included.insert(key.clone()) {
                nodes.push(IncidentTopologyNode {
                    kind: key.split_once(':').map_or("unknown", |value| value.0).to_owned(),
                    display_name: key.clone(),
                    key,
                    alert_count: count,
                });
            }
        }
        nodes.sort_by(|left, right| left.key.cmp(&right.key));
        let edges = all_edges
            .into_iter()
            .filter(|edge| included.contains(&edge.from) && included.contains(&edge.to))
            .collect();
        let partial = edge_rows.len() == MAX_TOPOLOGY_EDGES as usize || node_rows.len() == MAX_TOPOLOGY_NODES as usize;
        Ok(IncidentTopologyView {
            schema_version: "rocketmq-sre.incident-topology.v1",
            incident_id,
            nodes,
            edges,
            partial,
            warnings: partial
                .then(|| "topology_projection_truncated".to_owned())
                .into_iter()
                .collect(),
        })
    }

    pub(super) async fn cluster_incident_health(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<ClusterIncidentHealth, ControlPlaneError> {
        enforce_auth_scope(auth, auth.tenant_id, cluster_id)?;
        let row = sqlx::query(
            "SELECT
                COUNT(*) FILTER (WHERE status NOT IN ('resolved', 'escalated')) AS active_incidents,
                COUNT(*) FILTER (
                    WHERE status NOT IN ('resolved', 'escalated') AND severity = 'critical'
                ) AS critical_incidents,
                COUNT(*) FILTER (
                    WHERE status NOT IN ('resolved', 'escalated') AND owner_name = 'unassigned'
                ) AS unassigned_incidents,
                MAX(last_alert_at) AS last_alert_at
             FROM sre_incidents
             WHERE tenant_id = $1 AND cluster_id = $2",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        let active = to_u32(row.try_get::<i64, _>("active_incidents")?, "active incident count")?;
        let critical = to_u32(row.try_get::<i64, _>("critical_incidents")?, "critical incident count")?;
        let unassigned = to_u32(
            row.try_get::<i64, _>("unassigned_incidents")?,
            "unassigned incident count",
        )?;
        let status = if critical > 0 {
            "critical"
        } else if active > 0 {
            "degraded"
        } else {
            "healthy"
        };
        Ok(ClusterIncidentHealth {
            schema_version: "rocketmq-sre.cluster-incident-health.v1",
            cluster_id,
            status,
            active_incidents: active,
            critical_incidents: critical,
            unassigned_incidents: unassigned,
            last_alert_at: row.try_get("last_alert_at")?,
            observed_at: Utc::now(),
        })
    }

    pub(super) async fn enqueue_notification_test(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        incident_id: IncidentId,
        target_id: NotificationTargetId,
        public_base_url: &str,
    ) -> Result<(NotificationDeliveryId, bool, String, String), ControlPlaneError> {
        enforce_auth_scope(auth, auth.tenant_id, cluster_id)?;
        let row = sqlx::query(
            "SELECT i.title, t.enabled
             FROM sre_incidents i
             JOIN notification_targets t ON t.id = $4
             WHERE i.id = $1 AND i.tenant_id = $2 AND i.cluster_id = $3
               AND t.tenant_id = $2 AND (t.cluster_id IS NULL OR t.cluster_id = $3)",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        if !row.try_get::<bool, _>("enabled")? {
            return Err(ControlPlaneError::conflict("notification target is disabled"));
        }
        let delivery_id = NotificationDeliveryId::new();
        let summary = "RocketMQ SRE notification channel test".to_owned();
        let deep_link = incident_deep_link(public_base_url, incident_id);
        let delivery_key = format!("test:{incident_id}:{target_id}:{}", Utc::now().timestamp_millis());
        let result = sqlx::query(
            "INSERT INTO notification_outbox (
                id, target_id, tenant_id, cluster_id, incident_id, delivery_key,
                status, sanitized_summary, deep_link, attempt_count,
                next_attempt_at, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, 0, NOW(), NOW())
             ON CONFLICT (tenant_id, delivery_key) DO NOTHING",
        )
        .bind(delivery_id.as_uuid())
        .bind(target_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(delivery_key)
        .bind(&summary)
        .bind(&deep_link)
        .execute(&self.pool)
        .await?;
        Ok((delivery_id, result.rows_affected() == 1, summary, deep_link))
    }

    pub(super) async fn claim_notifications(&self, limit: u16) -> Result<Vec<NotificationClaim>, ControlPlaneError> {
        let claim_token = Uuid::new_v4();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE notification_outbox
             SET status = 'retry_scheduled', claim_token = NULL, claimed_at = NULL,
                 next_attempt_at = NOW()
             WHERE status = 'delivering' AND claimed_at < NOW() - INTERVAL '2 minutes'",
        )
        .execute(&mut *transaction)
        .await?;
        let rows = sqlx::query(
            "WITH candidates AS (
                SELECT outbox.id
                FROM notification_outbox outbox
                JOIN notification_targets target ON target.id = outbox.target_id
                WHERE outbox.status IN ('pending', 'retry_scheduled')
                  AND target.enabled
                  AND COALESCE(outbox.next_attempt_at, outbox.created_at) <= NOW()
                ORDER BY outbox.created_at, outbox.id
                FOR UPDATE SKIP LOCKED
                LIMIT $1
             )
             UPDATE notification_outbox outbox
             SET status = 'delivering', claim_token = $2, claimed_at = NOW()
             FROM candidates
             WHERE outbox.id = candidates.id
             RETURNING outbox.id, outbox.target_id, outbox.sanitized_summary,
                       outbox.deep_link, outbox.attempt_count, outbox.incident_id",
        )
        .bind(i64::from(limit.min(32)))
        .bind(claim_token)
        .fetch_all(&mut *transaction)
        .await?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let target_id: Uuid = row.try_get("target_id")?;
            let target = sqlx::query(
                "SELECT channel, endpoint, secret_reference, enabled
                 FROM notification_targets WHERE id = $1",
            )
            .bind(target_id)
            .fetch_one(&mut *transaction)
            .await?;
            debug_assert!(target.try_get::<bool, _>("enabled")?);
            claims.push(NotificationClaim {
                delivery_id: NotificationDeliveryId::from_uuid(row.try_get("id")?),
                claim_token,
                channel: parse_channel(target.try_get("channel")?)?,
                endpoint: target.try_get("endpoint")?,
                secret_reference: target.try_get("secret_reference")?,
                sanitized_summary: row.try_get("sanitized_summary")?,
                deep_link: row.try_get("deep_link")?,
                attempt_count: to_u16(row.try_get("attempt_count")?, "notification attempt count")?,
                incident_id: IncidentId::from_uuid(row.try_get("incident_id")?),
            });
        }
        transaction.commit().await?;
        Ok(claims)
    }

    pub(super) async fn finish_notification(
        &self,
        claim: &NotificationClaim,
        result: Result<(), &'static str>,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let next_attempt = claim.attempt_count.saturating_add(1);
        let (status, next_attempt_at, error_code, delivered_at) = match result {
            Ok(()) => ("delivered", None, None, Some(Utc::now())),
            Err(code) if next_attempt < NOTIFICATION_MAX_ATTEMPTS => {
                let delay_seconds = 30_i64.saturating_mul(1_i64 << next_attempt.min(5));
                (
                    "retry_scheduled",
                    Some(Utc::now() + Duration::seconds(delay_seconds.min(900))),
                    Some(code),
                    None,
                )
            }
            Err(code) => ("failed", None, Some(code), None),
        };
        let updated = sqlx::query(
            "UPDATE notification_outbox
             SET status = $3, attempt_count = $4, next_attempt_at = $5,
                 last_error_code = $6, delivered_at = $7,
                 claim_token = NULL, claimed_at = NULL
             WHERE id = $1 AND claim_token = $2 AND status = 'delivering'",
        )
        .bind(claim.delivery_id.as_uuid())
        .bind(claim.claim_token)
        .bind(status)
        .bind(i32::from(next_attempt))
        .bind(next_attempt_at)
        .bind(error_code)
        .bind(delivered_at)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 1 {
            let event_id = deterministic_uuid(&format!("notification:{}:{}", claim.delivery_id, next_attempt));
            sqlx::query(
                "INSERT INTO incident_timeline (
                    event_id, tenant_id, cluster_id, incident_id, event_type,
                    summary, details, correlation_id, actor_subject, occurred_at
                 )
                 SELECT $1, tenant_id, cluster_id, incident_id, 'notification_delivery',
                        $2, $3, $4, 'system:notification-outbox', NOW()
                 FROM notification_outbox WHERE id = $5
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(event_id)
            .bind(if status == "delivered" {
                "Notification delivered"
            } else {
                "Notification delivery deferred"
            })
            .bind(json!({
                "delivery_id": claim.delivery_id,
                "status": status,
                "attempt_count": next_attempt,
                "error_code": error_code,
            }))
            .bind(deterministic_uuid(&format!(
                "notification-correlation:{}",
                claim.delivery_id
            )))
            .bind(claim.delivery_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct IncidentCandidate {
    id: IncidentId,
    terminal: bool,
}

#[derive(Clone, Debug)]
struct OwnerResolution {
    owner: String,
    target_ids: Vec<NotificationTargetId>,
}

async fn linked_incident(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    alert_id: rocketmq_sre_contracts::AlertEventId,
) -> Result<Option<IncidentCandidate>, ControlPlaneError> {
    let row = sqlx::query(
        "SELECT i.id, i.status
         FROM incident_alerts ia
         JOIN sre_incidents i ON i.id = ia.incident_id
         WHERE ia.alert_id = $1 AND ia.tenant_id = $2 AND ia.cluster_id = $3
         ORDER BY ia.linked_at DESC, ia.incident_id DESC
         LIMIT 1",
    )
    .bind(alert_id.as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| incident_candidate(&row)).transpose()
}

async fn exact_incident(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    exact_key: &str,
) -> Result<Option<IncidentCandidate>, ControlPlaneError> {
    let row = sqlx::query(
        "SELECT id, status FROM sre_incidents
         WHERE tenant_id = $1 AND cluster_id = $2 AND alert_correlation_key = $3
         LIMIT 1",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(exact_key)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| incident_candidate(&row)).transpose()
}

async fn topology_candidate(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
) -> Result<Option<IncidentCandidate>, ControlPlaneError> {
    let rows = sqlx::query(
        "SELECT DISTINCT ON (i.id)
                i.id, i.status, i.last_alert_at, a.affected_resource
         FROM sre_incidents i
         JOIN incident_alerts ia ON ia.incident_id = i.id
         JOIN alert_events a ON a.id = ia.alert_id
         WHERE i.tenant_id = $1 AND i.cluster_id = $2
           AND i.symptom_family = $3
           AND i.status NOT IN ('resolved', 'escalated')
           AND i.last_alert_at >= $4
           AND i.last_alert_at < $5
         ORDER BY i.id, a.last_occurred_at DESC
         LIMIT 128",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(event.symptom_family.as_str())
    .bind(event.correlation_key.window_start)
    .bind(event.correlation_key.window_start + Duration::seconds(i64::from(event.correlation_key.window_seconds)))
    .fetch_all(&mut **transaction)
    .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let edge_rows = sqlx::query(
        "SELECT from_key, to_key
         FROM topology_edges
         WHERE tenant_id = $1 AND cluster_id = $2
           AND inventory_snapshot_id = (
                SELECT id FROM asset_inventory_snapshots
                WHERE tenant_id = $1 AND cluster_id = $2
                ORDER BY observed_at DESC, id DESC LIMIT 1
           )
         ORDER BY from_key, to_key
         LIMIT $3",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(MAX_TOPOLOGY_EDGES)
    .fetch_all(&mut **transaction)
    .await?;
    let mut graph = ResourceGraph::default();
    for row in edge_rows {
        graph.add_edge(
            row.try_get::<String, _>("from_key")?,
            row.try_get::<String, _>("to_key")?,
        );
    }
    let event_resource = canonical_resource(&event.affected_resource);
    let mut mapped = Vec::new();
    for row in &rows {
        let resource: ResourceRef = serde_json::from_value(row.try_get("affected_resource")?)
            .map_err(|_| ControlPlaneError::configuration("stored alert resource is invalid"))?;
        let distance = graph.distance_within(&event_resource, &canonical_resource(&resource), 3);
        if distance.is_some() {
            mapped.push(CorrelationCandidate {
                incident_key: row.try_get::<Uuid, _>("id")?.to_string(),
                exact_key: false,
                topology_distance: distance,
                last_occurred_at_epoch: row
                    .try_get::<Option<chrono::DateTime<Utc>>, _>("last_alert_at")?
                    .map_or(0, |value| value.timestamp()),
                terminal: false,
            });
        }
    }
    let Some(selected) = select_candidate(&mapped) else {
        return Ok(None);
    };
    Ok(Some(IncidentCandidate {
        id: selected
            .incident_key
            .parse()
            .map_err(|_| ControlPlaneError::configuration("stored incident identifier is invalid"))?,
        terminal: false,
    }))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the correlated incident insert records the complete immutable identity and workflow scope"
)]
async fn insert_correlated_incident(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    event: &AlertEvent,
    incident_id: IncidentId,
    correlation_key: &str,
    owner: &str,
    recurrence_from: Option<IncidentId>,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    let resource = canonical_resource(&event.affected_resource);
    let (sla_ack_due_at, sla_resolve_due_at) = match event.severity {
        rocketmq_sre_contracts::AlertSeverity::Critical => (
            event.occurred_at + Duration::minutes(15),
            event.occurred_at + Duration::hours(4),
        ),
        rocketmq_sre_contracts::AlertSeverity::Error => (
            event.occurred_at + Duration::minutes(30),
            event.occurred_at + Duration::hours(8),
        ),
        rocketmq_sre_contracts::AlertSeverity::Warning => (
            event.occurred_at + Duration::hours(2),
            event.occurred_at + Duration::hours(24),
        ),
        rocketmq_sre_contracts::AlertSeverity::Info => (
            event.occurred_at + Duration::hours(8),
            event.occurred_at + Duration::hours(72),
        ),
    };
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, resource, symptom_family,
            fingerprint, status, workflow_checkpoint, created_by_subject,
            created_at, updated_at, alert_correlation_key, severity, owner_name,
            occurrence_count, last_alert_at, reopened_from_incident_id,
            sla_ack_due_at, sla_resolve_due_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'new', $8, $9,
            $10, $10, $11, $12, $13, 0, $10, $14, $15, $16
         )",
    )
    .bind(incident_id.as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(notification_summary(event))
    .bind(&resource)
    .bind(event.symptom_family.as_str())
    .bind(&event.fingerprint)
    .bind(json!({
        "source": "alert_correlation",
        "correlation_id": correlation_id,
        "effective_access": "read_only",
    }))
    .bind(&auth.subject)
    .bind(event.occurred_at)
    .bind(correlation_key)
    .bind(severity_name(event.severity))
    .bind(owner)
    .bind(recurrence_from.map(IncidentId::as_uuid))
    .bind(sla_ack_due_at)
    .bind(sla_resolve_due_at)
    .execute(&mut **transaction)
    .await?;
    let timeline_id = deterministic_uuid(&format!("incident-created:{incident_id}"));
    insert_timeline(
        transaction,
        event,
        incident_id,
        timeline_id,
        "incident_created",
        if recurrence_from.is_some() {
            "Recurring incident created from a new alert occurrence"
        } else {
            "Incident created from correlated alert"
        },
        json!({
            "source": "alert_correlation",
            "owner": owner,
            "severity": severity_name(event.severity),
            "reopened_from_incident_id": recurrence_from,
        }),
        &auth.subject,
        correlation_id,
        event.received_at,
    )
    .await?;
    append_workflow_event(
        transaction,
        event,
        incident_id,
        timeline_id,
        "incident_created",
        json!({
            "owner": owner,
            "severity": severity_name(event.severity),
            "recurrence": recurrence_from.is_some(),
        }),
        correlation_id,
    )
    .await
}

async fn append_alert_timeline(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    event: &AlertEvent,
    incident_id: IncidentId,
    alert_id: rocketmq_sre_contracts::AlertEventId,
    timeline_id: TimelineEventId,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    insert_timeline(
        transaction,
        event,
        incident_id,
        timeline_id.as_uuid(),
        "alert",
        "Correlated alert occurrence",
        json!({
            "alert_id": alert_id,
            "source": event.source,
            "status": event.status,
            "severity": event.severity,
            "resource": event.affected_resource,
            "symptom_family": event.symptom_family,
            "sequence": event.sequence,
            "evidence_ids": event.evidence_ids,
        }),
        &auth.subject,
        correlation_id,
        event.occurred_at,
    )
    .await?;
    append_workflow_event(
        transaction,
        event,
        incident_id,
        timeline_id.as_uuid(),
        "incident_alert_correlated",
        json!({
            "alert_id": alert_id,
            "sequence": event.sequence,
            "severity": event.severity,
        }),
        correlation_id,
    )
    .await
}

async fn append_recent_changes(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    event: &AlertEvent,
    incident_id: IncidentId,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    let window_start = event.occurred_at - Duration::minutes(30);
    let alert_changes = sqlx::query(
        "SELECT id, source, affected_resource, last_occurred_at AS occurred_at
         FROM alert_events
         WHERE tenant_id = $1 AND cluster_id = $2
           AND source IN ('kubernetes_event', 'deployment')
           AND last_occurred_at >= $3 AND first_occurred_at <= $4
         ORDER BY last_occurred_at, id
         LIMIT $5",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(window_start)
    .bind(event.occurred_at)
    .bind(MAX_CHANGE_EVENTS)
    .fetch_all(&mut **transaction)
    .await?;
    for row in alert_changes {
        let change_id: Uuid = row.try_get("id")?;
        let source: String = row.try_get("source")?;
        let resource: ResourceRef = serde_json::from_value(row.try_get("affected_resource")?)
            .map_err(|_| ControlPlaneError::configuration("stored change resource is invalid"))?;
        insert_timeline(
            transaction,
            event,
            incident_id,
            deterministic_uuid(&format!("incident-change:{incident_id}:{change_id}")),
            if source == "deployment" {
                "deployment_change"
            } else {
                "kubernetes_event"
            },
            "Recent change correlated to incident",
            json!({
                "source_event_id": change_id,
                "resource": resource,
                "lookback_minutes": 30,
            }),
            "system:correlation",
            correlation_id,
            row.try_get("occurred_at")?,
        )
        .await?;
    }

    let evidence_changes = sqlx::query(
        "SELECT id, source, resource, observed_at
         FROM evidence_snapshots
         WHERE tenant_id = $1 AND cluster_id = $2
           AND observed_at BETWEEN $3 AND $4
           AND (
                source IN ('kubernetes', 'deployment_state', 'change_timeline')
                OR resource LIKE '%change_timeline%'
                OR resource LIKE '%certificates%'
           )
         ORDER BY observed_at, id
         LIMIT $5",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(window_start)
    .bind(event.occurred_at)
    .bind(MAX_CHANGE_EVENTS)
    .fetch_all(&mut **transaction)
    .await?;
    for row in evidence_changes {
        let evidence_id: Uuid = row.try_get("id")?;
        let resource: String = row.try_get("resource")?;
        let event_type = if resource.contains("certificate") {
            "certificate_change"
        } else if resource.contains("deployment") || resource.contains("stateful") {
            "deployment_change"
        } else {
            "configuration_change"
        };
        insert_timeline(
            transaction,
            event,
            incident_id,
            deterministic_uuid(&format!("incident-evidence-change:{incident_id}:{evidence_id}")),
            event_type,
            "Recent evidence-backed change correlated to incident",
            json!({
                "evidence_id": evidence_id,
                "source": row.try_get::<String, _>("source")?,
                "resource": resource,
                "lookback_minutes": 30,
            }),
            &auth.subject,
            correlation_id,
            row.try_get("observed_at")?,
        )
        .await?;
    }
    Ok(())
}

async fn update_incident_aggregate(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    incident_id: IncidentId,
    owner: &str,
    occurrence_count: u32,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "UPDATE sre_incidents
         SET severity = CASE
                WHEN CASE severity
                    WHEN 'critical' THEN 4 WHEN 'error' THEN 3
                    WHEN 'warning' THEN 2 WHEN 'info' THEN 1 ELSE 0 END
                   <= CASE $2
                    WHEN 'critical' THEN 4 WHEN 'error' THEN 3
                    WHEN 'warning' THEN 2 WHEN 'info' THEN 1 ELSE 0 END
                THEN $2 ELSE severity END,
             owner_name = CASE WHEN owner_name = 'unassigned' THEN $3 ELSE owner_name END,
             occurrence_count = $4,
             last_alert_at = GREATEST(last_alert_at, $5),
             updated_at = GREATEST(updated_at, $5)
         WHERE id = $1 AND tenant_id = $6 AND cluster_id = $7",
    )
    .bind(incident_id.as_uuid())
    .bind(severity_name(event.severity))
    .bind(owner)
    .bind(
        i32::try_from(occurrence_count)
            .map_err(|_| ControlPlaneError::configuration("incident occurrence count exceeds PostgreSQL INTEGER"))?,
    )
    .bind(event.occurred_at)
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn count_incident_occurrences(
    transaction: &mut Transaction<'_, Postgres>,
    incident_id: IncidentId,
) -> Result<u32, ControlPlaneError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM incident_timeline
         WHERE incident_id = $1 AND event_type = 'alert'",
    )
    .bind(incident_id.as_uuid())
    .fetch_one(&mut **transaction)
    .await?;
    to_u32(count, "incident occurrence count")
}

async fn resolve_owner(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
) -> Result<OwnerResolution, ControlPlaneError> {
    let label_owner = ["owner", "team", "on_call"]
        .iter()
        .find_map(|key| event.labels.get(*key))
        .filter(|value| valid_owner(value))
        .cloned();
    let canonical = canonical_resource(&event.affected_resource);
    let kind_wildcard = format!("{}:*", resource_kind_name(event.affected_resource.kind));
    let cluster_wildcard = "cluster:*";
    let row = sqlx::query(
        "SELECT owner_name, target_ids
         FROM on_call_owners
         WHERE tenant_id = $1
           AND (cluster_id IS NULL OR cluster_id = $2)
           AND valid_from <= $3
           AND (valid_until IS NULL OR valid_until > $3)
           AND (
                resource_selector = $4
                OR resource_selector = $5
                OR resource_selector = $6
                OR ($7::TEXT IS NOT NULL AND owner_name = $7)
           )
         ORDER BY
            CASE WHEN owner_name = $7 THEN 0
                 WHEN resource_selector = $4 THEN 1
                 WHEN resource_selector = $5 THEN 2 ELSE 3 END,
            CASE WHEN cluster_id IS NOT NULL THEN 0 ELSE 1 END,
            valid_from DESC
         LIMIT 1",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(event.occurred_at)
    .bind(&canonical)
    .bind(&kind_wildcard)
    .bind(cluster_wildcard)
    .bind(&label_owner)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return Ok(OwnerResolution {
            owner: label_owner.unwrap_or(row.try_get("owner_name")?),
            target_ids: row
                .try_get::<Vec<Uuid>, _>("target_ids")?
                .into_iter()
                .map(NotificationTargetId::from_uuid)
                .collect(),
        });
    }
    let owner = label_owner.unwrap_or_else(|| "unassigned".to_owned());
    let target_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM notification_targets
         WHERE tenant_id = $1 AND enabled
           AND (cluster_id IS NULL OR cluster_id = $2)
         ORDER BY CASE WHEN cluster_id IS NOT NULL THEN 0 ELSE 1 END, name, id
         LIMIT $3",
    )
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(MAX_NOTIFICATION_TARGETS)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(NotificationTargetId::from_uuid)
    .collect();
    Ok(OwnerResolution { owner, target_ids })
}

async fn enqueue_incident_notifications(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    incident_id: IncidentId,
    target_ids: &[NotificationTargetId],
    public_base_url: &str,
) -> Result<(), ControlPlaneError> {
    let summary = notification_summary(event);
    let deep_link = incident_deep_link(public_base_url, incident_id);
    for target_id in target_ids.iter().take(MAX_NOTIFICATION_TARGETS as usize) {
        let delivery_key = format!(
            "incident:{incident_id}:target:{target_id}:severity:{}:status:{}",
            severity_name(event.severity),
            alert_status_name(event.status)
        );
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, target_id, tenant_id, cluster_id, incident_id, delivery_key,
                status, sanitized_summary, deep_link, attempt_count,
                next_attempt_at, created_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, 'pending', $7, $8, 0, NOW(), $9
             )
             ON CONFLICT (tenant_id, delivery_key) DO NOTHING",
        )
        .bind(NotificationDeliveryId::new().as_uuid())
        .bind(target_id.as_uuid())
        .bind(event.tenant_id.as_uuid())
        .bind(event.cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(delivery_key)
        .bind(&summary)
        .bind(&deep_link)
        .bind(event.received_at)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn insert_recurrence_relation(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    previous: IncidentId,
    current: IncidentId,
    auth: &AuthContext,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO incident_relations (
            id, tenant_id, cluster_id, from_incident_id, to_incident_id,
            relation_kind, reason_code, evidence_ids, created_by, created_at
         ) VALUES ($1, $2, $3, $4, $5, 'recurrence', 'alert_recurrence',
                   $6, $7, $8)
         ON CONFLICT (
            tenant_id, cluster_id, from_incident_id, to_incident_id, relation_kind
         ) DO NOTHING",
    )
    .bind(IncidentRelationId::new().as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(previous.as_uuid())
    .bind(current.as_uuid())
    .bind(event.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
    .bind(&auth.subject)
    .bind(event.received_at)
    .execute(&mut **transaction)
    .await?;
    insert_timeline(
        transaction,
        event,
        current,
        deterministic_uuid(&format!("incident-recurrence:{previous}:{current}")),
        "incident_status_change",
        "Terminal incident recurred as a new protected incident",
        json!({
            "previous_incident_id": previous,
            "current_incident_id": current,
            "terminal_state_reopened": false,
        }),
        &auth.subject,
        correlation_id,
        event.received_at,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "timeline persistence requires the complete immutable event envelope"
)]
async fn insert_timeline(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    incident_id: IncidentId,
    event_id: Uuid,
    event_type: &str,
    summary: &str,
    details: Value,
    actor: &str,
    correlation_id: CorrelationId,
    occurred_at: chrono::DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO incident_timeline (
            event_id, tenant_id, cluster_id, incident_id, event_type, summary,
            details, correlation_id, actor_subject, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(event_type)
    .bind(summary)
    .bind(details)
    .bind(correlation_id.as_uuid())
    .bind(actor)
    .bind(occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn append_workflow_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &AlertEvent,
    incident_id: IncidentId,
    event_id: Uuid,
    event_type: &str,
    payload: Value,
    correlation_id: CorrelationId,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO workflow_events (
            event_id, tenant_id, cluster_id, aggregate_type, aggregate_id,
            event_type, event_payload, correlation_id, occurred_at
         ) VALUES ($1, $2, $3, 'incident', $4, $5, $6, $7, $8)
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event.tenant_id.as_uuid())
    .bind(event.cluster_id.as_uuid())
    .bind(incident_id.as_uuid())
    .bind(event_type)
    .bind(payload)
    .bind(correlation_id.as_uuid())
    .bind(event.received_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn ensure_cluster_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    cluster_id: ClusterId,
) -> Result<(), ControlPlaneError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM clusters
            WHERE id = $1 AND tenant_id = $2 AND onboarding_state <> 'offboarded'
        )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if !exists {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated tenant or is offboarded",
        ));
    }
    Ok(())
}

async fn ensure_incident_visible(
    pool: &sqlx::PgPool,
    auth: &AuthContext,
    incident_id: IncidentId,
) -> Result<ClusterId, ControlPlaneError> {
    let cluster_id =
        sqlx::query_scalar::<_, Uuid>("SELECT cluster_id FROM sre_incidents WHERE id = $1 AND tenant_id = $2")
            .bind(incident_id.as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .fetch_optional(pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)
            .map(ClusterId::from_uuid)?;
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "incident is outside the authenticated cluster scope",
        ));
    }
    Ok(cluster_id)
}

fn enforce_auth_scope(auth: &AuthContext, tenant_id: TenantId, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if auth.tenant_id != tenant_id {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "event tenant does not match the authenticated tenant",
        ));
    }
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "event cluster is outside the authenticated cluster scope",
        ));
    }
    Ok(())
}

fn incident_candidate(row: &sqlx::postgres::PgRow) -> Result<IncidentCandidate, ControlPlaneError> {
    let status: String = row.try_get("status")?;
    Ok(IncidentCandidate {
        id: IncidentId::from_uuid(row.try_get("id")?),
        terminal: matches!(status.as_str(), "resolved" | "escalated"),
    })
}

fn correlation_key_digest(event: &AlertEvent) -> String {
    let material = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        event.tenant_id,
        event.cluster_id,
        resource_kind_name(event.correlation_key.resource_kind),
        event.correlation_key.resource_key,
        event.correlation_key.symptom_family.as_str(),
        event.correlation_key.window_start.timestamp(),
        event.correlation_key.window_seconds
    );
    format!(
        "sha256:{}",
        rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(material.as_bytes()))
    )
}

fn deterministic_uuid(material: &str) -> Uuid {
    let digest = Sha256::digest(material.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn canonical_resource(resource: &ResourceRef) -> String {
    format!("{}:{}", resource_kind_name(resource.kind), resource.key)
}

fn incident_deep_link(public_base_url: &str, incident_id: IncidentId) -> String {
    format!("{}/incidents/{incident_id}", public_base_url.trim_end_matches('/'))
}

fn valid_owner(owner: &str) -> bool {
    let owner = owner.trim();
    !owner.is_empty() && owner.chars().count() <= 128 && !owner.chars().any(char::is_control)
}

const fn alert_status_name(status: AlertStatus) -> &'static str {
    match status {
        AlertStatus::Firing => "firing",
        AlertStatus::Resolved => "resolved",
    }
}

fn parse_channel(value: String) -> Result<NotificationChannel, ControlPlaneError> {
    match value.as_str() {
        "signed_webhook" => Ok(NotificationChannel::SignedWebhook),
        "email" => Ok(NotificationChannel::Email),
        "pager" => Ok(NotificationChannel::Pager),
        _ => Err(ControlPlaneError::configuration(
            "stored notification channel is invalid",
        )),
    }
}

fn to_u32(value: i64, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(value).map_err(|_| ControlPlaneError::configuration(format!("stored {field} is out of range")))
}

fn to_u16(value: i32, field: &'static str) -> Result<u16, ControlPlaneError> {
    u16::try_from(value).map_err(|_| ControlPlaneError::configuration(format!("stored {field} is out of range")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::Phase2Repository;
    use rocketmq_sre_contracts::AlertEventId;
    use rocketmq_sre_contracts::AlertSeverity;
    use rocketmq_sre_contracts::AlertSource;
    use rocketmq_sre_contracts::CorrelationKey;
    use rocketmq_sre_contracts::ResourceKind;
    use rocketmq_sre_contracts::SymptomFamily;

    #[test]
    fn correlation_key_is_tenant_cluster_and_window_scoped() {
        let now = Utc::now();
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let event = fixture_event(tenant_id, cluster_id, now);
        let same = fixture_event(tenant_id, cluster_id, now);
        let other_tenant = fixture_event(TenantId::new(), cluster_id, now);
        let other_cluster = fixture_event(tenant_id, ClusterId::new(), now);
        let later = fixture_event(tenant_id, cluster_id, now + Duration::minutes(5));

        assert_eq!(correlation_key_digest(&event), correlation_key_digest(&same));
        assert_ne!(correlation_key_digest(&event), correlation_key_digest(&other_tenant));
        assert_ne!(correlation_key_digest(&event), correlation_key_digest(&other_cluster));
        assert_ne!(correlation_key_digest(&event), correlation_key_digest(&later));
    }

    #[test]
    fn deterministic_ids_make_worker_and_webhook_retries_idempotent() {
        assert_eq!(deterministic_uuid("same"), deterministic_uuid("same"));
        assert_ne!(deterministic_uuid("same"), deterministic_uuid("other"));
        assert_eq!(deterministic_uuid("same").get_version_num(), 5);
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_correlation_is_idempotent_topology_aware_and_scope_safe() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 4)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        insert_test_cluster(&repository, tenant_id, cluster_id).await;
        let auth = AuthContext {
            tenant_id,
            subject: "alert-correlation-test".into(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::new(),
        };
        insert_test_topology(&repository, tenant_id, cluster_id).await;

        let now = Utc::now();
        let mut broker = fixture_event(tenant_id, cluster_id, now);
        broker.sequence = 2;
        let alert_id = AlertEventId::from_uuid(repository.store_alert(&broker).await.expect("store broker"));
        let first = repository
            .correlate_alert(&auth, &broker, alert_id, CorrelationId::new(), "http://localhost:3004")
            .await
            .expect("correlate broker");
        assert!(first.created);
        assert_eq!(first.occurrence_count, 1);

        let retry_id = AlertEventId::from_uuid(repository.store_alert(&broker).await.expect("retry broker"));
        let retry = repository
            .correlate_alert(&auth, &broker, retry_id, CorrelationId::new(), "http://localhost:3004")
            .await
            .expect("correlate retry");
        assert_eq!(retry.incident_id, first.incident_id);
        assert!(!retry.created);
        assert_eq!(retry.occurrence_count, 1);

        let mut out_of_order = broker.clone();
        out_of_order.sequence = 1;
        out_of_order.severity = AlertSeverity::Warning;
        out_of_order.occurred_at = now - Duration::seconds(10);
        let out_of_order_id = AlertEventId::from_uuid(
            repository
                .store_alert(&out_of_order)
                .await
                .expect("store older sequence"),
        );
        let older = repository
            .correlate_alert(
                &auth,
                &out_of_order,
                out_of_order_id,
                CorrelationId::new(),
                "http://localhost:3004",
            )
            .await
            .expect("correlate older sequence");
        assert_eq!(older.incident_id, first.incident_id);
        assert_eq!(older.occurrence_count, 2);

        for (kind, key, source_id) in [
            (ResourceKind::Store, "broker-a", "store-fault"),
            (ResourceKind::Controller, "controller-a", "controller-fault"),
            (ResourceKind::Pod, "broker-a-0", "pod-fault"),
        ] {
            let mut related = fixture_event(tenant_id, cluster_id, now);
            related.id = AlertEventId::new();
            related.source_event_id = source_id.into();
            related.affected_resource = ResourceRef {
                kind,
                key: key.into(),
                display_name: None,
            };
            related.correlation_key.resource_kind = kind;
            related.correlation_key.resource_key = key.into();
            related.fingerprint = format!("sha256:{:064x}", related.sequence + u64::from(kind as u8));
            let related_id =
                AlertEventId::from_uuid(repository.store_alert(&related).await.expect("store related alert"));
            let correlated = repository
                .correlate_alert(
                    &auth,
                    &related,
                    related_id,
                    CorrelationId::new(),
                    "http://localhost:3004",
                )
                .await
                .expect("correlate topology alert");
            assert_eq!(correlated.incident_id, first.incident_id);
        }

        let row = sqlx::query(
            "SELECT severity, occurrence_count, last_alert_at
             FROM sre_incidents WHERE id = $1",
        )
        .bind(first.incident_id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("incident aggregate");
        assert_eq!(row.try_get::<String, _>("severity").expect("severity"), "critical");
        assert_eq!(row.try_get::<i32, _>("occurrence_count").expect("count"), 5);
        assert_eq!(
            row.try_get::<chrono::DateTime<Utc>, _>("last_alert_at")
                .expect("last alert")
                .timestamp_micros(),
            now.timestamp_micros()
        );

        sqlx::query("UPDATE sre_incidents SET status = 'resolved' WHERE id = $1")
            .bind(first.incident_id.as_uuid())
            .execute(&repository.pool)
            .await
            .expect("resolve original incident");
        let mut recurrence = broker.clone();
        recurrence.sequence = 3;
        recurrence.occurred_at = now + Duration::minutes(1);
        recurrence.received_at = recurrence.occurred_at;
        let recurrence_alert_id =
            AlertEventId::from_uuid(repository.store_alert(&recurrence).await.expect("store recurrence"));
        let reopened = repository
            .correlate_alert(
                &auth,
                &recurrence,
                recurrence_alert_id,
                CorrelationId::new(),
                "http://localhost:3004",
            )
            .await
            .expect("correlate recurrence");
        assert!(reopened.created);
        assert!(reopened.recurrence);
        assert_ne!(reopened.incident_id, first.incident_id);
        let retry_reopened = repository
            .correlate_alert(
                &auth,
                &recurrence,
                recurrence_alert_id,
                CorrelationId::new(),
                "http://localhost:3004",
            )
            .await
            .expect("retry recurrence");
        assert_eq!(retry_reopened.incident_id, reopened.incident_id);
        assert!(!retry_reopened.created);
        let relation_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM incident_relations
             WHERE from_incident_id = $1 AND to_incident_id = $2
               AND relation_kind = 'recurrence'",
        )
        .bind(first.incident_id.as_uuid())
        .bind(reopened.incident_id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("recurrence relation");
        assert_eq!(relation_count, 1);
        let original_status: String = sqlx::query_scalar("SELECT status FROM sre_incidents WHERE id = $1")
            .bind(first.incident_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("original incident status");
        assert_eq!(original_status, "resolved");

        let note_correlation_id = CorrelationId::new();
        let note = repository
            .append_incident_note(
                &auth,
                reopened.incident_id,
                "Operator confirmed that consumer capacity is recovering.",
                note_correlation_id,
            )
            .await
            .expect("append operator note");
        assert_eq!(note.event_type, "operator_note");
        assert_eq!(note.correlation_id, note_correlation_id);
        let persisted_note_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM incident_timeline
             WHERE incident_id = $1 AND event_id = $2
               AND event_type = 'operator_note'",
        )
        .bind(reopened.incident_id.as_uuid())
        .bind(note.id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .expect("persisted operator note");
        assert_eq!(persisted_note_count, 1);

        let other_cluster = ClusterId::new();
        insert_test_cluster(&repository, tenant_id, other_cluster).await;
        let scoped_auth = AuthContext {
            clusters: BTreeSet::from([cluster_id, other_cluster]),
            ..auth.clone()
        };
        let cross_cluster = fixture_event(tenant_id, other_cluster, now);
        let cross_id = AlertEventId::from_uuid(
            repository
                .store_alert(&cross_cluster)
                .await
                .expect("store cross cluster"),
        );
        let cross_result = repository
            .correlate_alert(
                &scoped_auth,
                &cross_cluster,
                cross_id,
                CorrelationId::new(),
                "http://localhost:3004",
            )
            .await
            .expect("correlate cross cluster");
        assert_ne!(cross_result.incident_id, first.incident_id);

        let wrong_tenant = fixture_event(TenantId::new(), cluster_id, now);
        let denied = repository
            .correlate_alert(
                &auth,
                &wrong_tenant,
                AlertEventId::new(),
                CorrelationId::new(),
                "http://localhost:3004",
            )
            .await
            .expect_err("cross-tenant correlation must fail");
        assert!(matches!(
            denied,
            ControlPlaneError::Forbidden {
                code: "tenant_mismatch",
                ..
            }
        ));
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_notification_outbox_retries_without_duplicate_delivery_or_incident_loss() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 4)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        insert_test_cluster(&repository, tenant_id, cluster_id).await;
        let incident_id = IncidentId::new();
        sqlx::query(
            "INSERT INTO sre_incidents (
                id, tenant_id, cluster_id, title, symptom_family, fingerprint,
                status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'notification fixture', 'notification_test',
                $4, 'new', 'notification-test', NOW(), NOW()
             )",
        )
        .bind(incident_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(format!("sha256:{:064x}", 91))
        .execute(&repository.pool)
        .await
        .expect("test incident");
        let target_id = NotificationTargetId::new();
        sqlx::query(
            "INSERT INTO notification_targets (
                id, tenant_id, cluster_id, name, channel, endpoint,
                secret_reference, enabled, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'test email', 'email', 'mock:test',
                NULL, TRUE, NOW(), NOW()
             )",
        )
        .bind(target_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("test notification target");
        let delivery = rocketmq_sre_contracts::NotificationDelivery {
            id: NotificationDeliveryId::new(),
            target_id,
            tenant_id,
            cluster_id,
            incident_id,
            delivery_key: format!("notification-idempotency-{incident_id}"),
            status: rocketmq_sre_contracts::NotificationDeliveryStatus::Pending,
            sanitized_summary: "bounded summary".into(),
            deep_link: format!("http://localhost:3004/incidents/{incident_id}"),
            attempt_count: 0,
            next_attempt_at: Some(Utc::now()),
            last_error_code: None,
            delivered_at: None,
            created_at: Utc::now(),
        };
        assert!(repository.enqueue_notification(&delivery).await.expect("first enqueue"));
        assert!(!repository.enqueue_notification(&delivery).await.expect("retry enqueue"));

        let claim = repository
            .claim_notifications(8)
            .await
            .expect("claim delivery")
            .into_iter()
            .find(|claim| claim.delivery_id == delivery.id)
            .expect("delivery claim");
        repository
            .finish_notification(&claim, Err("transport_unavailable"))
            .await
            .expect("schedule retry");
        let (status, attempt_count): (String, i32) =
            sqlx::query_as("SELECT status, attempt_count FROM notification_outbox WHERE id = $1")
                .bind(delivery.id.as_uuid())
                .fetch_one(&repository.pool)
                .await
                .expect("retry state");
        assert_eq!(status, "retry_scheduled");
        assert_eq!(attempt_count, 1);
        let incident_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sre_incidents WHERE id = $1")
            .bind(incident_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("incident still exists");
        assert_eq!(incident_count, 1);
        let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM notification_outbox WHERE delivery_key = $1")
            .bind(&delivery.delivery_key)
            .fetch_one(&repository.pool)
            .await
            .expect("single outbox row");
        assert_eq!(outbox_count, 1);
    }

    fn fixture_event(tenant_id: TenantId, cluster_id: ClusterId, now: chrono::DateTime<Utc>) -> AlertEvent {
        let window_start =
            chrono::DateTime::from_timestamp(now.timestamp() - now.timestamp().rem_euclid(300), 0).expect("window");
        AlertEvent {
            id: AlertEventId::new(),
            tenant_id,
            cluster_id,
            source: AlertSource::Alertmanager,
            source_event_id: "broker-down".into(),
            fingerprint: "sha256:fixture".into(),
            correlation_key: CorrelationKey {
                tenant_id,
                cluster_id,
                resource_kind: ResourceKind::Broker,
                resource_key: "broker-a".into(),
                symptom_family: SymptomFamily::new("broker_unavailable"),
                window_start,
                window_seconds: 300,
            },
            affected_resource: ResourceRef {
                kind: ResourceKind::Broker,
                key: "broker-a".into(),
                display_name: None,
            },
            symptom_family: SymptomFamily::new("broker_unavailable"),
            severity: AlertSeverity::Critical,
            status: AlertStatus::Firing,
            summary: "broker unavailable".into(),
            labels: BTreeMap::new(),
            evidence_ids: Vec::new(),
            occurrence_count: 1,
            sequence: 1,
            occurred_at: now,
            received_at: now,
        }
    }

    async fn insert_test_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'local', 'test', 'test', 'alert-correlation-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("alert-correlation-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
    }

    async fn insert_test_topology(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
        let snapshot_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO asset_inventory_snapshots (
                id, tenant_id, cluster_id, sources, observed_at,
                freshness_seconds, partial, content_hash
             ) VALUES ($1, $2, $3, '[\"topology\"]', NOW(), 30, FALSE, $4)",
        )
        .bind(snapshot_id)
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(format!("sha256:{:064x}", 1))
        .execute(&repository.pool)
        .await
        .expect("inventory snapshot");
        for (from, to, relation, index) in [
            ("broker:broker-a", "store:broker-a", "stores_on", 2_u64),
            ("broker:broker-a", "controller:controller-a", "controlled_by", 3),
            ("controller:controller-a", "node:node-a", "runs_on", 4),
            ("node:node-a", "pod:broker-a-0", "runs_on", 5),
        ] {
            sqlx::query(
                "INSERT INTO topology_edges (
                    id, tenant_id, cluster_id, from_key, to_key, relation,
                    source, observed_at, freshness_seconds, partial,
                    content_hash, inventory_snapshot_id
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6, 'topology', NOW(), 30, FALSE, $7, $8
                 )",
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .bind(from)
            .bind(to)
            .bind(relation)
            .bind(format!("sha256:{index:064x}"))
            .bind(snapshot_id)
            .execute(&repository.pool)
            .await
            .expect("topology edge");
        }
    }
}
