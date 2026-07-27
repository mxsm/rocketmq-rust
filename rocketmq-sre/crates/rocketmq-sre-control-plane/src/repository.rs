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

use std::str::FromStr;
use std::time::Duration;

#[cfg(test)]
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use serde_json::json;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::CapabilitySnapshot;
use crate::Cluster;
use crate::ControlPlaneError;
use crate::HandshakeRequest;
use crate::OffboardRequest;
use crate::OnboardClusterRequest;
use crate::OnboardingState;
use crate::model::HandshakeDecision;
use crate::model::HandshakeOutcome;
use crate::model::OnboardOutcome;

pub(crate) trait ClusterRepository: Clone + Send + Sync + 'static {
    async fn ping(&self) -> Result<(), ControlPlaneError>;
    async fn onboard(&self, request: &OnboardClusterRequest) -> Result<OnboardOutcome, ControlPlaneError>;
    async fn list(&self) -> Result<Vec<Cluster>, ControlPlaneError>;
    async fn get(&self, id: ClusterId) -> Result<Cluster, ControlPlaneError>;
    async fn handshake(
        &self,
        id: ClusterId,
        request: &HandshakeRequest,
        decision: &HandshakeDecision,
    ) -> Result<HandshakeOutcome, ControlPlaneError>;
    async fn capability(&self, id: ClusterId) -> Result<CapabilitySnapshot, ControlPlaneError>;
    async fn offboard(&self, id: ClusterId, request: &OffboardRequest) -> Result<Cluster, ControlPlaneError>;
}

/// Production PostgreSQL repository. No plaintext connector secret is stored.
#[derive(Clone, Debug)]
pub struct PostgresRepository {
    pub(crate) pool: PgPool,
}

impl PostgresRepository {
    /// Connects to PostgreSQL and applies the embedded Phase 00 migration.
    ///
    /// # Errors
    ///
    /// Returns a sanitized database error when the pool cannot connect or a
    /// migration fails.
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, ControlPlaneError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await?;
        sqlx::migrate!("../../migrations").run(&pool).await.map_err(|error| {
            ControlPlaneError::configuration(format!("database migration failed: {}", migration_error_class(&error)))
        })?;
        Ok(Self { pool })
    }

    #[must_use]
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(crate) async fn connector_identity_known(
        &self,
        cluster_id: ClusterId,
        subject: &str,
        issuer: &str,
    ) -> Result<bool, ControlPlaneError> {
        sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM connector_identities
                WHERE cluster_id = $1 AND subject = $2 AND issuer = $3
            )",
        )
        .bind(cluster_id.as_uuid())
        .bind(subject)
        .bind(issuer)
        .fetch_one(&self.pool)
        .await
        .map_err(ControlPlaneError::from)
    }
}

fn migration_error_class(error: &sqlx::migrate::MigrateError) -> &'static str {
    match error {
        sqlx::migrate::MigrateError::Source(_) => "migration source error",
        sqlx::migrate::MigrateError::Execute(_) => "migration execution error",
        sqlx::migrate::MigrateError::VersionMissing(_) => "migration version missing",
        sqlx::migrate::MigrateError::VersionMismatch(_) => "migration version mismatch",
        sqlx::migrate::MigrateError::VersionTooOld(..) => "database migration version is too old",
        sqlx::migrate::MigrateError::VersionTooNew(..) => "database migration version is too new",
        sqlx::migrate::MigrateError::VersionNotPresent(_) => "database migration version is not present",
        sqlx::migrate::MigrateError::Dirty(_) => "database migration is incomplete",
        sqlx::migrate::MigrateError::ForceNotSupported => "migration force is unsupported",
        _ => "database migration error",
    }
}

impl ClusterRepository for PostgresRepository {
    async fn ping(&self) -> Result<(), ControlPlaneError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn onboard(&self, request: &OnboardClusterRequest) -> Result<OnboardOutcome, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        if let Some(row) = sqlx::query(CLUSTER_BY_EXTERNAL_KEY_FOR_UPDATE)
            .bind(&request.tenant_id)
            .bind(&request.external_cluster_key)
            .fetch_optional(&mut *transaction)
            .await?
        {
            let cluster = cluster_from_row(&row)?;
            transaction.commit().await?;
            return Ok(OnboardOutcome {
                cluster,
                created: false,
            });
        }

        let cluster_id = request.cluster_id.unwrap_or_default();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile,
                onboarding_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'read_only', 'read_only', 'pending')",
        )
        .bind(cluster_id.as_uuid())
        .bind(&request.tenant_id)
        .bind(&request.external_cluster_key)
        .bind(&request.environment)
        .bind(&request.region)
        .bind(&request.rocketmq_version)
        .bind(&request.deployment_mode)
        .bind(&request.owner)
        .execute(&mut *transaction)
        .await?;

        append_event(
            &mut transaction,
            cluster_id,
            "cluster_onboarded",
            &request.actor_subject,
            request.correlation_id.unwrap_or_default(),
            json!({
                "state": OnboardingState::Pending,
                "effective_access_profile": "read_only"
            }),
        )
        .await?;
        let cluster = get_cluster_in_transaction(&mut transaction, cluster_id).await?;
        transaction.commit().await?;
        Ok(OnboardOutcome { cluster, created: true })
    }

    async fn list(&self) -> Result<Vec<Cluster>, ControlPlaneError> {
        let rows = sqlx::query(&format!("{CLUSTER_COLUMNS} ORDER BY created_at ASC, id ASC"))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(cluster_from_row).collect()
    }

    async fn get(&self, id: ClusterId) -> Result<Cluster, ControlPlaneError> {
        let row = sqlx::query(&format!("{CLUSTER_COLUMNS} WHERE id = $1"))
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        cluster_from_row(&row)
    }

    async fn handshake(
        &self,
        id: ClusterId,
        request: &HandshakeRequest,
        decision: &HandshakeDecision,
    ) -> Result<HandshakeOutcome, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let current = get_cluster_for_update(&mut transaction, id).await?;
        if current.state.is_terminal() {
            return Err(ControlPlaneError::conflict("offboarded clusters cannot be handshaken"));
        }

        let reported_tool_surface_digest = request.capability.tool_surface_digest()?;
        let pinned_tool_surface_digest = pinned_tool_surface_digest_in_transaction(&mut transaction, id).await?;
        let effective_decision = enforce_tool_surface_pin(
            decision,
            pinned_tool_surface_digest.as_deref(),
            reported_tool_surface_digest,
        );
        let existing_capability = latest_capability_in_transaction(&mut transaction, id).await?;
        if current.state == effective_decision.state
            && existing_capability
                .as_ref()
                .is_some_and(|snapshot| capability_matches_request(snapshot, request))
        {
            transaction.commit().await?;
            return Ok(HandshakeOutcome {
                cluster: current,
                capability: existing_capability,
                reason: effective_decision.reason,
            });
        }

        sqlx::query(
            "UPDATE clusters
             SET onboarding_state = 'handshaking', updated_at = NOW()
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .execute(&mut *transaction)
        .await?;
        let correlation_id = request.correlation_id.unwrap_or_default();
        append_event(
            &mut transaction,
            id,
            "cluster_handshake_started",
            &request.connector_subject,
            correlation_id,
            json!({"state": OnboardingState::Handshaking}),
        )
        .await?;

        let capability = if effective_decision.persist_capability {
            sqlx::query(
                "INSERT INTO cluster_capability_snapshots (
                    id, cluster_id, manifest_digest, tool_surface_digest, protocol_version,
                    schema_version, mutation_supported, manifest, data_sources,
                    observed_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ",
            )
            .bind(Uuid::new_v4())
            .bind(id.as_uuid())
            .bind(&request.capability.digest)
            .bind(reported_tool_surface_digest)
            .bind(&request.capability.protocol_version)
            .bind(&request.capability.schema_version)
            .bind(request.capability.mutation_supported)
            .bind(&request.capability.manifest)
            .bind(
                serde_json::to_value(normalized_data_sources(&request.capability.data_sources)).map_err(|error| {
                    ControlPlaneError::validation(
                        "capability_mismatch",
                        format!("data source status is invalid: {error}"),
                    )
                })?,
            )
            .bind(request.capability.observed_at)
            .execute(&mut *transaction)
            .await?;
            latest_capability_in_transaction(&mut transaction, id).await?
        } else {
            None
        };

        if !request.capability.mutation_supported {
            sqlx::query(
                "INSERT INTO connector_identities (
                    id, cluster_id, subject, issuer
                 ) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (cluster_id, subject, issuer)
                 DO UPDATE SET revoked_at = NULL",
            )
            .bind(Uuid::new_v4())
            .bind(id.as_uuid())
            .bind(&request.connector_subject)
            .bind(&request.connector_issuer)
            .execute(&mut *transaction)
            .await?;
        }

        sqlx::query(
            "UPDATE clusters
             SET onboarding_state = $2, updated_at = NOW()
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .bind(effective_decision.state.to_string())
        .execute(&mut *transaction)
        .await?;
        append_event(
            &mut transaction,
            id,
            "cluster_handshake_completed",
            &request.connector_subject,
            correlation_id,
            json!({
                "state": effective_decision.state,
                "manifest_digest": request.capability.digest,
                "tool_surface_digest": reported_tool_surface_digest,
                "mutation_supported": request.capability.mutation_supported,
                "reason": effective_decision.reason
            }),
        )
        .await?;
        let cluster = get_cluster_in_transaction(&mut transaction, id).await?;
        transaction.commit().await?;
        Ok(HandshakeOutcome {
            cluster,
            capability,
            reason: effective_decision.reason,
        })
    }

    async fn capability(&self, id: ClusterId) -> Result<CapabilitySnapshot, ControlPlaneError> {
        self.get(id).await?;
        let row = sqlx::query(LATEST_CAPABILITY)
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        capability_from_row(&row)
    }

    async fn offboard(&self, id: ClusterId, request: &OffboardRequest) -> Result<Cluster, ControlPlaneError> {
        if request.actor_subject.trim().is_empty() {
            return Err(ControlPlaneError::validation(
                "unauthorized_scope",
                "actor_subject must not be empty",
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let current = get_cluster_for_update(&mut transaction, id).await?;
        if current.state == OnboardingState::Offboarded {
            transaction.commit().await?;
            return Ok(current);
        }

        sqlx::query(
            "UPDATE clusters
             SET onboarding_state = 'offboarded',
                 offboarded_at = NOW(),
                 updated_at = NOW()
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE connector_identities
             SET revoked_at = COALESCE(revoked_at, NOW())
             WHERE cluster_id = $1",
        )
        .bind(id.as_uuid())
        .execute(&mut *transaction)
        .await?;
        append_event(
            &mut transaction,
            id,
            "cluster_offboarded",
            &request.actor_subject,
            request.correlation_id.unwrap_or_default(),
            json!({
                "state": OnboardingState::Offboarded,
                "reason": request.reason
            }),
        )
        .await?;
        let cluster = get_cluster_in_transaction(&mut transaction, id).await?;
        transaction.commit().await?;
        Ok(cluster)
    }
}

const CLUSTER_COLUMNS: &str = "SELECT
    id, tenant_id, external_cluster_key, environment, region,
    rocketmq_version, deployment_mode, owner_name, onboarding_state,
    created_at, updated_at, offboarded_at
    FROM clusters";

const CLUSTER_BY_EXTERNAL_KEY_FOR_UPDATE: &str = "SELECT
    id, tenant_id, external_cluster_key, environment, region,
    rocketmq_version, deployment_mode, owner_name, onboarding_state,
    created_at, updated_at, offboarded_at
    FROM clusters
    WHERE tenant_id = $1 AND external_cluster_key = $2
    FOR UPDATE";

const LATEST_CAPABILITY: &str = "SELECT
    cluster_id, manifest_digest, tool_surface_digest, protocol_version, schema_version,
    mutation_supported, manifest, data_sources, observed_at
    FROM cluster_capability_snapshots
    WHERE cluster_id = $1
    ORDER BY created_at DESC, id DESC
    LIMIT 1";

const PINNED_TOOL_SURFACE_DIGEST: &str = "SELECT tool_surface_digest
    FROM cluster_capability_snapshots
    WHERE cluster_id = $1
    ORDER BY created_at ASC, id ASC
    LIMIT 1";

fn cluster_from_row(row: &PgRow) -> Result<Cluster, ControlPlaneError> {
    let id: Uuid = row.try_get("id")?;
    let state: String = row.try_get("onboarding_state")?;
    Ok(Cluster {
        id: ClusterId::from_uuid(id),
        tenant_id: row.try_get("tenant_id")?,
        external_cluster_key: row.try_get("external_cluster_key")?,
        environment: row.try_get("environment")?,
        region: row.try_get("region")?,
        rocketmq_version: row.try_get("rocketmq_version")?,
        deployment_mode: row.try_get("deployment_mode")?,
        owner: row.try_get("owner_name")?,
        state: OnboardingState::from_str(&state)?,
        effective_access_profile: "read_only",
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        offboarded_at: row.try_get("offboarded_at")?,
    })
}

fn capability_from_row(row: &PgRow) -> Result<CapabilitySnapshot, ControlPlaneError> {
    let cluster_id: Uuid = row.try_get("cluster_id")?;
    let data_sources = serde_json::from_value(row.try_get("data_sources")?).map_err(|error| {
        ControlPlaneError::configuration(format!("stored capability data source payload is invalid: {error}"))
    })?;
    Ok(CapabilitySnapshot {
        cluster_id: ClusterId::from_uuid(cluster_id),
        digest: row.try_get("manifest_digest")?,
        tool_surface_digest: row.try_get("tool_surface_digest")?,
        protocol_version: row.try_get("protocol_version")?,
        schema_version: row.try_get("schema_version")?,
        mutation_supported: row.try_get("mutation_supported")?,
        observed_at: row.try_get("observed_at")?,
        data_sources,
        manifest: row.try_get("manifest")?,
    })
}

async fn get_cluster_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    id: ClusterId,
) -> Result<Cluster, ControlPlaneError> {
    let row = sqlx::query(&format!("{CLUSTER_COLUMNS} WHERE id = $1 FOR UPDATE"))
        .bind(id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
    cluster_from_row(&row)
}

async fn get_cluster_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: ClusterId,
) -> Result<Cluster, ControlPlaneError> {
    let row = sqlx::query(&format!("{CLUSTER_COLUMNS} WHERE id = $1"))
        .bind(id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
    cluster_from_row(&row)
}

async fn latest_capability_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: ClusterId,
) -> Result<Option<CapabilitySnapshot>, ControlPlaneError> {
    sqlx::query(LATEST_CAPABILITY)
        .bind(id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await?
        .as_ref()
        .map(capability_from_row)
        .transpose()
}

async fn pinned_tool_surface_digest_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: ClusterId,
) -> Result<Option<String>, ControlPlaneError> {
    let row = sqlx::query(PINNED_TOOL_SURFACE_DIGEST)
        .bind(id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await?;
    row.map(|row| row.try_get("tool_surface_digest"))
        .transpose()
        .map_err(ControlPlaneError::from)
}

fn enforce_tool_surface_pin(
    decision: &HandshakeDecision,
    pinned_tool_surface_digest: Option<&str>,
    reported_tool_surface_digest: &str,
) -> HandshakeDecision {
    if decision.state != OnboardingState::Rejected
        && pinned_tool_surface_digest.is_some_and(|pinned| pinned != reported_tool_surface_digest)
    {
        HandshakeDecision {
            state: OnboardingState::ReadOnlyDegraded,
            reason: Some("schema_digest_mismatch".to_owned()),
            persist_capability: true,
        }
    } else {
        decision.clone()
    }
}

fn capability_matches_request(snapshot: &CapabilitySnapshot, request: &HandshakeRequest) -> bool {
    snapshot.digest == request.capability.digest
        && snapshot.tool_surface_digest
            == request
                .capability
                .manifest
                .get("tool_surface_digest")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
        && snapshot.protocol_version == request.capability.protocol_version
        && snapshot.schema_version == request.capability.schema_version
        && snapshot.mutation_supported == request.capability.mutation_supported
        && snapshot.manifest == request.capability.manifest
        && data_source_states_match(&snapshot.data_sources, &request.capability.data_sources)
}

fn data_source_states_match(left: &[crate::DataSourceStatus], right: &[crate::DataSourceStatus]) -> bool {
    let left = normalized_data_sources(left);
    let right = normalized_data_sources(right);
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.id == right.id && left.availability == right.availability && left.detail == right.detail
        })
}

fn normalized_data_sources(sources: &[crate::DataSourceStatus]) -> Vec<crate::DataSourceStatus> {
    let mut normalized = sources.to_vec();
    normalized.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| {
                data_source_availability_rank(left.availability).cmp(&data_source_availability_rank(right.availability))
            })
            .then_with(|| left.freshness_ms.cmp(&right.freshness_ms))
            .then_with(|| left.detail.cmp(&right.detail))
    });
    normalized
}

const fn data_source_availability_rank(availability: crate::DataSourceAvailability) -> u8 {
    match availability {
        crate::DataSourceAvailability::Existing => 0,
        crate::DataSourceAvailability::MissingInstrumentation => 1,
        crate::DataSourceAvailability::InProcessOnly => 2,
        crate::DataSourceAvailability::Queryable => 3,
    }
}

async fn append_event(
    transaction: &mut Transaction<'_, Postgres>,
    cluster_id: ClusterId,
    event_type: &str,
    actor_subject: &str,
    correlation_id: CorrelationId,
    event_payload: serde_json::Value,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO cluster_onboarding_events (
            event_id, cluster_id, event_type, actor_subject,
            correlation_id, event_payload
         ) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(cluster_id.as_uuid())
    .bind(event_type)
    .bind(actor_subject)
    .bind(correlation_id.as_uuid())
    .bind(event_payload)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod memory {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub(crate) struct InMemoryRepository {
        state: Arc<RwLock<MemoryState>>,
    }

    #[derive(Debug, Default)]
    struct MemoryState {
        clusters: BTreeMap<ClusterId, Cluster>,
        external_keys: BTreeMap<(String, String), ClusterId>,
        capabilities: BTreeMap<ClusterId, Vec<CapabilitySnapshot>>,
        identities: BTreeMap<ClusterId, Vec<(String, String, bool)>>,
        events: Vec<(ClusterId, String)>,
    }

    impl InMemoryRepository {
        pub(crate) async fn event_count(&self) -> usize {
            self.state.read().await.events.len()
        }

        pub(crate) async fn active_identity_count(&self, id: ClusterId) -> usize {
            self.state.read().await.identities.get(&id).map_or(0, |identities| {
                identities.iter().filter(|(_, _, revoked)| !revoked).count()
            })
        }

        pub(crate) async fn capability_count(&self, id: ClusterId) -> usize {
            self.state.read().await.capabilities.get(&id).map_or(0, Vec::len)
        }
    }

    impl ClusterRepository for InMemoryRepository {
        async fn ping(&self) -> Result<(), ControlPlaneError> {
            Ok(())
        }

        async fn onboard(&self, request: &OnboardClusterRequest) -> Result<OnboardOutcome, ControlPlaneError> {
            let mut state = self.state.write().await;
            let key = (request.tenant_id.clone(), request.external_cluster_key.clone());
            if let Some(id) = state.external_keys.get(&key) {
                return Ok(OnboardOutcome {
                    cluster: state.clusters.get(id).cloned().ok_or(ControlPlaneError::NotFound)?,
                    created: false,
                });
            }
            let now = Utc::now();
            let cluster = Cluster {
                id: ClusterId::new(),
                tenant_id: request.tenant_id.clone(),
                external_cluster_key: request.external_cluster_key.clone(),
                environment: request.environment.clone(),
                region: request.region.clone(),
                rocketmq_version: request.rocketmq_version.clone(),
                deployment_mode: request.deployment_mode.clone(),
                owner: request.owner.clone(),
                state: OnboardingState::Pending,
                effective_access_profile: "read_only",
                created_at: now,
                updated_at: now,
                offboarded_at: None,
            };
            state.external_keys.insert(key, cluster.id);
            state.clusters.insert(cluster.id, cluster.clone());
            state.events.push((cluster.id, "cluster_onboarded".to_owned()));
            Ok(OnboardOutcome { cluster, created: true })
        }

        async fn list(&self) -> Result<Vec<Cluster>, ControlPlaneError> {
            Ok(self.state.read().await.clusters.values().cloned().collect())
        }

        async fn get(&self, id: ClusterId) -> Result<Cluster, ControlPlaneError> {
            self.state
                .read()
                .await
                .clusters
                .get(&id)
                .cloned()
                .ok_or(ControlPlaneError::NotFound)
        }

        async fn handshake(
            &self,
            id: ClusterId,
            request: &HandshakeRequest,
            decision: &HandshakeDecision,
        ) -> Result<HandshakeOutcome, ControlPlaneError> {
            let mut state = self.state.write().await;
            let current = state.clusters.get(&id).cloned().ok_or(ControlPlaneError::NotFound)?;
            if current.state.is_terminal() {
                return Err(ControlPlaneError::conflict("offboarded clusters cannot be handshaken"));
            }
            let reported_tool_surface_digest = request.capability.tool_surface_digest()?;
            let pinned_tool_surface_digest = state
                .capabilities
                .get(&id)
                .and_then(|capabilities| capabilities.first())
                .map(|capability| capability.tool_surface_digest.as_str());
            let effective_decision =
                enforce_tool_surface_pin(decision, pinned_tool_surface_digest, reported_tool_surface_digest);
            let existing_capability = state
                .capabilities
                .get(&id)
                .and_then(|capabilities| capabilities.last())
                .cloned();
            if current.state == effective_decision.state
                && existing_capability
                    .as_ref()
                    .is_some_and(|capability| capability_matches_request(capability, request))
            {
                return Ok(HandshakeOutcome {
                    cluster: current,
                    capability: existing_capability,
                    reason: effective_decision.reason,
                });
            }

            state.events.push((id, "cluster_handshake_started".to_owned()));
            let capability = effective_decision.persist_capability.then(|| CapabilitySnapshot {
                cluster_id: id,
                digest: request.capability.digest.clone(),
                tool_surface_digest: reported_tool_surface_digest.to_owned(),
                protocol_version: request.capability.protocol_version.clone(),
                schema_version: request.capability.schema_version.clone(),
                mutation_supported: request.capability.mutation_supported,
                observed_at: request.capability.observed_at,
                data_sources: normalized_data_sources(&request.capability.data_sources),
                manifest: request.capability.manifest.clone(),
            });
            if let Some(capability) = &capability {
                let capabilities = state.capabilities.entry(id).or_default();
                if !capabilities
                    .last()
                    .is_some_and(|stored| capability_matches_request(stored, request))
                {
                    capabilities.push(capability.clone());
                }
            }
            if !request.capability.mutation_supported {
                let identities = state.identities.entry(id).or_default();
                if let Some(identity) = identities.iter_mut().find(|(subject, issuer, _)| {
                    subject == &request.connector_subject && issuer == &request.connector_issuer
                }) {
                    identity.2 = false;
                } else {
                    identities.push((
                        request.connector_subject.clone(),
                        request.connector_issuer.clone(),
                        false,
                    ));
                }
            }
            let cluster = state.clusters.get_mut(&id).ok_or(ControlPlaneError::NotFound)?;
            cluster.state = effective_decision.state;
            cluster.updated_at = Utc::now();
            let cluster = cluster.clone();
            state.events.push((id, "cluster_handshake_completed".to_owned()));
            Ok(HandshakeOutcome {
                cluster,
                capability,
                reason: effective_decision.reason,
            })
        }

        async fn capability(&self, id: ClusterId) -> Result<CapabilitySnapshot, ControlPlaneError> {
            self.state
                .read()
                .await
                .capabilities
                .get(&id)
                .and_then(|capabilities| capabilities.last())
                .cloned()
                .ok_or(ControlPlaneError::NotFound)
        }

        async fn offboard(&self, id: ClusterId, request: &OffboardRequest) -> Result<Cluster, ControlPlaneError> {
            if request.actor_subject.trim().is_empty() {
                return Err(ControlPlaneError::validation(
                    "unauthorized_scope",
                    "actor_subject must not be empty",
                ));
            }
            let mut state = self.state.write().await;
            let current = state.clusters.get(&id).cloned().ok_or(ControlPlaneError::NotFound)?;
            if current.state == OnboardingState::Offboarded {
                return Ok(current);
            }
            if let Some(identities) = state.identities.get_mut(&id) {
                for (_, _, revoked) in identities {
                    *revoked = true;
                }
            }
            let cluster = state.clusters.get_mut(&id).ok_or(ControlPlaneError::NotFound)?;
            cluster.state = OnboardingState::Offboarded;
            cluster.offboarded_at = Some(Utc::now());
            cluster.updated_at = Utc::now();
            let cluster = cluster.clone();
            state.events.push((id, "cluster_offboarded".to_owned()));
            Ok(cluster)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::memory::InMemoryRepository;
    use super::*;
    use crate::MCP_BUSINESS_SCHEMA;
    use crate::MCP_PROTOCOL_VERSION;
    use crate::model::HandshakeCapability;

    fn onboard_request() -> OnboardClusterRequest {
        OnboardClusterRequest {
            cluster_id: None,
            tenant_id: "tenant-a".to_owned(),
            external_cluster_key: "cluster-a".to_owned(),
            environment: "dev".to_owned(),
            region: "local".to_owned(),
            rocketmq_version: "5.x".to_owned(),
            deployment_mode: "local".to_owned(),
            owner: "sre".to_owned(),
            actor_subject: "test".to_owned(),
            correlation_id: None,
        }
    }

    fn handshake_request() -> HandshakeRequest {
        HandshakeRequest {
            connector_subject: "connector".to_owned(),
            connector_issuer: "test-issuer".to_owned(),
            correlation_id: None,
            capability: HandshakeCapability {
                digest: format!("sha256:{}", "1".repeat(64)),
                protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
                schema_version: MCP_BUSINESS_SCHEMA.to_owned(),
                mutation_supported: false,
                manifest: json!({
                    "mutation_supported": false,
                    "tool_surface_digest": format!("sha256:{}", "a".repeat(64))
                }),
                data_sources: Vec::new(),
                observed_at: Utc::now(),
            },
            compatible: true,
            incompatibility_code: None,
        }
    }

    #[tokio::test]
    async fn onboarding_and_same_digest_handshake_are_idempotent() {
        let repository = InMemoryRepository::default();
        let first = repository
            .onboard(&onboard_request())
            .await
            .expect("cluster should be onboarded");
        let second = repository
            .onboard(&onboard_request())
            .await
            .expect("duplicate should return existing cluster");
        assert!(first.created);
        assert!(!second.created);
        assert_eq!(first.cluster.id, second.cluster.id);

        let request = handshake_request();
        let decision = request.validate().expect("handshake should validate");
        let first_handshake = repository
            .handshake(first.cluster.id, &request, &decision)
            .await
            .expect("handshake should complete");
        let events_after_first = repository.event_count().await;
        let second_handshake = repository
            .handshake(first.cluster.id, &request, &decision)
            .await
            .expect("same digest should be idempotent");

        assert_eq!(first_handshake.cluster.state, OnboardingState::ReadyReadOnly);
        assert_eq!(second_handshake.cluster, first_handshake.cluster);
        assert_eq!(repository.event_count().await, events_after_first);
        assert_eq!(repository.active_identity_count(first.cluster.id).await, 1);
    }

    #[tokio::test]
    async fn source_state_changes_append_without_changing_the_surface_pin() {
        let repository = InMemoryRepository::default();
        let cluster = repository
            .onboard(&onboard_request())
            .await
            .expect("cluster should be onboarded")
            .cluster;
        let first = handshake_request();
        let first_decision = first.validate().expect("initial handshake should validate");
        repository
            .handshake(cluster.id, &first, &first_decision)
            .await
            .expect("initial handshake should complete");

        let mut source_change = first.clone();
        source_change.capability.data_sources.push(crate::DataSourceStatus {
            id: "prometheus".to_owned(),
            availability: crate::DataSourceAvailability::Queryable,
            freshness_ms: Some(0),
            detail: Some("source recovered".to_owned()),
        });
        source_change.capability.data_sources.push(crate::DataSourceStatus {
            id: "loki".to_owned(),
            availability: crate::DataSourceAvailability::Queryable,
            freshness_ms: Some(0),
            detail: Some("source recovered".to_owned()),
        });
        let source_decision = source_change.validate().expect("source change should validate");
        let outcome = repository
            .handshake(cluster.id, &source_change, &source_decision)
            .await
            .expect("source change should append");

        assert_eq!(outcome.cluster.state, OnboardingState::ReadyReadOnly);
        assert_eq!(outcome.reason, None);
        assert_eq!(repository.capability_count(cluster.id).await, 2);
        assert_eq!(
            outcome.capability.expect("latest capability").tool_surface_digest,
            format!("sha256:{}", "a".repeat(64))
        );

        let events_after_change = repository.event_count().await;
        source_change.capability.data_sources.reverse();
        repository
            .handshake(cluster.id, &source_change, &source_decision)
            .await
            .expect("source ordering alone should be idempotent");
        assert_eq!(repository.capability_count(cluster.id).await, 2);
        assert_eq!(repository.event_count().await, events_after_change);

        for source in &mut source_change.capability.data_sources {
            source.freshness_ms = Some(999);
        }
        repository
            .handshake(cluster.id, &source_change, &source_decision)
            .await
            .expect("freshness alone should be carried by source history");
        assert_eq!(repository.capability_count(cluster.id).await, 2);
        assert_eq!(repository.event_count().await, events_after_change);
    }

    #[tokio::test]
    async fn persisted_surface_pin_rejects_drift_after_repository_restart() {
        let repository = InMemoryRepository::default();
        let cluster = repository
            .onboard(&onboard_request())
            .await
            .expect("cluster should be onboarded")
            .cluster;
        let initial = handshake_request();
        let initial_decision = initial.validate().expect("initial handshake should validate");
        repository
            .handshake(cluster.id, &initial, &initial_decision)
            .await
            .expect("initial handshake should complete");

        let restarted_repository = repository.clone();
        drop(repository);
        let mut drift = initial;
        drift.capability.digest = format!("sha256:{}", "3".repeat(64));
        drift.capability.manifest["tool_surface_digest"] = json!(format!("sha256:{}", "b".repeat(64)));
        let drift_decision = drift.validate().expect("drift report should be structurally valid");
        let outcome = restarted_repository
            .handshake(cluster.id, &drift, &drift_decision)
            .await
            .expect("drift must be persisted as a degraded snapshot");

        assert_eq!(outcome.cluster.state, OnboardingState::ReadOnlyDegraded);
        assert_eq!(outcome.reason.as_deref(), Some("schema_digest_mismatch"));
        assert_eq!(restarted_repository.capability_count(cluster.id).await, 2);

        let second_restart = restarted_repository.clone();
        let events_before_retry = second_restart.event_count().await;
        let retry = second_restart
            .handshake(cluster.id, &drift, &drift_decision)
            .await
            .expect("same drift report should be idempotently degraded");
        assert_eq!(retry.cluster.state, OnboardingState::ReadOnlyDegraded);
        assert_eq!(retry.reason.as_deref(), Some("schema_digest_mismatch"));
        assert_eq!(second_restart.capability_count(cluster.id).await, 2);
        assert_eq!(second_restart.event_count().await, events_before_retry);
    }

    #[tokio::test]
    async fn offboarding_revokes_identity_and_blocks_new_handshake() {
        let repository = InMemoryRepository::default();
        let cluster = repository
            .onboard(&onboard_request())
            .await
            .expect("cluster should be onboarded")
            .cluster;
        let request = handshake_request();
        let decision = request.validate().expect("handshake should validate");
        repository
            .handshake(cluster.id, &request, &decision)
            .await
            .expect("handshake should complete");

        let offboarded = repository
            .offboard(cluster.id, &OffboardRequest::default())
            .await
            .expect("offboard should complete");

        assert_eq!(offboarded.state, OnboardingState::Offboarded);
        assert_eq!(repository.active_identity_count(cluster.id).await, 0);
        assert!(repository.handshake(cluster.id, &request, &decision).await.is_err());
    }
}
