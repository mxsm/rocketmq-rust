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

use rocketmq_sre_contracts::AutonomyOutcome;
use serde_json::Value;

use crate::ControlPlaneError;
use crate::PostgresRepository;

const RECONCILE_BATCH_LIMIT: i64 = 64;
const RECONCILER_ACTOR: &str = "system:autonomy-pause-reconciler";

/// Bounded result for one autonomy pause repair scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AutonomyReconcileSummary {
    pub(crate) candidates: u32,
    pub(crate) repaired: u32,
}

/// Repairs autonomous failure outcomes whose lifecycle pause was interrupted.
#[derive(Clone, Debug)]
pub(crate) struct AutonomyPauseReconciler {
    repository: PostgresRepository,
}

impl AutonomyPauseReconciler {
    pub(crate) const fn new(repository: PostgresRepository) -> Self {
        Self { repository }
    }

    /// Replays one bounded page of durable failure outcomes.
    ///
    /// The normal ingestion path writes the outcome, lifecycle pause, and
    /// outbox event in one transaction. This scan is the recovery path for
    /// legacy rows, interrupted deployments, or externally restored data.
    ///
    /// # Errors
    ///
    /// Returns a database error or rejects an incompatible stored snapshot.
    pub(crate) async fn run_once(&self) -> Result<AutonomyReconcileSummary, ControlPlaneError> {
        let snapshots = sqlx::query_scalar::<_, Value>(
            "SELECT outcome.outcome_snapshot
             FROM autonomy_outcomes AS outcome
             INNER JOIN autonomy_lifecycle_states AS lifecycle
                ON lifecycle.tenant_id = outcome.tenant_id
               AND lifecycle.cluster_id = outcome.cluster_id
               AND lifecycle.action_id = outcome.action_id
               AND lifecycle.action_version = outcome.action_version
             WHERE outcome.outcome_class = 'autonomous_execution_failure'
               AND lifecycle.mode <> 'paused'
               AND NOT EXISTS (
                   SELECT 1
                   FROM autonomy_outbox AS pause_event
                   WHERE pause_event.outcome_id = outcome.id
                     AND pause_event.event_kind = 'autonomy_paused'
               )
             ORDER BY outcome.reconciled_at, outcome.sequence_id
             LIMIT $1",
        )
        .bind(RECONCILE_BATCH_LIMIT)
        .fetch_all(&self.repository.pool)
        .await?;
        let candidates = u32::try_from(snapshots.len()).unwrap_or(u32::MAX);
        let mut repaired = 0_u32;
        for snapshot in snapshots {
            let outcome: AutonomyOutcome = serde_json::from_value(snapshot).map_err(|_| {
                ControlPlaneError::configuration(
                    "stored autonomy outcome snapshot is incompatible with the current schema",
                )
            })?;
            self.repository
                .record_autonomy_outcome(&outcome, RECONCILER_ACTOR)
                .await?;
            repaired = repaired.saturating_add(1);
        }
        Ok(AutonomyReconcileSummary { candidates, repaired })
    }
}
