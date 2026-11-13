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

use rocketmq_sre_contracts::{
    INVESTIGATION_RUN_EVENT_PAGE_SCHEMA_VERSION, INVESTIGATION_RUN_EVENT_SCHEMA_VERSION, InvestigationCursorRecovery,
    InvestigationEventSequence, InvestigationRunEvent, InvestigationRunEventKind, InvestigationRunEventPage,
};

use super::*;

const RETAINED_EVENTS: i64 = 256;

/// The caller holds the run's row lock. Snapshot, fencing metadata, sequence and
/// event are committed together; no event is emitted for an exact replay.
pub(super) async fn persist_event(
    tx: &mut Transaction<'_, Postgres>,
    state: &mut LockedRun,
    kind: InvestigationRunEventKind,
    actor: &str,
    at: DateTime<Utc>,
) -> Result<(), ControlPlaneRequestFailure> {
    state.run.validate().map_err(contract_failure)?;
    let next = state
        .head
        .checked_add(1)
        .ok_or_else(|| contract_failure(InvestigationRunFailure::CounterExhausted))?;
    let floor = state.floor.max(next.saturating_sub(RETAINED_EVENTS - 1)).max(1);
    let event = InvestigationRunEvent {
        schema_version: INVESTIGATION_RUN_EVENT_SCHEMA_VERSION.to_owned(),
        tenant_id: state.run.initial_scope.tenant_id,
        cluster_id: state.run.initial_scope.cluster_id,
        investigation_id: state.run.initial_scope.investigation_id,
        run_id: state.run.id,
        sequence: InvestigationEventSequence::from_u64(next as u64),
        revision: state.run.revision,
        control_epoch: state.run.control_epoch,
        status: state.run.status,
        actor_subject: actor.to_owned(),
        event: kind,
        occurred_at: at,
    };
    let updated = sqlx::query("UPDATE investigation_runs SET run_snapshot=$1,revision=$2,status=$3,event_sequence=$4,event_floor=$5,worker_owner=$6,worker_epoch=$7,worker_lease_until=$8,updated_at=$9 WHERE tenant_id=$10 AND id=$11 AND revision=$12 AND event_sequence=$13")
        .bind(encode(&state.run)?).bind(state.run.revision.get().to_string()).bind(status_name(state.run.status))
        .bind(next).bind(floor).bind(state.owner).bind(state.owner_epoch).bind(state.lease_until).bind(state.run.updated_at)
        .bind(state.run.initial_scope.tenant_id.as_uuid()).bind(state.run.id.as_uuid()).bind(&state.stored_revision).bind(state.head)
        .execute(&mut **tx).await?;
    if updated.rows_affected() != 1 {
        return Err(conflict("revision_conflict"));
    }
    sqlx::query("INSERT INTO investigation_run_events(tenant_id,run_id,sequence,event_snapshot,recorded_at) VALUES($1,$2,$3,$4,$5)")
        .bind(event.tenant_id.as_uuid()).bind(event.run_id.as_uuid()).bind(next).bind(encode(&event)?).bind(at)
        .execute(&mut **tx).await?;
    sqlx::query("DELETE FROM investigation_run_events WHERE tenant_id=$1 AND run_id=$2 AND sequence<$3")
        .bind(event.tenant_id.as_uuid())
        .bind(event.run_id.as_uuid())
        .bind(floor)
        .execute(&mut **tx)
        .await?;
    state.head = next;
    state.floor = floor;
    state.stored_revision = state.run.revision.get().to_string();
    Ok(())
}

impl PostgresRepository {
    pub(crate) async fn investigation_run_events(
        &self,
        auth: &AuthContext,
        id: InvestigationRunId,
        after: u64,
        limit: u32,
    ) -> Result<InvestigationRunEventPage, ControlPlaneRequestFailure> {
        if !(1..=100).contains(&limit) {
            return Err(invalid("event page limit is outside bounds"));
        }
        let mut tx = self.pool.begin().await?;
        // SHARE excludes concurrent mutations through the head and event read.
        let state = load_run(&mut tx, auth, id, RunLoadMode::ActiveRead).await?;
        let mut page = InvestigationRunEventPage {
            schema_version: INVESTIGATION_RUN_EVENT_PAGE_SCHEMA_VERSION.to_owned(),
            tenant_id: auth.tenant_id,
            cluster_id: state.run.initial_scope.cluster_id,
            investigation_id: state.run.initial_scope.investigation_id,
            run_id: id,
            events: vec![],
            earliest_sequence: InvestigationEventSequence::from_u64(state.floor as u64),
            latest_sequence: InvestigationEventSequence::from_u64(state.head as u64),
            next_sequence: InvestigationEventSequence::from_u64(after),
            has_more: false,
            snapshot_required: false,
            recovery_reason: None,
            current_snapshot: None,
        };
        let recovery = if after > state.head as u64 {
            Some(InvestigationCursorRecovery::CursorAhead)
        } else if after < (state.floor - 1) as u64 {
            Some(InvestigationCursorRecovery::CursorExpired)
        } else {
            let rows = sqlx::query("SELECT sequence,event_snapshot FROM investigation_run_events WHERE tenant_id=$1 AND run_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4")
                .bind(auth.tenant_id.as_uuid()).bind(id.as_uuid()).bind(after as i64).bind(i64::from(limit))
                .fetch_all(&mut *tx).await?;
            let mut expected = after;
            let mut gap = false;
            for row in rows {
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| contract_failure(InvestigationRunFailure::CounterExhausted))?;
                let sequence: i64 = row.try_get("sequence")?;
                let event: InvestigationRunEvent = decode(row.try_get("event_snapshot")?)?;
                if sequence as u64 != expected || event.sequence.get() != expected {
                    gap = true;
                    break;
                }
                page.events.push(event);
            }
            page.next_sequence = page
                .events
                .last()
                .map_or(InvestigationEventSequence::from_u64(after), |event| event.sequence);
            page.has_more = page.next_sequence < page.latest_sequence;
            if gap || (page.events.len() < limit as usize && page.has_more) {
                Some(InvestigationCursorRecovery::RetainedEventGap)
            } else {
                None
            }
        };
        if let Some(reason) = recovery {
            page.events.clear();
            page.next_sequence = page.latest_sequence;
            page.has_more = false;
            page.snapshot_required = true;
            page.recovery_reason = Some(reason);
            page.current_snapshot = Some(state.run);
        }
        page.validate()
            .map_err(|source| ControlPlaneRequestFailure::state_source("source_unavailable", source))?;
        tx.commit().await?;
        Ok(page)
    }
}
