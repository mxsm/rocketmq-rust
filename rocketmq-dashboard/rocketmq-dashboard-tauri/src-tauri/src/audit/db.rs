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

use super::types::*;
use crate::error::{DashboardError, DashboardResult};
use rusqlite::{Connection, params};
use uuid::Uuid;

pub(super) fn insert(connection: &Connection, event: &AuditEvent) -> DashboardResult<()> {
    connection.execute("INSERT INTO audit_events(event_id, request_id, actor, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) ON CONFLICT(event_id) DO NOTHING",
        params![event.event_id, event.request_id, event.actor, event.action, event.resource_type, event.resource_name, event.environment_id, event.outcome, serde_json::to_string(&event.detail)?, event.created_at_ms])?;
    Ok(())
}

pub(super) fn query(connection: &Connection, query: AuditQuery) -> DashboardResult<AuditPage> {
    let limit = query.limit.unwrap_or(50);
    if !(1..=200).contains(&limit) || matches!((query.from_ms, query.to_ms), (Some(from), Some(to)) if from > to) {
        return Err(DashboardError::Validation("invalid audit query".into()));
    }
    let cursor = query
        .cursor
        .as_ref()
        .map(|value| {
            let (time, id) = value
                .split_once(':')
                .ok_or_else(|| DashboardError::Validation("invalid audit cursor".into()))?;
            let time = time
                .parse::<i64>()
                .map_err(|_| DashboardError::Validation("invalid audit cursor".into()))?;
            Uuid::parse_str(id).map_err(|_| DashboardError::Validation("invalid audit cursor".into()))?;
            Ok::<_, DashboardError>((time, id))
        })
        .transpose()?;
    let mut statement = connection.prepare("SELECT event_id, request_id, actor, action, resource_type, resource_name, environment_id, outcome, detail_json, created_at_ms FROM audit_events
        WHERE (?1 IS NULL OR created_at_ms >= ?1) AND (?2 IS NULL OR created_at_ms <= ?2)
        AND (?3 IS NULL OR actor = ?3) AND (?4 IS NULL OR action = ?4)
        AND (?5 IS NULL OR outcome = ?5) AND (?6 IS NULL OR environment_id = ?6)
        AND (?7 IS NULL OR created_at_ms < ?7 OR (created_at_ms = ?7 AND event_id < ?8))
        ORDER BY created_at_ms DESC, event_id DESC LIMIT ?9")?;
    let rows = statement.query_map(
        params![
            query.from_ms,
            query.to_ms,
            query.actor,
            query.action,
            query.outcome.map(Outcome::as_str),
            query.environment_id,
            cursor.map(|(time, _)| time),
            cursor.map(|(_, id)| id),
            (limit + 1) as i64
        ],
        |row| {
            let json: String = row.get(8)?;
            Ok((
                AuditEvent {
                    event_id: row.get(0)?,
                    request_id: row.get(1)?,
                    actor: row.get(2)?,
                    action: row.get(3)?,
                    resource_type: row.get(4)?,
                    resource_name: row.get(5)?,
                    environment_id: row.get(6)?,
                    outcome: row.get(7)?,
                    detail: AuditDetail {
                        result_unknown: false,
                        error_code: None,
                        success_count: None,
                        failure_count: None,
                    },
                    created_at_ms: row.get(9)?,
                },
                json,
            ))
        },
    )?;
    let mut items = Vec::new();
    for row in rows {
        let (mut event, json) = row?;
        event.detail = serde_json::from_str(&json)?;
        items.push(event);
    }
    let next_cursor = if items.len() > limit {
        items.truncate(limit);
        items
            .last()
            .map(|event| format!("{}:{}", event.created_at_ms, event.event_id))
    } else {
        None
    };
    Ok(AuditPage { items, next_cursor })
}
