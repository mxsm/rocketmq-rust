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

use super::*;
use rusqlite::{Connection, TransactionBehavior, params};
const MAX_INTEGER: i64 = 9_007_199_254_740_991;

pub(super) fn list(connection: &Connection, environment: &str) -> DashboardResult<Vec<MonitorRule>> {
    let mut statement = connection.prepare("SELECT consumer_group, min_count, max_diff_total, revision, created_at_ms, updated_at_ms FROM consumer_monitor_rules WHERE environment_id = ?1 ORDER BY consumer_group")?;
    let rows = statement.query_map([environment], |row| {
        Ok(MonitorRule {
            consumer_group: row.get(0)?,
            min_count: row.get(1)?,
            max_diff_total: row.get(2)?,
            revision: row.get(3)?,
            created_at_ms: row.get(4)?,
            updated_at_ms: row.get(5)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

pub(super) fn change(
    connection: &mut Connection,
    environment: &str,
    change: Change,
    audit: &AuditContext,
) -> DashboardResult<()> {
    let (group, revision) = match &change {
        Change::Save(request) => {
            if !(0..=MAX_INTEGER).contains(&request.min_count) || !(0..=MAX_INTEGER).contains(&request.max_diff_total) {
                return Err(DashboardError::Validation(
                    "Thresholds must be nonnegative safe integers.".into(),
                ));
            }
            (&request.consumer_group, request.expected_revision)
        }
        Change::Delete(request) => (&request.consumer_group, request.expected_revision),
    };
    if environment.is_empty()
        || group.is_empty()
        || group.len() > 255
        || group.trim() != group
        || group.chars().any(|c| c.is_control() || c.is_whitespace())
        || !(0..MAX_INTEGER).contains(&revision)
    {
        return Err(DashboardError::Validation(
            "Invalid monitor identity or revision.".into(),
        ));
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let now = chrono::Utc::now().timestamp_millis();
    let changed = match &change {
        Change::Save(request) if revision == 0 => transaction.execute("INSERT INTO consumer_monitor_rules(environment_id,consumer_group,min_count,max_diff_total,revision,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,1,?5,?5) ON CONFLICT(environment_id,consumer_group) DO NOTHING", params![environment, group, request.min_count, request.max_diff_total, now])?,
        Change::Save(request) => transaction.execute("UPDATE consumer_monitor_rules SET min_count=?3,max_diff_total=?4,revision=revision+1,updated_at_ms=?5 WHERE environment_id=?1 AND consumer_group=?2 AND revision=?6", params![environment,group,request.min_count,request.max_diff_total,now,revision])?,
        Change::Delete(_) => transaction.execute("DELETE FROM consumer_monitor_rules WHERE environment_id=?1 AND consumer_group=?2 AND revision=?3", params![environment,group,revision])?,
    };
    if changed != 1 {
        return Err(DashboardError::MonitorConflict);
    }
    // Failure to persist the audit must roll back the rule too.
    audit.record_local_success(&transaction)?;
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn audit() -> AuditContext {
        AuditContext {
            actor: Some("tester".into()),
            environment: std::sync::Arc::new(std::sync::Mutex::new(Some("env-a".into()))),
            event_id: uuid::Uuid::new_v4().to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            action: AuditAction::SaveMonitor,
            resource_name: Some("group".into()),
        }
    }
    fn save(revision: i64, threshold: i64) -> Change {
        Change::Save(SaveRule {
            consumer_group: "group".into(),
            min_count: threshold,
            max_diff_total: 100,
            expected_revision: revision,
        })
    }
    #[test]
    fn monitor_rules_reopen_isolate_compare_and_swap_and_audit_atomically() {
        let path = std::env::temp_dir().join(format!("monitor-{}.db", uuid::Uuid::new_v4()));
        let mut db = Connection::open(&path).unwrap();
        crate::persistence::schema::initialize(&mut db).unwrap();
        change(&mut db, "env-a", save(0, 2), &audit()).unwrap();
        change(&mut db, "env-b", save(0, 7), &audit()).unwrap();
        assert!(change(&mut db, "env-a", save(0, 9), &audit()).is_err());
        assert!(change(&mut db, "env-a", save(1, -1), &audit()).is_err());
        change(&mut db, "env-a", save(1, 3), &audit()).unwrap();
        assert!(matches!(
            change(&mut db, "env-a", save(1, 9), &audit()),
            Err(DashboardError::MonitorConflict)
        ));
        let mut invalid_audit = audit();
        invalid_audit.actor = None;
        assert!(change(&mut db, "env-a", save(2, 9), &invalid_audit).is_err());
        drop(db);
        let mut db = Connection::open(&path).unwrap();
        let rules = list(&db, "env-a").unwrap();
        assert_eq!((rules[0].revision, rules[0].min_count), (2, 3));
        assert_eq!(list(&db, "env-b").unwrap()[0].min_count, 7);
        assert_eq!(
            db.query_row("SELECT count(*) FROM audit_events", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert!(
            change(
                &mut db,
                "env-a",
                Change::Delete(DeleteRule {
                    consumer_group: "group".into(),
                    expected_revision: 1
                }),
                &audit()
            )
            .is_err()
        );
        change(
            &mut db,
            "env-a",
            Change::Delete(DeleteRule {
                consumer_group: "group".into(),
                expected_revision: 2,
            }),
            &audit(),
        )
        .unwrap();
        assert!(list(&db, "env-a").unwrap().is_empty());
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}
