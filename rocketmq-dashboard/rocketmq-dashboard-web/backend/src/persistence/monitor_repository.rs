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
use crate::model::AuditEvent;
use crate::model::ConsumerMonitorRule;
use crate::model::EnvironmentId;
use crate::persistence::DashboardPersistence;
use crate::persistence::Revision;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;
use crate::persistence::file_store::FilePersistence;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use serde_json::to_value;

/// Persistence contract for environment-scoped monitor rules.
#[allow(async_fn_in_trait)]
pub trait MonitorRepository {
    async fn list_monitor_rules(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<Vec<ConsumerMonitorRule>, PersistenceError>;
    async fn upsert_monitor_rule(
        &self,
        rule: ConsumerMonitorRule,
        expected_revision: Revision,
    ) -> Result<ConsumerMonitorRule, PersistenceError>;
    async fn delete_monitor_rule(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError>;
}

impl DashboardPersistence {
    pub async fn list_monitor_rules(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.list_monitor_rules(environment_id).await,
            PersistenceBackend::Sql(store) => store.list_monitor_rules(environment_id).await,
        }
    }

    pub async fn upsert_monitor_rule(
        &self,
        rule: ConsumerMonitorRule,
        expected_revision: Revision,
    ) -> Result<ConsumerMonitorRule, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.upsert_monitor_rule(rule, expected_revision).await,
            PersistenceBackend::Sql(store) => store.upsert_monitor_rule(rule, expected_revision).await,
        }
    }

    pub async fn upsert_monitor_rule_with_audit(
        &self,
        rule: ConsumerMonitorRule,
        expected_revision: Revision,
        audit: AuditEvent,
    ) -> Result<ConsumerMonitorRule, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => {
                store
                    .upsert_monitor_rule_with_audit(rule, expected_revision, audit)
                    .await
            }
            PersistenceBackend::Sql(store) => {
                store
                    .upsert_monitor_rule_with_audit(rule, expected_revision, audit)
                    .await
            }
        }
    }

    pub async fn delete_monitor_rule(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => {
                store
                    .delete_monitor_rule(environment_id, consumer_group, expected_revision)
                    .await
            }
            PersistenceBackend::Sql(store) => {
                store
                    .delete_monitor_rule(environment_id, consumer_group, expected_revision)
                    .await
            }
        }
    }

    pub async fn delete_monitor_rule_with_audit(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => {
                store
                    .delete_monitor_rule_with_audit(environment_id, consumer_group, expected_revision, audit)
                    .await
            }
            PersistenceBackend::Sql(store) => {
                store
                    .delete_monitor_rule_with_audit(environment_id, consumer_group, expected_revision, audit)
                    .await
            }
        }
    }
}

impl MonitorRepository for DashboardPersistence {
    async fn list_monitor_rules(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
        Self::list_monitor_rules(self, environment_id).await
    }

    async fn upsert_monitor_rule(
        &self,
        rule: ConsumerMonitorRule,
        expected_revision: Revision,
    ) -> Result<ConsumerMonitorRule, PersistenceError> {
        Self::upsert_monitor_rule(self, rule, expected_revision).await
    }

    async fn delete_monitor_rule(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        Self::delete_monitor_rule(self, environment_id, consumer_group, expected_revision).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct FileMonitorSnapshot {
    rules: Vec<ConsumerMonitorRule>,
}

impl FilePersistence {
    pub(crate) async fn list_monitor_rules(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<Vec<ConsumerMonitorRule>, PersistenceError> {
        let _read_guard = self.read_guard().await;
        self.load_environment_locked(environment_id).await?;
        let snapshot = self.load_latest_snapshot(&monitor_collection(environment_id)).await?;
        let mut rules = match snapshot {
            Some(snapshot) => {
                serde_json::from_value::<FileMonitorSnapshot>(snapshot.payload)
                    .map_err(|_| PersistenceError::CorruptedData)?
                    .rules
            }
            None => Vec::new(),
        };
        rules.sort_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
        Ok(rules)
    }

    pub(crate) async fn upsert_monitor_rule(
        &self,
        mut rule: ConsumerMonitorRule,
        expected_revision: Revision,
    ) -> Result<ConsumerMonitorRule, PersistenceError> {
        rule.validate().map_err(PersistenceError::InvalidConfig)?;
        let _write_guard = self.write_guard().await;
        self.load_environment_locked(&rule.environment_id).await?;
        let collection = monitor_collection(&rule.environment_id);
        let current = self.load_latest_snapshot(&collection).await?;
        let snapshot_revision = current.as_ref().map_or(0, |snapshot| snapshot.revision);
        let mut snapshot: FileMonitorSnapshot = current
            .map(|snapshot| serde_json::from_value(snapshot.payload).map_err(|_| PersistenceError::CorruptedData))
            .transpose()?
            .unwrap_or_default();
        let now_ms = Utc::now().timestamp_millis();
        if let Some(existing) = snapshot
            .rules
            .iter_mut()
            .find(|existing| existing.consumer_group == rule.consumer_group)
        {
            if expected_revision != existing.revision {
                return Err(PersistenceError::Conflict);
            }
            rule.revision = Revision(existing.revision.0.checked_add(1).ok_or(PersistenceError::Conflict)?);
            rule.created_at_ms = existing.created_at_ms;
            rule.updated_at_ms = now_ms;
            *existing = rule.clone();
        } else {
            if expected_revision != Revision(0) {
                return Err(PersistenceError::Conflict);
            }
            rule.revision = Revision(1);
            rule.created_at_ms = now_ms;
            rule.updated_at_ms = now_ms;
            snapshot.rules.push(rule.clone());
        }
        snapshot
            .rules
            .sort_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
        self.compare_and_write_snapshot_locked(
            _write_guard,
            &collection,
            snapshot_revision,
            to_value(snapshot).map_err(PersistenceError::Serialization)?,
        )
        .await?;
        Ok(rule)
    }

    pub(crate) async fn upsert_monitor_rule_with_audit(
        &self,
        mut rule: ConsumerMonitorRule,
        expected_revision: Revision,
        audit: AuditEvent,
    ) -> Result<ConsumerMonitorRule, PersistenceError> {
        rule.validate().map_err(PersistenceError::InvalidConfig)?;
        let write_guard = self.write_guard().await;
        self.load_environment_locked(&rule.environment_id).await?;
        let collection = monitor_collection(&rule.environment_id);
        let current = self.load_latest_snapshot(&collection).await?;
        let snapshot_revision = current.as_ref().map_or(0, |snapshot| snapshot.revision);
        let mut snapshot: FileMonitorSnapshot = current
            .map(|snapshot| serde_json::from_value(snapshot.payload).map_err(|_| PersistenceError::CorruptedData))
            .transpose()?
            .unwrap_or_default();
        let now_ms = Utc::now().timestamp_millis();
        if let Some(existing) = snapshot
            .rules
            .iter_mut()
            .find(|existing| existing.consumer_group == rule.consumer_group)
        {
            if expected_revision != existing.revision {
                return Err(PersistenceError::Conflict);
            }
            rule.revision = Revision(existing.revision.0.checked_add(1).ok_or(PersistenceError::Conflict)?);
            rule.created_at_ms = existing.created_at_ms;
            rule.updated_at_ms = now_ms;
            *existing = rule.clone();
        } else {
            if expected_revision != Revision(0) {
                return Err(PersistenceError::Conflict);
            }
            rule.revision = Revision(1);
            rule.created_at_ms = now_ms;
            rule.updated_at_ms = now_ms;
            snapshot.rules.push(rule.clone());
        }
        snapshot
            .rules
            .sort_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
        self.publish_snapshot_transaction_with_audit_locked(
            write_guard,
            vec![crate::persistence::file_store::FileSnapshotTransactionWrite {
                collection,
                expected_revision: snapshot_revision,
                payload: to_value(snapshot).map_err(PersistenceError::Serialization)?,
            }],
            audit,
        )
        .await?;
        Ok(rule)
    }

    pub(crate) async fn delete_monitor_rule(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        let group = consumer_group.trim();
        if group.is_empty() {
            return Err(PersistenceError::InvalidConfig(
                "consumer group is required".to_string(),
            ));
        }
        let _write_guard = self.write_guard().await;
        self.load_environment_locked(environment_id).await?;
        let collection = monitor_collection(environment_id);
        let Some(current) = self.load_latest_snapshot(&collection).await? else {
            return Ok(false);
        };
        let mut snapshot: FileMonitorSnapshot =
            serde_json::from_value(current.payload).map_err(|_| PersistenceError::CorruptedData)?;
        let Some(index) = snapshot.rules.iter().position(|rule| rule.consumer_group == group) else {
            return Ok(false);
        };
        if snapshot.rules[index].revision != expected_revision {
            return Err(PersistenceError::Conflict);
        }
        snapshot.rules.remove(index);
        self.compare_and_write_snapshot_locked(
            _write_guard,
            &collection,
            current.revision,
            to_value(snapshot).map_err(PersistenceError::Serialization)?,
        )
        .await?;
        Ok(true)
    }

    pub(crate) async fn delete_monitor_rule_with_audit(
        &self,
        environment_id: &EnvironmentId,
        consumer_group: &str,
        expected_revision: Revision,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError> {
        let group = consumer_group.trim();
        if group.is_empty() {
            return Err(PersistenceError::InvalidConfig(
                "consumer group is required".to_string(),
            ));
        }
        let write_guard = self.write_guard().await;
        self.load_environment_locked(environment_id).await?;
        let collection = monitor_collection(environment_id);
        let Some(current) = self.load_latest_snapshot(&collection).await? else {
            return Ok(false);
        };
        let mut snapshot: FileMonitorSnapshot =
            serde_json::from_value(current.payload).map_err(|_| PersistenceError::CorruptedData)?;
        let Some(index) = snapshot.rules.iter().position(|rule| rule.consumer_group == group) else {
            return Ok(false);
        };
        if snapshot.rules[index].revision != expected_revision {
            return Err(PersistenceError::Conflict);
        }
        snapshot.rules.remove(index);
        self.publish_snapshot_transaction_with_audit_locked(
            write_guard,
            vec![crate::persistence::file_store::FileSnapshotTransactionWrite {
                collection,
                expected_revision: current.revision,
                payload: to_value(snapshot).map_err(PersistenceError::Serialization)?,
            }],
            audit,
        )
        .await?;
        Ok(true)
    }
}

pub(crate) fn monitor_collection(environment_id: &EnvironmentId) -> String {
    format!("monitors/{}", environment_id.0)
}

pub(crate) fn empty_monitor_snapshot_value() -> Result<serde_json::Value, PersistenceError> {
    to_value(FileMonitorSnapshot::default()).map_err(PersistenceError::Serialization)
}
