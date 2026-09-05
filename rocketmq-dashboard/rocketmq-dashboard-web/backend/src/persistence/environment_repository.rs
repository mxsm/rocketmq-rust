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
use crate::model::DashboardEnvironment;
use crate::model::EndpointType;
use crate::model::EnvironmentId;
use crate::persistence::DashboardPersistence;
use crate::persistence::Revision;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;
use crate::persistence::file_store::FilePersistence;
use crate::persistence::file_store::FileSnapshotTransactionWrite;
use serde::Deserialize;
use serde::Serialize;
use serde_json::to_value;

/// Persistence contract for the Environment aggregate. Implementations must
/// replace endpoints and advance the aggregate revision atomically.
#[allow(async_fn_in_trait)]
pub trait EnvironmentRepository {
    async fn load_environment(&self, environment_id: &EnvironmentId) -> Result<DashboardEnvironment, PersistenceError>;
    async fn load_default_environment(&self) -> Result<Option<DashboardEnvironment>, PersistenceError>;
    async fn list_environments(&self) -> Result<Vec<DashboardEnvironment>, PersistenceError>;
    async fn create_environment(
        &self,
        environment: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError>;
    async fn update_environment(
        &self,
        expected_revision: Revision,
        candidate: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError>;
    async fn delete_environment(
        &self,
        environment_id: &EnvironmentId,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError>;
}

impl DashboardPersistence {
    pub async fn load_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.load_environment(environment_id).await,
            PersistenceBackend::Sql(store) => store.load_environment(environment_id).await,
        }
    }

    pub async fn load_default_environment(&self) -> Result<Option<DashboardEnvironment>, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.load_default_environment().await,
            PersistenceBackend::Sql(store) => store.load_environment_by_name("default").await,
        }
    }

    pub async fn list_environments(&self) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.list_environments().await,
            PersistenceBackend::Sql(store) => store.list_environments().await,
        }
    }

    pub async fn create_environment(
        &self,
        mut environment: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        sort_environment_endpoints(&mut environment);
        match &self.backend {
            PersistenceBackend::File(store) => store.create_environment(environment).await,
            PersistenceBackend::Sql(store) => store.create_environment(environment).await,
        }
    }

    pub async fn update_environment(
        &self,
        expected_revision: Revision,
        mut candidate: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        sort_environment_endpoints(&mut candidate);
        match &self.backend {
            PersistenceBackend::File(store) => store.update_environment(expected_revision, candidate).await,
            PersistenceBackend::Sql(store) => store.update_environment(expected_revision, candidate).await,
        }
    }

    /// Persists an environment revision and its terminal successful audit event
    /// as one storage decision. Callers must publish the returned aggregate
    /// only after this method succeeds.
    pub async fn update_environment_with_audit(
        &self,
        expected_revision: Revision,
        mut candidate: DashboardEnvironment,
        audit: AuditEvent,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        sort_environment_endpoints(&mut candidate);
        match &self.backend {
            PersistenceBackend::File(store) => {
                store
                    .update_environment_with_audit(expected_revision, candidate, audit)
                    .await
            }
            PersistenceBackend::Sql(store) => {
                store
                    .update_environment_with_audit(expected_revision, candidate, audit)
                    .await
            }
        }
    }

    pub async fn delete_environment(
        &self,
        environment_id: &EnvironmentId,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        if environment_id.0 == DEFAULT_ENVIRONMENT_ID {
            return Err(PersistenceError::InvalidConfig(
                "the fixed default environment cannot be deleted".to_string(),
            ));
        }
        match &self.backend {
            PersistenceBackend::File(store) => store.delete_environment(environment_id, expected_revision).await,
            PersistenceBackend::Sql(store) => store.delete_environment(environment_id, expected_revision).await,
        }
    }
}

/// Keeps aggregate endpoint ordering independent of the storage engine.
pub(crate) fn sort_environment_endpoints(environment: &mut DashboardEnvironment) {
    environment.endpoints.sort_by(|left, right| {
        endpoint_type_sort_key(left.endpoint_type)
            .cmp(&endpoint_type_sort_key(right.endpoint_type))
            .then_with(|| left.sort_order.cmp(&right.sort_order))
            .then_with(|| left.endpoint_id.0.cmp(&right.endpoint_id.0))
    });
}

const fn endpoint_type_sort_key(endpoint_type: EndpointType) -> u8 {
    match endpoint_type {
        EndpointType::Nameserver => 0,
        EndpointType::Proxy => 1,
    }
}

impl EnvironmentRepository for DashboardPersistence {
    async fn load_environment(&self, environment_id: &EnvironmentId) -> Result<DashboardEnvironment, PersistenceError> {
        Self::load_environment(self, environment_id).await
    }

    async fn load_default_environment(&self) -> Result<Option<DashboardEnvironment>, PersistenceError> {
        Self::load_default_environment(self).await
    }

    async fn list_environments(&self) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
        Self::list_environments(self).await
    }

    async fn create_environment(
        &self,
        environment: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        Self::create_environment(self, environment).await
    }

    async fn update_environment(
        &self,
        expected_revision: Revision,
        candidate: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        Self::update_environment(self, expected_revision, candidate).await
    }

    async fn delete_environment(
        &self,
        environment_id: &EnvironmentId,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        Self::delete_environment(self, environment_id, expected_revision).await
    }
}

impl FilePersistence {
    pub(crate) async fn load_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        let _read_guard = self.read_guard().await;
        self.load_environment_locked(environment_id).await
    }

    pub(crate) async fn load_environment_locked(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        if let Some(environment) = self.load_default_environment_locked().await?
            && environment.environment_id == *environment_id
        {
            return Ok(environment);
        }
        load_file_environment(self, &environment_collection(environment_id)).await
    }

    pub(crate) async fn load_default_environment(&self) -> Result<Option<DashboardEnvironment>, PersistenceError> {
        let _read_guard = self.read_guard().await;
        self.load_default_environment_locked().await
    }

    pub(crate) async fn load_default_environment_locked(
        &self,
    ) -> Result<Option<DashboardEnvironment>, PersistenceError> {
        self.load_environment_collection(DEFAULT_ENVIRONMENT_COLLECTION).await
    }

    pub(crate) async fn list_environments(&self) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
        let _read_guard = self.read_guard().await;
        self.list_environments_locked().await
    }

    async fn list_environments_locked(&self) -> Result<Vec<DashboardEnvironment>, PersistenceError> {
        let mut environments = Vec::new();
        for collection in self.list_snapshot_collections("environments").await? {
            if let Some(environment) = self.load_environment_collection(&collection).await? {
                environments.push(environment);
            }
        }
        environments.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.environment_id.0.cmp(&right.environment_id.0))
        });
        Ok(environments)
    }

    pub(crate) async fn create_environment(
        &self,
        environment: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        environment.validate().map_err(PersistenceError::InvalidConfig)?;
        if environment.revision != Revision(1) {
            return Err(PersistenceError::InvalidConfig(
                "new environments must start at revision 1".to_string(),
            ));
        }
        validate_environment_identity(&environment)?;
        let _write_guard = self.write_guard().await;
        if self
            .list_environments_locked()
            .await?
            .iter()
            .any(|existing| existing.name == environment.name)
        {
            return Err(PersistenceError::Conflict);
        }
        let collection = collection_for_environment(&environment);
        self.write_snapshot_locked(
            _write_guard,
            &collection,
            environment.revision.0,
            to_value(&environment).map_err(PersistenceError::Serialization)?,
        )
        .await?;
        Ok(environment)
    }

    pub(crate) async fn update_environment(
        &self,
        expected_revision: Revision,
        mut candidate: DashboardEnvironment,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        candidate.validate().map_err(PersistenceError::InvalidConfig)?;
        validate_environment_identity(&candidate)?;
        let _write_guard = self.write_guard().await;
        if self
            .list_environments_locked()
            .await?
            .iter()
            .any(|existing| existing.environment_id != candidate.environment_id && existing.name == candidate.name)
        {
            return Err(PersistenceError::Conflict);
        }
        let collection = collection_for_environment(&candidate);
        let current = self
            .load_latest_snapshot(&collection)
            .await?
            .ok_or(PersistenceError::NotFound)?;
        let current_environment =
            decode_file_environment_snapshot(current.payload)?.ok_or(PersistenceError::NotFound)?;
        if current_environment.environment_id != candidate.environment_id
            || current_environment.revision != expected_revision
        {
            return Err(PersistenceError::Conflict);
        }
        candidate.revision = Revision(expected_revision.0.checked_add(1).ok_or(PersistenceError::Conflict)?);
        self.compare_and_write_snapshot_locked(
            _write_guard,
            &collection,
            current.revision,
            to_value(&candidate).map_err(PersistenceError::Serialization)?,
        )
        .await?;
        Ok(candidate)
    }

    pub(crate) async fn update_environment_with_audit(
        &self,
        expected_revision: Revision,
        mut candidate: DashboardEnvironment,
        audit: AuditEvent,
    ) -> Result<DashboardEnvironment, PersistenceError> {
        candidate.validate().map_err(PersistenceError::InvalidConfig)?;
        validate_environment_identity(&candidate)?;
        let write_guard = self.write_guard().await;
        if self
            .list_environments_locked()
            .await?
            .iter()
            .any(|existing| existing.environment_id != candidate.environment_id && existing.name == candidate.name)
        {
            return Err(PersistenceError::Conflict);
        }
        let collection = collection_for_environment(&candidate);
        let current = self
            .load_latest_snapshot(&collection)
            .await?
            .ok_or(PersistenceError::NotFound)?;
        let current_environment =
            decode_file_environment_snapshot(current.payload)?.ok_or(PersistenceError::NotFound)?;
        if current_environment.environment_id != candidate.environment_id
            || current_environment.revision != expected_revision
        {
            return Err(PersistenceError::Conflict);
        }
        candidate.revision = Revision(expected_revision.0.checked_add(1).ok_or(PersistenceError::Conflict)?);
        self.publish_snapshot_transaction_with_audit_locked(
            write_guard,
            vec![FileSnapshotTransactionWrite {
                collection,
                expected_revision: current.revision,
                payload: to_value(&candidate).map_err(PersistenceError::Serialization)?,
            }],
            audit,
        )
        .await?;
        Ok(candidate)
    }

    pub(crate) async fn delete_environment(
        &self,
        environment_id: &EnvironmentId,
        expected_revision: Revision,
    ) -> Result<bool, PersistenceError> {
        let _write_guard = self.write_guard().await;
        let collection = environment_collection_for_id(environment_id);
        let Some(current) = self.load_latest_snapshot(&collection).await? else {
            return Ok(false);
        };
        let environment =
            decode_file_environment_snapshot(current.payload.clone())?.ok_or(PersistenceError::NotFound)?;
        if environment.environment_id != *environment_id || environment.revision != expected_revision {
            return Err(PersistenceError::Conflict);
        }
        let monitor_collection = crate::persistence::monitor_repository::monitor_collection(environment_id);
        let monitor_revision = self
            .load_latest_snapshot(&monitor_collection)
            .await?
            .map_or(0, |snapshot| snapshot.revision);
        self.publish_snapshot_transaction_locked(
            _write_guard,
            vec![
                FileSnapshotTransactionWrite {
                    collection,
                    expected_revision: current.revision,
                    payload: to_value(FileEnvironmentRecord::Deleted { deleted: true })
                        .map_err(PersistenceError::Serialization)?,
                },
                FileSnapshotTransactionWrite {
                    collection: monitor_collection,
                    expected_revision: monitor_revision,
                    payload: crate::persistence::monitor_repository::empty_monitor_snapshot_value()?,
                },
            ],
        )
        .await?;
        Ok(true)
    }

    async fn load_environment_collection(
        &self,
        collection: &str,
    ) -> Result<Option<DashboardEnvironment>, PersistenceError> {
        self.load_latest_snapshot(collection)
            .await?
            .map(|snapshot| decode_file_environment_snapshot(snapshot.payload))
            .transpose()
            .map(Option::flatten)
    }
}

const DEFAULT_ENVIRONMENT_COLLECTION: &str = "environments/default";

fn environment_collection(environment_id: &EnvironmentId) -> String {
    format!("environments/{}", environment_id.0)
}

fn collection_for_environment(environment: &DashboardEnvironment) -> String {
    if environment.name == "default" {
        DEFAULT_ENVIRONMENT_COLLECTION.to_string()
    } else {
        environment_collection(&environment.environment_id)
    }
}

fn environment_collection_for_id(environment_id: &EnvironmentId) -> String {
    if environment_id.0 == DEFAULT_ENVIRONMENT_ID {
        DEFAULT_ENVIRONMENT_COLLECTION.to_string()
    } else {
        environment_collection(environment_id)
    }
}

async fn load_file_environment(
    store: &FilePersistence,
    collection: &str,
) -> Result<DashboardEnvironment, PersistenceError> {
    store
        .load_environment_collection(collection)
        .await?
        .ok_or(PersistenceError::NotFound)
}

pub(crate) const DEFAULT_ENVIRONMENT_ID: &str = "00000000-0000-7000-8000-000000000001";

pub(crate) fn validate_environment_identity(environment: &DashboardEnvironment) -> Result<(), PersistenceError> {
    if environment.name == "default" && environment.environment_id.0 != DEFAULT_ENVIRONMENT_ID {
        return Err(PersistenceError::InvalidConfig(
            "the default environment must use the fixed dashboard environment identifier".to_string(),
        ));
    }
    if environment.environment_id.0 == DEFAULT_ENVIRONMENT_ID && environment.name != "default" {
        return Err(PersistenceError::InvalidConfig(
            "the fixed dashboard environment identifier must retain the default name".to_string(),
        ));
    }
    Ok(())
}

/// Validates a decoded aggregate before it becomes observable. Persisted
/// records predate the fixed default-environment identity invariant, so reads
/// must reject a malformed historical row just as writes do.
pub(crate) fn validate_loaded_environment(
    mut environment: DashboardEnvironment,
) -> Result<DashboardEnvironment, PersistenceError> {
    environment.validate().map_err(PersistenceError::InvalidConfig)?;
    validate_environment_identity(&environment)?;
    sort_environment_endpoints(&mut environment);
    Ok(environment)
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum FileEnvironmentRecord {
    Environment(DashboardEnvironment),
    Deleted { deleted: bool },
}

fn decode_file_environment_snapshot(
    payload: serde_json::Value,
) -> Result<Option<DashboardEnvironment>, PersistenceError> {
    match serde_json::from_value(payload).map_err(|_| PersistenceError::CorruptedData)? {
        // A direct aggregate payload is retained as the normal snapshot form
        // for compatibility with snapshots written before delete support.
        FileEnvironmentRecord::Environment(environment) => Ok(Some(validate_loaded_environment(environment)?)),
        FileEnvironmentRecord::Deleted { deleted: true } => Ok(None),
        FileEnvironmentRecord::Deleted { deleted: false } => Err(PersistenceError::CorruptedData),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlPoolConfig;
    use crate::config::StorageConfig;
    use crate::model::ConsumerMonitorRule;
    use crate::model::DashboardConfigView;
    use crate::model::StorageBackend;
    use rocketmq_runtime::RuntimeOwner;

    #[test]
    fn file_environment_delete_transaction_rolls_back_all_collections_on_write_failure() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &StorageConfig {
                    backend: StorageBackend::File,
                    data_path: directory.path().join("dashboard"),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                },
                owner.root_context().component("file-delete-transaction"),
            )
            .await
            .expect("initialize file persistence");
            let mut environment = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
            environment.environment_id = EnvironmentId::new();
            environment.name = format!("file-delete-{}", environment.environment_id.0);
            let environment = store.create_environment(environment).await.expect("create environment");
            let rule = store
                .upsert_monitor_rule(
                    ConsumerMonitorRule {
                        environment_id: environment.environment_id.clone(),
                        consumer_group: "file-delete-group".to_string(),
                        min_count: 1,
                        max_diff_total: 10,
                        revision: Revision(0),
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    },
                    Revision(0),
                )
                .await
                .expect("create monitor rule");
            let environment_collection = environment_collection_for_id(&environment.environment_id);
            let monitor_collection =
                crate::persistence::monitor_repository::monitor_collection(&environment.environment_id);
            let _write_guard = store.write_guard().await;
            let result = store
                .publish_snapshot_transaction_with_failure_after_locked(
                    _write_guard,
                    vec![
                        FileSnapshotTransactionWrite {
                            collection: environment_collection,
                            expected_revision: environment.revision.0,
                            payload: to_value(FileEnvironmentRecord::Deleted { deleted: true })
                                .expect("serialize environment tombstone"),
                        },
                        FileSnapshotTransactionWrite {
                            collection: monitor_collection,
                            expected_revision: 1,
                            payload: crate::persistence::monitor_repository::empty_monitor_snapshot_value()
                                .expect("serialize empty monitor snapshot"),
                        },
                    ],
                    1,
                )
                .await;
            assert!(matches!(result, Err(PersistenceError::Io(_))));
            assert_eq!(
                store
                    .load_environment(&environment.environment_id)
                    .await
                    .expect("environment must survive failed delete"),
                environment
            );
            assert_eq!(
                store
                    .list_monitor_rules(&environment.environment_id)
                    .await
                    .expect("monitor rules must survive failed delete"),
                vec![rule]
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn file_environment_delete_reopens_and_recovers_after_rollback_cleanup_fails() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("dashboard");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let store = FilePersistence::initialize(
                &StorageConfig {
                    backend: StorageBackend::File,
                    data_path: root.clone(),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                },
                owner.root_context().component("file-delete-rollback-failure"),
            )
            .await
            .expect("initialize file persistence");
            let mut environment = DashboardEnvironment::bootstrap(&DashboardConfigView::default(), 1);
            environment.environment_id = EnvironmentId::new();
            environment.name = format!("file-delete-reopen-{}", environment.environment_id.0);
            let environment = store.create_environment(environment).await.expect("create environment");
            let rule = store
                .upsert_monitor_rule(
                    ConsumerMonitorRule {
                        environment_id: environment.environment_id.clone(),
                        consumer_group: "file-delete-reopen-group".to_string(),
                        min_count: 1,
                        max_diff_total: 10,
                        revision: Revision(0),
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    },
                    Revision(0),
                )
                .await
                .expect("create monitor rule");
            let environment_collection = environment_collection_for_id(&environment.environment_id);
            let monitor_collection =
                crate::persistence::monitor_repository::monitor_collection(&environment.environment_id);
            let write_guard = store.write_guard().await;
            let result = store
                .publish_snapshot_transaction_with_rollback_failure_locked(
                    write_guard,
                    vec![
                        FileSnapshotTransactionWrite {
                            collection: environment_collection,
                            expected_revision: environment.revision.0,
                            payload: to_value(FileEnvironmentRecord::Deleted { deleted: true })
                                .expect("serialize environment tombstone"),
                        },
                        FileSnapshotTransactionWrite {
                            collection: monitor_collection,
                            expected_revision: 1,
                            payload: crate::persistence::monitor_repository::empty_monitor_snapshot_value()
                                .expect("serialize empty monitor snapshot"),
                        },
                    ],
                    1,
                )
                .await;
            assert!(matches!(result, Err(PersistenceError::Io(_))));
            assert_eq!(
                store
                    .load_environment(&environment.environment_id)
                    .await
                    .expect("active instance must serve the restored environment"),
                environment
            );
            assert_eq!(
                store
                    .list_monitor_rules(&environment.environment_id)
                    .await
                    .expect("active instance must serve the restored monitor rules"),
                vec![rule.clone()]
            );
            assert!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory")
                    .next()
                    .is_none(),
                "the active instance must complete marker recovery before returning the failure"
            );
            drop(store);

            let recovered = FilePersistence::initialize(
                &StorageConfig {
                    backend: StorageBackend::File,
                    data_path: root.clone(),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                },
                owner.root_context().component("file-delete-rollback-recovery"),
            )
            .await
            .expect("reopen file persistence");
            assert_eq!(
                recovered
                    .load_environment(&environment.environment_id)
                    .await
                    .expect("environment after rollback recovery"),
                environment
            );
            assert_eq!(
                recovered
                    .list_monitor_rules(&environment.environment_id)
                    .await
                    .expect("monitor rules after rollback recovery"),
                vec![rule]
            );
            assert!(
                std::fs::read_dir(root.join("transactions"))
                    .expect("transaction directory after recovery")
                    .next()
                    .is_none(),
                "startup recovery must remove the completed marker"
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
