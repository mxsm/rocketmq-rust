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

use super::validation::verify_data;
use super::*;
use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::ConsumerMonitorRule;
use crate::model::DashboardEnvironment;
use crate::model::EnvironmentId;
use crate::model::StorageBackend;
use crate::persistence::Revision;
use crate::persistence::error::PersistenceError;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use serde_json::Value;

fn test_data() -> BackupData {
    let environment = DashboardEnvironment {
        environment_id: EnvironmentId("00000000-0000-7000-8000-000000000001".to_string()),
        name: "default".to_string(),
        use_vip_channel: true,
        use_tls: false,
        revision: Revision(1),
        created_at_ms: 1,
        updated_at_ms: 1,
        endpoints: Vec::new(),
    };
    let mut data = BackupData::with_backend(StorageBackend::File);
    data.environments.push(environment);
    data.refresh_counts().expect("counts");
    data
}

#[test]
fn backup_round_trip_and_unknown_or_extra_files_are_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let output = directory.path().join("backup");
    let data = test_data();
    write_backup(&output, &data).expect("write backup");
    assert_eq!(
        read_verified_backup(&output, Some(StorageBackend::File))
            .expect("verify")
            .manifest
            .counts,
        data.manifest.counts
    );
    std::fs::write(output.join("unexpected.txt"), b"x").expect("unexpected file");
    assert!(read_verified_backup(&output, Some(StorageBackend::File)).is_err());
}

#[test]
fn invalid_token_digest_and_missing_collection_are_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let output = directory.path().join("backup");
    let mut data = test_data();
    data.sessions.push(BackupSession {
        session_id: "00000000-0000-7000-8000-000000000002".to_string(),
        token_hash: "not-a-digest".to_string(),
        username: "admin".to_string(),
        created_at_ms: 1,
        expires_at_ms: 2,
        last_seen_at_ms: 1,
        revoked_at_ms: None,
    });
    data.refresh_counts().expect("counts");
    assert!(write_backup(&output, &data).is_err());

    let valid_output = directory.path().join("valid-backup");
    write_backup(&valid_output, &test_data()).expect("write valid backup");
    std::fs::remove_file(valid_output.join("history.ndjson")).expect("remove collection");
    assert!(read_verified_backup(&valid_output, Some(StorageBackend::File)).is_err());
}

#[test]
fn unknown_record_fields_truncated_lines_duplicate_identity_and_bad_relations_are_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let output = directory.path().join("backup");
    let data = test_data();
    write_backup(&output, &data).expect("write backup");

    let mut environment = serde_json::to_value(&data.environments[0]).expect("environment serializes");
    environment
        .as_object_mut()
        .expect("environment object")
        .insert("unexpected".to_string(), Value::Null);
    std::fs::write(
        output.join("environments.ndjson"),
        format!(
            "{}\n",
            serde_json::to_string(&environment).expect("environment encodes")
        ),
    )
    .expect("write unknown record");
    assert!(read_verified_backup(&output, Some(StorageBackend::File)).is_err());

    let truncated = directory.path().join("truncated");
    write_backup(&truncated, &data).expect("write truncated backup");
    std::fs::write(truncated.join("sessions.ndjson"), b"{").expect("write truncated line");
    assert!(read_verified_backup(&truncated, Some(StorageBackend::File)).is_err());

    let mut duplicate = test_data();
    duplicate.environments.push(duplicate.environments[0].clone());
    duplicate.refresh_counts().expect("counts");
    assert!(verify_data(&duplicate, Some(StorageBackend::File)).is_err());

    let mut bad_relation = test_data();
    bad_relation.monitors.push(ConsumerMonitorRule {
        environment_id: EnvironmentId("00000000-0000-7000-8000-000000000099".to_string()),
        consumer_group: "group".to_string(),
        min_count: 0,
        max_diff_total: 1,
        revision: Revision(1),
        created_at_ms: 1,
        updated_at_ms: 1,
    });
    bad_relation.refresh_counts().expect("counts");
    assert!(verify_data(&bad_relation, Some(StorageBackend::File)).is_err());
}

#[test]
fn sqlite_backup_verify_restore_and_nonempty_target_are_atomic() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    owner.block_on(async {
        let source_config = sqlite_config(directory.path().join("source.db"));
        let source = DashboardPersistence::initialize(
            &source_config,
            owner.root_context().component("storage-operations-source"),
        )
        .await
        .expect("initialize source");
        let mut source_data = test_data();
        let environment = source
            .create_environment(source_data.environments.remove(0))
            .await
            .expect("create source environment");
        let backup = directory.path().join("backup");
        write_backup(&backup, &snapshot(&source).await.expect("snapshot source")).expect("write backup");
        let verified = read_verified_backup(&backup, Some(StorageBackend::Sqlite)).expect("verify backup");
        assert!(read_verified_backup(&backup, Some(StorageBackend::File)).is_err());

        let target = DashboardPersistence::initialize(
            &sqlite_config(directory.path().join("target.db")),
            owner.root_context().component("storage-operations-target"),
        )
        .await
        .expect("initialize empty target");
        restore(&target, &verified).await.expect("restore empty target");
        assert_eq!(
            target
                .load_environment(&environment.environment_id)
                .await
                .expect("restored environment"),
            environment
        );

        let occupied = DashboardPersistence::initialize(
            &sqlite_config(directory.path().join("occupied.db")),
            owner.root_context().component("storage-operations-occupied"),
        )
        .await
        .expect("initialize occupied target");
        let existing = occupied
            .create_environment(DashboardEnvironment {
                environment_id: EnvironmentId("00000000-0000-7000-8000-000000000010".to_string()),
                name: "occupied".to_string(),
                use_vip_channel: false,
                use_tls: false,
                revision: Revision(1),
                created_at_ms: 1,
                updated_at_ms: 1,
                endpoints: Vec::new(),
            })
            .await
            .expect("create occupied environment");
        assert!(matches!(
            restore(&occupied, &verified).await,
            Err(PersistenceError::Conflict)
        ));
        assert_eq!(
            occupied
                .load_environment(&existing.environment_id)
                .await
                .expect("occupied environment remains"),
            existing
        );
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

#[test]
fn file_restore_publishes_only_a_complete_target() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    owner.block_on(async {
        let config = StorageConfig {
            backend: StorageBackend::File,
            data_path: directory.path().join("file-target"),
            database_url: None,
            pool: SqlPoolConfig::default(),
        };
        let data = test_data();
        restore_file_target(
            &data,
            &config,
            owner.root_context().component("storage-operations-file-restore"),
        )
        .await
        .expect("restore into absent file target");
        let restored = DashboardPersistence::initialize(
            &config,
            owner.root_context().component("storage-operations-file-verify"),
        )
        .await
        .expect("open restored file target");
        assert_eq!(
            restored.list_environments().await.expect("list restored environments"),
            data.environments
        );
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}

fn sqlite_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        backend: StorageBackend::Sqlite,
        data_path: path,
        database_url: None,
        pool: SqlPoolConfig::default(),
    }
}
