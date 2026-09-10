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

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("storage-operations-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn source(path: &Path) -> Connection {
    let mut db = Connection::open(path).unwrap();
    crate::persistence::schema::initialize(&mut db).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    db.execute_batch("INSERT INTO users(id,username,password_hash,must_change_password,created_at,updated_at) VALUES(1,'saved-user','saved-hash',0,'created','updated');
        INSERT INTO sessions(id,token_digest,user_id,created_at_ms,expires_at_ms,last_seen_at_ms) VALUES('session',X'1234',1,1,9999999999999,1);
        INSERT INTO nameserver_addresses(address,created_at,updated_at) VALUES('127.0.0.1:9876','created','updated');
        INSERT INTO consumer_monitor_rules VALUES('env','group',1,100,1,1,1);
        INSERT INTO history_samples VALUES('env','broker-count','',1,2);
        UPDATE storage_activity SET last_write_ms=123;").unwrap();
    db
}

#[test]
fn storage_backup_captures_wal_and_restore_revokes_sessions_without_changing_source() {
    let fixture = Fixture::new();
    let source_path = fixture.0.join(DATABASE);
    let source = source(&source_path);
    assert!(fixture.0.join("dashboard.db-wal").exists());
    let backup_path = fixture.0.join("snapshot");
    backup(&source_path, &backup_path).unwrap();
    let verified = verify(&backup_path).unwrap();
    assert_eq!(
        verified
            .query_row("SELECT username FROM users", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "saved-user"
    );
    drop(verified);
    let restored_path = fixture.0.join("restored");
    restore(&backup_path, &restored_path).unwrap();
    let restored = open_read_only(&restored_path.join(DATABASE)).unwrap();
    assert_eq!(
        restored
            .query_row("SELECT count(*) FROM sessions WHERE revoked_at_ms IS NULL", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    for table in [
        "users",
        "nameserver_addresses",
        "consumer_monitor_rules",
        "history_samples",
    ] {
        assert_eq!(
            restored
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    assert_eq!(
        restored
            .query_row(
                "SELECT count(*) FROM audit_events WHERE action='storage.restore' AND outcome='success'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        source
            .query_row("SELECT count(*) FROM sessions WHERE revoked_at_ms IS NULL", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    assert_eq!(
        source
            .query_row("SELECT last_write_ms FROM storage_activity", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        123
    );
    assert!(backup(&source_path, &backup_path).is_err());
    assert!(restore(&backup_path, &restored_path).is_err());
}

#[test]
fn storage_verify_rejects_corruption_versions_and_restore_requires_confirmation() {
    let fixture = Fixture::new();
    let source_path = fixture.0.join(DATABASE);
    let _source = source(&source_path);
    let backup_path = fixture.0.join("snapshot");
    backup(&source_path, &backup_path).unwrap();
    let metadata_path = backup_path.join(METADATA);
    let metadata = fs::read(&metadata_path).unwrap();
    let mut unsupported: Metadata = serde_json::from_slice(&metadata).unwrap();
    unsupported.format_version = 99;
    fs::write(&metadata_path, serde_json::to_vec(&unsupported).unwrap()).unwrap();
    assert!(verify(&backup_path).is_err());
    fs::write(metadata_path, metadata).unwrap();
    fs::write(backup_path.join(DATABASE), b"not a SQLite database").unwrap();
    assert!(verify(&backup_path).is_err());
    let target = fixture.0.join("not-created");
    assert!(restore(&backup_path, &target).is_err());
    assert!(!target.exists());
    let args = ["restore", "--input", "input", "--target", "target"].map(OsString::from);
    assert!(parse(&args).is_err());
}

#[test]
fn failed_restore_never_publishes_a_database_with_active_snapshot_sessions() {
    let fixture = Fixture::new();
    let source_path = fixture.0.join(DATABASE);
    let source = source(&source_path);
    // Force the audit transaction to fail after session revocation.
    source.execute_batch("CREATE TRIGGER reject_restore BEFORE INSERT ON audit_events BEGIN SELECT RAISE(ABORT,'test audit failure'); END;").unwrap();
    let backup_path = fixture.0.join("snapshot");
    backup(&source_path, &backup_path).unwrap();
    let target = fixture.0.join("restored");
    assert!(restore(&backup_path, &target).is_err());
    assert!(!target.join(DATABASE).exists());
    assert!(
        source
            .query_row("SELECT revoked_at_ms FROM sessions", [], |row| row
                .get::<_, Option<i64>>(0))
            .unwrap()
            .is_none()
    );
}
