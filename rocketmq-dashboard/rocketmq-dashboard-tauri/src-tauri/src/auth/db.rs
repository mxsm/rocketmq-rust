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

//! Embedded auth database layer.
//!
//! SQLite bootstrap and schema initialization for local dashboard auth.

use crate::auth::types::AuthResult;
use rusqlite::Connection;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(crate) struct AuthDb {
    db_path: PathBuf,
}

impl AuthDb {
    pub(crate) fn from_path(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub(crate) fn init(&self) -> AuthResult<()> {
        let mut connection = self.connection()?;
        crate::persistence::schema::initialize(&mut connection)
    }

    pub(crate) fn connection(&self) -> AuthResult<Connection> {
        crate::persistence::open_connection(&self.db_path)
    }
}

#[cfg(test)]
mod tests {
    use super::AuthDb;
    use rusqlite::params;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!("rocketmq-dashboard-tauri-auth-tests-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn db_path(&self) -> PathBuf {
            self.path.join("nested").join("dashboard.db")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn init_creates_database_file_and_users_table() {
        let test_dir = TestDir::new();
        let db_path = test_dir.db_path();
        let db = AuthDb::from_path(&db_path);

        assert!(!db_path.exists());

        db.init().expect("database initialization should succeed");

        assert!(db.db_path().exists());

        let connection = db.connection().expect("connection should open");
        let table_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params!["users"],
                |row| row.get(0),
            )
            .expect("users table query should succeed");

        assert_eq!(table_count, 1);
    }

    #[test]
    fn init_is_idempotent() {
        let test_dir = TestDir::new();
        let db = AuthDb::from_path(test_dir.db_path());

        db.init().expect("first initialization should succeed");
        db.init().expect("second initialization should also succeed");

        let connection = db.connection().expect("connection should open");
        let table_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params!["users"],
                |row| row.get(0),
            )
            .expect("users table query should succeed");

        assert_eq!(table_count, 1);
    }
}
