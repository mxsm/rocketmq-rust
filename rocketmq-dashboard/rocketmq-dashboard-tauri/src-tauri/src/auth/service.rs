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

use crate::auth::db::AuthDb;
use crate::auth::types::AuthError;
use crate::auth::types::AuthResult;
use crate::auth::types::BootstrapStatus;
use crate::auth::types::UserProfile;
use crate::auth::types::UserRecord;
use argon2::Argon2;
use chrono::Utc;
use password_hash::PasswordHash;
use password_hash::PasswordHasher;
use password_hash::PasswordVerifier;
use password_hash::SaltString;
use password_hash::rand_core::OsRng;
use rusqlite::OptionalExtension;
use rusqlite::params;

const DEFAULT_ADMIN_USERNAME: &str = "admin";
const DEFAULT_ADMIN_PASSWORD: &str = "admin123";
const DEFAULT_INIT_PASSWORD_ENV: &str = "ROCKETMQ_DASHBOARD_INIT_PASSWORD";
const MIN_PASSWORD_LENGTH: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct AuthService {
    db: AuthDb,
    initial_password_override: Option<String>,
}

impl AuthService {
    pub(crate) fn new(db: AuthDb) -> Self {
        Self {
            db,
            initial_password_override: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_initial_password(db: AuthDb, initial_password: impl Into<String>) -> Self {
        Self {
            db,
            initial_password_override: Some(initial_password.into()),
        }
    }

    pub(crate) fn bootstrap_default_admin(&self) -> AuthResult<BootstrapStatus> {
        if let Some(user) = self.find_user_by_username(DEFAULT_ADMIN_USERNAME)? {
            return Ok(BootstrapStatus {
                username: user.username,
                created: false,
                has_default_admin: true,
                must_change_password: user.must_change_password,
            });
        }

        let now = Utc::now().to_rfc3339();
        let password_hash = hash_password(&self.initial_password())?;
        let connection = self.db.connection()?;

        connection.execute(
            "
            INSERT INTO users (
                username,
                password_hash,
                is_active,
                must_change_password,
                created_at,
                updated_at,
                last_login_at
            ) VALUES (?1, ?2, 1, 1, ?3, ?3, NULL)
            ",
            params![DEFAULT_ADMIN_USERNAME, password_hash, now],
        )?;

        Ok(BootstrapStatus {
            username: DEFAULT_ADMIN_USERNAME.to_string(),
            created: true,
            has_default_admin: true,
            must_change_password: true,
        })
    }

    pub(crate) fn get_bootstrap_status(&self) -> AuthResult<BootstrapStatus> {
        if let Some(user) = self.find_user_by_username(DEFAULT_ADMIN_USERNAME)? {
            return Ok(BootstrapStatus {
                username: user.username,
                created: false,
                has_default_admin: true,
                must_change_password: user.must_change_password,
            });
        }

        Ok(BootstrapStatus {
            username: DEFAULT_ADMIN_USERNAME.to_string(),
            created: false,
            has_default_admin: false,
            must_change_password: false,
        })
    }

    pub(crate) fn authenticate(&self, username: &str, password: &str) -> AuthResult<UserRecord> {
        let user = self
            .find_user_by_username(username)?
            .ok_or_else(|| AuthError::Authentication("Invalid username or password".to_string()))?;

        if !user.is_active {
            return Err(AuthError::Authentication("Account is disabled".to_string()));
        }

        if !verify_password(password, &user.password_hash)? {
            return Err(AuthError::Authentication("Invalid username or password".to_string()));
        }

        Ok(user)
    }

    pub(crate) fn change_password(&self, user_id: i64, old_password: &str, new_password: &str) -> AuthResult<()> {
        let user = self
            .find_user_by_id(user_id)?
            .ok_or_else(|| AuthError::Authentication("User not found".to_string()))?;

        if !verify_password(old_password, &user.password_hash)? {
            return Err(AuthError::Authentication("Current password is incorrect".to_string()));
        }

        validate_new_password(old_password, new_password)?;

        let new_password_hash = hash_password(new_password)?;
        let now = Utc::now().to_rfc3339();
        let connection = self.db.connection()?;

        connection.execute(
            "
            UPDATE users
            SET password_hash = ?1,
                must_change_password = 0,
                updated_at = ?2
            WHERE id = ?3
            ",
            params![new_password_hash, now, user_id],
        )?;

        Ok(())
    }

    pub(crate) fn find_user_by_id(&self, user_id: i64) -> AuthResult<Option<UserRecord>> {
        let connection = self.db.connection()?;
        connection
            .query_row(
                "
                SELECT id, username, password_hash, is_active, must_change_password, created_at, updated_at, \
                 last_login_at
                FROM users
                WHERE id = ?1
                ",
                params![user_id],
                map_user_record,
            )
            .optional()
            .map_err(Into::into)
    }

    pub(crate) fn update_last_login(&self, user_id: i64) -> AuthResult<()> {
        let connection = self.db.connection()?;
        let now = Utc::now().to_rfc3339();
        connection.execute(
            "
            UPDATE users
            SET last_login_at = ?1,
                updated_at = ?1
            WHERE id = ?2
            ",
            params![now, user_id],
        )?;
        Ok(())
    }

    pub(crate) fn get_user_profile(&self, session_id: &str, user_id: i64) -> AuthResult<Option<UserProfile>> {
        let user = self.find_user_by_id(user_id)?;

        Ok(user.map(|user| UserProfile {
            session_id: session_id.to_string(),
            user_id: user.id,
            username: user.username,
            is_active: user.is_active,
            must_change_password: user.must_change_password,
            created_at: user.created_at,
            updated_at: user.updated_at,
            last_login_at: user.last_login_at,
        }))
    }

    fn find_user_by_username(&self, username: &str) -> AuthResult<Option<UserRecord>> {
        let connection = self.db.connection()?;
        connection
            .query_row(
                "
                SELECT id, username, password_hash, is_active, must_change_password, created_at, updated_at, \
                 last_login_at
                FROM users
                WHERE username = ?1
                ",
                params![username],
                map_user_record,
            )
            .optional()
            .map_err(Into::into)
    }

    fn initial_password(&self) -> String {
        self.initial_password_override
            .clone()
            .or_else(|| std::env::var(DEFAULT_INIT_PASSWORD_ENV).ok())
            .filter(|password| !password.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_ADMIN_PASSWORD.to_string())
    }
}

pub(crate) fn hash_password(password: &str) -> AuthResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    let password_hash = Argon2::default().hash_password(password.as_bytes(), &salt)?;
    Ok(password_hash.to_string())
}

pub(crate) fn verify_password(password: &str, hash: &str) -> AuthResult<bool> {
    let parsed_hash = PasswordHash::new(hash)?;

    match Argon2::default().verify_password(password.as_bytes(), &parsed_hash) {
        Ok(()) => Ok(true),
        Err(password_hash::Error::Password) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_new_password(old_password: &str, new_password: &str) -> AuthResult<()> {
    if new_password.len() < MIN_PASSWORD_LENGTH {
        return Err(AuthError::Validation(format!(
            "New password must be at least {MIN_PASSWORD_LENGTH} characters long"
        )));
    }

    if old_password == new_password {
        return Err(AuthError::Validation(
            "New password must be different from the current password".to_string(),
        ));
    }

    Ok(())
}

fn map_user_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<UserRecord> {
    Ok(UserRecord {
        id: row.get(0)?,
        username: row.get(1)?,
        password_hash: row.get(2)?,
        is_active: row.get::<_, i64>(3)? != 0,
        must_change_password: row.get::<_, i64>(4)? != 0,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
        last_login_at: row.get(7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::AuthService;
    use super::hash_password;
    use super::verify_password;
    use crate::auth::db::AuthDb;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "rocketmq-dashboard-tauri-auth-service-tests-{}",
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn db_path(&self) -> PathBuf {
            self.path.join("dashboard.db")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct TestContext {
        _test_dir: TestDir,
        service: AuthService,
    }

    fn setup_service(initial_password: &str) -> TestContext {
        let test_dir = TestDir::new();
        let db = AuthDb::from_path(test_dir.db_path());
        db.init().expect("database initialization should succeed");

        TestContext {
            _test_dir: test_dir,
            service: AuthService::with_initial_password(db, initial_password),
        }
    }

    #[test]
    fn bootstrap_creates_default_admin_with_argon2_hash() {
        let context = setup_service("change-me-now");
        let service = &context.service;

        let status = service.bootstrap_default_admin().expect("bootstrap should succeed");
        let user = service
            .authenticate("admin", "change-me-now")
            .expect("default admin should authenticate");

        assert!(status.created);
        assert!(status.has_default_admin);
        assert!(status.must_change_password);
        assert_ne!(user.password_hash, "change-me-now");
        assert!(verify_password("change-me-now", &user.password_hash).expect("password verification should succeed"));
    }

    #[test]
    fn bootstrap_is_idempotent() {
        let context = setup_service("change-me-now");
        let service = &context.service;

        let first = service
            .bootstrap_default_admin()
            .expect("first bootstrap should succeed");
        let second = service
            .bootstrap_default_admin()
            .expect("second bootstrap should succeed");

        assert!(first.created);
        assert!(!second.created);
        assert!(second.has_default_admin);
    }

    #[test]
    fn login_and_change_password_flow_updates_flags() {
        let context = setup_service("change-me-now");
        let service = &context.service;
        service.bootstrap_default_admin().expect("bootstrap should succeed");

        let user = service
            .authenticate("admin", "change-me-now")
            .expect("login should succeed");
        assert!(user.must_change_password);

        service
            .change_password(user.id, "change-me-now", "better-secret")
            .expect("password change should succeed");

        assert!(service.authenticate("admin", "better-secret").is_ok());
        assert!(service.authenticate("admin", "change-me-now").is_err());

        let updated_user = service
            .find_user_by_id(user.id)
            .expect("lookup should succeed")
            .expect("user should exist");
        assert!(!updated_user.must_change_password);
    }

    #[test]
    fn password_hashing_uses_randomized_salts() {
        let hash_a = hash_password("same-password").expect("hashing should succeed");
        let hash_b = hash_password("same-password").expect("hashing should succeed");

        assert_ne!(hash_a, hash_b);
        assert!(verify_password("same-password", &hash_a).expect("verification should succeed"));
        assert!(verify_password("same-password", &hash_b).expect("verification should succeed"));
    }

    #[test]
    fn get_user_profile_returns_persisted_account_fields() {
        let context = setup_service("change-me-now");
        let service = &context.service;
        service.bootstrap_default_admin().expect("bootstrap should succeed");

        let user = service
            .authenticate("admin", "change-me-now")
            .expect("login should succeed");
        service.update_last_login(user.id).expect("last login should update");

        let profile = service
            .get_user_profile("session-123", user.id)
            .expect("profile lookup should succeed")
            .expect("profile should exist");

        assert_eq!(profile.session_id, "session-123");
        assert_eq!(profile.user_id, user.id);
        assert_eq!(profile.username, "admin");
        assert!(profile.is_active);
        assert!(profile.must_change_password);
        assert!(!profile.created_at.is_empty());
        assert!(!profile.updated_at.is_empty());
        assert!(profile.last_login_at.is_some());
    }

    #[test]
    fn get_user_profile_keeps_null_last_login_when_never_logged_in() {
        let context = setup_service("change-me-now");
        let service = &context.service;
        service.bootstrap_default_admin().expect("bootstrap should succeed");

        let user = service
            .authenticate("admin", "change-me-now")
            .expect("authentication should succeed");

        let profile = service
            .get_user_profile("session-123", user.id)
            .expect("profile lookup should succeed")
            .expect("profile should exist");

        assert_eq!(profile.last_login_at, None);
    }
}
