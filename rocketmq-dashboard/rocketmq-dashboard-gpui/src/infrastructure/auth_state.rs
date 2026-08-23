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

//! Environment-backed credentials with an in-memory-only dashboard session.

#[cfg(test)]
use std::collections::BTreeMap;
use std::{fmt, sync::Arc};

use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_dashboard_common::CredentialSourceKind;

use super::config_store::AuthConfig;

/// Environment variable containing the local dashboard username.
pub const LOGIN_USERNAME_ENV: &str = "ROCKETMQ_DASHBOARD_USERNAME";
/// Environment variable containing the local dashboard password.
pub const LOGIN_PASSWORD_ENV: &str = "ROCKETMQ_DASHBOARD_PASSWORD";
/// Environment variable containing the RocketMQ Admin access key.
pub const ADMIN_ACCESS_KEY_ENV: &str = "ROCKETMQ_ADMIN_ACCESS_KEY";
/// Environment variable containing the RocketMQ Admin secret key.
pub const ADMIN_SECRET_KEY_ENV: &str = "ROCKETMQ_ADMIN_SECRET_KEY";
/// Optional environment variable containing the RocketMQ security token.
pub const ADMIN_SECURITY_TOKEN_ENV: &str = "ROCKETMQ_ADMIN_SECURITY_TOKEN";

/// Narrow environment seam for deterministic tests and process integration.
pub trait EnvironmentReader: Send + Sync {
    /// Returns one value without making it printable through this trait.
    fn read(&self, name: &'static str) -> Option<String>;
}

/// Process environment implementation.
pub struct ProcessEnvironment;

impl EnvironmentReader for ProcessEnvironment {
    fn read(&self, name: &'static str) -> Option<String> {
        std::env::var(name).ok().filter(|value| !value.is_empty())
    }
}

/// A non-sensitive local session marker.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct LocalSession {
    username: Option<String>,
}

impl fmt::Debug for LocalSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSession")
            .field("authenticated", &self.is_authenticated())
            .field("username_available", &self.username.is_some())
            .finish()
    }
}

impl LocalSession {
    /// Returns whether local sign-in has succeeded.
    pub fn is_authenticated(&self) -> bool {
        self.username.is_some()
    }

    /// Returns the display-safe signed-in username.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
}

/// Credential/source failure that never carries a supplied secret value.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthStateError {
    /// Required environment-backed configuration is absent.
    #[error("required environment credential {name} is not configured")]
    MissingEnvironment {
        /// Variable name only; its value is never retained.
        name: &'static str,
    },
    /// The submitted local login did not match.
    #[error("the username or password was not accepted")]
    Rejected,
    /// Admin credential construction rejected an incomplete value.
    #[error("the environment-backed Admin credential is incomplete")]
    InvalidAdminCredential,
}

/// Auth owner. Passwords and Admin secrets are resolved only for a call and never stored here.
pub struct DesktopAuthState {
    environment: Arc<dyn EnvironmentReader>,
    session: parking_lot::Mutex<LocalSession>,
}

impl DesktopAuthState {
    /// Uses the real process environment.
    pub fn from_process_environment() -> Arc<Self> {
        Self::new(Arc::new(ProcessEnvironment))
    }

    /// Uses an injected source for deterministic tests.
    pub fn new(environment: Arc<dyn EnvironmentReader>) -> Arc<Self> {
        Arc::new(Self {
            environment,
            session: parking_lot::Mutex::new(LocalSession::default()),
        })
    }

    /// Checks required environment entries during startup without retaining their values.
    pub fn validate_startup(&self, config: &AuthConfig) -> Result<(), AuthStateError> {
        if config.enabled {
            self.required(LOGIN_USERNAME_ENV)?;
            self.required(LOGIN_PASSWORD_ENV)?;
        }
        if config.credential_source == CredentialSourceKind::Environment {
            self.required(ADMIN_ACCESS_KEY_ENV)?;
            self.required(ADMIN_SECRET_KEY_ENV)?;
        }
        Ok(())
    }

    /// Authenticates a local operator and stores only the username in memory.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<LocalSession, AuthStateError> {
        let configured_username = self.required(LOGIN_USERNAME_ENV)?;
        let configured_password = self.required(LOGIN_PASSWORD_ENV)?;
        let accepted = constant_time_eq(username.as_bytes(), configured_username.as_bytes())
            & constant_time_eq(password.as_bytes(), configured_password.as_bytes());
        drop(configured_username);
        drop(configured_password);
        if !accepted {
            return Err(AuthStateError::Rejected);
        }
        let session = LocalSession {
            username: Some(username.to_owned()),
        };
        *self.session.lock() = session.clone();
        Ok(session)
    }

    /// Clears the complete in-memory local session.
    pub fn sign_out(&self) {
        *self.session.lock() = LocalSession::default();
    }

    /// Returns a non-sensitive session snapshot.
    pub fn session(&self) -> LocalSession {
        self.session.lock().clone()
    }

    /// Resolves Admin credentials for immediate session construction.
    pub fn resolve_admin_credentials(
        &self,
        source: CredentialSourceKind,
    ) -> Result<Option<AdminCredentials>, AuthStateError> {
        match source {
            CredentialSourceKind::None => Ok(None),
            CredentialSourceKind::Environment => {
                let access_key = self.required(ADMIN_ACCESS_KEY_ENV)?;
                let secret_key = self.required(ADMIN_SECRET_KEY_ENV)?;
                let security_token = self.environment.read(ADMIN_SECURITY_TOKEN_ENV);
                AdminCredentials::try_new(access_key, secret_key, security_token)
                    .map(Some)
                    .map_err(|_| AuthStateError::InvalidAdminCredential)
            }
        }
    }

    fn required(&self, name: &'static str) -> Result<String, AuthStateError> {
        self.environment
            .read(name)
            .filter(|value| !value.trim().is_empty())
            .ok_or(AuthStateError::MissingEnvironment { name })
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let maximum = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..maximum {
        difference |= usize::from(left.get(index).copied().unwrap_or_default())
            ^ usize::from(right.get(index).copied().unwrap_or_default());
    }
    difference == 0
}

#[cfg(test)]
pub struct MapEnvironment {
    values: BTreeMap<&'static str, String>,
}

#[cfg(test)]
impl MapEnvironment {
    pub fn new(values: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name, value.to_owned()))
                .collect(),
        }
    }
}

#[cfg(test)]
impl EnvironmentReader for MapEnvironment {
    fn read(&self, name: &'static str) -> Option<String> {
        self.values.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_reports_missing_auth_environment_as_recoverable_source_error() {
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([])));
        let error = auth
            .validate_startup(&AuthConfig {
                enabled: true,
                credential_source: CredentialSourceKind::None,
            })
            .expect_err("missing username");

        assert_eq!(
            error,
            AuthStateError::MissingEnvironment {
                name: LOGIN_USERNAME_ENV
            }
        );
    }

    #[test]
    fn login_keeps_only_username_and_sign_out_clears_the_session() {
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "sensitive-password"),
        ])));

        let session = auth.authenticate("operator", "sensitive-password").expect("login");
        assert_eq!(session.username(), Some("operator"));
        let debug = format!("{session:?}");
        assert!(!debug.contains("sensitive-password"));
        auth.sign_out();
        assert!(!auth.session().is_authenticated());
    }

    #[test]
    fn failed_login_does_not_create_or_preserve_a_session() {
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "right-password"),
        ])));

        assert_eq!(auth.authenticate("operator", "wrong"), Err(AuthStateError::Rejected));
        assert!(!auth.session().is_authenticated());
    }

    #[test]
    fn admin_credentials_debug_is_redacted_by_admin_core() {
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (ADMIN_ACCESS_KEY_ENV, "access-value"),
            (ADMIN_SECRET_KEY_ENV, "secret-value"),
            (ADMIN_SECURITY_TOKEN_ENV, "token-value"),
        ])));

        let credentials = auth
            .resolve_admin_credentials(CredentialSourceKind::Environment)
            .expect("source")
            .expect("credentials");
        let debug = format!("{credentials:?}");
        for secret in ["access-value", "secret-value", "token-value"] {
            assert!(!debug.contains(secret));
        }
    }
}
