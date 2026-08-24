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
use crate::error::DashboardError;
use crate::model::DashboardConfigView;
use crate::model::StorageBackend;
use axum::http::HeaderValue;
use axum::http::Uri;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_error::REDACTED;
use std::env;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub dashboard_history_interval_secs: u64,
    pub dashboard_history_retention_days: u32,
    pub dashboard_history_retention_batch_size: u32,
    pub dashboard_history_lease_ttl_secs: u64,
    pub initial_config: DashboardConfigView,
    pub admin_credentials: Option<AdminCredentials>,
}

impl fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppConfig")
            .field("server", &self.server)
            .field("storage", &self.storage)
            .field("auth", &self.auth)
            .field("dashboard_history_interval_secs", &self.dashboard_history_interval_secs)
            .field(
                "dashboard_history_retention_days",
                &self.dashboard_history_retention_days,
            )
            .field(
                "dashboard_history_retention_batch_size",
                &self.dashboard_history_retention_batch_size,
            )
            .field(
                "dashboard_history_lease_ttl_secs",
                &self.dashboard_history_lease_ttl_secs,
            )
            .field("initial_config", &self.initial_config)
            .field("admin_credentials", &self.admin_credentials.as_ref().map(|_| REDACTED))
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub connect_timeout_ms: u64,
    pub acquire_timeout_ms: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

impl Default for SqlPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            connect_timeout_ms: 5_000,
            acquire_timeout_ms: 3_000,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1_800,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    /// File data directory, or the SQLite database file.
    pub data_path: PathBuf,
    /// Present only for MySQL and PostgreSQL. It must never reach normal logs,
    /// Debug output, or API responses.
    pub database_url: Option<String>,
    pub pool: SqlPoolConfig,
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageConfig")
            .field("backend", &self.backend)
            .field("data_path", &self.data_path)
            .field("database_url", &self.database_url.as_ref().map(|_| REDACTED))
            .field("pool", &self.pool)
            .finish()
    }
}

impl StorageConfig {
    pub fn from_env() -> Result<Self, DashboardError> {
        let backend_value = env::var("DASHBOARD_WEB_STORAGE_BACKEND").unwrap_or_else(|_| "file".to_string());
        let backend = StorageBackend::parse(&backend_value).map_err(DashboardError::Config)?;
        let data_path = env::var("DASHBOARD_WEB_STORAGE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_storage_path(backend));
        let database_url = env::var("DASHBOARD_WEB_DATABASE_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let pool = SqlPoolConfig {
            min_connections: parse_u32_env("DASHBOARD_WEB_DB_MIN_CONNECTIONS", 1)?,
            max_connections: parse_u32_env("DASHBOARD_WEB_DB_MAX_CONNECTIONS", 10)?,
            connect_timeout_ms: parse_u64_env("DASHBOARD_WEB_DB_CONNECT_TIMEOUT_MS", 5_000)?,
            acquire_timeout_ms: parse_u64_env("DASHBOARD_WEB_DB_ACQUIRE_TIMEOUT_MS", 3_000)?,
            idle_timeout_secs: parse_u64_env("DASHBOARD_WEB_DB_IDLE_TIMEOUT_SECS", 600)?,
            max_lifetime_secs: parse_u64_env("DASHBOARD_WEB_DB_MAX_LIFETIME_SECS", 1_800)?,
        };
        let config = Self {
            backend,
            data_path,
            database_url,
            pool,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), DashboardError> {
        if self.data_path.as_os_str().is_empty() {
            return Err(DashboardError::Config(
                "DASHBOARD_WEB_STORAGE_PATH must not be empty".to_string(),
            ));
        }
        if self.pool.min_connections > self.pool.max_connections {
            return Err(DashboardError::Config(
                "DASHBOARD_WEB_DB_MIN_CONNECTIONS must not exceed DASHBOARD_WEB_DB_MAX_CONNECTIONS".to_string(),
            ));
        }
        if self.pool.max_connections == 0
            || self.pool.connect_timeout_ms == 0
            || self.pool.acquire_timeout_ms == 0
            || self.pool.idle_timeout_secs == 0
            || self.pool.max_lifetime_secs == 0
        {
            return Err(DashboardError::Config(
                "database pool limits and timeouts must be greater than zero".to_string(),
            ));
        }
        match self.backend {
            StorageBackend::Sqlite if sqlite_memory_path(&self.data_path) => Err(DashboardError::Config(
                "DASHBOARD_WEB_STORAGE_PATH must name an on-disk SQLite database; in-memory SQLite is not supported"
                    .to_string(),
            )),
            StorageBackend::File | StorageBackend::Sqlite if self.database_url.is_some() => {
                Err(DashboardError::Config(
                    "DASHBOARD_WEB_DATABASE_URL is only valid for mysql and postgres storage".to_string(),
                ))
            }
            StorageBackend::MySql => validate_database_url(self.database_url.as_deref(), "mysql://", "mysql"),
            StorageBackend::Postgres => {
                let Some(url) = self.database_url.as_deref() else {
                    return Err(DashboardError::Config(
                        "DASHBOARD_WEB_DATABASE_URL is required for postgres storage".to_string(),
                    ));
                };
                if url.starts_with("postgres://") || url.starts_with("postgresql://") {
                    Ok(())
                } else {
                    Err(DashboardError::Config(
                        "DASHBOARD_WEB_DATABASE_URL must use postgres:// or postgresql:// for postgres storage"
                            .to_string(),
                    ))
                }
            }
            StorageBackend::File | StorageBackend::Sqlite => Ok(()),
        }
    }
}

fn sqlite_memory_path(path: &Path) -> bool {
    let value = path.to_string_lossy().trim().to_ascii_lowercase();
    value == ":memory:" || value == "file::memory:" || (value.starts_with("file:") && value.contains("mode=memory"))
}

fn default_storage_path(backend: StorageBackend) -> PathBuf {
    match backend {
        StorageBackend::File => PathBuf::from("data/dashboard"),
        StorageBackend::Sqlite => PathBuf::from("data/dashboard/dashboard.db"),
        StorageBackend::MySql | StorageBackend::Postgres => PathBuf::from("data/dashboard"),
    }
}

fn validate_database_url(value: Option<&str>, expected_prefix: &str, backend: &str) -> Result<(), DashboardError> {
    match value {
        Some(url) if url.starts_with(expected_prefix) => Ok(()),
        Some(_) => Err(DashboardError::Config(format!(
            "DASHBOARD_WEB_DATABASE_URL must use {expected_prefix} for {backend} storage"
        ))),
        None => Err(DashboardError::Config(format!(
            "DASHBOARD_WEB_DATABASE_URL is required for {backend} storage"
        ))),
    }
}

#[derive(Clone)]
pub struct AuthConfig {
    pub login_required: bool,
    pub username: String,
    pub password: String,
    pub session_ttl_secs: u64,
    pub session_retention_days: u32,
    pub audit_retention_days: u32,
    pub cleanup_interval_secs: u64,
    pub cleanup_batch_size: u32,
    pub max_active_sessions: u32,
    pub cookie_secure: bool,
    /// Exact origin allowed for credentialed browser requests. `None` means
    /// CORS is disabled rather than falling back to an unsafe wildcard.
    pub allowed_origin: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            login_required: false,
            username: "admin".to_string(),
            password: "rocketmq".to_string(),
            session_ttl_secs: 28_800,
            session_retention_days: 7,
            audit_retention_days: 180,
            cleanup_interval_secs: 3_600,
            cleanup_batch_size: 1_000,
            max_active_sessions: 32,
            cookie_secure: true,
            allowed_origin: None,
        }
    }
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field("login_required", &self.login_required)
            .field("username", &self.username)
            .field("password", &REDACTED)
            .field("session_ttl_secs", &self.session_ttl_secs)
            .field("session_retention_days", &self.session_retention_days)
            .field("audit_retention_days", &self.audit_retention_days)
            .field("cleanup_interval_secs", &self.cleanup_interval_secs)
            .field("cleanup_batch_size", &self.cleanup_batch_size)
            .field("max_active_sessions", &self.max_active_sessions)
            .field("cookie_secure", &self.cookie_secure)
            .field("allowed_origin", &self.allowed_origin)
            .finish()
    }
}

impl AuthConfig {
    /// Validates and materializes the sole credentialed browser origin before
    /// the router is constructed. CORS must never attempt to interpret a
    /// wildcard, an opaque origin, or a URL with request-specific components.
    pub fn cors_origin(&self) -> Result<Option<HeaderValue>, DashboardError> {
        self.allowed_origin.as_deref().map(validate_cors_origin).transpose()
    }
}

fn validate_cors_origin(origin: &str) -> Result<HeaderValue, DashboardError> {
    const ERROR: &str = "DASHBOARD_WEB_CORS_ORIGIN must be one exact http:// or https:// origin";
    if origin == "*" || origin.eq_ignore_ascii_case("null") || origin.contains(',') {
        return Err(DashboardError::Config(ERROR.to_string()));
    }

    let uri = origin
        .parse::<Uri>()
        .map_err(|_| DashboardError::Config(ERROR.to_string()))?;
    let Some(scheme) = uri.scheme_str().filter(|scheme| matches!(*scheme, "http" | "https")) else {
        return Err(DashboardError::Config(ERROR.to_string()));
    };
    let Some(authority) = uri.authority() else {
        return Err(DashboardError::Config(ERROR.to_string()));
    };
    let supplied_port = authority.as_str().strip_prefix('[').map_or_else(
        || authority.as_str().rsplit_once(':').map(|(_, port)| port),
        |bracketed| {
            bracketed
                .split_once(']')
                .and_then(|(_, remainder)| remainder.strip_prefix(':'))
        },
    );
    let port_is_valid = supplied_port.is_none_or(|port| !port.is_empty() && port.parse::<u16>().is_ok());
    let bracketed_host_is_valid = authority.as_str().strip_prefix('[').is_none_or(|bracketed| {
        bracketed
            .split_once(']')
            .is_some_and(|(host, _)| host.parse::<std::net::Ipv6Addr>().is_ok())
    });
    if authority.as_str().is_empty()
        || authority.host().is_empty()
        || !port_is_valid
        || !bracketed_host_is_valid
        || authority.as_str().contains('@')
        || origin != format!("{scheme}://{authority}")
    {
        return Err(DashboardError::Config(ERROR.to_string()));
    }
    HeaderValue::from_str(origin).map_err(|_| DashboardError::Config(ERROR.to_string()))
}

impl AppConfig {
    pub fn load() -> Result<Self, DashboardError> {
        let storage = StorageConfig::from_env()?;
        let mut initial_config = DashboardConfigView {
            storage_backend: storage.backend,
            ..DashboardConfigView::default()
        };
        if let Ok(namesrv_addr) = env::var("NAMESRV_ADDR").or_else(|_| env::var("rocketmq.config.namesrvAddr")) {
            let namesrv_addr = namesrv_addr.trim().to_string();
            if !namesrv_addr.is_empty() {
                initial_config.current_namesrv = Some(namesrv_addr.clone());
                initial_config.namesrv_addr_list = vec![namesrv_addr];
            }
        }
        initial_config.use_vip_channel =
            parse_bool_env("DASHBOARD_WEB_USE_VIP_CHANNEL", initial_config.use_vip_channel);
        initial_config.use_tls = parse_bool_env("DASHBOARD_WEB_USE_TLS", initial_config.use_tls);

        Ok(Self {
            server: ServerConfig {
                host: env::var("DASHBOARD_WEB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
                port: parse_u16_env("DASHBOARD_WEB_PORT", 8082)?,
            },
            storage,
            auth: AuthConfig {
                login_required: parse_bool_env("DASHBOARD_WEB_LOGIN_REQUIRED", false),
                username: env::var("DASHBOARD_WEB_USERNAME").unwrap_or_else(|_| "admin".to_string()),
                password: env::var("DASHBOARD_WEB_PASSWORD").unwrap_or_else(|_| "rocketmq".to_string()),
                session_ttl_secs: positive_u64(
                    "DASHBOARD_WEB_SESSION_TTL_SECS",
                    parse_u64_env("DASHBOARD_WEB_SESSION_TTL_SECS", 28_800)?,
                    2_592_000,
                )?,
                session_retention_days: positive_u32(
                    "DASHBOARD_WEB_SESSION_RETENTION_DAYS",
                    parse_u32_env("DASHBOARD_WEB_SESSION_RETENTION_DAYS", 7)?,
                    36_500,
                )?,
                audit_retention_days: positive_u32(
                    "DASHBOARD_WEB_AUDIT_RETENTION_DAYS",
                    parse_u32_env("DASHBOARD_WEB_AUDIT_RETENTION_DAYS", 180)?,
                    36_500,
                )?,
                cleanup_interval_secs: positive_u64(
                    "DASHBOARD_WEB_SESSION_AUDIT_CLEANUP_INTERVAL_SECS",
                    parse_u64_env("DASHBOARD_WEB_SESSION_AUDIT_CLEANUP_INTERVAL_SECS", 3_600)?,
                    86_400,
                )?,
                cleanup_batch_size: positive_u32(
                    "DASHBOARD_WEB_SESSION_AUDIT_CLEANUP_BATCH_SIZE",
                    parse_u32_env("DASHBOARD_WEB_SESSION_AUDIT_CLEANUP_BATCH_SIZE", 1_000)?,
                    1_000,
                )?,
                max_active_sessions: positive_u32(
                    "DASHBOARD_WEB_MAX_ACTIVE_SESSIONS",
                    parse_u32_env("DASHBOARD_WEB_MAX_ACTIVE_SESSIONS", 32)?,
                    32,
                )?,
                cookie_secure: parse_bool_env("DASHBOARD_WEB_SESSION_COOKIE_SECURE", true),
                allowed_origin: env::var("DASHBOARD_WEB_CORS_ORIGIN")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            },
            dashboard_history_interval_secs: parse_u64_env("DASHBOARD_WEB_HISTORY_INTERVAL_SECS", 60)?,
            dashboard_history_retention_days: positive_u32(
                "DASHBOARD_WEB_HISTORY_RETENTION_DAYS",
                parse_u32_env("DASHBOARD_WEB_HISTORY_RETENTION_DAYS", 30)?,
                36_500,
            )?,
            dashboard_history_retention_batch_size: positive_u32(
                "DASHBOARD_WEB_HISTORY_RETENTION_BATCH_SIZE",
                parse_u32_env("DASHBOARD_WEB_HISTORY_RETENTION_BATCH_SIZE", 500)?,
                5_000,
            )?,
            dashboard_history_lease_ttl_secs: positive_u64(
                "DASHBOARD_WEB_HISTORY_LEASE_TTL_SECS",
                parse_u64_env("DASHBOARD_WEB_HISTORY_LEASE_TTL_SECS", 30)?,
                86_400,
            )?,
            initial_config,
            admin_credentials: admin_credentials_from_env()?,
        })
    }
}

fn admin_credentials_from_env() -> Result<Option<AdminCredentials>, DashboardError> {
    resolve_admin_credentials(
        env::var("DASHBOARD_WEB_ROCKETMQ_ACCESS_KEY").ok(),
        env::var("DASHBOARD_WEB_ROCKETMQ_SECRET_KEY").ok(),
        env::var("DASHBOARD_WEB_ROCKETMQ_SECURITY_TOKEN").ok(),
    )
}

fn resolve_admin_credentials(
    access_key: Option<String>,
    secret_key: Option<String>,
    security_token: Option<String>,
) -> Result<Option<AdminCredentials>, DashboardError> {
    match (access_key, secret_key, security_token) {
        (None, None, None) => Ok(None),
        (Some(access_key), Some(secret_key), security_token) => {
            AdminCredentials::try_new(access_key, secret_key, security_token)
                .map(Some)
                .map_err(|_| DashboardError::Config("RocketMQ admin credentials are invalid".to_string()))
        }
        _ => Err(DashboardError::Config(
            "RocketMQ admin credentials require both access and secret keys".to_string(),
        )),
    }
}

fn parse_bool_env(name: &str, default_value: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default_value)
}

fn parse_u16_env(name: &str, default_value: u16) -> Result<u16, DashboardError> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| DashboardError::Config(format!("{name} must be a valid u16")))
        })
        .transpose()
        .map(|value| value.unwrap_or(default_value))
}

fn parse_u32_env(name: &str, default_value: u32) -> Result<u32, DashboardError> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| DashboardError::Config(format!("{name} must be a valid positive integer")))
        })
        .transpose()
        .map(|value| value.unwrap_or(default_value))
}

fn parse_u64_env(name: &str, default_value: u64) -> Result<u64, DashboardError> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| DashboardError::Config(format!("{name} must be a valid positive integer")))
        })
        .transpose()
        .map(|value| value.unwrap_or(default_value))
}

fn positive_u32(name: &str, value: u32, maximum: u32) -> Result<u32, DashboardError> {
    if value == 0 || value > maximum {
        Err(DashboardError::Config(format!(
            "{name} must be between 1 and {maximum}"
        )))
    } else {
        Ok(value)
    }
}

fn positive_u64(name: &str, value: u64, maximum: u64) -> Result<u64, DashboardError> {
    if value == 0 || value > maximum {
        Err(DashboardError::Config(format!(
            "{name} must be between 1 and {maximum}"
        )))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_backend_values_are_strict() {
        assert_eq!(StorageBackend::parse("file"), Ok(StorageBackend::File));
        assert_eq!(StorageBackend::parse("mysql"), Ok(StorageBackend::MySql));
        assert!(StorageBackend::parse("memory").is_err());
    }

    #[test]
    fn sqlite_memory_locations_are_rejected() {
        for path in [":memory:", "file::memory:", "file:dashboard?mode=memory"] {
            let config = StorageConfig {
                backend: StorageBackend::Sqlite,
                data_path: path.into(),
                database_url: None,
                pool: SqlPoolConfig::default(),
            };
            assert!(config.validate().is_err(), "{path} must be rejected");
        }
    }

    #[test]
    fn storage_config_debug_redacts_database_url() {
        let config = StorageConfig {
            backend: StorageBackend::MySql,
            data_path: "unused".into(),
            database_url: Some("mysql://dashboard:super-secret@localhost/dashboard".to_string()),
            pool: SqlPoolConfig::default(),
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn auth_config_debug_redacts_password() {
        let config = AuthConfig {
            login_required: true,
            username: "admin".to_string(),
            password: "dashboard-secret".to_string(),
            ..AuthConfig::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("dashboard-secret"));
    }

    #[test]
    fn cors_origin_requires_one_exact_http_or_https_origin() {
        for origin in [
            "http://console.example",
            "https://console.example:8443",
            "http://[::1]:8080",
        ] {
            let config = AuthConfig {
                allowed_origin: Some(origin.to_string()),
                ..AuthConfig::default()
            };
            assert_eq!(
                config
                    .cors_origin()
                    .expect("valid exact origin")
                    .as_ref()
                    .and_then(|value| value.to_str().ok()),
                Some(origin)
            );
        }

        for origin in [
            "*",
            "null",
            "https://console.example/path",
            "https://console.example/",
            "https://user@console.example",
            "https://console.example,https://other.example",
            "https://:8443",
            "https://console.example:not-a-port",
            "https://console.example:65536",
            "https://[::1",
            "https://[not-an-ipv6]:8443",
            "ftp://console.example",
            "console.example",
        ] {
            let config = AuthConfig {
                allowed_origin: Some(origin.to_string()),
                ..AuthConfig::default()
            };
            assert!(config.cors_origin().is_err(), "{origin} must be rejected");
        }
    }

    #[test]
    fn app_config_debug_redacts_all_credentials() {
        let config = AppConfig {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8082,
            },
            storage: StorageConfig {
                backend: StorageBackend::MySql,
                data_path: "data/dashboard".into(),
                database_url: Some("mysql://dashboard:database-secret@localhost/dashboard".to_string()),
                pool: SqlPoolConfig::default(),
            },
            auth: AuthConfig {
                login_required: true,
                username: "admin".to_string(),
                password: "dashboard-secret".to_string(),
                ..AuthConfig::default()
            },
            dashboard_history_interval_secs: 60,
            dashboard_history_retention_days: 30,
            dashboard_history_retention_batch_size: 500,
            dashboard_history_lease_ttl_secs: 30,
            initial_config: DashboardConfigView::default(),
            admin_credentials: Some(
                AdminCredentials::try_new("access-value", "secret-value", Some("token-value".to_string()))
                    .expect("credentials"),
            ),
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("admin_credentials: Some(\"<redacted>\")"));
        assert!(!debug.contains("dashboard-secret"));
        assert!(!debug.contains("database-secret"));
        assert!(!debug.contains("access-value"));
        assert!(!debug.contains("secret-value"));
        assert!(!debug.contains("token-value"));
    }

    #[test]
    fn admin_credentials_require_a_complete_redacted_pair() {
        assert!(
            resolve_admin_credentials(None, None, None)
                .expect("no credentials")
                .is_none()
        );
        assert!(resolve_admin_credentials(Some("access".to_string()), None, None).is_err());
        let credentials =
            resolve_admin_credentials(Some("access-value".to_string()), Some("secret-value".to_string()), None)
                .expect("complete credentials")
                .expect("credentials");
        let debug = format!("{credentials:?}");
        assert!(!debug.contains("access-value"));
        assert!(!debug.contains("secret-value"));
        assert!(debug.contains("<redacted>"));
    }
}
