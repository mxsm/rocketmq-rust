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
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_error::REDACTED;
use rocketmq_runtime::BlockingExecutor;
use std::env;
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    /// Compatibility configuration location. It is deliberately independent
    /// of the selected backend so server SQL never silently falls back to a
    /// local storage backend.
    pub interim_config_path: PathBuf,
    pub auth: AuthConfig,
    pub monitor_store_path: PathBuf,
    pub dashboard_history_interval_secs: u64,
    pub initial_config: DashboardConfigView,
    pub admin_credentials: Option<AdminCredentials>,
}

impl fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppConfig")
            .field("server", &self.server)
            .field("storage", &self.storage)
            .field("interim_config_path", &self.interim_config_path)
            .field("auth", &self.auth)
            .field("monitor_store_path", &self.monitor_store_path)
            .field("dashboard_history_interval_secs", &self.dashboard_history_interval_secs)
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
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field("login_required", &self.login_required)
            .field("username", &self.username)
            .field("password", &REDACTED)
            .finish()
    }
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
            interim_config_path: env::var("DASHBOARD_WEB_INTERIM_CONFIG_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("data/dashboard-interim-config.json")),
            storage,
            auth: AuthConfig {
                login_required: parse_bool_env("DASHBOARD_WEB_LOGIN_REQUIRED", false),
                username: env::var("DASHBOARD_WEB_USERNAME").unwrap_or_else(|_| "admin".to_string()),
                password: env::var("DASHBOARD_WEB_PASSWORD").unwrap_or_else(|_| "rocketmq".to_string()),
            },
            monitor_store_path: env::var("DASHBOARD_WEB_MONITOR_STORE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("data/monitor/consumer-monitor-config.json")),
            dashboard_history_interval_secs: parse_u64_env("DASHBOARD_WEB_HISTORY_INTERVAL_SECS", 60)?,
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

/// Compatibility configuration store with all filesystem access routed through
/// the injected storage I/O executor.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    path: PathBuf,
    storage_io: BlockingExecutor,
}

impl ConfigStore {
    pub fn new(path: PathBuf, storage_io: BlockingExecutor) -> Self {
        Self { path, storage_io }
    }

    pub async fn load_or_init(
        &self,
        default_config: &DashboardConfigView,
    ) -> Result<DashboardConfigView, DashboardError> {
        let path = self.path.clone();
        let default_config = default_config.clone();
        self.storage_io
            .spawn_io("dashboard-compatibility-config-load", move || {
                load_or_init_file(&path, &default_config)
            })
            .await
            .map_err(|error| DashboardError::internal_source("Could not read compatibility config", error))?
    }

    pub async fn save(&self, config: &DashboardConfigView) -> Result<(), DashboardError> {
        let path = self.path.clone();
        let config = config.clone();
        self.storage_io
            .spawn_io("dashboard-compatibility-config-save", move || save_file(&path, &config))
            .await
            .map_err(|error| DashboardError::internal_source("Could not write compatibility config", error))?
    }
}

fn load_or_init_file(
    path: &PathBuf,
    default_config: &DashboardConfigView,
) -> Result<DashboardConfigView, DashboardError> {
    if !path.exists() {
        save_file(path, default_config)?;
        return Ok(default_config.clone());
    }
    let content = fs::read_to_string(path)
        .map_err(|error| DashboardError::config_source("Failed to read compatibility config file", error))?;
    serde_json::from_str(&content)
        .map_err(|error| DashboardError::config_source("Failed to parse compatibility config file", error))
}

fn save_file(path: &PathBuf, config: &DashboardConfigView) -> Result<(), DashboardError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| DashboardError::config_source("Failed to create compatibility config directory", error))?;
    }
    let content = serde_json::to_string_pretty(config)
        .map_err(|error| DashboardError::internal_source("Failed to serialize compatibility config", error))?;
    fs::write(path, content)
        .map_err(|error| DashboardError::config_source("Failed to write compatibility config file", error))
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
    fn interim_store_round_trips_config_without_selecting_a_storage_backend() {
        let dir = tempfile::tempdir().expect("temp dir");
        let owner =
            rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let store = ConfigStore::new(
                dir.path().join("interim-config.json"),
                owner
                    .root_context()
                    .component("compatibility-config-test")
                    .storage_io()
                    .clone(),
            );
            let config = DashboardConfigView {
                current_namesrv: Some("localhost:9876".to_string()),
                storage_backend: StorageBackend::Postgres,
                ..DashboardConfigView::default()
            };
            store.save(&config).await.expect("save config");
            let loaded = store
                .load_or_init(&DashboardConfigView::default())
                .await
                .expect("load config");
            assert_eq!(loaded.current_namesrv.as_deref(), Some("localhost:9876"));
            assert_eq!(loaded.storage_backend, StorageBackend::Postgres);
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
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
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("dashboard-secret"));
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
            interim_config_path: "data/interim-config.json".into(),
            auth: AuthConfig {
                login_required: true,
                username: "admin".to_string(),
                password: "dashboard-secret".to_string(),
            },
            monitor_store_path: "monitor-config.json".into(),
            dashboard_history_interval_secs: 60,
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
