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
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub monitor_store_path: PathBuf,
    pub dashboard_history_interval_secs: u64,
    pub initial_config: DashboardConfigView,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub login_required: bool,
    pub username: String,
    pub password: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, DashboardError> {
        let storage_backend =
            StorageBackend::parse(&env::var("DASHBOARD_WEB_STORAGE_BACKEND").unwrap_or_else(|_| "file".to_string()));
        let storage_path = env::var("DASHBOARD_WEB_STORAGE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/dashboard-config.json"));

        let mut initial_config = DashboardConfigView {
            storage_backend,
            ..DashboardConfigView::default()
        };

        if let Ok(namesrv_addr) = env::var("NAMESRV_ADDR").or_else(|_| env::var("rocketmq.config.namesrvAddr")) {
            let namesrv_addr = namesrv_addr.trim().to_string();
            if !namesrv_addr.is_empty() {
                initial_config.current_namesrv = Some(namesrv_addr.clone());
                initial_config.namesrv_addr_list = vec![namesrv_addr];
            }
        }

        let monitor_store_path = env::var("DASHBOARD_WEB_MONITOR_STORE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/monitor/consumer-monitor-config.json"));
        let dashboard_history_interval_secs = env::var("DASHBOARD_WEB_HISTORY_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60);

        Ok(Self {
            server: ServerConfig {
                host: env::var("DASHBOARD_WEB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
                port: env::var("DASHBOARD_WEB_PORT")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(8082),
            },
            storage: StorageConfig {
                backend: storage_backend,
                path: storage_path,
            },
            auth: AuthConfig {
                login_required: parse_bool_env("DASHBOARD_WEB_LOGIN_REQUIRED", false),
                username: env::var("DASHBOARD_WEB_USERNAME").unwrap_or_else(|_| "admin".to_string()),
                password: env::var("DASHBOARD_WEB_PASSWORD").unwrap_or_else(|_| "rocketmq".to_string()),
            },
            monitor_store_path,
            dashboard_history_interval_secs,
            initial_config,
        })
    }
}

fn parse_bool_env(name: &str, default_value: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default_value)
}

#[derive(Debug, Clone)]
pub enum ConfigStore {
    File(FileConfigStore),
    Sqlite(SqliteConfigStore),
}

impl ConfigStore {
    pub fn new(config: &StorageConfig) -> Result<Self, DashboardError> {
        match config.backend {
            StorageBackend::File => Ok(Self::File(FileConfigStore {
                path: config.path.clone(),
            })),
            StorageBackend::Sqlite => Ok(Self::Sqlite(SqliteConfigStore {
                path: config.path.clone(),
            })),
        }
    }

    pub fn load_or_init(&self, default_config: &DashboardConfigView) -> Result<DashboardConfigView, DashboardError> {
        match self {
            Self::File(store) => store.load_or_init(default_config),
            Self::Sqlite(store) => store.load_or_init(default_config),
        }
    }

    pub fn save(&self, config: &DashboardConfigView) -> Result<(), DashboardError> {
        match self {
            Self::File(store) => store.save(config),
            Self::Sqlite(store) => store.save(config),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileConfigStore {
    path: PathBuf,
}

impl FileConfigStore {
    fn load_or_init(&self, default_config: &DashboardConfigView) -> Result<DashboardConfigView, DashboardError> {
        if !self.path.exists() {
            self.save(default_config)?;
            return Ok(default_config.clone());
        }

        let content = fs::read_to_string(&self.path)
            .map_err(|error| DashboardError::Config(format!("Failed to read config file: {error}")))?;
        serde_json::from_str(&content)
            .map_err(|error| DashboardError::Config(format!("Failed to parse config file: {error}")))
    }

    fn save(&self, config: &DashboardConfigView) -> Result<(), DashboardError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| DashboardError::Config(format!("Failed to create config directory: {error}")))?;
        }
        let content = serde_json::to_string_pretty(config)
            .map_err(|error| DashboardError::Internal(format!("Failed to serialize config: {error}")))?;
        fs::write(&self.path, content)
            .map_err(|error| DashboardError::Config(format!("Failed to write config file: {error}")))
    }
}

#[derive(Debug, Clone)]
pub struct SqliteConfigStore {
    path: PathBuf,
}

impl SqliteConfigStore {
    fn load_or_init(&self, default_config: &DashboardConfigView) -> Result<DashboardConfigView, DashboardError> {
        let connection = self.open_connection()?;
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS dashboard_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    payload TEXT NOT NULL
                )",
                [],
            )
            .map_err(|error| DashboardError::Config(format!("Failed to initialize config table: {error}")))?;

        let payload = connection
            .query_row("SELECT payload FROM dashboard_config WHERE id = 1", [], |row| {
                row.get::<_, String>(0)
            })
            .ok();

        if let Some(payload) = payload {
            return serde_json::from_str(&payload)
                .map_err(|error| DashboardError::Config(format!("Failed to parse SQLite config payload: {error}")));
        }

        self.save(default_config)?;
        Ok(default_config.clone())
    }

    fn save(&self, config: &DashboardConfigView) -> Result<(), DashboardError> {
        let connection = self.open_connection()?;
        let payload = serde_json::to_string_pretty(config)
            .map_err(|error| DashboardError::Internal(format!("Failed to serialize config: {error}")))?;
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS dashboard_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    payload TEXT NOT NULL
                )",
                [],
            )
            .map_err(|error| DashboardError::Config(format!("Failed to initialize config table: {error}")))?;
        connection
            .execute(
                "INSERT INTO dashboard_config (id, payload) VALUES (1, ?1)
                 ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
                [payload],
            )
            .map_err(|error| DashboardError::Config(format!("Failed to write SQLite config: {error}")))?;
        Ok(())
    }

    fn open_connection(&self) -> Result<rusqlite::Connection, DashboardError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| DashboardError::Config(format!("Failed to create SQLite directory: {error}")))?;
        }
        rusqlite::Connection::open(&self.path)
            .map_err(|error| DashboardError::Config(format!("Failed to open SQLite config store: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigStore;
    use super::StorageConfig;
    use crate::model::DashboardConfigView;
    use crate::model::StorageBackend;

    #[test]
    fn file_store_round_trips_config() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = ConfigStore::new(&StorageConfig {
            backend: StorageBackend::File,
            path: dir.path().join("dashboard-config.json"),
        })
        .expect("store");
        let config = DashboardConfigView {
            current_namesrv: Some("localhost:9876".to_string()),
            ..DashboardConfigView::default()
        };

        store.save(&config).expect("save config");
        let loaded = store
            .load_or_init(&DashboardConfigView::default())
            .expect("load config");

        assert_eq!(loaded.current_namesrv.as_deref(), Some("localhost:9876"));
    }

    #[test]
    fn sqlite_store_round_trips_config() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = ConfigStore::new(&StorageConfig {
            backend: StorageBackend::Sqlite,
            path: dir.path().join("dashboard.db"),
        })
        .expect("store");
        let config = DashboardConfigView {
            current_proxy_addr: Some("localhost:8081".to_string()),
            storage_backend: StorageBackend::Sqlite,
            ..DashboardConfigView::default()
        };

        store.save(&config).expect("save config");
        let loaded = store
            .load_or_init(&DashboardConfigView::default())
            .expect("load config");

        assert_eq!(loaded.current_proxy_addr.as_deref(), Some("localhost:8081"));
        assert_eq!(loaded.storage_backend, StorageBackend::Sqlite);
    }
}
