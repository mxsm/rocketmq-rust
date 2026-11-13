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

use serde::Deserialize;
use serde::Serialize;

use super::tls_config::TlsConfig;
use crate::file_region::FileTransferMode;

/// Default value functions for Serde deserialization
mod defaults {
    pub fn listen_port() -> u32 {
        10911
    }

    pub fn bind_address() -> String {
        "0.0.0.0".to_string()
    }
}

/// How a transport session schedules reading requests and writing responses on its socket.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionIoModel {
    /// The session task reads and writes, taking turns between the two.
    ///
    /// The kernel serializes reads and writes on one socket. A reader and a writer on two
    /// threads wait on each other's socket lock, which caps the throughput of a busy
    /// connection; taking turns on one task removes that wait.
    #[default]
    SharedTask,
    /// The writer runs as a task of its own, as before the shared task was introduced.
    ///
    /// Kept for one release as a fallback.
    SeparateTasks,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "defaults::listen_port")]
    pub listen_port: u32,

    #[serde(default = "defaults::bind_address")]
    pub bind_address: String,

    #[serde(default, alias = "tls")]
    pub tls_config: TlsConfig,

    #[serde(default)]
    pub file_transfer_mode: FileTransferMode,

    #[serde(default)]
    pub session_io_model: SessionIoModel,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen_port: defaults::listen_port(),
            bind_address: defaults::bind_address(),
            tls_config: TlsConfig::default(),
            file_transfer_mode: FileTransferMode::Auto,
            session_io_model: SessionIoModel::SharedTask,
        }
    }
}

impl ServerConfig {
    pub fn bind_address(&self) -> String {
        self.bind_address.clone()
    }

    pub fn listen_port(&self) -> u32 {
        self.listen_port
    }

    pub fn tls_config(&self) -> &TlsConfig {
        &self.tls_config
    }

    pub const fn file_transfer_mode(&self) -> FileTransferMode {
        self.file_transfer_mode
    }

    pub const fn session_io_model(&self) -> SessionIoModel {
        self.session_io_model
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config() {
        let config = ServerConfig::default();

        assert_eq!(config.listen_port(), 10911);

        assert_eq!(config.bind_address(), "0.0.0.0".to_string());

        assert_eq!(
            config.tls_config().server.mode,
            crate::config::tls_config::TlsMode::Permissive
        );
        assert_eq!(config.session_io_model(), SessionIoModel::SharedTask);
    }

    #[test]
    fn the_session_io_model_defaults_to_the_shared_task_and_parses_the_fallback() {
        let omitted: ServerConfig = serde_json::from_str("{}").expect("an empty config parses");
        assert_eq!(omitted.session_io_model(), SessionIoModel::SharedTask);

        let fallback: ServerConfig =
            serde_json::from_str(r#"{"sessionIoModel":"separate-tasks"}"#).expect("the fallback parses");
        assert_eq!(fallback.session_io_model(), SessionIoModel::SeparateTasks);

        assert!(serde_json::from_str::<ServerConfig>(r#"{"sessionIoModel":"threads"}"#).is_err());
    }
}
