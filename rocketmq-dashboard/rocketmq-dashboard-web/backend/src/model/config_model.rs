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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageBackend {
    File,
    Sqlite,
}

impl StorageBackend {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "sqlite" | "sqlite3" => Self::Sqlite,
            _ => Self::File,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardConfigView {
    pub current_namesrv: Option<String>,
    pub namesrv_addr_list: Vec<String>,
    #[serde(rename = "useVIPChannel")]
    pub use_vip_channel: bool,
    #[serde(rename = "useTLS")]
    pub use_tls: bool,
    pub current_proxy_addr: Option<String>,
    pub proxy_addr_list: Vec<String>,
    pub storage_backend: StorageBackend,
}

impl Default for DashboardConfigView {
    fn default() -> Self {
        Self {
            current_namesrv: Some("127.0.0.1:9876".to_string()),
            namesrv_addr_list: vec!["127.0.0.1:9876".to_string()],
            use_vip_channel: true,
            use_tls: false,
            current_proxy_addr: Some("127.0.0.1:8080".to_string()),
            proxy_addr_list: vec!["127.0.0.1:8080".to_string()],
            storage_backend: StorageBackend::File,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AddressRequest {
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NameserverListRequest {
    pub namesrv_addr_list: Vec<String>,
    pub current_namesrv: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BoolSettingRequest {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMutationResult {
    pub message: String,
    pub config: DashboardConfigView,
}
