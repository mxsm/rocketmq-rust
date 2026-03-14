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
use std::fmt;

pub(crate) type NameServerResult<T> = Result<T, NameServerError>;

#[derive(Debug)]
pub(crate) enum NameServerError {
    Io(std::io::Error),
    Database(rusqlite::Error),
    AppPath(String),
    Validation(String),
}

impl fmt::Display for NameServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Database(error) => write!(f, "Database error: {error}"),
            Self::AppPath(message) => write!(f, "Application path error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
        }
    }
}

impl std::error::Error for NameServerError {}

impl From<std::io::Error> for NameServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for NameServerError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NameServerRecord {
    pub(crate) id: i64,
    pub(crate) address: String,
    pub(crate) is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NameServerHomePageResponse {
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) namesrv_addr_list: Vec<String>,
    pub(crate) current_namesrv: Option<String>,
    pub(crate) use_vip_channel: bool,
    pub(crate) use_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NameServerMutationResponse {
    pub(crate) success: bool,
    pub(crate) message: String,
}
