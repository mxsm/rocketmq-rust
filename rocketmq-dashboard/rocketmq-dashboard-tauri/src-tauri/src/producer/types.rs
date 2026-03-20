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

pub(crate) type ProducerResult<T> = Result<T, ProducerError>;

#[derive(Debug)]
pub(crate) enum ProducerError {
    Configuration(String),
    Validation(String),
    RocketMQ(String),
}

impl fmt::Display for ProducerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "Configuration error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
            Self::RocketMQ(message) => write!(f, "RocketMQ error: {message}"),
        }
    }
}

impl std::error::Error for ProducerError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProducerConnectionItem {
    pub(crate) client_id: String,
    pub(crate) client_addr: String,
    pub(crate) language: String,
    pub(crate) version: i32,
    pub(crate) version_desc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProducerConnectionView {
    pub(crate) topic: String,
    pub(crate) producer_group: String,
    pub(crate) connection_count: usize,
    pub(crate) connections: Vec<ProducerConnectionItem>,
}
