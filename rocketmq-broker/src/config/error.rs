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

use std::fmt;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigSection {
    Identity,
    Network,
    HighAvailability,
    Storage,
    Security,
    Resources,
}

impl fmt::Display for ConfigSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Identity => "identity",
            Self::Network => "network",
            Self::HighAvailability => "high-availability",
            Self::Storage => "storage",
            Self::Security => "security",
            Self::Resources => "resources",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Error)]
pub enum BrokerConfigError {
    #[error("failed to load broker configuration from {path}: {source}")]
    Load {
        path: PathBuf,
        #[source]
        source: config::ConfigError,
    },

    #[error("invalid {section} configuration `{field}`: {message}")]
    Invalid {
        section: ConfigSection,
        field: &'static str,
        message: String,
    },

    #[error("broker configuration fields require restart: {fields}")]
    RestartRequired { fields: String },

    #[error("unsupported broker configuration keys: {keys}")]
    UnsupportedKeys { keys: String },

    #[error("broker configuration `{key}` expects {expected}, got `{value}`")]
    InvalidProperty {
        key: String,
        value: String,
        expected: &'static str,
    },

    #[error("configuration generation conflict: expected {expected}, actual {actual}")]
    GenerationConflict { expected: u64, actual: u64 },

    #[error("configuration generation counter is exhausted")]
    GenerationExhausted,
}

impl BrokerConfigError {
    pub(crate) fn invalid(section: ConfigSection, field: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            section,
            field,
            message: message.into(),
        }
    }

    pub(crate) fn restart_required(mut fields: Vec<String>) -> Self {
        fields.sort();
        fields.dedup();
        Self::RestartRequired {
            fields: fields.join(","),
        }
    }

    pub(crate) fn unsupported_keys(mut keys: Vec<String>) -> Self {
        keys.sort();
        keys.dedup();
        Self::UnsupportedKeys { keys: keys.join(",") }
    }
}
