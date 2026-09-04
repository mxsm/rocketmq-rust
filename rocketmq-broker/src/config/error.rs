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

use rocketmq_runtime::common::parse_config_file::RedactedConfigError;

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

pub enum BrokerConfigError {
    Load {
        path: PathBuf,
        source: RedactedConfigError,
    },

    Invalid {
        section: ConfigSection,
        field: &'static str,
        message: String,
    },

    RestartRequired {
        fields: String,
    },

    UnsupportedKeys {
        keys: String,
    },

    InvalidProperty {
        key: String,
        value: String,
        expected: &'static str,
    },

    GenerationConflict {
        expected: u64,
        actual: u64,
    },

    GenerationExhausted,

    RuntimeProjectionUnavailable {
        component: &'static str,
    },

    RuntimeCoordination {
        detail: String,
    },
}

impl fmt::Display for BrokerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load { source, .. } => write!(formatter, "failed to load broker configuration: {source}"),
            Self::Invalid {
                section,
                field,
                message,
            } => write!(formatter, "invalid {section} configuration `{field}`: {message}"),
            Self::RestartRequired { fields } => {
                write!(formatter, "broker configuration fields require restart: {fields}")
            }
            Self::UnsupportedKeys { keys } => write!(formatter, "unsupported broker configuration keys: {keys}"),
            Self::InvalidProperty { key, value, expected } => write!(
                formatter,
                "broker configuration `{key}` expects {expected}, got `{value}`"
            ),
            Self::GenerationConflict { expected, actual } => {
                write!(
                    formatter,
                    "configuration generation conflict: expected {expected}, actual {actual}"
                )
            }
            Self::GenerationExhausted => formatter.write_str("configuration generation counter is exhausted"),
            Self::RuntimeProjectionUnavailable { component } => {
                write!(
                    formatter,
                    "runtime configuration projection is unavailable: {component}"
                )
            }
            Self::RuntimeCoordination { detail } => {
                write!(formatter, "runtime configuration coordination failed: {detail}")
            }
        }
    }
}

impl fmt::Debug for BrokerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for BrokerConfigError {}

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
