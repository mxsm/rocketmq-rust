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

//! Typed failures for the Broker configuration pipeline.
//!
//! Every fallible step that loads, validates, projects, or version-controls
//! the Broker configuration reports one of the variants in
//! `BrokerConfigError`. Each error carries the `ConfigSection` it belongs
//! to (where applicable) so callers can attribute a failure to a specific
//! configuration group and render a section-qualified diagnostic. The rest of
//! the crate matches on these variants, so they are part of the crate's
//! documented surface.

use std::fmt;
use std::path::PathBuf;

use rocketmq_runtime::common::parse_config_file::render_safe_config_error;
use rocketmq_runtime::RuntimeContractViolation;
use rocketmq_runtime::RuntimeError;

/// The configuration group a failure belongs to.
///
/// A Broker configuration is partitioned into ordered sections; this enum
/// names them so a rejected change can be routed to the matching in-memory
/// section and so errors can be rendered with a section-qualified message
/// (see the [`fmt::Display`] impl).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigSection {
    /// Broker identity (name, id, cluster membership).
    Identity,
    /// Networking and listener configuration.
    Network,
    /// High-availability / replication configuration.
    HighAvailability,
    /// Message store and on-disk layout configuration.
    Storage,
    /// Security, authentication, and ACL configuration.
    Security,
    /// Resource limits and capacity tuning.
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

/// An error produced while loading, validating, or applying Broker
/// configuration.
///
/// The variants distinguish the stage that failed. Only [`Load`](Self::Load),
/// [`Runtime`](Self::Runtime), and [`Contract`](Self::Contract) wrap an
/// underlying source error (exposed through [`std::error::Error::source`]);
/// the others are self-describing.
pub enum BrokerConfigError {
    /// The raw configuration at `path` could not be read or parsed.
    ///
    /// Wraps the underlying [`config::ConfigError`] as its source. Recoverable:
    /// correct the file and reload.
    Load {
        path: PathBuf,
        source: Box<config::ConfigError>,
    },

    /// A configuration `field` failed validation, with a human-readable
    /// `message` explaining why.
    ///
    /// Recoverable: supply a valid value for the field and re-validate.
    Invalid {
        section: ConfigSection,
        field: &'static str,
        message: String,
    },

    /// The runtime could not resolve or apply a configuration `field`.
    ///
    /// Wraps the originating [`RuntimeError`] as its source.
    Runtime {
        section: ConfigSection,
        field: &'static str,
        source: RuntimeError,
    },

    /// A configuration `field` violated a runtime contract.
    ///
    /// Wraps the originating [`RuntimeContractViolation`] as its source.
    Contract {
        section: ConfigSection,
        field: &'static str,
        source: RuntimeContractViolation,
    },

    /// One or more changed `fields` can only take effect after a Broker
    /// restart and were therefore not applied live.
    RestartRequired { fields: String },

    /// The configuration contained `keys` the Broker does not recognize.
    UnsupportedKeys { keys: String },

    /// A property `key` was present but its `value` did not match the
    /// `expected` type or shape.
    ///
    /// Recoverable: correct the property value.
    InvalidProperty {
        key: String,
        value: String,
        expected: &'static str,
    },

    /// An optimistic configuration-generation check failed because another
    /// update advanced the generation concurrently.
    ///
    /// Recoverable: re-read the current generation and retry the update.
    GenerationConflict { expected: u64, actual: u64 },

    /// The configuration generation counter can no longer be advanced.
    GenerationExhausted,

    /// The runtime configuration projection for `component` is not available.
    RuntimeProjectionUnavailable { component: &'static str },

    /// Coordination of a runtime configuration change failed; `detail`
    /// describes the failure.
    RuntimeCoordination { detail: String },
}

impl fmt::Display for BrokerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load { source, .. } => write!(
                formatter,
                "failed to load broker configuration: {}",
                render_safe_config_error(source)
            ),
            Self::Invalid {
                section,
                field,
                message,
            } => write!(formatter, "invalid {section} configuration `{field}`: {message}"),
            Self::Runtime { section, field, .. } => {
                write!(formatter, "runtime could not resolve {section} configuration `{field}`")
            }
            Self::Contract { section, field, .. } => {
                write!(formatter, "invalid {section} configuration `{field}`")
            }
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

impl std::error::Error for BrokerConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Load { source, .. } => Some(source.as_ref()),
            Self::Runtime { source, .. } => Some(source),
            Self::Contract { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl BrokerConfigError {
    /// Builds an [`Invalid`](Self::Invalid) error for `field` in `section`,
    /// capturing a human-readable `message`.
    pub(crate) fn invalid(section: ConfigSection, field: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            section,
            field,
            message: message.into(),
        }
    }

    /// Builds a [`Runtime`](Self::Runtime) error wrapping the `source`
    /// [`RuntimeError`] raised while resolving `field` in `section`.
    pub(crate) fn runtime(section: ConfigSection, field: &'static str, source: RuntimeError) -> Self {
        Self::Runtime { section, field, source }
    }

    /// Builds a [`Contract`](Self::Contract) error wrapping the `source`
    /// [`RuntimeContractViolation`] for `field` in `section`.
    pub(crate) fn contract(section: ConfigSection, field: &'static str, source: RuntimeContractViolation) -> Self {
        Self::Contract { section, field, source }
    }

    /// Builds a [`RestartRequired`](Self::RestartRequired) error from the list
    /// of changed `fields`. The list is sorted and de-duplicated so the
    /// rendered message is stable regardless of input order.
    pub(crate) fn restart_required(mut fields: Vec<String>) -> Self {
        fields.sort();
        fields.dedup();
        Self::RestartRequired {
            fields: fields.join(","),
        }
    }

    /// Builds an [`UnsupportedKeys`](Self::UnsupportedKeys) error from the
    /// unrecognized `keys`. The list is sorted and de-duplicated for a stable
    /// message.
    pub(crate) fn unsupported_keys(mut keys: Vec<String>) -> Self {
        keys.sort();
        keys.dedup();
        Self::UnsupportedKeys { keys: keys.join(",") }
    }
}

#[cfg(test)]
mod tests {
    use super::BrokerConfigError;

    #[test]
    fn broker_config_error_stays_below_the_large_result_threshold() {
        assert!(std::mem::size_of::<BrokerConfigError>() <= 96);
    }
}
