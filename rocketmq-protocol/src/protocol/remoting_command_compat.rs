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

//! Remoting-owned construction and source-compatibility helpers.
//!
//! `RemotingCommand` itself is always the canonical protocol type. Process
//! environment defaults remain here so the protocol owner does not acquire an
//! environment dependency.

use std::fmt;
use std::sync::LazyLock;
use std::sync::OnceLock;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::remoting_command_defaults::initialize_remoting_command_defaults;
use crate::protocol::remoting_command_defaults::RemotingCommandDefaults;
use crate::protocol::remoting_command_defaults::RemotingCommandFactory;
use crate::protocol::SerializeType;
use crate::ProtocolContractViolation;

use super::remoting_command::REMOTING_VERSION_KEY;
use super::remoting_command::SERIALIZE_TYPE_ENV;
use super::remoting_command::SERIALIZE_TYPE_PROPERTY;

/// Returned when the process attempts to replace an initialized remoting
/// version with a different value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemotingVersionConflict {
    initialized: i32,
    requested: i32,
}

impl RemotingVersionConflict {
    /// Returns the immutable version already selected for the process.
    pub fn initialized(self) -> i32 {
        self.initialized
    }

    /// Returns the conflicting version requested by the caller.
    pub fn requested(self) -> i32 {
        self.requested
    }
}

impl fmt::Display for RemotingVersionConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "remoting version is already initialized to {}, requested {}",
            self.initialized, self.requested
        )
    }
}

impl std::error::Error for RemotingVersionConflict {}

static REMOTING_VERSION: OnceLock<i32> = OnceLock::new();

static SERIALIZE_TYPE: LazyLock<SerializeType> = LazyLock::new(|| {
    std::env::var(SERIALIZE_TYPE_PROPERTY)
        .or_else(|_| std::env::var(SERIALIZE_TYPE_ENV))
        .ok()
        .and_then(|value| match value.as_str() {
            "JSON" => Some(SerializeType::JSON),
            "ROCKETMQ" => Some(SerializeType::ROCKETMQ),
            _ => None,
        })
        .unwrap_or(SerializeType::JSON)
});

/// An unsupported value selected for the process remoting serialization type.
#[derive(Clone, PartialEq, Eq)]
pub struct InvalidRemotingSerializeType {
    key: &'static str,
    value: String,
}

impl InvalidRemotingSerializeType {
    pub const fn key(&self) -> &'static str {
        self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for InvalidRemotingSerializeType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid remoting serialization type for {}: {:?}; expected JSON or ROCKETMQ",
            self.key, self.value
        )
    }
}

impl std::error::Error for InvalidRemotingSerializeType {}

/// Resolves the serialization type with Java-compatible configuration precedence.
///
/// The property-style value takes precedence over the environment fallback.
/// When neither value is configured, JSON is selected.
///
/// # Errors
///
/// Returns [`ProtocolContractViolation::InvalidSerializeType`] when the
/// selected value is not exactly `JSON` or `ROCKETMQ`.
pub fn resolve_remoting_serialize_type(
    property_value: Option<&str>,
    environment_value: Option<&str>,
) -> Result<SerializeType, ProtocolContractViolation> {
    let (key, value) = property_value
        .map(|value| (SERIALIZE_TYPE_PROPERTY, value))
        .or_else(|| environment_value.map(|value| (SERIALIZE_TYPE_ENV, value)))
        .unwrap_or((SERIALIZE_TYPE_PROPERTY, "JSON"));

    match value {
        "JSON" => Ok(SerializeType::JSON),
        "ROCKETMQ" => Ok(SerializeType::ROCKETMQ),
        _ => Err(InvalidRemotingSerializeType {
            key,
            value: value.to_string(),
        }
        .into()),
    }
}

impl fmt::Debug for InvalidRemotingSerializeType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvalidRemotingSerializeType")
            .field("key", &self.key)
            .field("value_present", &true)
            .finish()
    }
}

fn read_environment_value(key: &'static str) -> Result<Option<String>, ProtocolContractViolation> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(value)) => Err(InvalidRemotingSerializeType {
            key,
            value: value.to_string_lossy().into_owned(),
        }
        .into()),
    }
}

/// Initializes the immutable defaults used by all business command factories.
///
/// `rocketmq.serialize.type` takes precedence over
/// `ROCKETMQ_SERIALIZE_TYPE`. Unsupported values fail startup instead of
/// silently selecting JSON.
///
/// # Errors
///
/// Returns [`ProtocolContractViolation`] when configuration is invalid or a
/// different process default was initialized earlier.
pub fn initialize_remoting_command_factory(version: i32) -> Result<RemotingCommandFactory, ProtocolContractViolation> {
    let property_value = read_environment_value(SERIALIZE_TYPE_PROPERTY)?;
    let environment_value = if property_value.is_none() {
        read_environment_value(SERIALIZE_TYPE_ENV)?
    } else {
        None
    };
    let serialize_type = resolve_remoting_serialize_type(property_value.as_deref(), environment_value.as_deref())?;
    let defaults = RemotingCommandDefaults::new(version, serialize_type);
    initialize_remoting_command_defaults(defaults)?;
    Ok(RemotingCommandFactory::new(defaults))
}

/// Initializes the immutable defaults used by all business command factories.
///
/// This compatibility entry point discards the resolved factory. New
/// application owners should retain the value returned by
/// [`initialize_remoting_command_factory`] and inject it into command
/// producers.
///
/// # Errors
///
/// Returns [`ProtocolContractViolation`] when configuration is invalid or a
/// different process default was initialized earlier.
pub fn initialize_remoting_defaults(version: i32) -> Result<(), ProtocolContractViolation> {
    initialize_remoting_command_factory(version)?;
    Ok(())
}

fn resolve_remoting_version(cell: &OnceLock<i32>) -> i32 {
    *cell.get_or_init(|| {
        std::env::var(REMOTING_VERSION_KEY)
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(crate::version::CURRENT_VERSION as i32)
    })
}

fn initialize_version(cell: &OnceLock<i32>, version: i32) -> Result<(), RemotingVersionConflict> {
    match cell.set(version) {
        Ok(()) => Ok(()),
        Err(requested) => {
            let initialized = resolve_remoting_version(cell);
            if initialized == requested {
                Ok(())
            } else {
                Err(RemotingVersionConflict { initialized, requested })
            }
        }
    }
}

/// Selects the immutable remoting version used by compatibility constructors.
///
/// Repeating initialization with the same version is idempotent. A different
/// value is rejected so callers cannot silently change wire defaults after
/// other threads have started constructing commands.
///
/// # Errors
///
/// Returns [`RemotingVersionConflict`] when a different value was initialized
/// earlier.
pub fn initialize_remoting_version(version: i32) -> Result<(), RemotingVersionConflict> {
    initialize_version(&REMOTING_VERSION, version)
}

fn compatibility_factory() -> RemotingCommandFactory {
    RemotingCommandFactory::new(RemotingCommandDefaults::new(
        resolve_remoting_version(&REMOTING_VERSION),
        *SERIALIZE_TYPE,
    ))
}

pub fn create_remoting_command(code: impl Into<i32>) -> RemotingCommand {
    compatibility_factory().create_remoting_command(code)
}

pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> RemotingCommand
where
    T: CommandCustomHeader + Sync + Send + 'static,
{
    compatibility_factory().create_request_command(code, header)
}

pub fn create_response_command() -> RemotingCommand {
    compatibility_factory().create_success_response_command()
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use crate::protocol::header::empty_header::EmptyHeader;

    use super::compatibility_factory;
    use super::create_remoting_command;
    use super::create_request_command;
    use super::create_response_command;
    use super::initialize_version;
    use super::RemotingVersionConflict;

    #[test]
    fn remoting_version_initialization_is_idempotent() {
        let version = OnceLock::new();

        assert_eq!(initialize_version(&version, 321), Ok(()));
        assert_eq!(initialize_version(&version, 321), Ok(()));
        assert_eq!(version.get(), Some(&321));
    }

    #[test]
    fn remoting_version_initialization_rejects_conflicts() {
        let version = OnceLock::new();

        assert_eq!(initialize_version(&version, 321), Ok(()));
        assert_eq!(
            initialize_version(&version, 654),
            Err(RemotingVersionConflict {
                initialized: 321,
                requested: 654,
            })
        );
        assert_eq!(version.get(), Some(&321));
    }

    #[test]
    fn compatibility_constructors_share_factory_defaults() {
        let expected_defaults = compatibility_factory().defaults();
        let commands = [
            create_remoting_command(10),
            create_request_command(11, EmptyHeader {}),
            create_response_command(),
        ];

        for command in commands {
            assert_eq!(command.version(), expected_defaults.version());
            assert_eq!(command.serialize_type(), expected_defaults.serialize_type());
        }
    }
}
