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
use crate::protocol::SerializeType;

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

pub fn create_remoting_command(code: impl Into<i32>) -> RemotingCommand {
    RemotingCommand::with_resolved_defaults(resolve_remoting_version(&REMOTING_VERSION), *SERIALIZE_TYPE).set_code(code)
}

pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> RemotingCommand
where
    T: CommandCustomHeader + Sync + Send + 'static,
{
    RemotingCommand::create_request_command_with_defaults(
        code,
        header,
        resolve_remoting_version(&REMOTING_VERSION),
        *SERIALIZE_TYPE,
    )
}

pub fn create_response_command() -> RemotingCommand {
    create_remoting_command(crate::code::response_code::RemotingSysResponseCode::Success).mark_response_type()
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

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
}
