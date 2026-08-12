// Copyright 2026 The RocketMQ Rust Authors
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

//! Immutable process defaults for business `RemotingCommand` factories.

use std::fmt;
use std::sync::OnceLock;

use crate::protocol::SerializeType;

/// Wire defaults shared by all business command factories in a process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemotingCommandDefaults {
    version: i32,
    serialize_type: SerializeType,
}

impl RemotingCommandDefaults {
    pub const fn new(version: i32, serialize_type: SerializeType) -> Self {
        Self {
            version,
            serialize_type,
        }
    }

    pub const fn version(self) -> i32 {
        self.version
    }

    pub const fn serialize_type(self) -> SerializeType {
        self.serialize_type
    }
}

impl Default for RemotingCommandDefaults {
    fn default() -> Self {
        Self::new(crate::version::CURRENT_VERSION as i32, SerializeType::JSON)
    }
}

/// Returned when a process attempts to replace initialized command defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemotingCommandDefaultsConflict {
    initialized: RemotingCommandDefaults,
    requested: RemotingCommandDefaults,
}

impl RemotingCommandDefaultsConflict {
    pub const fn initialized(self) -> RemotingCommandDefaults {
        self.initialized
    }

    pub const fn requested(self) -> RemotingCommandDefaults {
        self.requested
    }
}

impl fmt::Display for RemotingCommandDefaultsConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "remoting command defaults are already initialized to {:?}, requested {:?}",
            self.initialized, self.requested
        )
    }
}

impl std::error::Error for RemotingCommandDefaultsConflict {}

static APPLICATION_DEFAULTS: OnceLock<RemotingCommandDefaults> = OnceLock::new();

fn initialize_defaults(
    cell: &OnceLock<RemotingCommandDefaults>,
    defaults: RemotingCommandDefaults,
) -> Result<(), RemotingCommandDefaultsConflict> {
    match cell.set(defaults) {
        Ok(()) => Ok(()),
        Err(requested) => {
            // OnceLock returns Err only after retaining the previously initialized value.
            let initialized = *cell
                .get()
                .expect("failed OnceLock::set always retains an initialized value");
            if initialized == requested {
                Ok(())
            } else {
                Err(RemotingCommandDefaultsConflict { initialized, requested })
            }
        }
    }
}

/// Initializes the immutable defaults used by business command factories.
///
/// Repeating initialization with the same value is idempotent. A different
/// value is rejected so wire defaults cannot change after command creation.
///
/// # Errors
///
/// Returns [`RemotingCommandDefaultsConflict`] when different defaults were
/// initialized earlier in the process.
pub fn initialize_remoting_command_defaults(
    defaults: RemotingCommandDefaults,
) -> Result<(), RemotingCommandDefaultsConflict> {
    initialize_defaults(&APPLICATION_DEFAULTS, defaults)
}

pub(crate) fn application_remoting_command_defaults() -> RemotingCommandDefaults {
    *APPLICATION_DEFAULTS.get_or_init(RemotingCommandDefaults::default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialization_is_idempotent_and_rejects_conflicts() {
        let cell = OnceLock::new();
        let first = RemotingCommandDefaults::new(1, SerializeType::JSON);
        let second = RemotingCommandDefaults::new(2, SerializeType::ROCKETMQ);

        assert_eq!(initialize_defaults(&cell, first), Ok(()));
        assert_eq!(initialize_defaults(&cell, first), Ok(()));
        assert_eq!(
            initialize_defaults(&cell, second),
            Err(RemotingCommandDefaultsConflict {
                initialized: first,
                requested: second,
            })
        );
    }
}
