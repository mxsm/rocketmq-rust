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

//! Immutable defaults and factories for business `RemotingCommand` values.

use std::fmt;
use std::sync::OnceLock;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;

use crate::code::response_code::RemotingSysResponseCode;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::SerializeType;

const JAVA_DEFAULT_RESPONSE_REMARK: &str = "not set any response code";

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

/// Builds business commands from one immutable set of wire defaults.
///
/// A factory does not read process configuration. Application owners may keep
/// the process-default compatibility path or inject explicit factories when
/// multiple remoting configurations must coexist in one process.
///
/// # Examples
///
/// ```
/// use rocketmq_protocol::protocol::remoting_command_defaults::{
///     RemotingCommandDefaults, RemotingCommandFactory,
/// };
/// use rocketmq_protocol::protocol::SerializeType;
///
/// let json = RemotingCommandFactory::new(RemotingCommandDefaults::new(101, SerializeType::JSON));
/// let binary = RemotingCommandFactory::new(RemotingCommandDefaults::new(202, SerializeType::ROCKETMQ));
///
/// assert_eq!(json.create_remoting_command(10).version(), 101);
/// assert_eq!(binary.create_remoting_command(10).version(), 202);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemotingCommandFactory {
    defaults: RemotingCommandDefaults,
}

impl RemotingCommandFactory {
    /// Creates a factory with explicit version and serialization defaults.
    pub const fn new(defaults: RemotingCommandDefaults) -> Self {
        Self { defaults }
    }

    /// Returns the immutable defaults owned by this factory.
    pub const fn defaults(&self) -> RemotingCommandDefaults {
        self.defaults
    }

    /// Creates a base command with an explicit code.
    pub fn create_remoting_command(&self, code: impl Into<i32>) -> RemotingCommand {
        RemotingCommand::with_resolved_defaults(self.defaults.version(), self.defaults.serialize_type()).set_code(code)
    }

    /// Creates a request with an external body.
    pub fn create_request(&self, code: impl Into<i32>, body: impl Into<Bytes>) -> RemotingCommand {
        self.create_remoting_command(code).set_body(body)
    }

    /// Creates a request with a typed custom header.
    pub fn create_request_command<T>(&self, code: impl Into<i32>, header: T) -> RemotingCommand
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.create_remoting_command(code).set_command_custom_header(header)
    }

    /// Creates a response with an explicit response code.
    pub fn create_response_command_with_code(&self, code: impl Into<i32>) -> RemotingCommand {
        self.create_remoting_command(code).mark_response_type()
    }

    /// Creates a response with an explicit code and typed custom header.
    pub fn create_response_command_with_code_and_header(
        &self,
        code: impl Into<i32>,
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> RemotingCommand {
        self.create_response_command_with_code(code)
            .set_command_custom_header(header)
    }

    /// Creates a response with an explicit code and remark.
    pub fn create_response_command_with_code_remark(
        &self,
        code: impl Into<i32>,
        remark: impl Into<CheetahString>,
    ) -> RemotingCommand {
        self.create_remoting_command(code)
            .set_remark_option(Some(remark.into()))
            .mark_response_type()
    }

    /// Creates a response from the central typed-error remoting mapping.
    pub fn create_response_command_from_error(&self, error: &RocketMQError) -> RemotingCommand {
        let view = error.boundary_view();
        self.create_response_command_with_code_remark(view.remoting().code.as_i32(), view.message())
    }

    /// Creates a mapped typed-error response with an explicit wire remark.
    pub fn create_response_command_from_error_with_remark(
        &self,
        error: &RocketMQError,
        remark: impl Into<CheetahString>,
    ) -> RemotingCommand {
        let view = error.boundary_view();
        self.create_response_command_with_code_remark(view.remoting().code.as_i32(), remark)
    }

    /// Creates an explicitly successful response.
    pub fn create_success_response_command(&self) -> RemotingCommand {
        self.create_response_command_with_code(RemotingSysResponseCode::Success)
    }

    /// Creates an explicitly successful response with a typed custom header.
    pub fn create_success_response_command_with_header(
        &self,
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> RemotingCommand {
        self.create_response_command_with_code_and_header(RemotingSysResponseCode::Success, header)
    }

    /// Creates Java's unset error response.
    pub fn create_java_default_error_response_command(&self) -> RemotingCommand {
        self.create_response_command_with_code_remark(
            RemotingSysResponseCode::SystemError,
            JAVA_DEFAULT_RESPONSE_REMARK,
        )
    }

    /// Creates Java's unset error response with a typed custom header.
    pub fn create_java_default_error_response_command_with_header(
        &self,
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> RemotingCommand {
        self.create_java_default_error_response_command()
            .set_command_custom_header(header)
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

/// Returns the immutable compatibility factory selected by the application.
///
/// Instance owners should capture this value once at their composition
/// boundary and pass it to request/response producers. The returned factory
/// does not read environment variables.
pub fn application_remoting_command_factory() -> RemotingCommandFactory {
    RemotingCommandFactory::new(application_remoting_command_defaults())
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

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

    #[test]
    fn typed_error_response_keeps_factory_defaults() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(654, SerializeType::ROCKETMQ));
        let error = RocketMQError::illegal_argument("invalid nameserver request");

        let response = factory.create_response_command_from_error_with_remark(&error, "invalid route request");

        assert_eq!(response.version(), 654);
        assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(response.code(), error.boundary_view().remoting().code.as_i32());
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("invalid route request")
        );
        assert!(response.is_response_type());
    }
}
