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

use std::str::FromStr;

/// Selects one server without changing its transport or authorization policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerMode {
    /// Queries and diagnostics using the existing query-server configuration.
    Query,
    /// HTTPS control using the existing control-server configuration and durable audit.
    Control,
}

impl FromStr for ServerMode {
    type Err = LaunchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "query" => Ok(Self::Query),
            "control" => Ok(Self::Control),
            _ => Err(LaunchError::InvalidMode),
        }
    }
}

impl ServerMode {
    /// Starts this server through its runtime-owning synchronous entrypoint.
    ///
    /// # Errors
    ///
    /// Returns an error before startup when the selected server is not compiled, or when
    /// the server's configuration, initialization, execution, or graceful shutdown fails.
    pub fn run(self) -> Result<(), LaunchError> {
        match self {
            Self::Query => run_query(),
            Self::Control => run_control(),
        }
    }
}

/// A bounded startup error that never echoes environment or configuration values.
#[derive(Debug, thiserror::Error)]
pub enum LaunchError {
    #[error("ROCKETMQ_MCP_MODE must be query or control")]
    InvalidMode,
    #[error("the selected MCP server is not compiled into this binary")]
    FeatureDisabled,
    #[cfg(feature = "query")]
    #[error("query MCP server failed")]
    Query(#[source] rocketmq_mcp_query::McpError),
    #[cfg(feature = "control")]
    #[error("control MCP server failed")]
    Control(#[source] rocketmq_mcp_control::error::ControlError),
}

/// Starts one server according to `ROCKETMQ_MCP_MODE`, which defaults to `query`.
///
/// Query mode preserves the existing arguments and `ROCKETMQ_MCP_CONFIG`. Control mode reads
/// `ROCKETMQ_MCP_CONTROL_CONFIG` and uses its enforcing HTTPS transport. Call this entrypoint
/// from synchronous code: the selected server owns its runtime and shutdown boundary.
///
/// # Errors
///
/// Returns an error for an invalid or unavailable mode, or a server startup, execution, or
/// shutdown failure. Selecting control never enables mutations by itself.
pub fn run_from_env() -> Result<(), LaunchError> {
    let mode = match std::env::var("ROCKETMQ_MCP_MODE") {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => ServerMode::Query,
        Err(std::env::VarError::NotUnicode(_)) => return Err(LaunchError::InvalidMode),
    };
    mode.run()
}

fn run_query() -> Result<(), LaunchError> {
    #[cfg(feature = "query")]
    {
        rocketmq_mcp_query::entrypoint::run_from_env().map_err(LaunchError::Query)
    }
    #[cfg(not(feature = "query"))]
    {
        Err(LaunchError::FeatureDisabled)
    }
}

fn run_control() -> Result<(), LaunchError> {
    #[cfg(feature = "control")]
    {
        rocketmq_mcp_control::entrypoint::run_from_env().map_err(LaunchError::Control)
    }
    #[cfg(not(feature = "control"))]
    {
        Err(LaunchError::FeatureDisabled)
    }
}

#[cfg(test)]
mod tests {
    use super::{LaunchError, ServerMode};

    #[test]
    fn invalid_mode_does_not_retain_its_value() {
        let error = "private-sentinel-value".parse::<ServerMode>().unwrap_err();
        assert!(matches!(error, LaunchError::InvalidMode));
        assert!(!format!("{error:?}: {error}").contains("private-sentinel-value"));
    }

    #[cfg(not(feature = "query"))]
    #[test]
    fn missing_query_refuses_before_startup() {
        assert!(matches!(ServerMode::Query.run(), Err(LaunchError::FeatureDisabled)));
    }

    #[cfg(not(feature = "control"))]
    #[test]
    fn missing_control_refuses_before_startup() {
        assert!(matches!(ServerMode::Control.run(), Err(LaunchError::FeatureDisabled)));
    }
}
