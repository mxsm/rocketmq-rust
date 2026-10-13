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

//! Feature-composed RocketMQ MCP entrypoint.
//!
//! The default build serves queries and diagnostics. `control` includes the separately
//! authenticated control server, and `write-tools` compiles its reviewed mutation tools.
//! A process starts one server selected by [`ServerMode`]; each retains its own security policy.

mod launch;
pub use launch::{run_from_env, LaunchError, ServerMode};

#[cfg(feature = "query")]
pub use rocketmq_mcp_query as query;
// Preserve the original query embedding API at the entrypoint crate root.
#[cfg(feature = "query")]
pub use rocketmq_mcp_query::{
    adapter, app, config, guard, model, prompts, protocol, resources, service, tools, transport, McpError, McpResult,
};

#[cfg(feature = "auth")]
pub use rocketmq_mcp_auth as auth;

#[cfg(feature = "control")]
pub use rocketmq_mcp_control as control;
