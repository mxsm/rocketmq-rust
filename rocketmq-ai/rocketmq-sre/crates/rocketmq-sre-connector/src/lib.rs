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

//! Fail-closed Streamable HTTP connector between RocketMQ MCP and canonical
//! RocketMQ AI SRE evidence.
//!
//! The connector imports only the generic `rmcp` client and public wire
//! contracts. It never links the RocketMQ MCP server crate or its Rust DTOs.

#![recursion_limit = "256"]

mod api;
mod auth;
mod capability;
mod channel;
mod config;
mod engine;
mod error;
mod mcp;
mod read_gateway;
mod sources;
mod wire;

pub use api::run;
pub use capability::CapabilityManifest;
pub use capability::CapabilityTool;
pub use capability::MCP_BUSINESS_SCHEMA;
pub use capability::MCP_PROTOCOL_VERSION;
pub use capability::VerifiedCapability;
pub use capability::verify_manifest;
pub use config::ConnectorConfig;
pub use config::ControlPlaneConfig;
pub use engine::ConnectorCapabilitiesView;
pub use engine::EvidenceQueryRequest;
pub use error::ConnectorError;
pub use error::ConnectorErrorCode;
pub use wire::EvidenceOperation;
pub use wire::WireEvidenceEnvelope;

use engine::ConnectorEngine;
use mcp::McpGateway;
use mcp::RmcpGateway;

pub const DEFAULT_CONNECTOR_PORT: u16 = 8091;
