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

//! PostgreSQL-backed, read-only control plane for RocketMQ AI SRE.
//!
//! The control plane owns cluster onboarding, diagnosis workflows, immutable
//! plans, deterministic policy decisions, human approval, and durable
//! execution submission. Target-side mutation remains isolated behind the
//! Executor and Execution Agent boundary; this crate exposes no arbitrary
//! RocketMQ Admin, Kubernetes, or configuration mutation surface.

mod alerting;
mod api;
mod assets;
mod auth;
mod config;
mod connector_channel;
mod coverage;
mod error;
mod evidence;
mod execution_authority;
mod forecast;
mod inspection;
mod knowledge;
mod model;
mod models;
pub mod observability;
mod openapi;
mod operator_workbench;
mod orchestrator;
mod phase1_api;
mod phase2_repository;
mod postmortem;
mod read_audit;
mod repository;
mod slo;
mod supervised_execution;
mod supervised_repository;
mod workflow;

pub use api::CapabilityDocuments;
pub use api::build_router;
pub use api::run;
pub use config::ControlPlaneConfig;
pub use error::ControlPlaneError;
pub use model::CapabilitySnapshot;
pub use model::Cluster;
pub use model::ClusterSummary;
pub use model::DataSourceAvailability;
pub use model::DataSourceStatus;
pub use model::HandshakeRequest;
pub use model::OffboardRequest;
pub use model::OnboardClusterRequest;
pub use model::OnboardingState;
pub use phase2_repository::Phase2Repository;
pub use repository::PostgresRepository;
pub use supervised_repository::StoredActionPlan;
pub use supervised_repository::SupervisedRepository;

pub const DEFAULT_CONTROL_PLANE_PORT: u16 = 8090;
pub const DEFAULT_CONNECTOR_CHANNEL_PORT: u16 = 8093;
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
pub const MCP_BUSINESS_SCHEMA: &str = "rocketmq-mcp.v2";
