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

#![feature(arbitrary_self_types)]

//! # RocketMQ Controller Module
//!
//! High-availability controller implementation for RocketMQ, providing:
//! - Raft-based consensus for leader election and metadata replication
//! - Broker registration and heartbeat management
//! - Topic metadata management and synchronization
//! - Configuration management across the cluster
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │      Controller Manager (Entry)         │
//! └──────────────┬──────────────────────────┘
//!                │
//!        ┌───────┴────────┐
//!        │                │
//! ┌──────▼──────┐  ┌─────▼──────┐
//! │ Raft Module │  │  Processor │
//! │  (raft-rs)  │  │   Layer    │
//! └──────┬──────┘  └─────┬──────┘
//!        │                │
//!        └────────┬───────┘
//!                 │
//!        ┌────────▼─────────┐
//!        │  Metadata Store  │
//!        │  (DashMap/Raft)  │
//!        └──────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rocketmq_controller::ControllerConfig;
//! use rocketmq_controller::ControllerManager;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = ControllerConfig::default();
//!     let mut controller = ControllerManager::new(config)?;
//!
//!     controller.start().await?;
//!
//!     // Controller is now running...
//!
//!     controller.shutdown().await?;
//!     Ok(())
//! }
//! ```

#![warn(rust_2018_idioms)]
#![warn(clippy::all)]
#![allow(dead_code)]
#![allow(clippy::module_inception)]

pub mod cli;
pub mod config;
pub mod controller;
pub(crate) mod elect;
pub mod error;
pub mod event;
pub mod heartbeat;
pub mod helper;
pub mod manager;
pub mod metadata;
pub mod metrics;
pub mod openraft;
pub mod processor;
pub mod raft;
pub mod rpc;
pub mod storage;
pub mod task;
pub mod typ;
pub mod protobuf {
    tonic::include_proto!("rocketmq_rust_controller");

    // OpenRaft protobuf definitions
    pub mod openraft {
        tonic::include_proto!("rocketmq_rust_controller.openraft");
    }
}

pub use cli::parse_command_line;
pub use cli::ControllerCli;
pub use config::ControllerConfig;
pub use controller::open_raft_controller::OpenRaftController;
pub use controller::raft_controller::RaftController;
pub use controller::raft_rs_controller::RaftRsController;
pub use controller::Controller;
pub use controller::MockController;
pub use elect::policy::DefaultElectPolicy;
pub use error::ControllerError;
pub use error::Result;
pub use manager::replicas_info_manager::ReplicasInfoManager;
pub use manager::ControllerManager;

/// Controller module version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default controller listen port
pub const DEFAULT_CONTROLLER_PORT: u16 = 9878;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
