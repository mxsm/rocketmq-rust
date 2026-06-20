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
//! │ OpenRaft    │  │  Processor │
//! │  Module     │  │   Layer    │
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

#[doc(hidden)]
pub mod bench_support {
    use std::time::Duration;
    use std::time::Instant;

    use rocketmq_runtime::ShutdownReport;
    use rocketmq_rust::ArcMut;
    use serde::Serialize;

    use crate::config::ControllerConfig;
    use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
    use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;

    #[derive(Clone, Debug, Serialize)]
    pub struct ControllerHeartbeatLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    pub async fn run_controller_heartbeat_lifecycle_probe() -> ControllerHeartbeatLifecycleProbe {
        let mut manager =
            DefaultBrokerHeartbeatManager::new(ArcMut::new(ControllerConfig::test_config())).with_scan_interval_ms(1);

        manager.start();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let task_count_before_shutdown = manager.scan_task_count();

        let shutdown_started_at = Instant::now();
        let shutdown_report = manager.shutdown_gracefully_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = manager.scan_task_count();
        let finished_tasks = shutdown_report.completed + shutdown_report.cancelled;
        let healthy = shutdown_report.is_healthy()
            && task_count_before_shutdown == 1
            && task_count_after_shutdown == 0
            && finished_tasks == 1;

        ControllerHeartbeatLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }
}

#[cfg(test)]
mod bench_support_tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_heartbeat_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_controller_heartbeat_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert!(
            probe.shutdown_report.is_healthy(),
            "{}",
            probe.shutdown_report.to_json()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
