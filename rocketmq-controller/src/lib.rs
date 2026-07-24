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
//! Broker request
//!      |
//!      v
//! ControllerRequestProcessor -> RaftController -> OpenRaft client_write
//!                                                |
//!                                                v
//!                                  committed state machine
//!                                                |
//!                                                v
//!                                    RocksDB durable state
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use rocketmq_controller::ControllerConfig;
//! use rocketmq_controller::ControllerManager;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = ControllerConfig::default();
//!     let controller = Arc::new(ControllerManager::new(config).await?);
//!
//!     controller.initialize().await?;
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
pub mod metrics;
pub mod openraft;
pub mod processor;
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
pub use config::ControllerConfigReader;
pub use controller::open_raft_controller::resolve_controller_raft_bind_addr;
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
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use rocketmq_common::common::controller::controller_config::RaftPeer;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;

    use crate::config::ControllerConfig;
    use crate::config::ControllerConfigReader;
    use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
    use crate::controller::open_raft_controller::OpenRaftController;
    use crate::controller::Controller;
    use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
    use crate::typ::Node;
    use crate::ControllerManager;

    #[derive(Clone, Debug, Serialize)]
    pub struct ControllerHeartbeatLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct ControllerLeadershipWatchLifecycleProbe {
        pub scheduling_enabled: bool,
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub healthy: bool,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct ControllerOpenRaftScanLifecycleProbe {
        pub became_leader: bool,
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub healthy: bool,
    }

    pub async fn run_controller_heartbeat_lifecycle_probe() -> ControllerHeartbeatLifecycleProbe {
        let mut manager =
            DefaultBrokerHeartbeatManager::new(ControllerConfigReader::new(ControllerConfig::test_config()))
                .with_scan_interval_ms(1);

        manager.start();
        let mut snapshots = manager.scan_schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = manager.scan_schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = manager.scan_task_count();

        let shutdown_started_at = Instant::now();
        let shutdown_report = manager.shutdown_gracefully_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = manager.scan_task_count();
        let finished_tasks = shutdown_report.completed + shutdown_report.cancelled;
        let healthy = shutdown_report.is_healthy()
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown == 1
            && task_count_after_shutdown == 0
            && finished_tasks <= 1;

        ControllerHeartbeatLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_controller_leadership_watch_lifecycle_probe() -> ControllerLeadershipWatchLifecycleProbe {
        let remoting_addr = free_loopback_addr();
        let raft_addr = free_loopback_addr();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![RaftPeer { id: 1, addr: raft_addr }])
            .with_controller_peers(vec![RaftPeer {
                id: 1,
                addr: remoting_addr,
            }])
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300)
            .with_storage_backend(crate::config::StorageBackendType::Memory);

        let manager = Arc::new(
            ControllerManager::new(config)
                .await
                .expect("benchmark controller manager should create"),
        );
        manager
            .initialize()
            .await
            .expect("benchmark controller manager should initialize");
        manager
            .start()
            .await
            .expect("benchmark controller manager should start");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1,
            Node {
                node_id: 1,
                rpc_addr: raft_addr.to_string(),
            },
        );
        manager
            .controller()
            .initialize_cluster(nodes)
            .await
            .expect("benchmark controller cluster should initialize");

        let scheduling_enabled = wait_until(Duration::from_secs(5), || {
            manager.is_leader() && manager.scheduling_enabled()
        })
        .await;

        let mut snapshots = manager.leadership_watch_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            snapshots = manager.leadership_watch_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = manager.leadership_watch_task_count();
        let shutdown_started_at = Instant::now();
        manager
            .shutdown()
            .await
            .expect("benchmark controller manager should shutdown");
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = manager.leadership_watch_task_count();

        let healthy = scheduling_enabled
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0;

        ControllerLeadershipWatchLifecycleProbe {
            scheduling_enabled,
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            healthy,
        }
    }

    pub async fn run_controller_openraft_scan_lifecycle_probe() -> ControllerOpenRaftScanLifecycleProbe {
        let remoting_addr = free_loopback_addr();
        let raft_addr = free_loopback_addr();
        let config = ControllerConfig::default()
            .with_node_info(1, remoting_addr)
            .with_raft_peers(vec![RaftPeer { id: 1, addr: raft_addr }])
            .with_scan_not_active_broker_interval(1)
            .with_raft_scan_wait_timeout_ms(0)
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300)
            .with_storage_backend(crate::config::StorageBackendType::Memory);
        let mut controller = OpenRaftController::new(ControllerConfigReader::new(config));
        controller
            .startup()
            .await
            .expect("benchmark OpenRaft controller should start");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1,
            Node {
                node_id: 1,
                rpc_addr: raft_addr.to_string(),
            },
        );
        controller
            .initialize_cluster(nodes)
            .await
            .expect("benchmark OpenRaft cluster should initialize");
        let became_leader = wait_until(Duration::from_secs(5), || controller.is_leader()).await;
        controller
            .start_scheduling()
            .await
            .expect("benchmark OpenRaft scheduling should start");

        let mut snapshots = controller.scan_schedule_snapshot();
        for _ in 0..300 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            snapshots = controller.scan_schedule_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = controller.scan_task_count();
        let shutdown_started_at = Instant::now();
        controller
            .shutdown()
            .await
            .expect("benchmark OpenRaft controller should shutdown");
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = controller.scan_task_count();

        let healthy = became_leader
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0;

        ControllerOpenRaftScanLifecycleProbe {
            became_leader,
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            healthy,
        }
    }

    fn free_loopback_addr() -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("benchmark loopback port should bind");
        let addr = listener
            .local_addr()
            .expect("benchmark loopback port should have local addr");
        drop(listener);
        addr
    }

    async fn wait_until<F>(timeout: Duration, mut predicate: F) -> bool
    where
        F: FnMut() -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if predicate() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        predicate()
    }
}

#[cfg(test)]
mod bench_support_tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_heartbeat_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_controller_heartbeat_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
        assert!(
            probe.shutdown_report.is_healthy(),
            "{}",
            probe.shutdown_report.to_json()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_leadership_watch_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_controller_leadership_watch_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.scheduling_enabled, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_openraft_scan_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_controller_openraft_scan_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.became_leader, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
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
