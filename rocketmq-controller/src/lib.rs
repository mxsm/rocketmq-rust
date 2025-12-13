//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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

pub mod config;
pub mod error;
pub mod manager;
pub mod metadata;
pub mod processor;
pub mod raft;
pub mod rpc;
pub mod storage;

pub use config::ControllerConfig;
pub use error::ControllerError;
pub use error::Result;
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
