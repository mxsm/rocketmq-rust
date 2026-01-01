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

//! RocketMQ Tools - Admin and CLI utilities
//!
//! This crate provides both:
//! - **Core**: Reusable business logic for RocketMQ operations
//! - **CLI**: Command-line interface with formatting and validation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           CLI Layer (bin/)              │
//! │  - Command parsing (clap)               │
//! │  - Output formatting (formatters/)      │
//! │  - Input validation (validators/)       │
//! └─────────────────┬───────────────────────┘
//!                   │
//!                   ▼
//! ┌─────────────────────────────────────────┐
//! │         Core Logic (core/)              │
//! │  - Topic operations                     │
//! │  - NameServer operations                │
//! │  - Broker operations (future)           │
//! │  - Consumer operations (future)         │
//! └─────────────────┬───────────────────────┘
//!                   │
//!                   ▼
//! ┌─────────────────────────────────────────┐
//! │        Admin API (admin/)               │
//! │  - DefaultMQAdminExt                    │
//! │  - MQAdminExt trait implementations     │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Usage Examples
//!
//! ## As a Library (using core)
//!
//! ```rust,ignore
//! use rocketmq_tools::core::admin::AdminBuilder;
//! use rocketmq_tools::core::topic::TopicService;
//!
//! // With RAII auto-cleanup
//! let mut admin = AdminBuilder::new()
//!     .namesrv_addr("127.0.0.1:9876")
//!     .build_with_guard()
//!     .await?;
//!
//! let clusters = TopicService::get_topic_cluster_list(&mut admin, "MyTopic").await?;
//! // admin automatically cleaned up here
//! ```
//!
//! ## As a CLI Tool
//!
//! ```bash
//! rocketmq-admin-cli-rust topic topicClusterList -t MyTopic -n 127.0.0.1:9876
//! ```

// Core business logic - reusable across different interfaces
pub mod core {
    //! Core business logic module
    //!
    //! This module contains reusable business logic that is independent of
    //! any presentation layer (CLI, API, TUI, etc.).
    //!
    //! # Design Principles
    //!
    //! 1. **Pure Business Logic**: No I/O, no formatting, no CLI concerns
    //! 2. **Testable**: Can be tested independently without CLI
    //! 3. **Reusable**: Can be used by different interfaces
    //! 4. **Type-Safe**: Uses strong types and Result for error handling
    //!
    //! # Available Modules
    //!
    //! - [`admin`] - Admin client builder and RAII management
    //! - [`topic`] - Topic management operations
    //! - [`namesrv`] - NameServer management operations

    pub mod admin;
    pub mod cache;
    pub mod concurrent;
    pub mod namesrv;
    pub mod topic;

    // Re-export error types from rocketmq-error
    pub use rocketmq_error::RocketMQError;
    pub use rocketmq_error::RocketMQResult;
    pub use rocketmq_error::ToolsError;

    // Future modules (uncomment when implemented):
    // pub mod broker;
    // pub mod consumer;
    // pub mod message;
    // pub mod stats;
}

// CLI presentation layer
pub mod cli {
    //! CLI presentation layer
    //!
    //! This module contains everything related to the command-line interface:
    //! - [`commands`] - Command definitions and parsing
    //! - [`formatters`] - Output formatters (JSON, YAML, Table)
    //! - [`validators`] - Input validators
    //!
    //! The CLI layer is a thin wrapper around the core business logic,
    //! handling user interaction concerns separately from business logic.
    //!
    //! # Architecture
    //!
    //! ```text
    //! CLI Layer (this module)
    //!     ↓
    //! Core Business Logic (../core)
    //!     ↓
    //! Admin API (../admin)
    //! ```

    pub mod commands;
    pub mod formatters;
    pub mod validators;
}

// UI utilities for enhanced CLI experience
pub mod ui;

// Admin API layer
pub mod admin;

// Legacy command structure (will be gradually migrated)
pub(crate) mod commands;

// CLI entry point
pub mod rocketmq_cli;
