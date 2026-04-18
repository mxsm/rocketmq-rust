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

#![recursion_limit = "256"]

//! Reusable RocketMQ admin capability layer.
//!
//! This crate owns admin domain logic, admin client lifecycle management, and
//! structured service results. Command-line parsing, terminal rendering, and
//! interactive prompts belong in `rocketmq-admin-cli`; future terminal UI state
//! belongs in `rocketmq-admin-tui`.

pub mod core {
    //! Presentation-independent admin services and supporting utilities.

    pub mod admin;
    pub mod auth;
    pub mod broker;
    pub mod cache;
    pub mod cluster;
    pub mod concurrent;
    pub mod connection;
    pub mod consumer;
    pub mod controller;
    pub mod export_data;
    pub mod ha;
    pub mod lite;
    pub mod message;
    pub mod namesrv;
    pub mod offset;
    pub mod producer;
    pub mod queue;
    pub mod resolver;
    pub mod static_topic;
    pub mod stats;
    pub mod topic;

    pub use rocketmq_error::RocketMQError;
    pub use rocketmq_error::RocketMQResult;
    pub use rocketmq_error::ToolsError;
}

pub mod admin;
