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

//! Client-backed implementations used by the Admin CLI and TUI.
//!
//! Request parsing and result shaping live in this adapter layer when they
//! still require Client SDK types. Backend-neutral contracts remain in
//! [`crate::core`].

pub use rocketmq_error::RocketMQError;
pub use rocketmq_error::RocketMQResult;
pub use rocketmq_error::ToolsError;

pub mod admin;
pub mod auth;
pub mod broker;
pub mod cache;
pub mod cluster;
pub mod concurrent;
pub mod connection;
pub mod consumer;
pub mod container;
pub mod controller;
pub mod error_view;
pub(crate) mod errors;
pub mod export_data;
pub mod ha;
pub mod lite;
pub mod message;
pub(crate) mod mq_admin_utils;
pub mod namesrv;
pub mod offset;
pub mod producer;
pub mod queue;
pub mod resolver;
pub mod static_topic;
pub mod stats;
pub mod topic;

pub use self::error_view::stable_error_code;
pub use self::error_view::stable_error_message;
pub use self::error_view::AdminErrorView;
