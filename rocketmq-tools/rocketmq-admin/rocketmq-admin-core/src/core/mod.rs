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

//! Presentation-independent admin contracts and deterministic planning logic.
//!
//! This module is always available. It deliberately contains no RocketMQ
//! Client, Common, or Remoting imports, so `--no-default-features` builds only the
//! admin-owned contract surface.

use std::future::Future;
use std::pin::Pin;

pub mod admin;
pub mod broker;
pub mod client_connection;
pub mod clock;
pub mod consumer;
pub mod consumer_workspace;
pub mod dashboard;
pub mod error;
pub mod error_view;
pub mod lite;
pub mod message;
pub mod proxy;
pub mod query;
pub mod queue;
pub mod release_checkpoint;
pub mod security;
pub mod static_topic;
pub mod topic;

pub use self::error::AdminError;
pub use self::error::AdminResult;
pub use self::error_view::stable_error_code;
pub use self::error_view::stable_error_message;
pub use self::error_view::AdminErrorView;

pub type AdminFuture<'a, T> = Pin<Box<dyn Future<Output = AdminResult<T>> + Send + 'a>>;
