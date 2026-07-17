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
//! client or legacy-common imports, so `--no-default-features` builds only the
//! admin-owned contract surface.

use std::future::Future;
use std::pin::Pin;

pub mod admin;
pub mod broker;
pub mod clock;
pub mod consumer;
pub mod error;
pub mod lite;
pub mod security;
pub mod static_topic;
pub mod topic;

pub use self::error::AdminError;
pub use self::error::AdminResult;

pub type AdminFuture<'a, T> = Pin<Box<dyn Future<Output = AdminResult<T>> + Send + 'a>>;

#[cfg(feature = "legacy-common-compat")]
pub use rocketmq_error::RocketMQError;
#[cfg(feature = "legacy-common-compat")]
pub use rocketmq_error::RocketMQResult;
#[cfg(feature = "legacy-common-compat")]
pub use rocketmq_error::ToolsError;

#[cfg(feature = "legacy-common-compat")]
pub mod auth {
    pub use crate::client_adapter::legacy::core::auth::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod cache {
    pub use crate::client_adapter::legacy::core::cache::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod cluster {
    pub use crate::client_adapter::legacy::core::cluster::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod concurrent {
    pub use crate::client_adapter::legacy::core::concurrent::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod connection {
    pub use crate::client_adapter::legacy::core::connection::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod container {
    pub use crate::client_adapter::legacy::core::container::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod controller {
    pub use crate::client_adapter::legacy::core::controller::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod error_view;

#[cfg(feature = "legacy-common-compat")]
pub(crate) mod errors {
    pub(crate) use crate::client_adapter::legacy::core::errors::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod export_data {
    pub use crate::client_adapter::legacy::core::export_data::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod ha {
    pub use crate::client_adapter::legacy::core::ha::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod message {
    pub use crate::client_adapter::legacy::core::message::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod namesrv {
    pub use crate::client_adapter::legacy::core::namesrv::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod offset {
    pub use crate::client_adapter::legacy::core::offset::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod producer {
    pub use crate::client_adapter::legacy::core::producer::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod queue {
    pub use crate::client_adapter::legacy::core::queue::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod resolver {
    pub use crate::client_adapter::legacy::core::resolver::*;
}

#[cfg(feature = "legacy-common-compat")]
pub mod stats {
    pub use crate::client_adapter::legacy::core::stats::*;
}

#[cfg(feature = "legacy-common-compat")]
pub use self::error_view::stable_error_code;
#[cfg(feature = "legacy-common-compat")]
pub use self::error_view::stable_error_message;
#[cfg(feature = "legacy-common-compat")]
pub use self::error_view::AdminErrorView;
