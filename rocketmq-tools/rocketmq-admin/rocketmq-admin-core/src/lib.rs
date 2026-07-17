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

#[cfg(feature = "legacy-common-compat")]
#[doc(hidden)]
pub extern crate self as rocketmq_error;
#[cfg(feature = "legacy-common-compat")]
#[doc(hidden)]
pub extern crate self as rocketmq_remoting;
#[cfg(feature = "legacy-common-compat")]
#[doc(hidden)]
pub extern crate self as rocketmq_rust;

#[cfg(feature = "legacy-common-compat")]
#[doc(hidden)]
pub use crate::client_adapter::legacy::error_compat::*;
#[cfg(feature = "legacy-common-compat")]
#[doc(hidden)]
pub use crate::client_adapter::legacy::remoting_compat::*;

pub mod core;

#[cfg(feature = "client-adapter")]
pub mod client_adapter;

#[cfg(feature = "legacy-common-compat")]
pub mod admin;
