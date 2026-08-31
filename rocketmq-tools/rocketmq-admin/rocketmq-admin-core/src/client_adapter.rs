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

//! RocketMQ client-backed implementations of the admin-owned contracts.

#[cfg(feature = "client-adapter")]
mod broker;
#[cfg(feature = "client-adapter")]
mod connection;
#[cfg(feature = "client-adapter")]
mod consumer;
#[cfg(feature = "client-adapter")]
mod dashboard;
pub(crate) mod lifecycle;
#[cfg(feature = "client-adapter")]
mod lite;
#[cfg(feature = "client-adapter")]
mod message;
#[cfg(feature = "mutation-client-adapter")]
#[path = "client_adapter/topic/producer.rs"]
pub(crate) mod producer;
#[cfg(feature = "client-adapter")]
mod security;
#[cfg(feature = "client-adapter")]
pub mod services;
#[cfg(feature = "client-adapter")]
mod static_topic;
#[cfg(feature = "client-adapter")]
mod targeted_read;
#[cfg(feature = "client-adapter")]
mod topic;

#[cfg(feature = "client-adapter")]
pub use lifecycle::AdminBuilder;
#[cfg(feature = "client-adapter")]
pub use lifecycle::AdminGuard;
#[cfg(feature = "client-adapter")]
pub use lifecycle::AdminSession;
#[cfg(feature = "client-adapter")]
pub use lifecycle::ClientRuntime;
#[cfg(feature = "client-adapter")]
pub use lifecycle::ClientRuntimeConfig;
#[cfg(feature = "client-adapter")]
pub use lifecycle::TelemetryHandle;
