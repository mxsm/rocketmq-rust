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

//! Isolated, deny-by-default foundation for supervised RocketMQ mutations.
//!
//! Authentication test seams are intentionally not public API:
//!
//! ```compile_fail
//! use rocketmq_mcp_control::auth::AuthState;
//! ```

pub mod audit;
mod auth;
pub mod catalog;
pub mod config;
pub mod error;
pub mod guard;
pub mod model;
pub mod server;
pub mod session;
pub mod transport;
pub mod workflow;

#[cfg(feature = "write-tools")]
pub mod mutation_adapter {
    //! Compile-time proof that `write-tools` exposes only the Admin mutation boundary.

    /// The only Admin Core session type admitted by the optional feature.
    pub type AdminMutationSession = rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
}
