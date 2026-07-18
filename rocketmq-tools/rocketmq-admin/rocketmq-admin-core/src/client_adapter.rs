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

mod broker;
mod consumer;
mod dashboard;
pub mod lifecycle;
mod lite;
mod message;
mod security;
mod static_topic;
mod topic;

pub use self::lifecycle::AdminGuard;
pub use self::lifecycle::AdminSession;
pub use self::lifecycle::ClientAdminBuilder;
pub use self::security::admin_acl_rpc_hook;

#[cfg(feature = "legacy-common-compat")]
#[doc(hidden)]
pub mod legacy;
