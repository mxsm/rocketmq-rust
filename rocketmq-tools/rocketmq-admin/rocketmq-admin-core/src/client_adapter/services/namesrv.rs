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

//! NameServer management core logic
//!
//! This module provides operations for managing RocketMQ NameServers,
//! including configuration, KV store, and broker permissions.
//!
//! # Examples
//!
//! ```rust,ignore
//! use rocketmq_admin_core::core::namesrv::NameServerService;
//!
//! let config = NameServerService::get_namesrv_config(&mut admin, addrs).await?;
//! ```

pub mod operations;
mod types;

// Re-export all public items
pub use self::operations::NameServerService;
pub use self::types::BrokerPermission;
pub use self::types::ConfigItem;
pub use self::types::KvConfigDeleteRequest;
pub use self::types::KvConfigUpdateRequest;
pub use self::types::KvConfigUpdateResult;
pub use self::types::NamesrvConfigQueryRequest;
pub use self::types::NamesrvConfigQueryResult;
pub use self::types::NamesrvConfigUpdateRequest;
pub use self::types::NamesrvConfigUpdateResult;
pub use self::types::WritePermRequest;
pub use self::types::WritePermResult;
pub use self::types::WritePermResultEntry;
