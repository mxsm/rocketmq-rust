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

//! Concurrent data table modules for route management
//!
//! These modules use DashMap for lock-free concurrent access,
//! replacing the previous global RwLock approach.

mod broker_table;
mod cluster_table;
mod filter_server_table;
mod live_table;
mod topic_queue_mapping_table;
mod topic_table;

pub use broker_table::BrokerAddrTable;
pub use cluster_table::ClusterAddrTable;
pub use filter_server_table::FilterServerTable;
pub use live_table::BrokerLiveInfo;
pub use live_table::BrokerLiveTable;
pub use topic_queue_mapping_table::TopicQueueMappingInfoTable;
pub use topic_table::TopicQueueTable;

// Re-export BrokerAddrInfo for public API usage
pub use crate::route_info::broker_addr_info::BrokerAddrInfo;
