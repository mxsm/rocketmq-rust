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

//! CLI commands module
//!
//! This module contains refactored CLI command implementations that use
//! core business logic. These are thin wrappers that handle:
//! - Argument parsing (clap)
//! - Input validation
//! - Output formatting
//! - Error display
//!
//! # Available Commands (New Architecture - Phase 1)
//!
//! ## Topic Commands
//! - [`TopicClusterSubCommand`] - Get cluster list for a topic
//! - [`DeleteTopicCommand`] - Delete a topic from cluster/broker
//! - [`TopicRouteCommand`] - Query topic route information
//! - [`UpdateTopicCommand`] - Create or update topic configuration
//!
//! ## NameServer Commands
//! - [`GetNamesrvConfigCommand`] - Get NameServer configuration
//! - [`UpdateNamesrvConfigCommand`] - Update NameServer configuration

// Topic commands
pub mod allocate_mq_command;
pub mod delete_topic_command;
pub mod topic_cluster_command;
pub mod topic_list_command;
pub mod topic_route_command;
pub mod topic_status_command;
pub mod update_order_conf_command;
pub mod update_topic_command;
pub mod update_topic_perm_command;

// NameServer commands
pub mod add_write_perm_command;
pub mod delete_kv_config_command;
pub mod get_namesrv_config_command;
pub mod update_kv_config_command;
pub mod update_namesrv_config_command;
pub mod wipe_write_perm_command;

// Re-export command structs for convenience
pub use self::add_write_perm_command::AddWritePermCommand;
pub use self::allocate_mq_command::AllocateMqCommand;
pub use self::delete_kv_config_command::DeleteKvConfigCommand;
pub use self::delete_topic_command::DeleteTopicCommand;
pub use self::get_namesrv_config_command::GetNamesrvConfigCommand;
pub use self::topic_cluster_command::TopicClusterSubCommand;
pub use self::topic_list_command::TopicListCommand;
pub use self::topic_route_command::TopicRouteCommand;
pub use self::topic_status_command::TopicStatusCommand;
pub use self::update_kv_config_command::UpdateKvConfigCommand;
pub use self::update_namesrv_config_command::UpdateNamesrvConfigCommand;
pub use self::update_order_conf_command::UpdateOrderConfCommand;
pub use self::update_topic_command::UpdateTopicCommand;
pub use self::update_topic_perm_command::UpdateTopicPermCommand;
pub use self::wipe_write_perm_command::WipeWritePermCommand;
