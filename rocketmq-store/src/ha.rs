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

pub(crate) mod auto_switch;
pub(crate) mod default_ha_client;
mod default_ha_connection;
pub(crate) mod default_ha_service;
pub(crate) mod flow_monitor;
pub(crate) mod general_ha_client;
pub(crate) mod general_ha_connection;
pub(crate) mod general_ha_service;
mod group_transfer_service;
pub(crate) mod ha_client;
pub(crate) mod ha_connection;
pub mod ha_connection_state;
pub mod ha_connection_state_notification_request;
mod ha_connection_state_notification_service;
pub mod ha_service;
pub(crate) mod wait_notify_object;

/// Error types
#[derive(Debug, thiserror::Error)]
pub enum HAConnectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Service error: {0}")]
    Service(String),
    #[error("Invalid state: {0}")]
    InvalidState(String),
}
