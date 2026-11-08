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

//! Native document models shared with the selected storage authority.

use super::config_store::ConfigStoreFailure;
use rocketmq_dashboard_common::EnvironmentId;
pub(super) use rocketmq_dashboard_storage::{DesktopDocument, LocalOperationKind, LocalOperationRecord};
pub use rocketmq_dashboard_storage::{
    EnvironmentCatalog, EnvironmentChange, EnvironmentProfile, MonitorMigrationState, StoredMonitorRule,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum EnvironmentValidation {
    #[error("the selected environment profile does not exist")]
    MissingProfile,
    #[error("environment profiles need distinct identities and a nonempty name")]
    InvalidProfile,
}

pub(super) fn new_environment_id() -> Result<EnvironmentId, ConfigStoreFailure> {
    EnvironmentId::parse(format!("env-{:032x}", rand::random::<u128>())).map_err(ConfigStoreFailure::validation)
}

#[cfg(test)]
#[path = "environment_store_tests.rs"]
mod tests;
