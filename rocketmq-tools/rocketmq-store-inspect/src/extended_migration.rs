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

//! Offline migration of a stopped Broker's Extended Timeline timers to the native engine.

use std::path::PathBuf;

use rocketmq_error::Error as CanonicalError;
use rocketmq_store::migrate_extended_timers_to_native;
use rocketmq_store::ExtendedTimerMigrationMode;
use rocketmq_store::ExtendedTimerMigrationReport;
use rocketmq_store::MessageStoreConfig;

use crate::downgrade_preflight::OfflineLock;

/// A migration of the store a Broker configuration file describes.
#[derive(Clone, Debug)]
pub struct ExtendedMigrationRequest {
    config_path: PathBuf,
    mode: ExtendedTimerMigrationMode,
}

impl ExtendedMigrationRequest {
    pub fn new(config_path: impl Into<PathBuf>, mode: ExtendedTimerMigrationMode) -> Self {
        Self {
            config_path: config_path.into(),
            mode,
        }
    }
}

/// Reads the `[store]` section of the Broker configuration, takes the store's lock, and
/// migrates its Extended Timeline timers. Blockers are returned in the report, and nothing is
/// changed then.
///
/// # Errors
///
/// Returns `storage.read.failed` when the configuration cannot be read or the store lock is
/// taken, and `core.internal.failure` when the migration fails.
pub fn run_extended_migration(
    request: &ExtendedMigrationRequest,
) -> Result<ExtendedTimerMigrationReport, CanonicalError> {
    let read_error = |error: config::ConfigError| crate::errors::storage_read_failed_by("store-inspect", error);
    let store: MessageStoreConfig = config::Config::builder()
        .add_source(config::File::from(request.config_path.clone()))
        .build()
        .map_err(read_error)?
        .get("store")
        .map_err(read_error)?;
    let _lock = OfflineLock::acquire(std::path::Path::new(store.store_path_root_dir.as_str()))?;
    migrate_extended_timers_to_native(&store, request.mode)
        .map_err(|error| crate::errors::internal_failed_by("extended_timer_migration", error))
}
