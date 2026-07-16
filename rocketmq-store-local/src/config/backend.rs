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

use std::path::PathBuf;

/// Immutable local-store settings projected from the legacy Serde envelope.
///
/// This type contains only normalized values consumed by local-store policies.
/// Configuration aliases, defaults, and compatibility fields remain owned by
/// the caller that deserializes the legacy configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalBackendConfig {
    pub store_path_root_dir: PathBuf,
    pub commit_log_paths: Vec<PathBuf>,
    pub store_io_hint_enabled: bool,
    pub recovery: LocalRecoveryConfig,
    pub query: LocalQueryConfig,
    pub reput: LocalReputConfig,
    pub cleanup: LocalCleanupConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalRecoveryConfig {
    pub consume_queue_parallelism: usize,
    pub background_index_rebuild_enabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalQueryConfig {
    pub message_index_enabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalReputConfig {
    pub read_uncommitted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LocalCleanupConfig {
    pub disk_warning_ratio: f64,
    pub disk_clean_forcibly_ratio: f64,
    pub disk_max_used_ratio: f64,
    pub clean_file_forcibly_enabled: bool,
}
