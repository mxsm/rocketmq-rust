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

pub mod cleanup;
pub mod lifecycle;
pub mod local_file_message_store;
pub mod query;
pub mod reput;

pub use local_file_message_store::LocalStoreComposition;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::backend::LocalBackendConfig;
    use crate::config::backend::LocalCleanupConfig;
    use crate::config::backend::LocalQueryConfig;
    use crate::config::backend::LocalRecoveryConfig;
    use crate::config::backend::LocalReputConfig;

    use super::*;
    use crate::message_store::lifecycle::LocalStoreState;

    #[test]
    fn composition_initializes_each_local_boundary_from_normalized_config() {
        let config = LocalBackendConfig {
            store_path_root_dir: PathBuf::from("store"),
            commit_log_paths: vec![PathBuf::from("store/commitlog")],
            store_io_hint_enabled: true,
            recovery: LocalRecoveryConfig {
                consume_queue_parallelism: 2,
                background_index_rebuild_enabled: true,
            },
            query: LocalQueryConfig {
                message_index_enabled: true,
            },
            reput: LocalReputConfig {
                read_uncommitted: false,
            },
            cleanup: LocalCleanupConfig {
                disk_warning_ratio: 0.9,
                disk_clean_forcibly_ratio: 0.85,
                disk_max_used_ratio: 0.75,
                clean_file_forcibly_enabled: true,
            },
        };

        let composition = LocalStoreComposition::new(config);

        assert_eq!(composition.lifecycle().state(), LocalStoreState::Created);
        assert!(composition.query().index_safety(10, 10).safe);
        assert_eq!(composition.reput().end_offset(8, 10), 8);
        assert_eq!(composition.cleanup().disk_warning_ratio(), 0.9);
    }
}
