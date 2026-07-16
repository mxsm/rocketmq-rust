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

pub use rocketmq_store_local::base::allocate_mapped_file_service::AllocateMappedFileService;
use rocketmq_store_local::base::allocate_mapped_file_service::AllocateMappedFileServiceConfig;
use rocketmq_store_local::mapped_file::allocation_policy::MappedFileWarmupConfig;

use crate::config::message_store_config::MessageStoreConfig;

impl AllocateMappedFileServiceConfig for MessageStoreConfig {
    fn mapped_file_warmup_config(&self) -> MappedFileWarmupConfig {
        MappedFileWarmupConfig::new(
            self.warm_mapped_file_enable,
            self.flush_disk_type,
            self.mapped_file_size_commit_log,
            self.flush_least_pages_when_warm_mapped_file,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_store_config_projects_the_local_warmup_policy() {
        let config = MessageStoreConfig {
            warm_mapped_file_enable: true,
            mapped_file_size_commit_log: 1024,
            flush_least_pages_when_warm_mapped_file: 3,
            ..MessageStoreConfig::default()
        };

        let warmup = config.mapped_file_warmup_config();
        assert!(!warmup.should_warm(1023));
        assert!(warmup.should_warm(1024));
        assert_eq!(warmup.flush_disk_type(), config.flush_disk_type);
        assert_eq!(warmup.flush_least_pages(), 3);
    }
}
