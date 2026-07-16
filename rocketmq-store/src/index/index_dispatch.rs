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

use std::sync::Arc;

use rocketmq_store_local::index::dispatch::IndexDispatchRoot;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::config::message_store_config::MessageStoreConfig;
use crate::index::index_service::IndexService;

#[derive(Clone)]
pub struct CommitLogDispatcherBuildIndex {
    root: IndexDispatchRoot<IndexDispatchAdapter>,
}

#[derive(Clone)]
pub struct IndexDispatchAdapter {
    index_service: IndexService,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildIndex {
    pub fn new(index_service: IndexService, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            root: IndexDispatchRoot::new(IndexDispatchAdapter {
                index_service,
                message_store_config,
            }),
        }
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildIndex {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        let enabled = self.root.adapter().message_store_config.message_index_enable;
        self.root.dispatch(enabled, dispatch_request, |adapter, request| {
            adapter.index_service.build_index(request);
        });
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        let enabled = self.root.adapter().message_store_config.message_index_enable;
        self.root.progress(enabled, |adapter| {
            adapter.index_service.get_max_dispatch_commit_log_offset()
        })
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use tempfile::tempdir;

    use super::*;
    use crate::base::store_checkpoint::StoreCheckpoint;
    use crate::store::running_flags::RunningFlags;

    #[test]
    fn disabled_index_dispatch_skips_build_and_progress() {
        let temp_dir = tempdir().unwrap();
        let (index_service, config) = index_service(&temp_dir, false, "disabled-checkpoint");
        let dispatcher = CommitLogDispatcherBuildIndex::new(index_service.clone(), config);
        let mut request = indexable_request();

        dispatcher.dispatch(&mut request);

        assert_eq!(index_service.get_total_size(), 0);
        assert_eq!(dispatcher.dispatch_progress_offset(0), None);
    }

    #[test]
    fn enabled_index_dispatch_builds_and_projects_progress() {
        let temp_dir = tempdir().unwrap();
        let (index_service, config) = index_service(&temp_dir, true, "enabled-checkpoint");
        let dispatcher = CommitLogDispatcherBuildIndex::new(index_service.clone(), config);
        let mut request = indexable_request();

        dispatcher.dispatch(&mut request);

        assert!(index_service.get_total_size() > 0);
        assert_eq!(dispatcher.dispatch_progress_offset(0), Some(1000));
    }

    fn index_service(
        temp_dir: &tempfile::TempDir,
        message_index_enable: bool,
        checkpoint_name: &str,
    ) -> (IndexService, Arc<MessageStoreConfig>) {
        let config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().into_owned()),
            message_index_enable,
            max_hash_slot_num: 32,
            max_index_num: 64,
            ..MessageStoreConfig::default()
        });
        let checkpoint = Arc::new(StoreCheckpoint::new(temp_dir.path().join(checkpoint_name)).unwrap());
        let running_flags = Arc::new(RunningFlags::default());
        (IndexService::new(config.clone(), checkpoint, running_flags), config)
    }

    fn indexable_request() -> DispatchRequest {
        DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1_000_000_000_000,
            keys: CheetahString::from_slice("key"),
            ..DispatchRequest::default()
        }
    }
}
