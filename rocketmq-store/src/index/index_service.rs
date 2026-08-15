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

use std::fs;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all::time_millis_to_human_string;
#[cfg(test)]
use rocketmq_store_local::index::service::build_index_key;
use rocketmq_store_local::index::service::build_index_key_into;
#[cfg(test)]
use rocketmq_store_local::index::service::build_index_key_with_type;
use rocketmq_store_local::index::service::destroy_index_files_with_outcome;
use rocketmq_store_local::index::service::drive_index_build_keys;
use rocketmq_store_local::index::service::drive_index_service_put;
use rocketmq_store_local::index::service::expired_index_file_count;
use rocketmq_store_local::index::service::has_indexable_keys;
use rocketmq_store_local::index::service::index_build_key_capacity;
use rocketmq_store_local::index::service::index_flush_checkpoint_timestamp;
use rocketmq_store_local::index::service::index_key_len;
use rocketmq_store_local::index::service::index_safe_offset;
use rocketmq_store_local::index::service::max_index_dispatch_offset;
use rocketmq_store_local::index::service::plan_index_build;
use rocketmq_store_local::index::service::plan_last_index_file;
use rocketmq_store_local::index::service::query_index_files;
use rocketmq_store_local::index::service::restore_index_safe_offset;
use rocketmq_store_local::index::service::retry_index_file_create;
use rocketmq_store_local::index::service::should_remove_unsafe_index_file;
use rocketmq_store_local::index::service::shutdown_index_files;
use rocketmq_store_local::index::service::total_index_file_size;
use rocketmq_store_local::index::service::IndexBuildKeyKind;
use rocketmq_store_local::index::service::IndexBuildPreflight;
use rocketmq_store_local::index::service::IndexServiceFile;
use rocketmq_store_local::index::service::IndexServiceRoot;
use rocketmq_store_local::index::service::MAX_TRY_INDEX_FILE_CREATE;
use rocketmq_store_local::mapped_file::MappedFileDestroyOutcome;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::message_store_config::MessageStoreConfig;
use crate::index::index_file::IndexFile;
use crate::index::query_offset_result::QueryOffsetResult;
use crate::runtime::StoreRuntimeScope;
use crate::store::running_flags::RunningFlags;
use crate::store_path_config_helper::get_store_path_index;

#[derive(Clone)]
pub struct IndexService {
    root: IndexServiceRoot<IndexServiceAdapter>,
}

#[derive(Clone)]
pub struct IndexServiceAdapter {
    runtime_scope: StoreRuntimeScope,
    hash_slot_num: u32,
    index_num: u32,
    store_path: String,
    index_file_list: Arc<RwLock<Vec<Arc<IndexFile>>>>,
    operation_closing: Arc<RwLock<bool>>,
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    running_flags: Arc<RunningFlags>,
}

impl IndexService {
    pub fn new(
        runtime_scope: StoreRuntimeScope,
        message_store_config: Arc<MessageStoreConfig>,
        store_checkpoint: Arc<StoreCheckpoint>,
        running_flags: Arc<RunningFlags>,
    ) -> Self {
        Self {
            root: IndexServiceRoot::new(IndexServiceAdapter::new(
                runtime_scope,
                message_store_config,
                store_checkpoint,
                running_flags,
            )),
        }
    }
}

impl Deref for IndexService {
    type Target = IndexServiceAdapter;

    fn deref(&self) -> &Self::Target {
        self.root.adapter()
    }
}

impl DerefMut for IndexService {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.root.adapter_mut()
    }
}

impl IndexServiceAdapter {
    fn new(
        runtime_scope: StoreRuntimeScope,
        message_store_config: Arc<MessageStoreConfig>,
        store_checkpoint: Arc<StoreCheckpoint>,
        running_flags: Arc<RunningFlags>,
    ) -> Self {
        Self {
            runtime_scope,
            hash_slot_num: message_store_config.max_hash_slot_num,
            index_num: message_store_config.max_index_num,
            store_path: get_store_path_index(message_store_config.store_path_root_dir.as_str()),
            index_file_list: Arc::new(Default::default()),
            operation_closing: Arc::new(RwLock::new(false)),
            message_store_config,
            store_checkpoint,
            running_flags,
        }
    }

    #[inline]
    pub fn start(&self) {
        // Empty implementation
    }

    pub fn shutdown(&self) {
        self.shutdown_with_callbacks(|| {}, || {});
    }

    pub fn load(&mut self, last_exit_ok: bool) -> bool {
        let Some(_operation) = self.try_enter_operation() else {
            return false;
        };
        let dir = Path::new(&self.store_path);
        let Ok(read_dir) = fs::read_dir(dir) else {
            return true;
        };

        let mut files: Vec<_> = read_dir.filter_map(Result::ok).map(|entry| entry.path()).collect();

        // Sort files in ascending order
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        let checkpoint_timestamp = self.store_checkpoint.index_msg_timestamp() as i64;
        let mut write_list = self.index_file_list.write();

        let mut removed_unsafe_index_file = false;
        for file in files {
            let Some(file_path) = file.to_str() else {
                warn!("Invalid file path: {:?}", file);
                continue;
            };

            let index_file =
                match IndexFile::try_new(file_path, self.hash_slot_num as usize, self.index_num as usize, 0, 0) {
                    Ok(index_file) => index_file,
                    Err(error) => {
                        error!("load index file {} failed: {}", file_path, error);
                        return false;
                    }
                };
            index_file.load();

            if should_remove_unsafe_index_file(last_exit_ok, index_file.get_end_timestamp(), checkpoint_timestamp) {
                if !index_file.try_destroy(0).is_namespace_removed() {
                    error!(
                        file_path,
                        "unsafe index file cleanup deferred; aborting load so the path remains retryable"
                    );
                    return false;
                }
                removed_unsafe_index_file = true;
                continue;
            }

            info!("load index file OK, {}", file_path);
            write_list.push(Arc::new(index_file));
        }

        let restored_index_safe_offset = self.store_checkpoint.index_safe_phy_offset();
        let effective_index_safe_offset =
            restore_index_safe_offset(&write_list, restored_index_safe_offset, removed_unsafe_index_file);
        let loaded_index_safe_offset = restore_index_safe_offset(&write_list, 0, true);
        self.store_checkpoint
            .set_index_safe_phy_offset(effective_index_safe_offset);
        info!(
            "index safe offset restored, persisted: {}, loaded: {}, effective: {}, removedUnsafeIndexFile: {}",
            restored_index_safe_offset,
            loaded_index_safe_offset,
            effective_index_safe_offset,
            removed_unsafe_index_file
        );

        true
    }

    #[inline]
    pub fn get_total_size(&self) -> u64 {
        let index_file_list = self.index_file_list.read();
        total_index_file_size(&index_file_list)
    }

    #[inline]
    pub fn get_max_dispatch_commit_log_offset(&self) -> Option<i64> {
        max_index_dispatch_offset(&self.index_file_list.read())
    }

    #[inline]
    pub fn index_safe_phy_offset(&self) -> u64 {
        self.store_checkpoint.index_safe_phy_offset()
    }

    #[inline]
    pub fn advance_index_safe_offset_to(&self, index_safe_offset: i64) {
        if index_safe_offset <= 0 {
            return;
        }
        self.store_checkpoint
            .advance_index_safe_phy_offset(index_safe_offset as u64);
    }

    #[inline]
    pub fn flush_index_safe_offset(&self) -> std::io::Result<()> {
        self.store_checkpoint.flush()
    }

    pub fn delete_expired_file(&self, offset: u64) {
        let files = {
            let index_file_list = self.index_file_list.read();
            let expired_count = expired_index_file_count(&index_file_list, offset);
            if expired_count == 0 {
                return;
            }
            index_file_list.iter().take(expired_count).cloned().collect::<Vec<_>>()
        };

        if !files.is_empty() {
            self.delete_expired_file_list(files);
        }
    }

    fn delete_expired_file_list(&self, files: Vec<Arc<IndexFile>>) {
        if files.is_empty() {
            return;
        }

        info!("Delete expired index files, count: {}", files.len());

        let mut destroyed_files = Vec::new();
        for file in files.iter() {
            let file_name = file.get_file_name();
            match file.try_destroy(3000) {
                MappedFileDestroyOutcome::NamespaceRemoved => {
                    destroyed_files.push(file_name.clone());
                    info!(file_name = %file_name, "expired index-file namespace removal succeeded");
                }
                MappedFileDestroyOutcome::CleanupPending { ref_count } => {
                    warn!(
                        file_name = %file_name,
                        ref_count,
                        "expired index-file cleanup deferred; retaining this and later identities"
                    );
                    break;
                }
                MappedFileDestroyOutcome::DeleteFailed { kind, raw_os_error } => {
                    error!(
                        file_name = %file_name,
                        ?kind,
                        ?raw_os_error,
                        "expired index-file namespace removal failed; retaining this and later identities"
                    );
                    break;
                }
            }
        }

        if !destroyed_files.is_empty() {
            let mut index_file_list = self.index_file_list.write();
            index_file_list.retain(|f| !destroyed_files.contains(f.get_file_name()));
        }
    }

    #[inline]
    pub fn destroy(&self) {
        let _ = self.destroy_with_outcome();
    }

    #[must_use]
    pub fn destroy_with_outcome(&self) -> bool {
        let mut index_file_list = self.index_file_list.write();
        destroy_index_files_with_outcome(&mut index_file_list, |file| match file.try_destroy(3000) {
            MappedFileDestroyOutcome::NamespaceRemoved => true,
            MappedFileDestroyOutcome::CleanupPending { ref_count } => {
                warn!(
                    file_name = %file.get_file_name(),
                    ref_count,
                    "index-file cleanup deferred; retaining this and later identities"
                );
                false
            }
            MappedFileDestroyOutcome::DeleteFailed { kind, raw_os_error } => {
                error!(
                    file_name = %file.get_file_name(),
                    ?kind,
                    ?raw_os_error,
                    "index-file namespace removal failed; retaining this and later identities"
                );
                false
            }
        })
    }

    pub fn query_offset(&self, topic: &str, key: &str, max_num: i32, begin: i64, end: i64) -> QueryOffsetResult {
        self.query_offset_with_type(topic, key, max_num, begin, end, None)
    }

    /// Query offset with index type support (matches Java overload)
    ///
    /// # Arguments
    /// * `index_type` - Optional index type (e.g., MessageConst::INDEX_TAG_TYPE or
    ///   MessageConst::INDEX_UNIQUE_TYPE). If None or empty, uses default format: topic#key.
    pub fn query_offset_with_type(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
        index_type: Option<&str>,
    ) -> QueryOffsetResult {
        let max_num = max_num.max(0).min(self.message_store_config.max_msgs_num_batch as i32);
        if max_num == 0 {
            return QueryOffsetResult::new(Vec::new(), 0, 0);
        }

        let query_index_type = index_type.filter(|idx_type| {
            !idx_type.is_empty()
                && matches!(
                    *idx_type,
                    MessageConst::INDEX_TAG_TYPE | MessageConst::INDEX_UNIQUE_TYPE
                )
        });
        let mut query_key = String::with_capacity(index_key_len(topic, key, query_index_type));
        let query_key = build_index_key_into(&mut query_key, topic, key, query_index_type);
        query_index_files(&self.index_file_list.read(), query_key, max_num as usize, begin, end)
    }

    pub fn build_index(&self, dispatch_request: &DispatchRequest) {
        let _ = self.build_index_with(dispatch_request, || {});
    }

    fn build_index_with<OnEntered>(&self, dispatch_request: &DispatchRequest, on_entered: OnEntered) -> bool
    where
        OnEntered: FnOnce(),
    {
        let Some(_operation) = self.try_enter_operation() else {
            return false;
        };
        on_entered();
        self.build_index_admitted(dispatch_request);
        true
    }

    fn build_index_admitted(&self, dispatch_request: &DispatchRequest) {
        let tran_type = MessageSysFlag::get_transaction_value(dispatch_request.sys_flag);
        let topic = dispatch_request.topic.as_str();
        let keys = dispatch_request.keys.as_str();
        let tags = dispatch_request
            .properties_map
            .as_ref()
            .and_then(|properties| properties.get(MessageConst::PROPERTY_TAGS))
            .filter(|tags| !tags.is_empty());
        let preflight = plan_index_build(
            tran_type == MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            has_indexable_keys(
                dispatch_request.uniq_key.as_ref().map(CheetahString::as_str),
                keys,
                tags.map(CheetahString::as_str),
                MessageConst::KEY_SEPARATOR,
            ),
            dispatch_request.commit_log_offset,
            dispatch_request.msg_size,
            self.get_max_dispatch_commit_log_offset(),
        );
        match preflight {
            IndexBuildPreflight::Build => {}
            IndexBuildPreflight::AdvanceSafeOffset(safe_offset) => {
                if let Some(safe_offset) = safe_offset {
                    self.advance_index_safe_offset_to(safe_offset);
                }
                return;
            }
            IndexBuildPreflight::SkipOldOffset => return,
        }

        let index_file = self.retry_get_and_create_index_file();
        match index_file {
            Some(index_file_inner) => {
                let end_phy_offset = index_file_inner.get_end_phy_offset();
                if dispatch_request.commit_log_offset < end_phy_offset {
                    return;
                }
                let mut index_file_new = Some(index_file_inner);
                let mut index_key = String::with_capacity(index_build_key_capacity(
                    topic,
                    dispatch_request.uniq_key.as_ref().map(CheetahString::as_str),
                    keys,
                    tags.map(CheetahString::as_str),
                    MessageConst::KEY_SEPARATOR,
                    MessageConst::INDEX_UNIQUE_TYPE,
                    MessageConst::INDEX_TAG_TYPE,
                ));
                let key_outcome = drive_index_build_keys(
                    dispatch_request.uniq_key.as_ref().map(CheetahString::as_str),
                    keys,
                    tags.map(CheetahString::as_str),
                    MessageConst::KEY_SEPARATOR,
                    MessageConst::INDEX_UNIQUE_TYPE,
                    MessageConst::INDEX_TAG_TYPE,
                    |kind, key, index_type| {
                        let kind_name = match kind {
                            IndexBuildKeyKind::Unique => "uniq key",
                            IndexBuildKeyKind::Normal => "key",
                            IndexBuildKeyKind::Tag => "tags",
                        };
                        let Some(index_file) = index_file_new.take() else {
                            error!(
                                "skip index {} {} because no writable index file is available, commitlog {}",
                                kind_name, key, dispatch_request.commit_log_offset
                            );
                            return false;
                        };
                        index_file_new = self.put_key(
                            index_file,
                            dispatch_request,
                            build_index_key_into(&mut index_key, topic, key, index_type),
                        );
                        if index_file_new.is_none() {
                            error!(
                                "putKey error commitlog {} {} {}",
                                dispatch_request.commit_log_offset, kind_name, key
                            );
                        }
                        index_file_new.is_some()
                    },
                );
                if key_outcome.advances_safe_offset() {
                    self.advance_index_safe_offset_for_request(dispatch_request);
                }
            }
            None => {
                error!("build index error, stop building index");
            }
        }
    }

    fn advance_index_safe_offset_for_request(&self, dispatch_request: &DispatchRequest) {
        if let Some(safe_offset) = index_safe_offset(dispatch_request.commit_log_offset, dispatch_request.msg_size) {
            self.advance_index_safe_offset_to(safe_offset);
        }
    }

    #[inline]
    fn put_key(&self, index_file: Arc<IndexFile>, msg: &DispatchRequest, idx_key: &str) -> Option<Arc<IndexFile>> {
        drive_index_service_put(
            index_file,
            |index_file| index_file.put_key(idx_key, msg.commit_log_offset, msg.store_timestamp),
            |index_file| {
                warn!(
                    "Index file [{}] is full, trying to create another one",
                    index_file.get_file_name()
                );
                self.retry_get_and_create_index_file()
            },
        )
    }

    fn retry_get_and_create_index_file(&self) -> Option<Arc<IndexFile>> {
        let index_file = retry_index_file_create(
            MAX_TRY_INDEX_FILE_CREATE,
            || self.get_and_create_last_index_file_admitted(),
            |attempt, max_attempts| {
                warn!("Failed to create index file, attempt {attempt}/{max_attempts}");
                thread::sleep(Duration::from_secs(1));
            },
        );
        if index_file.is_none() {
            self.running_flags.make_index_file_error();
            error!(
                "Failed to create index file after {} attempts, marking error flag",
                MAX_TRY_INDEX_FILE_CREATE
            );
        }
        index_file
    }

    pub fn get_and_create_last_index_file(&self) -> Option<Arc<IndexFile>> {
        let _operation = self.try_enter_operation()?;
        self.get_and_create_last_index_file_admitted()
    }

    fn get_and_create_last_index_file_admitted(&self) -> Option<Arc<IndexFile>> {
        let seed = plan_last_index_file(&self.index_file_list.read());
        if let Some(index_file) = seed.reusable {
            return Some(index_file);
        }

        let file_name = format!(
            "{}{}{}",
            self.store_path,
            std::path::MAIN_SEPARATOR,
            time_millis_to_human_string(current_millis() as i64)
        );
        let new_index_file = match IndexFile::try_new(
            file_name.as_str(),
            self.hash_slot_num as usize,
            self.index_num as usize,
            seed.previous_end_phy_offset,
            seed.previous_end_timestamp,
        ) {
            Ok(index_file) => Arc::new(index_file),
            Err(error) => {
                error!("create index file {} failed: {}", file_name, error);
                self.running_flags.make_index_file_error();
                return None;
            }
        };
        self.index_file_list.write().push(new_index_file.clone());

        if let Some(previous_index_file) = seed.previous_full {
            let index_service = self.clone();
            let fallback_index_file = previous_index_file.clone();
            if let Err(error) =
                crate::runtime::spawn_background_io(&self.runtime_scope, "index-file-flush", move || {
                    index_service.flush(Some(previous_index_file));
                })
            {
                warn!("failed to spawn index file flush task, flushing inline: {error}");
                self.flush(Some(fallback_index_file));
            }
        }
        Some(new_index_file)
    }

    fn try_enter_operation(&self) -> Option<RwLockReadGuard<'_, bool>> {
        let operation = self.operation_closing.read();
        if *operation {
            return None;
        }
        Some(operation)
    }

    fn shutdown_with_callbacks<BeforeWait, OnClosing>(&self, before_wait: BeforeWait, on_closing: OnClosing)
    where
        BeforeWait: FnOnce(),
        OnClosing: FnOnce(),
    {
        before_wait();
        let mut closing = self.operation_closing.write();
        if *closing {
            return;
        }
        *closing = true;
        on_closing();

        let files = self.index_file_list.read().clone();
        shutdown_index_files(&files, |index_file| index_file.shutdown());
    }

    #[inline]
    pub fn flush(&self, index_file: Option<Arc<IndexFile>>) {
        let Some(index_file) = index_file else {
            return;
        };

        let index_msg_timestamp = index_flush_checkpoint_timestamp(index_file.as_ref());

        index_file.flush();

        if let Some(index_msg_timestamp) = index_msg_timestamp {
            self.store_checkpoint.set_index_msg_timestamp(index_msg_timestamp);
            let _ = self.store_checkpoint.flush();
        }
    }

    pub(crate) fn flush_release_checkpoint(&self) -> std::io::Result<i64> {
        let files = self.index_file_list.read().clone();
        for index_file in &files {
            index_file.flush();
        }
        let safe_offset = max_index_dispatch_offset(&files).unwrap_or_default().max(0);
        self.store_checkpoint.advance_index_safe_phy_offset(safe_offset as u64);
        self.store_checkpoint.flush()?;
        Ok(safe_offset)
    }
}

impl IndexServiceFile for IndexFile {
    fn file_size(&self) -> usize {
        IndexFile::get_file_size(self)
    }

    fn has_entries(&self) -> bool {
        IndexFile::has_entries(self)
    }

    fn is_write_full(&self) -> bool {
        IndexFile::is_write_full(self)
    }

    fn begin_timestamp(&self) -> i64 {
        IndexFile::get_begin_timestamp(self)
    }

    fn end_timestamp(&self) -> i64 {
        IndexFile::get_end_timestamp(self)
    }

    fn end_phy_offset(&self) -> i64 {
        IndexFile::get_end_phy_offset(self)
    }

    fn is_time_matched(&self, begin: i64, end: i64) -> bool {
        IndexFile::is_time_matched(self, begin, end)
    }

    fn select_phy_offsets(&self, offsets: &mut Vec<i64>, key: &str, max_num: usize, begin: i64, end: i64) {
        IndexFile::select_phy_offset(self, offsets, key, max_num, begin, end);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::store::running_flags::RunningFlags;

    fn new_index_service_for_test(temp_dir: &tempfile::TempDir, checkpoint_name: &str) -> IndexService {
        new_index_service_for_test_with_limits(temp_dir, checkpoint_name, 64)
    }

    fn new_index_service_for_test_with_limits(
        temp_dir: &tempfile::TempDir,
        checkpoint_name: &str,
        max_index_num: u32,
    ) -> IndexService {
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir),
            max_hash_slot_num: 32,
            max_index_num,
            ..MessageStoreConfig::default()
        });
        let temp_file = temp_dir.path().join(checkpoint_name);
        let store_checkpoint = Arc::new(StoreCheckpoint::new(&temp_file).unwrap());
        let running_flags = Arc::new(RunningFlags::default());

        IndexService::new(
            crate::runtime::test_scope("index-service-test"),
            message_store_config,
            store_checkpoint,
            running_flags,
        )
    }

    #[test]
    fn test_build_key_formats() {
        // Test default key format: topic#key
        let key1 = build_index_key("TestTopic", "key123");
        assert_eq!(key1, "TestTopic#key123");

        // Test key with type format: topic#indexType#key
        let key2 = build_index_key_with_type("TestTopic", "tagValue", MessageConst::INDEX_TAG_TYPE);
        assert_eq!(key2, "TestTopic#T#tagValue");
    }

    #[test]
    fn index_build_key_capacity_covers_uniq_normal_and_tag_keys() {
        assert_eq!(
            index_build_key_capacity(
                "Topic",
                Some("uniq-longer"),
                "k1 k22",
                Some("Tag"),
                MessageConst::KEY_SEPARATOR,
                MessageConst::INDEX_UNIQUE_TYPE,
                MessageConst::INDEX_TAG_TYPE,
            ),
            index_key_len("Topic", "uniq-longer", Some(MessageConst::INDEX_UNIQUE_TYPE))
        );
        assert_eq!(
            index_build_key_capacity(
                "Topic",
                Some("u"),
                "short very-long-normal-key",
                Some("Tag"),
                MessageConst::KEY_SEPARATOR,
                MessageConst::INDEX_UNIQUE_TYPE,
                MessageConst::INDEX_TAG_TYPE,
            ),
            index_key_len("Topic", "very-long-normal-key", None)
        );
        assert_eq!(
            index_build_key_capacity(
                "Topic",
                None,
                "k1",
                Some("longer-tag-value"),
                MessageConst::KEY_SEPARATOR,
                MessageConst::INDEX_UNIQUE_TYPE,
                MessageConst::INDEX_TAG_TYPE,
            ),
            index_key_len("Topic", "longer-tag-value", Some(MessageConst::INDEX_TAG_TYPE))
        );
    }

    #[test]
    fn test_build_index_with_tags() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir.clone()),
            ..MessageStoreConfig::default()
        });
        let temp_file = temp_dir.path().join("store_checkpoint_test_build_index_with_tags");
        let store_checkpoint = Arc::new(StoreCheckpoint::new(&temp_file).unwrap());
        let running_flags = Arc::new(RunningFlags::default());

        let index_service = IndexService::new(
            crate::runtime::test_scope("index-tags-test"),
            message_store_config,
            store_checkpoint,
            running_flags,
        );

        // Create dispatch request with tags in properties_map
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TAGS),
            CheetahString::from_slice("TestTag"),
        );

        let dispatch_request = DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            queue_id: 0,
            commit_log_offset: 1000,
            msg_size: 100,
            body_size: 0,
            tags_code: 0,
            store_timestamp: 1000000000000,
            consume_queue_offset: 0,
            keys: CheetahString::from_string(format!("key1{}key2", MessageConst::KEY_SEPARATOR)),
            success: true,
            uniq_key: Some(CheetahString::from_slice("uniq123")),
            sys_flag: 0,
            prepared_transaction_offset: 0,
            properties_map: Some(properties),
            bit_map: None,
            buffer_size: -1,
            msg_base_offset: -1,
            batch_size: 1,
            next_reput_from_offset: -1,
            offset_id: None,
        };

        index_service.build_index(&dispatch_request);

        for key in ["key1", "key2"] {
            let result = index_service.query_offset("TestTopic", key, 10, 0, i64::MAX);
            assert_eq!(result.get_phy_offsets(), &[1000], "missing index for key {key}");
        }

        let unique_result = index_service.query_offset_with_type(
            "TestTopic",
            "uniq123",
            10,
            0,
            i64::MAX,
            Some(MessageConst::INDEX_UNIQUE_TYPE),
        );
        assert_eq!(unique_result.get_phy_offsets(), &[1000]);

        let tag_result = index_service.query_offset_with_type(
            "TestTopic",
            "TestTag",
            10,
            0,
            i64::MAX,
            Some(MessageConst::INDEX_TAG_TYPE),
        );
        assert_eq!(tag_result.get_phy_offsets(), &[1000]);
    }

    #[test]
    fn build_index_skips_message_without_indexable_keys() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_empty_index_skip");

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            ..DispatchRequest::default()
        });

        assert_eq!(index_service.get_total_size(), 0);
        assert_eq!(index_service.index_safe_phy_offset(), 1100);
    }

    #[test]
    fn build_index_skips_rollback_without_creating_index_file() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_rollback_index_skip");

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            uniq_key: Some(CheetahString::from_slice("uniq123")),
            sys_flag: MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            ..DispatchRequest::default()
        });

        assert_eq!(index_service.get_total_size(), 0);
        assert_eq!(index_service.index_safe_phy_offset(), 1100);
    }

    #[test]
    fn build_index_advances_index_safe_offset_after_indexed_message() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_index_safe_advance");

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            ..DispatchRequest::default()
        });

        assert_eq!(index_service.index_safe_phy_offset(), 1100);
    }

    #[test]
    fn shutdown_waits_for_entered_build_and_rejects_late_build_and_create() {
        let temp_dir = tempdir().unwrap();
        let index_service = Arc::new(new_index_service_for_test(
            &temp_dir,
            "store_checkpoint_test_shutdown_build_fence",
        ));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let build_service = Arc::clone(&index_service);
        let build_entered = Arc::clone(&entered);
        let build_release = Arc::clone(&release);
        let build = thread::spawn(move || {
            build_service.build_index_with(
                &DispatchRequest {
                    topic: CheetahString::from_slice("TestTopic"),
                    commit_log_offset: 1000,
                    msg_size: 100,
                    store_timestamp: 1_000_000_000_000,
                    keys: CheetahString::from_slice("entered-key"),
                    ..DispatchRequest::default()
                },
                || {
                    build_entered.wait();
                    build_release.wait();
                },
            )
        });
        entered.wait();

        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (closing_tx, closing_rx) = mpsc::channel();
        let shutdown_service = Arc::clone(&index_service);
        let shutdown = thread::spawn(move || {
            shutdown_service.shutdown_with_callbacks(
                || waiting_tx.send(()).expect("publish shutdown wait"),
                || closing_tx.send(()).expect("publish service closing"),
            );
        });

        waiting_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("shutdown reached the service operation fence");
        assert!(matches!(closing_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        release.wait();
        assert!(build.join().expect("join entered index build"));
        closing_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("shutdown entered closing after the build drained");
        shutdown.join().expect("join index shutdown");

        let file_count = index_service.index_file_list.read().len();
        let safe_offset = index_service.index_safe_phy_offset();
        assert!(!index_service.build_index_with(
            &DispatchRequest {
                topic: CheetahString::from_slice("TestTopic"),
                commit_log_offset: 2000,
                msg_size: 100,
                store_timestamp: 1_000_000_001_000,
                keys: CheetahString::from_slice("late-key"),
                ..DispatchRequest::default()
            },
            || panic!("closed service must not enter a late build"),
        ));
        assert!(index_service.get_and_create_last_index_file().is_none());
        assert_eq!(index_service.index_file_list.read().len(), file_count);
        assert_eq!(index_service.index_safe_phy_offset(), safe_offset);
    }

    #[test]
    fn load_restores_index_safe_offset_from_loaded_index_files_for_legacy_checkpoint() {
        let temp_dir = tempdir().unwrap();
        let checkpoint_name = "store_checkpoint_test_index_safe_legacy_load";
        let index_service = new_index_service_for_test(&temp_dir, checkpoint_name);

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            ..DispatchRequest::default()
        });
        let index_file = index_service.index_file_list.read().last().cloned();
        index_service.flush(index_file);
        index_service.store_checkpoint.set_index_safe_phy_offset(0);
        index_service.store_checkpoint.flush().unwrap();

        let mut reloaded = new_index_service_for_test(&temp_dir, checkpoint_name);
        assert!(reloaded.load(true));

        assert_eq!(reloaded.index_safe_phy_offset(), 1000);
    }

    #[test]
    fn load_clamps_index_safe_offset_when_unsafe_index_file_removed() {
        let temp_dir = tempdir().unwrap();
        let checkpoint_name = "store_checkpoint_test_index_safe_clamp_removed";
        let index_service = new_index_service_for_test(&temp_dir, checkpoint_name);

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            ..DispatchRequest::default()
        });
        let index_file = index_service.index_file_list.read().last().cloned();
        index_service.flush(index_file);
        index_service.store_checkpoint.set_index_safe_phy_offset(1100);
        index_service.store_checkpoint.set_index_msg_timestamp(0);
        index_service.store_checkpoint.flush().unwrap();

        let mut reloaded = new_index_service_for_test(&temp_dir, checkpoint_name);
        assert!(reloaded.load(false));

        assert_eq!(reloaded.index_safe_phy_offset(), 0);
    }

    #[test]
    fn build_index_skips_old_offset_before_rolling_full_file() {
        let temp_dir = tempdir().unwrap();
        let index_service =
            new_index_service_for_test_with_limits(&temp_dir, "store_checkpoint_test_old_offset_no_roll", 2);

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("new-key"),
            ..DispatchRequest::default()
        });
        let initial_total_size = index_service.get_total_size();
        assert!(initial_total_size > 0);

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 999,
            store_timestamp: 1000000001000,
            keys: CheetahString::from_slice("old-key"),
            ..DispatchRequest::default()
        });

        assert_eq!(index_service.get_total_size(), initial_total_size);
        let result = index_service.query_offset("TestTopic", "old-key", 10, 0, i64::MAX);
        assert!(result.get_phy_offsets().is_empty());
    }

    #[test]
    fn test_query_offset_with_type_api() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir),
            ..MessageStoreConfig::default()
        });
        let temp_file = temp_dir.path().join("store_checkpoint_test_query_offset_with_type_api");
        let store_checkpoint = Arc::new(StoreCheckpoint::new(&temp_file).unwrap());
        let running_flags = Arc::new(RunningFlags::default());

        let index_service = IndexService::new(
            crate::runtime::test_scope("index-query-type-test"),
            message_store_config,
            store_checkpoint,
            running_flags,
        );

        // Test default query (no type)
        let result1 = index_service.query_offset("TestTopic", "key1", 10, 0, i64::MAX);
        assert_eq!(result1.get_phy_offsets().len(), 0); // Empty because no index files

        // Test query with INDEX_TAG_TYPE
        let result2 = index_service.query_offset_with_type(
            "TestTopic",
            "tagValue",
            10,
            0,
            i64::MAX,
            Some(MessageConst::INDEX_TAG_TYPE),
        );
        assert_eq!(result2.get_phy_offsets().len(), 0); // Empty because no index files

        // Verify both methods work without panicking
    }

    #[test]
    fn query_offset_with_negative_max_num_returns_empty_result() {
        let temp_dir = tempdir().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().to_string();
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root_dir),
            ..MessageStoreConfig::default()
        });
        let temp_file = temp_dir.path().join("store_checkpoint_test_negative_max_num");
        let store_checkpoint = Arc::new(StoreCheckpoint::new(&temp_file).unwrap());
        let running_flags = Arc::new(RunningFlags::default());

        let index_service = IndexService::new(
            crate::runtime::test_scope("index-negative-max-test"),
            message_store_config,
            store_checkpoint,
            running_flags,
        );

        let result = index_service.query_offset("TestTopic", "key1", -1, 0, i64::MAX);

        assert!(result.get_phy_offsets().is_empty());
        assert_eq!(result.get_index_last_update_timestamp(), 0);
        assert_eq!(result.get_index_last_update_phyoffset(), 0);
    }

    #[test]
    fn destroy_outcome_retains_failed_index_identity_until_retry() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_destroy_retry");
        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            ..DispatchRequest::default()
        });
        let index_file = index_service
            .index_file_list
            .read()
            .last()
            .cloned()
            .expect("index file");
        assert!(index_file.hold_for_testing());

        assert!(!index_service.destroy_with_outcome());
        assert!(index_service
            .index_file_list
            .read()
            .last()
            .is_some_and(|current| Arc::ptr_eq(current, &index_file)));

        index_file.release_for_testing();
        assert!(index_service.destroy_with_outcome());
        assert!(index_service.index_file_list.read().is_empty());
    }

    #[test]
    fn shutdown_retains_index_identity_and_destroy_retry_capability() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_shutdown_destroy");
        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            ..DispatchRequest::default()
        });
        let index_file = index_service
            .index_file_list
            .read()
            .last()
            .cloned()
            .expect("index file");
        let file_name = index_file.get_file_name().clone();
        assert!(index_file.hold_for_testing());

        index_service.shutdown();

        assert_eq!(index_service.index_file_list.read().len(), 1);
        assert!(Path::new(file_name.as_str()).exists());
        assert!(!index_file.put_key("late-key", 2000, 1_000_000_001_000));
        assert!(!index_service.destroy_with_outcome());
        assert!(index_service
            .index_file_list
            .read()
            .last()
            .is_some_and(|current| Arc::ptr_eq(current, &index_file)));
        assert!(Path::new(file_name.as_str()).exists());

        index_file.release_for_testing();
        assert!(index_service.destroy_with_outcome());
        assert!(index_service.index_file_list.read().is_empty());
        assert!(!Path::new(file_name.as_str()).exists());
    }

    #[test]
    fn expired_cleanup_retains_failed_index_identity_until_retry() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_expired_retry");
        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            msg_size: 100,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            ..DispatchRequest::default()
        });
        let index_file = index_service
            .index_file_list
            .read()
            .last()
            .cloned()
            .expect("index file");
        assert!(index_file.hold_for_testing());

        index_service.delete_expired_file_list(vec![Arc::clone(&index_file)]);
        assert!(index_service
            .index_file_list
            .read()
            .last()
            .is_some_and(|current| Arc::ptr_eq(current, &index_file)));

        index_file.release_for_testing();
        index_service.delete_expired_file_list(vec![index_file]);
        assert!(index_service.index_file_list.read().is_empty());
    }
}
