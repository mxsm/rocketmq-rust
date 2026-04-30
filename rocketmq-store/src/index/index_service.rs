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
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::time_millis_to_human_string;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::message_store_config::MessageStoreConfig;
use crate::index::index_file::IndexFile;
use crate::index::query_offset_result::QueryOffsetResult;
use crate::store::running_flags::RunningFlags;
use crate::store_path_config_helper::get_store_path_index;

const MAX_TRY_IDX_CREATE: i32 = 3;

#[derive(Clone)]
pub struct IndexService {
    hash_slot_num: u32,
    index_num: u32,
    store_path: String,
    index_file_list: Arc<RwLock<Vec<Arc<IndexFile>>>>,
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    running_flags: Arc<RunningFlags>,
}

impl IndexService {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        store_checkpoint: Arc<StoreCheckpoint>,
        running_flags: Arc<RunningFlags>,
    ) -> Self {
        Self {
            hash_slot_num: message_store_config.max_hash_slot_num,
            index_num: message_store_config.max_index_num,
            store_path: get_store_path_index(message_store_config.store_path_root_dir.as_str()),
            index_file_list: Arc::new(Default::default()),
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
        let mut list = self.index_file_list.write();
        for index_file in list.iter() {
            index_file.shutdown();
        }
        list.clear();
    }

    pub fn load(&mut self, last_exit_ok: bool) -> bool {
        let dir = Path::new(&self.store_path);
        let Ok(read_dir) = fs::read_dir(dir) else {
            return true;
        };

        let mut files: Vec<_> = read_dir.filter_map(Result::ok).map(|entry| entry.path()).collect();

        // Sort files in ascending order
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        let checkpoint_timestamp = self.store_checkpoint.index_msg_timestamp() as i64;
        let mut write_list = self.index_file_list.write();

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

            if !last_exit_ok && index_file.get_end_timestamp() > checkpoint_timestamp {
                index_file.destroy(0);
                continue;
            }

            info!("load index file OK, {}", file_path);
            write_list.push(Arc::new(index_file));
        }

        true
    }

    #[inline]
    pub fn get_total_size(&self) -> u64 {
        let index_file_list = self.index_file_list.read();
        index_file_list
            .first()
            .map_or(0, |f| (f.get_file_size() * index_file_list.len()) as u64)
    }

    #[inline]
    pub fn get_max_dispatch_commit_log_offset(&self) -> Option<i64> {
        self.index_file_list
            .read()
            .iter()
            .rev()
            .find(|index_file| index_file.has_entries())
            .map(|index_file| index_file.get_end_phy_offset())
    }

    pub fn delete_expired_file(&self, offset: u64) {
        let files = {
            let index_file_list = self.index_file_list.read();

            if index_file_list.is_empty() {
                return;
            }

            let Some(first_index_file) = index_file_list.first() else {
                return;
            };
            let end_phy_offset = first_index_file.get_end_phy_offset() as u64;
            if end_phy_offset >= offset {
                return;
            }

            // Collect files to delete (all except the last one)
            index_file_list
                .iter()
                .take(index_file_list.len() - 1)
                .take_while(|f| (f.get_end_phy_offset() as u64) < offset)
                .cloned()
                .collect::<Vec<_>>()
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

        // Destroy files first and collect successful ones
        let mut destroyed_files = Vec::new();
        for file in files.iter() {
            let file_name = file.get_file_name();
            if file.destroy(3000) {
                destroyed_files.push(file_name.clone());
                info!("Delete expired index file success: {}", file_name);
            } else {
                error!("Delete expired index file failed: {}", file_name);
                break;
            }
        }

        if !destroyed_files.is_empty() {
            let mut index_file_list = self.index_file_list.write();
            index_file_list.retain(|f| !destroyed_files.contains(f.get_file_name()));
        }
    }

    #[inline]
    pub fn destroy(&self) {
        let mut index_file_list = self.index_file_list.write();
        index_file_list.iter().for_each(|f| {
            f.destroy(3000);
        });
        index_file_list.clear();
    }

    pub fn query_offset(&self, topic: &str, key: &str, max_num: i32, begin: i64, end: i64) -> QueryOffsetResult {
        self.query_offset_with_type(topic, key, max_num, begin, end, None)
    }

    /// Query offset with index type support (matches Java overload)
    ///
    /// # Arguments
    /// * `index_type` - Optional index type (e.g., MessageConst::INDEX_TAG_TYPE) If None or empty,
    ///   uses default format: topic#key If INDEX_TAG_TYPE, uses format: topic#T#key
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

        let query_index_type =
            index_type.filter(|idx_type| !idx_type.is_empty() && *idx_type == MessageConst::INDEX_TAG_TYPE);
        let mut query_key = String::with_capacity(index_key_len(topic, key, query_index_type));
        let query_key = build_key_into(&mut query_key, topic, key, query_index_type);
        let mut phy_offsets = Vec::with_capacity(max_num as usize);
        let mut index_last_update_timestamp = 0;
        let mut index_last_update_phyoffset = 0;

        {
            let index_file_list = self.index_file_list.read();

            if !index_file_list.is_empty() {
                for i in (1..=index_file_list.len()).rev() {
                    let f = &index_file_list[i - 1];
                    let last_file = i == index_file_list.len();

                    if last_file {
                        index_last_update_timestamp = f.get_end_timestamp();
                        index_last_update_phyoffset = f.get_end_phy_offset();
                    }

                    if f.is_time_matched(begin, end) {
                        f.select_phy_offset(&mut phy_offsets, query_key, max_num as usize, begin, end);
                    }

                    if f.get_begin_timestamp() < begin {
                        break;
                    }

                    if phy_offsets.len() >= max_num as usize {
                        break;
                    }
                }
            }
        } // Read lock automatically released here

        QueryOffsetResult::new(phy_offsets, index_last_update_timestamp, index_last_update_phyoffset)
    }

    pub fn build_index(&self, dispatch_request: &DispatchRequest) {
        let tran_type = MessageSysFlag::get_transaction_value(dispatch_request.sys_flag);
        match tran_type {
            MessageSysFlag::TRANSACTION_NOT_TYPE
            | MessageSysFlag::TRANSACTION_PREPARED_TYPE
            | MessageSysFlag::TRANSACTION_COMMIT_TYPE => {}
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => return,
            _ => {}
        }

        let topic = dispatch_request.topic.as_str();
        let keys = dispatch_request.keys.as_str();
        let tags = dispatch_request
            .properties_map
            .as_ref()
            .and_then(|properties| properties.get(MessageConst::PROPERTY_TAGS))
            .filter(|tags| !tags.is_empty());

        let has_normal_key = keys.split(MessageConst::KEY_SEPARATOR).any(|key| !key.is_empty());
        if dispatch_request.uniq_key.is_none() && !has_normal_key && tags.is_none() {
            return;
        }

        if self
            .get_max_dispatch_commit_log_offset()
            .is_some_and(|end_phy_offset| dispatch_request.commit_log_offset < end_phy_offset)
        {
            return;
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
                ));
                if let Some(ref uniq_key) = dispatch_request.uniq_key {
                    let Some(index_file) = index_file_new.take() else {
                        error!(
                            "skip index uniq key {} because no writable index file is available, commitlog {}",
                            uniq_key, dispatch_request.commit_log_offset
                        );
                        return;
                    };
                    index_file_new = self.put_key(
                        index_file,
                        dispatch_request,
                        build_key_into(&mut index_key, topic, uniq_key.as_str(), None),
                    );
                    if index_file_new.is_none() {
                        error!(
                            "putKey error commitlog {} uniqkey {}",
                            dispatch_request.commit_log_offset, uniq_key
                        );
                        return;
                    }
                }

                if !keys.is_empty() {
                    let keyset = keys.split(MessageConst::KEY_SEPARATOR);
                    for key in keyset {
                        if !key.is_empty() {
                            let Some(index_file) = index_file_new.take() else {
                                error!(
                                    "skip index key {} because no writable index file is available, commitlog {}",
                                    key, dispatch_request.commit_log_offset
                                );
                                return;
                            };
                            index_file_new = self.put_key(
                                index_file,
                                dispatch_request,
                                build_key_into(&mut index_key, topic, key, None),
                            );
                            if index_file_new.is_none() {
                                error!(
                                    "putKey error commitlog {} key {} uniqkey {}",
                                    dispatch_request.commit_log_offset,
                                    key,
                                    dispatch_request
                                        .uniq_key
                                        .as_ref()
                                        .map_or("<none>", CheetahString::as_str)
                                );
                                return;
                            }
                        }
                    }
                }

                // Index tags
                if let Some(tags) = tags {
                    let Some(index_file) = index_file_new.take() else {
                        error!(
                            "skip index tags {} because no writable index file is available, commitlog {}",
                            tags, dispatch_request.commit_log_offset
                        );
                        return;
                    };
                    index_file_new = self.put_key(
                        index_file,
                        dispatch_request,
                        build_key_into(&mut index_key, topic, tags.as_str(), Some(MessageConst::INDEX_TAG_TYPE)),
                    );
                    if index_file_new.is_none() {
                        error!(
                            "putKey error commitlog {} tags {}",
                            dispatch_request.commit_log_offset, tags
                        );
                    }
                }
            }
            None => {
                error!("build index error, stop building index");
            }
        }
    }

    #[inline]
    fn put_key(&self, mut index_file: Arc<IndexFile>, msg: &DispatchRequest, idx_key: &str) -> Option<Arc<IndexFile>> {
        loop {
            if index_file.put_key(idx_key, msg.commit_log_offset, msg.store_timestamp) {
                return Some(index_file);
            }

            warn!(
                "Index file [{}] is full, trying to create another one",
                index_file.get_file_name()
            );

            index_file = self.retry_get_and_create_index_file()?;
        }
    }

    fn retry_get_and_create_index_file(&self) -> Option<Arc<IndexFile>> {
        for attempt in 1..=MAX_TRY_IDX_CREATE {
            if let Some(index_file) = self.get_and_create_last_index_file() {
                return Some(index_file);
            }

            warn!(
                "Failed to create index file, attempt {}/{}",
                attempt, MAX_TRY_IDX_CREATE
            );
            thread::sleep(Duration::from_secs(1));
        }

        self.running_flags.make_index_file_error();
        error!(
            "Failed to create index file after {} attempts, marking error flag",
            MAX_TRY_IDX_CREATE
        );
        None
    }

    pub fn get_and_create_last_index_file(&self) -> Option<Arc<IndexFile>> {
        let mut index_file = None;
        let mut prev_index_file = None;
        let mut last_update_end_phy_offset = 0;
        let mut last_update_index_timestamp = 0;

        {
            let read = self.index_file_list.read();
            if let Some(tmp) = read.last().cloned() {
                if !tmp.is_write_full() {
                    index_file = Some(tmp);
                } else {
                    last_update_end_phy_offset = tmp.get_end_phy_offset();
                    last_update_index_timestamp = tmp.get_end_timestamp();
                    prev_index_file = Some(tmp);
                }
            }
        } // Read lock released here

        if index_file.is_none() {
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
                last_update_end_phy_offset,
                last_update_index_timestamp,
            ) {
                Ok(index_file) => Arc::new(index_file),
                Err(error) => {
                    error!("create index file {} failed: {}", file_name, error);
                    self.running_flags.make_index_file_error();
                    return None;
                }
            };

            {
                let mut write = self.index_file_list.write();
                write.push(new_index_file.clone());
            } // Write lock released here

            index_file = Some(new_index_file);

            // Spawn async flush task for previous file
            if prev_index_file.is_some() {
                let index_service = self.clone();
                tokio::task::spawn_blocking(move || {
                    index_service.flush(prev_index_file);
                });
            }
        }
        index_file
    }

    #[inline]
    pub fn flush(&self, index_file: Option<Arc<IndexFile>>) {
        let Some(index_file) = index_file else {
            return;
        };

        let index_msg_timestamp = if index_file.is_write_full() {
            index_file.get_end_timestamp() as u64
        } else {
            0
        };

        index_file.flush();

        if index_msg_timestamp > 0 {
            self.store_checkpoint.set_index_msg_timestamp(index_msg_timestamp);
            let _ = self.store_checkpoint.flush();
        }
    }
}

#[inline]
fn build_key(topic: &str, key: &str) -> String {
    let mut buffer = String::with_capacity(index_key_len(topic, key, None));
    build_key_into(&mut buffer, topic, key, None);
    buffer
}

#[inline]
fn build_key_with_type(topic: &str, key: &str, index_type: &str) -> String {
    let mut buffer = String::with_capacity(index_key_len(topic, key, Some(index_type)));
    build_key_into(&mut buffer, topic, key, Some(index_type));
    buffer
}

#[inline]
fn build_key_into<'a>(buffer: &'a mut String, topic: &str, key: &str, index_type: Option<&str>) -> &'a str {
    buffer.clear();
    let required_len = index_key_len(topic, key, index_type);
    if buffer.capacity() < required_len {
        buffer.reserve(required_len - buffer.capacity());
    }

    buffer.push_str(topic);
    buffer.push('#');
    if let Some(index_type) = index_type {
        buffer.push_str(index_type);
        buffer.push('#');
    }
    buffer.push_str(key);
    buffer.as_str()
}

#[inline]
fn index_key_len(topic: &str, key: &str, index_type: Option<&str>) -> usize {
    topic.len() + key.len() + 1 + index_type.map_or(0, |index_type| index_type.len() + 1)
}

#[inline]
fn index_build_key_capacity(topic: &str, uniq_key: Option<&str>, keys: &str, tags: Option<&str>) -> usize {
    let normal_key_len = keys
        .split(MessageConst::KEY_SEPARATOR)
        .map(str::len)
        .max()
        .unwrap_or_default();
    let plain_key_len = uniq_key.map_or(normal_key_len, |uniq_key| normal_key_len.max(uniq_key.len()));
    let typed_key_len = tags.map_or(0, |tags| tags.len() + MessageConst::INDEX_TAG_TYPE.len() + 1);
    topic.len() + plain_key_len.max(typed_key_len) + 1
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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

        IndexService::new(message_store_config, store_checkpoint, running_flags)
    }

    #[test]
    fn test_build_key_formats() {
        // Test default key format: topic#key
        let key1 = build_key("TestTopic", "key123");
        assert_eq!(key1, "TestTopic#key123");

        // Test key with type format: topic#indexType#key
        let key2 = build_key_with_type("TestTopic", "tagValue", MessageConst::INDEX_TAG_TYPE);
        assert_eq!(key2, "TestTopic#T#tagValue");
    }

    #[test]
    fn index_build_key_capacity_covers_uniq_normal_and_tag_keys() {
        assert_eq!(
            index_build_key_capacity("Topic", Some("uniq-longer"), "k1 k22", Some("Tag")),
            index_key_len("Topic", "uniq-longer", None)
        );
        assert_eq!(
            index_build_key_capacity("Topic", Some("u"), "short very-long-normal-key", Some("Tag")),
            index_key_len("Topic", "very-long-normal-key", None)
        );
        assert_eq!(
            index_build_key_capacity("Topic", None, "k1", Some("longer-tag-value")),
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

        let index_service = IndexService::new(message_store_config, store_checkpoint, running_flags);

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

        for key in ["key1", "key2", "uniq123"] {
            let result = index_service.query_offset("TestTopic", key, 10, 0, i64::MAX);
            assert_eq!(result.get_phy_offsets(), &[1000], "missing index for key {key}");
        }

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
            store_timestamp: 1000000000000,
            ..DispatchRequest::default()
        });

        assert_eq!(index_service.get_total_size(), 0);
    }

    #[test]
    fn build_index_skips_rollback_without_creating_index_file() {
        let temp_dir = tempdir().unwrap();
        let index_service = new_index_service_for_test(&temp_dir, "store_checkpoint_test_rollback_index_skip");

        index_service.build_index(&DispatchRequest {
            topic: CheetahString::from_slice("TestTopic"),
            commit_log_offset: 1000,
            store_timestamp: 1000000000000,
            keys: CheetahString::from_slice("key1"),
            uniq_key: Some(CheetahString::from_slice("uniq123")),
            sys_flag: MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            ..DispatchRequest::default()
        });

        assert_eq!(index_service.get_total_size(), 0);
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

        let index_service = IndexService::new(message_store_config, store_checkpoint, running_flags);

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

        let index_service = IndexService::new(message_store_config, store_checkpoint, running_flags);

        let result = index_service.query_offset("TestTopic", "key1", -1, 0, i64::MAX);

        assert!(result.get_phy_offsets().is_empty());
        assert_eq!(result.get_index_last_update_timestamp(), 0);
        assert_eq!(result.get_index_last_update_phyoffset(), 0);
    }
}
