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
use rocketmq_common::TimeUtils::get_current_millis;
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

            let index_file = IndexFile::new(file_path, self.hash_slot_num as usize, self.index_num as usize, 0, 0);
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

    pub fn delete_expired_file(&self, offset: u64) {
        let files = {
            let index_file_list = self.index_file_list.read();

            if index_file_list.is_empty() {
                return;
            }

            let end_phy_offset = index_file_list.first().unwrap().get_end_phy_offset() as u64;
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
        let mut phy_offsets = Vec::with_capacity(max_num as usize);
        let mut index_last_update_timestamp = 0;
        let mut index_last_update_phyoffset = 0;
        let max_num = max_num.min(self.message_store_config.max_msgs_num_batch as i32);

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
                        // Match only use INDEX_TAG_TYPE if explicitly specified
                        let query_key = if let Some(idx_type) = index_type {
                            if !idx_type.is_empty() && idx_type == MessageConst::INDEX_TAG_TYPE {
                                build_key_with_type(topic, key, MessageConst::INDEX_TAG_TYPE)
                            } else {
                                build_key(topic, key)
                            }
                        } else {
                            build_key(topic, key)
                        };
                        f.select_phy_offset(&mut phy_offsets, &query_key, max_num as usize, begin, end);
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
        let index_file = self.retry_get_and_create_index_file();
        match index_file {
            Some(index_file_inner) => {
                let end_phy_offset = index_file_inner.get_end_phy_offset();
                let topic = dispatch_request.topic.as_str();
                let keys = dispatch_request.keys.as_str();
                if dispatch_request.commit_log_offset < end_phy_offset {
                    return;
                }

                let tran_type = MessageSysFlag::get_transaction_value(dispatch_request.sys_flag);
                match tran_type {
                    MessageSysFlag::TRANSACTION_NOT_TYPE
                    | MessageSysFlag::TRANSACTION_PREPARED_TYPE
                    | MessageSysFlag::TRANSACTION_COMMIT_TYPE => {}
                    MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => return,
                    _ => {}
                }

                let mut index_file_new = Some(index_file_inner);
                if let Some(ref uniq_key) = dispatch_request.uniq_key {
                    index_file_new = self.put_key(
                        index_file_new.take().unwrap(),
                        dispatch_request,
                        build_key(topic, uniq_key.as_str()).as_str(),
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
                            index_file_new = self.put_key(
                                index_file_new.take().unwrap(),
                                dispatch_request,
                                build_key(topic, key).as_str(),
                            );
                            if index_file_new.is_none() {
                                error!(
                                    "putKey error commitlog {} uniqkey {}",
                                    dispatch_request.commit_log_offset,
                                    dispatch_request.uniq_key.as_ref().unwrap()
                                );
                                return;
                            }
                        }
                    }
                }

                // Index tags
                if let Some(properties) = &dispatch_request.properties_map {
                    if let Some(tags) = properties.get(&CheetahString::from_static_str(MessageConst::PROPERTY_TAGS)) {
                        if !tags.is_empty() {
                            index_file_new = self.put_key(
                                index_file_new.take().unwrap(),
                                dispatch_request,
                                build_key_with_type(topic, tags.as_str(), MessageConst::INDEX_TAG_TYPE).as_str(),
                            );
                            if index_file_new.is_none() {
                                error!(
                                    "putKey error commitlog {} tags {}",
                                    dispatch_request.commit_log_offset, tags
                                );
                            }
                        }
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
            if !read.is_empty() {
                let tmp = read.last().unwrap().clone();
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
                time_millis_to_human_string(get_current_millis() as i64)
            );

            let new_index_file = Arc::new(IndexFile::new(
                file_name.as_str(),
                self.hash_slot_num as usize,
                self.index_num as usize,
                last_update_end_phy_offset,
                last_update_index_timestamp,
            ));

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
    format!("{topic}#{key}")
}

#[inline]
fn build_key_with_type(topic: &str, key: &str, index_type: &str) -> String {
    format!("{topic}#{index_type}#{key}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::store::running_flags::RunningFlags;

    #[test]
    fn test_build_key_formats() {
        // Test default key format: topic#key
        let key1 = build_key("TestTopic", "key123");
        assert_eq!(key1, "TestTopic#key123");

        // Test key with type format: topic#indexType#key
        let key2 = build_key_with_type("TestTopic", "tagValue", MessageConst::INDEX_TAG_TYPE);
        assert_eq!(key2, "TestTopic#T#tagValue");
    }

    #[tokio::test]
    async fn test_build_index_with_tags() {
        // This test verifies build_index correctly processes tags from properties_map
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("store_checkpoint_test_build_index_with_tags");
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
            keys: CheetahString::from_slice("key1"),
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

        // Build index should handle tags properly without panicking
        // (actual file creation needs proper directory setup, this tests the logic flow)
        index_service.build_index(&dispatch_request);

        // Give async tasks time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    #[test]
    fn test_query_offset_with_type_api() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("store_checkpoint_test_query_offset_with_type_api");
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
}
