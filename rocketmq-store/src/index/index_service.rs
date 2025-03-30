/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
}

impl IndexService {
    #[inline]
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        Self {
            hash_slot_num: message_store_config.max_hash_slot_num,
            index_num: message_store_config.max_index_num,
            store_path: get_store_path_index(message_store_config.store_path_root_dir.as_str()),
            index_file_list: Arc::new(Default::default()),
            message_store_config,
            store_checkpoint,
        }
    }

    pub fn start(&self) {
        //nothing to do
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
        if let Ok(ls) = fs::read_dir(dir) {
            let files: Vec<_> = ls
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect();
            let mut files = files;
            files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

            let mut write_list = self.index_file_list.write();
            for file in files {
                let index_file = IndexFile::new(
                    file.to_str().unwrap(),
                    self.hash_slot_num as usize,
                    self.index_num as usize,
                    0,
                    0,
                );
                index_file.load();
                if !last_exit_ok
                    && index_file.get_end_timestamp()
                        > self.store_checkpoint.index_msg_timestamp() as i64
                {
                    index_file.destroy(0);
                    continue;
                }
                write_list.push(Arc::new(index_file));
            }
        }
        true
    }

    pub fn get_total_size(&self) -> u64 {
        if self.index_file_list.read().is_empty() {
            0
        } else {
            let read_guard = self.index_file_list.read();
            (read_guard.first().unwrap().get_file_size() * read_guard.len()) as u64
        }
    }

    pub fn delete_expired_file(&self, offset: u64) {
        let mut index_file_list_lock = self.index_file_list.write();
        if index_file_list_lock.is_empty() {
            return;
        }
        let mut files = Vec::new();
        for index_file in index_file_list_lock.iter() {
            if (index_file.get_end_phy_offset() as u64) < offset {
                files.push(index_file.clone());
            } else {
                break;
            }
        }
        if !files.is_empty() {
            index_file_list_lock.retain(|index_file| !files.contains(index_file));
        }
    }

    pub fn destroy(&self) {
        let mut index_file_list_lock = self.index_file_list.write();
        for index_file in index_file_list_lock.iter() {
            index_file.destroy(1000 * 3);
        }
        index_file_list_lock.clear();
    }

    pub fn query_offset(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> QueryOffsetResult {
        let mut phy_offsets = Vec::new();
        let mut index_last_update_timestamp = 0;
        let mut index_last_update_phyoffset = 0;
        let max_num = max_num.min(self.message_store_config.max_msgs_num_batch as i32);

        let index_file_list = self.index_file_list.read();
        let mut index = 0;
        if !index_file_list.is_empty() {
            for f in index_file_list.iter() {
                let last_file = index == index_file_list.len() - 1;
                if last_file {
                    // Assuming IndexFile has methods to get end timestamp and phy offset
                    index_last_update_timestamp = f.get_end_timestamp();
                    index_last_update_phyoffset = f.get_end_phy_offset();
                }

                // Assuming IndexFile has a method to check if the file's timestamps match the query
                if f.is_time_matched(begin, end) {
                    // Assuming IndexFile has a method to select physical offsets based on the query
                    f.select_phy_offset(
                        &mut phy_offsets,
                        build_key(topic, key).as_str(),
                        max_num as usize,
                        begin,
                        end,
                    );
                }

                if f.get_begin_timestamp() < begin {
                    break;
                }

                if phy_offsets.len() as i32 >= max_num {
                    break;
                }
                index += 1;
            }
        }
        QueryOffsetResult::new(
            phy_offsets,
            index_last_update_timestamp,
            index_last_update_phyoffset,
        )
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
                    | MessageSysFlag::TRANSACTION_COMMIT_TYPE => (),
                    MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => return,
                    _ => (),
                }

                let mut index_file_new = None;
                if let Some(ref uniq_key) = dispatch_request.uniq_key {
                    index_file_new = self.put_key(
                        index_file_inner,
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
                            let index_file = self.put_key(
                                index_file_new.take().unwrap(),
                                dispatch_request,
                                build_key(topic, key).as_str(),
                            );
                            if index_file.is_none() {
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
            }
            None => {
                error!("build index error, stop building index");
            }
        }
    }

    fn put_key(
        &self,
        mut index_file: Arc<IndexFile>,
        msg: &DispatchRequest,
        idx_key: &str,
    ) -> Option<Arc<IndexFile>> {
        let mut ok = index_file.put_key(idx_key, msg.commit_log_offset, msg.store_timestamp);

        while !ok {
            warn!(
                "Index file [{}] is full, trying to create another one",
                index_file.get_file_name()
            );

            match self.retry_get_and_create_index_file() {
                Some(new_index_file) => {
                    index_file = new_index_file;
                    ok = index_file.put_key(idx_key, msg.commit_log_offset, msg.store_timestamp);
                }
                None => return None,
            }
        }

        Some(index_file)
    }

    fn retry_get_and_create_index_file(&self) -> Option<Arc<IndexFile>> {
        let mut index_file = None;

        for times in 0..MAX_TRY_IDX_CREATE {
            index_file = self.get_and_create_last_index_file();
            if index_file.is_some() {
                break;
            }

            info!("Tried to create index file {} times", times);
            thread::sleep(Duration::from_secs(1));
        }

        if index_file.is_none() {
            error!("Mark index file cannot build flag");
        }

        index_file
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
        }

        if index_file.is_none() {
            let file_name = format!(
                "{}{}{}",
                self.store_path,
                std::path::MAIN_SEPARATOR,
                time_millis_to_human_string(get_current_millis() as i64)
            );
            index_file = Some(Arc::new(IndexFile::new(
                file_name.as_str(),
                self.hash_slot_num as usize,
                self.index_num as usize,
                last_update_end_phy_offset,
                last_update_index_timestamp,
            )));

            {
                let mut write = self.index_file_list.write();
                write.push(index_file.clone().unwrap());
            }

            if let Some(ref _index_file) = index_file {
                let flush_this_file = prev_index_file.clone();
                let index_service = self.clone();
                tokio::spawn(async move {
                    index_service.flush(flush_this_file);
                });
                //flush_thread.join().unwrap();
            }
        }
        index_file
    }

    pub fn flush(&self, index_file: Option<Arc<IndexFile>>) {
        match index_file {
            None => {}
            Some(index_file) => {
                let mut index_msg_timestamp = 0u64;
                if index_file.is_write_full() {
                    index_msg_timestamp = index_file.get_end_timestamp() as u64;
                }
                index_file.flush();
                if index_msg_timestamp > 0 {
                    self.store_checkpoint
                        .set_index_msg_timestamp(index_msg_timestamp);
                    let _ = self.store_checkpoint.flush();
                }
            }
        }
    }
}

#[inline]
fn build_key(topic: &str, key: &str) -> String {
    let mut keys = String::new();
    keys.push_str(topic);
    keys.push('#');
    keys.push_str(key);
    keys
}
