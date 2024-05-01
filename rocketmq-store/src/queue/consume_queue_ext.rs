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
use std::path::PathBuf;

use crate::consume_queue::mapped_file_queue::MappedFileQueue;

const END_BLANK_DATA_LENGTH: usize = 4;

/// Addr can not exceed this value. For compatible.
const MAX_ADDR: i64 = i32::MIN as i64 - 1;
const MAX_REAL_OFFSET: i64 = MAX_ADDR - i64::MIN;

#[derive(Clone)]
pub struct ConsumeQueueExt {
    mapped_file_queue: MappedFileQueue,
    topic: String,
    queue_id: i32,
    store_path: String,
    mapped_file_size: i32,
}

impl ConsumeQueueExt {
    pub fn new(
        topic: String,
        queue_id: i32,
        store_path: String,
        mapped_file_size: i32,
        bit_map_length: i32,
    ) -> Self {
        let queue_dir = PathBuf::from(store_path.clone())
            .join(topic.clone())
            .join(queue_id.to_string());
        let mapped_file_queue = MappedFileQueue::new(
            queue_dir.to_string_lossy().to_string(),
            mapped_file_size as u64,
            None,
        );
        Self {
            mapped_file_queue,
            topic,
            queue_id,
            store_path,
            mapped_file_size,
        }
    }

    pub fn is_ext_addr(address: i64) -> bool {
        address <= MAX_ADDR
    }
}

impl ConsumeQueueExt {
    pub fn truncate_by_max_address(&self, max_address: i64) {}
}
