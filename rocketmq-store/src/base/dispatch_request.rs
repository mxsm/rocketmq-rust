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

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

#[derive(Debug, Default)]
pub struct DispatchRequest {
    pub topic: String,
    pub queue_id: i32,
    pub commit_log_offset: i64,
    pub msg_size: i32,
    pub tags_code: i64,
    pub store_timestamp: i64,
    pub consume_queue_offset: i64,
    pub keys: String,
    pub success: bool,
    pub uniq_key: Option<String>,
    pub sys_flag: i32,
    pub prepared_transaction_offset: i64,
    pub properties_map: HashMap<String, String>,
    pub bit_map: Vec<u8>,
    pub buffer_size: i32,
    pub msg_base_offset: i64,
    pub batch_size: i16,
    pub next_reput_from_offset: i64,
    pub offset_id: String,
}

impl Display for DispatchRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DispatchRequest {{ topic: {}, queue_id: {}, commit_log_offset: {}, msg_size: {}, \
             tags_code: {}, store_timestamp: {}, consume_queue_offset: {}, keys: {}, success: {}, \
             uniq_key: {:?}, sys_flag: {}, prepared_transaction_offset: {}, properties_map: {:?}, \
             bit_map: {:?}, buffer_size: {}, msg_base_offset: {}, batch_size: {}, \
             next_reput_from_offset: {}, offset_id: {} }}",
            self.topic,
            self.queue_id,
            self.commit_log_offset,
            self.msg_size,
            self.tags_code,
            self.store_timestamp,
            self.consume_queue_offset,
            self.keys,
            self.success,
            self.uniq_key,
            self.sys_flag,
            self.prepared_transaction_offset,
            self.properties_map,
            self.bit_map,
            self.buffer_size,
            self.msg_base_offset,
            self.batch_size,
            self.next_reput_from_offset,
            self.offset_id
        )
    }
}
