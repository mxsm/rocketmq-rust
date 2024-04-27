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

use std::collections::HashMap;

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
