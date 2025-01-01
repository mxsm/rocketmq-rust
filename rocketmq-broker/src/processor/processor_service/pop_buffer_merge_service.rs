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

use rocketmq_store::pop::AckMessage;

pub(crate) struct PopBufferMergeService;

impl PopBufferMergeService {
    pub fn add_ack(&mut self, _revive_qid: i32, _ack_msg: &dyn AckMessage) -> bool {
        unimplemented!("Not implemented yet");
    }

    pub fn get_latest_offset(&self, _lock_key: &str) -> i64 {
        unimplemented!("Not implemented yet");
    }

    pub fn clear_offset_queue(&self, _lock_key: &str) {
        unimplemented!("Not implemented yet");
    }
}
