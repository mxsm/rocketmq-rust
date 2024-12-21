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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_rust::ArcMut;

use crate::consumer::pull_status::PullStatus;

pub struct PullResult {
    pub pull_status: PullStatus,
    pub next_begin_offset: u64,
    pub min_offset: u64,
    pub max_offset: u64,
    pub msg_found_list: Option<Vec<ArcMut<MessageExt>>>,
}

impl PullResult {
    pub fn new(
        pull_status: PullStatus,
        next_begin_offset: u64,
        min_offset: u64,
        max_offset: u64,
        msg_found_list: Option<Vec<ArcMut<MessageExt>>>,
    ) -> Self {
        Self {
            pull_status,
            next_begin_offset,
            min_offset,
            max_offset,
            msg_found_list,
        }
    }

    pub fn pull_status(&self) -> &PullStatus {
        &self.pull_status
    }

    pub fn next_begin_offset(&self) -> u64 {
        self.next_begin_offset
    }

    pub fn min_offset(&self) -> u64 {
        self.min_offset
    }

    pub fn max_offset(&self) -> u64 {
        self.max_offset
    }

    pub fn msg_found_list(&self) -> &Option<Vec<ArcMut<MessageExt>>> {
        &self.msg_found_list
    }

    pub fn set_msg_found_list(&mut self, msg_found_list: Option<Vec<ArcMut<MessageExt>>>) {
        self.msg_found_list = msg_found_list;
    }
}

impl std::fmt::Display for PullResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PullResult [pull_status: {:?}, next_begin_offset: {}, min_offset: {}, max_offset: \
             {}, msg_found_list: {}]",
            self.pull_status,
            self.next_begin_offset,
            self.min_offset,
            self.max_offset,
            self.msg_found_list.as_ref().map_or(0, |v| v.len()),
        )
    }
}
