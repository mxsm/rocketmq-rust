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

#[derive(Debug, Clone, Copy)]
pub struct PopProcessQueueInfo {
    wait_ack_count: i32,
    droped: bool,
    last_pop_timestamp: u64,
}

impl PopProcessQueueInfo {
    pub fn new(wait_ack_count: i32, droped: bool, last_pop_timestamp: u64) -> Self {
        Self {
            wait_ack_count,
            droped,
            last_pop_timestamp,
        }
    }

    pub fn wait_ack_count(&self) -> i32 {
        self.wait_ack_count
    }

    pub fn set_wait_ack_count(&mut self, wait_ack_count: i32) {
        self.wait_ack_count = wait_ack_count;
    }

    pub fn droped(&self) -> bool {
        self.droped
    }

    pub fn set_droped(&mut self, droped: bool) {
        self.droped = droped;
    }

    pub fn last_pop_timestamp(&self) -> u64 {
        self.last_pop_timestamp
    }

    pub fn set_last_pop_timestamp(&mut self, last_pop_timestamp: u64) {
        self.last_pop_timestamp = last_pop_timestamp;
    }
}

impl std::fmt::Display for PopProcessQueueInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopProcessQueueInfo [wait_ack_count: {}, droped: {}, last_pop_timestamp: {}]",
            self.wait_ack_count, self.droped, self.last_pop_timestamp
        )
    }
}
