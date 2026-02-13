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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_process_queue_init() {
        let queue: PopProcessQueueInfo = PopProcessQueueInfo::new(10, false, 123456789);
        assert_eq!(queue.wait_ack_count(), 10);
        assert!(!queue.droped());
        assert_eq!(queue.last_pop_timestamp(), 123456789);
    }

    #[test]
    fn pop_process_queue_setters() {
        let mut queue: PopProcessQueueInfo = PopProcessQueueInfo::new(10, false, 123456789);
        queue.set_wait_ack_count(20);
        queue.set_droped(true);
        queue.set_last_pop_timestamp(987654321);

        assert_eq!(queue.wait_ack_count(), 20);
        assert!(queue.droped());
        assert_eq!(queue.last_pop_timestamp(), 987654321);
    }

    #[test]
    fn pop_process_queue_clone() {
        let queue: PopProcessQueueInfo = PopProcessQueueInfo::new(10, false, 123456789);
        let cloned = queue.clone();
        assert_eq!(cloned.wait_ack_count(), 10);
        assert!(!cloned.droped());
        assert_eq!(cloned.last_pop_timestamp(), 123456789);
    }

    #[test]
    fn pop_process_queue_display() {
        let queue: PopProcessQueueInfo = PopProcessQueueInfo::new(10, false, 123456789);
        let display = format!("{}", queue);
        assert_eq!(
            display,
            "PopProcessQueueInfo [wait_ack_count: 10, droped: false, last_pop_timestamp: 123456789]"
        );
    }

    #[test]
    fn pop_process_queue_copy() {
        let queue: PopProcessQueueInfo = PopProcessQueueInfo::new(10, false, 123456789);
        let copied = queue;
        assert_eq!(copied.wait_ack_count(), 10);
        assert!(!copied.droped());
        assert_eq!(copied.last_pop_timestamp(), 123456789);
    }
}
